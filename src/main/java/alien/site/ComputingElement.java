package alien.site;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.taskQueue.GetNumberFreeSlots;
import alien.api.taskQueue.GetNumberWaitingJobs;
import alien.api.token.GetTokenCertificate;
import alien.api.token.TokenCertificateType;
import alien.config.ConfigUtils;
import alien.log.LogUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.batchqueue.BatchQueue;
import alien.site.packman.CVMFS;
import alien.user.JAKeyStore;
import apmon.ApMon;
import apmon.ApMonException;
import lazyj.ExtProperties;
import lazyj.Utils;

/**
 * @author mmmartin
 *
 */
public final class ComputingElement extends Thread {

	/**
	 * ApMon sender
	 */
	static final ApMon apmon = MonitorFactory.getApMonSender();

	// Logger object
	private static Logger logger = ConfigUtils.getLogger(ComputingElement.class.getCanonicalName());

	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

	// Config, env, classad
	private int port = 10000;
	private String site;
	private HashMap<String, Object> siteMap = new HashMap<>();
	private HashMap<String, Object> config = null;
	private HashMap<String, String> host_environment = null;
	private HashMap<String, String> ce_environment = null;
	private BatchQueue queue = null;
	private String host_logdir_resolved = "";

	/**
	 *
	 */
	public ComputingElement() {
		try {
			config = ConfigUtils.getConfigFromLdap();
			site = (String) config.get("site_accountname");
			getSiteMap();

			ExtProperties ep = ConfigUtils.getConfiguration("ce-logging");
			if (ep != null) {
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				ep.getProperties().store(output, null);
				ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
				LogManager.getLogManager().readConfiguration(input);
			}

			queue = getBatchQueue((String) config.get("ce_type"));

			host_logdir_resolved = Functions.resolvePathWithEnv((String) config.get("host_logdir"));
			if (host_logdir_resolved == null)
				host_logdir_resolved = Functions.resolvePathWithEnv((String) config.get("host_tmpdir"));

			logger = LogUtils.redirectToCustomHandler(logger, host_logdir_resolved + "/CE");

			if (config.containsKey("proxy_cache_file")) {
				int ttl = ((Integer) siteMap.get("TTL")).intValue();
				TokenFileGenerationThread tk = new TokenFileGenerationThread((String) config.get("proxy_cache_file"), logger, ttl);
				tk.start();
			}

		}
		catch (final Exception e) {
			logger.severe("Problem in construction of ComputingElement: " + e.toString());
		}
	}

	private static final long LDAP_REFRESH_INTERVAL = 5 * 60 * 1000L;
	private long lastLdapRefresh = 0;

	@Override
	public void run() {
		logger.log(Level.INFO, "Starting ComputingElement in " + config.get("host_host"));

		try {
			Files.writeString(Paths.get(host_logdir_resolved + "/CE.pid"),
					Integer.toString(MonitorFactory.getSelfProcessID()));
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Could not create a pidfile in: " + host_logdir_resolved + "/CE.pid", e);
		}

		logger.info("Looping");
		while (true) {

			try {
				if (JAKeyStore.checkExpireSoonAndReload() >= 2) {
					// we need at least a couple of days if valid identity to request JA tokens with
					logger.log(Level.WARNING, "Certificate is about to expire and a newer one was not available");
					System.exit(0);
				}
			}
			catch (Exception e) {
				logger.log(Level.SEVERE, "Exception reloading the identity", e);
				System.exit(1);
			}

			if (System.currentTimeMillis() - lastLdapRefresh > LDAP_REFRESH_INTERVAL) {
				logger.info("Time to sync with LDAP");
				logger.info("Building new SiteMap.");

				try {
					final HashMap<String, Object> newConfig = ConfigUtils.getConfigFromLdap();
					if (newConfig != null && !newConfig.equals(config)) {
						config = newConfig;

						site = (String) config.get("site_accountname");
						getSiteMap();

						logger.info("New sitemap: ");

						siteMap.forEach((field, entry) -> {
							logger.info("[" + field + ": " + entry + "]");
						});
						lastStartupScriptGenerated = 0;
					}
				}
				catch (Exception ex) {
					logger.log(Level.WARNING, "Error syncing with LDAP: ", ex);
				}

				lastLdapRefresh = System.currentTimeMillis();
			}

			offerAgent();

			try {
				Thread.sleep(
						System.getenv("ce_loop_time") != null ? Long.parseLong(System.getenv("ce_loop_time")) : 60000);
			}
			catch (InterruptedException e) {
				logger.severe("Unable to sleep: " + e.toString());
			}

		}

	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		ConfigUtils.setApplicationName("CE");

		final ComputingElement CE = new ComputingElement();
		CE.start();
	}

	private int getNumberFreeSlots() {
		// First we get the maxJobs and maxQueued from the Central Services
		final GetNumberFreeSlots jobSlots = commander.q_api.getNumberFreeSlots((String) config.get("host_host"), port, siteMap.get("CE").toString(),
				ConfigUtils.getConfig().gets("version", "J-1.0").trim());

		if (jobSlots == null) {
			logger.info("Cannot get values from getNumberFreeSlots");
			return 0;
		}

		final List<Integer> slots = jobSlots.getJobSlots();
		int max_jobs = 0;
		int max_queued = 0;

		if (slots == null) {
			logger.info("Cannot get values from getNumberFreeSlots");
			return 0;
		}

		if (slots.get(0).intValue() == 0 && slots.size() >= 3) { // OK
			max_jobs = slots.get(1).intValue();
			max_queued = slots.get(2).intValue();
			logger.info("Max jobs: " + max_jobs + " Max queued: " + max_queued);
		}
		else { // Error
			switch (slots.get(0).intValue()) {
				case 1:
					logger.info("Failed getting or inserting host in getNumberFreeSlots");
					break;
				case 2:
					logger.info("Failed updating host in getNumberFreeSlots");
					break;
				case 3:
					logger.info("Failed getting slots in getNumberFreeSlots");
					break;
				case -2:
					logger.info("The queue is centrally locked!");
					break;
				default:
					logger.info("Unknown error in getNumberFreeSlots");
			}
			return 0;
		}

		// Now we get the values from the batch interface and calculate the slots available
		final int queued = queue.getNumberQueued();
		if (queued < 0) {
			logger.info("There was a problem getting the number of queued agents!");
			return 0;
		}
		logger.info("Agents queued: " + queued);

		final int active = queue.getNumberActive();
		if (active < 0) {
			logger.info("There was a problem getting the number of active agents!");
			return 0;
		}
		logger.info("Agents active: " + active);

		int free = max_queued - queued;
		if ((max_jobs - active) < free)
			free = max_jobs - active;

		// Report slot status to ML
		final Vector<String> paramNames = new Vector<>();
		final Vector<Object> paramValues = new Vector<>();
		paramNames.add("jobAgents_queued");
		paramNames.add("jobAgents_running");
		paramNames.add("jobAgents_slots");
		paramValues.add(Integer.valueOf(queued));
		paramValues.add(Integer.valueOf(active - queued));
		paramValues.add(Integer.valueOf(free < 0 ? 0 : free));

		try {
			apmon.sendParameters(site + "_CE_" + siteMap.get("CE"), config.get("host_host").toString(), paramNames.size(), paramNames, paramValues);
		}
		catch (ApMonException | IOException e) {
			logger.severe("Can't send parameter to ML (getNumberFreeSlots): " + e);
		}

		return free;
	}

	private void offerAgent() {
		int slots_to_submit = getNumberFreeSlots();

		if (slots_to_submit <= 0) {
			logger.info("No slots available in the CE!");
			return;
		}
		logger.info("CE free slots: " + slots_to_submit);

		// We ask the broker how many jobs we could run
		final GetNumberWaitingJobs jobMatch = commander.q_api.getNumberWaitingForSite(siteMap);
		if (jobMatch == null) {
			logger.warning("Could not get number of waiting jobs!");
			return;
		}

		final int waiting_jobs = jobMatch.getNumberJobsWaitingForSite().intValue();

		if (waiting_jobs <= 0) {
			logger.info("Broker returned 0 available waiting jobs");
			return;
		}
		logger.info("Waiting jobs: " + waiting_jobs);

		if (waiting_jobs < slots_to_submit)
			slots_to_submit = waiting_jobs;

		logger.info("Going to submit " + slots_to_submit + " agents");

		final String script = getAgentStartupScript();
		if (script == null) {
			logger.info("Cannot create startup script");
			return;
		}
		logger.info("Created AgentStartup script: " + script);

		while (slots_to_submit > 0) {
			queue.submit(script);
			slots_to_submit--;
		}

		logger.info("Submitted " + slots_to_submit);

		return;
	}

	private String lastStartupScript = null;
	private long lastStartupScriptGenerated = 0;

	private static final long SCRIPT_REFRESH_INTERVAL = 5 * 60 * 1000L;

	private String getAgentStartupScript() {
		// reuse the previously generated JA startup script for 5 minutes, but check that it still exists on disk
		if (System.currentTimeMillis() - lastStartupScriptGenerated < SCRIPT_REFRESH_INTERVAL && lastStartupScript != null) {
			final File fTest = new File(lastStartupScript);

			if (fTest.exists())
				return lastStartupScript;
		}

		final String newStartupScript = createAgentStartup();

		// only update the pointer if the new script could be written to disk, otherwise try to reuse the previous one
		if (newStartupScript != null) {
			if (lastStartupScript != null && !lastStartupScript.equals(newStartupScript)) {
				final File fPrevious = new File(lastStartupScript);

				if (fPrevious.exists())
					fPrevious.delete();
			}

			lastStartupScript = newStartupScript;
			lastStartupScriptGenerated = System.currentTimeMillis();
		}

		return lastStartupScript;
	}

	private static String startup_customization(int i) {
		final String custom_file = System.getenv("HOME") + "/JA-custom-" + i + ".sh";
		String s = "";

		if ((new File(custom_file)).exists()) {
			s += "\n#\n# customization " + i + " start\n#\n\n";

			final String content = Utils.readFile(custom_file);

			if (content != null)
				s += content;
			else {
				logger.log(Level.INFO, "Error reading " + custom_file);
				return "";
			}

			s += "\n#\n# customization " + i + " end\n#\n\n";
			logger.info("JA customization " + i + " added from file: " + custom_file);
		}

		return s;
	}

	/*
	 * Creates script to execute on worker nodes
	 */
	private String createAgentStartup() {
		String before = "";

		final String host_tempdir_resolved = Functions.resolvePathWithEnv((String) config.get("host_tmpdir"));

		int ttl_hours = ((Integer) siteMap.get("TTL")).intValue();
		ttl_hours = ttl_hours / 3600;

		// get the jobagent token
		final String[] token_certificate_full = getTokenCertificate(ttl_hours / 24 + 1);
		final String token_cert_str = token_certificate_full[0].trim();
		final String token_key_str = token_certificate_full[1].trim();

		// prepare the jobagent token certificate
		before += "export JALIEN_TOKEN_CERT=\"" + token_cert_str + "\";\n" + "export JALIEN_TOKEN_KEY=\"" + token_key_str + "\";\n";
		if (config.containsKey("proxy_cache_file")) {
			String resolvedPath = Functions.resolvePathWithEnv((String) (config.get("proxy_cache_file")));
			before += "if test -f \'"
					+ resolvedPath
					+ "\' ; then\n";
			before += "source "
					+ resolvedPath
					+ "\n";
			before += "fi\n";
		}

		// pass environment variables
		before += "export HOME=`pwd`" + "\n";
		before += "export PATH=`echo $PATH`" + "\n";
		before += "export LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH`" + "\n";
		before += "export TMP=$HOME" + "\n";
		before += "export TMPDIR=$TMP" + "\n";
		if (config.containsKey("host_logdir") || config.containsKey("site_logdir"))
			before += "export LOGDIR=\"" + (config.containsKey("host_logdir") ? config.get("host_logdir") : config.get("site_logdir")) + "\"\n";
		if (config.containsKey("host_cachedir") || config.containsKey("site_cachedir"))
			before += "export CACHEDIR=\"" + (config.containsKey("host_cachedir") ? config.get("host_cachedir") : config.get("site_cachedir")) + "\"\n";
		if (config.containsKey("host_tmpdir") || config.containsKey("site_tmpdir"))
			before += "export TMPDIR=\"" + (config.containsKey("host_tmpdir") ? config.get("host_tmpdir") : config.get("site_tmpdir")) + "\"\n";
		if (config.containsKey("host_workdir") || config.containsKey("site_workdir"))
			before += "export WORKDIR=\"" + (config.containsKey("host_workdir") ? config.get("host_workdir") : config.get("site_workdir")) + "\"\n";
		// if (System.getenv().containsKey("cerequirements"))
		// before += "export cerequirements=\'" + System.getenv().get("cerequirements") + "\'\n";
		// if (System.getenv().containsKey("partition"))
		// before += "export partition=\"" + System.getenv().get("partition") + "\"\n";
		if (config.containsKey("ce_cerequirements"))
			before += "export cerequirements=\'" + config.get("ce_cerequirements") + "\'\n";
		if (siteMap.containsKey("RequiredCpusCe"))
			before += "export RequiredCpusCe=\"" + siteMap.get("RequiredCpusCe") + "\"\n";
		if (siteMap.containsKey("cpuIsolation"))
			before += "export cpuIsolation=\"" + siteMap.get("cpuIsolation") + "\"\n";
		before += "export ALIEN_CM_AS_LDAP_PROXY=\"" + config.get("ALIEN_CM_AS_LDAP_PROXY") + "\"\n";
		before += "export site=\"" + site + "\"\n";
		before += "export ALIEN_SITE=\"" + site + "\"\n";
		before += "export CE=\"" + siteMap.get("CE") + "\"\n";
		before += "export CEhost=\"" + siteMap.get("Localhost") + "\"\n";
		before += "export TTL=\"" + siteMap.get("TTL") + "\"\n";
		before += "export -n _LMFILES_" + "\n";
		/*
		 * If the admin wants to use another ML instance, on the same
		 * site, we should enable them to do so
		 */
		if (config.containsKey("monalisa_host"))
			before += "export APMON_CONFIG=\"" + config.get("monalisa_host") + "\"\n";
		else if (config.containsKey("APMON_CONFIG"))
			before += "export APMON_CONFIG='" + config.get("APMON_CONFIG") + "'\n";
		else
			before += "export APMON_CONFIG='" + ConfigUtils.getLocalHostname() + "'\n";
		if (config.containsKey("ce_installationmethod"))
			before += "export installationMethod=\"" + config.get("ce_installationmethod") + "\"\n";
		if (config.containsKey("RESERVED_DISK"))
			before += "export RESERVED_DISK='" + config.get("RESERVED_DISK") + "'\n";
		if (config.containsKey("RESERVED_RAM"))
			before += "export RESERVED_RAM='" + config.get("RESERVED_RAM") + "'\n";
		if (config.containsKey("MAX_RETRIES"))
			before += "export MAX_RETRIES='" + config.get("MAX_RETRIES") + "'\n";
		if (config.containsKey("ce_matcharg") && getValuesFromLDAPField(config.get("ce_matcharg")).containsKey("cpucores"))
			before += "export CPUCores=\"" + getValuesFromLDAPField(config.get("ce_matcharg")).get("cpucores") + "\"\n";
		if (siteMap.containsKey("closeSE"))
			before += "export closeSE=\"" + siteMap.get("closeSE") + "\"\n";
		if (siteMap.containsKey("Partition"))
			before += "export partition=\"" + siteMap.get("Partition") + "\"\n";

		final ExtProperties containerConfig = ConfigUtils.getConfiguration("container");
		if (Objects.nonNull(containerConfig)) {
			if (!containerConfig.gets("additional.binds").isBlank())
				before += "export ADDITIONAL_BINDS='" + containerConfig.gets("additional.binds") + "'\n";
			if (!containerConfig.gets("meta.variables", "").isBlank())
				before += "export META_VARIABLES='" + containerConfig.gets("meta.variables") + "'\n";
			if (!containerConfig.gets("job.container.path").isBlank())
				before += "export JOB_CONTAINER_PATH='" + containerConfig.gets("job.container.path") + "'\n";
		}

		//
		// allow any shell code to be inserted for debugging, testing etc.
		//

		before += startup_customization(0);

		before += "export ALIENV=\"$( (" + CVMFS.getAlienvPrint() + ") 2> >(if grep -q 'ERROR'; then echo 'export ALIENV_ERRORS=TRUE;'; fi;) )" + "\"\n";

		before += startup_customization(1);

		before += "source <( echo $ALIENV ); " + "\n";

		before += "export XRDCP_ERRORS=$(xrdcp 2> >(if grep -q 'error\\|not found' ; then echo 'TRUE'; fi;) ) " + "\n";

		before += "export JALIEN_JOBAGENT_CMD=\"" + getStartup() + "\"\n";

		final String content_str = before + "eval $JALIEN_JOBAGENT_CMD" + "\n" + startup_customization(2);

		final String agent_startup_path = host_tempdir_resolved + "/agent.startup." + ProcessHandle.current().pid();
		final File agent_startup_file = new File(agent_startup_path);
		try {
			agent_startup_file.createNewFile();
			agent_startup_file.setExecutable(true);

			//
			// rather keep those files for debugging:
			//
			// agent_startup_file.deleteOnExit();
		}
		catch (final IOException e1) {
			logger.info("Error creating Agent Startup file: " + e1.toString());
			return null;
		}

		try (PrintWriter writer = new PrintWriter(agent_startup_path, "UTF-8")) {
			writer.println("#!/bin/bash");
			// writer.println("[ \"$HOME\" != \"\" ] && exec -c $0"); // Used to start with a clean env
			writer.println(content_str);
		}
		catch (final FileNotFoundException e) {
			logger.info("Agent Startup file not found: " + e.toString());
		}
		catch (final UnsupportedEncodingException e) {
			logger.info("Encoding error while writing Agent Startup file: " + e.toString());
		}

		return agent_startup_path;
	}

	private String[] getTokenCertificate(final int ttl_days) {
		GetTokenCertificate gtc = new GetTokenCertificate(commander.getUser(), commander.getUsername(), TokenCertificateType.JOB_AGENT_TOKEN, null, ttl_days);

		try {
			gtc = Dispatcher.execute(gtc);
			final String[] token_cert_and_key = new String[2];
			token_cert_and_key[0] = gtc.getCertificateAsString();
			token_cert_and_key[1] = gtc.getPrivateKeyAsString();
			return token_cert_and_key;
		}
		catch (Exception e) {
			logger.info("Getting JobAgent TokenCertificate failed: " + e);
		}

		return null;
	}

	private static String getStartup() {

		final String javaCmd = "java -client -Xms16M -Xmx128M -Djdk.lang.Process.launchMechanism=vfork -XX:+UseSerialGC -cp";
		final String jarPath = "$(dirname $(which jalien))/../lib/alien-users.jar";
		final String jarClass = "alien.site.JobRunner";

		final String jarPathCustom = ConfigUtils.getConfiguration("version").gets("custom.jobagent.jar");
		if (jarPathCustom != null && !jarPathCustom.isBlank())
			return javaCmd + " " + jarPathCustom + " " + jarClass;

		return javaCmd + " " + jarPath + " " + jarClass;
	}

	/**
	 * Class to periodically update the JA token in a shared folder, for long waiting JAs in the queue to find a fresh one at startup
	 */
	final class TokenFileGenerationThread extends Thread {
		private final String resolvedPath;
		private final Logger log;
		private final int ttlDays;

		/**
		 * @param tokenFilePath location where to store the refreshed token files
		 * @param logr an instance of the logger to write important stuff to
		 * @param ttl in seconds
		 */
		public TokenFileGenerationThread(final String tokenFilePath, final Logger logr, final int ttl) {
			this.resolvedPath = Functions.resolvePathWithEnv(tokenFilePath);
			this.ttlDays = ttl / 3600 / 24 + 1;
			this.log = logr;
		}

		@Override
		public void run() {
			log.info("Starting");

			setName("Token file generation thread");
			while (true) {
				String[] certs = getTokenCertificate(ttlDays);
				if (certs != null) {
					try (FileOutputStream writer = new FileOutputStream(resolvedPath)) {
						String certCmd = "export JALIEN_TOKEN_CERT=\""
								+ certs[0].trim() + "\";\n";
						String keyCmd = "export JALIEN_TOKEN_KEY=\""
								+ certs[1].trim() + "\";\n";
						writer.write((certCmd + keyCmd).getBytes());
					}
					catch (Exception e) {
						log.log(Level.WARNING, "Exception writing token to " + resolvedPath, e);
						break;
					}
				}
				try {
					Thread.sleep(5 * 60 * 1000);
				}
				catch (InterruptedException e) {
					logger.log(Level.WARNING, "Getting JobAgent TokenCertificate failed", e);
				}
			}
		}
	}

	/**
	 * Prepares a hash to create the sitemap
	 */
	void getSiteMap() {
		final HashMap<String, String> smenv = new HashMap<>();

		smenv.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);
		config.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);

		smenv.put("site", site);
		smenv.put("CEhost", ConfigUtils.getLocalHostname());
		smenv.put("CE", "ALICE::" + site + "::" + config.get("host_ce"));

		// TTL will be recalculated in the loop depending on proxy lifetime
		if (config.containsKey("ce_ttl"))
			smenv.put("TTL", (String) config.get("ce_ttl"));
		else
			smenv.put("TTL", "86400");

		// if (System.getenv().containsKey("cerequirements"))
		// smenv.put("cerequirements", System.getenv().get("cerequirements")); //Local overrides value in LDAP if present
		// else
		if (config.containsKey("ce_cerequirements"))
			smenv.put("cerequirements", config.get("ce_cerequirements").toString());

		if (config.containsKey("ce_partition"))
			smenv.put("partition", config.get("ce_partition").toString());

		if (config.containsKey("host_closese"))
			smenv.put("closeSE", config.get("host_closese").toString());
		else if (config.containsKey("site_closese"))
			smenv.put("closeSE", config.get("site_closese").toString());

		if (config.containsKey("host_environment"))
			host_environment = getValuesFromLDAPField(config.get("host_environment"));

		// environment field can have n values
		if (config.containsKey("ce_environment"))
			ce_environment = getValuesFromLDAPField(config.get("ce_environment"));

		if (host_environment != null)
			config.putAll(host_environment);
		if (ce_environment != null)
			config.putAll(ce_environment);

		siteMap = (new SiteMap()).getSiteParameters(smenv);

		// CE storage space does not matter for WNs
		siteMap.remove("Disk");

		if (config.containsKey("ce_matcharg") && getValuesFromLDAPField(config.get("ce_matcharg")).containsKey("cpucores")) {
			siteMap.put("CPUCores", Integer.valueOf(getValuesFromLDAPField(config.get("ce_matcharg")).get("cpucores")));
		}
	}

	/**
	 * @param field
	 * @return Map with the values of the LDAP field
	 */
	public static HashMap<String, String> getValuesFromLDAPField(final Object field) {
		final HashMap<String, String> map = new HashMap<>();
		if (field instanceof TreeSet) {
			@SuppressWarnings("unchecked")
			final Set<String> host_env_set = (Set<String>) field;
			for (final String env_entry : host_env_set) {
				final String[] host_env_str = env_entry.split("=");
				map.put(host_env_str[0].toLowerCase(), host_env_str[1]);
			}
		}
		else {
			final String[] host_env_str = ((String) field).split("=");
			map.put(host_env_str[0].toLowerCase(), host_env_str[1]);
		}
		return map;
	}

	/**
	 * Get queue class with reflection
	 *
	 * @param type batch queue base class name
	 * @return an instance of that class, if found
	 */
	BatchQueue getBatchQueue(final String type) {
		Class<?> cl = null;
		try {
			cl = Class.forName("alien.site.batchqueue." + type);
		}
		catch (final ClassNotFoundException e) {
			logger.severe("Cannot find class for type: " + type + "\n" + e);
			return null;
		}

		Constructor<?> con = null;
		try {
			con = cl.getConstructor(config.getClass(), logger.getClass());
		}
		catch (NoSuchMethodException | SecurityException e) {
			logger.severe("Cannot find class for ceConfig: " + e);
			return null;
		}

		// prepare some needed fields for the queue
		if (!config.containsKey("host_host") || (config.get("host_host") == null))
			config.put("host_host", ConfigUtils.getLocalHostname());

		if (!config.containsKey("host_port") || (config.get("host_port") == null))
			config.put("host_port", Integer.valueOf(this.port));

		try {
			queue = (BatchQueue) con.newInstance(config, logger);
		}
		catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			logger.severe("Cannot instantiate queue class for type: " + type + "\n" + e);
		}

		return queue;
	}

}
