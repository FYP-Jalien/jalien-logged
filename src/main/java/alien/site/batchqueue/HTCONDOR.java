package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.site.Functions;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;

/**
 * @author mmmartin
 */
public class HTCONDOR extends BatchQueue {

	private final HashMap<String, String> environment = new HashMap<>();
	private TreeSet<String> envFromConfig;
	private final String submitCmd;
	private String submitArgs = "";
	private String htc_logdir = "$HOME/htcondor";
	private String grid_resource = null;
	private String local_pool = null;
	private String token_file = System.getenv("HOME") + "/.globus/wlcg.dat";
	private int use_token = 0;
	private String proxy = null;
	private boolean use_job_router = false;
	private boolean use_external_cloud = false;
	private long seq_number = 0;

	private static final Pattern pJobNumbers = Pattern.compile("^\\s*([12]+)\\s.*?(\\S+)");
	private static final Pattern pLoadBalancer = Pattern.compile("(\\d+)\\s*\\*\\s*(\\S+)");

	//
	// 2020-06-24 - Maarten Litmaath, Maxim Storetvedt
	//
	// to support weighted, round-robin load-balancing over a CE set:
	//

	private final ArrayList<String> ce_list = new ArrayList<>();
	private final HashMap<String, Double> ce_weight = new HashMap<>();
	private int next_ce = 0;
	private final HashMap<String, AtomicInteger> running = new HashMap<>();
	private final HashMap<String, AtomicInteger> waiting = new HashMap<>();
	private int tot_running = 0;
	private int tot_waiting = 0;
	private long job_numbers_timestamp = 0;
	private long proxy_check_timestamp = 0;

	//
	// our own Elvis operator approximation...
	//

	private static String if_else(final String value, final String fallback) {
		return value != null ? value : fallback;
	}

	/**
	 * @param conf
	 * @param logr
	 */
	@SuppressWarnings("unchecked")
	public HTCONDOR(final HashMap<String, Object> conf, final Logger logr) {
		System.out.println("This is HTCONDOR constructor");
		config = conf;
		System.out.println("config: " + config);
		logger = logr;

		System.out.println("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") +
				", site is " + config.get("site_accountname"));
		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") +
				", site is " + config.get("site_accountname"));

		final String ce_env_str = "ce_environment";

		if (config.get(ce_env_str) == null) {
			System.out.println("config.get(ce_env_str) is null");
			final String msg = ce_env_str + " needs to be defined!";
			logger.warning(msg);
			config.put(ce_env_str, new TreeSet<>());
		}

		try {
			envFromConfig = (TreeSet<String>) config.get(ce_env_str);
			System.out.println("CE envFromConfig: " + envFromConfig);
		} catch (@SuppressWarnings("unused") final ClassCastException e) {
			envFromConfig = new TreeSet<>(Arrays.asList((String) config.get(ce_env_str)));
		}

		//
		// initialize our environment from the LDAP configuration
		//
		System.out.println("Initializing our environment from the LDAP configuration");
		for (final String env_field : envFromConfig) {
			final String[] parts = env_field.split("=", 2);
			final String var = parts[0];
			final String val = parts.length > 1 ? parts[1] : "";
			environment.put(var, val);
			logger.info("envFromConfig: " + var + "=" + val);
		}
		System.out.println("environment: " + environment);

		//
		// allow the process environment to override any variable and add others
		//
		System.out
				.println("allow the process environment to override any variable and add others(Adds system variabes)");
		environment.putAll(System.getenv());
		System.out.println("environment: " + environment);
		proxy = environment.get("X509_USER_PROXY");
		System.out.println("proxy: " + proxy);
		final String ce_submit_cmd_str = "CE_SUBMITCMD";

		submitCmd = if_else(environment.get(ce_submit_cmd_str),
				if_else((String) config.get(ce_submit_cmd_str), "condor_submit"));
		System.out.println("submitCmd: " + submitCmd);
		String use_job_router_tmp = "0";
		String use_external_cloud_tmp = "0";

		for (final Map.Entry<String, String> entry : environment.entrySet()) {
			final String var = entry.getKey();
			final String val = entry.getValue();

			if ("CE_LCGCE".equals(var)) {
				System.out.println("Processing for CE_LGCE");
				double tot = 0;

				//
				// support weighted, round-robin load-balancing over a CE set
				// (mind: the WLCG SiteMon VO feed currently needs the ports):
				//
				// CE_LCGCE=[N1 * ]ce1.abc.xyz[:port], [N2 * ]ce2.abc.xyz[:port], ...
				//
				System.out.println("Load-balancing over these CEs with configured weights:");
				logger.info("Load-balancing over these CEs with configured weights:");

				for (final String str : val.split(",")) {
					double w = 1;
					String ce = str;
					final Matcher m = pLoadBalancer.matcher(str);

					if (m.find()) {
						w = Double.parseDouble(m.group(1));
						ce = m.group(2);
					} else {
						ce = ce.replaceAll("\\s+", "");
					}

					if (!Pattern.matches(".*\\w.*", ce)) {
						logger.severe("syntax error in CE_LCGCE");
						System.out.println("syntax error in CE_LCGCE");
						tot = 0;
						break;
					}

					if (!Pattern.matches(".*:.*", ce)) {
						ce += ":9619";
					}

					//
					// hack for job submission to a local pool
					//

					if (!Pattern.matches(".*\\..*", ce)) {
						ce = local_pool = "local_pool";
					}

					logger.info(ce + " --> " + String.format("%5.3f", Double.valueOf(w)));
					System.out.println(ce + " --> " + String.format("%5.3f", Double.valueOf(w)));

					ce_list.add(ce);
					ce_weight.put(ce, Double.valueOf(w));
					tot += w;
				}

				if (tot <= 0) {
					final String msg = "CE_LCGCE invalid: " + val;
					System.out.println(msg);
					logger.severe(msg);
					throw new IllegalArgumentException(msg);
				}

				if (ce_weight.size() != ce_list.size()) {
					final String msg = "CE_LCGCE has duplicate CEs: " + val;
					System.out.println(msg);
					logger.severe(msg);
					throw new IllegalArgumentException(msg);
				}

				logger.info("Load-balancing over these CEs with normalized weights:");
				System.out.println("Load-balancing over these CEs with normalized weights:");
				for (final String ce : ce_list) {
					final Double w = Double.valueOf(ce_weight.get(ce).doubleValue() / tot);
					ce_weight.replace(ce, w);
					logger.info(ce + " --> " + String.format("%5.3f", w));
					System.out.println(ce + " --> " + String.format("%5.3f", w));
				}

				continue;
			}

			if ("SUBMIT_ARGS".equals(var)) {
				System.out.println("Processing for SUBMIT_ARGS");
				submitArgs = val;
				logger.info("environment: " + var + "=" + val);
				System.out.println("submitArgs: " + submitArgs);
				continue;
			}

			if ("HTCONDOR_LOG_PATH".equals(var)) {
				System.out.println("Processing for HTCONDOR_LOG_PATH");
				htc_logdir = val;
				System.out.println("htc_logdir: " + htc_logdir);
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("GRID_RESOURCE".equals(var)) {
				System.out.println("Processing for GRID_RESOURCE");
				grid_resource = val;
				System.out.println("grid_resource: " + grid_resource);
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("TOKEN_FILE".equals(var)) {
				System.out.println("Processing for TOKEN_FILE");
				token_file = val;
				System.out.println("token_file: " + token_file);
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("USE_TOKEN".equals(var)) {
				System.out.println("Processing for USE_TOKEN");
				use_token = Integer.parseInt(val);
				System.out.println("use_token: " + use_token);
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("USE_JOB_ROUTER".equals(var)) {
				System.out.println("Processing for USE_JOB_ROUTER");
				use_job_router_tmp = val;
				System.out.println("use_job_router_tmp: " + use_job_router_tmp);
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("USE_EXTERNAL_CLOUD".equals(var)) {
				System.out.println("Processing for USE_EXTERNAL_CLOUD");
				use_external_cloud_tmp = val;
				System.out.println("use_external_cloud_tmp: " + use_external_cloud_tmp);
				logger.info("environment: " + var + "=" + val);
				continue;
			}
		}

		htc_logdir = Functions.resolvePathWithEnv(htc_logdir);
		System.out.println("htc_logdir: " + htc_logdir);
		logger.info("htc_logdir: " + htc_logdir);

		use_job_router = Integer.parseInt(use_job_router_tmp) == 1;
		System.out.println("use_job_router: " + use_job_router);
		use_external_cloud = Integer.parseInt(use_external_cloud_tmp) == 1;
		System.out.println("use_external_cloud: " + use_external_cloud);

		if (ce_list.size() <= 0 && grid_resource == null && !use_job_router) {
			final String msg = "No CE usage specified in the environment";
			System.out.println(msg);
			logger.severe(msg);
			throw new IllegalArgumentException(msg);
		}
	}

	private void proxyCheck() {
		System.out.println("This is HTCONDOR proxyCheck");
		final File proxy_no_check = new File(environment.get("HOME") + "/no-proxy-check");

		if (proxy == null || proxy_no_check.exists()) {
			return;
		}

		final String vo_str = if_else((String) config.get("LCGVO"), "alice");
		final String proxy_renewal_str = String.format("/etc/init.d/%s-box-proxyrenewal", vo_str);
		final File proxy_renewal_svc = new File(proxy_renewal_str);

		if (!proxy_renewal_svc.exists()) {
			return;
		}

		final String threshold = if_else((String) config.get("CE_PROXYTHRESHOLD"), String.valueOf(46 * 3600));
		System.out.println("threshold: " + threshold);
		logger.info(String.format("X509_USER_PROXY is %s", proxy));
		logger.info("Checking remaining proxy lifetime");

		final String proxy_info_cmd = "voms-proxy-info -acsubject -actimeleft 2>&1";
		System.out.println("Executing proxy info command");
		ExitStatus exitStatus = executeCommand(proxy_info_cmd);
		final List<String> proxy_info_output = getStdOut(exitStatus);
		System.out.println("proxy_info_output: " + proxy_info_output);
		String dn_str = "";
		String time_left_str = "";

		for (final String line : proxy_info_output) {
			final String trimmed_line = line.trim();

			if (trimmed_line.matches("^/.+")) {
				dn_str = trimmed_line;
				continue;
			}

			if (trimmed_line.matches("^\\d+$")) {
				time_left_str = trimmed_line;
				continue;
			}
		}

		if (dn_str.length() == 0) {
			logger.warning("[LCG] No valid VOMS proxy found!");
			System.out.println("[LCG] No valid VOMS proxy found!");
			return;
		}

		logger.info(String.format("DN is %s", dn_str));
		logger.info(String.format("Proxy timeleft is %s (threshold is %s)", time_left_str, threshold));
		System.out.println(String.format("DN is %s", dn_str));
		System.out.println(String.format("Proxy timeleft is %s (threshold is %s)", time_left_str, threshold));

		if (Integer.parseInt(time_left_str) > Integer.parseInt(threshold)) {
			return;
		}

		//
		// the proxy shall be managed by the proxy renewal service for the VO;
		// restart it as needed...
		//

		logger.info("Checking proxy renewal service");
		System.out.println("Checking proxy renewal service");
		final String proxy_renewal_cmd = String.format("%s start 2>&1", proxy_renewal_svc);
		List<String> proxy_renewal_output = null;

		try {
			System.out.println("Executing proxy renewal command");
			exitStatus = executeCommand(proxy_renewal_cmd);
			proxy_renewal_output = getStdOut(exitStatus);
			System.out.println("proxy_renewal_output: " + proxy_renewal_output);
		} catch (final Exception e) {
			logger.info(String.format("[LCG] Problem while executing command: %s", proxy_renewal_cmd));
			e.printStackTrace();
		} finally {
			if (proxy_renewal_output != null) {
				logger.info("Proxy renewal output:\n");

				for (final String line : proxy_renewal_output) {
					logger.info(line.trim());
				}
			}
		}
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit HTCONDOR");
		System.out.println("This is Submit HTCONDOR");

		final DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		System.out.println("Date Format initialized");
		final String current_date_str = date_format.format(new Date());
		System.out.println("Current Date String: " + current_date_str);
		final String log_folder_path = htc_logdir + "/" + current_date_str;
		System.out.println("Log Folder Path: " + log_folder_path);
		final File log_folder = new File(log_folder_path);
		System.out.println("Log Folder created");

		if (!log_folder.exists()) {
			System.out.println("Log folder does not exist. Creating...");

			try {
				log_folder.mkdirs();
				System.out.println("Log folder created successfully");
			} catch (final Exception e) {
				System.out.println("Exception during log folder creation:");
				e.printStackTrace();
				logger.severe(String.format("[HTCONDOR] log folder mkdirs() exception: %s", log_folder_path));
			}

			if (!log_folder.exists()) {
				System.out.println("Log folder still does not exist. Couldn't create log folder.");
				logger.severe(String.format("[HTCONDOR] Couldn't create log folder: %s", log_folder_path));
				return;
			}
		}

		final String file_base_name = String.format("%s/jobagent_%d_%d", log_folder_path,
				Long.valueOf(ProcessHandle.current().pid()), Long.valueOf(seq_number++));
		System.out.println("File Base Name: " + file_base_name);

		final String log_cmd = String.format("log = %s.log%n", file_base_name);
		System.out.println("Log Command: " + log_cmd);

		String out_cmd = "";
		String err_cmd = "";

		final File enable_sandbox_file = new File(environment.get("HOME") + "/enable-sandbox");

		if (enable_sandbox_file.exists()) {
			System.out.println("Enable sandbox file exists. Configuring output and error commands...");

			out_cmd = String.format("output = %s.out%n", file_base_name);
			System.out.println("Out Command: " + out_cmd);

			err_cmd = String.format("error = %s.err%n", file_base_name);
			System.out.println("Error Command: " + err_cmd);
		}

		String per_hold_grid = (local_pool != null) ? ""
				: "(JobStatus == 1 && GridJobStatus =?= undefined && CurrentTime - EnteredCurrentStatus > 1800) || ";
		System.out.println("Per Hold Grid: " + per_hold_grid);

		String submit_jdl = "cmd = " + script + "\n" +
				out_cmd +
				err_cmd +
				log_cmd +
				"+TransferOutput = \"\"\n" +
				"periodic_hold = " + per_hold_grid +
				"(JobStatus <= 2 && CurrentTime - EnteredCurrentStatus > 172800)\n" +
				"periodic_remove = CurrentTime - QDate > 259200\n";
		System.out.println("Submit JDL: " + submit_jdl);

		//
		// via our own load-balancing (preferred), via the JobRouter, or to the single
		// CE
		//
		System.out.println("Determine next CE to use, using a load balancing mechanism");
		if (!use_job_router && ce_list.size() > 0) {
			logger.info("Determining the next CE to use:");

			for (int i = 0; i < ce_list.size(); i++) {
				final String ce = ce_list.get(next_ce);
				final AtomicInteger idle = waiting.computeIfAbsent(ce, (r) -> new AtomicInteger(0));
				final Double w = ce_weight.get(ce);
				final Double f = Double.valueOf(tot_waiting > 0 ? idle.doubleValue() / tot_waiting : 0);

				logger.info(String.format(
						"--> %s has idle fraction %d / %d = %5.3f vs. weight %5.3f",
						ce, Integer.valueOf(idle.intValue()), Integer.valueOf(tot_waiting), f, w));

				if (f.doubleValue() < w.doubleValue()) {
					break;
				}

				next_ce++;
				next_ce %= ce_list.size();
			}

			final String ce = ce_list.get(next_ce);

			logger.info("--> next CE to use: " + ce);

			waiting.computeIfAbsent(ce, (r) -> new AtomicInteger(0)).incrementAndGet();
			tot_waiting++;

			next_ce++;
			next_ce %= ce_list.size();

			final String h = ce.replaceAll(":.*", "");
			grid_resource = "condor " + h + " " + ce;
		}

		if (local_pool != null || use_job_router) {
			submit_jdl += "universe = vanilla\n" +
					"job_lease_duration = 7200\n" +
					"ShouldTransferFiles = YES\n";
			if (use_job_router) {
				submit_jdl += "+WantJobRouter = True\n";
			}
		} else {
			submit_jdl += "universe = grid\n" +
					"grid_resource = " + grid_resource + "\n";
		}

		if (use_external_cloud) {
			submit_jdl += "+WantExternalCloud = True\n";
		}

		if (use_token > 0) {
			submit_jdl += "use_scitokens = true\n";
			submit_jdl += "scitokens_file = " + token_file + "\n";

			if (use_token > 1 && proxy != null) {
				submit_jdl += "x509userproxy = " + proxy + "\n";
			}
		} else {
			submit_jdl += "use_x509userproxy = true\n";
		}

		System.out.println("host_host: " + config.get("host_host"));
		System.out.println("host_port: " + config.get("host_port"));

		final String cm = config.get("host_host") + ":" + config.get("host_port");
		final String env_cmd = String.format("ALIEN_CM_AS_LDAP_PROXY='%s'", cm);
		submit_jdl += String.format("environment = \"%s\"%n", env_cmd);

		//
		// allow preceding attributes to be overridden and others added if needed
		//

		final String custom_jdl_path = environment.get("HOME") + "/custom-classad.jdl";

		if ((new File(custom_jdl_path)).exists()) {
			String custom_attr_str = "\n#\n# custom attributes start\n#\n\n";
			custom_attr_str += readJdlFile(custom_jdl_path);
			custom_attr_str += "\n#\n# custom attributes end\n#\n\n";
			submit_jdl += custom_attr_str;
			logger.info("Custom attributes added from file: " + custom_jdl_path);
		}

		//
		// finally
		//

		submit_jdl += "queue 1\n";

		//
		// keep overwriting the same file for ~1 minute
		//

		final String submit_file = log_folder_path + "/htc-submit." + (job_numbers_timestamp >> 16) + ".jdl";
		System.out.println("Submit File: " + submit_file);
		try (PrintWriter out = new PrintWriter(submit_file)) {
			System.out.println("SUbmit jdl : " + submit_jdl);
			out.println(submit_jdl);
		} catch (final Exception e) {
			System.out.println("Error writing to submit file: " + submit_file);
			logger.severe("Error writing to submit file: " + submit_file);
			e.printStackTrace();
			return;
		}

		final String submit_cmd = submitCmd + " " + submitArgs + " " + submit_file;
		System.out.println("Submit Command: " + submit_cmd);
		final ExitStatus exitStatus = executeCommand(submit_cmd);
		System.out.println("Exit Status: " + exitStatus);
		final List<String> output = getStdOut(exitStatus);
		System.out.println("Output: " + output);

		for (final String line : output) {
			final String trimmed_line = line.trim();
			logger.info(trimmed_line);
		}
	}

	private String readJdlFile(final String path) {
		final StringBuilder file_contents = new StringBuilder();
		String line;

		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			final Pattern comment_pattern = Pattern.compile("^\\s*(#.*|//.*)?$");
			final Pattern err_spaces_pattern = Pattern.compile("\\\\\\s*$");

			while ((line = br.readLine()) != null) {
				final Matcher comment_matcher = comment_pattern.matcher(line);
				// skip over comment lines

				if (comment_matcher.matches()) {
					continue;
				}

				// remove erroneous spaces
				line = line.replaceAll(err_spaces_pattern.pattern(), "\\\\\n");
				file_contents.append(line).append('\n');
			}
		} catch (final Exception e) {
			logger.severe("ERROR when reading JDL file: " + path);
			e.printStackTrace();
			return "";
		}

		return file_contents.toString();
	}

	private boolean getJobNumbers() {
		System.out.println("This is HTCONDOR getJobNumbers");
		final long now = System.currentTimeMillis();
		final long dt = (now - job_numbers_timestamp) / 1000;

		if (dt < 60) {
			System.out.println("Reusing cached job numbers collected " + dt + " seconds ago");
			logger.info("Reusing cached job numbers collected " + dt + " seconds ago");
			return true;
		}

		//
		// take advantage of this regular call to check how the proxy is doing as well
		//

		if ((now - proxy_check_timestamp) / 1000 > 3600) {
			System.out.println("Checking proxy");
			proxyCheck();
			proxy_check_timestamp = now;
		}

		//
		// reset the numbers...
		//

		tot_running = 0;
		tot_waiting = 444444; // the traditional safe default...

		// in case the CE list has changed
		running.clear();
		waiting.clear();

		for (final String ce : ce_list) {
			waiting.put(ce, new AtomicInteger(0));
			running.put(ce, new AtomicInteger(0));
		}

		//
		// hack for job submission to a local pool
		//

		final String fmt = (local_pool != null) ? " -format " + local_pool : "";
		final String cmd = "condor_q -const 'JobStatus < 3' -af JobStatus" +
				fmt + " GridResource";
		ExitStatus exitStatus = null;

		try {
			System.out.println("Executing this condor comand : " + cmd);
			exitStatus = executeCommand(cmd);
		} catch (final Exception e) {
			System.out.println("Exception while executing command: " + cmd + " : " + e);
			logger.warning(String.format("[LCG] Exception while executing command: %s", cmd));
			e.printStackTrace();
			return false;
		}

		if (exitStatus == null) {
			System.out.println("Null result for command: " + cmd);
			logger.warning(String.format("[LCG] Null result for command: %s", cmd));
			return false;
		}

		final List<String> job_list = getStdOut(exitStatus);
		System.out.println("Job List: " + job_list);

		if (exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL) {
			logger.warning(String.format("[LCG] Abnormal exit status for command: %s", cmd));
			System.out.println("Warning : Abnormal exit status for command: " + cmd);
			int i = 1;

			for (final String line : job_list) {
				logger.warning(String.format("[LCG] Line %2d: %s", Integer.valueOf(i), line));
				System.out.println("Warning : Line " + i + " : " + line);
				if (i++ > 10) {
					logger.warning("[LCG] [...]");
					break;
				}
			}

			return false;
		}

		tot_waiting = 0; // start calculating the real number...

		for (final String line : job_list) {
			final Matcher m = pJobNumbers.matcher(line);

			if (m.matches()) {
				final int job_status = Integer.parseInt(m.group(1));
				final String ce = m.group(2);

				if (job_status == 1) {
					final AtomicInteger w = waiting.get(ce);

					if (w != null)
						w.incrementAndGet();

					tot_waiting++;
				} else if (job_status == 2) {
					final AtomicInteger r = running.get(ce);

					if (r != null)
						r.incrementAndGet();

					tot_running++;
				}
			}
		}
		System.out.println("Found " + tot_waiting + " idle (and " + tot_running + " running) jobs:");
		logger.info("Found " + tot_waiting + " idle (and " + tot_running + " running) jobs:");

		for (final String ce : ce_list) {
			logger.info(String.format("%5d (%5d) for %s", Integer.valueOf(waiting.get(ce).intValue()),
					Integer.valueOf(running.get(ce).intValue()), ce));
			System.out.println(String.format("%5d (%5d) for %s", Integer.valueOf(waiting.get(ce).intValue()),
					Integer.valueOf(running.get(ce).intValue()), ce));
		}

		job_numbers_timestamp = now;
		return true;
	}

	@Override
	public int getNumberActive() {
		System.out.println("This is HTCONDOR getNumberActive");
		if (!getJobNumbers()) {
			return -1;
		}

		return tot_running + tot_waiting;
	}

	@Override
	public int getNumberQueued() {
		System.out.println("This is HTCONDOR getNumberQueued");
		if (!getJobNumbers()) {
			return -1;
		}

		return tot_waiting;
	}

	@Override
	public int kill() {
		System.out.println("This is HTCONDOR kill");
		System.out.println("Kill command not implemented");
		logger.info("Kill command not implemented");
		return 0;
	}
}
