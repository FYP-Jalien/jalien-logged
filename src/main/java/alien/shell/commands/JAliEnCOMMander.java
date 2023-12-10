package alien.shell.commands;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.JBoxServer;
import alien.api.Request;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFN_CSD;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.log.RequestEvent;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.ErrNo;
import alien.shell.FileEditor;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.UsersHelper;
import joptsimple.OptionException;
import lazyj.Format;
import lazyj.LRUMap;
import utils.CachedThreadPool;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCOMMander implements Runnable {

	/**
	 * Thread pool size monitoring
	 */
	private static final Monitor monitor = MonitorFactory.getMonitor(JAliEnCOMMander.class.getCanonicalName());

	private static CachedThreadPool COMMAND_EXECUTOR = new CachedThreadPool(
			ConfigUtils.getConfig().geti(JAliEnCOMMander.class.getCanonicalName() + ".executorSize", 200), 15,
			TimeUnit.SECONDS,
			(r) -> new Thread(r, "CommandExecutor"));

	static {
		if (monitor != null)
			monitor.addMonitoring("thread_pool_status", (names, values) -> {
				names.add("active_threads");
				values.add(Double.valueOf(COMMAND_EXECUTOR.getActiveCount()));

				names.add("allocated_threads");
				values.add(Double.valueOf(COMMAND_EXECUTOR.getPoolSize()));

				names.add("max_threads");
				values.add(Double.valueOf(COMMAND_EXECUTOR.getMaximumPoolSize()));
			});
	}

	/**
	 * Atomic status update of the command execution
	 *
	 * @author costing
	 * @since 2018-09-11
	 */
	public static class CommanderStatus {
		private int status = 0;

		/**
		 * @return current value
		 */
		public synchronized int get() {
			return status;
		}

		/**
		 * Set the new status code
		 *
		 * @param newValue
		 * @return the old value
		 */
		public synchronized int set(final int newValue) {
			final int oldValue = status;

			status = newValue;

			return oldValue;
		}
	}

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	/**
	 *
	 */
	public final CatalogueApiUtils c_api;

	/**
	 *
	 */
	public final TaskQueueApiUtils q_api;

	/**
	 * The commands that have a JAliEnCommand* implementation
	 */
	private static final String[] jAliEnCommandList = new String[] {
			"cd", "pwd", "mkdir", "rmdir",
			"ls", "find", "toXml", "cat", "whereis", "cp", "rm", "mv", "touch", "type", "lfn2guid", "guid2lfn",
			"guidinfo", "access", "commit", "chown", "chmod", "deleteMirror", "md5sum", "mirror",
			"grep",
			"changeDiff",
			"listFilesFromCollection", "archiveList",
			"packages",
			"listCEs", "jobListMatch", "listpartitions", "setCEstatus",
			"submit", "ps", "masterjob", "kill", "w", "uptime", "resubmit", "top", "registerOutput",
			"df", "du", "fquota", "jquota",
			"listSEs", "listSEDistance", "setSite", "testSE", "listTransfer", "uuid", "stat", "xrdstat", "randomPFNs",
			"resyncLDAP", "optimiserLogs",
			"showTagValue",
			"time", "timing", "commandlist", "motd", "ping", "version", "echo",
			"whoami", "user", "whois", "groups", "token",
			"lfnexpiretime"
	};

	private static final String[] jAliEnAdminCommandList = new String[] { "groupmembers", "moveDirectory" };

	/**
	 * The commands that are advertised on the shell, e.g. by tab+tab
	 */
	private static final String[] commandList;

	private final Set<String> userAvailableCommands = new LinkedHashSet<>();

	private static final AtomicLong commanderIDSequence = new AtomicLong();

	/**
	 * Unique identifier of the commander
	 */
	public final long commanderId = commanderIDSequence.incrementAndGet();

	private final UUID clientId = UUID.randomUUID();

	static {
		final List<String> comm_set = new ArrayList<>(Arrays.asList(jAliEnCommandList));
		final List<String> comms = comm_set;
		comms.add("shutdown");

		comms.addAll(FileEditor.getAvailableEditorCommands());

		commandList = comms.toArray(new String[comms.size()]);
	}

	/**
	 * Commands to let UI talk internally with us here
	 */
	private static final String[] hiddenCommandList = new String[] { "roleami", "cdir", "gfilecomplete", "cdirtiled",
			"blackwhite", "color", "setshell", "randomPFNs" };

	private UIPrintWriter out = null;

	/**
	 * marker for -Colour argument
	 */
	protected boolean bColour;

	/**
	 *
	 */
	protected AliEnPrincipal user;

	/**
	 *
	 */
	protected String site;

	private final String myHome;

	private final HashMap<String, File> localFileCash;

	private static JAliEnCOMMander lastInstance = null;

	private long commandCount = 0;

	private boolean returnTiming = false;

	private String certificateSubject = "unknown";

	private boolean shouldRateLimit = true;

	/**
	 * @return a commander instance
	 */
	public static synchronized JAliEnCOMMander getInstance() {
		if (lastInstance == null)
			lastInstance = new JAliEnCOMMander();

		return lastInstance;
	}

	/**
	 */
	public JAliEnCOMMander() {
		this(null, null, null, null);
	}

	private static void setName(final String threadName) {
		Thread.currentThread().setName(threadName);
	}

	/**
	 * @param user
	 * @param curDir
	 * @param site
	 * @param out
	 */
	public JAliEnCOMMander(final AliEnPrincipal user, final LFN curDir, final String site, final UIPrintWriter out) {
		this(user, curDir, site, out, null);
	}

	/**
	 * @param user
	 * @param curDir
	 * @param site
	 * @param out
	 * @param userProperties
	 */
	public JAliEnCOMMander(final AliEnPrincipal user, final LFN curDir, final String site, final UIPrintWriter out,
			final Map<String, Object> userProperties) {
		System.out.println("Initializing JAliEnCOMMander");

		c_api = new CatalogueApiUtils(this);
		q_api = new TaskQueueApiUtils(this);

		this.user = (user != null) ? user : AuthorizationFactory.getDefaultUser();
		System.out.println("User: " + this.user);

		this.site = (site != null) ? site : ConfigUtils.getCloseSite();
		System.out.println("Site: " + this.site);

		localFileCash = new HashMap<>();
		this.out = out;
		System.out.println("out: " + this.out);
		this.bColour = out != null ? out.colour() : false;
		System.out.println("bColour: " + this.bColour);
		if (this.user.isJobAgent()) {
			// For job agents, we do not care about directories
			System.out.println("User is a job agent. Setting current directory to " + curDir);
			myHome = "";
			this.curDir = curDir;
		} else {
			// User directories must be set correctly
			System.out.println("User is not a job agent. Setting current directory to user's home directory: ");
			myHome = UsersHelper.getHomeDir(this.user.getName());
			System.out.println("Home directory: " + myHome);
			if (curDir == null) {
				System.out.println("Current Dir null, use getLFN to get curDir");
				try {
					this.curDir = c_api.getLFN(myHome);
					System.out.println("Current directory set to user's home directory: " + myHome);
				} catch (final Exception e) {
					System.out.println("Exception initializing connection: " + e.getMessage());
					logger.log(Level.WARNING, "Exception initializing connection", e);
				}
			} else {
				this.curDir = curDir;
				System.out.println("curDIr is not null So Current directory set to provided directory: " + curDir);
			}
		}

		Collections.addAll(userAvailableCommands, jAliEnCommandList);
		System.out.println("Added user commands to available commands.");
		System.out.println("User Aaiable comamnds");
		System.out.println(userAvailableCommands);
		System.out.println("jAliEnAdminCommandList");
		System.out.println(jAliEnAdminCommandList);
		if (this.user.canBecome("admin")) {
			Collections.addAll(userAvailableCommands, jAliEnAdminCommandList);
			System.out.println("User can become admin. Added admin commands to available commands.");
		}

		for (final String s : hiddenCommandList) {
			System.out.println("Removing hidden commands from available commands. - " + s);
			userAvailableCommands.remove(s);
		}
		System.out.println("Removed hidden commands from available commands.");
		bootMessage(userProperties);
		System.out.println("bootMessage called. : " + userProperties);
		System.out.println("JAliEnCOMMander initialized.");
	}

	/**
	 * @param md5
	 * @param localFile
	 */
	protected void cashFile(final String md5, final File localFile) {
		localFileCash.put(md5, localFile);
	}

	/**
	 * @param md5
	 * @return local file name
	 */
	protected File checkLocalFileCache(final String md5) {
		if (md5 != null && localFileCash.containsKey(md5))
			return localFileCash.get(md5);
		return null;
	}

	/**
	 * Debug level as the status
	 */
	protected int debug = 0;

	/**
	 * If <code>true</code> remove stdout message from the command output
	 */
	protected boolean nomsg = false;

	/**
	 * If <code>true</code> remove key-values from the command output
	 */
	protected boolean nokeys = false;

	/**
	 * Current directory as the status
	 */
	protected LFN curDir;

	/**
	 * Current directory as the status
	 */
	protected LFN_CSD curDirCsd;

	/**
	 * get list of commands
	 *
	 * @return array of commands
	 */
	public static String getCommandList() {
		return getCommandList(null);
	}

	/**
	 * get list of commands
	 * 
	 * @param user
	 *
	 * @return array of commands
	 */
	public static String getCommandList(final AliEnPrincipal user) {
		final StringBuilder commands = new StringBuilder();

		for (int i = 0; i < commandList.length; i++) {
			if (i > 0)
				commands.append(' ');

			commands.append(commandList[i]);
		}

		if (user != null && user.hasRole("admin"))
			for (int i = 0; i < jAliEnAdminCommandList.length; i++) {
				if (i > 0)
					commands.append(' ');

				commands.append(jAliEnAdminCommandList[i]);
			}

		return commands.toString();
	}

	/**
	 * Get the user
	 *
	 * @return user
	 */
	public AliEnPrincipal getUser() {
		return user;
	}

	/**
	 * Get the site
	 *
	 * @return site
	 */
	public String getSite() {
		return site;
	}

	/**
	 * @param newSiteName site name to set the affinity to
	 */
	void setSite(final String newSiteName) {
		site = newSiteName;
	}

	/**
	 * get the user's name
	 *
	 * @return user name
	 */
	public String getUsername() {
		return user.getName();
	}

	/**
	 * get the current directory
	 *
	 * @return LFN of the current directory
	 */
	public LFN getCurrentDir() {
		return curDir;
	}

	/**
	 * get the current directory
	 *
	 * @return LFNCSD of the current directory
	 */
	public LFN_CSD getCurrentDirCsd() {
		return curDirCsd;
	}

	/**
	 * get the current directory as string
	 *
	 * @return String of the current directory
	 */
	public String getCurrentDirName() {
		if (getCurrentDir() != null)
			return getCurrentDir().getCanonicalName();
		return "[none]";
	}

	/**
	 * get the current directory, replace home with ~
	 *
	 * @return name of the current directory, ~ places home
	 */
	public String getCurrentDirTilded() {

		return getCurrentDir().getCanonicalName().substring(0, getCurrentDir().getCanonicalName().length() - 1)
				.replace(myHome.substring(0, myHome.length() - 1), "~");
	}

	private String[] arg = null;

	private JAliEnBaseCommand jcommand = null;

	/**
	 * Current status : 0 = idle, 1 = busy executing a command
	 */
	public CommanderStatus status = new CommanderStatus();

	/**
	 * Set this variable to finish commander's execution
	 */
	public volatile boolean kill = false;

	private static File accessLogFile = null;

	private static OutputStream accessLogStream = null;

	private static synchronized OutputStream getAccessLogTarget() {
		if (accessLogFile != null && !accessLogFile.exists()) {
			// log file was rotated, close the old handle and create a new one
			if (accessLogStream != null) {
				try {
					accessLogStream.close();
				} catch (@SuppressWarnings("unused") final IOException e) {
					// ignore
				}
				accessLogStream = null;
			}

			accessLogFile = null;
		}

		if (accessLogStream == null) {
			final String accessLogFileName = ConfigUtils.getConfig().gets("alien.shell.commands.access_log");

			if (accessLogFileName.length() > 0) {
				try {
					accessLogFile = new File(accessLogFileName);
					accessLogStream = new FileOutputStream(accessLogFile, true);
				} catch (final IOException ioe) {
					logger.log(Level.WARNING,
							"Cannot write to access log " + accessLogFileName + ", will write to stderr instead", ioe);
					accessLogFile = null;
				}
			}

			if (accessLogStream == null)
				accessLogStream = System.err;

			try (RequestEvent event = new RequestEvent(accessLogStream)) {
				event.command = "boot";
				event.identity = AuthorizationFactory.getDefaultUser();
				event.clientID = Request.getVMID();

				event.arguments = new ArrayList<>();

				if (JAKeyStore.getKeyStore().getCertificateChain("User.cert") != null)
					for (final Certificate cert : JAKeyStore.getKeyStore().getCertificateChain("User.cert")) {
						final X509Certificate x509cert = (java.security.cert.X509Certificate) cert;
						event.arguments.add(x509cert.getSubjectX500Principal().getName() + " (expires "
								+ x509cert.getNotAfter() + ")");
					}
				else
					event.errorMessage = "Local identity doesn't have a certificate chain associated";
			} catch (@SuppressWarnings("unused") IOException | KeyStoreException e) {
				// ignore exception in logging the startup message
			}
		}

		return accessLogStream;
	}

	private void notifyExecutionEnd() {
		status.set(0);
		synchronized (status) {
			status.notifyAll();
		}
	}

	@Override
	public void run() {
		if (shouldRateLimit) {
			final int counter = incrementCommandCount(certificateSubject, 1);

			final int throttleTime = getThrottleTime(counter);

			if (throttleTime > 0) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Sleeping " + throttleTime + " for " + certificateSubject
							+ " having executed " + counter + " commands");

				try {
					Thread.sleep(throttleTime);
				} catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}
			}
		}

		try (RequestEvent event = new RequestEvent(getAccessLogTarget())) {
			event.identity = getUser();
			event.site = getSite();
			event.serverThreadID = Long.valueOf(commanderId);
			event.requestId = Long.valueOf(++commandCount);
			event.clientID = clientId;

			try {
				setName("Commander " + commanderId + ": Executing: " + Arrays.toString(arg));

				execute(event);
			} catch (final Exception e) {
				event.exception = e;

				logger.log(Level.WARNING, "Got exception", e);
			}
		} catch (final Exception e) {
			logger.log(Level.WARNING, "Got exception", e);
		} finally {
			out = null;

			setName("Commander " + commanderId + ": Idle");

			notifyExecutionEnd();
		}
	}

	/**
	 *
	 */
	private void bootMessage(final Map<String, Object> userProperties) {
		logger.log(Level.FINE, "Starting Commander");

		try (RequestEvent event = new RequestEvent(getAccessLogTarget())) {
			event.command = "login";
			event.identity = getUser();
			event.serverThreadID = Long.valueOf(commanderId);
			event.clientID = clientId;

			if (event.identity != null && event.identity.getUserCert() != null) {
				final ArrayList<String> certificates = new ArrayList<>();

				for (final X509Certificate cert : event.identity.getUserCert()) {
					final String subject = cert.getSubjectX500Principal().getName();

					if (certificateSubject.length() < 10)
						certificateSubject = subject;

					certificates.add(subject + " (expires " + cert.getNotAfter() + ")");
				}

				event.arguments = certificates;
				event.userProperties = userProperties;

				if (certificateSubject.startsWith("OU=alitrain,") || certificateSubject.startsWith("OU=alihyperloop,"))
					shouldRateLimit = false;
				else
					incrementCommandCount(certificateSubject, 10);
			}
		} catch (@SuppressWarnings("unused") final IOException ioe) {
			// ignore any exception in writing out the event
		}
	}

	private void closeMessage() {
		try (RequestEvent event = new RequestEvent(getAccessLogTarget())) {
			event.command = "logout";
			event.identity = getUser();
			event.site = getSite();
			event.serverThreadID = Long.valueOf(commanderId);
			event.requestId = Long.valueOf(commandCount);
			event.clientID = clientId;

			incrementCommandCount(certificateSubject, 1);
		} catch (@SuppressWarnings("unused") final IOException ioe) {
			// ignore any exception in writing out the event
		}
	}

	/**
	 * Discard any currently running command and stop accepting others
	 */
	public void shutdown() {
		setLine(null, null);
		kill = true;
		closeMessage();
	}

	/**
	 * @param out
	 * @param arg
	 */
	public void setLine(final UIPrintWriter out, final String[] arg) {
		if (kill) {
			notifyExecutionEnd();
			return;
		}

		clearLastError();

		this.out = out;
		this.arg = arg;

		if (this.out != null && this.arg != null) {
			bColour = out.colour();
			status.set(1);
			COMMAND_EXECUTOR.submit(this);
		} else
			notifyExecutionEnd();
	}

	/**
	 * execute a command line
	 *
	 * @param event the logging for this event
	 *
	 * @throws Exception
	 *
	 */
	public void execute(final RequestEvent event) throws Exception {
		boolean help = false;
		nomsg = false;
		nokeys = false;

		if (arg == null || arg.length == 0) {
			event.errorMessage = "No command to execute";
			// flush();
			return;
		}

		final ArrayList<String> args = new ArrayList<>(Arrays.asList(arg));

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, "Received JSh call " + args);

		final String comm = args.remove(0);

		event.command = comm;
		event.arguments = new ArrayList<>(args);

		// Set default return code and error message
		if (out != null)
			out.setReturnCode(0, "");

		for (int i = 1; i < arg.length; i++)
			if (arg[i].startsWith("-pwd=")) {
				curDir = c_api.getLFN(arg[i].substring(arg[i].indexOf('=') + 1));
				args.remove(arg[i]);
			} else if (arg[i].startsWith("-debug=")) {
				try {
					debug = Integer.parseInt(arg[i].substring(arg[i].indexOf('=') + 1));
				} catch (@SuppressWarnings("unused") final NumberFormatException n) {
					// ignore
				}
				args.remove(arg[i]);
			} else if ("-silent".equals(arg[i])) {
				jcommand.silent();
				args.remove(arg[i]);
			} else if ("-nomsg".equals(arg[i])) {
				nomsg = true;
				args.remove(arg[i]);
			} else if ("-nokeys".equals(arg[i])) {
				nokeys = true;
				args.remove(arg[i]);
			} else if ("-h".equals(arg[i]) || "--h".equals(arg[i]) || "-help".equals(arg[i])
					|| "--help".equals(arg[i])) {
				help = true;
				args.remove(arg[i]);
			}

		if (!Arrays.asList(jAliEnCommandList).contains(comm) &&
		// ( AliEnPrincipal.roleIsAdmin( AliEnPrincipal.userRole()) &&
				!Arrays.asList(jAliEnAdminCommandList).contains(comm) /* ) */) {
			if (Arrays.asList(hiddenCommandList).contains(comm)) {
				if ("blackwhite".equals(comm) && out != null) {
					out.blackwhitemode();
					bColour = out.colour();
				} else if ("color".equals(comm) && out != null) {
					out.colourmode();
					bColour = out.colour();
				}
				// else if ("shutdown".equals(comm))
				// jbox.shutdown();
				// } else if (!"setshell".equals(comm)) {
			} else {
				event.errorMessage = "Command [" + comm + "] not found!";

				if (out != null)
					out.setReturnCode(ErrNo.ENOENT, event.errorMessage);
			}
			// }
		} else {

			final Object[] param = { this, args };

			try {
				final UIPrintWriter savedOut = out;

				if (help)
					out = null;

				try {
					jcommand = getCommand(comm, param);
				} finally {
					out = savedOut;
				}
			} catch (final Exception e) {
				if (e.getCause() instanceof OptionException || e.getCause() instanceof NumberFormatException) {
					event.errorMessage = "Illegal command options";

					if (out != null)
						out.setReturnCode(ErrNo.EINVAL, event.errorMessage);
				} else {
					event.exception = e;

					e.printStackTrace();
					if (out != null)
						out.setReturnCode(ErrNo.EREMOTEIO,
								"Error executing command [" + comm + "] : \n" + Format.stackTraceToString(e));
				}

				if (out != null)
					out.flush();

				return;
			}

			try {
				if (jcommand == null) {
					if (out != null)
						out.setReturnCode(ErrNo.ENOENT.getErrorCode(), "Command not found or not implemented yet");
				} else {
					if (help) {
						// Force enable stdout message
						nomsg = false;
						jcommand.printHelp();
						setReturnCode(0, "");
					} else if (jcommand.areArgumentsOk() && (args.size() != 0 || jcommand.canRunWithoutArguments())) {
						jcommand.run();
					} else {
						if (out != null && out.getReturnCode() == 0) {
							// if non zero then an error message was already printed to the client, don't
							// bloat the output with a generic help
							out.setReturnCode(ErrNo.EINVAL, "Command requires an argument");
							jcommand.printHelp();
						}
					}
				}
			} catch (final Exception e) {
				event.exception = e;
				e.printStackTrace();

				if (out != null)
					out.setReturnCode(ErrNo.EREMOTEIO,
							"Error executing the command [" + comm + "]: \n" + Format.stackTraceToString(e));
			}
		}

		if (out != null) {
			if (returnTiming)
				out.setMetaInfo("timing_ms", String.valueOf(event.timing.getMillis()));
			else
				out.setMetaInfo("timing_ms", null);

			event.exitCode = out.getReturnCode();
			event.errorMessage = out.getErrorMessage();

			flush();
		} else {
			event.exitCode = ErrNo.ECONNRESET.getErrorCode();
			event.errorMessage = "Client went away";
		}
	}

	/**
	 * Set the timing flag, to return server side command execution timing to the
	 * client
	 *
	 * @param timingInfo
	 * @return the previous value of this flag
	 */
	public boolean setTiming(final boolean timingInfo) {
		final boolean previousValue = returnTiming;

		returnTiming = timingInfo;

		return previousValue;
	}

	/**
	 * @return the timing flag
	 * @see #setTiming(boolean)
	 */
	public boolean getTiming() {
		return returnTiming;
	}

	/**
	 * flush the buffer and produce status to be send to client
	 */
	public void flush() {
		if (out != null) {
			out.setenv(getCurrentDirName(), getUsername());
			out.flush();
		}
	}

	/**
	 * create and return a object of
	 * alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	 *
	 * @param classSuffix
	 *                    the name of the shell command, which will be taken as the
	 *                    suffix for the classname
	 * @param objectParm
	 *                    array of argument objects, need to fit to the class
	 * @return an instance of
	 *         alien.shell.commands.JAliEnCommand.JAliEnCommand<classSuffix>
	 * @throws Exception
	 */
	protected static JAliEnBaseCommand getCommand(final String classSuffix, final Object[] objectParm)
			throws Exception {
		logger.log(Level.FINE, "Entering command with " + classSuffix + " and options " + Arrays.toString(objectParm));
		try {
			@SuppressWarnings("rawtypes")
			final Class cl = Class.forName("alien.shell.commands.JAliEnCommand" + classSuffix);

			@SuppressWarnings({ "rawtypes", "unchecked" })
			final java.lang.reflect.Constructor co = cl.getConstructor(JAliEnCOMMander.class, List.class);
			return (JAliEnBaseCommand) co.newInstance(objectParm);
		} catch (@SuppressWarnings("unused") final ClassNotFoundException e) {
			// System.out.println("No such command or not implemented");
			return null;
		} catch (final java.lang.reflect.InvocationTargetException e) {
			logger.log(Level.SEVERE, "Exception running command", e);
			return null;
		}
	}

	/**
	 * @return <code>true</code> if the command was silenced
	 */
	public final boolean commandIsSilent() {
		if (jcommand != null)
			return jcommand.isSilent();

		return out == null;
	}

	/**
	 * Complete current message and start the next one
	 */
	public void outNextResult() {
		if (!commandIsSilent() && out != null)
			out.nextResult();
	}

	/**
	 * Print a key-value pair to the output stream
	 *
	 * @param key
	 * @param value
	 */
	public void printOut(final String key, final Object value) {
		if (!commandIsSilent() && out != null && out.isRootPrinter() && !nokeys)
			out.setField(key, value);
	}

	/**
	 * Print the string to the output stream
	 *
	 * @param value
	 */
	public void printOut(final String value) {
		if (!commandIsSilent() && out != null && !nomsg)
			out.printOut(value);
	}

	/**
	 * Print a key-value (+"\n") pair to the output stream
	 *
	 * @param key
	 * @param value
	 */
	public void printOutln(final String key, final String value) {
		printOut(key, value + "\n");
	}

	/**
	 * Print the line to the output stream
	 *
	 * @param value
	 */
	public void printOutln(final String value) {
		printOut(value + "\n");
	}

	/**
	 * Print an empty line to the output stream
	 */
	public void printOutln() {
		printOut("\n");
	}

	/**
	 * Print an error message to the output stream
	 *
	 * @param value
	 */
	public void printErr(final String value) {
		if (!commandIsSilent() && out != null)
			out.printErr(value);
	}

	/**
	 * Print an error message line to the output stream
	 *
	 * @param value
	 */
	public void printErrln(final String value) {
		printErr(value + "\n");
	}

	private int lastExitCode = 0;
	private String lastErrorMessage = null;

	private void setLastError(final int exitCode, final String errorMessage) {
		this.lastExitCode = exitCode;
		this.lastErrorMessage = errorMessage;
	}

	/**
	 * Reset status for the next command
	 */
	public void clearLastError() {
		setLastError(0, null);
	}

	/**
	 * @return exit code set by the last executed command
	 * @see #setReturnCode(ErrNo)
	 * @see #setReturnCode(ErrNo, String)
	 * @see #setReturnCode(int, String)
	 * @see #clearLastError()
	 * @see #getLastErrorMessage()
	 */
	public int getLastExitCode() {
		return lastExitCode;
	}

	/**
	 * @return message produced by the last executed command
	 * @see #setReturnCode(ErrNo)
	 * @see #setReturnCode(ErrNo, String)
	 * @see #setReturnCode(int, String)
	 * @see #clearLastError()
	 * @see #getLastExitCode()
	 */
	public String getLastErrorMessage() {
		return lastErrorMessage;
	}

	/**
	 * Set the command's return code and print an error message to the output stream
	 *
	 * @param exitCode
	 * @param errorMessage
	 */
	public void setReturnCode(final int exitCode, final String errorMessage) {
		setLastError(exitCode, errorMessage);

		if (out != null)
			out.setReturnCode(exitCode, errorMessage);
	}

	/**
	 * Set the command's return code and print the default error message associated
	 * to it to the output stream
	 *
	 * @param errno
	 */
	public void setReturnCode(final ErrNo errno) {
		setLastError(errno.getErrorCode(), errno.getMessage());

		if (out != null)
			out.setReturnCode(errno);
	}

	/**
	 * Set the command's return code and print the default error message associated
	 * to it plus an additional information string
	 *
	 * @param errno
	 * @param additionalMessage
	 */
	public void setReturnCode(final ErrNo errno, final String additionalMessage) {
		setLastError(errno.getErrorCode(), errno.getMessage()
				+ (additionalMessage != null && !additionalMessage.isBlank() ? " : " + additionalMessage : ""));

		if (out != null)
			out.setReturnCode(errno, additionalMessage);
	}

	/**
	 * Set the command's return arguments (for RootPrinter)
	 *
	 * @param args
	 */
	public void setReturnArgs(final String args) {
		if (out != null)
			out.setReturnArgs(args);
	}

	/**
	 *
	 */
	public void pending() {
		if (!commandIsSilent() && out != null)
			out.pending();
	}

	/**
	 * Get commander's output stream writer
	 *
	 * @return UIPrintWriter
	 */
	public UIPrintWriter getPrintWriter() {
		return out;
	}

	/**
	 * @return the available internal commands to the user
	 */
	protected Set<String> getUserAvailableCommands() {
		return Collections.unmodifiableSet(userAvailableCommands);
	}

	private static final int KEY_MEMORY_SIZE = 1024;

	private static final long DECAY_TIME = 1000 * 60 * 5;

	private static final LRUMap<String, AtomicInteger> commandsPerKey = new LRUMap<>(KEY_MEMORY_SIZE);

	private static long nextSwap = System.currentTimeMillis() + DECAY_TIME;

	private static synchronized int incrementCommandCount(final String key, final int delta) {
		final int value = commandsPerKey.computeIfAbsent(key, (k) -> new AtomicInteger()).addAndGet(delta);

		if (System.currentTimeMillis() > nextSwap) {
			for (final AtomicInteger ai : commandsPerKey.values())
				ai.set(ai.get() / 2);

			nextSwap = System.currentTimeMillis() + DECAY_TIME;
		}

		return value;
	}

	private static final int getThrottleTime(final int counter) {
		if (counter <= 100)
			return 0;

		return counter - 100;
	}
}