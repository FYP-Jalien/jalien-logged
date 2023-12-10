package alien.site.batchqueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;
import java.util.logging.Level;

/**
 * Base interface for batch queues
 *
 * @author mmmartin
 */
public abstract class BatchQueue {
	/**
	 * Logging mechanism shared with the implementing code
	 */
	protected Logger logger = null;

	/**
	 * Common configuration mechanism with the BQ implementations
	 */
	protected HashMap<String, Object> config = null;

	/**
	 * Submit a new job agent to the queue
	 * 
	 * @param script
	 */
	public abstract void submit(final String script);

	/**
	 * @return number of currently active jobs
	 */
	public abstract int getNumberActive();

	/**
	 * @return number of queued jobs
	 */
	public abstract int getNumberQueued();

	/**
	 * @return how many jobs were killed
	 */
	public abstract int kill();
	// Previously named "_system" in perl

	/**
	 * @param cmd
	 * @return the output of the given command, one array entry per line
	 */
	public ExitStatus executeCommand(String cmd) {
		
		System.out.println("This is the batch queue execute command");

		ExitStatus exitStatus = null;
	
		System.out.println("Executing: " + cmd);
		logger.info("Executing: " + cmd);
	
		try {
			ArrayList<String> cmd_full = new ArrayList<>();
			cmd_full.add("/bin/bash");
			cmd_full.add("-c");
			cmd_full.add(cmd);
			System.out.println("Full command for the processbuilder : " + cmd_full);
			final ProcessBuilder proc_builder = new ProcessBuilder(cmd_full);
	
			Map<String, String> env = proc_builder.environment();
			System.out.println("ProcessBuilder Environment: " + env);
			String[] dirs = {
					"/cvmfs/alice.cern.ch/",
					env.get("JALIEN_ROOT"),
					env.get("JALIEN_HOME"),
					env.get("ALIEN_ROOT"),
			};
	
			HashMap<String, String> cleaned_env_vars = new HashMap<>();
			Pattern p = Pattern.compile(".*PATH$");
	
			for (final Map.Entry<String, String> entry : env.entrySet()) {
				final String var = entry.getKey();
				Matcher m = p.matcher(var);
	
				if (!m.matches()) {
					System.out.println("Skipping environment variable(not match patter, .*PATH$): " + var);
					continue;
				}
	
				String val = entry.getValue();
				String oldVal = val;
				// Debug print for environment variable cleanup
				System.out.println("Cleaning environment variable: " + var);
				for (String d : dirs) {
					if (d == null) {
						continue;
					}
	
					String dir = d.replaceAll("/+$", "");
					String pat = "\\Q" + dir + "\\E/[^:]*:?";
					val = val.replaceAll(pat, "");
				}
				System.out.println(var + " = " + oldVal + " cleaned to" + var + " = " + val);
				cleaned_env_vars.put(var, val);
			}
	
			env.putAll(cleaned_env_vars);
	
			// Debug print for the final cleaned environment variables
			System.out.println("Final cleaned environment variables: " + env);
	
			proc_builder.redirectErrorStream(true);
			System.out.println("Process builder redirect error stream: " + proc_builder.redirectErrorStream());
	
			System.out.println("Starting process builder");
			final Process proc = proc_builder.start();
			final ProcessWithTimeout pTimeout = new ProcessWithTimeout(proc, proc_builder);
			System.out.println("Process buildder timeout: " + pTimeout);
			System.out.println("Waiting for process builder to finish");
			pTimeout.waitFor(60, TimeUnit.SECONDS);
	
			exitStatus = pTimeout.getExitStatus();

			// Debug print for process exit status
			System.out.println("Process exit status: " + exitStatus);
	
			if (exitStatus.getExecutorFinishStatus() == ExecutorFinishStatus.ERROR)
				// Debug print for error detection
				System.out.println("An error was detected: " + exitStatus.getStdOut());
	
		} catch (final Throwable t) {
			// Debug print for exceptions
			System.out.println("Exception executing command: " + cmd);
			t.printStackTrace();
		}
	
		return exitStatus;
	}
	

	static List<String> getStdOut(ExitStatus exitStatus) {
		return Arrays.asList(exitStatus.getStdOut().split("\n")).stream().map(String::trim).filter((s) -> !s.isBlank()).collect(Collectors.toList());
	}

	static List<String> getStdErr(ExitStatus exitStatus) {
		return Arrays.asList(exitStatus.getStdErr().split("\n")).stream().map(String::trim).filter((s) -> !s.isBlank()).collect(Collectors.toList());
	}

	/**
	 * @param keyValue
	 * @param key
	 * @param defaultValue
	 * @return the value of the given key, if found, otherwise returning the default value
	 */
	public static final String getValue(final String keyValue, final String key, final String defaultValue) {
		if (keyValue.startsWith(key + '='))
			return keyValue.substring(key.length() + 1).trim();

		return defaultValue;
	}
}
