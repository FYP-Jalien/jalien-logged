package alien.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.nfunk.jep.JEP;

import com.datastax.driver.core.ConsistencyLevel;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.Format;

/**
 * LFNCSD utilities
 *
 * @author mmmartin
 *
 */
public class LFNCSDUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFNCSDUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LFNCSDUtils.class.getCanonicalName());

	/** Thread pool */
	static ThreadPoolExecutor tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	static {
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);
	}

	/**
	 * Cassandra table suffix
	 */
	static public String append_table = "";
	/**
	 * Cassandra consistency
	 */
	static public ConsistencyLevel clevel = ConsistencyLevel.QUORUM;

	/**
	 * the "-s" flag of AliEn `find`
	 */
	public static final int FIND_NO_SORT = 1;

	/**
	 * the "-d" flag of AliEn `find`
	 */
	public static final int FIND_INCLUDE_DIRS = 2;

	/**
	 * the "-y" flag of AliEn `find`
	 */
	public static final int FIND_BIGGEST_VERSION = 4;

	/**
	 * @param command
	 * @param start_path
	 * @param pattern
	 * @param metadata
	 * @param flags
	 * @return list of lfns that fit the patterns, if any
	 */
	public static Collection<LFN_CSD> recurseAndFilterLFNs(final String command, final String start_path, final String pattern, final String metadata, final int flags) {
		final Set<LFN_CSD> ret;
		final AtomicInteger counter_left = new AtomicInteger();

		// we create a base for search and a file pattern
		int index = 0;
		String path = start_path;
		String file_pattern = (pattern == null ? "*" : pattern);
		if (!start_path.endsWith("/") && pattern == null) {
			file_pattern = start_path.substring(start_path.lastIndexOf('/') + 1);
			path = start_path.substring(0, start_path.lastIndexOf('/') + 1);
		}

		// choose to use sorted/unsorted type according to flag (-s)
		if ((flags & LFNCSDUtils.FIND_NO_SORT) != 0)
			ret = new LinkedHashSet<>();
		else
			ret = new TreeSet<>();

		// Split the base into directories, change asterisk and interrogation marks to regex format
		ArrayList<String> path_parts;
		if (!path.endsWith("/"))
			path += "*/";

		path = Format.replace(Format.replace(path, "*", ".*"), "?", ".");

		path_parts = new ArrayList<>(Arrays.asList(path.split("/")));

		if (file_pattern.contains("/")) {
			file_pattern = "*" + file_pattern;
			file_pattern = Format.replace(Format.replace(file_pattern, "*", ".*"), "?", ".?");
			String[] pattern_parts = file_pattern.split("/");
			file_pattern = pattern_parts[pattern_parts.length - 1];

			for (int i = 0; i < pattern_parts.length - 1; i++) {
				path_parts.add(pattern_parts[i]);
			}

		}
		else {
			file_pattern = Format.replace(Format.replace(file_pattern, "*", ".*"), "?", ".?");
		}

		path = "/";
		path_parts.remove(0); // position 0 otherwise is an empty string
		for (int i = 0; i < path_parts.size(); i++) {
			String s = path_parts.get(i);
			if (s.contains(".*") || s.contains(".?"))
				break;
			path += s + "/";
			index++;
		}

		Pattern pat = Pattern.compile(file_pattern);

		logger.info("Going to recurseAndFilterLFNs: " + path + " - " + file_pattern + " - " + index + " - " + flags + " - " + path_parts.toString());

		counter_left.incrementAndGet();
		try {
			tPool.submit(new RecurseLFNs(ret, counter_left, path, pat, index, path_parts, flags, metadata, (command.equals("find") ? true : false)));
		} catch (RejectedExecutionException ree) {
			logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit: " + ree);
			return null;
		}

		while (counter_left.get() > 0) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't wait?: " + e);
			}
		}

		return ret;
	}

	private static class RecurseLFNs implements Runnable {
		final Collection<LFN_CSD> col;
		final AtomicInteger counter_left;
		final String base;
		final Pattern file_pattern;
		final int index;
		final ArrayList<String> parts;
		final int flags;
		final String metadata;
		final boolean recurseInfinite;

		public RecurseLFNs(final Collection<LFN_CSD> col, final AtomicInteger counter_left, final String base, final Pattern file_pattern, final int index, final ArrayList<String> parts,
				final int flags, final String metadata, final boolean recurse) {
			this.col = col;
			this.counter_left = counter_left;
			this.base = base;
			this.file_pattern = file_pattern;
			this.index = index;
			this.parts = parts;
			this.flags = flags;
			this.metadata = metadata;
			this.recurseInfinite = recurse;
		}

		@Override
		public String toString() {
			return base + index;
		}

		@Override
		public void run() {
			boolean lastpart = (index >= parts.size());

			boolean includeDirs = false;
			if ((flags & LFNCSDUtils.FIND_INCLUDE_DIRS) != 0)
				includeDirs = true;

			LFN_CSD dir = new LFN_CSD(base, true, append_table, null, null);
			if (!dir.exists || dir.type != 'd') {
				logger.severe("LFNCSDUtils recurseAndFilterLFNs: initial dir invalid - " + base);
				counter_left.decrementAndGet();
				return;
			}

			List<LFN_CSD> list = dir.list(true, append_table, clevel);

			Pattern p;
			if (lastpart)
				p = this.file_pattern;
			else
				p = Pattern.compile(parts.get(index));

			JEP jep = null;

			if (metadata != null && !metadata.equals("")) {
				jep = new JEP();
				jep.setAllowUndeclared(true);
				String expression = Format.replace(Format.replace(Format.replace(metadata, "and", "&&"), "or", "||"), ":", "__");
				jep.parseExpression(expression);
			}

			ArrayList<LFN_CSD> filesVersion = null;
			if ((flags & LFNCSDUtils.FIND_BIGGEST_VERSION) != 0)
				filesVersion = new ArrayList<>();

			// loop entries
			for (LFN_CSD lfnc : list) {
				if (lfnc.type != 'd') {
					// no dir
					if (lastpart) {
						// check pattern
						Matcher m = p.matcher(lfnc.child);
						if (m.matches()) {
							if (jep != null) {
								// we check the metadata of the file against our expression
								Set<String> keys_values = new HashSet<>();

								// set the variable values from the metadata map
								for (String s : lfnc.metadata.keySet()) {
									Double value;
									try {
										value = Double.valueOf(lfnc.metadata.get(s));
									} catch (NumberFormatException e) {
										logger.info("Skipped: " + s + e);
										continue;
									}
									keys_values.add(s);
									jep.addVariable(s, value);
								}

								try {
									// This should return 1.0 or 0.0
									Object result = jep.getValueAsObject();
									if (result != null && result instanceof Double && ((Double) result).intValue() == 1.0) {
										if (filesVersion != null)
											filesVersion.add(lfnc);
										else
											col.add(lfnc);
									}
								} catch (Exception e) {
									logger.info("RecurseLFNs metadata - cannot get result: " + e);
								}

								// unset the variables for the next lfnc to be processed
								for (String s : keys_values) {
									jep.setVarValue(s, null);
								}
							}
							else {
								col.add(lfnc);
							}
						}
					}
				}
				else {
					// dir
					if (lastpart) {
						// if we already passed the hierarchy introduced on the command, all dirs are valid
						try {
							if (includeDirs) {
								Matcher m = p.matcher(lfnc.child);
								if (m.matches())
									col.add(lfnc);
							}
							if (recurseInfinite) {
								counter_left.incrementAndGet();
								tPool.submit(new RecurseLFNs(col, counter_left, base + lfnc.child + "/", file_pattern, index + 1, parts, flags, metadata, recurseInfinite));
							}
						} catch (RejectedExecutionException ree) {
							logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir(l) - " + base + lfnc.child + "/" + ": " + ree);
						}
					}
					else {
						// while exploring introduced dir, need to check patterns
						Matcher m = p.matcher(lfnc.child);
						if (m.matches()) {
							// submit the dir
							try {
								counter_left.incrementAndGet();
								tPool.submit(new RecurseLFNs(col, counter_left, base + lfnc.child + "/", file_pattern, index + 1, parts, flags, metadata, recurseInfinite));
							} catch (RejectedExecutionException ree) {
								logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir - " + base + lfnc.child + "/" + ": " + ree);
							}
						}
					}
				}
			}

			// we filter and add the file if -y and metadata
			if (jep != null && filesVersion != null) {
				Integer maxVersion = Integer.valueOf(-1);
				LFN_CSD maxLfn = null;

				for (LFN_CSD lfnc : filesVersion) {
					// Other way: String version_str = lfnc.child.substring(lfnc.child.indexOf("_v") + 2, lfnc.child.indexOf("_s0"));
					if (lfnc.metadata.containsKey("CDB__version")) {
						Integer version = Integer.valueOf(lfnc.metadata.get("CDB__version"));
						if (maxVersion.intValue() < version.intValue()) {
							maxVersion = version;
							maxLfn = lfnc;
						}
					}
				}

				if (maxLfn != null)
					col.add(maxLfn);

			}

			counter_left.decrementAndGet();
		}
	}

	/**
	 * @param base_path
	 * @param pattern
	 * @param flags
	 * @param metadata
	 * @return list of files for find command
	 */
	public static Collection<LFN_CSD> find(final String base_path, final String pattern, final int flags, final String metadata) {
		return recurseAndFilterLFNs("find", base_path, pattern, metadata, flags);
	}

	/**
	 * @param path
	 * @param flags
	 * @return list of files for ls command
	 */
	public static Collection<LFN_CSD> ls(final String path, final int flags) {
		final Set<LFN_CSD> ret = new TreeSet<>();

		// if need to resolve wildcard and recurse, we call the recurse method
		if (path.contains("*")) {
			return recurseAndFilterLFNs("ls", path, null, null, flags);
		}

		// otherwise we should be able to create the LFN_CSD from the path
		LFN_CSD lfnc = new LFN_CSD(path, true, append_table, null, null);
		if (lfnc.isDirectory()) {
			List<LFN_CSD> list = lfnc.list(true, append_table, clevel);
			ret.addAll(list);
		}
		else {
			ret.add(lfnc);
		}

		return ret;
	}

}
