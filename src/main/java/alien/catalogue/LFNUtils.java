package alien.catalogue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.TransferUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import lazyj.DBFunctions;
import lazyj.Format;
import utils.DBUtils;
import utils.ExpireTime;

/**
 * LFN utilities
 *
 * @author costing
 *
 */
public class LFNUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(LFNUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(LFNUtils.class.getCanonicalName());

	/**
	 * Get an LFN which corresponds to the given GUID
	 *
	 * @param g
	 * @return one of the matching LFNs, if there is any such entry
	 */
	public static LFN getLFN(final GUID g) {
		if (g == null)
			return null;

		final Collection<IndexTableEntry> indextable = CatalogueUtils.getAllIndexTables();

		if (indextable == null)
			return null;

		for (final IndexTableEntry ite : indextable) {
			final LFN l = ite.getLFN(g.guid);

			if (l != null)
				return l;
		}

		return null;
	}

	/**
	 * Bulk guid2lfn operation
	 *
	 * @param guids
	 *            the GUIDs to look for
	 * @return a list of LFNs for which the size and order is independent of the input collection
	 */
	public static List<LFN> getLFNs(final Collection<GUID> guids) {
		if (guids == null || guids.size() == 0)
			return null;

		final Set<UUID> uuids = new HashSet<>(guids.size());

		for (final GUID g : guids)
			uuids.add(g.guid);

		return getLFNsFromUUIDs(uuids);
	}

	/**
	 * Bulk guid2lfn operation
	 *
	 * @param uuids
	 *            the GUIDs to look for
	 * @return a list of LFNs for which the size and order is independent of the input collection
	 */
	public static List<LFN> getLFNsFromUUIDs(final Collection<UUID> uuids) {
		if (uuids == null || uuids.size() == 0)
			return null;

		final Collection<IndexTableEntry> indextable = CatalogueUtils.getAllIndexTables();

		if (indextable == null)
			return null;

		final List<LFN> ret = new ArrayList<>(uuids.size());

		final Set<UUID> remainingGUIDs = new HashSet<>(uuids);

		for (final IndexTableEntry ite : indextable) {
			final List<LFN> chunk = ite.getLFNs(remainingGUIDs);

			if (chunk != null) {
				for (final LFN l : chunk)
					remainingGUIDs.remove(l.guid);

				ret.addAll(chunk);

				if (remainingGUIDs.size() == 0)
					break;
			}
		}

		return ret;
	}

	/**
	 * Get the LFN entry for this catalog filename
	 *
	 * @param fileName
	 * @return LFN entry
	 */
	public static LFN getLFN(final String fileName) {
		return getLFN(fileName, false);
	}

	/**
	 * @param fileName
	 * @return the cleaned up path
	 */
	public static String processFileName(final String fileName) {
		String processedFileName = fileName;

		while (processedFileName.indexOf("//") >= 0)
			processedFileName = Format.replace(processedFileName, "//", "/");

		processedFileName = Format.replace(processedFileName, "/./", "/");

		int idx = processedFileName.indexOf("/../");

		while (idx > 0) {
			final int idx2 = processedFileName.lastIndexOf("/", idx - 1);

			if (idx2 > 0)
				processedFileName = processedFileName.substring(0, idx2) + processedFileName.substring(idx + 3);

			// System.err.println("After replacing .. : "+processedFileName);

			idx = processedFileName.indexOf("/../");
		}

		if (processedFileName.endsWith("/..")) {
			final int idx2 = processedFileName.lastIndexOf('/', processedFileName.length() - 4);

			if (idx2 > 0)
				processedFileName = processedFileName.substring(0, idx2);
		}

		return processedFileName;
	}

	/**
	 * Get the LFN entry for this catalog filename, optionally returning an empty object if the entry doesn't exist (yet)
	 *
	 * @param fileName
	 * @param evenIfDoesntExist
	 * @return entry
	 */
	public static LFN getLFN(final String fileName, final boolean evenIfDoesntExist) {
		if (fileName == null || fileName.length() == 0)
			return null;

		final String processedFileName = processFileName(fileName);

		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(processedFileName);

		if (ite == null) {
			logger.log(Level.FINE, "IndexTableEntry is null for: " + processedFileName + " (even if doesn't exist: " + evenIfDoesntExist + ")");

			return null;
		}

		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Using " + ite + " for: " + processedFileName);

		return ite.getLFN(processedFileName, evenIfDoesntExist);
	}

	/**
	 * Get the LFN entry for this catalog filename, optionally returning an empty object if the entry doesn't exist (yet)
	 *
	 * @param ignoreFolders
	 * @param fileName
	 * @return entry
	 */
	public static List<LFN> getLFNs(final boolean ignoreFolders, final Collection<String> fileName) {
		System.out.println("This is Getting LFNs in LFN Utils");
	
		if (fileName == null || fileName.size() == 0) {
			System.out.println("File names collection is empty");
			return null;
		}
	
		final List<LFN> retList = new ArrayList<>(fileName.size());
	
		if (fileName.size() == 1) {
			final String singleFileName = fileName.iterator().next();
			System.out.println("Single File Name: " + singleFileName);
	
			final LFN l = getLFN(singleFileName);
	
			if (l != null) {
				retList.add(l);
				System.out.println("LFN added to the list");
				return retList;
			}
	
			System.out.println("Failed to get LFN for the single file name");
			return null;
		}
	
		final Map<IndexTableEntry, List<String>> mapping = new HashMap<>();
	
		for (final String file : fileName) {
			final String processedFileName = processFileName(file);
			System.out.println("Processed File Name: " + processedFileName);
	
			final IndexTableEntry ite = CatalogueUtils.getClosestMatch(processedFileName);
	
			if (ite != null) {
				List<String> files = mapping.get(ite);
	
				if (files == null) {
					files = new LinkedList<>();
					mapping.put(ite, files);
				}
	
				files.add(processedFileName);
			}
		}
	
		if (mapping.size() == 0) {
			System.out.println("No mapping found");
			return null;
		}
	
		for (final Map.Entry<IndexTableEntry, List<String>> entry : mapping.entrySet()) {
			final IndexTableEntry ite = entry.getKey();
			final List<String> fileList = entry.getValue();
			System.out.println("Processing IndexTableEntry: " + ite);
			System.out.println("File List: " + fileList);
	
			final List<LFN> tempList = ite.getLFNs(ignoreFolders, fileList);
	
			if (tempList != null && tempList.size() > 0) {
				retList.addAll(tempList);
				System.out.println("LFNs added to the list");
			}
		}
	
		System.out.println("LFN retrieval completed");
		System.out.println("retList: " + retList);
		return retList;
	}
	

	/**
	 * @param user
	 * @param lfn
	 * @param recursive
	 * @return status of the removal
	 */
	public static boolean rmLFN(final AliEnPrincipal user, final LFN lfn, final boolean recursive) {
		if (lfn != null && lfn.exists) {
			if (AuthorizationChecker.canWrite(lfn, user)) {
				logger.log(Level.SEVERE, "Request from [" + user.getName() + "], rmLFN [" + lfn.getCanonicalName() + "]");
				return lfn.delete(true, recursive);
			}
			return false;

		}

		return false;
	}

	/**
	 * @param user
	 * @param lfn
	 * @param recursive
	 * @param purge
	 * @return status of the removal
	 */
	public static boolean rmLFN(final AliEnPrincipal user, final LFN lfn, final boolean recursive, final boolean purge) {
		if (lfn != null && lfn.exists) {
			if (AuthorizationChecker.canWrite(lfn, user)) {
				logger.log(Level.SEVERE, "Request from [" + user.getName() + "], rmLFN [" + lfn.getCanonicalName() + "]");
				return lfn.delete(purge, recursive);
			}
			return false;

		}

		return false;
	}

	/**
	 * @param user
	 * @param lfn
	 * @param newpath
	 * @return status of the removal
	 */
	public static LFN mvLFN(final AliEnPrincipal user, final LFN lfn, final String newpath) {
		if (lfn == null || !lfn.exists) {
			logger.log(Level.WARNING, "The file to move doesn't exist.");
			return null;
		}

		if (!AuthorizationChecker.canWrite(lfn.getParentDir(), user)) {
			logger.log(Level.WARNING, "Not authorized to move the file [" + lfn.getCanonicalName() + "]");
			logger.log(Level.WARNING, "YOU ARE [" + user + "]");
			logger.log(Level.WARNING, "parent is [" + lfn.getParentDir() + "]");
			return null;
		}

		final LFN tLFN = getLFN(newpath, true);

		if (tLFN.exists || !tLFN.getParentDir().exists || !AuthorizationChecker.canWrite(tLFN.getParentDir(), user)) {
			logger.log(Level.WARNING, "Not possible to move to [" + tLFN.getCanonicalName() + "]");
			return null;
		}

		tLFN.aclId = lfn.aclId;
		tLFN.broken = lfn.broken;
		tLFN.ctime = lfn.ctime;
		tLFN.dir = lfn.dir;
		tLFN.expiretime = lfn.expiretime;
		tLFN.gowner = lfn.gowner;
		tLFN.guid = lfn.guid;
		tLFN.guidtime = lfn.guidtime;
		tLFN.jobid = lfn.jobid;
		tLFN.md5 = lfn.md5;
		tLFN.owner = lfn.owner;
		tLFN.perm = lfn.perm;
		tLFN.replicated = lfn.replicated;
		tLFN.size = lfn.size;
		tLFN.type = lfn.type;

		if (!LFNUtils.insertLFN(tLFN)) {
			logger.log(Level.WARNING, "Could not insert: " + tLFN);
			return null;
		}

		if (lfn.isDirectory()) {
			final List<LFN> subentries = lfn.list();

			if (subentries != null)
				for (final LFN subentry : subentries)
					if (mvLFN(user, subentry, tLFN.getCanonicalName() + "/" + subentry.getFileName()) == null) {
						logger.log(Level.WARNING, "Could not move " + subentry.getCanonicalName() + " to " + tLFN.getCanonicalName() + "/" + subentry.getFileName() + ", bailing out");
						return null;
					}
		}

		if (!lfn.delete(false, false)) {
			logger.log(Level.WARNING, "Could not delete: " + lfn);
			return null;
		}

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, "Deleted entry [" + lfn.getCanonicalName() + "]");

		return tLFN;
	}

	/**
	 * Make sure the parent directory exists
	 *
	 * @param lfn
	 * @return the updated LFN entry
	 */
	static LFN ensureDir(final LFN lfn) {
		if (lfn.exists)
			return lfn;

		if (lfn.perm == null)
			lfn.perm = "755";

		LFN parent = lfn.getParentDir(true);

		if (!parent.exists) {
			parent.owner = lfn.owner;
			parent.gowner = lfn.gowner;
			parent.perm = lfn.perm;
			parent = ensureDir(parent);
		}

		if (parent == null)
			return null;

		lfn.parentDir = parent;
		lfn.type = 'd';

		if (insertLFN(lfn))
			return lfn;

		return null;
	}

	/**
	 * Create a new directory with a given owner
	 *
	 * @param owner
	 *            owner of the newly created structure(s)
	 * @param path
	 *            the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final String path) {
		return mkdir(owner, path, false);
	}

	/**
	 * Create a new directory hierarchy with a given owner
	 *
	 * @param owner
	 *            owner of the newly created structure(s)
	 * @param path
	 *            the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdirs(final AliEnPrincipal owner, final String path) {
		return mkdir(owner, path, true);
	}

	/**
	 * Create a new directory (hierarchy) with a given owner
	 *
	 * @param owner
	 *            owner of the newly created structure(s)
	 * @param path
	 *            the path to be created
	 * @param createMissingParents
	 *            if <code>true</code> then it will try to create any number of intermediate directories, otherwise the direct parent must already exist
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final String path, final boolean createMissingParents) {
		final LFN lfn = LFNUtils.getLFN(path, true);

		return mkdir(owner, lfn, createMissingParents);
	}

	/**
	 * Create a new directory with a given owner
	 *
	 * @param owner
	 *            owner of the newly created structure(s)
	 * @param lfn
	 *            the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final LFN lfn) {
		return mkdir(owner, lfn, false);
	}

	/**
	 * Create a new directory hierarchy with a given owner
	 *
	 * @param owner
	 *            owner of the newly created structure(s)
	 * @param lfn
	 *            the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdirs(final AliEnPrincipal owner, final LFN lfn) {
		return mkdir(owner, lfn, true);
	}

	/**
	 * Create a new directory (hierarchy) with a given owner
	 *
	 * @param owner
	 *            owner of the newly created structure(s)
	 * @param lfn
	 *            the path to be created
	 * @param createMissingParents
	 *            if <code>true</code> then it will try to create any number of intermediate directories, otherwise the direct parent must already exist
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final LFN lfn, final boolean createMissingParents) {

		if (owner == null || lfn == null)
			return null;

		if (lfn.exists) {
			if (lfn.isDirectory() && AuthorizationChecker.canWrite(lfn, owner))
				return lfn;

			return null;
		}

		lfn.owner = owner.getName();
		lfn.gowner = lfn.owner;

		lfn.size = 0;

		LFN parent = lfn.getParentDir(true);

		if (!parent.exists && !createMissingParents)
			return null;

		while (parent != null && !parent.exists)
			parent = parent.getParentDir(true);

		if (parent != null && parent.isDirectory() && AuthorizationChecker.canWrite(parent, owner))
			return ensureDir(lfn);

		return null;
	}

	/**
	 * @param user
	 * @param lfn
	 * @param recursive
	 * @return status of the removal
	 */
	public static boolean rmdir(final AliEnPrincipal user, final LFN lfn, final boolean recursive) {
		if (lfn != null && lfn.exists && lfn.isDirectory()) {
			if (AuthorizationChecker.canWrite(lfn, user)) {
				logger.log(Level.SEVERE, "Request from [" + user.getName() + "], rmdir [" + lfn.getCanonicalName() + "]");
				return lfn.delete(true, recursive);
			}
			return false;

		}

		return false;
	}

	/**
	 * Touch an LFN: if the entry exists, update its timestamp, otherwise try to create an empty file
	 *
	 * @param user
	 *            who wants to do the operation
	 * @param lfn
	 *            LFN to be touched (from {@link LFNUtils#getLFN(String, boolean)}, called with the second argument <code>false</code> if the entry doesn't exist yet
	 * @return <code>true</code> if the LFN was touched
	 */
	public static boolean touchLFN(final AliEnPrincipal user, final LFN lfn) {
		if (!lfn.exists) {
			final LFN parentDir = lfn.getParentDir();

			if (parentDir == null || !AuthorizationChecker.canWrite(parentDir, user)) {
				logger.log(Level.SEVERE, "Cannot write to the Current Directory OR Parent Directory is null BUT file exists. Terminating");
				return false;
			}

			lfn.type = 'f';
			lfn.size = 0;
			lfn.md5 = "d41d8cd98f00b204e9800998ecf8427e";
			lfn.guid = null;
			lfn.guidtime = null;
			lfn.owner = user.getName();
			lfn.gowner = user.getRoles().iterator().next();
			lfn.perm = "755";
		}
		else if (!AuthorizationChecker.canWrite(lfn, user)) {
			logger.log(Level.SEVERE, "Cannot write to the Current Directory BUT file does not exist. Terminating");
			return false;
		}

		lfn.ctime = new Date();

		if (!lfn.exists)
			return insertLFN(lfn);

		return lfn.update();
	}

	/**
	 * Insert an LFN in the catalogue
	 *
	 * @param lfn
	 * @return true if the entry was inserted (or previously existed), false if there was an error
	 */
	public static boolean insertLFN(final LFN lfn) {
		if (lfn.exists)
			// nothing to be done, the entry already exists
			return true;

		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(lfn.getCanonicalName());

		if (ite == null) {
			logger.log(Level.WARNING, "IndexTableEntry is null for: " + lfn.getCanonicalName());

			return false;
		}

		final LFN parent = ensureDir(lfn.getParentDir(true));

		if (parent == null) {
			logger.log(Level.WARNING, "Parent dir is null for " + lfn.getCanonicalName());

			return false;
		}

		lfn.parentDir = parent;
		lfn.indexTableEntry = ite;

		if (lfn.indexTableEntry.equals(parent.indexTableEntry))
			lfn.dir = parent.entryId;

		return lfn.insert();
	}

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
	 * Use Perl-style regexp in the pattern instead of SQL-style (which is the default for find). This is to be used for wildcard expansion.
	 */
	public static final int FIND_REGEXP = 8;

	/**
	 * the "-x" flag of JAliEn `find`
	 */
	public static final int FIND_SAVE_XML = 16;

	/**
	 * find only files of a given job, -j in AliEn `find`
	 */
	public static final int FIND_FILTER_JOBID = 32;

	/**
	 * @param path
	 * @param pattern
	 * @param flags
	 *            a combination of FIND_* flags
	 * @return the list of LFNs that match
	 */
	public static Collection<LFN> find(final String path, final String pattern, final int flags) {
		return find(path, pattern, flags, null, "", Long.valueOf(0));
	}

	/**
	 * @param path
	 * @param pattern
	 * @param flags
	 *            a combination of FIND_* flags
	 * @param owner
	 * @param xmlCollectionName
	 * @param queueid
	 *            a job id to filter for its files
	 * @return the list of LFNs that match
	 */
	public static Collection<LFN> find(final String path, final String pattern, final int flags, final AliEnPrincipal owner, final String xmlCollectionName, final Long queueid) {
		return find(path, pattern, null, flags, owner, xmlCollectionName, queueid, 0);
	}

	/**
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 *            a combination of FIND_* flags
	 * @param owner
	 * @param xmlCollectionName
	 * @param queueid
	 *            a job id to filter for its files
	 * @param queryLimit if strictly positive then restrict the number of returned entries to this many; if more would be produced, exit with an exception
	 * @return the list of LFNs that match
	 */
	public static Collection<LFN> find(final String path, final String pattern, final String query, final int flags, final AliEnPrincipal owner, final String xmlCollectionName, final Long queueid,
			final long queryLimit) {
		return find(path, pattern, query, flags, owner, xmlCollectionName, queueid, queryLimit, null);
	}

	/**
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 *            a combination of FIND_* flags
	 * @param owner
	 * @param xmlCollectionName
	 * @param queueid
	 *            a job id to filter for its files
	 * @param queryLimit if strictly positive then restrict the number of returned entries to this many; if more would be produced, exit with an exception
	 * @param excludedPatterns
	 * @return the list of LFNs that match
	 */
	public static Collection<LFN> find(final String path, final String pattern, final String query, final int flags, final AliEnPrincipal owner, final String xmlCollectionName, final Long queueid,
			final long queryLimit, final Collection<String> excludedPatterns) {
		final String processedPattern;

		if ((flags & FIND_REGEXP) == 0)
			processedPattern = Format.replace(pattern, "*", "%");
		else
			processedPattern = pattern;

		if ((flags & FIND_BIGGEST_VERSION) != 0) {
			final String tag = query.substring(0, query.indexOf(":"));
			return findByMetadata(path, processedPattern, tag, query);
		}

		final Set<LFN> ret;

		if ((flags & FIND_NO_SORT) != 0)
			ret = new LinkedHashSet<>();
		else
			ret = new TreeSet<>();

		final Collection<IndexTableEntry> matchingTables = CatalogueUtils.getAllMatchingTables(path);

		for (final IndexTableEntry ite : matchingTables) {
			final List<LFN> findResults = ite.find(path, processedPattern, flags, queueid, queryLimit > 0 ? queryLimit - ret.size() : 0, excludedPatterns);

			if (findResults == null)
				return null;

			ret.addAll(findResults);

			if (queryLimit > 0 && ret.size() >= queryLimit)
				break;
		}

		if ((flags & FIND_SAVE_XML) != 0) {
			// Create the xml collection

			final XmlCollection c = new XmlCollection();
			c.addAll(ret);
			c.setName(xmlCollectionName);
			c.setOwner(owner.getName());

			final StringBuilder str = new StringBuilder("find");

			str.append(' ').append(path);
			str.append(' ').append(pattern);
			str.append(' ').append("-x");
			str.append(' ').append(xmlCollectionName);

			// Append the command, that was executed to receive this collection
			c.setCommand(str.toString());
			try {
				// Create a local temp file
				final File f = File.createTempFile("collection-" + xmlCollectionName + System.currentTimeMillis(), ".xml");

				if (f != null) {
					// Save xml collection to local file
					final String content = c.toString();
					try (BufferedWriter o = new BufferedWriter(new FileWriter(f))) {
						o.write(content);
					}

					// Upload this file to grid
					IOUtils.upload(f, xmlCollectionName, owner, 4);
					f.delete();
				}

			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "Could not upload the XML collection because " + e.toString());
				e.printStackTrace();
			}
		}

		return ret;
	}

	private static final String[] CATALOGUE_DBS = new String[] { "alice_data", "alice_users" };

	/**
	 * @param path starting path
	 * @param tag tag to search for
	 * @param includeParents whether to include the parent folders in the search (for package dependencies for example) or not (for OCDB searches from a base path)
	 * @return metadata table where this tag can be found for this path, or <code>null</code> if there is no such entry
	 */
	public static Set<String> getTagTableNames(final String path, final String tag, final boolean includeParents) {
		final Set<String> ret = new HashSet<>();

		for (final String database : CATALOGUE_DBS) {
			try (DBFunctions db = ConfigUtils.getDB(database)) {
				if (db == null)
					continue;

				db.setReadOnly(true);
				db.setQueryTimeout(30);

				if (includeParents)
					db.query("SELECT distinct tableName FROM TAG0 WHERE tagName=? AND ? LIKE concat(path,'%') ORDER BY length(path) desc, path desc;", false, tag, path);
				else
					db.query("SELECT distinct tableName FROM TAG0 WHERE tagName=? AND path LIKE ?", false, tag, path + "%");

				while (db.moveNext())
					ret.add(database + "." + db.gets(1));

				if (ret.size() > 0)
					return ret;
			}
		}

		return ret;
	}

	/**
	 * @param path
	 * @return all tags that are defined for the given path
	 */
	public static Set<String> getTags(final String path) {
		final Set<String> ret = new LinkedHashSet<>();

		try (DBFunctions db = ConfigUtils.getDB("alice_data")) {
			db.query("SELECT distinct tagName from TAG0 where ? like concat(path,'%');", false, path);

			while (db.moveNext()) {
				ret.add(db.gets(1));
			}
		}

		return ret;
	}

	/**
	 * @param path file or directory to get the tag values for
	 * @param tag table to query
	 * @param includeParents whether to look up the folder structure for anything that matches the path or this entry alone
	 * @param columnConstraints ignore any column not present in this list. Can be <code>null</code> or empty, in which case everything will be taken in.
	 * @return all columns from the respective tag table for this entry and potentially any parent folders too
	 */
	public static Map<String, String> getTagValues(final String path, final String tag, final boolean includeParents, final Set<String> columnConstraints) {
		final Map<String, String> ret = new LinkedHashMap<>();

		try (DBFunctions db = ConfigUtils.getDB("alice_data")) {
			db.setReadOnly(true);
			db.setQueryTimeout(30);

			for (final String tableName : getTagTableNames(path, tag, includeParents)) {
				if (includeParents)
					db.query("SELECT * FROM " + tableName + " WHERE ? LIKE concat(file, '%') ORDER BY length(file) ASC;", false, path);
				else
					db.query("SELECT * FROM " + tableName + " WHERE file=?", false, path);

				while (db.moveNext()) {
					final String file = db.gets("file");

					final String oldFile = ret.get("file");

					final boolean overwrite = oldFile == null || file.length() > oldFile.length();

					for (final String columnName : db.getColumnNames()) {
						if (columnConstraints != null && columnConstraints.size() > 0 && !columnConstraints.contains(columnName))
							continue;

						final String value = db.gets(columnName);

						if (overwrite) {
							ret.put(columnName, value);
						}
						else {
							final String oldValue = ret.get(columnName);

							if (oldValue == null || oldValue.length() == 0)
								ret.put(columnName, value);
						}
					}
				}
			}
		}

		return ret;
	}

	/**
	 * @param path
	 * @param pattern
	 * @param tag
	 * @param query
	 * @return the files that match the metadata query
	 */
	public static Set<LFN> findByMetadata(final String path, final String pattern, final String tag, final String query) {
		if (monitor != null)
			monitor.incrementCounter("LFN_findByMetadata");

		try (DBFunctions db = ConfigUtils.getDB("alice_data")) {
			if (db == null) {
				logger.log(Level.WARNING, "Cannot get a DB instance");

				return Collections.emptySet();
			}

			db.setQueryTimeout(600);
			db.setReadOnly(true);

			final Map<String, LFN> retMap = new HashMap<>();

			for (final String tableName : getTagTableNames(path, tag, false)) {
				final String q = "SELECT file FROM " + Format.escSQL(tableName) + " "
						+ Format.escSQL(tag) + " WHERE file LIKE '" + Format.escSQL(path + "%" + pattern + "%") + "' AND "
						+ Format.escSQL(query.replace(":", ".") + " order by dir_number, version desc, file desc;");

				if (!db.query(q))
					continue;

				while (db.moveNext()) {
					final String fileName = db.gets(1);

					final String dirName = fileName.substring(0, fileName.lastIndexOf('/'));

					if (!retMap.containsKey(dirName)) {
						final LFN l = LFNUtils.getLFN(db.gets(1));

						if (l != null)
							retMap.put(dirName, l);
					}
				}
			}

			return new TreeSet<>(retMap.values());
		}
	}

	/**
	 * Create a new collection with the given path
	 *
	 * @param collectionName
	 *            full path (LFN) of the collection
	 * @param owner
	 *            collection owner
	 * @return the newly created collection
	 */
	public static LFN createCollection(final String collectionName, final AliEnPrincipal owner) {
		if (collectionName == null || owner == null)
			return null;

		final LFN lfn = getLFN(collectionName, true);

		if (lfn.exists) {
			if (lfn.isCollection() && AuthorizationChecker.canWrite(lfn, owner))
				return lfn;

			return null;
		}

		final LFN parentDir = lfn.getParentDir();

		if (parentDir == null)
			// will not create directories up to this path, do it explicitly
			// before calling this
			return null;

		if (!AuthorizationChecker.canWrite(parentDir, owner))
			// not allowed to write here. Not sure we should double check here,
			// but it doesn't hurt to be sure
			return null;

		final GUID guid = GUIDUtils.createGuid();

		guid.ctime = lfn.ctime = new Date();
		guid.owner = lfn.owner = owner.getName();

		final Set<String> roles = owner.getRoles();
		guid.gowner = lfn.gowner = (roles != null && roles.size() > 0) ? roles.iterator().next() : lfn.owner;
		guid.size = lfn.size = 0;
		guid.type = lfn.type = 'c';

		lfn.guid = guid.guid;
		lfn.perm = guid.perm = "755";
		lfn.aclId = guid.aclId = -1;
		lfn.jobid = -1;
		lfn.md5 = guid.md5 = "n/a";

		if (!guid.update())
			return null;

		if (!insertLFN(lfn))
			return null;

		if (monitor != null)
			monitor.incrementCounter("LFN_createCollection");

		final String q = "INSERT INTO COLLECTIONS (collGUID) VALUES (string2binary(?));";

		try (DBFunctions db = ConfigUtils.getDB("alice_data")) {
			db.setQueryTimeout(60);

			if (!db.query(q, false, lfn.guid.toString()))
				return null;
		}

		return lfn;
	}

	/**
	 * @param collection
	 * @param lfns
	 * @return <code>true</code> if the collection was modified
	 */
	public static boolean removeFromCollection(final LFN collection, final Set<LFN> lfns) {
		if (!collection.exists || !collection.isCollection() || lfns == null || lfns.size() == 0)
			return false;

		if (monitor != null)
			monitor.incrementCounter("LFN_removeFromCollection");

		try (DBFunctions db = ConfigUtils.getDB("alice_data")) {
			db.setQueryTimeout(60);

			db.setReadOnly(true);

			db.query("SELECT collectionId FROM COLLECTIONS where collGUID=string2binary(?);", false, collection.guid.toString());

			db.setReadOnly(false);

			if (!db.moveNext())
				return false;

			final int collectionId = db.geti(1);

			final Set<String> currentLFNs = collection.listCollection();

			final GUID guid = GUIDUtils.getGUID(collection);

			boolean updated = false;

			boolean shouldUpdateSEs = false;

			for (final LFN l : lfns) {
				if (!currentLFNs.contains(l.getCanonicalName()))
					continue;

				if (!db.query("DELETE FROM COLLECTIONS_ELEM where collectionId=? AND origLFN=? AND guid=string2binary(?);", false, Integer.valueOf(collectionId), l.getCanonicalName(),
						l.guid.toString()))
					continue;

				if (db.getUpdateCount() != 1)
					continue;

				guid.size -= l.size;
				updated = true;

				if (!shouldUpdateSEs) {
					final Set<PFN> whereis = l.whereisReal();

					if (whereis != null)
						for (final PFN p : whereis)
							if (!guid.seStringList.contains(Integer.valueOf(p.seNumber))) {
								shouldUpdateSEs = true;
								break;
							}
				}
			}

			if (updated) {
				collection.size = guid.size;

				collection.ctime = guid.ctime = new Date();

				if (shouldUpdateSEs) {
					Set<Integer> ses = null;

					final Set<String> remainingLFNs = collection.listCollection();

					for (final String s : remainingLFNs)
						if (ses == null || ses.size() > 0) {
							final LFN l = LFNUtils.getLFN(s);

							if (l == null)
								continue;

							final Set<PFN> whereis = l.whereisReal();

							if (whereis != null) {
								final Set<Integer> lses = new HashSet<>();

								for (final PFN pfn : whereis)
									lses.add(Integer.valueOf(pfn.seNumber));

								if (ses != null)
									ses.retainAll(lses);
								else
									ses = lses;
							}
						}

					if (ses != null)
						guid.seStringList = ses;
				}

				guid.update();
				collection.update();

				return true;
			}

			return false;
		}
	}

	/**
	 * @param collection
	 * @param lfns
	 * @return <code>true</code> if anything was changed
	 */
	public static boolean addToCollection(final LFN collection, final Collection<String> lfns) {
		if (lfns == null || lfns.size() == 0)
			return false;

		final TreeSet<LFN> toAdd = new TreeSet<>();

		final List<LFN> foundLFNs = LFNUtils.getLFNs(true, lfns);

		if (foundLFNs != null)
			toAdd.addAll(foundLFNs);

		if (toAdd.size() == 0) {
			logger.log(Level.FINER, "Quick exit");
			return false;
		}

		return addToCollection(collection, toAdd);
	}

	/**
	 * @param collection
	 * @param lfns
	 * @return <code>true</code> if anything was changed
	 */
	public static boolean addToCollection(final LFN collection, final Set<LFN> lfns) {
		if (!collection.exists || !collection.isCollection() || lfns == null || lfns.size() == 0) {
			logger.log(Level.FINER, "Quick exit");
			return false;
		}

		if (monitor != null)
			monitor.incrementCounter("LFN_addToCollection");

		try (DBFunctions db = ConfigUtils.getDB("alice_data")) {
			db.setQueryTimeout(300);

			final Set<String> currentLFNs = collection.listCollection();

			db.setReadOnly(true);

			db.query("SELECT collectionId FROM COLLECTIONS where collGUID=string2binary(?);", false, collection.guid.toString());

			db.setReadOnly(false);

			if (!db.moveNext()) {
				logger.log(Level.WARNING, "Didn't find any collectionId for guid " + collection.guid.toString());
				return false;
			}

			final int collectionId = db.geti(1);

			final Set<LFN> toAdd = new LinkedHashSet<>();

			for (final LFN lfn : lfns) {
				if (currentLFNs.contains(lfn.getCanonicalName()))
					continue;

				toAdd.add(lfn);
			}

			if (toAdd.size() == 0) {
				logger.log(Level.INFO, "Nothing to add to " + collection.getCanonicalName() + ", all " + lfns.size() + " entries are listed already");
				return false;
			}

			final GUID guid = GUIDUtils.getGUID(collection);

			Set<Integer> commonSEs = guid.size == 0 && guid.seStringList.size() == 0 ? null : new HashSet<>(guid.seStringList);

			boolean updated = false;

			for (final LFN lfn : toAdd) {
				if (commonSEs == null || commonSEs.size() > 0) {
					final Set<PFN> pfns = lfn.whereisReal();

					if (pfns != null) {
						final Set<Integer> ses = new HashSet<>();

						for (final PFN pfn : pfns)
							ses.add(Integer.valueOf(pfn.seNumber));

						if (commonSEs != null)
							commonSEs.retainAll(ses);
						else
							commonSEs = ses;
					}
				}

				if (db.query("INSERT INTO COLLECTIONS_ELEM (collectionId,origLFN,guid) VALUES (?, ?, string2binary(?));", false, Integer.valueOf(collectionId), lfn.getCanonicalName(),
						lfn.guid.toString())) {
					guid.size += lfn.size;
					updated = true;
				}
			}

			if (!updated) {
				logger.log(Level.FINER, "No change to the collection");
				return false; // nothing changed
			}

			if (commonSEs != null)
				guid.seStringList = commonSEs;

			collection.size = guid.size;

			collection.ctime = guid.ctime = new Date();

			guid.update();
			collection.update();
		}

		return true;
	}

	/**
	 * Change owner
	 *
	 * @param path
	 * @param new_owner
	 * @param new_group
	 * @return <code>true</code> if successful
	 */
	public static boolean chownLFN(final String path, final String new_owner, final String new_group) {
		final LFN lfn = getLFN(path);

		if (lfn == null)
			return false;

		if (new_owner == null || new_owner.isEmpty())
			return false;

		final GUID g = GUIDUtils.getGUID(lfn);

		lfn.owner = new_owner;

		if (g != null)
			g.owner = new_owner;

		if (new_group != null && !new_group.isEmpty()) {
			lfn.gowner = new_group;

			if (g != null)
				g.gowner = new_group;
		}

		if (!lfn.update())
			return false;

		if (g != null)
			g.update();

		return true;
	}

	/**
	 * Change owner
	 *
	 * @param path
	 * @param newMode
	 * @return <code>true</code> if successful
	 */
	public static boolean chmodLFN(final String path, final String newMode) {
		final LFN lfn = getLFN(path);

		if (lfn == null)
			return false;

		if (newMode == null || newMode.isBlank())
			return false;

		try {
			lfn.chmod(newMode);

			final GUID g = GUIDUtils.getGUID(lfn);

			if (g != null)
				g.chmod(newMode);
		}
		catch (@SuppressWarnings("unused") final IllegalAccessError err) {
			return false;
		}

		return true;
	}

	/**
	 * @param path
	 * @param dstSE
	 * @param is_guid
	 * @param attempts
	 * @return transfer ID
	 */
	public static long mirrorLFN(final String path, final String dstSE, final boolean is_guid, final Integer attempts) {
		LFN lfn;
		if (is_guid) {
			final GUID g = GUIDUtils.getGUID(UUID.fromString(path), false);
			lfn = getLFN(g);
		}
		else
			lfn = getLFN(path);

		if (lfn == null)
			return -256;

		if (dstSE == null || dstSE.length() == 0)
			return -255;

		final SE se = SEUtils.getSE(dstSE);
		if (se == null)
			return -254;

		// find closest SE
		final String site = ConfigUtils.getCloseSite();
		final List<SE> ses = SEUtils.getClosestSEs(site, true);

		if (ses.size() == 0)
			return -253;

		// run mirror
		return attempts != null && attempts.intValue() > 0 ? TransferUtils.mirror(lfn, se, null, attempts.intValue()) : TransferUtils.mirror(lfn, se);
	}

	/**
	 * @param path
	 * @param ses
	 * @param exses
	 * @param qos
	 * @param is_guid
	 * @param attempts
	 * @param removeReplica remove this source after a successful transfer
	 * @return transfer IDs to each SE
	 */
	public static HashMap<String, Long> mirrorLFN(final String path, final List<String> ses, final List<String> exses, final Map<String, Integer> qos, final boolean is_guid,
			final Integer attempts, final String removeReplica) {
		LFN lfn;
		if (is_guid) {
			final GUID g = GUIDUtils.getGUID(UUID.fromString(path), false);
			lfn = getLFN(g);
		}
		else
			lfn = getLFN(path);

		if (lfn == null)
			return null;

		final List<String> excludedSEs = new ArrayList<>();

		if (exses != null)
			excludedSEs.addAll(exses);

		final Set<PFN> existingReplicas = lfn.whereisReal();

		if (existingReplicas != null)
			for (final PFN p : existingReplicas) {
				final SE se = p.getSE();

				if (se != null)
					excludedSEs.add(se.getName());
			}

		// find closest SE
		final String site = ConfigUtils.getCloseSite();
		final List<SE> found_ses = SEUtils.getBestSEsOnSpecs(site, ses, excludedSEs, qos, true);
		final HashMap<String, Long> resmap = new HashMap<>();
		for (final SE s : found_ses) {
			final long transferID = attempts != null && attempts.intValue() > 0 ? TransferUtils.mirror(lfn, s, removeReplica, attempts.intValue()) : TransferUtils.mirror(lfn, s, removeReplica);
			resmap.put(s.getName(), Long.valueOf(transferID));
		}

		return resmap;
	}

	/**
	 * Get the archive members, if any
	 *
	 * @param archive
	 *            .zip file
	 * @return the file in the same directory with the given file that are members of this zip archive. Can return <code>null</code> if the input is not an archive
	 */
	public static List<LFN> getArchiveMembers(final LFN archive) {
		// archives are only produced by jobs, thus fail fast if the job ID is not set for this file
		if (archive == null || !archive.exists || !archive.isFile() || archive.jobid <= 0 || !archive.isReal())
			return null;

		final List<LFN> ret = new ArrayList<>();

		final LFN parentDir = archive.getParentDir();
		if (parentDir.exists) {
			final Collection<LFN> allJobFiles = find(parentDir.getCanonicalName(), "*", "", LFNUtils.FIND_FILTER_JOBID | LFNUtils.FIND_NO_SORT, null, "", Long.valueOf(archive.jobid), 0);

			if (allJobFiles == null || allJobFiles.size() == 0)
				return null;

			for (final LFN file : allJobFiles)
				if (file.isFile()) {
					final Set<PFN> pfns = file.whereis();

					if (pfns != null)
						for (final PFN p : pfns)
							if (p.pfn.startsWith("guid:/"))
								try {
									// '8' is always an index of first cipher of GUID after "guid:///"
									final UUID guid = UUID.fromString(p.pfn.substring(8, p.pfn.indexOf('?')));

									if (guid.equals(archive.guid)) {
										ret.add(file);
										continue;
									}
								}
								catch (final Exception e) {
									logger.log(Level.WARNING, "Failed to get GUID: " + e);
									return null;
								}
				}
		}
		return ret;
	}

	/**
	 * Get the real file to which the given LFN belongs to. It can be the same file if it exists and has a physical replica or a zip archive containing it, if such an archive can be located.
	 *
	 * @param file
	 *            reference LFN
	 * @return an LFN with physical backing containing the given file, if such an entry can be found, <code>null</code> if not
	 */
	public static LFN getRealLFN(final LFN file) {
		if (file == null || !file.exists)
			return null;

		if (file.isReal())
			return file;

		UUID guid = null;

		String zipMember = null;

		final Set<PFN> listing = file.whereis();
		if (listing != null)
			for (final PFN p : listing)
				if (p.pfn != null && p.pfn.startsWith("guid:/"))
					try {
						guid = UUID.fromString(p.pfn.substring(p.pfn.lastIndexOf('/', p.pfn.indexOf('?')) + 1, p.pfn.indexOf('?')));

						zipMember = p.pfn.substring(p.pfn.indexOf('?') + 1);

						break;
					}
					catch (final Exception e) {
						logger.log(Level.WARNING, "Failed to parse guid ", e);
						return null;
					}

		if (guid == null)
			return null;

		try {
			LFN parentDir = file.getParentDir();

			if (zipMember != null && zipMember.indexOf('/') > 0 && parentDir != null)
				parentDir = parentDir.getParentDir();

			if (parentDir != null && parentDir.list() != null)
				for (final LFN otherFile : parentDir.list())
					if (otherFile.isFile() && otherFile.guid != null && otherFile.guid.equals(guid))
						return otherFile;
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Failed to return real LFN ", e);
		}

		return null;
	}

	/**
	 * Add, extend or remove the expire time for a given LFN.
	 *
	 * @param lfn the LFNs
	 * @param expireTime an ExppireTime object specifying the expire time to add
	 * @param extend specify if the given expire time should extend or replace the current one
	 */
	public static void setExpireTime(final LFN lfn, final ExpireTime expireTime, final boolean extend) {
		Date currentDate;
		Date newExpireTime;
		final Calendar c = Calendar.getInstance();

		if (expireTime == null) {
			newExpireTime = null;
		}
		else {
			if (extend && lfn.expiretime != null) {
				currentDate = lfn.expiretime;
			}
			else {
				currentDate = new Date();
			}

			c.setTime(currentDate);

			c.add(Calendar.YEAR, expireTime.getYears());
			c.add(Calendar.MONTH, expireTime.getMonths());
			c.add(Calendar.WEEK_OF_YEAR, expireTime.getWeeks());
			c.add(Calendar.DAY_OF_YEAR, expireTime.getDays());

			newExpireTime = c.getTime();
		}

		lfn.setExpireTime(newExpireTime);
	}

	/**
	 * Add, extend or remove the expire time for given LFNs.
	 *
	 * @param user who requested the operation to be performed
	 * @param paths paths to the LFNs
	 * @param expireTime an ExppireTime object specifying the expire time to add
	 * @param extend specify if the given expire time should extend or replace the current one
	 */
	public static void setLFNExpireTime(final AliEnPrincipal user, final List<String> paths, final ExpireTime expireTime, final boolean extend) {

		final Set<LFN> lfns = new HashSet<>();

		if (user == null) {
			return;
		}

		for (final String path : paths) {
			final LFN lfn = getLFN(path);

			if (lfn == null) {
				continue;
			}

			final List<LFN> archiveMembers = getArchiveMembers(getRealLFN(lfn));

			if (archiveMembers == null || archiveMembers.isEmpty()) {
				lfns.add(lfn);
			}
			else {
				for (final LFN memberLFN : archiveMembers) {
					lfns.add(memberLFN);
				}
			}

		}

		for (final LFN lfn : lfns) {
			if (!AuthorizationChecker.canWrite(lfn, user)) {
				continue;
			}
			setExpireTime(lfn, expireTime, extend);
		}

	}

	/**
	 * Move the contents of a directory into a separate table.
	 *
	 * @param user who requested the operation to be performed
	 * @param path path to the directory
	 * @return A message to print to the client
	 */
	public static synchronized String moveDirectory(final AliEnPrincipal user, final String path) {
		// check user permissions
		if (user == null || !user.canBecome("admin")) {
			return "The user does not have enough permissions";
		}

		// get LFN
		LFN lfn = getLFN(path);

		if (lfn == null || !lfn.isDirectory()) {
			return "The LFN does not exist or it is not a directory";
		}

		// get DB from the index table entry
		try (DBFunctions db = lfn.indexTableEntry.getDB()) {
			db.setReadOnly(false);

			// check if the directory content is already in a separate table
			String checkExistsQuery = "SELECT indexId from INDEXTABLE WHERE lfn=?";
			if (!db.query(checkExistsQuery, false, lfn.getCanonicalName())) {
				return "DB query failed";
			}

			if (db.moveNext()) {
				return "The directory " + lfn.getCanonicalName() + " is already in a separate table";
			}

			String alterSourceEngineQuery = "ALTER TABLE L" + lfn.indexTableEntry.tableName + "L ENGINE=InnoDB";
			if (!db.query(alterSourceEngineQuery, false)) {
				return "DB query failed:\n" + alterSourceEngineQuery;
			}

			// generate a name for the destination table
			int tableName = (int) (Math.abs((long) (lfn.lfn.hashCode()) % Integer.MAX_VALUE));
			// check if table name exists
			do {
				String tableNameQuery = "SELECT indexId FROM INDEXTABLE WHERE tablename=?";
				if (!db.query(tableNameQuery, false, Integer.valueOf(tableName))) {
					return "DB query failed";
				}

				if (db.moveNext()) {
					tableName++;

					if (tableName <= 0)
						tableName = ThreadLocalRandom.current().nextInt(2, Integer.MAX_VALUE - 2);
				}
				else
					break;
			} while (true);

			// create the destination table
			String createTableQuery = "CREATE TABLE L" + tableName + "L LIKE L" + lfn.indexTableEntry.tableName + "L";
			if (!db.query(createTableQuery, false)) {
				return "DB query failed:\n" + createTableQuery;
			}

			boolean allOk = false;

			final String indexTableEntryQuery;

			try {
				// insert the "" directory in the destination table
				String insertEmptyQuery = "INSERT INTO L" + tableName + "L (owner, lfn, dir, gowner, type, perm) VALUES (?, '', 0, ?, 'd', ?)";
				if (!db.query(insertEmptyQuery, false, lfn.owner, lfn.gowner, lfn.perm)) {
					return "DB query failed:\n" + insertEmptyQuery;
				}

				// get the entryId for the "" directory, normally it is == 1
				int entryId = 1;
				String entryIdQuery = "SELECT entryId FROM L" + tableName + "L WHERE lfn=''";

				if (!db.query(entryIdQuery) || !db.moveNext()) {
					return "DB query failed:\n" + entryIdQuery;
				}

				entryId = db.geti(1);

				try (DBUtils dbu = new DBUtils(db.getConnection())) {
					// lock tables in order to move the files in the given directory
					dbu.lockTables("L" + lfn.indexTableEntry.tableName + "L WRITE, L" + tableName + "L WRITE, INDEXTABLE WRITE");
					// insert directory entries into the destination table
					String newLFN = "substring(lfn, " + (lfn.lfn.length() + 1) + ")";

					String columns = "entryId, owner, replicated, ctime, guidtime, jobid, aclId, lfn, broken, expiretime, size, dir, gowner, type, guid, md5, perm";
					String insertQuery = "INSERT INTO L" + tableName + "L (" + columns
							+ ") SELECT entryId, owner, replicated, ctime, guidtime, jobid, aclId, " + newLFN
							+ " as lfn, broken, expiretime, size, dir, gowner, type, guid, md5, perm FROM L" + lfn.indexTableEntry.tableName + "L WHERE lfn LIKE \""
							+ Format.escSQL(lfn.lfn + "_%") + "\"";

					if (!dbu.executeQuery(insertQuery))
						return "Failed to insert into the newly created table:\n" + insertQuery;

					// update parent directory to point to the "" directory
					String updatedAllQuery = "UPDATE L" + tableName + "L SET dir=" + entryId + " WHERE dir=" + lfn.entryId;

					if (!dbu.executeQuery(updatedAllQuery))
						return "Failed to update the parent directory to the root of the new table:\n" + updatedAllQuery;

					// delete directory entries from the source table
					String deleteQuery = "DELETE FROM L" + lfn.indexTableEntry.tableName + "L WHERE lfn LIKE \"" + Format.escSQL(lfn.lfn + "_%") + "\"";
					if (!dbu.executeQuery(deleteQuery))
						return "Failed to delete the entries from the old table:\n" + deleteQuery;

					// insert lfn into indextable
					indexTableEntryQuery = "INSERT INTO INDEXTABLE (hostIndex, tableName, lfn) VALUES (" + lfn.indexTableEntry.hostIndex + ", " + tableName + ", \""
							+ Format.escSQL(lfn.getCanonicalName()) + "\")";

					if (!dbu.executeQuery(indexTableEntryQuery))
						return "Failed inserting the new table in the INDEXTABLE:\n" + indexTableEntryQuery;

					allOk = dbu.unlockTables();
				}
				catch (Exception e) {
					return "Error executing the DB operations: " + e.getMessage();
				}
			}
			finally {
				if (!allOk) {
					db.query("DROP TABLE L" + tableName + "L");
					return "Operation failed, rolling back the changes";
				}
			}

			String message = "The table L" + tableName + "L has been created successfully";

			for (final Host h : CatalogueUtils.getAllHosts()) {
				if (h.hostIndex != lfn.indexTableEntry.hostIndex)
					try (DBFunctions dbOtherHosts = h.getDB()) {
						if (dbOtherHosts != null)
							if (!dbOtherHosts.query(indexTableEntryQuery)) {
								message = "INDEXTABLE of host " + h.hostIndex
										+ " could not be updated with the same value as the other tables. We are in an inconsistent state! Failed query:\n" + indexTableEntryQuery;

								logger.log(Level.SEVERE, message);
							}
					}
			}

			// update the timestamp where the INDEXTABLE has last been modified
			CatalogueUtils.setIndexTableUpdate();

			return message;
		}
	}

	/**
	 * @param lfns
	 * @return the resolved locations of the given LFNs (the ones that could be found)
	 */
	public static Map<LFN, Set<PFN>> getPFNs(final Collection<LFN> lfns) {
		final List<UUID> uuids = lfns.stream().map((l) -> l.guid).collect(Collectors.toList());

		final Map<UUID, Set<PFN>> pfns = GUIDUtils.getPFNs(uuids.toArray(new UUID[0]));

		final Map<LFN, Set<PFN>> ret = new LinkedHashMap<>(pfns.size());

		for (final LFN l : lfns) {
			final Set<PFN> locations = pfns.get(l.guid);

			if (locations != null)
				ret.put(l, locations);
		}

		return ret;
	}

	/**
	 * @param lfns
	 * @return the resolved physical locations of the given LFNs (the ones that could be found)
	 */
	public static Map<LFN, Set<PFN>> getRealPFNs(final Collection<LFN> lfns) {
		final List<UUID> uuids = lfns.stream().map((l) -> l.guid).collect(Collectors.toList());

		final Map<UUID, Set<PFN>> pfns = GUIDUtils.getRealPFNs(uuids.toArray(new UUID[0]));

		final Map<LFN, Set<PFN>> ret = new LinkedHashMap<>(pfns.size());

		for (final LFN l : lfns) {
			final Set<PFN> locations = pfns.get(l.guid);

			if (locations != null)
				ret.put(l, locations);
		}

		return ret;
	}
}
