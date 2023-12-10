package alien.api.catalogue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.taskQueue.CE;
import alien.catalogue.BookingTable.BOOKING_STATE;
import alien.catalogue.CatalogEntity;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.LFN_CSD;
import alien.catalogue.PFN;
import alien.catalogue.Package;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.io.TransferDetails;
import alien.se.SE;
import alien.shell.ErrNo;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JAliEnCommandcp;
import alien.site.OutputEntry;
import alien.user.AliEnPrincipal;
import utils.ExpireTime;

/**
 *
 * @author ron
 * @since Jun 03, 2011
 */
public class CatalogueApiUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(CatalogueApiUtils.class.getCanonicalName());

	private final JAliEnCOMMander commander;

	/**
	 * @param commander
	 */
	public CatalogueApiUtils(final JAliEnCOMMander commander) {
		this.commander = commander;
	}

	/**
	 * Get LFN from String, only if it exists
	 *
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN object
	 */
	public LFN getLFN(final String slfn) {
		System.out.println("This is getLFN : public LFN getLFN(final String slfn)");
		System.out.println("slfn : " + slfn);
		return getLFN(slfn, false);
	}

	/**
	 * Get LFN from String
	 *
	 * @param slfn
	 * @param evenIfDoesntExist
	 * @return the LFN object, that might exist or not (if <code>evenIfDoesntExist = true</code>)
	 */
	public LFN getLFN(final String slfn, final boolean evenIfDoesntExist) {
				System.out.println("This is getLFN : public LFN getLFN(final String slfn, final boolean evenIfDoesntExist)");
				System.out.println("slfn : " + slfn);
		System.out.println(evenIfDoesntExist);
		final Collection<LFN> ret = getLFNs(Arrays.asList(slfn), false, evenIfDoesntExist);
		System.out.println("Goot ret : "+ret);
		return ret != null && !ret.isEmpty() ? ret.iterator().next() : null;
	}

	/**
	 * Get LFNs from String as a directory listing, only if it exists
	 *
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN objects, or <code>null</code> if failed to get them
	 */
	public List<LFN> getLFNs(final String slfn) {
		try {
			return Dispatcher.execute(new LFNListingfromString(commander.getUser(), slfn)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * @param lfns
	 * @return the mapping of LFN to physical (real) locations of each
	 */
	public Map<LFN, Set<PFN>> getPFNs(final Collection<String> lfns) {
		try {
			return Dispatcher.execute(new RealPFNs(commander.getUser(), lfns)).getMap();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get physical locations of: " + lfns);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get LFN from String
	 *
	 * @param slfn
	 *            name of the LFN
	 * @param ignoreFolders
	 * @param evenIfDoesntExist
	 * @return the list of LFNs, or <code>null</code> if failed to get them
	 */
	public List<LFN> getLFNs(final Collection<String> slfn, final boolean ignoreFolders,
			final boolean evenIfDoesntExist) {
						System.out.println("This is getLFNs : public List<LFN> getLFNs(final Collection<String> slfn, final boolean ignoreFolders,\n" + //
								"\t\t\tfinal boolean evenIfDoesntExist)");
				System.out.println("slfn : " + slfn);
				try {
			System.out.println("Commander ge user : "+commander.getUser());
			return Dispatcher.execute(new LFNfromString(commander.getUser(), ignoreFolders, evenIfDoesntExist, slfn)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get the real file to which the given LFN belongs to. It can be the same file if it exists and has a physical replica or a zip archive containing it, if such an archive can be located.
	 *
	 * @param slfn
	 *            name of the LFN
	 * @return an LFN with physical backing containing the given file, if such an entry can be found, <code>null</code> if not
	 */
	public LFN getRealLFN(final String slfn) {
		try {
			return Dispatcher.execute(new RealLFN(commander.getUser(), slfn)).getRealLFN();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Remove a LFN in the Catalogue
	 *
	 * @param path
	 *            absolute path to the LFN
	 * @return state of the LFN's deletion or <code>null</code>
	 */
	public boolean removeLFN(final String path) {
		try {
			return Dispatcher.execute(new RemoveLFNfromString(commander.getUser(), path, false)).wasRemoved();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the LFN: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * Remove a LFN in the Catalogue
	 *
	 * @param path
	 *            absolute path to the LFN
	 * @param recursive
	 *            <code>true</code> to delete directory's content recursively
	 * @return state of the LFN's deletion or <code>null</code>
	 */
	public boolean removeLFN(final String path, final boolean recursive) {
		try {
			return Dispatcher.execute(new RemoveLFNfromString(commander.getUser(), path, recursive)).wasRemoved();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the LFN: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * Remove a LFN in the Catalogue
	 *
	 * @param path
	 *            absolute path to the LFN
	 * @param recursive
	 *            <code>true</code> to delete directory's content recursively
	 * @param purge
	 *            <code>true</code> to delete a physical copy
	 * @return state of the LFN's deletion or <code>null</code>
	 */
	public boolean removeLFN(final String path, final boolean recursive, final boolean purge) {
		try {
			return Dispatcher.execute(new RemoveLFNfromString(commander.getUser(), path, recursive, purge)).wasRemoved();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the LFN: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * Move a LFN in the Catalogue
	 *
	 * @param path
	 *            absolute path to the LFN
	 * @param newpath
	 *            absolute path to the target
	 * @return state of the LFN's deletion or <code>null</code>
	 */
	public LFN moveLFN(final String path, final String newpath) {
		try {
			return Dispatcher.execute(new MoveLFNfromString(commander.getUser(), path, newpath)).newLFN();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not move the LFN-->newLFN: " + path + "-->" + newpath);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get GUID from String
	 *
	 * @param sguid
	 *            GUID as String
	 * @return the GUID object, or <code>null</code> if failed to get it
	 */
	public GUID getGUID(final String sguid) {
		return getGUID(sguid, false, false);
	}

	/**
	 * Get GUID from String
	 *
	 * @param sguid
	 *            GUID as String
	 * @param evenIfDoesNotExist
	 * @param resolveLFNs
	 *            populate the LFN cache of the GUID object
	 * @return the GUID object, or <code>null</code> if failed to get it
	 */
	public GUID getGUID(final String sguid, final boolean evenIfDoesNotExist, final boolean resolveLFNs) {
		try {
			return Dispatcher.execute(new GUIDfromString(commander.getUser(), sguid, evenIfDoesNotExist, resolveLFNs)).getGUID();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get GUID: " + sguid);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get GUID from String
	 *
	 * @param uuids
	 *            UUIDs to retrieve from the database
	 * @param resolveLFNs
	 *            populate the LFN cache of the GUID object
	 * @return the GUID object, or <code>null</code> if failed to get it
	 */
	public Collection<GUID> getGUIDs(final Collection<UUID> uuids, final boolean resolveLFNs) {
		try {
			return Dispatcher.execute(new GUIDfromUUID(commander.getUser(), uuids, resolveLFNs)).getGUIDs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get GUID: " + uuids);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get PFNs from GUID as String
	 *
	 * @param sguid
	 *            GUID as String
	 * @return the PFNs, or <code>null</code> if failed to get them
	 */
	public Set<PFN> getPFNs(final String sguid) {
		try {
			return Dispatcher.execute(new PFNfromString(commander.getUser(), sguid)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get GUID: " + sguid);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get PFNs for reading by LFN
	 *
	 * @param entity
	 *            LFN or GUID to get access to
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to deprioritize to read from
	 * @return PFNs, filled with read envelopes and credentials if necessary and authorized, or <code>null</code> if failed to get them
	 */
	public List<PFN> getPFNsToRead(final CatalogEntity entity, final List<String> ses, final List<String> exses) {
		try {
			return Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.READ, entity, ses, exses)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFN for: " + entity);
			e.getCause().printStackTrace();

		}
		return null;
	}

	/**
	 * Get PFNs for writing by LFN
	 *
	 * @param lfn
	 *            LFN of the entry as String
	 * @param guid
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to de-prioritize
	 * @param qos
	 *            QoS types and counts to ask for
	 * @return PFNs, filled with write envelopes and credentials if necessary and authorized, or <code>null</code> if failed to get them
	 */
	public List<PFN> getPFNsToWrite(final LFN lfn, final GUID guid, final List<String> ses, final List<String> exses, final HashMap<String, Integer> qos) {
		try {
			return Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getSite(), lfn, guid, ses, exses, qos)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFN for: " + lfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Register PFNs with envelopes
	 *
	 * @param envelopes
	 * @param state the state to register the files in
	 * @return PFNs that were successfully registered or <code>null</code> if failed to register
	 */
	public List<PFN> registerEnvelopes(final List<String> envelopes, final BOOKING_STATE state) {
		try {
			final List<String> encryptedEnvelopes = new LinkedList<>();
			final List<String> signedEnvelopes = new LinkedList<>();

			for (final String envelope : envelopes)
				if (envelope.contains("&signature="))
					signedEnvelopes.add(envelope);
				else
					encryptedEnvelopes.add(envelope);

			final List<PFN> ret = new LinkedList<>();

			if (signedEnvelopes.size() > 0) {
				final List<PFN> signedPFNs = Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), envelopes, state)).getPFNs();

				if (signedPFNs != null && signedPFNs.size() > 0)
					ret.addAll(signedPFNs);
			}

			for (final String envelope : encryptedEnvelopes) {
				final List<PFN> encryptedPFNs = Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), envelope, 0, null, state)).getPFNs();

				if (encryptedPFNs != null && encryptedPFNs.size() > 0)
					ret.addAll(encryptedPFNs);
			}

			return ret;
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFNs for: " + envelopes.toString());
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Register PFNs with envelopes
	 *
	 * @param encryptedEnvelope
	 * @param size
	 * @param md5
	 * @param state what to do with the given files
	 * @return PFNs that were successfully registered or <code>null</code> if failed to register
	 */
	public List<PFN> registerEncryptedEnvelope(final String encryptedEnvelope, final long size, final String md5, final BOOKING_STATE state) {
		try {
			return Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), encryptedEnvelope, size, md5, state)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFNs for: " + encryptedEnvelope);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Create a directory in the Catalogue
	 *
	 * @param path
	 * @return LFN of the created directory, if successful, else <code>null</code>
	 */
	public LFN createCatalogueDirectory(final String path) {
		return createCatalogueDirectory(path, false);
	}

	/**
	 *
	 * @param path
	 * @return LFN of the created file, if successful, else <code>null</code>
	 */
	public LFN touchLFN(final String path) {
		try {
			return Dispatcher.execute(new TouchLFNfromString(commander.getUser(), path)).getLFN();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the file: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Create a directory in the Catalogue
	 *
	 * @param path
	 * @param createNonExistentParents
	 * @return LFN of the created directory, if successful, else <code>null</code>
	 */
	public LFN createCatalogueDirectory(final String path, final boolean createNonExistentParents) {
		try {
			return Dispatcher.execute(new CreateCatDirfromString(commander.getUser(), path, createNonExistentParents)).getDir();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the CatDir: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Remove a directory in the Catalogue
	 *
	 * @param path
	 * @return state of directory's deletion
	 */
	public boolean removeCatalogueDirectory(final String path) {
		try {
			return Dispatcher.execute(new RemoveCatDirfromString(commander.getUser(), path)).wasRemoved();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the CatDir: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * Find an LFN based on pattern
	 *
	 * @param path
	 * @param pattern
	 * @param flags
	 * @return result LFNs or <code>null</code> if no LFNs found
	 */
	public Collection<LFN> find(final String path, final String pattern, final int flags) {
		return find(path, pattern, null, flags, "", Long.valueOf(0), 1000000);
	}

	/**
	 * Find an LFN based on pattern and save to XmlCollection
	 *
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 * @param queryLimit how many LFNs to return at maximum. The server will throw and exception if the set is larger than this value.
	 * @return result LFNs or <code>null</code> if no LFNs found
	 */
	public Collection<LFN> find(final String path, final String pattern, final String query, final int flags, final String xmlCollectionName, final Long queueid, final long queryLimit) {
		try {
			return Dispatcher.execute(new FindfromString(commander.getUser(), path, pattern, query, flags, xmlCollectionName, queueid, queryLimit)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Unable to execute find: path (" + path + "), pattern (" + pattern + "), flags (" + flags + ")");
			e.getCause().printStackTrace();
			commander.setReturnCode(ErrNo.EREMOTEIO, e.getMessage());
		}

		return null;
	}

	/**
	 * Find an LFN based on pattern and save to XmlCollection
	 *
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 * @param queryLimit how many LFNs to return at maximum. The server will throw and exception if the set is larger than this value.
	 * @param readSiteSorting
	 * @return result LFNs or <code>null</code> if no LFNs found
	 */
	public Collection<LFN> find(final String path, final String pattern, final String query, final int flags, final String xmlCollectionName, final Long queueid, final long queryLimit,
			final String readSiteSorting) {
		return find(path, pattern, query, flags, xmlCollectionName, queueid, queryLimit, readSiteSorting, null);
	}

	/**
	 * Find an LFN based on pattern and save to XmlCollection
	 *
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 * @param queryLimit how many LFNs to return at maximum. The server will throw and exception if the set is larger than this value.
	 * @param readSiteSorting
	 * @param excludedPatterns patterns to remove from matching
	 * @return result LFNs or <code>null</code> if no LFNs found
	 */
	public Collection<LFN> find(final String path, final String pattern, final String query, final int flags, final String xmlCollectionName, final Long queueid, final long queryLimit,
			final String readSiteSorting, final Collection<String> excludedPatterns) {
		try {
			return Dispatcher.execute(new FindfromString(commander.getUser(), path, pattern, query, flags, xmlCollectionName, queueid, queryLimit, readSiteSorting, excludedPatterns)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Unable to execute find: path (" + path + "), pattern (" + pattern + "), flags (" + flags + ")");
			e.getCause().printStackTrace();
			commander.setReturnCode(ErrNo.EREMOTEIO, e.getMessage());
		}

		return null;
	}

	/**
	 * Get an SE by its name
	 *
	 * @param se
	 *            name of the SE
	 * @return SE object or <code>null</code> if failed to get it
	 */
	public SE getSE(final String se) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), se)).getSE();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get SE: " + se);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get an SE by its number
	 *
	 * @param seno
	 *            number of the SE
	 * @return SE object or <code>null</code> if failed to get it
	 */
	public SE getSE(final int seno) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), seno)).getSE();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get SE: " + seno);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get the SEs matching the given set of names. The argument can be null or empty, in which case all defined SEs are returned. Invalid SEs are silently skipped, so the returned list could have a
	 * different size (and order) than the given argument.
	 *
	 * @param ses
	 * @return the SEs matching the request or <code>null</code> if failed to get the SE
	 */
	public List<SE> getSEs(final List<String> ses) {
		try {
			return Dispatcher.execute(new ListSEs(commander.getUser(), ses)).getSEs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not list SEs: " + ses);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get the CEs matching the given set of names. The argument can be null or empty, in which case all defined CEs are returned. Invalid SEs are silently skipped, so the returned list could have a
	 * different size (and order) than the given argument.
	 *
	 * @param ceNames
	 * @return the CEs matching the request or <code>null</code> if failed to get the CE
	 */
	public List<CE> getCEs(final List<String> ceNames) {
		try {
			final List<CE> ces = Dispatcher.execute(new ListCEs(commander.getUser(), ceNames)).getCEs();
			return ces;
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not list CEs: " + ceNames);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get Packages for a certain platform
	 *
	 * @param platform
	 * @return the Packages or <code>null</code> if failed to get them
	 */
	public List<Package> getPackages(final String platform) {
		try {
			return Dispatcher.execute(new PackagesfromString(commander.getUser(), platform)).getPackages();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get Packages for: " + platform);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * @param lfn_name
	 * @param username_to_chown
	 * @param groupname_to_chown
	 * @param recursive
	 * @return command result for each lfn or <code>null</code>
	 */
	public HashMap<String, Boolean> chownLFN(final String lfn_name, final String username_to_chown, final String groupname_to_chown, final boolean recursive) {
		if (lfn_name == null || lfn_name.length() == 0)
			return null;

		final LFN lfn = this.getLFN(lfn_name);

		if (lfn == null)
			return null;
		try {
			final ChownLFN cl = Dispatcher.execute(new ChownLFN(commander.getUser(), lfn_name, username_to_chown, groupname_to_chown, recursive));
			if (cl != null)
				return cl.getResults();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not chown " + lfn_name + " for " + username_to_chown);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param lfn_name
	 * @param newMode
	 * @param recursive
	 * @return command result for each lfn or <code>null</code>
	 */
	public HashMap<String, Boolean> chmodLFN(final String lfn_name, final String newMode, final boolean recursive) {
		if (lfn_name == null || lfn_name.length() == 0)
			return null;

		final LFN lfn = this.getLFN(lfn_name);

		if (lfn == null)
			return null;
		try {
			final ChmodLFN cl = Dispatcher.execute(new ChmodLFN(commander.getUser(), lfn_name, newMode, recursive));
			if (cl != null)
				return cl.getResults();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not chmod " + lfn_name + " to " + newMode);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param lfn_name
	 * @param ses
	 * @param exses
	 * @param qos
	 * @param useLFNasGuid
	 * @param attempts
	 * @param removeReplica SE to remove from, after the transfer is successful (making it a `move` operation)
	 * @return command result for each lfn or <code>null</code>
	 */
	public HashMap<String, Long> mirrorLFN(final String lfn_name, final List<String> ses, final List<String> exses, final Map<String, Integer> qos, final boolean useLFNasGuid,
			final Integer attempts, final String removeReplica) {

		if (lfn_name == null || lfn_name.length() == 0)
			throw new IllegalArgumentException("Empty LFN name");

		try {
			HashMap<String, Integer> actualQoS = null;

			if (qos != null) {
				if (qos instanceof HashMap)
					actualQoS = (HashMap<String, Integer>) qos;
				else
					actualQoS = new HashMap<>(qos);
			}

			final MirrorLFN ml = Dispatcher.execute(new MirrorLFN(commander.getUser(), lfn_name, ses, exses, actualQoS, useLFNasGuid, attempts, removeReplica));
			return ml.getResultHashMap();
		}
		catch (final SecurityException e) {
			logger.log(Level.WARNING, e.getMessage());
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param site
	 * @param write
	 * @param lfn
	 * @param qos
	 * @return SE distance list or <code>null</code> if failed to get it
	 */
	public List<HashMap<SE, Double>> listSEDistance(final String site, final boolean write, final String lfn, final String qos) {
		ListSEDistance lsd;
		try {
			lsd = Dispatcher.execute(new ListSEDistance(commander.getUser(), site, write, lfn, qos));
			return (lsd != null ? lsd.getSEDistances() : null);
		}
		catch (final ServerException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param status
	 * @param toSE
	 * @param user
	 * @param id
	 * @param count
	 * @param desc
	 * @return transfer details or <code>null</code> if failed to get them
	 */
	public List<TransferDetails> listTransfer(final String status, final String toSE, final String user, final Long id, final int count, final boolean desc) {

		ListTransfer lt;
		try {
			lt = Dispatcher.execute(new ListTransfer(commander.getUser(), status, toSE, user, id, count, desc));
			return (lt != null ? lt.getTransfers() : null);
		}
		catch (final ServerException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param file
	 * @param isGuid
	 * @param se
	 * @return exit code
	 */
	public int deleteMirror(final String file, final boolean isGuid, final String se) {
		try {
			final DeleteMirror dm = Dispatcher.execute(new DeleteMirror(commander.getUser(), file, isGuid, se));
			return dm.getResult();
		}
		catch (final ServerException e) {
			e.printStackTrace();
			return -100;
		}
	}

	/**
	 * Register the LFNs contained in the OutputEntry in the catalogue
	 *
	 * @param entry
	 * @param outputDir
	 * @param user
	 * @return <code>true</code> if the entry was registered
	 */
	public static boolean registerEntry(final OutputEntry entry, final String outputDir, final AliEnPrincipal user) {
		try {
			return Dispatcher.execute(new RegisterEntry(entry, outputDir, user)).wasRegistered();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not register entry " + entry.getName());
			e.getCause().printStackTrace();
		}
		return false;
	}

	/**
	 * Book the LFNs contained in the OutputEntry in catalogue
	 *
	 * @param entry
	 * @param entry_lfn
	 * @param outputDir
	 * @param user
	 * @return <code>true</code> if no errors
	 */
	public static boolean bookArchiveEntries(final OutputEntry entry, final LFN entry_lfn, final String outputDir, final AliEnPrincipal user) {
		try {
			return Dispatcher.execute(new BookArchiveEntries(entry, entry_lfn, outputDir, user)).wasBooked();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not book entry " + entry.getName());
			e.getCause().printStackTrace();
		}
		return false;
	}

	/**
	 * Upload a local file to the Grid
	 *
	 * @param localFile
	 *            full path to local file
	 * @param toLFN
	 *            catalogue entry name
	 * @param args
	 *            other `cp` command parameters to pass
	 * @throws IOException
	 * @return the uploaded LFN, if everything went ok, <code>null</code> if something didn't work
	 */
	public LFN uploadFile(final File localFile, final String toLFN, final String... args) throws IOException {
		final LFN l = getLFN(toLFN, true);

		if (l.exists)
			throw new IOException("LFN already exists: " + toLFN);

		final String lfnAbsolutePath = FileSystemUtils.getAbsolutePath(commander.getUser().getName(), commander.getCurrentDirName(), toLFN);

		final ArrayList<String> cpArgs = new ArrayList<>();
		cpArgs.add("file:" + localFile.getAbsolutePath());
		cpArgs.add(lfnAbsolutePath);

		if (args != null)
			Collections.addAll(cpArgs, args);

		final JAliEnCommandcp cp = new JAliEnCommandcp(commander, cpArgs);

		return cp.copyLocalToGrid(localFile, lfnAbsolutePath);
	}

	/**
	 * Download a remote file from the Grid
	 *
	 * @param fromLFN
	 *
	 * @param localFile
	 *            full path to local file
	 * @param args
	 *            other `cp` command parameters to pass
	 * @return the indicated or a temporary local file if successful, <code>null</code> if not
	 * @throws IOException
	 */
	public File downloadFile(final String fromLFN, final File localFile, final String... args) throws IOException {
		if (localFile.exists())
			throw new IOException("localFile already exists: " + localFile.getAbsolutePath());

		final String lfnAbsolutePath = FileSystemUtils.getAbsolutePath(commander.getUser().getName(), commander.getCurrentDirName(), fromLFN);

		final ArrayList<String> cpArgs = new ArrayList<>();
		cpArgs.add(lfnAbsolutePath);
		cpArgs.add("file:" + localFile.getAbsolutePath());

		if (args != null)
			Collections.addAll(cpArgs, args);

		final JAliEnCommandcp cp = new JAliEnCommandcp(commander, cpArgs);

		return cp.copyGridToLocal(lfnAbsolutePath, localFile);
	}

	/**
	 * Find files that are members of this archive
	 *
	 * @param archive
	 *
	 * @return list of archive members LFNs or <code>null</code> if failed to get it
	 */
	public List<LFN> getArchiveMembers(final String archive) {
		try {
			return Dispatcher.execute(new GetArchiveMembers(commander.getUser(), archive)).getArchiveMembers();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not return members for " + archive);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * Get LFN_CSDs from String as a listing, only if it exists
	 *
	 * @param slfn
	 *            name of the LFN
	 * @return the LFNCSD objects or <code>null</code> if failed to get them
	 */
	public Collection<LFN_CSD> getLFNCSDs(final String slfn) {
		try {
			return Dispatcher.execute(new LFNCSDListingfromString(commander.getUser(), slfn)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Find an LFNCSD based on pattern and save to XmlCollection
	 *
	 * @param path
	 * @param pattern
	 * @param metadata
	 * @param flags
	 * @return result LFNCSDs or <code>null</code> if failed to get them
	 */
	public Collection<LFN_CSD> find_csd(final String path, final String pattern, final String metadata, final int flags) {
		try {
			return Dispatcher.execute(new FindCsdfromString(commander.getUser(), path, pattern, metadata, flags)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Unable to execute find: path (" + path + "), pattern (" + pattern + "), flags (" + flags + ")");
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Create a directory in the Cassandra catalogue
	 *
	 * @param path
	 * @param createNonExistentParents
	 * @return LFN_CSD of the created directory, if successful, else <code>null</code>
	 */
	public LFN_CSD createCatalogueDirectoryCsd(final String path, final boolean createNonExistentParents) {
		try {
			return Dispatcher.execute(new CreateCsdCatDirfromString(commander.getUser(), path, createNonExistentParents)).getDir();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the CatDir: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 *
	 * @param path
	 * @return LFNCSD of the created file, if successful, else <code>null</code>
	 */
	public LFN_CSD touchLFNCSD(final String path) {
		try {
			return Dispatcher.execute(new TouchLFNCSDfromString(commander.getUser(), path)).getLFN();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the file: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get LFN_CSDs from collection
	 *
	 * @param slfn
	 * @return the LFNCSD objects
	 */
	public Collection<LFN_CSD> getLFNCSD(final Collection<String> slfn) {
		return getLFNCSD(slfn, false);
	}

	/**
	 * Get LFN_CSD from String
	 *
	 * @param lfn_path
	 * @return the LFNCSD object or <code>null</code> if failed to get it
	 */
	public LFN_CSD getLFNCSD(final String lfn_path) {
		final ArrayList<String> slfn = new ArrayList<>();
		slfn.add(lfn_path);
		final Collection<LFN_CSD> slfn_res = getLFNCSD(slfn, false);

		if (slfn_res != null && slfn_res.iterator().hasNext())
			return slfn_res.iterator().next();

		return null;
	}

	/**
	 * Get LFN_CSDs from collection, potentially using UUIDs
	 *
	 * @param slfn
	 * @param lfns_are_uuids
	 * @return the LFNCSD objects or <code>null</code> if failed to get them
	 */
	public Collection<LFN_CSD> getLFNCSD(final Collection<String> slfn, final boolean lfns_are_uuids) {
		try {
			return Dispatcher.execute(new LFNCSDfromString(commander.getUser(), true, false, lfns_are_uuids, slfn)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFNs: " + slfn.toString());
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Move a LFNCSD in the catalogue
	 *
	 * @param path
	 *            absolute path to the LFN
	 * @param newpath
	 *            absolute path to the target
	 * @return code of the LFNCSD mv
	 */
	public int moveLFNCSD(final String path, final String newpath) {
		try {
			return Dispatcher.execute(new MoveLFNCSDfromString(commander.getUser(), path, newpath)).getMvCode();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not move the LFN-->newLFN: " + path + "-->" + newpath);
			e.getCause().printStackTrace();
		}

		return -1;
	}

	/**
	 * Get GUID from String
	 *
	 * @param uuid
	 *            UUID as String
	 * @return the LFNCSD object or <code>null</code> if failed to get it
	 */
	public LFN_CSD guid2lfncsd(final String uuid) {
		try {
			return Dispatcher.execute(new LFNCSDfromUUIDString(commander.getUser(), uuid)).getLFNCSD();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get UUID: " + uuid);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * @param lfn_name
	 * @param username_to_chown
	 * @param groupname_to_chown
	 * @param recursive
	 * @return command result for each lfncsd
	 */
	public boolean chownLFNCSD(final String lfn_name, final String username_to_chown, final String groupname_to_chown, final boolean recursive) {
		if (lfn_name == null || lfn_name.length() == 0)
			return false;

		// final LFN_CSD lfn = this.getLFNCSD(lfn_name);
		//
		// if (lfn == null || !lfn.exists)
		// return false;
		try {
			final ChownLFNCSD cl = Dispatcher.execute(new ChownLFNCSD(commander.getUser(), lfn_name, username_to_chown, groupname_to_chown, recursive));
			if (cl != null)
				return cl.getSuccess();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not chown " + lfn_name + " for " + username_to_chown);
			e.getCause().printStackTrace();
		}
		return false;
	}

	/**
	 * Get PFNs for reading by LFNCSD
	 *
	 *
	 * @param entity
	 *            LFN or GUID to get access to
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to deprioritize to read from
	 * @return PFNs, filled with read envelopes and credentials if necessary and authorized or <code>null</code> if failed to get them
	 */
	public List<PFN> getPFNsToReadCsd(final CatalogEntity entity, final List<String> ses, final List<String> exses) {
		try {
			return Dispatcher.execute(new PFNforReadOrDelCsd(commander.getUser(), commander.getSite(), AccessType.READ, entity, ses, exses)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFN for: " + entity);
			e.getCause().printStackTrace();

		}
		return null;
	}

	/**
	 * Register the output of a given job ID
	 *
	 * @param jobId job ID to register the output of
	 * @return the list of booked (so written but not yet committed) LFNs for this (normally failed) job ID or <code>null</code> if failed to get them
	 */
	public Collection<LFN> registerOutput(final long jobId) {
		try {
			return Dispatcher.execute(new RegisterOutput(commander.getUser(), jobId)).getRegisteredLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get booked LFNs for job ID " + jobId);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param seNumber
	 * @param fileCount
	 * @return at most <code>fileCount</code> random PFNs that should be present on this SE
	 */
	public Collection<PFN> getRandomPFNsFromSE(final int seNumber, final int fileCount) {
		try {
			return Dispatcher.execute(new SERandomPFNs(commander.getUser(), seNumber, fileCount)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get random PFNs for SE with number" + seNumber);
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param paths the paths to the LFNs
	 * @param expireTime an ExppireTime object specifying the expire time to add
	 * @param extend specify if the given expire time should extend or replace the current one
	 */
	public void setLFNExpireTime(final List<String> paths, final ExpireTime expireTime, final boolean extend) {
		try {
			Dispatcher.execute(new LFNExpireTime(commander.getUser(), paths, expireTime, extend));
		}
		catch (final ServerException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param path the path to the directory
	 * @return the result of this operation on the server side
	 */
	public String moveDirectory(final String path) {
		try {
			return Dispatcher.execute(new MoveDirectory(commander.getUser(), path)).getResponse();
		}
		catch (final ServerException e) {
			e.printStackTrace();
			return "Error executing the command";
		}

	}
}
