package alien.catalogue.access;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.Format;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author ron
 * 
 */
public class XrootDEnvelope implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1024787790575833398L;

	/**
	 * the order the key-vals have to appear for sign and verify
	 */
	public static final String hashord = "turl-xurl-access-lfn-guid-zguid-size-md5-se";

	/**
	 * the access ticket this envelope belongs to
	 */
	public AccessType type = null;

	/**
	 * pfn of the file on the SE (proto:://hostdns:port//storagepath)
	 */
	public final PFN pfn;

	/**
	 * A LFN that is pointing to this envelope's GUID/PFN us as a guid://
	 * archive link
	 */
	private LFN archiveAnchorLFN;

	/**
	 * Signed transaction url
	 */
	protected String turl;

	/**
	 * Signed envelope
	 */
	protected String signedEnvelope;

	/**
	 * UnSigned envelope
	 */
	protected String unSignedEnvelope;

	/**
	 * UnEncrypted envelope
	 */
	protected String unEncryptedEnvelope;

	/**
	 * Encrypted envelope
	 */
	protected String encryptedEnvelope;

	/**
	 * @param type
	 * @param pfn
	 */
	public XrootDEnvelope(final AccessType type, final PFN pfn) {
		this.type = type;
		this.pfn = pfn;
		setUnsignedEnvelope();
		setUnEncryptedEnvelope();
	}

	/**
	 * Create a signed only envelope in order to verify it
	 * 
	 * @param envelope
	 */
	public XrootDEnvelope(String envelope) {

		StringTokenizer st = new StringTokenizer(envelope, "\\&");
		String spfn = "";
		turl = "";
		String lfn = "";
		String guid = "";
		String se = "";
		int size = 0;
		String md5 = "";

		while (st.hasMoreTokens()) {
			String tok = st.nextToken();

			int idx = tok.indexOf('=');

			if (idx >= 0) {
				String key = tok.substring(0, idx);
				String value = tok.substring(idx + 1);

				if ("access".equals(key))
					if (value.startsWith("write")) {
						type = AccessType.WRITE;
					} else if (value.equals("read")) {
						type = AccessType.READ;
					} else if (value.equals("delete")) {
						type = AccessType.DELETE;
					} else {
						System.err.println("illegal access type!");
					}
				else if ("turl".equals(key))
					turl = value;
				else if ("pfn".equals(key))
					spfn = value;
				else if ("lfn".equals(key))
					lfn = value;
				else if ("guid".equals(key))
					guid = value;
				else if ("size".equals(key))
					size = Integer.parseInt(value);
				else if ("md5".equals(key))
					md5 = value;
				else if ("se".equals(key))
					se = value;
			}
		}
		GUID g = GUIDUtils.getGUID(UUID.fromString(guid), true);

		g.md5 = md5;
		g.size = size;
		if (turl.endsWith(spfn))
			spfn = turl;
		else {
			// turl has #archive
			if (turl.contains("#"))
				turl = turl.substring(0, turl.indexOf('#'));
			// turl has LFN rewrite for dCache etc
			if (turl.endsWith(lfn))
				turl = turl.replace(lfn, spfn);
		}

		this.pfn = new PFN(spfn, g, SEUtils.getSE(se));

		unSignedEnvelope = envelope;
	}

	/**
	 * Set the LFN that is pointing to this envelope's GUID/PFN us as a guid://
	 * archive link
	 * 
	 * @param anchor
	 *            Anchor LFN
	 */
	public void setArchiveAnchor(final LFN anchor) {
		archiveAnchorLFN = anchor;
	}

	/**
	 * @return envelope xml
	 */
	public String getUnEncryptedEnvelope() {
		return unEncryptedEnvelope;
	}

	/**
	 * set envelope
	 */
	public void setUnEncryptedEnvelope() {

		final String access = type.toString().replace("write", "write-once");

		String sPFN = pfn.getPFN();

		final SE se = SEUtils.getSE(pfn.seNumber);

		String sStoragePrefix = se.generateProtocol();

		if (sPFN.startsWith(sStoragePrefix)) {
			sPFN = sPFN.substring(sStoragePrefix.length());
		} else {
			final String[] pfnsplit = pfn.getPFN().split("//");

			if (pfnsplit.length < 3) {
				System.err.println("Split is not ok : " + pfnsplit.length
						+ " for " + pfn.getPFN());
				// return null;
			}

			sPFN = "/" + pfnsplit[2];
		}

		final GUID guid = pfn.getGuid();

		final Set<LFN> lfns = guid.getLFNs();

		String ret = "<authz>\n  <file>\n" + "    <access>" + access
				+ "</access>\n";

		String sturl = pfn.getPFN();
		if (archiveAnchorLFN != null)
			sturl += "#" + archiveAnchorLFN.getFileName();

		ret += "    <turl>" + Format.escHtml(sturl) + "</turl>\n";

		LFN refLFN = null;
		GUID refGUID = guid;

		if (archiveAnchorLFN != null) {
			refGUID = GUIDUtils.getGUID(archiveAnchorLFN.guid);
			refLFN = archiveAnchorLFN;
		} else if (lfns != null && lfns.size() > 0)
			refLFN = lfns.iterator().next();

		if (refLFN != null)
			ret += "    <lfn>" + Format.escHtml(refLFN.getCanonicalName())
					+ "</lfn>\n";
		else
			ret += "    <lfn>/NOLFN</lfn>\n";

		ret += "    <size>" + refGUID.size + "</size>" + "\n" + "    <guid>"
				+ Format.escHtml(refGUID.getName().toUpperCase()) + "</guid>\n"
				+ "    <md5>" + Format.escHtml(refGUID.md5) + "</md5>\n"
				+ "    <pfn>" + Format.escHtml(sPFN) + "</pfn>\n" + "    <se>"
				+ Format.escHtml(se.getName()) + "</se>\n"
				+ "  </file>\n</authz>\n";

		unEncryptedEnvelope = ret;
	}

	/**
	 * Splitter of PFNs
	 */
	public static final Pattern PFN_EXTRACT = Pattern
			.compile("^\\w+://([\\w-]+(\\.[\\w-]+)*(:\\d+))?/(.*)$");

	/**
	 * @return URL of the storage. This is passed as argument to xrdcp and in
	 *         most cases it is the PFN but for DCACHE it is a special path ...
	 */
	public String getTransactionURL() {

		return turl;
	}

	/**
		 *
		 */
	public void setTransactionURL() {
		final SE se = SEUtils.getSE(pfn.seNumber);

		if (se.seName.indexOf("DCACHE") > 0) {
			final GUID guid = pfn.getGuid();

			final Set<LFN> lfns = guid.getLFNs();

			if (lfns != null && lfns.size() > 0)
				turl = se.seioDaemons + "/"
						+ lfns.iterator().next().getCanonicalName();
			else
			turl =  se.seioDaemons + "//NOLFN";
		} else {

			final Matcher m = PFN_EXTRACT.matcher(pfn.pfn);

			if (m.matches()) {
				if (archiveAnchorLFN != null)
					turl =  se.seioDaemons + "/" + m.group(4) + "#"
							+ archiveAnchorLFN.getFileName();
				else
					turl =  se.seioDaemons + "/" + m.group(4);
			}
			if (archiveAnchorLFN != null)
				turl =  pfn.pfn + "#" + archiveAnchorLFN.getFileName();
			else 
			turl =  pfn.pfn;
		}
	}

	/**
	 * @return url envelope
	 */
	public String getUnsignedEnvelope() {
		return unSignedEnvelope;
	}

	/**
	 * set url envelope
	 */
	public void setUnsignedEnvelope() {
		
		setTransactionURL();

		final GUID guid = pfn.getGuid();

		final Set<LFN> lfns = guid.getLFNs();

		final SE se = SEUtils.getSE(pfn.seNumber);

		HashMap<String, String> e = new HashMap<String, String>(8);

		e.put("turl", pfn.getPFN());
		if (archiveAnchorLFN != null)
			e.put("turl", pfn.getPFN() + "#" + archiveAnchorLFN.getFileName());

		e.put("access", type.toString());

		e.put("lfn", "/NOLFN");

		if (archiveAnchorLFN != null)
			e.put("lfn", archiveAnchorLFN.getCanonicalName());
		else if (lfns != null && lfns.size() > 0)
			e.put("lfn", lfns.iterator().next().getCanonicalName());

		if (archiveAnchorLFN == null) {
			e.put("guid", guid.getName());
			e.put("size", String.valueOf(guid.size));
			e.put("md5", guid.md5);

		} else {
			GUID archiveAnchorGUID = GUIDUtils.getGUID(archiveAnchorLFN.guid);
			e.put("zguid", guid.getName());
			e.put("guid", archiveAnchorGUID.getName());
			e.put("size", String.valueOf(archiveAnchorGUID.size));
			e.put("md5", archiveAnchorGUID.md5);

		}
		if ("alice::cern::setest".equals(se.getName().toLowerCase()))
			e.put("se", "alice::cern::testse");
		else
			e.put("se", se.getName());

		e.put("xurl", addXURLForSpecialSEs(e.get("lfn")));

		StringTokenizer hash = new StringTokenizer(hashord, "-");

		String ret = "";
		String usedHashOrd = "";

		System.out.println("Creating unsigned envelope:");

		while (hash.hasMoreTokens()) {
			String key = hash.nextToken();
			if (e.get(key) != null) {
				ret += key + "=" + e.get(key) + "&";
				usedHashOrd += key + "-";
				System.out.println(key + ": " + e.get(key));
			}
		}

		ret += "hashord=" + usedHashOrd + "hashord";

		unSignedEnvelope = ret;
	}

	private String addXURLForSpecialSEs(String lfn) {

		SE se = SEUtils.getSE(pfn.seNumber);

		// $se =~ /dcache/i
		// $se =~ /alice::((RAL)|(CNAF))::castor/i
		// $se =~ /alice::RAL::castor2_test/i
		if ((se.seName.toLowerCase()).contains("dcache"))
			return se.seioDaemons + "/" + lfn;
		return null;
	}

	/**
	 * @param signedEnvelope
	 */
	public void setSignedEnvelope(String signedEnvelope) {
		this.signedEnvelope = signedEnvelope;
	}

	/**
	 * @return the signed envelope
	 */
	public String getSignedEnvelope() {
		return signedEnvelope;
	}

	/**
	 * @param encryptedEnvelope
	 */
	public void setEncryptedEnvelope(String encryptedEnvelope) {
		this.encryptedEnvelope = encryptedEnvelope;
	}

	/**
	 * @return encrypted envelope
	 */
	public String getEncryptedEnvelope() {
		return encryptedEnvelope;
	}
}
