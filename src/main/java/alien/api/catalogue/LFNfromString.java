package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the LFN object for this path
 *
 * @author costing
 * @since 2011-03-04
 */
public class LFNfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -1720547988105993480L;

	private final Collection<String> path;
	private final boolean ignoreFolders;
	private final boolean evenIfDoesntExist;

	private List<LFN> lfns = null;

	/**
	 * @param user
	 * @param paths
	 * @param ignoreFolders
	 * @param evenIfDoesntExist
	 *
	 */
	public LFNfromString(final AliEnPrincipal user, final boolean ignoreFolders, final boolean evenIfDoesntExist,
			final Collection<String> paths) {
		System.out.println("This is LFNfromString Class Constrcutor");
		setRequestUser(user);
		System.out.println("Request User is : "+user);
		this.path = paths;
		System.out.println("Path is : "+paths);
		this.ignoreFolders = ignoreFolders;
		this.evenIfDoesntExist = evenIfDoesntExist;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path != null ? this.path.toString() : null, String.valueOf(this.ignoreFolders), String.valueOf(this.evenIfDoesntExist));
	}

	@Override
	public void run() {
		System.out.println("This is LFNfromString Class run() method");
		if (evenIfDoesntExist) {
			for (final String s : path) {
				final LFN l = LFNUtils.getLFN(s, evenIfDoesntExist);

				if (l != null && !(l.isDirectory() && ignoreFolders)) {
					if (this.lfns == null)
						this.lfns = new ArrayList<>();

					this.lfns.add(l);
				}
			}
		}
		else
			this.lfns = LFNUtils.getLFNs(ignoreFolders, path);
	}

	/**
	 * @return the requested LFN
	 */
	public List<LFN> getLFNs() {
		return this.lfns;
	}

	/**
	 * @return an LFN with physical backing containing the given file, if such an entry can be found, <code>null</code> if not
	 */
	public List<LFN> getRealLFNs() {
		final List<LFN> ret = new ArrayList<>();
		final Iterator<LFN> it = this.lfns.iterator();
		while (it.hasNext())
			ret.add(LFNUtils.getRealLFN(it.next()));

		return ret;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + " (" + this.ignoreFolders + "), reply is: " + this.lfns;
	}
}
