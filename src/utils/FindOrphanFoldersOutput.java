package utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;

import alien.catalogue.CatalogueUtils;
import alien.catalogue.IndexTableEntry;
import alien.taskQueue.JDL;
import lazyj.DBFunctions;

/**
 * @author mmmartin
 *
 */
public class FindOrphanFoldersOutput {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/utils/FindOrphanFoldersOutput <masterjobid1> [<masterjobid2> <masterjobid3>...]");
			System.exit(-1);
		}

		for (int i = 0; i < nargs; i++) {
			long jobid = Long.parseLong(args[i]);
			JDL jobjdl = null;

			try {
				jobjdl = new JDL(jobid);
			} catch (IOException e) {
				System.err.println("Can't get job JDL: " + e);
				return;
			}

			String outputdir = jobjdl.getOutputDir();

			IndexTableEntry ite = CatalogueUtils.getClosestMatch(outputdir);

			if (ite == null) {
				System.err.println("Can't find indextable for output: " + outputdir);
				return;
			}

			String dblfn = outputdir.replaceAll(ite.lfn, "");

			System.out.println("db pattern: " + dblfn);

			try (DBFunctions db = ite.getDB()) {
				db.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);

				System.out.println("Going to select distinct dir");

				if (!db.query("select distinct dir as dir from L" + ite.tableName + "L where type='d' and lfn like '" + dblfn + "/%/' order by 1 asc")) {
					System.err.println("Can't get distinct dirs");
					System.exit(-1);
				}
				int count = db.count();

				if (count <= 1) {
					System.out.println("Only 1 dir found");
				}

				System.out.println("Found several folders: " + count);

				db.moveNext();
				int mindir = db.geti("dir");

				System.out.println("select lfn from L" + ite.tableName + "L where type='d' and dir=" + mindir + " order by 1 asc");

				if (!db.query("select lfn from L" + ite.tableName + "L where type='d' and dir=" + mindir + " order by 1 asc")) {
					System.err.println("Can't get distinct dirs");
					System.exit(-1);
				}

				try (PrintWriter outfile = new PrintWriter(new FileOutputStream(jobid + ".dirs"))) {
					while (db.moveNext()) {
						System.out.println(ite.lfn + db.gets("lfn"));
						outfile.println(ite.lfn + db.gets("lfn"));
					}
				} catch (FileNotFoundException e) {
					System.err.println("Could not write to file for: " + jobid + " :" + e);
					return;
				}
			}
		}
	}
}
