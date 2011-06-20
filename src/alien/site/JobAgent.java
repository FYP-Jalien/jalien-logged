package alien.site;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.JobSigner;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;

/**
 * @author ron
 * @since June 5, 2011
 */
public class JobAgent extends Thread {

	private static final String tempDirPrefix = "jAliEn.JobAgent.tmp";
	private File tempDir = null;

	private static final String defaultOutputDirPrefix = "~/jalien-job-";

	private JDL jdl = null;
	private Job job = null;

	private final AliEnPrincipal user;
	
	private static final String site = null;	// TODO : how to determine the site where we currently run ?
	
	/**
	 * @param job
	 */
	public JobAgent(Job job) {
		this.job = job;
		
		this.user = UserFactory.getByUsername(job.getOwner());
		
		try {
			jdl = new JDL(job.getJDL());
			
			
		} catch (IOException e) {
			System.err.println("Unable to get JDL from Job.");
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		if(verifiedJob()){
		TaskQueueApiUtils.setJobStatus(job.queueId, "STARTED");
		if (createTempDir())
			if (getInputFiles()) {
				if (execute())
					if (uploadOutputFiles())
						System.out.println("Job sucessfully executed.");
			} else {
				System.out.println("Could not get input files.");
				TaskQueueApiUtils.setJobStatus(job.queueId, "ERROR_IB");
			}
		} else {
			TaskQueueApiUtils.setJobStatus(job.queueId, "ERROR_VER");
		}

	}

	private boolean verifiedJob(){
		try {
			return JobSigner.verifyEnvelope(jdl.getPlainJDL());
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (SignatureException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	
	private boolean getInputFiles() {

		boolean gotAllInputFiles = true;

		for (String slfn : jdl.getInputFiles()) {
			File localFile;
			try {
				localFile = new File(tempDir.getCanonicalFile() + "/"
						+ slfn.substring(slfn.lastIndexOf('/') + 1));

				System.out.println("Getting input file into local file: "
						+ tempDir.getCanonicalFile() + "/"
						+ slfn.substring(slfn.lastIndexOf('/') + 1));

				System.out.println("Getting input file: " + slfn);
				LFN lfn = CatalogueApiUtils.getLFN(slfn);
				System.out.println("Getting input file lfn: " + lfn);
				List<PFN> pfns = CatalogueApiUtils.getPFNsToRead(user, site, lfn, null, null);
				System.out.println("Getting input file pfns: " + pfns);

				for (PFN pfn : pfns) {

					List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
					for (final Protocol protocol : protocols) {

						localFile = protocol.get(pfn, localFile);
						break;

					}
					System.out.println("Suppossed to have input file: "
							+ localFile.getCanonicalPath());
				}
				if (!localFile.exists())
					gotAllInputFiles = false;
			} catch (IOException e) {
				e.printStackTrace();
				gotAllInputFiles = false;
			}
		}
		return gotAllInputFiles;

	}

	private boolean execute() {

		boolean ran = true;

		LinkedList<String> command = (LinkedList<String>) jdl.getExecutable();
		if (jdl.getArguments() != null)
			command.addAll(jdl.getArguments());

		System.out.println("we will run: " + command.toString());
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
				command);

		pBuilder.returnOutputOnExit(true);

		pBuilder.directory(tempDir);

		pBuilder.timeout(24, TimeUnit.HOURS);

		pBuilder.redirectErrorStream(true);

		try {
			final ExitStatus exitStatus;

			TaskQueueApiUtils.setJobStatus(job.queueId, "RUNNING");

			exitStatus = pBuilder.start().waitFor();

			if (exitStatus.getExtProcExitStatus() == 0) {

				BufferedWriter out = new BufferedWriter(new FileWriter(
						tempDir.getCanonicalFile() + "/stdout"));
				out.write(exitStatus.getStdOut());
				out.close();
				BufferedWriter err = new BufferedWriter(new FileWriter(
						tempDir.getCanonicalFile() + "/stderr"));
				err.write(exitStatus.getStdErr());
				err.close();

				System.out.println("we ran, stdout+stderr should be there now.");
			}

			System.out.println("A local cat on stdout: " + exitStatus.getStdOut());
			System.out.println("A local cat on stderr: " + exitStatus.getStdErr());

		} catch (final InterruptedException ie) {
			System.err
					.println("Interrupted while waiting for the following command to finish : "
							+ command.toString());
			ran = false;
		} catch (IOException e) {
			ran = false;
		}
		return ran;
	}

	private boolean uploadOutputFiles() {

		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;
		TaskQueueApiUtils.setJobStatus(job.queueId, "SAVING");

		String outputDir = jdl.getOutputDir();

		if (outputDir == null)
			outputDir = defaultOutputDirPrefix + job.queueId;

		System.out.println("QueueID: " + job.queueId);

		System.out.println("Full catpath of outDir is: "
				+ FileSystemUtils.getAbsolutePath(
						user.getName(), "~", outputDir));

		if (CatalogueApiUtils.getLFN(FileSystemUtils.getAbsolutePath(user.getName(), null, outputDir)) != null) {
			System.err.println("OutputDir [" + outputDir + "] already exists.");
			return false;
		}

		LFN outDir = CatalogueApiUtils.createCatalogueDirectory(user, outputDir);

		if (outDir == null) {
			System.err.println("Error creating the OutputDir [" + outputDir
					+ "].");
			uploadedAllOutFiles = false;
		}
		else{
			for (String slfn : jdl.getOutputFiles()) {
				File localFile;
				try {
					localFile = new File(tempDir.getCanonicalFile() + "/"
							+ slfn);

					if (localFile.exists() && localFile.isFile()
							&& localFile.canRead() && localFile.length() > 0) {

						long size = localFile.length();
						if (size <= 0) {
							System.err.println("Local file has size zero: "
									+ localFile.getAbsolutePath());
						}
						String md5 = null;
						try {
							md5 = FileSystemUtils.calculateMD5(localFile);
						} catch (Exception e1) {
							// ignore, treated below
						}
						
						if (md5 == null) {
							System.err.println("Could not calculate md5 checksum of the local file: " + localFile.getAbsolutePath());
						}

						List<PFN> pfns = null;

						LFN lfn = null;
						lfn = CatalogueApiUtils.getLFN(outDir.getCanonicalName() + slfn, true);

						lfn.size = size;
						lfn.md5 = md5;

						pfns = CatalogueApiUtils.getPFNsToWrite(user, site, lfn, null, null, null, 0);

						if (pfns != null) {
							ArrayList<String> envelopes = new ArrayList<String>(
									pfns.size());
							for (PFN pfn : pfns) {

								List<Protocol> protocols = Transfer
										.getAccessProtocols(pfn);
								for (final Protocol protocol : protocols) {

									envelopes.add(protocol.put(pfn, localFile));
									break;

								}

							}

							// drop the following three lines once put replies
							// correctly
							// with the signed envelope
							envelopes.clear();
							for (PFN pfn : pfns)
								envelopes.add(pfn.ticket.envelope
										.getSignedEnvelope());

							List<PFN> pfnsok = CatalogueApiUtils.registerEnvelopes(user, envelopes);
							if (!pfns.equals(pfnsok)) {
								if (pfnsok != null && pfnsok.size() > 0) {
									System.out.println("Only " + pfnsok.size()
											+ " could be uploaded");
									uploadedNotAllCopies = true;
								} else {

									System.err.println("Upload failed, sorry!");
									uploadedAllOutFiles = false;
									break;
								}
							}
						} else {
							System.out.println("Couldn't get write envelopes for output file"); 
						}
					} else {
						System.out.println("Can't upload output file "
								+ localFile.getName()
								+ ", does not exist or has zero size.");
					}

				} catch (IOException e) {
					e.printStackTrace();
					uploadedAllOutFiles = false;
				}
			}
		}
		
		if (uploadedNotAllCopies)
			TaskQueueApiUtils.setJobStatus(job.queueId, "DONE_WARN");
		else if (uploadedAllOutFiles)
			TaskQueueApiUtils.setJobStatus(job.queueId, "DONE");
		else
			TaskQueueApiUtils.setJobStatus(job.queueId, "ERROR_SV");

		return uploadedAllOutFiles;
	}

	private boolean createTempDir() {

		String tmpDirStr = System.getProperty("java.io.tmpdir");
		if (tmpDirStr == null) {
			System.err
					.println("System temp dir config [java.io.tmpdir] does not exist.");
			return false;
		}

		File tmpDir = new File(tmpDirStr);
		if (!tmpDir.exists()) {
			boolean created = tmpDir.mkdirs();
			if (!created) {
				System.err
						.println("System temp dir [java.io.tmpdir] does not exist and can't be created.");
				return false;
			}
		}

		int suffix = (int) System.currentTimeMillis();
		int failureCount = 0;
		do {
			tempDir = new File(tmpDir, tempDirPrefix + suffix % 10000);
			suffix++;
			failureCount++;
		} while (tempDir.exists() && failureCount < 50);

		if (tempDir.exists()) {
			System.err.println("Could not create temporary directory.");
			return false;
		}
		boolean created = tempDir.mkdir();
		if (!created) {
			System.err.println("Could not create temporary directory.");
			return false;
		}

		return true;
	}

}
