package alien.shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import utils.SystemProcess;

/**
 * @author ron
 * @since November 11, 2011
 */
public class FileEditor {
	/**
	 * Available editors
	 */
	public static final String[] editors = { "sensible-editor", "edit", "mcedit", "vim", "joe", "emacs", "more", "less", "nano" };

	private static final Map<String, String> editorCommands = new TreeMap<>();

	static {
		for (final String editor : editors) {
			final String path = which(editor);

			if (path != null)
				editorCommands.put(editor, path);
		}
	}

	private final String editor;

	public static final List<String> getAvailableEditorCommands() {
		final List<String> ret = new ArrayList<>();

		ret.addAll(editorCommands.keySet());

		return ret;
	}

	/**
	 * @param editorname
	 * @throws IOException
	 */
	public FileEditor(final String editorname) throws IOException {
		editor = editorCommands.get(editorname);

		if (editor == null)
			throw new IOException(editorname + ": command not found");
	}

	/**
	 * @param filename
	 */
	public void edit(final String filename) {
		final SystemProcess edit = new SystemProcess(editor + " " + filename);

		edit.execute();

		try {
			edit.wait();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} catch (final IllegalMonitorStateException e) {
			// ignore
		}
	}

	private static String which(final String command) {
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(Arrays.asList("which", command));

		pBuilder.returnOutputOnExit(true);

		pBuilder.timeout(7, TimeUnit.SECONDS);
		try {
			final ExitStatus exitStatus = pBuilder.start().waitFor();

			if (exitStatus.getExtProcExitStatus() == 0)
				return exitStatus.getStdOut().trim();

		} catch (final Exception e) {
			// ignore
		}

		// System.err.println("Command [" + command + "] not found.");
		return null;
	}
}
