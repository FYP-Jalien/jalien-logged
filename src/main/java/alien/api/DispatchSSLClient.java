package alien.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;

/**
 * @author costing
 *
 */
public class DispatchSSLClient extends Thread {

	/**
	 * Reset the object stream every this many objects sent
	 */
	private static final int RESET_OBJECT_STREAM_COUNTER = 1000;

	private int objectsSentCounter = 0;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(DispatchSSLClient.class.getCanonicalName());

	private static final int defaultPort = 5282;
	private static final String defaultHost = "localhost";
	private static String serviceName = "apiService";

	private static String addr = null;
	private static int port = 0;

	private final Socket connection;

	private final ObjectInputStream ois;
	private final ObjectOutputStream oos;

	private final OutputStream os;

	/**
	 * E.g. the CE proxy should act as a fowarding bridge between JA and central services
	 *
	 * @param servName
	 *            name of the config parameter for the host:port settings
	 */
	public static void overWriteServiceAndForward(final String servName) {
		// TODO: we could drop the serviceName overwrite, once we assume to run
		// not on one single host everything
		serviceName = servName;
	}

	/**
	 * @param connection
	 * @throws IOException
	 */
	protected DispatchSSLClient(final Socket connection) throws IOException {

		this.connection = connection;

		connection.setTcpNoDelay(true);
		connection.setTrafficClass(0x10);

		this.ois = new ObjectInputStream(connection.getInputStream());

		this.os = connection.getOutputStream();

		this.oos = new ObjectOutputStream(this.os);
		this.oos.flush();
	}

	@Override
	public String toString() {
		return this.connection.getInetAddress().toString();
	}

	@Override
	public void run() {
		// check
	}

	private static HashMap<Integer, DispatchSSLClient> instance = new HashMap<>(20);

	/**
	 * @param address
	 * @param p
	 * @return instance
	 * @throws IOException
	 */
	public static DispatchSSLClient getInstance(final String address, final int p) throws IOException {

		final Integer portNo = Integer.valueOf(p);

		if (instance.get(portNo) == null) {
			// connect to the other end
			logger.log(Level.INFO, "Connecting to JCentral on " + address + ":" + p);
			System.out.println("Connecting to JCentral on " + address + ":" + p);

			Security.addProvider(new BouncyCastleProvider());

			final List<InetAddress> allAddresses = new ArrayList<>();

			try {
				final InetAddress[] resolvedAddresses = InetAddress.getAllByName(address);

				for (final InetAddress logAddress : resolvedAddresses)
					allAddresses.add(logAddress);
			}
			catch (final IOException ex) {
				logger.log(Level.SEVERE, "Could not resolve IP address of central services (" + address + ":" + p + ")", ex);
				System.err.println("Could not resolve IP address of central services (" + address + ":" + p + "): " + ex.getMessage());
				return null;
			}

			if (ConfigUtils.getConfig().getb("alien.api.DispatchSSLClient.PreferIPv6", true)) {
				System.err.println("Sorting the addresses in IPv6 first mode");
				Collections.sort(allAddresses, (a1, a2) -> (a1 instanceof Inet6Address) ? -1 : 0);
			}
			else
				if (ConfigUtils.getConfig().getb("alien.api.DispatchSSLClient.PreferIPv4", false)) {
					System.err.println("Sorting the addresses in IPv4 first mode");
					Collections.sort(allAddresses, (a1, a2) -> (a1 instanceof Inet6Address) ? 1 : 0);
				}

			logger.log(Level.FINER, "Will try to connect to the central services in the following order: " + allAddresses.toString());

			final SSLSocketFactory f;
			try {
				// get factory
				final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");

				logger.log(Level.INFO, "Connecting with client cert: " + ((java.security.cert.X509Certificate) JAKeyStore.getKeyStore().getCertificateChain("User.cert")[0]).getSubjectDN());
				// initialize factory, with clientCert(incl. priv+pub)
				kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);

				java.lang.System.setProperty("jdk.tls.client.protocols", "TLSv1.2");
				final SSLContext ssc = SSLContext.getInstance("TLS");

				// initialize SSL with certificate and the trusted CA and pub
				// certs
				ssc.init(kmf.getKeyManagers(), JAKeyStore.trusts, new SecureRandom());

				f = ssc.getSocketFactory();
			}
			catch (final Throwable t) {
				logger.log(Level.SEVERE, "Could not load the client certificate", t);
				System.err.println("Could not load the client certificate: " + t.getMessage());
				return null;
			}

			// connect timeout in config should be given in seconds
			final int connectTimeout = ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.ConnectTimeout", 10) * 1000;

			for (final InetAddress endpointToTry : allAddresses) {
				try {
					@SuppressWarnings("resource")
					// the socket will be passed along to the SSL one below, and to the connection cache
					final Socket s = new Socket();
					s.connect(new InetSocketAddress(endpointToTry, p), connectTimeout);

					@SuppressWarnings("resource")
					// this object is kept in the map, cannot be closed here
					final SSLSocket client = (SSLSocket) f.createSocket(s, address, p, true);

					// print info
					printSocketInfo(client);

					client.startHandshake();

					final Certificate[] peerCerts = client.getSession().getPeerCertificates();

					if (peerCerts != null) {
						if (logger.isLoggable(Level.INFO)) {
							logger.log(Level.INFO, "Printing peer's information:");

							for (final Certificate peerCert : peerCerts) {
								final X509Certificate xCert = (X509Certificate) peerCert;

								logger.log(Level.INFO, "Peer's Certificate Information:\n" + Level.INFO, "- Subject: " + xCert.getSubjectDN().getName() + "\n" + xCert.getIssuerDN().getName() + "\n"
										+ Level.INFO + "- Start Time: " + xCert.getNotBefore().toString() + "\n" + Level.INFO + "- End Time: " + xCert.getNotAfter().toString());
							}
						}

						final DispatchSSLClient sc = new DispatchSSLClient(client);
						System.out.println("Connection to JCentral (" + endpointToTry.getHostAddress() + ":" + p + ") established.");
						instance.put(portNo, sc);
						break;
					}
					logger.log(Level.FINE, "We didn't get any peer/service cert from " + endpointToTry.getHostAddress() + ":" + p);
				}
				catch (final ConnectException e) {
					logger.log(Level.FINE, "Could not connect to JCentral instance on " + endpointToTry.getHostAddress() + ":" + p, e);
				}
				catch (final Throwable e) {
					logger.log(Level.FINE, "Could not initiate SSL connection to the server on " + endpointToTry.getHostAddress() + ":" + p, e);
				}
			}
		}

		return instance.get(portNo);
	}

	@SuppressWarnings("unused")
	private void close() {
		if (ois != null)
			try {
				ois.close();
			}
			catch (final IOException ioe) {
				// ignore
			}

		if (oos != null)
			try {
				oos.close();
			}
			catch (final IOException ioe) {
				// ignore
			}

		if (connection != null)
			try {
				connection.close();
			}
			catch (final IOException ioe) {
				// ignore
			}

		instance = null;
	}

	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the socket.
	 */
	public static long lSerialization = 0;

	private static synchronized void initializeSocketInfo() {
		addr = ConfigUtils.getConfig().gets(serviceName).trim();

		if (addr.length() == 0) {
			addr = defaultHost;
			port = defaultPort;
		}
		else {

			final String address = addr;
			final int idx = address.indexOf(':');

			if (idx >= 0)
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					addr = address.substring(0, idx);
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					addr = defaultHost;
					port = defaultPort;
				}
		}
	}

	/**
	 * @param r
	 * @return the reply, or <code>null</code> in case of connectivity problems
	 * @throws ServerException
	 */
	public static synchronized <T extends Request> T dispatchRequest(final T r) throws ServerException {
		initializeSocketInfo();
		try {
			return dispatchARequest(r);
		}
		catch (@SuppressWarnings("unused") final IOException e) {
			// Now let's try, if we can reconnect
			instance.put(Integer.valueOf(port), null);
			try {
				return dispatchARequest(r);
			}
			catch (final IOException e1) {
				// This time we give up
				logger.log(Level.SEVERE, "Error running request, potential connection error.", e1);
				return null;
			}
		}

	}

	/**
	 * @param r
	 * @return the processed request, if successful
	 * @throws IOException
	 *             in case of connectivity problems
	 * @throws ServerException
	 *             if the server didn't like the request content
	 */
	public static synchronized <T extends Request> T dispatchARequest(final T r) throws IOException, ServerException {

		final DispatchSSLClient c = getInstance(addr, port);

		if (c == null)
			throw new IOException("Connection is null");

		final long lStart = System.currentTimeMillis();

		c.oos.writeObject(r);

		if (++c.objectsSentCounter >= RESET_OBJECT_STREAM_COUNTER) {
			c.oos.reset();
			c.objectsSentCounter = 0;
		}

		c.oos.flush();
		c.os.flush();

		lSerialization += System.currentTimeMillis() - lStart;

		Object o;
		try {
			o = c.ois.readObject();
		}
		catch (final ClassNotFoundException | IOException e) {
			throw new IOException(e.getMessage());
		}

		if (o == null) {
			logger.log(Level.WARNING, "Read a null object as reply to this request: " + r);
			return null;
		}

		if (logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "Got back an object of type " + o.getClass().getCanonicalName() + " : " + o);

			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Call stack is: ", new Throwable());
		}

		@SuppressWarnings("unchecked")
		final T reply = (T) o;

		final ServerException ex = reply.getException();

		if (ex != null)
			throw ex;

		return reply;
	}

	private static void printSocketInfo(final SSLSocket s) {

		logger.log(Level.INFO, "Remote address: " + s.getInetAddress().toString() + ":" + s.getPort());
		logger.log(Level.INFO, "   Local socket address = " + s.getLocalSocketAddress().toString());

		logger.log(Level.INFO, "   Cipher suite = " + s.getSession().getCipherSuite());
		logger.log(Level.INFO, "   Protocol = " + s.getSession().getProtocol());
	}

}
