package alien.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
public class DispatchSSLClient {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(DispatchSSLClient.class.getCanonicalName());

	private static final int defaultPort = 8098;
	private static final String defaultHost = "alice-jcentral.cern.ch";
	private static String serviceName = "apiService";

	private static String addr = null;
	private static int port = 0;

	private final Socket connection;

	private final ObjectInputStream ois;
	private final ObjectOutputStream oos;

	private final OutputStream os;

	private static long idleTimeout = 0;

	private static long lastCommand = System.currentTimeMillis();

	private static IdleWatcher watcherThread = null;

	private static final class IdleWatcher extends Thread {
		public IdleWatcher() {
			setDaemon(true);
			setName("DispatchSSLClient.IdleWatcher");
		}

		@Override
		public void run() {
			while (idleTimeout > 0) {
				checkIdleConnection();

				try {
					Thread.sleep(idleTimeout / 10 + 1);
				} catch (@SuppressWarnings("unused") InterruptedException e) {
					break;
				}
			}

			watcherThread = null;
		}
	}

	/**
	 * @param timeout in milliseconds
	 */
	public static synchronized void setIdleTimeout(final long timeout) {
		idleTimeout = timeout;

		if (idleTimeout > 0 && watcherThread == null) {
			watcherThread = new IdleWatcher();
			watcherThread.start();
		}
	}

	/**
	 * 
	 */
	static synchronized void checkIdleConnection() {
		if (instance != null && idleTimeout > 0 && System.currentTimeMillis() - lastCommand > idleTimeout) {
			logger.log(Level.INFO, "Closing idle socket");

			final var toClose = instance;

			instance = null;

			new Thread(() -> toClose.close()).start();
		}
	}

	/**
	 * E.g. the CE proxy should act as a fowarding bridge between JA and central
	 * services
	 *
	 * @param servName
	 *                 name of the config parameter for the host:port settings
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
		connection.setSoLinger(false, 0);

		// client expect an answer promptly, the 15 minute default value should be large
		// enough to accommodate even heavy requests
		connection.setSoTimeout(
				ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.readTimeout_seconds", 900) * 1000);

		this.ois = new ObjectInputStream(connection.getInputStream());

		this.os = connection.getOutputStream();

		this.oos = new ObjectOutputStream(this.os);
		this.oos.flush();
	}

	@Override
	public String toString() {
		return this.connection.getInetAddress().toString();
	}

	private static final Object globalLock = new Object();

	private static DispatchSSLClient instance = null;

	private static void connectTo(final List<InetAddress> allAddresses, final int targetPort,
			final SSLSocketFactory factory, final Object callback, final AtomicInteger connectionState) {
		System.out.println("Entering connectTo method");
		System.out.println("address list : " + allAddresses);
		System.out.println("targetPort : " + targetPort);
		System.out.println("SSLSocketFactory : " + factory);
		System.out.println("callback : " + callback);
		System.out.println("connectionState : " + connectionState);
		// connect timeout in config should be given in seconds
		final int connectTimeout = ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.ConnectTimeout", 10)
				* 1000;
		System.out.println("connectTimeout : " + connectTimeout);
		DispatchSSLClient ret = null;

		for (final InetAddress endpointToTry : allAddresses) {
			try {
				if (connectionState.get() != 0) {
					System.out.println("The thread trying to connect to " + allAddresses
							+ " was told to exit, currently having reached " + endpointToTry);
					logger.log(Level.FINEST, "The thread trying to connect to " + allAddresses
							+ " was told to exit, currently having reached " + endpointToTry);

					return;
				}

				@SuppressWarnings("resource")
				// the socket will be passed along to the SSL one below, and to the connection
				// cache
				final Socket s = new Socket();
				System.out.println("Socket instance created");
				s.connect(new InetSocketAddress(endpointToTry, targetPort), connectTimeout);
				System.out.println("Socket connected to " + endpointToTry.getHostAddress() + ":" + targetPort);
				@SuppressWarnings("resource")
				// this object is kept in the map, cannot be closed here
				final SSLSocket client = (SSLSocket) factory.createSocket(s, endpointToTry.getHostAddress(), targetPort,
						true);
				System.out.println("SSLSocket instance created");
				// 10s to negociate SSL, if it takes more than this to connect just try another
				// endpoint
				client.setSoTimeout(10 * 1000);
				System.out.println("SSLSocket timeout set to 10s");
				// print info
				printSocketInfo(client, Level.FINE);
				System.out.println("Socket info printed");
				client.startHandshake();
				System.out.println("Handshake started");
				final Certificate[] peerCerts = client.getSession().getPeerCertificates();
				System.out.println("Peer certificates : " + peerCerts);
				if (peerCerts != null) {

					if (logger.isLoggable(Level.INFO)) {
						logger.log(Level.INFO, "Printing peer's information:");

						for (final Certificate peerCert : peerCerts) {
							final X509Certificate xCert = (X509Certificate) peerCert;

							logger.log(Level.INFO, "Peer's Certificate Information:\n" + Level.INFO,
									"- Subject: " + xCert.getSubjectDN().getName() + "\n"
											+ xCert.getIssuerDN().getName() + "\n"
											+ Level.INFO + "- Start Time: " + xCert.getNotBefore().toString() + "\n"
											+ Level.INFO + "- End Time: " + xCert.getNotAfter().toString());
						}
					}
					System.out.println("Printing peer's information:");
					for (final Certificate peerCert : peerCerts) {
						final X509Certificate xCert = (X509Certificate) peerCert;

						System.out.println("Peer's Certificate Information:\n" +
								"- Subject: " + xCert.getSubjectDN().getName() + "\n"
								+ xCert.getIssuerDN().getName() + "\n"
								+ Level.INFO + "- Start Time: " + xCert.getNotBefore().toString() + "\n"
								+ Level.INFO + "- End Time: " + xCert.getNotAfter().toString());
					}

					if (connectionState.compareAndSet(0, 1)) {
						final DispatchSSLClient sc = new DispatchSSLClient(client);
						System.out.println("DispatchSSLClient instance created");
						System.out.println("Connection to JCentral (" + endpointToTry.getHostAddress() + ":"
								+ targetPort + ") established.");
						System.out.println("Connection to JCentral (" + endpointToTry.getHostAddress() + ":"
								+ targetPort + ") established.");
						ret = sc;
					} else {
						// somebody beat us and connected faster on another socket, close this slow one
						client.close();
					}

					break;
				}
				System.out.println("We didn't get any peer/service cert from " + endpointToTry.getHostAddress()
						+ ":" + targetPort);
				logger.log(Level.FINE, "We didn't get any peer/service cert from " + endpointToTry.getHostAddress()
						+ ":" + targetPort);
			} catch (final ConnectException e) {
				System.out.println("Could not connect to JCentral instance on " + endpointToTry.getHostAddress()
						+ ":" + targetPort);
				System.out.println("Error is : " + e.getMessage());
				logger.log(Level.FINE, "Could not connect to JCentral instance on " + endpointToTry.getHostAddress()
						+ ":" + targetPort, e);
			} catch (final Throwable e) {
				System.out.println("Could not initiate SSL connection to the server on "
						+ endpointToTry.getHostAddress() + ":" + targetPort);
				System.out.println("Error is : " + e.getMessage());
				logger.log(Level.FINE, "Could not initiate SSL connection to the server on "
						+ endpointToTry.getHostAddress() + ":" + targetPort, e);
			}
		}

		synchronized (globalLock) {
			if (ret != null) {
				if (instance == null) {
					lastCommand = System.currentTimeMillis();

					instance = ret;
					synchronized (callback) {
						callback.notifyAll();
					}
				} else {
					ret.close();
				}
			}
		}
	}

	/**
	 * @return instance
	 * @throws IOException
	 */
	private static DispatchSSLClient getInstance() throws IOException {
		System.out.println("Entering getInstance method");

		if (instance == null) {
			initializeSocketInfo();

			logger.log(Level.INFO, "Connecting to JCentral on " + addr + ":" + port);
			System.out.println("Trying to connect to JCentral on " + addr + ":" + port);

			Security.addProvider(new BouncyCastleProvider());

			final List<InetAddress> ipv4 = new ArrayList<>();
			final List<InetAddress> ipv6 = new ArrayList<>();

			System.out.println("Initializing lists for IPv4 and IPv6 addresses.");

			try {
				final InetAddress[] resolvedAddresses = InetAddress.getAllByName(addr);

				if (resolvedAddresses == null || resolvedAddresses.length == 0) {
					logger.log(Level.SEVERE, "Empty address list for this hostname: " + addr);
					System.err.println("Empty address list for this hostname: " + addr);
					return null;
				}

				for (final InetAddress logAddress : resolvedAddresses) {
					System.out.println("Checking InetAddress: " + logAddress);

					if (logAddress instanceof Inet6Address) {
						System.out.println("Adding to ipv6 list: " + logAddress);
						ipv6.add(logAddress);
					} else {
						System.out.println("Adding to ipv4 list: " + logAddress);
						ipv4.add(logAddress);
					}
				}
			} catch (final IOException ex) {
				logger.log(Level.SEVERE, "Could not resolve IP address of central services (" + addr + ":" + port + ")",
						ex);
				System.err.println("Could not resolve IP address of central services (" + addr + ":" + port + "): "
						+ ex.getMessage());
				return null;
			}

			System.out.println("Shuffling IPv6 addresses.");
			Collections.shuffle(ipv6);
			System.out.println(ipv6);

			System.out.println("Shuffling IPv4 addresses.");
			Collections.shuffle(ipv4);
			System.out.println(ipv4);

			final List<InetAddress> mainProtocol;
			final List<InetAddress> fallbackProtocol;

			System.out.println("Checking IPv6 preference in configuration.");
			if (ConfigUtils.getConfig().getb("alien.api.DispatchSSLClient.PreferIPv6", true)) {
				System.out.println("IPv6 preference is set to true.");
				if (ipv6.size() > 0) {
					System.out.println("Using IPv6 addresses as the main protocol.");
					mainProtocol = ipv6;
					fallbackProtocol = ipv4;
				} else {
					System.out.println("No IPv6 addresses available. Using IPv4 addresses as the main protocol.");
					mainProtocol = ipv4;
					fallbackProtocol = null;
				}
			} else if (ConfigUtils.getConfig().getb("alien.api.DispatchSSLClient.PreferIPv4", false)) {
				System.out.println("IPv4 preference is set to true.");
				if (ipv4.size() > 0) {
					System.out.println("Using IPv4 addresses as the main protocol.");
					mainProtocol = ipv4;
					fallbackProtocol = ipv6;
				} else {
					System.out.println("No IPv4 addresses available. Using IPv6 addresses as the main protocol.");
					mainProtocol = ipv6;
					fallbackProtocol = null;
				}
			} else {
				mainProtocol = new ArrayList<>();
				System.out.println("Neither IPv4 nor IPv6 predered. Using both protocols");
				final Iterator<InetAddress> it6 = ipv6.iterator();
				final Iterator<InetAddress> it4 = ipv4.iterator();

				while (it6.hasNext() || it4.hasNext()) {
					if (it6.hasNext())
						mainProtocol.add(it6.next());

					if (it4.hasNext())
						mainProtocol.add(it4.next());
				}
				System.out.println("Main protocol list: " + mainProtocol);
				fallbackProtocol = null;
			}

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Will try to connect to the central services in the following order: "
						+ mainProtocol + " then " + fallbackProtocol);
			System.out.println("Will try to connect to the central services in the following order: "
					+ mainProtocol + " then " + fallbackProtocol);
			final SSLSocketFactory f;
			try {
				final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");
				System.out.println("KeyManagerFactory instance created");

				// System.out.println("Connecting with client cert: " +
				// ((java.security.cert.X509Certificate) JAKeyStore
				// .getKeyStore().getCertificateChain("User.cert")[0]).getSubjectDN());

				System.out.println("JA KeyStore pass : " + JAKeyStore.pass.toString());
				kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);
				System.out.println("KeyManagerFactory instance initiated");

				java.lang.System.setProperty("jdk.tls.client.protocols", "TLSv1.2,TLSv1.3");
				final SSLContext ssc = SSLContext.getInstance("TLS");
				System.out.println("SSLContext instance created");

				ssc.init(kmf.getKeyManagers(), JAKeyStore.trusts, new SecureRandom());
				System.out.println("SSLContext instance initiated");

				f = ssc.getSocketFactory();
				System.out.println("SSLSocketFactory instance created");
			} catch (final Throwable t) {
				logger.log(Level.SEVERE, "Could not load the client certificate", t);
				System.err.println("Could not load the client certificate: " + t.getMessage());
				return null;
			}

			final Object callbackObject = new Object();

			final AtomicInteger connectionState = new AtomicInteger(0);
			System.out.println("Connection state set to 0");
			final Thread tMain = new Thread(() -> connectTo(mainProtocol, port, f, callbackObject, connectionState));
			tMain.start();
			System.out.println("Thread ConnetTo started");

			try {
				synchronized (callbackObject) {
					if (instance == null)
						callbackObject.wait(
								ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.happyEyeballsTimeout", 1000));
				}
			} catch (final InterruptedException ie) {
				throw new IOException("Connection was interrupted", ie);
			}

			if (instance != null)
				return instance;

			final Thread tFallback;

			if (fallbackProtocol != null && fallbackProtocol.size() > 0) {
				System.out.println("Fallback protocol is not null and So it is used");
				logger.log(Level.FINE,
						"Could not establish a connection on the preferred protocol so far, will add the fallback solution to the mix");

				tFallback = new Thread(() -> connectTo(fallbackProtocol, port, f, callbackObject, connectionState));
				tFallback.start();
			} else
				tFallback = null;

			for (int i = 0; i < ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.connectionTimeoutSteps",
					60); i++) {
				System.out.println("Connection timeout steps : " + i + " out of "
						+ ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.connectionTimeoutSteps", 60));
				try {
					synchronized (callbackObject) {
						if (instance == null)
							callbackObject.wait(ConfigUtils.getConfig()
									.geti("alien.api.DispatchSSLClient.connectionTimeoutResolution", 1000));
					}
				} catch (final InterruptedException ie) {
					throw new IOException("Connection was interrupted", ie);
				}

				if (instance != null) {
					logger.log(Level.FINE, "Connection worked at step " + i);

					break;
				}
			}

			connectionState.set(2);

			if (tMain.isAlive())
				tMain.interrupt();

			if (tFallback != null && tFallback.isAlive())
				tFallback.interrupt();
		}

		System.out.println("Returning DispatchSSLClient instance");
		return instance;
	}

	@SuppressWarnings("unused")
	private void close() {
		if (ois != null)
			try {
				ois.close();
			} catch (final IOException ioe) {
				// ignore
			}

		if (oos != null)
			try {
				oos.close();
			} catch (final IOException ioe) {
				// ignore
			}

		if (connection != null)
			try {
				connection.close();
			} catch (final IOException ioe) {
				// ignore
			}
	}

	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the
	 * socket.
	 */
	private static long lSerialization = 0;

	private static synchronized void initializeSocketInfo() {
		System.out.println("Entering initializeSocketInfo method in Dispatch SSL client");
		System.out.println("serviceName : " + serviceName);
		System.out.println("Default Host : " + defaultHost);
		System.out.println("DEfault Port : " + defaultPort);
		System.out.println("Config from config utils : " + ConfigUtils.getConfig());
		addr = ConfigUtils.getConfig().gets(serviceName).trim();
		System.out.println("addr : " + addr);
		if (addr.length() == 0) {
			System.out.println("addr.length() =0, So settign to default Host and port");
			addr = defaultHost;
			port = defaultPort;

		} else {
			final String address = addr;
			final int idx = address.indexOf(':');

			if (idx >= 0)
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					addr = address.substring(0, idx);
				} catch (@SuppressWarnings("unused") final Exception e) {
					addr = defaultHost;
					port = defaultPort;
				}
			else
				port = defaultPort;
		}
		System.out.println("addr : " + addr);
		System.out.println("port : " + port);
	}

	/**
	 * @param r
	 * @return the reply, or <code>null</code> in case of connectivity problems
	 * @throws ServerException
	 */
	public static synchronized <T extends Request> T dispatchRequest(final T r) throws ServerException {
		System.out.println("This is Dispatch SSL Client dispatchRequest() method");

		try {
			return dispatchARequest(r);
		} catch (final IOException e) {
			System.out.println("IOException occured : " + e.getMessage());
			if (e instanceof StreamCorruptedException) {
				System.out.println("First attempt to deserialize the response failed");
				logger.log(Level.SEVERE, "First attempt to deserialize the response failed", e);
			}

			// Now let's try, if we can reconnect
			if (instance != null) {
				instance.close();
				instance = null;
			}

			for (int i = 0; i < ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.connectAttempts", 5); i++) {
				try {
					return dispatchARequest(r);
				} catch (final IOException e1) {
					// This time we give up
					System.out.println("IOException occured : " + e1.getMessage());
					System.out.println("Exception connecting at attempt no. " + i);
					logger.log(Level.SEVERE, "Exception connecting at attempt no. " + i, e1);
				}

				try {
					int sleepTime = ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.sleepFactor", 1000)
							* (i + 1);
					System.out.println("Sleeping for " + sleepTime + " milliseconds");
					Thread.sleep(sleepTime);
				} catch (InterruptedException ie) {
					System.out.println("Reconnect cycle interrupted at iteration " + i);
					logger.log(Level.SEVERE, "Reconnect cycle interrupted at iteration " + i, ie);
					break;
				}
			}

			return null;
		} finally {
			lastCommand = System.currentTimeMillis();
			System.out.println("Request dispatch completed");
		}
	}

	/**
	 * @param r
	 * @return the processed request, if successful
	 * @throws IOException
	 *                         in case of connectivity problems
	 * @throws ServerException
	 *                         if the server didn't like the request content
	 */
	public static synchronized <T extends Request> T dispatchARequest(final T r) throws IOException, ServerException {
		System.out.println("Dispatching a Request");
		lastCommand = System.currentTimeMillis();

		final DispatchSSLClient c = getInstance();

		if (c == null)
			throw new IOException("Connection is null");

		System.out.println("Got DispatchSSLClient instance");

		final long lStart = System.currentTimeMillis();

		c.oos.writeUnshared(r);
		System.out.println("Request written to ObjectOutputStream");

		c.oos.flush();
		System.out.println("ObjectOutputStream flushed");

		lSerialization += System.currentTimeMillis() - lStart;

		Object o;
		try {
			o = c.ois.readObject();
		} catch (final ClassNotFoundException e) {
			throw new IOException(e);
		}

		if (o == null) {
			System.out.println("Read a null object as reply to this request: " + r);
			logger.log(Level.WARNING, "Read a null object as reply to this request: " + r);
			return null;
		}

		if (logger.isLoggable(Level.FINE)) {
			System.out.println("Got back an object of type " + o.getClass().getCanonicalName());
			logger.log(Level.FINE, "Got back an object of type " + o.getClass().getCanonicalName());

			if (logger.isLoggable(Level.FINEST)) {
				System.out.println("Call stack is: ");
				logger.log(Level.FINEST, "Call stack is: ", new Throwable());
			}
		}

		@SuppressWarnings("unchecked")
		final T reply = (T) o;

		reply.setPartnerAddress(c.connection.getInetAddress());

		final ServerException ex = reply.getException();

		if (ex != null) {
			System.out.println("Exception received: " + ex.getMessage());
			throw ex;
		}

		System.out.println("Request dispatch completed");
		return reply;
	}

	/**
	 * @return total time in milliseconds spent in serializing objects
	 */
	public static long getSerializationTime() {
		return lSerialization;
	}

	private static void printSocketInfo(final SSLSocket s, final Level level) {
		if (logger.isLoggable(level)) {
			logger.log(level, "Remote address: " + s.getInetAddress().toString() + ":" + s.getPort());
			logger.log(level, "   Local socket address = " + s.getLocalSocketAddress().toString());

			logger.log(level, "   Cipher suite = " + s.getSession().getCipherSuite());
			logger.log(level, "   Protocol = " + s.getSession().getProtocol());
		}
		System.out.println("Remote address: " + s.getInetAddress().toString() + ":" + s.getPort());
		System.out.println("Local socket address = " + s.getLocalSocketAddress().toString());

		System.out.println("Cipher suite = " + s.getSession().getCipherSuite());
		System.out.println("Protocol = " + s.getSession().getProtocol());
	}

}
