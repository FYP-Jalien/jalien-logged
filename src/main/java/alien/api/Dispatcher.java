package alien.api;

import java.lang.ref.WeakReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.JobKilledException;
import alien.api.taskQueue.PutJobLog;
import alien.api.taskQueue.RecordPreemption;
import alien.api.taskQueue.SetJobStatus;
import alien.api.token.GetTokenCertificate;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;
import lazyj.cache.ExpirationCache;

/**
 * @author costing
 * @since 2011-03-04
 */
public class Dispatcher {

	private static final boolean useParallelConnections = false;

	private static final ExpirationCache<String, WeakReference<Request>> cache = new ExpirationCache<>(10240);

	private static final Monitor monitor = MonitorFactory.getMonitor(Dispatcher.class.getCanonicalName());

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(Dispatcher.class.getCanonicalName());

	static {
		monitor.addMonitoring("object_cache_status", (names, values) -> {
			names.add("object_cache_size");
			values.add(Double.valueOf(cache.size()));
		});

		// make sure the Version class is initialized
		ConfigUtils.getVersion();
	}

	/**
	 * @param r
	 *            request to execute
	 * @return the processed request
	 * @throws ServerException
	 *             exception thrown by the processing
	 */
	public static <T extends Request> T execute(final T r) throws ServerException {
		
		return execute(r, false);
	}

	/**
	 * @param r
	 *            request to execute
	 * @param forceRemote
	 *            request to force remote execution
	 * @return the processed request
	 * @throws ServerException
	 *             exception thrown by the processing
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Request> T execute(final T r, final boolean forceRemote) throws ServerException {
		System.out.println("This is Dispatcher Class execute() method");
		final boolean isCacheable = r instanceof Cacheable;
		System.out.println("isCacheable : "+isCacheable);
		final String key;

		if (isCacheable)
			key = r.getClass().getCanonicalName() + "#" + ((Cacheable) r).getKey();
		else
			key = null;
		System.out.println("Cache Key : "+key);
		if (isCacheable) {
			final WeakReference<Request> cachedObject = cache.get(key);

			final Object cachedValue;

			if (cachedObject != null && (cachedValue = cachedObject.get()) != null) {
				monitor.incrementCacheHits("object_cache");
				return (T) cachedValue;
			}
			monitor.incrementCacheMisses("object_cache");
		}
		else
			monitor.incrementCounter("non_cacheable");

		final T ret;

		try (Timing timing = new Timing()) {
			System.out.println("ConfigUtils.isCentralService() : "+ConfigUtils.isCentralService());
			if (ConfigUtils.isCentralService() && !forceRemote) {
				
				r.authorizeUserAndRole();
System.out.println("passesFirewallRules(r) : "+passesFirewallRules(r));
				if (passesFirewallRules(r)) {
					
					try {
						System.out.println("Firewall rules passes, so runs");
						r.run();
					}
					catch (final Throwable t) {
						throw new ServerException(t.getMessage(), t);
					}
					finally {
						monitor.addMeasurement("executed_requests", timing);
					}
				}
				else {
					monitor.addMeasurement("firewalled_requests", timing);
				}

				ret = r;
			}
			else {
				System.out.println("Not central srvice, so dispatching request");
				try {
					ret = dispatchRequest(r);
				}
				finally {
					monitor.addMeasurement("forwarded_requests", timing);
				}
			}
		}

		if (isCacheable && ret != null)
			cache.put(key, new WeakReference<>(ret), ((Cacheable) ret).getTimeout());

		return ret;
	}

	private static final ExpirationCache<Long, Integer> jobResubmissionsCache = new ExpirationCache<>(10000);

	private static final boolean isJobStillValid(final Long queueId, final Integer resubmissionCounter) {
		if (queueId == null || resubmissionCounter == null || queueId.longValue() <= 0 || resubmissionCounter.intValue() < 0)
			return false;

		Integer cachedResubmissionCounter = jobResubmissionsCache.get(queueId);

		if (cachedResubmissionCounter == null) {
			cachedResubmissionCounter = Integer.valueOf(TaskQueueUtils.getResubmission(queueId));

			// the database query didn't work (likely a timeout)
			if (cachedResubmissionCounter.intValue() == -2)
				return true;

			jobResubmissionsCache.put(queueId, cachedResubmissionCounter, 1000 * 60);
		}

		if (!resubmissionCounter.equals(cachedResubmissionCounter)) {
			logger.log(Level.WARNING, "Resubmission counter of " + queueId + " is " + resubmissionCounter + " while the current status is " + cachedResubmissionCounter);
			return false;
		}

		return true;
	}

	/**
	 * Check if the request should be allowed run
	 *
	 * @param r request to check
	 * @return <code>true</code> if it can be run, <code>false</code> if not
	 */
	private static final boolean passesFirewallRules(final Request r) {
		final AliEnPrincipal requester = r.getEffectiveRequester();

		if (requester.isJobAgent()) {
			// Main JobAgent functionality - getting a job from the task queue to run
			if (r instanceof GetMatchJob)
				return true;

			// Allowing the JobAgent to change the job status enables it to act on possible JobWrapper terminations/faults
			if (r instanceof SetJobStatus)
				return true;

			// Enables the JobAgent to report its progress/the resources it allocates for the JobWrapper sandbox
			if (r instanceof PutJobLog)
				return true;

			// Allows JobAgents to retrieve job token certificates for the actual job to run with
			if (r instanceof GetTokenCertificate)
				return true;

			if (r instanceof RecordPreemption)
				return true;

			// TODO : add above all commands that a JobAgent should run (setting job status, uploading traces)
			logger.log(Level.SEVERE, "A request was firewalled: " + r.getClass().getName() + " as " + r.getEffectiveRequester());

			r.setException(new ServerException("You are not allowed to call " + r.getClass().getName() + " as job agent", null));
			return false;
		}

		if (requester.isJob()) {
			if (!isJobStillValid(requester.getJobID(), requester.getResubmissionCount())) {
				r.setException(new JobKilledException("This job is not supposed to still be running", null));
				return false;
			}

			// TODO : firewall all the commands that the job can have access to (whereis, access (read only for anything but the output directory ...))
		}

		return true;
	}

	private static <T extends Request> T dispatchRequest(final T r) throws ServerException {

		// return DispatchSSLClient.dispatchRequest(r);
		System.out.println("Use parallel connections : "+useParallelConnections);
		return useParallelConnections ? DispatchSSLMTClient.dispatchRequest(r) : DispatchSSLClient.dispatchRequest(r);
	}

}
