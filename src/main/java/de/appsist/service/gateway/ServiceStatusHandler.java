package de.appsist.service.gateway;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import de.appsist.commons.misc.ServiceStatusSignal;

/**
 * Handler for service status.
 * 
 * @author simon.schwantzer(at)im-c.de
 */
public class ServiceStatusHandler implements Handler<ServiceStatusSignal> {
	private static final Logger logger = LoggerFactory.getLogger(ServiceStatusHandler.class);
	
	private final Map<String, ServiceStatusSignal> status; // Service ID -> Last Signal
	
	public ServiceStatusHandler() {
		status = new HashMap<>();
	}
	
	/**
	 * Returns the latest signal received after a specific point in time.
	 * @param serviceId Identifier of the service sending the signal.
	 * @param since Date time limiting the request.
	 * @return Last signal of a signal as been received after the given date time. <code>null</code> if no signal has been received.
	 */
	public ServiceStatusSignal getSignalSince(String serviceId, Date since) {
		ServiceStatusSignal signalForService = status.get(serviceId);
		if (signalForService != null && signalForService.getCreated().after(since)) {
			return signalForService;
		} else {
			return null;
		}
	}

	@Override
	public void handle(ServiceStatusSignal signal) {
		if ("de.appsist.service".equals(signal.getGroupId())) {
			status.put(signal.getServiceId(), signal);
			switch (signal.getServiceStatus()) {
			case WARN:
				logger.warn("Service \"" + signal.getServiceId() + "\" exceeds normal runtime parameters.");
				break;
			case ERROR:
				logger.error("Service \"" + signal.getServiceId() + "\" reports an error status.");
				break;
			case UNKNOWN:
				logger.warn("The status of service \"" + signal.getServiceId() + "\" is unknown.");
				break;
			default:
				logger.debug("Received status signal from service: " + signal.getServiceId());
			}
		}
	}
	
	/**
	 * Returns the latest signal of a service.
	 * @param serviceId Service identifier.
	 * @return Service status signal or <code>null</code> if no status signal has arrived since startup.
	 */
	public ServiceStatusSignal getSignalForService(String serviceId) {
		return status.get(serviceId);
	}

}
