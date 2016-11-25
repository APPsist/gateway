package de.appsist.service.gateway;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.xml.bind.DatatypeConverter;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.HttpServerResponse;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Verticle;

import de.appsist.commons.event.StartupCompleteEvent;
import de.appsist.commons.lang.LangUtil;
import de.appsist.commons.lang.StringBundle;
import de.appsist.commons.misc.ServiceStatus;
import de.appsist.commons.misc.ServiceStatusSignal;
import de.appsist.commons.misc.StatusSignalConfiguration;
import de.appsist.commons.misc.StatusSignalReceiver;

/**
 * Main verticle for the service gateway. It deploys other APPsist services depending on its configuration. 
 * @author simon.schwantzer(at)im-c.de
 */
public class MainVerticle extends Verticle {
	private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);
	
	private static final int HEALTHCHECK_TIMEOUT = 120000; // 2 Minutes
	
	private JsonObject config;
	private RouteMatcher routeMatcher;
	private ServiceStatusHandler serviceStatusHandler;

	@Override
	public void start() {
		if (container.config() != null && container.config().size() > 0) {
			config = container.config();
		} else {
			logger.warn("Warning: No configuration applied! Using default settings.");
			config = getDefaultConfiguration();
		}
		
		initializeLanguageBundles();
		deployServices();
		
		initializeEventBusHandler();
		initializeHTTPRouting();
		HttpServer httpServer = vertx.createHttpServer();
		httpServer.requestHandler(routeMatcher);
		
		// Initialize the event bus client bridge.
		JsonObject bridgeConfig = config.getObject("eventBusBridge");
		if (bridgeConfig != null) {
			bridgeConfig.putString("prefix", "/eventbus");
			
			vertx.createSockJSServer(httpServer).bridge(bridgeConfig, bridgeConfig.getArray("inbound"), bridgeConfig.getArray("outbound"));
		}
		httpServer.listen(config.getObject("webserver").getInteger("port"));
		
		logger.info("APPsist Service Gateway has been initialized.");
	}
	
	@Override
	public void stop() {
		logger.info("APPsist Service Gateway has been stopped.");
	}
	
	/**
	 * Create a configuration which used if no configuration is passed to the module.
	 * @return Configuration object.
	 */
	private static JsonObject getDefaultConfiguration() {
		JsonObject defaultConfig =  new JsonObject();
		JsonObject webserverConfig = new JsonObject();
		webserverConfig.putNumber("port", 8088);
		webserverConfig.putString("statics", "www");
		defaultConfig.putObject("webserver", webserverConfig);
		defaultConfig.putObject("services", new JsonObject());
		return defaultConfig;
	}
	
	private void initializeLanguageBundles() {
		String languageBundleDir = config.getString("languageBundleDir");
		
		if (languageBundleDir != null) {
			LangUtil.initializeBundles(vertx.sharedData(), languageBundleDir);
		} else {
			logger.error("No language bundles available, please check the [languageBundleDir] configuration entry.");
			return;
		}
	}
	
	private void deployServices() {
		JsonArray deploys = config.getArray("deploys");
		if (deploys != null) {
			Set<String> serviceIds = new HashSet<>();
			for (Object entry : deploys) {
				serviceIds.add(((JsonObject) entry).getString("id"));
			}
			ResultAggregationHandler<String, String> resultAggregation = new ResultAggregationHandler<String, String>(serviceIds, new AsyncResultHandler<String>() {

				@Override
				public void handle(AsyncResult<String> deployRequest) {
					if (deployRequest.succeeded()) {
						logger.info("Successfully deployed all system services.");
						initializeAPPsistServices();
					} else {
						logger.error("Failed to deploy all system services. Aborting.", deployRequest.cause());
						System.exit(1);
					}
				}
			});
			for (Object entry : deploys) {
				JsonObject deployConfig = (JsonObject) entry;
				String serviceId = deployConfig.getString("id");
				JsonObject config = deployConfig.getObject("config");
				logger.info("Trying to deploy system service: " + serviceId);
				container.deployModule(serviceId, config, resultAggregation.getRequestHandler(serviceId));
			}
		}
	}
	
	/**
	 * Initializes all services with the given configurations.
	 */
	private void initializeAPPsistServices() {
		JsonObject services = config.getObject("services");
		ResultAggregationHandler<String, String> resultAggregation = new ResultAggregationHandler<String, String>(services.getFieldNames(), new AsyncResultHandler<String>() {

			@Override
			public void handle(AsyncResult<String> deployRequest) {
				if (deployRequest.succeeded()) {
					logger.info("Successfully deployed all APPsist services.");
					StartupCompleteEvent event = new StartupCompleteEvent(UUID.randomUUID().toString());
					vertx.eventBus().publish("appsist:event:" + event.getModelId(), new JsonObject(event.asMap()));
					initializeServiceHealthcheck();
				} else {
					logger.error("Failed to deploy APPsist services.", deployRequest.cause());
					System.exit(1);
				}
			}
		});
		
		JsonObject commonConfig = config.getObject("commonConfig");
		
		for (String serviceId : services.getFieldNames()) {
			JsonObject serviceConfig = services.getObject(serviceId);
			StringBuilder builder = new StringBuilder(100);
			builder.append("de.appsist.service~");
			builder.append(serviceId);
			builder.append("~");
			builder.append(serviceConfig.getString("version"));
			logger.info("Trying to deploy APPsist service: " + serviceId);
			JsonObject configObject = serviceConfig.getObject("config");
			if (commonConfig != null) {
				// Inject common configuration parameters.
				for (String commonConfigField : commonConfig.getFieldNames()) {
					if (!configObject.containsField(commonConfigField)) {
						configObject.putValue(commonConfigField, commonConfig.getValue(commonConfigField));
					}
				}
			}
			
			container.deployModule(builder.toString(), configObject, resultAggregation.getRequestHandler(serviceId));
		}
	}
	
	private void initializeServiceHealthcheck() {
		serviceStatusHandler = new ServiceStatusHandler();
		
		JsonObject commonConfig = config.getObject("commonConfig");
		StatusSignalConfiguration statusSignalConfig;
		if (commonConfig != null) {
			JsonObject statusSignalObject = commonConfig.getObject("statusSignal");
			statusSignalConfig = statusSignalObject != null ? new StatusSignalConfiguration(statusSignalObject) : new StatusSignalConfiguration();
		} else {
			statusSignalConfig = new StatusSignalConfiguration();
		}
		
		StatusSignalReceiver receiver = new StatusSignalReceiver(vertx.eventBus(), statusSignalConfig);
		receiver.registerSignalHandler(serviceStatusHandler);
		
		final Set<String> serviceIds = config.getObject("services").getFieldNames();
		
		vertx.setPeriodic(HEALTHCHECK_TIMEOUT, new Handler<Long>() {
			
			@Override
			public void handle(Long handlerId) {
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.MILLISECOND, -HEALTHCHECK_TIMEOUT);
				for (String serviceId : serviceIds) {
					ServiceStatusSignal lastSignal = serviceStatusHandler.getSignalSince(serviceId, cal.getTime());
					if (lastSignal == null) {
						logger.error("No status signal received from service: " + serviceId);
					}
				}
			}
		});
	}
	
	/**
	 * In this method the handlers for the event bus are initialized.
	 */
	private void initializeEventBusHandler() {
		Handler<Message<JsonObject>> serviceConfigRequestReplyHandler = new Handler<Message<JsonObject>>() {
			
			@Override
			public void handle(Message<JsonObject> message) {
				JsonObject messageBody = message.body();
				String serviceId = messageBody.getString("serviceId");
				if (serviceId != null) {
					JsonObject serviceConfig = config.getObject("services").getObject(serviceId);
					if (serviceConfig == null) {
						serviceConfig = new JsonObject();
					}
					message.reply(serviceConfig);
				} else {
					message.reply(config.getObject("services"));
				}
			}
		};
		
		vertx.eventBus().registerHandler("appsist:services:gateway#getServiceConfig", serviceConfigRequestReplyHandler);	
	}
	
	/**
	 * In this method the HTTP API build using a route matcher.
	 */
	private void initializeHTTPRouting() {
		routeMatcher = new RouteMatcher();
		
		// All requests to /services/:serviceId/... are proxied to the webserver of the related service.
		for (String serviceId : config.getObject("services").getFieldNames()) {
			JsonObject serviceConfig = config.getObject("services").getObject(serviceId);
			JsonObject serviceWebserverConfig = serviceConfig.getObject("config").getObject("webserver");
			if (serviceWebserverConfig != null) {
				HttpServiceProxy httpServiceProxy = new HttpServiceProxy(vertx, serviceId, serviceWebserverConfig);
				routeMatcher.allWithRegEx(serviceWebserverConfig.getString("basePath") + "/.*", httpServiceProxy);
			}
		}

		// Lists the service configuration.
		routeMatcher.get("/services", new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest request) {
				HttpServerResponse response = request.response();
				response.end(config.getObject("services").encodePrettily());
			}
		});
		
		// Lists the service configuration.
		routeMatcher.get("/log", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest request) {
				int num;
				try {
					num = new Integer(request.params().get("num"));
				} catch (Exception e) {
					num = 1000;
				}
				
				String level = request.params().get("level");
				
				retrieveLogEntries(num, level, new AsyncResultHandler<JsonArray>() {

					@Override
					public void handle(AsyncResult<JsonArray> logRequest) {
						JsonObject response = new JsonObject();
						if (logRequest.succeeded()) {
							response.putString("status", "ok");
							response.putArray("entries", logRequest.result());
						} else {
							logger.warn("Failed to retrieve log from database.", logRequest.cause());
							response.putString("status", "error");
							response.putNumber("code", 500);
							response.putString("messag", "Failed to retrieve log from database: " + logRequest.cause().getMessage());
						}
						request.response().end(response.encode());
					}
				});
			}
		});
		
		// Returns the system status.
		routeMatcher.get("/status", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest request) {
				JsonObject systemStatus = new JsonObject();
				systemStatus.putString("created", DatatypeConverter.printDateTime(Calendar.getInstance()));
				JsonObject services = new JsonObject();
				Set<String> serviceIds = config.getObject("services").getFieldNames();
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.MILLISECOND, -HEALTHCHECK_TIMEOUT);
				for (String serviceId : serviceIds) {
					JsonObject serviceConfig = config.getObject("services").getObject(serviceId);
					JsonObject service = new JsonObject();
					service.putString("version", serviceConfig.getString("version"));
					service.putObject("serviceConfiguration", serviceConfig.getObject("config"));
					ServiceStatusSignal lastSignal = serviceStatusHandler.getSignalSince(serviceId, cal.getTime());
					if (lastSignal != null) {
						service.putString("status", lastSignal.getServiceStatus().toString());
						service.putObject("lastStatusSignal", lastSignal.asJson());
					} else {
						service.putString("status", ServiceStatus.UNKNOWN.toString());
						ServiceStatusSignal lastOutdatedSignal = serviceStatusHandler.getSignalForService(serviceId);
						if (lastOutdatedSignal != null) {
							service.putObject("lastStatusSignal", lastOutdatedSignal.asJson());
						}
					}
					services.putObject(serviceId, service);
				}
				systemStatus.putObject("services", services);
				JsonObject response = new JsonObject()
					.putString("status", "ok")
					.putObject("systemStatus", systemStatus);
				request.response().end(response.encode());
			}
		});
		
		// Returns a language bundle.
		routeMatcher.get("/bundle", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest request) {
				String languageId = request.params().get("lang");
				if (languageId == null) {
					languageId = LangUtil.DEFAULT_LANGUAGE;
				}
				StringBundle bundle = LangUtil.getInstance(vertx.sharedData()).getBundle(languageId);
				JsonObject response = new JsonObject()
					.putString("status", "ok")
					.putObject("bundle", bundle.asJson());
				request.response().end(response.encode());
			}
		});
		
		routeMatcher.getWithRegEx("/admin/.*", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest request) {
				request.response().sendFile("www" + request.path().substring(6));
			}
		});
	}
	
	private void retrieveLogEntries(int num, String level, final AsyncResultHandler<JsonArray> resultHandler) {
		JsonObject request = new JsonObject();
		request.putString("action", "find");
		request.putString("collection", "logs");
		JsonObject matcher = new JsonObject();
		if (level != null && !level.isEmpty()) {
			JsonArray inLevelArray = new JsonArray();
			switch (level) {
			case "ALL":
				inLevelArray.addString("ALL");
			case "TRACE":
				inLevelArray.addString("TRACE");
			case "DEBUG":
				inLevelArray.addString("DEBUG");
			case "INFO":
				inLevelArray.addString("INFO");
			case "WARN":
				inLevelArray.addString("WARN");
			case "ERROR":
				inLevelArray.addString("ERROR");
			case "FATAL":
				inLevelArray.addString("FATAL");
			}
			matcher.putObject("level", new JsonObject().putArray("$in", inLevelArray));		
		}
		request.putObject("matcher", matcher);
		request.putNumber("batch_size", num);
		request.putNumber("limit", num);
		request.putObject("sort", new JsonObject().putNumber("$natural", -1));
		request.putObject("keys", new JsonObject()
				.putNumber("_id", 0));
		vertx.eventBus().send("vertx.mongopersistor", request, new Handler<Message<JsonObject>>() {

			@Override
			public void handle(Message<JsonObject> message) {
				final JsonObject body = message.body();
				resultHandler.handle(new AsyncResult<JsonArray>() {
					
					@Override
					public boolean succeeded() {
						switch (body.getString("status")) {
						case "ok":
						case "more-exist":
							return true;
						default:
							return false;
						}
					}
					
					@Override
					public JsonArray result() {
						if (succeeded()) {
							JsonArray result = body.getArray("results");
							return result;
						} else {
							return null;
						}
					}
					
					@Override
					public boolean failed() {
						return !succeeded();
					}
					
					@Override
					public Throwable cause() {
						return failed() ? new Throwable(body.getString("message")) : null;
					}
				});
			}
		});
	}
}
