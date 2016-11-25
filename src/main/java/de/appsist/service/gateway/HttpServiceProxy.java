package de.appsist.service.gateway;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * Proxy for HTTP request to appsist services.
 * 
 * @author simon.schwantzer(at)im-c.de
 */
public class HttpServiceProxy implements Handler<HttpServerRequest> {
	
	private final HttpClient client;
	private final int pathPrefixLength;
	
	/**
	 * Creates the proxy with the given service configuration.
	 * @param vertx Vertx runtime. Used to generate a HTTP client used to proxy requests.
	 * @param serviceId Service identifier.
	 * @param serviceConfig Configuration of the webserver of the service to address.
	 */
	public HttpServiceProxy(Vertx vertx, String serviceId, JsonObject serviceConfig) {
		int port = serviceConfig.getInteger("port");
		client = vertx.createHttpClient().setHost("localhost").setPort(port);
		// pathPrefixLength = 10 + serviceId.length();
		pathPrefixLength = 0;
	}

	@Override
	public void handle(final HttpServerRequest request) {
		String localPath = request.path().substring(pathPrefixLength); 
		if (request.query() != null) {
			localPath += "?" + request.query();
		}
		final HttpClientRequest clientRequest = client.request(
			request.method(),
			localPath,
			new Handler<HttpClientResponse>() {
				public void handle(HttpClientResponse clientResponse) {
					request.response().setStatusCode(clientResponse.statusCode());
					request.response().headers().set(clientResponse.headers());
					if ("chunked".equals(clientResponse.headers().get("Transfer-Encoding"))) {
						request.response().setChunked(true);
					}
					clientResponse.dataHandler(new Handler<Buffer>() {
						@Override
						public void handle(Buffer data) {
							request.response().write(data);
						}
					});
					clientResponse.endHandler(new VoidHandler() {
						@Override
						public void handle() {
							request.response().end();
						}
					});
				}
			}
		);
		clientRequest.headers().set(request.headers());
		clientRequest.setChunked(true);
		request.dataHandler(new Handler<Buffer>() {
			@Override
			public void handle(Buffer data) {
				clientRequest.write(data);
			}
		});
		request.endHandler(new VoidHandler() {
			@Override
			public void handle() {
				clientRequest.end();
			}
		});
	}
}
