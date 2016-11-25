package de.appsist.service.gateway;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;

/**
 * 
 * @author simon.schwantzer(at)im-c.de
 *
 * @param <T> Key class for the result handlers.
 * @param <E> Result type for request.
 */
public class ResultAggregationHandler<T, E> {
	private final AsyncResultHandler<E> finalResultHandler;
	private final Set<AsyncResultHandler<E>> openRequests;
	private final Map<T, AsyncResultHandler<E>> resultHandlers;
	private boolean isAborted;
	
	public ResultAggregationHandler(Set<T> requesters, AsyncResultHandler<E> finalResultHandler) {
		this.finalResultHandler = finalResultHandler;
		openRequests = new HashSet<>();
		isAborted = false;
		resultHandlers = new HashMap<>();
		for (T requester : requesters) {
			AsyncResultHandler<E> resultHandler = new AsyncResultHandler<E>() {

				@Override
				public void handle(AsyncResult<E> result) {
					openRequests.remove(this);
					checkAndComplete(result);
				}
			};
			resultHandlers.put(requester, resultHandler);
			openRequests.add(resultHandler);
		}
	}
	
	public AsyncResultHandler<E> getRequestHandler(T requester) {
		return resultHandlers.get(requester);
	}
	
	
	private void checkAndComplete(final AsyncResult<E> result) {
		if (isAborted) return; // We already threw an error.
		if (openRequests.isEmpty() && result.succeeded()) {
			finalResultHandler.handle(new AsyncResult<E>() {
				
				@Override
				public boolean succeeded() {
					return true;
				}
				
				@Override
				public E result() {
					return null;
				}
				
				@Override
				public boolean failed() {
					return false;
				}
				
				@Override
				public Throwable cause() {
					return null;
				}
			});
		} else {
			if (result.failed()) {
				isAborted = true; // We are done here.
				finalResultHandler.handle(new AsyncResult<E>() {
					
					@Override
					public boolean succeeded() {
						return false;
					}
					
					@Override
					public E result() {
						return null;
					}
					
					@Override
					public boolean failed() {
						return true;
					}
					
					@Override
					public Throwable cause() {
						return result.cause();
					}
				});
			}
		}
	}
	
	
}