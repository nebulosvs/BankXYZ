package com.banco.xyz.batch.policy;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.stereotype.Component;

/**
 * Politica de reintento personalizada (RetryPolicy).
 * Reintenta fallas transitorias de base de datos (hasta 3 veces por defecto).
 */
@Component
public class CustomRetryPolicy implements RetryPolicy {

	private static final Logger log = LoggerFactory.getLogger(CustomRetryPolicy.class);

	private final SimpleRetryPolicy delegate;

	public CustomRetryPolicy(@Value("${bank.batch.retry-limit:3}") int retryLimit) {
		Map<Class<? extends Throwable>, Boolean> retryables = new HashMap<>();
		retryables.put(TransientDataAccessException.class, true);
		this.delegate = new SimpleRetryPolicy(retryLimit, retryables, true);
	}

	@Override
	public boolean canRetry(RetryContext context) {
		boolean retry = delegate.canRetry(context);
		if (retry && esReintentable(context.getLastThrowable())) {
			log.warn("CustomRetryPolicy - reintento {} causa={}", context.getRetryCount(),
					String.valueOf(context.getLastThrowable()));
		}
		return retry;
	}

	@Override
	public RetryContext open(RetryContext parent) {
		return delegate.open(parent);
	}

	@Override
	public void close(RetryContext context) {
		delegate.close(context);
	}

	@Override
	public void registerThrowable(RetryContext context, Throwable throwable) {
		if (esReintentable(throwable)) {
			log.warn("CustomRetryPolicy - registrando fallo transitorio: {}", throwable.toString());
		}
		delegate.registerThrowable(context, throwable);
	}

	private boolean esReintentable(Throwable t) {
		Throwable actual = t;
		while (actual != null) {
			if (actual instanceof TransientDataAccessException) {
				return true;
			}
			actual = actual.getCause();
		}
		return false;
	}
}
