package com.banco.xyz.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.stereotype.Component;

/**
 * Registra en consola cada item omitido por politicas de skip.
 */
@Component
public class LoggingSkipListener implements SkipListener<Object, Object> {

	private static final Logger log = LoggerFactory.getLogger(LoggingSkipListener.class);

	@Override
	public void onSkipInRead(Throwable t) {
		log.warn("[SKIP-READ] Se omitio un registro al leer: {}", t.getMessage());
	}

	@Override
	public void onSkipInProcess(Object item, Throwable t) {
		log.warn("[SKIP-PROCESS] Item omitido por excepcion: item={}, causa={}", item, t.getMessage());
	}

	@Override
	public void onSkipInWrite(Object item, Throwable t) {
		log.warn("[SKIP-WRITE] Item omitido al escribir: item={}, causa={}", item, t.getMessage());
	}
}
