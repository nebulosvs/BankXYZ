package com.banco.xyz.batch.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.format.DateTimeParseException;

import com.banco.xyz.batch.exception.InvalidDataException;

/**
 * Politica de omision personalizada (SkipPolicy).
 * Omite errores de parseo del CSV y datos de negocio invalidos, hasta un limite.
 */
@Component
public class FileVerificationSkipper implements SkipPolicy {

	private static final Logger log = LoggerFactory.getLogger(FileVerificationSkipper.class);

	private final int skipLimit;

	public FileVerificationSkipper(@Value("${bank.batch.skip-limit:50}") int skipLimit) {
		this.skipLimit = skipLimit;
	}

	@Override
	public boolean shouldSkip(Throwable t, long skipCount) {
		if (skipCount >= skipLimit) {
			log.error("CustomSkipPolicy - se alcanzo el limite de omisiones ({})", skipLimit);
			throw new SkipLimitExceededException(skipLimit, t);
		}

		if (esOmisible(t)) {
			log.info("Error: {}", t.getClass().getSimpleName());
			log.warn("CustomSkipPolicy - Excepcion omitida: {}", mensaje(t));
			return true;
		}

		log.error("CustomSkipPolicy - excepcion no omitible: {}", t.toString());
		return false;
	}

	private boolean esOmisible(Throwable t) {
		Throwable actual = t;
		while (actual != null) {
			if (actual instanceof InvalidDataException
					|| actual instanceof FlatFileParseException
					|| actual instanceof DateTimeParseException
					|| actual instanceof NumberFormatException
					|| actual instanceof IllegalArgumentException) {
				return true;
			}
			actual = actual.getCause();
		}
		return false;
	}

	private String mensaje(Throwable t) {
		if (t instanceof FlatFileParseException parseEx) {
			return parseEx.getMessage() + " | input=[" + parseEx.getInput() + "]";
		}
		return t.getMessage();
	}
}
