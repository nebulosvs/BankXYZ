package com.banco.xyz.batch.support;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.List;

import com.banco.xyz.batch.exception.InvalidDataException;

/**
 * Normaliza fechas del CSV legacy (yyyy-MM-dd, yyyy/MM/dd, dd-MM-yyyy, dd/MM/yyyy).
 */
public final class LegacyDateParser {

	private static final List<DateTimeFormatter> FORMATTERS = List.of(
			strict("uuuu-MM-dd"),
			strict("uuuu/MM/dd"),
			strict("dd-MM-uuuu"),
			strict("dd/MM/uuuu"));

	private LegacyDateParser() {
	}

	public static LocalDate parse(String raw) {
		if (raw == null || raw.isBlank()) {
			throw new InvalidDataException("Fecha vacia");
		}
		String value = raw.trim();
		for (DateTimeFormatter formatter : FORMATTERS) {
			try {
				return LocalDate.parse(value, formatter);
			} catch (DateTimeParseException ignored) {
				// probar siguiente formato
			}
		}
		throw new InvalidDataException("Fecha invalida o malformada: " + raw);
	}

	private static DateTimeFormatter strict(String pattern) {
		return DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.STRICT);
	}
}
