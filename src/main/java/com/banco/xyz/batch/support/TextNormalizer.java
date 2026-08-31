package com.banco.xyz.batch.support;

import java.text.Normalizer;
import java.util.Locale;

public final class TextNormalizer {

	private TextNormalizer() {
	}

	public static String normalize(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		if (trimmed.isEmpty()) {
			return "";
		}
		return Normalizer.normalize(trimmed, Normalizer.Form.NFD)
				.replaceAll("\\p{M}+", "")
				.toLowerCase(Locale.ROOT);
	}
}
