package com.banco.xyz.batch.processor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import com.banco.xyz.batch.exception.InvalidDataException;
import com.banco.xyz.domain.Transaccion;

public class TransaccionItemProcessor implements ItemProcessor<Transaccion, Transaccion> {

	private static final Logger log = LoggerFactory.getLogger(TransaccionItemProcessor.class);
	private static final Set<String> TIPOS_VALIDOS = Set.of("debito", "credito");

	private final Set<String> vistos = new HashSet<>();

	@Override
	public Transaccion process(Transaccion item) {
		if (item.getId() == null || item.getFecha() == null || item.getMonto() == null
				|| item.getTipo() == null || item.getTipo().isBlank()) {
			throw new InvalidDataException("Transaccion incompleta: " + item);
		}

		String tipo = item.getTipo().trim().toLowerCase(Locale.ROOT);
		if (!TIPOS_VALIDOS.contains(tipo)) {
			throw new InvalidDataException("Transaccion " + item.getId() + " con tipo no valido: " + item.getTipo());
		}
		item.setTipo(tipo);

		String claveDuplicado = item.getFecha() + "|" + item.getMonto() + "|" + tipo;
		if (!vistos.add(claveDuplicado)) {
			throw new InvalidDataException("Transaccion duplicada omitida: " + item);
		}

		if (item.getMonto().compareTo(BigDecimal.ZERO) < 0) {
			item.setAnomalia(true);
			item.setObservacion("Monto negativo detectado");
			log.warn("Anomalia marcada (no se omite): {}", item);
		} else if (item.getMonto().compareTo(BigDecimal.ZERO) == 0) {
			item.setAnomalia(true);
			item.setObservacion("Monto cero detectado");
			log.warn("Anomalia marcada (no se omite): {}", item);
		} else {
			item.setAnomalia(false);
			item.setObservacion("OK");
		}

		log.info("Transaccion procesada: id={}, anomalia={}", item.getId(), item.isAnomalia());
		return item;
	}
}
