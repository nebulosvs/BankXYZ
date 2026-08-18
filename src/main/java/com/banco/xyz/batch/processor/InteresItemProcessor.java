package com.banco.xyz.batch.processor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import com.banco.xyz.batch.exception.InvalidDataException;
import com.banco.xyz.domain.CuentaInteres;

public class InteresItemProcessor implements ItemProcessor<CuentaInteres, CuentaInteres> {

	private static final Logger log = LoggerFactory.getLogger(InteresItemProcessor.class);

	private static final Map<String, BigDecimal> TASAS = Map.of(
			"ahorro", new BigDecimal("0.0100"),
			"prestamo", new BigDecimal("0.0150"));

	private final Set<String> firmas = new HashSet<>();

	@Override
	public CuentaInteres process(CuentaInteres item) {
		if (item.getCuentaId() == null || item.getNombre() == null || item.getNombre().isBlank()
				|| item.getEdad() == null || item.getTipo() == null || item.getTipo().isBlank()) {
			throw new InvalidDataException("Cuenta incompleta: " + item);
		}
		if (item.getEdad() < 18 || item.getEdad() > 100) {
			throw new InvalidDataException("Cuenta " + item.getCuentaId() + " con edad invalida: " + item.getEdad());
		}
		if (item.getSaldo() == null || item.getSaldo().compareTo(BigDecimal.ZERO) <= 0) {
			throw new InvalidDataException("Cuenta " + item.getCuentaId() + " con saldo no positivo");
		}

		String tipo = item.getTipo().trim().toLowerCase(Locale.ROOT);
		item.setTipo(tipo);

		// Hipoteca no aplica al calculo mensual: se filtra (no es error de datos)
		if (!TASAS.containsKey(tipo)) {
			log.warn("Tipo de cuenta no aplica interes mensual (filtrada): {}", item);
			return null;
		}

		String firma = item.getNombre().trim().toLowerCase(Locale.ROOT) + "|" + item.getSaldo() + "|" + tipo + "|"
				+ item.getEdad();
		if (!firmas.add(firma)) {
			throw new InvalidDataException("Cuenta duplicada: " + item);
		}

		BigDecimal tasa = TASAS.get(tipo);
		BigDecimal interes = item.getSaldo().multiply(tasa).setScale(2, RoundingMode.HALF_UP);
		BigDecimal saldoFinal = item.getSaldo().add(interes).setScale(2, RoundingMode.HALF_UP);

		item.setTasaInteres(tasa);
		item.setInteresCalculado(interes);
		item.setSaldoFinal(saldoFinal);

		log.info("Interes calculado cuenta {}: tasa={}, interes={}, saldoFinal={}", item.getCuentaId(), tasa, interes,
				saldoFinal);
		return item;
	}
}
