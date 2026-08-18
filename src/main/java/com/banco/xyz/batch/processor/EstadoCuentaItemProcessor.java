package com.banco.xyz.batch.processor;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import com.banco.xyz.batch.exception.InvalidDataException;
import com.banco.xyz.domain.MovimientoAnual;

public class EstadoCuentaItemProcessor implements ItemProcessor<MovimientoAnual, MovimientoAnual> {

	private static final Logger log = LoggerFactory.getLogger(EstadoCuentaItemProcessor.class);
	private static final Set<String> TIPOS_VALIDOS = Set.of("deposito", "retiro", "compra");

	@Override
	public MovimientoAnual process(MovimientoAnual item) {
		if (item.getCuentaId() == null || item.getFecha() == null || item.getMonto() == null
				|| item.getTransaccion() == null || item.getTransaccion().isBlank()) {
			throw new InvalidDataException("Movimiento anual incompleto: " + item);
		}

		String tipo = item.getTransaccion().trim().toLowerCase(Locale.ROOT);
		if (!TIPOS_VALIDOS.contains(tipo)) {
			throw new InvalidDataException("Tipo de transaccion anual no valido: " + item.getTransaccion());
		}
		item.setTransaccion(tipo);

		if (item.getMonto().compareTo(BigDecimal.ZERO) <= 0) {
			throw new InvalidDataException(
					"Monto no positivo en estado de cuenta (cuenta " + item.getCuentaId() + "): " + item.getMonto());
		}

		if (item.getDescripcion() == null || item.getDescripcion().isBlank()) {
			item.setDescripcion("Sin descripcion");
		}

		item.setClasificacion(clasificar(tipo));
		log.info("Movimiento anual procesado: cuenta={}, tipo={}, monto={}", item.getCuentaId(), tipo, item.getMonto());
		return item;
	}

	private String clasificar(String tipo) {
		return switch (tipo) {
			case "deposito" -> "INGRESO";
			case "retiro", "compra" -> "EGRESO";
			default -> "OTRO";
		};
	}
}
