package com.banco.xyz.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public class MovimientoAnual {

	private Long cuentaId;
	private LocalDate fecha;
	private String transaccion;
	private BigDecimal monto;
	private String descripcion;
	private String clasificacion;

	public Long getCuentaId() {
		return cuentaId;
	}

	public void setCuentaId(Long cuentaId) {
		this.cuentaId = cuentaId;
	}

	public LocalDate getFecha() {
		return fecha;
	}

	public void setFecha(LocalDate fecha) {
		this.fecha = fecha;
	}

	public String getTransaccion() {
		return transaccion;
	}

	public void setTransaccion(String transaccion) {
		this.transaccion = transaccion;
	}

	public BigDecimal getMonto() {
		return monto;
	}

	public void setMonto(BigDecimal monto) {
		this.monto = monto;
	}

	public String getDescripcion() {
		return descripcion;
	}

	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}

	public String getClasificacion() {
		return clasificacion;
	}

	public void setClasificacion(String clasificacion) {
		this.clasificacion = clasificacion;
	}

	@Override
	public String toString() {
		return "MovimientoAnual{cuentaId=" + cuentaId + ", fecha=" + fecha + ", transaccion='" + transaccion
				+ "', monto=" + monto + "}";
	}
}
