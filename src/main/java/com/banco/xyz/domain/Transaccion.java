package com.banco.xyz.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public class Transaccion {

	private Long id;
	private LocalDate fecha;
	private BigDecimal monto;
	private String tipo;
	private boolean anomalia;
	private String observacion;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public LocalDate getFecha() {
		return fecha;
	}

	public void setFecha(LocalDate fecha) {
		this.fecha = fecha;
	}

	public BigDecimal getMonto() {
		return monto;
	}

	public void setMonto(BigDecimal monto) {
		this.monto = monto;
	}

	public String getTipo() {
		return tipo;
	}

	public void setTipo(String tipo) {
		this.tipo = tipo;
	}

	public boolean isAnomalia() {
		return anomalia;
	}

	public void setAnomalia(boolean anomalia) {
		this.anomalia = anomalia;
	}

	public String getObservacion() {
		return observacion;
	}

	public void setObservacion(String observacion) {
		this.observacion = observacion;
	}

	@Override
	public String toString() {
		return "Transaccion{id=" + id + ", fecha=" + fecha + ", monto=" + monto + ", tipo='" + tipo + "'}";
	}
}
