package com.banco.xyz.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		String jobName = jobExecution.getJobInstance().getJobName();
		if (jobExecution.getStatus() != BatchStatus.COMPLETED) {
			log.error("Job '{}' finalizo con estado {}", jobName, jobExecution.getStatus());
			return;
		}

		log.info("===== Job '{}' COMPLETADO =====", jobName);
		switch (jobName) {
			case "reporteTransaccionesDiariasJob" -> resumenTransacciones();
			case "calculoInteresesMensualesJob" -> resumenIntereses();
			case "estadosCuentaAnualesJob" -> resumenEstados();
			default -> log.info("Sin resumen configurado para {}", jobName);
		}
	}

	private void resumenTransacciones() {
		Integer total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM transacciones_diarias", Integer.class);
		Integer anomalias = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM transacciones_diarias WHERE anomalia = TRUE", Integer.class);
		log.info("Resumen transacciones: total={}, anomalias={}", total, anomalias);
		jdbcTemplate.query("SELECT id, fecha, monto, tipo, anomalia, observacion FROM transacciones_diarias ORDER BY id",
				rs -> {
					log.info(" -> id={}, fecha={}, monto={}, tipo={}, anomalia={}, obs={}", rs.getLong("id"),
							rs.getDate("fecha"), rs.getBigDecimal("monto"), rs.getString("tipo"),
							rs.getBoolean("anomalia"), rs.getString("observacion"));
				});
	}

	private void resumenIntereses() {
		Integer total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM cuentas_con_interes", Integer.class);
		log.info("Resumen intereses: cuentas actualizadas={}", total);
		jdbcTemplate.query(
				"SELECT cuenta_id, nombre, tipo, saldo_inicial, interes_calculado, saldo_final FROM cuentas_con_interes ORDER BY cuenta_id",
				rs -> {
					log.info(" -> cuenta={}, nombre={}, tipo={}, saldoInicial={}, interes={}, saldoFinal={}",
							rs.getLong("cuenta_id"), rs.getString("nombre"), rs.getString("tipo"),
							rs.getBigDecimal("saldo_inicial"), rs.getBigDecimal("interes_calculado"),
							rs.getBigDecimal("saldo_final"));
				});
	}

	private void resumenEstados() {
		Integer total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM estados_cuenta_anuales", Integer.class);
		log.info("Resumen estados de cuenta anuales: movimientos validos={}", total);
		jdbcTemplate.query(
				"SELECT cuenta_id, COUNT(*) AS movimientos, SUM(monto) AS total FROM estados_cuenta_anuales GROUP BY cuenta_id ORDER BY cuenta_id",
				rs -> {
					log.info(" -> cuenta={}, movimientos={}, totalMonto={}", rs.getLong("cuenta_id"),
							rs.getInt("movimientos"), rs.getBigDecimal("total"));
				});
	}
}
