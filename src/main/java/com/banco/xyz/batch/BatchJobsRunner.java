package com.banco.xyz.batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.banco.xyz.batch.benchmark.EscaladoComparisonCollector;
import com.banco.xyz.batch.listener.ErrorFileStepExecutionListener;
import com.banco.xyz.batch.processor.InteresItemProcessor;
import com.banco.xyz.batch.processor.TransaccionItemProcessor;

@Component
public class BatchJobsRunner implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(BatchJobsRunner.class);

	private final JobLauncher jobLauncher;
	private final Job reporteTransaccionesDiariasJob;
	private final Job calculoInteresesMensualesJob;
	private final Job estadosCuentaAnualesJob;
	private final Job comparacionEscaladoGrid1Job;
	private final Job comparacionEscaladoGrid2Job;
	private final Job comparacionEscaladoGrid3Job;
	private final Job comparacionEscaladoGrid4Job;
	private final JdbcTemplate jdbcTemplate;
	private final EscaladoComparisonCollector comparisonCollector;
	private final TransaccionItemProcessor transaccionItemProcessor;
	private final InteresItemProcessor interesItemProcessor;
	private final ConfigurableApplicationContext applicationContext;
	private final boolean compareParams;
	private final int chunkSize;

	public BatchJobsRunner(JobLauncher jobLauncher,
			@Qualifier("reporteTransaccionesDiariasJob") Job reporteTransaccionesDiariasJob,
			@Qualifier("calculoInteresesMensualesJob") Job calculoInteresesMensualesJob,
			@Qualifier("estadosCuentaAnualesJob") Job estadosCuentaAnualesJob,
			@Qualifier("comparacionEscaladoGrid1Job") Job comparacionEscaladoGrid1Job,
			@Qualifier("comparacionEscaladoGrid2Job") Job comparacionEscaladoGrid2Job,
			@Qualifier("comparacionEscaladoGrid3Job") Job comparacionEscaladoGrid3Job,
			@Qualifier("comparacionEscaladoGrid4Job") Job comparacionEscaladoGrid4Job,
			JdbcTemplate jdbcTemplate,
			EscaladoComparisonCollector comparisonCollector,
			TransaccionItemProcessor transaccionItemProcessor,
			InteresItemProcessor interesItemProcessor,
			ConfigurableApplicationContext applicationContext,
			@Value("${bank.batch.compare-params:true}") boolean compareParams,
			@Value("${bank.batch.chunk-size:10}") int chunkSize) {
		this.jobLauncher = jobLauncher;
		this.reporteTransaccionesDiariasJob = reporteTransaccionesDiariasJob;
		this.calculoInteresesMensualesJob = calculoInteresesMensualesJob;
		this.estadosCuentaAnualesJob = estadosCuentaAnualesJob;
		this.comparacionEscaladoGrid1Job = comparacionEscaladoGrid1Job;
		this.comparacionEscaladoGrid2Job = comparacionEscaladoGrid2Job;
		this.comparacionEscaladoGrid3Job = comparacionEscaladoGrid3Job;
		this.comparacionEscaladoGrid4Job = comparacionEscaladoGrid4Job;
		this.jdbcTemplate = jdbcTemplate;
		this.comparisonCollector = comparisonCollector;
		this.transaccionItemProcessor = transaccionItemProcessor;
		this.interesItemProcessor = interesItemProcessor;
		this.applicationContext = applicationContext;
		this.compareParams = compareParams;
		this.chunkSize = chunkSize;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		log.info("Iniciando migracion batch Banco XYZ (particiones, chunk={})", chunkSize);
		prepararSalida();

		if (compareParams) {
			log.info(">>> Fase 1: comparacion de parametros de escalado (gridSize 1/2/3/4)");
			ejecutarComparacion(comparacionEscaladoGrid1Job, 1);
			ejecutarComparacion(comparacionEscaladoGrid2Job, 2);
			ejecutarComparacion(comparacionEscaladoGrid3Job, 3);
			ejecutarComparacion(comparacionEscaladoGrid4Job, 4);
			comparisonCollector.logTabla();
			comparisonCollector.escribirInforme(Path.of("output", "comparacion_escalado.txt"));
		}

		log.info(">>> Fase 2: jobs de produccion (gridSize=3, chunk={})", chunkSize);
		limpiarTablas();
		prepararSalida();
		ejecutarProduccion(reporteTransaccionesDiariasJob);
		ejecutarProduccion(calculoInteresesMensualesJob);
		ejecutarProduccion(estadosCuentaAnualesJob);

		log.info("Migracion batch finalizada correctamente");
		SpringApplication.exit(applicationContext, () -> 0);
	}

	private void ejecutarComparacion(Job job, int gridSize) throws Exception {
		limpiarTablas();
		transaccionItemProcessor.reset();
		interesItemProcessor.reset();
		var params = new JobParametersBuilder()
				.addLong("timestamp", System.currentTimeMillis())
				.addLong("gridSize", (long) gridSize)
				.addLong("chunkSize", (long) chunkSize)
				.toJobParameters();
		log.info(">>> Comparacion job={} gridSize={} chunkSize={}", job.getName(), gridSize, chunkSize);
		jobLauncher.run(job, params);
	}

	private void ejecutarProduccion(Job job) throws Exception {
		transaccionItemProcessor.reset();
		interesItemProcessor.reset();
		var params = new JobParametersBuilder()
				.addLong("timestamp", System.currentTimeMillis())
				.toJobParameters();
		log.info(">>> Lanzando job de produccion: {}", job.getName());
		jobLauncher.run(job, params);
	}

	private void limpiarTablas() {
		jdbcTemplate.update("DELETE FROM transacciones_diarias");
		jdbcTemplate.update("DELETE FROM cuentas_con_interes");
		jdbcTemplate.update("DELETE FROM estados_cuenta_anuales");
	}

	private void prepararSalida() {
		try {
			Files.createDirectories(ErrorFileStepExecutionListener.ERROR_DIR);
			Files.deleteIfExists(ErrorFileStepExecutionListener.ERROR_FILE);
			try (var stream = Files.list(ErrorFileStepExecutionListener.ERROR_DIR)) {
				stream.filter(p -> {
					String name = p.getFileName().toString();
					return name.startsWith("errores-") && name.endsWith(".csv");
				}).forEach(p -> {
					try {
						Files.deleteIfExists(p);
					} catch (IOException ignored) {
						// se regenera en el siguiente step
					}
				});
			}
		} catch (IOException ex) {
			log.warn("No se pudo limpiar la carpeta de errores: {}", ex.getMessage());
		}
	}
}
