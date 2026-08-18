package com.banco.xyz.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class BatchJobsRunner implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(BatchJobsRunner.class);

	private final JobLauncher jobLauncher;
	private final Job reporteTransaccionesDiariasJob;
	private final Job calculoInteresesMensualesJob;
	private final Job estadosCuentaAnualesJob;

	public BatchJobsRunner(JobLauncher jobLauncher,
			Job reporteTransaccionesDiariasJob,
			Job calculoInteresesMensualesJob,
			Job estadosCuentaAnualesJob) {
		this.jobLauncher = jobLauncher;
		this.reporteTransaccionesDiariasJob = reporteTransaccionesDiariasJob;
		this.calculoInteresesMensualesJob = calculoInteresesMensualesJob;
		this.estadosCuentaAnualesJob = estadosCuentaAnualesJob;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		log.info("Iniciando migracion batch Banco XYZ (3 jobs)");

		ejecutar(reporteTransaccionesDiariasJob);
		ejecutar(calculoInteresesMensualesJob);
		ejecutar(estadosCuentaAnualesJob);

		log.info("Migracion batch finalizada correctamente");
	}

	private void ejecutar(Job job) throws Exception {
		var params = new JobParametersBuilder()
				.addLong("timestamp", System.currentTimeMillis())
				.toJobParameters();
		log.info(">>> Lanzando job: {}", job.getName());
		jobLauncher.run(job, params);
	}
}
