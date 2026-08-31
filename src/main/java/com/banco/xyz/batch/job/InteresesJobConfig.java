package com.banco.xyz.batch.job;

import java.math.BigDecimal;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.transaction.PlatformTransactionManager;

import com.banco.xyz.batch.listener.ErrorFileStepExecutionListener;
import com.banco.xyz.batch.listener.JobCompletionNotificationListener;
import com.banco.xyz.batch.listener.LoggingSkipListener;
import com.banco.xyz.batch.listener.PerformanceStepListener;
import com.banco.xyz.batch.partition.CsvLineRangePartitioner;
import com.banco.xyz.batch.policy.CustomRetryPolicy;
import com.banco.xyz.batch.policy.FileVerificationSkipper;
import com.banco.xyz.batch.processor.InteresItemProcessor;
import com.banco.xyz.domain.CuentaInteres;

@Configuration
public class InteresesJobConfig {

	@Bean
	public Partitioner interesPartitioner() {
		return new CsvLineRangePartitioner(new ClassPathResource("data/intereses.csv"), "intereses");
	}

	@Bean
	@StepScope
	public FlatFileItemReader<CuentaInteres> interesWorkerReader(
			@Value("#{stepExecutionContext['start'] ?: 1}") int start,
			@Value("#{stepExecutionContext['end'] ?: 999999}") int end) {
		int maxItems = Math.max(1, end - start + 1);
		return new FlatFileItemReaderBuilder<CuentaInteres>()
				.name("interesWorkerReader")
				.resource(new ClassPathResource("data/intereses.csv"))
				.delimited()
				.names("cuentaId", "nombre", "saldo", "edad", "tipo")
				.linesToSkip(start)
				.maxItemCount(maxItems)
				.fieldSetMapper(fieldSet -> {
					CuentaInteres c = new CuentaInteres();
					String cuentaId = fieldSet.readString("cuentaId");
					if (cuentaId != null && !cuentaId.isBlank()) {
						c.setCuentaId(Long.parseLong(cuentaId.trim()));
					}
					c.setNombre(fieldSet.readString("nombre"));
					String saldo = fieldSet.readString("saldo");
					if (saldo != null && !saldo.isBlank()) {
						c.setSaldo(new BigDecimal(saldo.trim()));
					}
					String edad = fieldSet.readString("edad");
					if (edad != null && !edad.isBlank()) {
						c.setEdad(Integer.parseInt(edad.trim()));
					}
					c.setTipo(fieldSet.readString("tipo"));
					return c;
				})
				.build();
	}

	@Bean
	public InteresItemProcessor interesItemProcessor() {
		return new InteresItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<CuentaInteres> interesItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<CuentaInteres>()
				.dataSource(dataSource)
				.sql("""
						INSERT INTO cuentas_con_interes
						(cuenta_id, nombre, saldo_inicial, edad, tipo, tasa_interes, interes_calculado, saldo_final)
						VALUES (:cuentaId, :nombre, :saldo, :edad, :tipo, :tasaInteres, :interesCalculado, :saldoFinal)
						""")
				.beanMapped()
				.build();
	}

	@Bean
	public Step calcularInteresesWorkerStep(JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			FlatFileItemReader<CuentaInteres> interesWorkerReader,
			InteresItemProcessor interesItemProcessor,
			JdbcBatchItemWriter<CuentaInteres> interesItemWriter,
			FileVerificationSkipper fileVerificationSkipper,
			CustomRetryPolicy customRetryPolicy,
			BackOffPolicy exponentialBackOffPolicy,
			LoggingSkipListener loggingSkipListener,
			ErrorFileStepExecutionListener errorFileStepExecutionListener,
			PerformanceStepListener performanceStepListener,
			@Value("${bank.batch.chunk-size:10}") int chunkSize) {
		return new StepBuilder("calcularInteresesWorkerStep", jobRepository)
				.<CuentaInteres, CuentaInteres>chunk(chunkSize, transactionManager)
				.reader(interesWorkerReader)
				.processor(interesItemProcessor)
				.writer(interesItemWriter)
				.faultTolerant()
				.processorNonTransactional()
				.skipPolicy(fileVerificationSkipper)
				.retryPolicy(customRetryPolicy)
				.backOffPolicy(exponentialBackOffPolicy)
				.listener(loggingSkipListener)
				.listener(errorFileStepExecutionListener)
				.listener(performanceStepListener)
				.build();
	}

	@Bean
	public Step calcularInteresesPartitionStep(JobRepository jobRepository,
			Step calcularInteresesWorkerStep,
			Partitioner interesPartitioner,
			@Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor,
			PerformanceStepListener performanceStepListener,
			@Value("${bank.batch.grid-size:3}") int gridSize) {
		return new StepBuilder("calcularInteresesPartitionStep", jobRepository)
				.partitioner(calcularInteresesWorkerStep.getName(), interesPartitioner)
				.step(calcularInteresesWorkerStep)
				.gridSize(gridSize)
				.taskExecutor(batchTaskExecutor)
				.listener(performanceStepListener)
				.build();
	}

	@Bean
	public Job calculoInteresesMensualesJob(JobRepository jobRepository,
			Step calcularInteresesPartitionStep,
			JobExecutionDecider jobCompletionDecider,
			JobCompletionNotificationListener listener) {
		return new JobBuilder("calculoInteresesMensualesJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(calcularInteresesPartitionStep)
				.on("*").to(jobCompletionDecider)
				.from(jobCompletionDecider).on("RETRY").to(calcularInteresesPartitionStep)
				.from(jobCompletionDecider).on("FAILED").fail()
				.from(jobCompletionDecider).on("*").end()
				.end()
				.build();
	}
}
