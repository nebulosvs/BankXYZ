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
import com.banco.xyz.batch.processor.TransaccionItemProcessor;
import com.banco.xyz.batch.support.LegacyDateParser;
import com.banco.xyz.domain.Transaccion;

@Configuration
public class TransaccionesJobConfig {

	@Bean
	public Partitioner transaccionPartitioner() {
		return new CsvLineRangePartitioner(new ClassPathResource("data/transacciones.csv"), "transacciones");
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Transaccion> transaccionWorkerReader(
			@Value("#{stepExecutionContext['start'] ?: 1}") int start,
			@Value("#{stepExecutionContext['end'] ?: 999999}") int end) {
		int maxItems = Math.max(1, end - start + 1);
		return new FlatFileItemReaderBuilder<Transaccion>()
				.name("transaccionWorkerReader")
				.resource(new ClassPathResource("data/transacciones.csv"))
				.delimited()
				.names("id", "fecha", "monto", "tipo")
				.linesToSkip(start)
				.maxItemCount(maxItems)
				.fieldSetMapper(fieldSet -> {
					Transaccion t = new Transaccion();
					String id = fieldSet.readString("id");
					if (id != null && !id.isBlank()) {
						t.setId(Long.parseLong(id.trim()));
					}
					t.setFecha(LegacyDateParser.parse(fieldSet.readString("fecha")));
					String monto = fieldSet.readString("monto");
					if (monto != null && !monto.isBlank()) {
						t.setMonto(new BigDecimal(monto.trim()));
					}
					t.setTipo(fieldSet.readString("tipo"));
					return t;
				})
				.build();
	}

	@Bean
	public TransaccionItemProcessor transaccionItemProcessor() {
		return new TransaccionItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Transaccion> transaccionItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaccion>()
				.dataSource(dataSource)
				.sql("""
						INSERT INTO transacciones_diarias (id, fecha, monto, tipo, anomalia, observacion)
						VALUES (:id, :fecha, :monto, :tipo, :anomalia, :observacion)
						""")
				.beanMapped()
				.build();
	}

	@Bean
	public Step procesarTransaccionesWorkerStep(JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			FlatFileItemReader<Transaccion> transaccionWorkerReader,
			TransaccionItemProcessor transaccionItemProcessor,
			JdbcBatchItemWriter<Transaccion> transaccionItemWriter,
			FileVerificationSkipper fileVerificationSkipper,
			CustomRetryPolicy customRetryPolicy,
			BackOffPolicy exponentialBackOffPolicy,
			LoggingSkipListener loggingSkipListener,
			ErrorFileStepExecutionListener errorFileStepExecutionListener,
			PerformanceStepListener performanceStepListener,
			@Value("${bank.batch.chunk-size:10}") int chunkSize) {
		return new StepBuilder("procesarTransaccionesWorkerStep", jobRepository)
				.<Transaccion, Transaccion>chunk(chunkSize, transactionManager)
				.reader(transaccionWorkerReader)
				.processor(transaccionItemProcessor)
				.writer(transaccionItemWriter)
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
	public Step procesarTransaccionesPartitionStep(JobRepository jobRepository,
			Step procesarTransaccionesWorkerStep,
			Partitioner transaccionPartitioner,
			@Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor,
			PerformanceStepListener performanceStepListener,
			@Value("${bank.batch.grid-size:3}") int gridSize) {
		return new StepBuilder("procesarTransaccionesPartitionStep", jobRepository)
				.partitioner(procesarTransaccionesWorkerStep.getName(), transaccionPartitioner)
				.step(procesarTransaccionesWorkerStep)
				.gridSize(gridSize)
				.taskExecutor(batchTaskExecutor)
				.listener(performanceStepListener)
				.build();
	}

	@Bean
	public Job reporteTransaccionesDiariasJob(JobRepository jobRepository,
			Step procesarTransaccionesPartitionStep,
			JobExecutionDecider jobCompletionDecider,
			JobCompletionNotificationListener listener) {
		return new JobBuilder("reporteTransaccionesDiariasJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(procesarTransaccionesPartitionStep)
				.on("*").to(jobCompletionDecider)
				.from(jobCompletionDecider).on("RETRY").to(procesarTransaccionesPartitionStep)
				.from(jobCompletionDecider).on("FAILED").fail()
				.from(jobCompletionDecider).on("*").end()
				.end()
				.build();
	}
}
