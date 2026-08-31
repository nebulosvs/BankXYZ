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
import com.banco.xyz.batch.processor.EstadoCuentaItemProcessor;
import com.banco.xyz.batch.support.LegacyDateParser;
import com.banco.xyz.domain.MovimientoAnual;

@Configuration
public class EstadosCuentaJobConfig {

	@Bean
	public Partitioner estadosCuentaPartitioner() {
		return new CsvLineRangePartitioner(new ClassPathResource("data/cuentas_anuales.csv"), "estados-cuenta");
	}

	@Bean
	@StepScope
	public FlatFileItemReader<MovimientoAnual> estadosCuentaWorkerReader(
			@Value("#{stepExecutionContext['start'] ?: 1}") int start,
			@Value("#{stepExecutionContext['end'] ?: 999999}") int end) {
		int maxItems = Math.max(1, end - start + 1);
		return new FlatFileItemReaderBuilder<MovimientoAnual>()
				.name("estadosCuentaWorkerReader")
				.resource(new ClassPathResource("data/cuentas_anuales.csv"))
				.delimited()
				.names("cuentaId", "fecha", "transaccion", "monto", "descripcion")
				.linesToSkip(start)
				.maxItemCount(maxItems)
				.fieldSetMapper(fieldSet -> {
					MovimientoAnual m = new MovimientoAnual();
					String cuentaId = fieldSet.readString("cuentaId");
					if (cuentaId != null && !cuentaId.isBlank()) {
						m.setCuentaId(Long.parseLong(cuentaId.trim()));
					}
					m.setFecha(LegacyDateParser.parse(fieldSet.readString("fecha")));
					m.setTransaccion(fieldSet.readString("transaccion"));
					String monto = fieldSet.readString("monto");
					if (monto != null && !monto.isBlank()) {
						m.setMonto(new BigDecimal(monto.trim()));
					}
					m.setDescripcion(fieldSet.readString("descripcion"));
					return m;
				})
				.build();
	}

	@Bean
	public EstadoCuentaItemProcessor estadoCuentaItemProcessor() {
		return new EstadoCuentaItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<MovimientoAnual> estadoCuentaItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<MovimientoAnual>()
				.dataSource(dataSource)
				.sql("""
						INSERT INTO estados_cuenta_anuales
						(cuenta_id, fecha, transaccion, monto, descripcion, clasificacion)
						VALUES (:cuentaId, :fecha, :transaccion, :monto, :descripcion, :clasificacion)
						""")
				.beanMapped()
				.build();
	}

	@Bean
	public Step generarEstadosCuentaWorkerStep(JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			FlatFileItemReader<MovimientoAnual> estadosCuentaWorkerReader,
			EstadoCuentaItemProcessor estadoCuentaItemProcessor,
			JdbcBatchItemWriter<MovimientoAnual> estadoCuentaItemWriter,
			FileVerificationSkipper fileVerificationSkipper,
			CustomRetryPolicy customRetryPolicy,
			BackOffPolicy exponentialBackOffPolicy,
			LoggingSkipListener loggingSkipListener,
			ErrorFileStepExecutionListener errorFileStepExecutionListener,
			PerformanceStepListener performanceStepListener,
			@Value("${bank.batch.chunk-size:10}") int chunkSize) {
		return new StepBuilder("generarEstadosCuentaWorkerStep", jobRepository)
				.<MovimientoAnual, MovimientoAnual>chunk(chunkSize, transactionManager)
				.reader(estadosCuentaWorkerReader)
				.processor(estadoCuentaItemProcessor)
				.writer(estadoCuentaItemWriter)
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
	public Step generarEstadosCuentaPartitionStep(JobRepository jobRepository,
			Step generarEstadosCuentaWorkerStep,
			Partitioner estadosCuentaPartitioner,
			@Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor,
			PerformanceStepListener performanceStepListener,
			@Value("${bank.batch.grid-size:3}") int gridSize) {
		return new StepBuilder("generarEstadosCuentaPartitionStep", jobRepository)
				.partitioner(generarEstadosCuentaWorkerStep.getName(), estadosCuentaPartitioner)
				.step(generarEstadosCuentaWorkerStep)
				.gridSize(gridSize)
				.taskExecutor(batchTaskExecutor)
				.listener(performanceStepListener)
				.build();
	}

	@Bean
	public Job estadosCuentaAnualesJob(JobRepository jobRepository,
			Step generarEstadosCuentaPartitionStep,
			JobExecutionDecider jobCompletionDecider,
			JobCompletionNotificationListener listener) {
		return new JobBuilder("estadosCuentaAnualesJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(generarEstadosCuentaPartitionStep)
				.on("*").to(jobCompletionDecider)
				.from(jobCompletionDecider).on("RETRY").to(generarEstadosCuentaPartitionStep)
				.from(jobCompletionDecider).on("FAILED").fail()
				.from(jobCompletionDecider).on("*").end()
				.end()
				.build();
	}
}
