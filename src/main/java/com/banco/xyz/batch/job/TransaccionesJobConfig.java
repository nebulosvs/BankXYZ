package com.banco.xyz.batch.job;

import java.math.BigDecimal;
import java.time.LocalDate;

import javax.sql.DataSource;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;

import com.banco.xyz.batch.exception.InvalidDataException;
import com.banco.xyz.batch.listener.JobCompletionNotificationListener;
import com.banco.xyz.batch.listener.LoggingSkipListener;
import com.banco.xyz.batch.processor.TransaccionItemProcessor;
import com.banco.xyz.domain.Transaccion;

@Configuration
public class TransaccionesJobConfig {

	@Bean
	public FlatFileItemReader<Transaccion> transaccionItemReader() {
		return new FlatFileItemReaderBuilder<Transaccion>()
				.name("transaccionItemReader")
				.resource(new ClassPathResource("data/transacciones.csv"))
				.delimited()
				.names("id", "fecha", "monto", "tipo")
				.linesToSkip(1)
				.fieldSetMapper(fieldSet -> {
					Transaccion t = new Transaccion();
					t.setId(fieldSet.readLong("id"));
					t.setFecha(LocalDate.parse(fieldSet.readString("fecha").replace('/', '-')));
					t.setMonto(new BigDecimal(fieldSet.readString("monto")));
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
	public Step procesarTransaccionesStep(JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			FlatFileItemReader<Transaccion> transaccionItemReader,
			TransaccionItemProcessor transaccionItemProcessor,
			JdbcBatchItemWriter<Transaccion> transaccionItemWriter,
			LoggingSkipListener loggingSkipListener) {
		return new StepBuilder("procesarTransaccionesStep", jobRepository)
				.<Transaccion, Transaccion>chunk(1, transactionManager)
				.reader(transaccionItemReader)
				.processor(transaccionItemProcessor)
				.writer(transaccionItemWriter)
				.faultTolerant()
				.skip(InvalidDataException.class)
				.skipLimit(50)
				.retry(TransientDataAccessException.class)
				.retryLimit(3)
				.listener(loggingSkipListener)
				.build();
	}

	@Bean
	public Job reporteTransaccionesDiariasJob(JobRepository jobRepository,
			Step procesarTransaccionesStep,
			JobCompletionNotificationListener listener) {
		return new JobBuilder("reporteTransaccionesDiariasJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(procesarTransaccionesStep)
				.build();
	}
}
