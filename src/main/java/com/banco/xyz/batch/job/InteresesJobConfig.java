package com.banco.xyz.batch.job;

import java.math.BigDecimal;

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
import com.banco.xyz.batch.processor.InteresItemProcessor;
import com.banco.xyz.domain.CuentaInteres;

@Configuration
public class InteresesJobConfig {

	@Bean
	public FlatFileItemReader<CuentaInteres> interesItemReader() {
		return new FlatFileItemReaderBuilder<CuentaInteres>()
				.name("interesItemReader")
				.resource(new ClassPathResource("data/intereses.csv"))
				.delimited()
				.names("cuentaId", "nombre", "saldo", "edad", "tipo")
				.linesToSkip(1)
				.fieldSetMapper(fieldSet -> {
					CuentaInteres c = new CuentaInteres();
					c.setCuentaId(fieldSet.readLong("cuentaId"));
					c.setNombre(fieldSet.readString("nombre"));
					String saldo = fieldSet.readString("saldo");
					if (saldo != null && !saldo.isBlank()) {
						c.setSaldo(new BigDecimal(saldo.trim()));
					}
					c.setEdad(fieldSet.readInt("edad"));
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
	public Step calcularInteresesStep(JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			FlatFileItemReader<CuentaInteres> interesItemReader,
			InteresItemProcessor interesItemProcessor,
			JdbcBatchItemWriter<CuentaInteres> interesItemWriter,
			LoggingSkipListener loggingSkipListener) {
		return new StepBuilder("calcularInteresesStep", jobRepository)
				.<CuentaInteres, CuentaInteres>chunk(1, transactionManager)
				.reader(interesItemReader)
				.processor(interesItemProcessor)
				.writer(interesItemWriter)
				.faultTolerant()
				.skip(InvalidDataException.class)
				.skipLimit(50)
				.retry(TransientDataAccessException.class)
				.retryLimit(3)
				.listener(loggingSkipListener)
				.build();
	}

	@Bean
	public Job calculoInteresesMensualesJob(JobRepository jobRepository,
			Step calcularInteresesStep,
			JobCompletionNotificationListener listener) {
		return new JobBuilder("calculoInteresesMensualesJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(calcularInteresesStep)
				.build();
	}
}
