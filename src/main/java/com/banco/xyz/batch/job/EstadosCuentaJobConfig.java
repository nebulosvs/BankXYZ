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
import com.banco.xyz.batch.processor.EstadoCuentaItemProcessor;
import com.banco.xyz.domain.MovimientoAnual;

@Configuration
public class EstadosCuentaJobConfig {

	@Bean
	public FlatFileItemReader<MovimientoAnual> movimientoAnualItemReader() {
		return new FlatFileItemReaderBuilder<MovimientoAnual>()
				.name("movimientoAnualItemReader")
				.resource(new ClassPathResource("data/cuentas_anuales.csv"))
				.delimited()
				.names("cuentaId", "fecha", "transaccion", "monto", "descripcion")
				.linesToSkip(1)
				.fieldSetMapper(fieldSet -> {
					MovimientoAnual m = new MovimientoAnual();
					m.setCuentaId(fieldSet.readLong("cuentaId"));
					m.setFecha(LocalDate.parse(fieldSet.readString("fecha").replace('/', '-')));
					m.setTransaccion(fieldSet.readString("transaccion"));
					m.setMonto(new BigDecimal(fieldSet.readString("monto")));
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
	public Step generarEstadosCuentaStep(JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			FlatFileItemReader<MovimientoAnual> movimientoAnualItemReader,
			EstadoCuentaItemProcessor estadoCuentaItemProcessor,
			JdbcBatchItemWriter<MovimientoAnual> estadoCuentaItemWriter,
			LoggingSkipListener loggingSkipListener) {
		return new StepBuilder("generarEstadosCuentaStep", jobRepository)
				.<MovimientoAnual, MovimientoAnual>chunk(1, transactionManager)
				.reader(movimientoAnualItemReader)
				.processor(estadoCuentaItemProcessor)
				.writer(estadoCuentaItemWriter)
				.faultTolerant()
				.skip(InvalidDataException.class)
				.skipLimit(50)
				.retry(TransientDataAccessException.class)
				.retryLimit(3)
				.listener(loggingSkipListener)
				.build();
	}

	@Bean
	public Job estadosCuentaAnualesJob(JobRepository jobRepository,
			Step generarEstadosCuentaStep,
			JobCompletionNotificationListener listener) {
		return new JobBuilder("estadosCuentaAnualesJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(generarEstadosCuentaStep)
				.build();
	}
}
