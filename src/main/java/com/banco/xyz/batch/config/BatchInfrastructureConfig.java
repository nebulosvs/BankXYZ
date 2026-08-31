package com.banco.xyz.batch.config;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchInfrastructureConfig {

	private static final Logger log = LoggerFactory.getLogger(BatchInfrastructureConfig.class);

	@Bean
	public PlatformTransactionManager transactionManager(DataSource dataSource) {
		return new JdbcTransactionManager(dataSource);
	}

	/**
	 * Pool local para PartitionHandler (un hilo por particion). Prefijo visible en logs.
	 */
	@Bean(name = "batchTaskExecutor")
	public TaskExecutor batchTaskExecutor(
			@Value("${bank.batch.threads:3}") int threads,
			@Value("${bank.batch.queue-capacity:25}") int queueCapacity) {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(threads);
		executor.setMaxPoolSize(Math.max(threads, 4));
		executor.setQueueCapacity(queueCapacity);
		executor.setThreadNamePrefix("taskExecutor-");
		executor.setWaitForTasksToCompleteOnShutdown(true);
		executor.initialize();
		log.info("TaskExecutor de particiones inicializado: core={}, max={}, cola={}, prefix=taskExecutor-",
				threads, Math.max(threads, 4), queueCapacity);
		return executor;
	}

	@Bean
	public BackOffPolicy exponentialBackOffPolicy(
			@Value("${bank.batch.backoff-initial-ms:80}") long initial,
			@Value("${bank.batch.backoff-multiplier:2.0}") double multiplier,
			@Value("${bank.batch.backoff-max-ms:800}") long max) {
		ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
		policy.setInitialInterval(initial);
		policy.setMultiplier(multiplier);
		policy.setMaxInterval(max);
		log.info("BackOffPolicy exponencial: initial={}ms, multiplier={}, max={}ms", initial, multiplier, max);
		return policy;
	}
}
