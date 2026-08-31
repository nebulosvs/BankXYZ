package com.banco.xyz.batch.benchmark;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

import com.banco.xyz.batch.listener.PerformanceStepListener;

@Configuration
public class ScalingComparisonConfig {

	@Bean
	public Job comparacionEscaladoGrid1Job(JobRepository jobRepository,
			@Qualifier("procesarTransaccionesWorkerStep") Step worker,
			@Qualifier("transaccionPartitioner") Partitioner partitioner,
			@Qualifier("batchTaskExecutor") TaskExecutor executor,
			PerformanceStepListener performanceStepListener,
			ComparisonTimingListener comparisonTimingListener) {
		return comparisonJob(jobRepository, "comparacionEscaladoGrid1Job", 1, worker, partitioner, executor,
				performanceStepListener, comparisonTimingListener);
	}

	@Bean
	public Job comparacionEscaladoGrid2Job(JobRepository jobRepository,
			@Qualifier("procesarTransaccionesWorkerStep") Step worker,
			@Qualifier("transaccionPartitioner") Partitioner partitioner,
			@Qualifier("batchTaskExecutor") TaskExecutor executor,
			PerformanceStepListener performanceStepListener,
			ComparisonTimingListener comparisonTimingListener) {
		return comparisonJob(jobRepository, "comparacionEscaladoGrid2Job", 2, worker, partitioner, executor,
				performanceStepListener, comparisonTimingListener);
	}

	@Bean
	public Job comparacionEscaladoGrid3Job(JobRepository jobRepository,
			@Qualifier("procesarTransaccionesWorkerStep") Step worker,
			@Qualifier("transaccionPartitioner") Partitioner partitioner,
			@Qualifier("batchTaskExecutor") TaskExecutor executor,
			PerformanceStepListener performanceStepListener,
			ComparisonTimingListener comparisonTimingListener) {
		return comparisonJob(jobRepository, "comparacionEscaladoGrid3Job", 3, worker, partitioner, executor,
				performanceStepListener, comparisonTimingListener);
	}

	@Bean
	public Job comparacionEscaladoGrid4Job(JobRepository jobRepository,
			@Qualifier("procesarTransaccionesWorkerStep") Step worker,
			@Qualifier("transaccionPartitioner") Partitioner partitioner,
			@Qualifier("batchTaskExecutor") TaskExecutor executor,
			PerformanceStepListener performanceStepListener,
			ComparisonTimingListener comparisonTimingListener) {
		return comparisonJob(jobRepository, "comparacionEscaladoGrid4Job", 4, worker, partitioner, executor,
				performanceStepListener, comparisonTimingListener);
	}

	private Job comparisonJob(JobRepository jobRepository, String name, int gridSize, Step worker,
			Partitioner partitioner, TaskExecutor executor, PerformanceStepListener performanceStepListener,
			ComparisonTimingListener comparisonTimingListener) {
		Step manager = new StepBuilder(name + ".manager", jobRepository)
				.partitioner(worker.getName(), partitioner)
				.step(worker)
				.gridSize(gridSize)
				.taskExecutor(executor)
				.listener(performanceStepListener)
				.build();
		return new JobBuilder(name, jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(comparisonTimingListener)
				.start(manager)
				.build();
	}
}
