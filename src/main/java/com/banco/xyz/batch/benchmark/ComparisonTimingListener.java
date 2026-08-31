package com.banco.xyz.batch.benchmark;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class ComparisonTimingListener implements JobExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(ComparisonTimingListener.class);

	private final EscaladoComparisonCollector collector;

	public ComparisonTimingListener(EscaladoComparisonCollector collector) {
		this.collector = collector;
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		Long grid = jobExecution.getJobParameters().getLong("gridSize");
		log.info("Comparacion - inicio job={} gridSize={}", jobExecution.getJobInstance().getJobName(),
				grid != null ? grid : -1L);
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		long duration = 0L;
		if (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null) {
			duration = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
		}
		StepExecution manager = jobExecution.getStepExecutions().stream()
				.filter(s -> !s.getStepName().contains(":partition"))
				.findFirst()
				.orElse(null);
		long read = manager != null ? manager.getReadCount() : 0L;
		long write = manager != null ? manager.getWriteCount() : 0L;
		long skip = manager != null ? manager.getSkipCount() : 0L;
		Long gridParam = jobExecution.getJobParameters().getLong("gridSize");
		Long chunkParam = jobExecution.getJobParameters().getLong("chunkSize");
		int grid = gridParam != null ? gridParam.intValue() : 0;
		int chunk = chunkParam != null ? chunkParam.intValue() : 10;
		collector.registrar(new EscaladoComparisonCollector.Resultado(
				jobExecution.getJobInstance().getJobName(), grid, chunk, duration,
				jobExecution.getStatus().name(), read, write, skip));
		log.info("Comparacion - fin job={} durationMs={} status={} read={} write={} skip={}",
				jobExecution.getJobInstance().getJobName(), duration, jobExecution.getStatus(), read, write, skip);
	}
}
