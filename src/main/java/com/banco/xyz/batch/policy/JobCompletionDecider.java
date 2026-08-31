package com.banco.xyz.batch.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Finalizacion / reejecucion. Agrega skips de workers particionados.
 */
@Component
public class JobCompletionDecider implements JobExecutionDecider {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionDecider.class);
	private static final String RETRY_COUNT_KEY = "jobRetryCount";

	private final int maxJobRetries;

	public JobCompletionDecider(@Value("${bank.batch.job-retry-limit:1}") int maxJobRetries) {
		this.maxJobRetries = maxJobRetries;
	}

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
		long retryCount = jobExecution.getExecutionContext().getLong(RETRY_COUNT_KEY, 0L);
		BatchStatus status = stepExecution != null ? stepExecution.getStatus() : jobExecution.getStatus();
		long skips = stepExecution != null
				? stepExecution.getSkipCount()
				: jobExecution.getStepExecutions().stream()
						.filter(s -> !s.getStepName().contains(":partition"))
						.mapToLong(StepExecution::getSkipCount)
						.sum();

		log.info("JobExecutionDecider - job={}, step={}, stepStatus={}, skipsTotales={}, reintentoJob={}",
				jobExecution.getJobInstance().getJobName(),
				stepExecution != null ? stepExecution.getStepName() : "-", status, skips, retryCount);

		if (status == BatchStatus.FAILED && retryCount < maxJobRetries) {
			jobExecution.getExecutionContext().putLong(RETRY_COUNT_KEY, retryCount + 1);
			log.warn("JobExecutionDecider - el step fallo; se solicita RETRY ({}/{})",
					retryCount + 1, maxJobRetries);
			return new FlowExecutionStatus("RETRY");
		}

		if (status == BatchStatus.FAILED) {
			log.error("JobExecutionDecider - sin reintentos restantes; el job se marca FAILED");
			return new FlowExecutionStatus("FAILED");
		}

		if (skips > 0) {
			log.info("JobExecutionDecider - finalizacion COMPLETED_WITH_SKIPS ({} omisiones)", skips);
			return new FlowExecutionStatus("COMPLETED_WITH_SKIPS");
		}

		log.info("JobExecutionDecider - finalizacion COMPLETED");
		return FlowExecutionStatus.COMPLETED;
	}
}
