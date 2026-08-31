package com.banco.xyz.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

/**
 * Logs de rendimiento para ajustar chunk size, hilos y uso de memoria.
 */
@Component
public class PerformanceStepListener implements StepExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(PerformanceStepListener.class);

	@Override
	public void beforeStep(StepExecution stepExecution) {
		log.info("[PERF] Inicio step={} hilo={} heapUsadoMB={}", stepExecution.getStepName(),
				Thread.currentThread().getName(), heapUsedMb());
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		long durationMs = 0L;
		if (stepExecution.getStartTime() != null && stepExecution.getEndTime() != null) {
			durationMs = java.time.Duration.between(stepExecution.getStartTime(), stepExecution.getEndTime())
					.toMillis();
		}
		double itemsPerSec = durationMs > 0
				? (stepExecution.getWriteCount() * 1000.0) / durationMs
				: stepExecution.getWriteCount();

		log.info(
				"[PERF] Step={} status={} durationMs={} read={} write={} skip={} commit={} rollback={} itemsPorSeg={} heapUsadoMB={} hilo={}",
				stepExecution.getStepName(), stepExecution.getStatus(), durationMs, stepExecution.getReadCount(),
				stepExecution.getWriteCount(), stepExecution.getSkipCount(), stepExecution.getCommitCount(),
				stepExecution.getRollbackCount(), String.format("%.2f", itemsPerSec), heapUsedMb(),
				Thread.currentThread().getName());
		return stepExecution.getExitStatus();
	}

	private long heapUsedMb() {
		Runtime rt = Runtime.getRuntime();
		return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
	}
}
