package com.banco.xyz.batch.listener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

/**
 * Prepara el archivo consolidado y uno por particion (guia semana 3).
 */
@Component
public class ErrorFileStepExecutionListener implements StepExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(ErrorFileStepExecutionListener.class);
	public static final Path ERROR_DIR = Path.of("output");
	public static final Path ERROR_FILE = ERROR_DIR.resolve("errores.csv");

	@Override
	public void beforeStep(StepExecution stepExecution) {
		log.info("Se ejecuto el ErrorFileStepExecutionListener");
		log.info("Abre el archivo de errores al inicio (step={})", stepExecution.getStepName());
		try {
			Files.createDirectories(ERROR_DIR);
			if (!Files.exists(ERROR_FILE)) {
				escribirCabecera(ERROR_FILE);
			}
			Path partitionFile = partitionFile(stepExecution.getStepName());
			if (partitionFile != null && !Files.exists(partitionFile)) {
				escribirCabecera(partitionFile);
				log.info("Archivo de errores de particion: {}", partitionFile);
			}
		} catch (IOException ex) {
			log.error("No se pudo preparar archivos de error", ex);
		}
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		log.info("Cierra el archivo de errores al final (step={}, read={}, write={}, skip={})",
				stepExecution.getStepName(), stepExecution.getReadCount(), stepExecution.getWriteCount(),
				stepExecution.getSkipCount());
		return stepExecution.getExitStatus();
	}

	public static Path partitionFile(String stepName) {
		if (stepName == null || !stepName.contains(":partition")) {
			return null;
		}
		String safe = stepName.replace(':', '-').replace(' ', '_');
		return ERROR_DIR.resolve("errores-" + safe + ".csv");
	}

	private static void escribirCabecera(Path file) throws IOException {
		Files.writeString(file, "hilo,fase,tipoError,item,error" + System.lineSeparator(),
				StandardCharsets.UTF_8, StandardOpenOption.CREATE);
	}
}
