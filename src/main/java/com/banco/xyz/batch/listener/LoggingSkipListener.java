package com.banco.xyz.batch.listener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

/**
 * Registra omisiones en consola, errores.csv y el archivo de la particion.
 */
@Component
public class LoggingSkipListener implements SkipListener<Object, Object> {

	private static final Logger log = LoggerFactory.getLogger(LoggingSkipListener.class);

	@Override
	public void onSkipInRead(Throwable t) {
		log.info("Entro a onSkipInRead: {}", t.getClass().getName());
		if (t instanceof FlatFileParseException parseEx) {
			log.warn("Linea omitida debido a un error en la lectura: {}", parseEx.getInput());
			escribirError("READ", parseEx.getInput(), t);
		} else {
			log.warn("[SKIP-READ] Se omitio un registro al leer: {}", t.getMessage());
			escribirError("READ", "", t);
		}
	}

	@Override
	public void onSkipInProcess(Object item, Throwable t) {
		log.info("Tipo de excepcion en SkipListener: {}", t.getClass().getName());
		log.warn("SkipListener activado - Error al procesar registro: {}", item);
		log.warn("[SKIP-PROCESS] Item omitido por excepcion: item={}, causa={}", item, t.getMessage());
		escribirError("PROCESS", String.valueOf(item), t);
		log.info("Registro escrito en errores.csv: {}", item);
	}

	@Override
	public void onSkipInWrite(Object item, Throwable t) {
		log.warn("[SKIP-WRITE] Item omitido al escribir: item={}, causa={}", item, t.getMessage());
		escribirError("WRITE", String.valueOf(item), t);
	}

	private synchronized void escribirError(String fase, String item, Throwable t) {
		String linea = String.format("%s,%s,%s,\"%s\",\"%s\"%n",
				Thread.currentThread().getName(), fase, t.getClass().getSimpleName(),
				escapar(item), escapar(t.getMessage()));
		try {
			Files.createDirectories(ErrorFileStepExecutionListener.ERROR_DIR);
			append(ErrorFileStepExecutionListener.ERROR_FILE, linea);
			Path partitionFile = currentPartitionFile();
			if (partitionFile != null) {
				append(partitionFile, linea);
			}
		} catch (IOException ex) {
			log.error("No se pudo escribir el error omitido", ex);
		}
	}

	private Path currentPartitionFile() {
		var context = StepSynchronizationManager.getContext();
		if (context == null || context.getStepExecution() == null) {
			return null;
		}
		return ErrorFileStepExecutionListener.partitionFile(context.getStepExecution().getStepName());
	}

	private void append(Path file, String linea) throws IOException {
		if (!Files.exists(file)) {
			Files.writeString(file, "hilo,fase,tipoError,item,error" + System.lineSeparator(),
					StandardCharsets.UTF_8, StandardOpenOption.CREATE);
		}
		Files.writeString(file, linea, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
				StandardOpenOption.APPEND);
	}

	private String escapar(String value) {
		if (value == null) {
			return "";
		}
		return value.replace("\"", "'").replace('\n', ' ').replace('\r', ' ');
	}
}
