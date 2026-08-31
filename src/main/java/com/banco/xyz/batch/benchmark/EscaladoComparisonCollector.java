package com.banco.xyz.batch.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EscaladoComparisonCollector {

	private static final Logger log = LoggerFactory.getLogger(EscaladoComparisonCollector.class);

	public record Resultado(String job, int gridSize, int chunkSize, long durationMs, String status,
			long read, long write, long skip) {
	}

	private final List<Resultado> resultados = new ArrayList<>();

	public synchronized void registrar(Resultado resultado) {
		resultados.add(resultado);
	}

	public synchronized List<Resultado> resultados() {
		return List.copyOf(resultados);
	}

	public synchronized Resultado optimo() {
		return resultados.stream()
				.filter(r -> "COMPLETED".equals(r.status()))
				.min(Comparator.comparingLong(Resultado::durationMs))
				.orElse(null);
	}

	public synchronized void escribirInforme(Path path) {
		StringBuilder sb = new StringBuilder();
		sb.append("Comparacion de parametros de escalado (particiones)").append(System.lineSeparator());
		sb.append("job;gridSize;chunkSize;durationMs;status;read;write;skip").append(System.lineSeparator());
		resultados.forEach(r -> sb.append(r.job()).append(';')
				.append(r.gridSize()).append(';')
				.append(r.chunkSize()).append(';')
				.append(r.durationMs()).append(';')
				.append(r.status()).append(';')
				.append(r.read()).append(';')
				.append(r.write()).append(';')
				.append(r.skip()).append(System.lineSeparator()));
		Resultado best = optimo();
		if (best != null) {
			sb.append(System.lineSeparator())
					.append("Configuracion mas rapida: gridSize=")
					.append(best.gridSize())
					.append(", chunkSize=")
					.append(best.chunkSize())
					.append(", durationMs=")
					.append(best.durationMs())
					.append(System.lineSeparator());
			sb.append("Configuracion de produccion elegida: gridSize=3, chunkSize=10 ")
					.append("(equilibrio entre tiempo, hilos y overhead de particiones, alineada a la guia).")
					.append(System.lineSeparator());
		}
		try {
			Files.createDirectories(path.getParent());
			Files.writeString(path, sb.toString(), StandardCharsets.UTF_8);
			log.info("Informe de comparacion escrito en {}", path.toAbsolutePath());
		} catch (IOException ex) {
			log.error("No se pudo escribir el informe de comparacion", ex);
		}
	}

	public synchronized void logTabla() {
		log.info("========== COMPARACION DE ESCALADO ==========");
		log.info(String.format("%-32s %8s %8s %12s %12s %8s %8s %8s",
				"job", "grid", "chunk", "ms", "status", "read", "write", "skip"));
		resultados.forEach(r -> log.info(String.format("%-32s %8d %8d %12d %12s %8d %8d %8d",
				r.job(), r.gridSize(), r.chunkSize(), r.durationMs(), r.status(), r.read(), r.write(), r.skip())));
		Resultado best = optimo();
		if (best != null) {
			log.info("Mas rapida: {} ({} ms). Produccion: gridSize=3 chunk=10", best.job(), best.durationMs());
		}
		log.info("============================================");
	}
}
