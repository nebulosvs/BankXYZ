package com.banco.xyz.batch.partition;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.core.io.Resource;

/**
 * Divide un CSV (con encabezado) en rangos de lineas start/end para cada particion.
 */
public class CsvLineRangePartitioner implements Partitioner {

	private static final Logger log = LoggerFactory.getLogger(CsvLineRangePartitioner.class);

	private final Resource resource;
	private final String nombre;

	public CsvLineRangePartitioner(Resource resource, String nombre) {
		this.resource = resource;
		this.nombre = nombre;
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		int dataLines = contarLineasDatos();
		int size = Math.max(1, gridSize);
		int window = (int) Math.ceil(dataLines / (double) size);

		Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
		int start = 1;
		int index = 0;
		while (start <= dataLines) {
			int end = Math.min(start + window - 1, dataLines);
			ExecutionContext context = new ExecutionContext();
			context.putInt("start", start);
			context.putInt("end", end);
			context.putInt("partitionNumber", index);
			context.putString("fileName", nombre);
			partitions.put("partition" + index, context);
			log.info("Partitioner [{}] partition{} -> start={} end={} (lineas={})",
					nombre, index, start, end, end - start + 1);
			start = end + 1;
			index++;
		}

		log.info("Partitioner [{}] archivo={} lineasDatos={} gridSolicitado={} particionesCreadas={}",
				nombre, resource.getFilename(), dataLines, gridSize, partitions.size());
		return partitions;
	}

	private int contarLineasDatos() {
		int total = 0;
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
			while (reader.readLine() != null) {
				total++;
			}
		} catch (Exception ex) {
			throw new IllegalStateException("No se pudo leer " + resource + " para particionar", ex);
		}
		return Math.max(0, total - 1);
	}
}
