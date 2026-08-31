# Banco XYZ — Migración de procesos batch (Spring Batch)

Proyecto sumativo **Exp1 / Semana 3 (PBY2203)** — *Optimizando procesos batch para mejorar la resiliencia de procesos*.

Continúa la modernización del sistema legacy del Banco XYZ con **particiones**, **procesamiento paralelo**, **políticas de reintento/omisión** y **comparación de parámetros** para elegir la configuración de escalado.

## Objetivo

Migrar tres procesos batch desde archivos CSV legacy (`bank_legacy_data`) hacia una base de datos relacional:

1. **Reporte de Transacciones Diarias** (`reporteTransaccionesDiariasJob`)
2. **Cálculo de Intereses Mensuales** (`calculoInteresesMensualesJob`)
3. **Estados de Cuenta Anuales** (`estadosCuentaAnualesJob`)

Cada Job usa el flujo **PartitionStep (manager) → minion/worker (ItemReader → ItemProcessor → ItemWriter)**.

## Requisitos

- Java 17+
- Maven Wrapper incluido (`mvnw` / `mvnw.cmd`)

## Estructura del código

```
src/main/java/com/banco/xyz/
├── XyzApplication.java
├── batch/
│   ├── BatchJobsRunner.java
│   ├── config/BatchInfrastructureConfig.java   # TaskExecutor + BackOffPolicy
│   ├── partition/CsvLineRangePartitioner.java  # rangos start/end
│   ├── support/LegacyDateParser.java
│   ├── exception/InvalidDataException.java
│   ├── policy/                                 # SkipPolicy, RetryPolicy, Decider
│   ├── listener/                               # skip, errores, [PERF], resumen
│   ├── processor/
│   ├── job/                                    # 3 jobs de produccion
│   └── benchmark/                              # comparacion gridSize 1/2/3/4
└── domain/

src/main/resources/
├── application.properties
├── schema.sql
└── data/
    ├── transacciones.csv
    ├── intereses.csv
    └── cuentas_anuales.csv
```

## Escalado (semana 3) — particiones

Se eligió **particionado local** (`TaskExecutorPartitionHandler`) en lugar de solo multi-thread sobre un único reader, porque:

- Cada partición procesa un rango `start`/`end` independiente del CSV.
- Un fallo se aísla en el worker (`minionStep:partitionN`).
- Coincide con la guía de la semana (PartitionStep, PartitionHandler, gridSize).

| Parámetro | Valor de producción | Dónde |
|-----------|---------------------|--------|
| Técnica | Particiones locales | `*PartitionStep` + `CsvLineRangePartitioner` |
| gridSize | **3** | `bank.batch.grid-size` |
| Chunk size | **10** | `bank.batch.chunk-size` |
| Hilos | **3** (prefijo `taskExecutor-`) | `bank.batch.threads` |
| SkipPolicy | omite parseo inválido y `InvalidDataException` (máx. 2000) | `FileVerificationSkipper` |
| RetryPolicy | reintenta `TransientDataAccessException` (3 veces) | `CustomRetryPolicy` |
| BackOffPolicy | exponencial 80ms ×2, tope 800ms | `exponentialBackOffPolicy` |
| Decider | `RETRY` si el manager falla; `COMPLETED_WITH_SKIPS` si hubo omisiones | `JobCompletionDecider` |

### Comparación de parámetros

Al arrancar (si `bank.batch.compare-params=true`) se ejecuta el job de transacciones con **gridSize 1, 2, 3 y 4** (mismo chunk=10). Los tiempos quedan en consola y en `output/comparacion_escalado.txt`.

Medición real de esta entrega:

| gridSize | chunk | Tiempo | Persistidos |
|----------|-------|--------|-------------|
| 1 | 10 | 1219 ms | 476 |
| 2 | 10 | 635 ms | 476 |
| 3 | 10 | **623 ms** (óptimo) | 476 |
| 4 | 10 | 629 ms | 476 |

La configuración de producción **gridSize=3 / chunk=10** es la más rápida y coincide con `setGridSize(3)` de la guía.

## Reglas de negocio / manejo de errores

| Proceso | Validaciones principales |
|--------|---------------------------|
| Transacciones | Fechas legacy (varios formatos); omite tipos inválidos, incompletos y duplicados; **marca anomalía** si monto ≤ 0 (no se omite) |
| Intereses | Solo `ahorro` y `prestamo`; omite saldo ≤ 0, edad fuera de 18–100, incompletos y duplicados; `hipoteca`/`unknown` se filtran |
| Estados anuales | Normaliza `depósito` → `deposito`; omite montos ≤ 0 y tipos no válidos (`pago`); clasifica INGRESO/EGRESO |

### Estrategia de errores

1. **`InvalidDataException`**: la lanzan reader/processor ante datos inconsistentes.
2. **`FileVerificationSkipper`**: omite parseo inválido del CSV y datos de negocio.
3. **`CustomRetryPolicy` + BackOff exponencial**: reintenta fallas transitorias de BD.
4. **`LoggingSkipListener`**: consola + `output/errores.csv` + un archivo por partición.
5. **`PerformanceStepListener`**: duración, read/write/skip y heap (`[PERF]`).
6. **`JobCompletionDecider`**: reejecuta el PartitionStep si falla; cierra con `COMPLETED_WITH_SKIPS`.

## Base de datos

**H2** embebida (`./data/bankxyz`) en modo PostgreSQL. Pool Hikari de 12 conexiones para los workers.

Tablas:

- `transacciones_diarias`
- `cuentas_con_interes`
- `estados_cuenta_anuales`

Consola (si dejas la app en ejecución): http://localhost:8080/h2-console  
JDBC URL: `jdbc:h2:file:./data/bankxyz;MODE=PostgreSQL;AUTO_SERVER=TRUE`  
Usuario: `sa` (sin clave)

## Cómo ejecutar

Desde la carpeta `xyz`:

```bash
.\mvnw.cmd -DskipTests spring-boot:run
```

`BatchJobsRunner`:

1. Compara gridSize 1/2/3/4 sobre transacciones.
2. Ejecuta los tres Jobs de producción con 3 particiones.
3. Finaliza el proceso (la evidencia queda en consola y en `output/`).

### Compilar

```bash
.\mvnw.cmd -DskipTests package
```

## Evidencia de ejecución

Tras correr la aplicación deberías ver:

- `TaskExecutor de particiones inicializado`
- `Partitioner [...] partition0/1/2 -> start=... end=...`
- Workers `...WorkerStep:partition0` en hilos `taskExecutor-*`
- `CustomSkipPolicy - Excepcion omitida`
- `[SKIP-PROCESS]` / `onSkipInRead`
- `JobExecutionDecider - finalizacion COMPLETED_WITH_SKIPS`
- Bloques `[PERF]` por partición y por manager
- Tabla `COMPARACION DE ESCALADO`
- `===== Job '...' COMPLETADO =====` con filas persistidas

Archivos generados:

| Archivo | Contenido |
|---------|-----------|
| `evidencia_ejecucion.txt` | Salida de consola completa (comparación + 3 jobs + resultados) |
| `output/comparacion_escalado.txt` | Tiempos de gridSize 1/2/3/4 |
| `output/errores.csv` | Omisiones consolidadas de producción |
| `output/errores-*.csv` | Omisiones por partición |

Resultados de producción (CSV de `bank_legacy_data`, ~1000 filas c/u):

| Job | Persistidos | Notas |
|-----|-------------|--------|
| Transacciones | 476 (89 anomalías de monto ≤ 0) | Tipos inválidos, fechas rotas y duplicados omitidos |
| Intereses | 202 cuentas con saldo final | Hipoteca/unknown filtradas; incompletos omitidos |
| Estados anuales | 657 movimientos | Informe por cuenta (INGRESO/EGRESO) |

## Propuesta técnica (resumen)

- **Arquitectura**: un Job por proceso; PartitionStep manager + worker chunk-oriented; flujo con `JobExecutionDecider`.
- **Lectura**: `FlatFileItemReader` **step-scoped** que usa `start`/`end` del `ExecutionContext`.
- **Particionado**: `CsvLineRangePartitioner` reparte las líneas de datos del CSV.
- **Transformación**: `ItemProcessor` con reglas de consistencia y normalización (tildes, fechas).
- **Escritura**: `JdbcBatchItemWriter` hacia H2.
- **Paralelismo**: `TaskExecutor` local, gridSize=3.
- **Tolerancia a fallos**: SkipPolicy + RetryPolicy + BackOffPolicy; reintento de job si el manager falla.
- **Observabilidad**: SkipListener, `[PERF]`, archivo de errores por partición y comparación de parámetros.
