# Banco XYZ — Migración de procesos batch (Spring Batch)

Proyecto formativo **Exp1 / Semana 1 (PBY2203)** para modernizar procesos legacy del Banco XYZ con Spring Batch.

## Objetivo

Migrar tres procesos batch desde archivos CSV legacy hacia una base de datos relacional:

1. **Reporte de Transacciones Diarias** (`reporteTransaccionesDiariasJob`)
2. **Cálculo de Intereses Mensuales** (`calculoInteresesMensualesJob`)
3. **Estados de Cuenta Anuales** (`estadosCuentaAnualesJob`)

Cada Job aplica el flujo **ItemReader → ItemProcessor → ItemWriter**, con omisión (`skip`) de registros inválidos o duplicados.

## Requisitos

- Java 17+
- Maven Wrapper incluido (`mvnw` / `mvnw.cmd`)

## Estructura del código

```
src/main/java/com/banco/xyz/
├── XyzApplication.java
├── batch/
│   ├── BatchJobsRunner.java          # Lanza los 3 jobs al iniciar
│   ├── exception/InvalidDataException.java
│   ├── listener/JobCompletionNotificationListener.java
│   ├── processor/                    # Validaciones y transformaciones
│   └── job/                          # Configuración de Jobs y Steps
└── domain/                           # Modelos de datos

src/main/resources/
├── application.properties
├── schema.sql
└── data/                             # CSV de semana_1 (bank_legacy_data)
    ├── transacciones.csv
    ├── intereses.csv
    └── cuentas_anuales.csv
```

## Reglas de negocio / manejo de errores

| Proceso | Validaciones principales |
|--------|---------------------------|
| Transacciones | Omite duplicados y tipos inválidos; marca anomalías si monto ≤ 0 |
| Intereses | Solo `ahorro` y `prestamo`; omite saldo ≤ 0, edad fuera de rango, duplicados e `hipoteca` |
| Estados anuales | Omite montos ≤ 0; normaliza descripción vacía; clasifica INGRESO/EGRESO |

### Estrategia de errores (alineada a la guía)

1. **`InvalidDataException`**: los `ItemProcessor` la lanzan ante datos inconsistentes (duplicados, saldo ≤ 0, montos inválidos en estados, etc.).
2. **`faultTolerant().skip(InvalidDataException)`**: el Step omite el registro defectuoso y continúa (SkipPolicy).
3. **`retry(TransientDataAccessException)`**: reintenta fallas transitorias de BD hasta 3 veces (RetryPolicy).
4. **`LoggingSkipListener`**: deja evidencia en consola de cada skip (`[SKIP-PROCESS]`).
5. **Filtro con `null`**: solo para casos de negocio que no son error (p. ej. `hipoteca` fuera del cálculo de intereses).
6. En transacciones, montos ≤ 0 se **marcan como anomalía** y se persisten (reporte), no se omiten.

## Base de datos

Se usa **H2** embebida (archivo `./data/bankxyz`) en modo PostgreSQL, alineada al proyecto base de Spring Initializr y a la guía de la semana.

Tablas de negocio:

- `transacciones_diarias`
- `cuentas_con_interes`
- `estados_cuenta_anuales`

## Cómo ejecutar

Desde la carpeta `xyz`:

```bash
.\mvnw.cmd spring-boot:run
```

Al arrancar, `BatchJobsRunner` ejecuta los tres Jobs en secuencia y deja un resumen en consola.

### Compilar

```bash
.\mvnw.cmd -DskipTests package
```

## Evidencia de ejecución

Tras correr la aplicación, en la consola deberías ver:

- Inicio de cada Job (`>>> Lanzando job: ...`)
- Registros omitidos / anomalías en logs
- Bloques `===== Job '...' COMPLETADO =====` con resumen de filas persistidas
- Mensaje final `Migracion batch finalizada correctamente`

Resultado esperado con los CSV de `semana_1`:

| Job | Resultado |
|-----|-----------|
| Transacciones | 9 filas (2 anomalías; 1 duplicado omitido) |
| Intereses | 5 cuentas con saldo final actualizado |
| Estados anuales | 6 movimientos válidos |

## Propuesta técnica (resumen)

- **Arquitectura**: un Job por proceso legacy; un Step chunk-oriented por Job.
- **Lectura**: `FlatFileItemReader` sobre CSV en classpath.
- **Transformación**: `ItemProcessor` con reglas de consistencia.
- **Escritura**: `JdbcBatchItemWriter` hacia H2.
- **Tolerancia a fallos**: `faultTolerant().skip(InvalidDataException).skipLimit(50)`.
- **Trazabilidad**: `JobCompletionNotificationListener` imprime resultados persistidos.
