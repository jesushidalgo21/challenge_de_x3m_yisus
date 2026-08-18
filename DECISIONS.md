# DECISIONS.md

## 1. Decisiones técnicas y trade-offs

### Postgres como única base
Un solo motor cubre metadata de Airflow, capa raw, staging y mart (bases/esquemas
separados). Menos piezas móviles = más probabilidad de que levante en un entorno
ajeno, que es la condición excluyente del challenge.

### Raw en JSONB, sin aplanar en Python
La extracción persiste la respuesta cruda de la API en `raw_products` / `raw_carts`
(columna JSONB + `load_date`) y no transforma nada. El aplanado a columnas tipadas
ocurre en dbt.

Trade-off: aplanar en Python sería más directo y haría la validación temprana más
fácil. Se eligió lo contrario para poder reprocesar toda la lógica de parseo desde
raw sin volver a pegarle a la API, y para mantener un solo lugar donde vive la
transformación (versionado y testeado en dbt) en vez de repartirla entre un script
y modelos SQL.

Lo único que sí ocurre en Python es un chequeo estructural liviano de la respuesta
(forma esperada, no vacía) antes de escribir en raw — evita ensuciar la capa cruda
con una respuesta rota, sin invadir el rol de dbt.

### dbt en vez de PySpark
El volumen es de cientos de filas. PySpark no aporta paralelismo aprovechable acá
y sumaría un runtime más al compose. dbt corre instalado en la misma imagen que
Airflow (no como contenedor propio) para evitar Docker-in-Docker, que es
justamente el tipo de dependencia que puede no levantar en otra máquina.

### `dbt build` en vez de `dbt run` + `dbt test`
`build` respeta el grafo: testea staging antes de construir el mart. Si un test de
staging falla, el mart no se construye y el dato malo no se propaga. Con run y test
como tasks separadas, el mart se construiría igual y el fallo se detectaría después.

### La fecha la genera el pipeline, no el dato
Ni Products ni Carts traen timestamp. Cada corrida representa "las ventas del día
X", y `date` sale de la fecha lógica de Airflow (`{{ ds }}`), nunca de
`datetime.now()`. Con wall-clock, un backfill escribiría la fecha de hoy en todas
las corridas y se pisarían entre sí.

### Particionado lógico, no físico
`product_daily_revenue` es incremental con estrategia merge y `unique_key`
(product_id, date), más un test de unicidad como red de seguridad. Correr la misma
fecha dos veces actualiza, no duplica.

No se usó particionado declarativo de Postgres: con este volumen no habría pruning
que ganar y sumaría complejidad sin beneficio. A escala media (años de histórico)
la respuesta sería `PARTITION BY RANGE` sobre `date` — mensual o diaria — por
pruning en queries por rango y retención vía `DROP PARTITION`. A escala real de big
data, saldría de un único Postgres hacia un lake particionado por fecha
(`date=YYYY-MM-DD/`) procesado con Spark. La clave de partición es la fecha en los
tres escenarios porque el patrón de consulta es por rango de fechas, no por
categoría.

### Revenue = `discountedTotal`
El descuento es menor ingreso, no un gasto: esa plata nunca entró. `total` (precio
de lista) sobreestimaría el revenue real. No se calcula ganancia porque Products no
expone costo.

### Solo filas con ventas
Una fact table modela eventos, no su ausencia. Un cross join producto×fecha
generaría mayoría de ceros sin información nueva. Si una vista necesita mostrar
huecos, se resuelve con LEFT JOIN en la query, no persistiendo ceros.

### `max_active_runs=1` (encontrado corriendo, no leyendo)
El backfill paralelo expuso colisiones de DDL concurrente: primero dos `load_raw`
creando el mismo esquema (`CREATE SCHEMA IF NOT EXISTS` no es atómico bajo
concurrencia en Postgres), después cinco procesos de `dbt build` creando la misma
vista de staging. El primer fix fue un advisory lock; el segundo caso mostró que
parchear DDL puntual era tratar el síntoma. `max_active_runs=1` ataca la causa y
además es el comportamiento correcto para un batch diario: no hay valor real en
paralelizar fechas.

### Backfill explícito, no `catchup`
El backfill es un comando documentado en el README, no un efecto automático del
scheduler. Con `catchup=True` el resultado depende de cuándo levantó el scheduler
y de que el DAG no esté pausado — el evaluador podría abrir el dashboard y no ver
nada sin saber por qué. Como comando explícito, el resultado es determinístico.

### Sin dependencias de red en el build
El constraints file de Airflow está vendorizado en el repo tras un 503 real de
`raw.githubusercontent.com` durante un build. Por la misma razón no se usó
`dbt_utils`: se escribieron macros de test propias en vez de depender de que
`dbt deps` baje algo de internet en cada arranque. Cada descarga en el camino
crítico es un modo de falla en la máquina del evaluador.

## 2. Flujo de trabajo con IA

Usé Claude Code como pair programmer durante todo el proyecto. El reparto fue
deliberado: cerré la arquitectura antes de escribir una línea de código y se la
pasé como restricciones no negociables (raw/staging/mart, incremental + merge,
LocalExecutor, sin Docker-in-Docker, revenue neto, fecha lógica de Airflow). El
trabajo de la IA fue setup e implementación dentro de esos límites, con la
instrucción explícita de escalarme cualquier decisión no cubierta en vez de
asumirla.

Lo que más valor aportó no fue la generación de código sino la verificación:
buildear las imágenes, levantar Postgres, correr backfills reales, repetidas veces.
Los bugs que importaron —el `CREATE SCHEMA` no atómico, la colisión de vistas de
staging bajo `dbt build` paralelo, `PIP_USER=true` de la imagen base rompiendo el
venv de dbt, el bit ejecutable perdido al commitear desde Windows— no aparecen
leyendo código. Aparecen ejecutándolo.

Lo que corregí o cuestioné a mano:
- El código y los comentarios estaban en español; pedí pasarlos a inglés.
- Cuestioné `date` como nombre de columna por chocar con el tipo de Postgres. La
  respuesta fue que el enunciado lo exige para la tabla de salida, así que no era
  evitable ahí; se ofreció confinarlo solo a esa tabla y no propagarlo a los
  modelos internos. Decidí no aplicarlo para no divergir del contrato de salida.
- Rechacé alcance no pedido (CI, features extra) cada vez que apareció.
- Ajusté `pytest` a 7.4.4 al ver que la versión que había pedido chocaba con el
  constraints file de Airflow.

La verificación final fue clonar el repo pusheado en un directorio limpio y correr
los tres comandos exactos del README más la query de validación y pytest, aislado
del entorno de desarrollo — para eliminar cualquier "funciona en mi máquina".

## 3. Alcance

Verificado end-to-end: backfill de 5 fechas (2026-08-12 a 2026-08-16), 11 tests de
pytest, tests nativos de dbt incluyendo unicidad del mart, dashboard renderizando
en browser real, y clon limpio corriendo el README literal.

Fuera de alcance, a propósito:

- **CI.** No estaba pedido explícitamente. Preferí no sumar alcance no solicitado.
- **Observabilidad.** Solo logs nativos de Airflow: sin logging estructurado,
  alertas ni métricas custom. En producción esto es lo primero que agregaría —
  alertas sobre fallo de tasks y métricas de freshness y volumen por corrida.
- **`dbt_utils`.** Reemplazado por macros propias para no depender de red en el
  arranque.
- **Dashboard mínimo.** Tres vistas, sin auth, cache con TTL de 30s, consulta la DB
  en cada request. Es una prueba de que el mart es consumible, no un producto.

Qué haría distinto en producción real:

- La API es estática y se asume que cambia a medianoche. Con una fuente real habría
  que decidir qué hacer con ventas retroactivas (carritos que llegan tarde) —
  probablemente ventana de reproceso de N días en vez de merge solo del día actual.
- Contratos de datos sobre la fuente y detección de cambios de esquema, en vez de un
  chequeo estructural de forma.
- Retención sobre la capa raw: hoy crece indefinidamente sin política de purga.
- Trazabilidad de linaje y freshness expuestas fuera de dbt (docs o catálogo), no
  solo como tests que corren en el pipeline.
