# Guion de la exposición: «¿Cuánto viven los discos?»

Duración estimada: unos 17 minutos más preguntas.

| Orden | Expone     | Bloque                                               | Tiempo |
| ----- | ---------- | ---------------------------------------------------- | ------ |
| 1     | Juan José  | Problema, datos, arquitectura y Parte A (MapReduce)  | ~6 min |
| 2     | Jairo      | Parte B (balanceo de carga) y Parte C (Kafka)        | ~6 min |
| 3     | Antonio    | Parte D (MongoDB), calidad y conclusiones            | ~5 min |

> Los textos entre comillas son una propuesta para decir en voz alta; adáptenlos a su forma de hablar.
> Lo que va en *[corchetes]* son acciones (mostrar terminal, abrir un archivo, etc.).

---

## 1. Juan José: introducción, arquitectura y Parte A (~6 min)

### Apertura (30 s)

«Buenas. Somos Juan José, Jairo y Antonio. Nuestro proyecto responde una pregunta concreta:
**¿cuánto viven los discos duros y qué modelos fallan más?** Para responderla usamos datos
reales de Backblaze, una empresa que publica cada día el estado de todos sus discos.
Lo resolvimos en cuatro partes: MapReduce, balanceo de carga, streaming con Kafka y MongoDB.
Yo presento el problema y la Parte A, Jairo las partes B y C, y Antonio la Parte D y el cierre.»

### Los datos y la métrica (1 min)

- Cada día Backblaze publica un CSV con **una fila por disco**: fecha, serial, modelo y
  `failure` (0 o 1).
- «Lo importante es que **cada fila es un disco en un día**, es decir, un *día-disco*. Contar
  filas nos da la exposición al riesgo y sumar `failure` nos da las fallas.»
- Muestra por defecto: **1 al 7 de abril de 2026**, ~160 MB, 346.921 discos, 78 modelos.
- La métrica: **AFR (%) = fallas ÷ (días-disco ÷ 365) × 100**, con un intervalo de confianza
  exacto del 95 % (Poisson).
- «Validamos la fórmula con las cifras oficiales de Backblaze para Q2 2026: da 1,73 %, y eso
  está como test en `tests/test_metrics.py`.»

### Arquitectura (1 min 30 s)

*[Mostrar el diagrama del README o la terminal con `docker compose up`]*

«Todo corre con **un solo comando: `docker compose up`**. Solo hace falta Docker; Python,
Java 21 y Spark van dentro de una única imagen, así el driver y los executors tienen
exactamente las mismas versiones.»

```text
pipeline-a ──► pipeline-b ──► pipeline-c ──► pipeline-d
 MapReduce      balanceo       streaming       MongoDB
```

- Infraestructura: Spark (1 master + 2 workers), Kafka y MongoDB.
- Las etapas se encadenan con `depends_on: service_completed_successfully`: **si una falla, el
  pipeline se corta ahí** y no se calcula nada sobre datos malos.
- Descarga inteligente: «El ZIP del trimestre pesa 2,1 GB. No lo bajamos entero: leemos su
  índice con **peticiones HTTP Range** y descargamos solo los días que faltan (~23 MB cada uno).
  Cada archivo se verifica con **CRC32** contra un `manifest.json`; si está truncado, se baja de
  nuevo.»
- El pipeline completo tarda unos 22 minutos (la mayor parte es la Parte B).

### Parte A: MapReduce (2 min)

«La Parte A es un MapReduce clásico con RDD. Por cada fila emitimos la pareja
`(modelo, (1, falla))` y la sumamos con `reduceByKey`.»

```python
pairs = rows.map(lambda r: (r[MODEL], (1, r[FAILURE])))
by_model = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
```

- «La primera suma son los días-disco y la segunda las fallas de cada modelo.»
- **Verificación**: «Calculamos lo mismo con DataFrames (`groupBy().agg()`) y comparamos modelo
  a modelo. Si un solo par no coincide, el job termina con error. Coincidieron los 78 modelos.»
- Resultados:
  - **2.419.966 días-disco, 108 fallas.**
  - **AFR global: 1,63 %** (IC 95 %: 1,34 – 1,97 %). «El 1,73 % oficial cae dentro del
    intervalo, así que nuestra muestra de una semana es coherente con el trimestre.»
  - «Pero **7 días son pocos datos por modelo**: 57 de los 78 modelos tienen menos de 10.000
    días-disco y su AFR no es interpretable. Solo dos modelos tienen el intervalo entero por
    encima del promedio: HGST HUH721212ALN604 (11,97 %) y ST24000NM002H (5,03 %).»

### Censura (1 min)

«Con la misma técnica, usando el serial como clave, calculamos cuántos días se vio cada disco.»

- «El **99,97 % de los discos sigue vivo** al final de la ventana. Eso se llama **censura por
  la derecha**: solo sabemos que vivieron *al menos* esos días.»
- «Además hay truncamiento por la izquierda: los discos ya funcionaban antes del 1 de abril.
  Para conocer su edad real haría falta `smart_9_raw` (horas encendido).»
- La salida `vida_por_serial.csv` es la entrada para un análisis de Kaplan-Meier.

**Transición:** «Hasta aquí, el cálculo es correcto. La pregunta siguiente es si está bien
repartido entre los workers. Eso lo cuenta Jairo.»

---

## 2. Jairo: Parte B (balanceo) y Parte C (streaming) (~6 min)

### Parte B: el experimento (1 min)

«En la Parte B repetimos el MapReduce de la Parte A con **2, 8, 32 y 200 particiones**,
5 veces cada una, y medimos la mediana.»

- «Para medir bien: los datos se reparten y se cachean **antes** de cronometrar, y hacemos una
  ejecución de calentamiento para que arranquen los procesos de Python de cada executor. Así
  solo medimos el MapReduce, no la lectura de los CSV.»
- «Además comparamos dos formas de repartir: **por clave** (`repartition(n, 'model')`, hash del
  modelo) y **sin clave** (`repartition(n)`, round-robin), y contamos las filas de cada
  partición con `spark_partition_id()`.»

### Parte B: resultados de tiempo (1 min)

*[Mostrar `output/part_b/tiempos_resumen.csv` de su ejecución]*

> ⚠️ Jairo: los tiempos no están guardados en el repositorio. Antes de exponer, ejecuta
> `docker compose run --rm part-b` y llena esta tabla con la columna `median_s` y `speedup`.

| Particiones | Mediana (s) | Speedup vs. 2 |
| ----------- | ----------- | ------------- |
| 2           | [__]        | 1,00          |
| 8           | [__]        | [__]          |
| 32          | [__]        | [__]          |
| 200         | [__]        | [__]          |

Idea a transmitir: «Más particiones no siempre es más rápido. Con pocas no aprovechamos los
núcleos; con demasiadas, el costo de programar tantas tareas pequeñas y del *shuffle* domina.
Con 200 particiones cada repetición tardó alrededor de un minuto.»

### Parte B: el sesgo por clave (1 min 30 s)

«Al repartir por modelo, todas las filas de un modelo caen en la misma partición; no se pueden
separar. El modelo más grande, **WDC WUH722222ALE6L4, tiene el 13,2 % de las filas**
(319.093 de 2.419.966).»

- «Eso pone un **piso** al desbalance: la partición más cargada tiene al menos ese 13,2 %.
  Medido como *máximo ÷ media* (1 = reparto perfecto):»

| Particiones | Piso con clave = modelo | Sin clave (round-robin) |
| ----------- | ----------------------- | ----------------------- |
| 8           | ≥ 1,05                  | ≈ 1                     |
| 32          | ≥ 4,2                   | ≈ 1                     |
| 200         | ≥ 26,4                  | ≈ 1                     |

- «Y con 200 particiones y solo 78 modelos, **al menos 122 particiones quedan vacías**.»
- Conclusión: «La tarea más cargada marca el ritmo de toda la etapa. Repartir por una clave
  con pocos valores y muy desiguales desperdicia el clúster.»

*[Opcional: completar con los valores reales de `sesgo_resumen.csv`]*

**Transición:** «Esta lección la aplicamos directamente en la Parte C, con Kafka.»

### Parte C: el flujo (1 min)

```text
CSV ──► producer ──JSON──► Kafka (topic «drivestats», 4 particiones, clave = serial)
                                   │
                                   ▼
               Spark Structured Streaming: ventanas de 7 días + watermark de 2 días
```

- «Un producer en Python publica los registros **un día cada 10 segundos** (reloj acelerado),
  filtrando 5 modelos: unos 120.000 eventos por día.»
- «La clave del mensaje es el **serial**, no el modelo. Resultado: las 4 particiones quedaron
  casi iguales, ~210.000 mensajes cada una. Con clave = modelo, el WDC (38 % de estos eventos)
  habría saturado una sola partición: el mismo sesgo de la Parte B.»
- «Para simular datos desordenados, **un 2 % de los discos llega con un día de retraso**.»

### Parte C: ventanas y watermark (1 min)

- «Usamos **tiempo de evento**, no de llegada: un evento del 3 de abril que llega el 4 va a la
  ventana del 3.»
- «El **watermark** es la fecha máxima vista menos 2 días; significa "ya no espero eventos
  anteriores a esto". Cuando pasa el final de una ventana, Spark la **cierra**, la emite como
  definitiva y borra su estado.»
- Dos salidas: `update` (la AFR en vivo, que cambia lote a lote) y `append` (cada ventana una
  sola vez, al cerrarse).
- Ejemplo: «La AFR en vivo del modelo HGST pasó por 15,98 %, 3,84 %, 7,66 %... hasta quedar en
  11,97 %. Con poca exposición la tasa salta mucho, igual que vimos en la Parte A.»
- Verificación: «Al terminar, comparamos el streaming contra el mismo cálculo en batch:
  **coinciden los 5 modelos**, incluidos los 16.610 eventos que llegaron tarde.»

### Parte C: experimento de eventos tardíos (30 s)

«Con ventanas de 3 días probamos retrasos mayores que el watermark:»

- Retraso de 4 días: llegaron en el **mismo lote** que cerró la ventana → se contaron.
- Retraso de 5 días: llegaron **un lote después** → se descartaron **2.373 eventos**, justo los
  tardíos del 3 de abril.
- «La garantía del watermark es en un solo sentido: lo que llega dentro del umbral se cuenta
  siempre; lo que llega después *puede o no* contarse. Y una ventana cerrada no se reabre.»

**Transición:** «Ya tenemos agregados por ventana. Falta guardarlos y consultarlos. Eso lo
presenta Antonio.»

---

## 3. Antonio: Parte D (MongoDB) y cierre (~5 min)

### Parte D: qué guardamos (1 min 30 s)

«La Parte D es la cuarta etapa del pipeline. Spark agrega por **modelo y ventana de 7 días**,
reutilizando el mismo código de ventanas de la Parte C, y guarda **un documento por par** en
`drivestats_db.model_aggregates`.»

```js
{
  model: 'HGST HUH721212ALN604',
  window_start: ISODate('2026-04-01'),
  window_end:   ISODate('2026-04-08'),   // exclusivo
  total_days: 67082,
  total_failures: 22,
  afr: 11.97
}
```

- «Un **índice único sobre `(window_start, model)`** impide duplicar un par; cada ejecución
  reemplaza los documentos de la anterior.»
- «¿Por qué en batch y no desde el streaming? Porque el streaming solo publica 5 modelos, y un
  top 5 entre 5 modelos no dice nada. Aquí calculamos los 78.»
- «Si no hay CSV, termina con error **sin tocar Mongo**: no inventamos resultados.»

### Parte D: la consulta (1 min 30 s)

«Con un pipeline de agregación de MongoDB sacamos los 5 modelos con mayor AFR de cada ventana:»

1. `$match`: solo modelos con al menos **100 días-disco** en la ventana.
2. `$group` por ventana con **`$topN`**: los 5 de mayor AFR.
3. `$sort` por fecha.

Resultado con la muestra (una sola ventana de 7 días):
HGST HUH721212ALN604 (11,97 %), WDC WUH721816ALE6L0 (5,27 %), ST24000NM002H (5,03 %),
ST8000DM002 (3,05 %) y ST14000NM001G (3,00 %).

- Consistencia: «Si sumamos las ventanas de cada modelo, salen exactamente los días-disco y
  fallas de la Parte A, en los 78 modelos. Y en los 5 modelos del streaming, los documentos son
  iguales a los de la Parte C.»

*[Opcional: demo en vivo]*
```bash
docker compose exec mongo mongosh drivestats_db --quiet --eval \
  'db.model_aggregates.find({}, {_id: 0}).sort({window_start: 1, afr: -1}).limit(5)'
```

### Parte D: la lección del umbral (45 s)

«Con ventanas de 3 días, la última solo tiene un día de datos. Ahí el modelo HGST
HUH721212ALE600 entra 3.º del top con **una sola falla** en 2.599 días-disco (14,04 %). Eso es
justo la conclusión engañosa que el umbral debe evitar: con ventanas cortas hay que subirlo
(por ejemplo, 10.000 días-disco) y mirar los intervalos de confianza.»

### Calidad del proyecto (45 s)

- **Verificaciones cruzadas en cada parte**: RDD vs. DataFrame (A), streaming vs. batch (C),
  Mongo vs. Parte A (D). Si algo no cuadra, el pipeline se detiene.
- **Pruebas automáticas** con pytest (fórmulas, validación de descarga, partes B, C y D) y
  estilo con ruff.
- **Reproducible**: una imagen, dependencias congeladas con `uv.lock`, un comando.

### Conclusiones (30 s)

«Para cerrar, tres ideas:»

1. «**El volumen no garantiza la confianza**: 2,4 millones de filas, pero solo 108 fallas. Sin
   intervalos de confianza, una sola falla puede parecer un modelo terrible.»
2. «**Cómo repartes importa tanto como qué calculas**: una mala clave de partición deja
   workers ociosos, en Spark y en Kafka.»
3. «**En streaming, la exactitud depende del tiempo**: el watermark es un compromiso entre
   aceptar datos tardíos y publicar resultados a tiempo.»

«Gracias. ¿Preguntas?»

---

## Preguntas probables y quién responde

| Pregunta | Responde | Respuesta corta |
| -------- | -------- | --------------- |
| ¿Por qué RDD y no solo DataFrames? | Juan José | El enunciado pide MapReduce explícito; el DataFrame sirve de verificación. |
| ¿Por qué 7 días y no el trimestre? | Juan José | El enunciado recomienda 7–14; se cambia con `DS_DAYS` en `.env`. |
| ¿Qué número de particiones conviene? | Jairo | El que deja tareas de tamaño parecido y cerca de los núcleos disponibles; ver la tabla de tiempos. |
| ¿Por qué la clave de Kafka es el serial? | Jairo | Reparte uniforme; con modelo, una partición recibe el 38 %. |
| ¿Qué pasa si un evento llega muy tarde? | Jairo | Puede descartarse; la ventana cerrada no se reabre (experimento de 5 días). |
| ¿Por qué Mongo y no una tabla SQL? | Antonio | Documentos por (modelo, ventana) y el pipeline de agregación con `$topN` resuelven el top N directamente. |
| ¿Por qué el umbral de 100 días-disco? | Antonio | Evita AFR de modelos casi sin exposición; con ventanas cortas conviene subirlo. |

## Checklist antes de exponer

- [ ] Ejecutar `docker compose up` una vez (o `part-b`) para tener las salidas frescas.
- [ ] Jairo: llenar la tabla de tiempos de la Parte B con `output/part_b/tiempos_resumen.csv`.
- [ ] Dejar Mongo arriba si Antonio va a hacer la demo (`docker compose up -d mongo`).
- [ ] Ensayar las transiciones entre expositores.
