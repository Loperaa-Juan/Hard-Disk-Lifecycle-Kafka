# **HARD DISK LIFECYCLE WITH KAFKA**

Ejercicio guiado «¿Cuánto viven los discos?»: MapReduce, balanceo de carga, streaming con
Kafka y MongoDB sobre los datos reales de vida útil de discos duros de
[Backblaze Drive Stats](https://www.backblaze.com/drivestats).

| Parte | Tema                | Estado    |
| ----- | ------------------- | --------- |
| A     | MapReduce           | ✅ Hecha  |
| B     | Balanceo de carga   | ✅ Hecha  |
| C     | Streaming con Kafka | ✅ Hecha  |
| D     | MongoDB (opcional)  | ✅ Hecha  |

## Requisitos

Solo **Docker con Compose v2**. En Windows: Docker Desktop con la integración WSL activada
(*Settings → Resources → WSL integration*). Python, Java 21 y Spark 4.2 van dentro de la imagen.

## Pipeline completo: `docker compose up`

```bash
docker compose up     # A → B → C → D, cada etapa solo si la anterior terminó bien
docker compose down   # apaga Spark, Kafka y MongoDB
```

`docker compose up` levanta la infraestructura (Spark, Kafka y MongoDB) y ejecuta las cuatro
partes como un pipeline:

```text
pipeline-a ──(código 0)──► pipeline-b ──(código 0)──► pipeline-c ──(código 0)──► pipeline-d ──► fin
 MapReduce                  balanceo                   streaming                  MongoDB
```

- **Encadenadas con `depends_on`.** Cada etapa depende de la anterior con
  `condition: service_completed_successfully`, así que Compose no la arranca hasta que la
  anterior haya terminado con éxito.
- **Si una etapa falla, el pipeline se corta ahí.** Por ejemplo, si la verificación de la
  Parte A no cuadra, B, C y D no se ejecutan, y `docker compose up` termina con error:
  `service "pipeline-a" didn't complete successfully: exit 1`.
- **Solo verás la salida de las etapas**, con su prefijo (`pipeline-a-1 | …`). Spark, Kafka y
  MongoDB llevan `attach: false`: no mezclan sus logs y siguen arriba al terminar, por si
  quieres mirar la UI de Spark en <http://localhost:8080> o consultar Mongo.
  `docker compose down` los apaga.
- **Cada etapa valida los datos antes de empezar.** La primera descarga lo que falte; las
  demás solo verifican, en menos de un segundo.
- **Tarda unos 22 minutos** con la muestra por defecto: A ~2 min, B ~14 min (repite 5 veces
  cada número de particiones, y con 200 cada repetición tarda ~1 min), C ~3 min y D ~1,5 min. La
  primera vez, además, construye la imagen, descarga los datos y la imagen de MongoDB.
- **Tras cambiar código**, usa `docker compose up --build` para reconstruir la imagen.

Cada parte se sigue pudiendo ejecutar sola, con argumentos propios:
`docker compose run --rm part-a` (o `part-b`, `part-c`, `part-d`), como se explica abajo.

## Parte A: MapReduce

```bash
docker compose run --rm part-a   # construye la imagen, levanta Spark (1 master + 2 workers),
                                 # valida los datos (y descarga lo que falte) y ejecuta la Parte A
docker compose down              # apaga el clúster
```

La primera vez tarda unos minutos porque construye la imagen y descarga unos 160 MB de datos.
Después, cada ejecución tarda alrededor de un minuto.

#### Validación de los datos antes de cada ejecución

Antes de `spark-submit`, el contenedor `part-a` ejecuta `with-data.sh`, que comprueba la
muestra en `data/raw/`:

```text
Validando la muestra Q2_2026 en data/raw: 2026-04-01.csv … 2026-04-07.csv (7 días)
  ✓ 2026-04-01.csv: válido (tamaño y CRC32 coinciden con manifest.json)
  ✗ 2026-04-06.csv: tamaño o CRC32 no coinciden con manifest.json, se descargará de nuevo
  ↓ 2026-04-07.csv: no está, se descargará
```

| Estado del archivo | Qué hace |
| ------------------ | -------- |
| ✓ Está y su tamaño y CRC32 coinciden con `data/raw/manifest.json` | Nada. Si todos están así, no toca la red |
| ↓ No está | Lo descarga |
| ✗ Está, pero truncado o modificado | Lo descarga de nuevo |
| ? Está, pero no figura en el manifiesto (copiado a mano o bajado por una versión anterior) | Lo coteja con el índice del ZIP: si coincide lo registra, si no lo descarga |

Si la validación falla (por ejemplo, falta un archivo y no hay red), el job no arranca. Sin red,
los archivos ya válidos se usan igual. Verificar el CRC32 de los 7 días tarda menos de un
segundo.

#### La muestra se define en `.env`

```ini
# Trimestre del ZIP de Backblaze (Q1_2026, Q2_2026, …)
DS_QUARTER=Q2_2026
# Primer día (AAAA-MM-DD); vacío = primer día del trimestre
DS_START=
# Número de días consecutivos (el enunciado recomienda de 7 a 14)
DS_DAYS=7
```

Cambia los valores en `.env` (o pásalos en la línea de comandos, como
`DS_DAYS=14 docker compose run --rm part-a`). En la siguiente ejecución se descargan solo los
días nuevos. El job lee **todos** los CSV de `data/raw/`: si reduces la muestra, la validación
avisa de los que sobran, y conviene borrarlos.

| Para…                                    | Comando                                                         |
| ---------------------------------------- | --------------------------------------------------------------- |
| Solo validar y descargar, sin Spark      | `docker compose run --rm download`                              |
| Usar 14 días en vez de 7                 | `DS_DAYS=14 docker compose run --rm part-a` (o cambia `.env`)   |
| Otro trimestre o día de inicio           | `DS_QUARTER=Q1_2026 DS_START=2026-02-01 docker compose run --rm part-a` |
| Pasar opciones al job                    | `docker compose run --rm part-a --min-drive-days 50000 --top 10` |
| Fijar las particiones de los reduce      | `docker compose run --rm part-a --partitions 8`                 |
| Más workers                              | `SPARK_WORKERS=3 docker compose run --rm part-a`                |
| Ver la UI del job (puerto 4040)          | `docker compose run --rm --service-ports part-a`                |
| Reconstruir tras cambiar el código       | `docker compose build`                                          |

La UI del master está en <http://localhost:8080> mientras el clúster esté arriba.

### Arquitectura

```text
Backblaze B2 ──HTTP Range──► with-data.sh ──► ./data/raw/*.csv ◄── leen ───┐
(solo los días que faltan)    (en part-a)      + manifest.json             │
part-a (driver) ──► spark-master :7077 ──► spark-worker ×N (executors) ────┘
   │                     (UI :8080)
   └──► ./output/part_a/   (UI del job :4040)
```

- **Una sola imagen** (`Dockerfile`) para todos los servicios: así el driver y los executors
  tienen la misma versión de Python y PySpark. Las dependencias se instalan con `uv sync --frozen`
  desde `uv.lock`.
- **La descarga** (`drivestats.download`, que usan `with-data.sh` y el servicio `download`) no
  baja el ZIP de 2,1 GB: lee su índice con peticiones HTTP Range y descarga solo las entradas
  de los días que faltan (unos 23 MB comprimidos por día). Comprueba el CRC32 de cada archivo y
  lo registra en `data/raw/manifest.json`.
- **`part-a`** valida la muestra y luego se ejecuta en modo cliente contra
  `spark://spark-master:7077`. Los workers leen los CSV desde el mismo `./data` montado; el
  driver escribe los resultados en `./output`.
- **`download`** está en el perfil `tools`: `docker compose up` no lo arranca, para que no
  descargue a la vez que las etapas del pipeline. `docker compose run --rm download` lo activa
  solo.

### Qué calcula

1. **MapReduce con RDD.** Por cada fila se emite `(modelo, (1, falla))` y se suma con
   `reduceByKey`. Cada fila es un disco en un día, así que la primera suma da los
   **días-disco** y la segunda las **fallas** de cada modelo:

   ```python
   pairs = rows.map(lambda r: (r[MODEL], (1, r[FAILURE])))
   by_model = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
   ```

   Con eso, `AFR (%) = fallas ÷ (días-disco ÷ 365) × 100`, con un intervalo de confianza
   exacto del 95 % (Poisson) para ver cuánto se puede confiar en cada tasa. Con las cifras de
   Backblaze para Q2 2026 la fórmula da 1,73 % (está en `tests/test_metrics.py`).
2. **Verificación.** Los mismos CSV se leen con DataFrames y se agrupan con
   `groupBy("model").agg(count, sum)`. El job compara modelo a modelo y termina con error si
   algún par (días-disco, fallas) no coincide.
3. **Días observados por serial (censura).** Se aplica el mismo patrón con clave
   `serial_number`: se obtienen los días que se vio cada disco en la ventana y si falló. Un
   disco que no falló está **censurado por la derecha**: solo se sabe que vivió *al menos*
   esos días. Como control, la suma de los días observados debe ser igual al total de
   días-disco.

### Salidas (`output/part_a/`)

| Archivo                  | Contenido                                                                        |
| ------------------------ | -------------------------------------------------------------------------------- |
| `afr_por_modelo.csv`     | `model, drive_days, drive_years, failures, afr_pct, afr_ci95_low, afr_ci95_high` |
| `vida_por_serial.csv`    | `serial_number, model, first_seen, last_seen, days_observed, failed, censored`   |
| `censura_por_modelo.csv` | `model, drives, failed_drives, censored_drives, pct_censored, mean_days_observed` |
| `resumen.json`           | Ventana, totales, AFR global, resultado de la verificación, censura y tiempos    |

`vida_por_serial.csv` es la entrada que necesita el reto de Kaplan-Meier.

### Resultados con la muestra por defecto (1 al 7 de abril de 2026)

- **2.419.966 días-disco, 108 fallas, 78 modelos y 346.921 discos.** RDD y DataFrame coinciden
  en los 78 modelos.
- **AFR global de la muestra: 1,63 %** (IC 95 %: 1,34 – 1,97 %). Es compatible con el 1,73 %
  que Backblaze publica para el trimestre completo, que queda dentro del intervalo.
- **Por modelo, 7 días son pocos datos.** Incluso el modelo con más días-disco
  (WDC WUH722222ALE6L4, 874 años-disco) tiene solo 4 fallas: 0,46 %, con un IC de 0,12 a
  1,17 %. Los modelos con 0 fallas muestran un AFR de 0 %, pero su límite superior llega al
  2,7 – 5,3 %. Solo HGST HUH721212ALN604 (11,97 %; IC 7,50 – 18,12 %) y ST24000NM002H
  (5,03 %; IC 2,30 – 9,55 %) tienen el intervalo entero por encima del 1,63 % global. 57 de los 78 modelos
  tienen menos de 10.000 días-disco y su AFR no es interpretable.
- **Censura: el 99,97 % de los discos sigue vivo al final de la ventana.** Además, en los
  discos que fallaron, `days_observed` es el día de la falla dentro de la ventana y no su
  vida útil: los discos ya funcionaban antes del 1 de abril (truncamiento por la izquierda).
  Para estimar su edad real haría falta `smart_9_raw` (horas encendido).

## Parte B: Balanceo de carga

```bash
docker compose run --rm part-b   # tiempos con 2, 8, 32 y 200 particiones + sesgo por modelo
```

Toda la lógica, la tabla de tiempos y la explicación del sesgo están en
[`part_b/README.md`](part_b/README.md). Las salidas quedan en `output/part_b/`.

## Parte C: Streaming con Kafka

```bash
docker compose run --rm part-c   # levanta Kafka y Spark, publica un día cada 10 s y agrega
                                 # por modelo en ventanas de 7 días con watermark de 2 días
docker compose down              # apaga Spark y Kafka
```

Un producer en Python publica en Kafka los registros día por día, con reloj acelerado (un día
cada 10 s) y filtrando 5 modelos (unos 120.000 eventos por día). Structured Streaming los
agrupa por modelo en ventanas de 7 días de tiempo de evento, con watermark de 2 días, y
calcula días-disco, fallas y AFR por ventana. Un 2 % de los discos llega con un día de
retraso, para ver cómo el watermark absorbe los datos desordenados. Al cerrarse todas las
ventanas, el job compara el resultado con el mismo cálculo en batch sobre los CSV y termina
solo. Tarda unos 3 minutos.

La lógica, las opciones (`STREAM_*` en `.env`), los resultados y el experimento con eventos
que llegan después de cerrarse su ventana están en [`part_c/README.md`](part_c/README.md). Las
salidas quedan en `output/part_c/`.

## Parte D: MongoDB

```bash
docker compose run --rm part-d   # levanta Spark y MongoDB, guarda los agregados por modelo y
                                 # ventana y consulta el top 5 de AFR de cada ventana
docker compose down              # apaga Spark y MongoDB (los datos de Mongo se conservan)
```

`part_d/mongo_pipeline.py` hace tres pasos y tarda alrededor de un minuto y medio:

1. **Spark agrega por modelo y ventana.** Usa las mismas ventanas que la Parte C (7 días,
   alineadas al primer día de la muestra) y su mismo código (`window_column`,
   `windowed_counts`), y calcula días-disco, fallas y AFR de cada par (modelo, ventana).
2. **Guarda un documento por par** en `drivestats_db.model_aggregates` y reemplaza los de la
   ejecución anterior. Un índice único sobre `(window_start, model)` impide repetir un par.
3. **Consulta en MongoDB**, con un pipeline de agregación, los 5 modelos con mayor AFR de cada
   ventana que tengan al menos 100 días-disco en ella: `$match` (umbral) → `$group` por
   ventana con `$topN` (los 5 de mayor AFR) → `$sort` por fecha.

```js
{
  model: 'HGST HUH721212ALN604',
  window_start: ISODate('2026-04-01T00:00:00.000Z'),
  window_end: ISODate('2026-04-08T00:00:00.000Z'),   // exclusivo: la ventana es del 1 al 7
  total_days: 67082,
  total_failures: 22,
  afr: 11.97042425687964
}
```

| Para…                         | Comando                                                  |
| ----------------------------- | -------------------------------------------------------- |
| Ventanas de otro tamaño       | `docker compose run --rm part-d --window-days 3`         |
| Otro mínimo de días-disco     | `docker compose run --rm part-d --min-drive-days 10000`  |

- **Por qué en batch y no desde el streaming.** El streaming de la Parte C publica solo 5
  modelos (el enunciado pide filtrar de 3 a 5), y un top 5 entre 5 modelos no dice nada. La
  Parte D calcula las mismas ventanas para los 78 modelos sobre los CSV. La verificación de la
  Parte C ya comprueba que sus ventanas coinciden con ese cálculo en batch, y en sus 5 modelos
  los documentos de Mongo son iguales a `output/part_c/ventanas.csv`.
- **Cuadra con la Parte A.** La lectura es la misma (`load_dataframe`, con sus filtros) y las
  ventanas no se solapan, así que al sumar las ventanas de cada modelo salen exactamente los
  días-disco y las fallas de `output/part_a/afr_por_modelo.csv`, tanto con ventanas de 7 días
  como de 3 (78 de 78 modelos).
- **Sin datos no inventa resultados.** Si no hay CSV, termina con error (`No hay CSV en data`)
  sin tocar Mongo, y el pipeline se detiene. En Compose no llega a pasar, porque `with-data.sh`
  descarga antes lo que falte.
- **El contenedor `mongo`** (`mongo:8.0.34`) se levanta con `docker compose up` y con
  `part-d`, y su healthcheck (`mongosh … ping`) hace que el job no arranque hasta que acepte
  conexiones. Los jobs le llegan por `mongodb://mongo:27017/` (variable `MONGO_URI`).
- **Los datos se guardan en el volumen `mongo-data`**: sobreviven a `docker compose down`,
  pero no a `docker compose down -v`.

### Resultados con la muestra por defecto (1 al 7 de abril de 2026)

- **Con ventanas de 7 días hay una sola ventana** (78 documentos), así que el top 5 es el de
  toda la muestra: HGST HUH721212ALN604 (11,97 %), WDC WUH721816ALE6L0 (5,27 %),
  ST24000NM002H (5,03 %), ST8000DM002 (3,05 %) y ST14000NM001G (3,00 %). Con `DS_DAYS=14`
  saldrían dos ventanas.
- **Con `--window-days 3` hay tres ventanas** (234 documentos), y la última solo tiene un día
  de datos (7 de abril; 346.375 días-disco frente a ~1.036.000 de las otras dos). El top 5
  cambia de una ventana a otra: HGST HUH721212ALN604 tiene 3, 12 y 7 fallas, y es 4.º, 1.º
  y 1.º.
- **El umbral importa cuando la ventana es corta.** Con 7 días, todos los modelos con alguna
  falla superan los 18.000 días-disco, y con un umbral de 10.000 sale el mismo top 5. En la
  ventana de un día, en cambio, HGST HUH721212ALE600 entra 3.º con **una sola falla** en 2.599
  días-disco (14,04 %). Es justo la conclusión con muestra pequeña que el umbral debe evitar:
  ahí conviene subirlo (`--min-drive-days 10000`) y mirar los intervalos de confianza de la
  Parte A.

Para consultar Mongo mientras está arriba (`docker compose up -d mongo` si lo apagaste):

```bash
docker compose exec mongo mongosh drivestats_db   # consola interactiva
docker compose exec mongo mongosh drivestats_db --quiet --eval \
  'db.model_aggregates.find({}, {_id: 0}).sort({window_start: 1, afr: -1}).limit(5)'
```

Desde el anfitrión también se llega por `mongodb://localhost:27017` (por ejemplo, con MongoDB
Compass). Si ese puerto ya está ocupado, cámbialo con `MONGO_PORT=27018 docker compose up`.

## Desarrollo local (opcional)

```bash
uv sync                      # entorno con PySpark, ruff y pytest
uv run pytest                # pruebas de las fórmulas, la validación de datos y las partes B, C y D
uv run ruff check && uv run ruff format
uv run python -m drivestats.download         # valida/descarga data/raw (DS_* o por defecto)
# necesitan Java 17+ en el PATH; PYTHONPATH=. para que se importen los paquetes part_*
PYTHONPATH=. uv run spark-submit --master "local[*]" part_a/job.py
PYTHONPATH=. uv run spark-submit --master "local[*]" part_b/job.py
# la Parte D usa el Mongo del Compose, publicado en localhost:27017
docker compose up -d mongo
PYTHONPATH=. uv run spark-submit --master "local[*]" part_d/mongo_pipeline.py
```

## Estructura

```text
├── Dockerfile               # imagen única: Python 3.12 + Java 21 + PySpark (vía uv)
├── .env                     # muestra de datos (DS_*) y parámetros del streaming (STREAM_*)
├── docker-compose.yml       # infraestructura (image, spark-master, spark-worker ×N, kafka,
│                            # mongo), pipeline (pipeline-a → b → c → d) y partes sueltas
│                            # (part-a, part-b, part-c, part-c-producer, part-d, download)
├── conf/log4j2.properties   # logs de Spark en WARN
├── docker/entrypoint.sh     # anuncia la IP del contenedor a los demás nodos de Spark
├── docker/with-data.sh      # valida/descarga la muestra y después lanza el job
├── src/drivestats/          # código común a todas las partes
│   ├── download.py          # validación con manifest.json + descarga parcial con HTTP Range
│   └── metrics.py           # AFR e intervalo de confianza de Poisson
├── part_a/
│   ├── job.py               # punto de entrada de spark-submit
│   └── mapreduce.py         # MapReduce, verificación y censura
├── part_b/
│   ├── job.py               # punto de entrada de spark-submit
│   ├── balancing.py         # tiempos por particiones y sesgo del reparto por modelo
│   └── README.md            # lógica, resultados y explicación del sesgo
├── part_c/
│   ├── job.py               # punto de entrada de spark-submit
│   ├── events.py            # formato de los eventos, muestreo y aritmética de ventanas
│   ├── producer.py          # producer de Kafka con reloj acelerado y eventos tardíos
│   ├── streaming.py         # Structured Streaming: ventanas, watermark y verificación
│   └── README.md            # lógica, resultados y experimentos
├── part_d/
│   └── mongo_pipeline.py    # agregados por modelo y ventana en MongoDB y top 5 por ventana
└── tests/                   # test_metrics.py, test_download.py, test_part_b.py, test_part_c.py,
                             # test_part_d.py
```

## Datos

Datos de [Backblaze Drive Stats](https://www.backblaze.com/drivestats), © Backblaze. Son de
uso gratuito si se cita a Backblaze como fuente y no se venden los datos.
