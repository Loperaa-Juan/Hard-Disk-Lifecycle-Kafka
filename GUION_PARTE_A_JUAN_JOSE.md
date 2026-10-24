# Guion de Juan José: introducción y Parte A (MapReduce)

**Duración:** unos 8 minutos.
**Estilo:** la idea se cuenta primero en palabras sencillas y después se da el detalle técnico
para el profesor.

---

## 1. La pregunta (1 min)

**Para todos**
«Todos guardamos fotos, documentos y correos en *la nube*. Pero la nube no es algo etéreo: son
edificios llenos de miles de discos duros, y los discos, como cualquier aparato, se dañan.
Nuestra pregunta fue: **¿qué tan seguido se dañan los discos, y hay modelos que fallan más que
otros?**»

**El detalle técnico**
«Usamos los datos abiertos de **Backblaze Drive Stats**. Backblaze es una empresa de copias de
seguridad que publica todos los días un CSV con una fila por cada disco en operación. De las
cerca de 190 columnas usamos cuatro: `date`, `serial_number`, `model` y `failure`, que vale 1
el día en que el disco falla y 0 en los demás.»

---

## 2. La métrica: días-disco y AFR (1 min 30 s)

**Para todos**
«No basta con contar cuántos discos se dañaron. Si de una marca de bombillos se dañan 10 y de
otra 2, no sabemos cuál es peor hasta saber cuántos bombillos teníamos de cada una. Es como el
promedio de bateo: no importa cuántos *hits* tienes, sino cuántos por cada turno al bate.»

**El detalle técnico**
- «Cada fila del CSV es un disco observado durante un día: un **día-disco**. Es la unidad de
  exposición al riesgo.»
- «Con eso calculamos la tasa anualizada de fallas, la misma fórmula que usa Backblaze:»

  > **AFR (%) = fallas ÷ (días-disco ÷ 365) × 100**

- «Como las fallas son eventos raros e independientes, las modelamos como una **distribución de
  Poisson** y calculamos un **intervalo de confianza exacto del 95 %** (método de Garwood),
  implementado en Python puro en `src/drivestats/metrics.py`. El intervalo es clave: con pocas
  fallas se abre mucho y nos dice cuándo una tasa no es confiable.»
- «Para validar la fórmula usamos las cifras oficiales de Backblaze para Q2 2026: 1.498 fallas
  en 31.553.350 días-disco dan **1,73 %**, que es lo que ellos publican. Eso quedó como prueba
  automática en `tests/test_metrics.py`.»

---

## 3. La muestra y cómo la obtenemos (1 min 30 s)

**Para todos**
«Tomamos una semana, del 1 al 7 de abril de 2026: casi **347.000 discos de 78 modelos** y
alrededor de 2,4 millones de registros.»

**El detalle técnico**
- «Backblaze publica un ZIP por trimestre de **2,1 GB**. No lo bajamos entero: un ZIP guarda al
  final un índice con la posición de cada archivo, así que lo leemos con **peticiones HTTP
  Range** (pedirle al servidor solo un rango de bytes) y descargamos únicamente los días que
  necesitamos, unos 23 MB comprimidos por día.»
- «Cada archivo se verifica con su **CRC32** y se registra en un `manifest.json`. Antes de cada
  ejecución, el script `with-data.sh` revisa la muestra: si un archivo falta o está truncado,
  lo vuelve a bajar; si todo está bien, no toca la red. Revisar los 7 días tarda menos de un
  segundo.»
- «La muestra se configura en el archivo `.env` (trimestre, día de inicio y número de días),
  así que se puede ampliar a 14 días sin tocar el código.»

---

## 4. La arquitectura (1 min)

**Para todos**
«Todo el proyecto se ejecuta con **un solo comando**. Quien lo quiera repetir solo necesita
Docker: no tiene que instalar nada más.»

**El detalle técnico**
- «Usamos **Docker Compose** con un clúster de Spark: **un master y dos workers**. El programa
  principal, el *driver*, se conecta en modo cliente a `spark://spark-master:7077`.»
- «Hay **una sola imagen** para todos los servicios, con Python 3.12, Java 21 y PySpark. Así el
  driver y los executors tienen exactamente las mismas versiones; las dependencias se congelan
  con `uv.lock`.»
- «Los workers leen los CSV desde una carpeta compartida (`./data`) y el driver escribe los
  resultados en `./output`.»
- «Con `docker compose up` corren las cuatro partes en cadena: la B solo arranca si la A
  terminó bien, y así sucesivamente. Si una falla, el pipeline se detiene ahí.»

---

## 5. El MapReduce (1 min 30 s)

**Para todos**
«Eran millones de registros. Para contarlos rápido usamos **MapReduce**, que funciona como el
escrutinio de unas elecciones: cada mesa cuenta sus votos por separado y al final se suman los
resultados de todas las mesas. Aquí, cada computador cuenta su parte de los datos y luego se
juntan los totales por modelo.»

**El detalle técnico**
- **Lectura:** «Leemos cada CSV como texto con un RDD y extraemos solo las cuatro columnas que
  necesitamos. Descartamos las filas sin modelo o con un valor de `failure` que no sea 0 o 1.
  Como algunos archivos pueden traer las columnas en otro orden, los agrupamos por encabezado.»
- **Map:** por cada fila emitimos un par clave-valor:

  ```python
  pairs = rows.map(lambda r: (r[MODEL], (1, r[FAILURE])))
  ```

- **Reduce:** sumamos los valores de cada clave:

  ```python
  by_model = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
  ```

- «La primera suma da los **días-disco** de cada modelo y la segunda, las **fallas**.
  `reduceByKey` combina primero dentro de cada partición, antes del *shuffle*, así que por la
  red viajan pocos datos: solo un total parcial por modelo y partición.»

---

## 6. La verificación (45 s)

**Para todos**
«Para estar seguros, contamos **dos veces, de dos maneras distintas**, y comparamos.»

**El detalle técnico**
- «Leemos los mismos CSV con la **API de DataFrames** y hacemos
  `groupBy("model").agg(count, sum)`, aplicando los mismos filtros.»
- «El job compara modelo a modelo, y si un solo par (días-disco, fallas) no coincide, **termina
  con error** y el pipeline no sigue. En nuestra muestra coincidieron los **78 de 78 modelos**.»

---

## 7. Los resultados (1 min 30 s)

**Para todos**
«En esa semana hubo **108 discos dañados**. Eso equivale a que se dañaría cerca del **1,6 % de
los discos en un año**, unos 16 de cada mil. Backblaze publica 1,7 % para el trimestre: nuestro
resultado es muy parecido.»

**El detalle técnico**
- **2.419.966 días-disco, 108 fallas, 78 modelos y 346.921 discos.**
- **AFR global: 1,63 %**, con un IC del 95 % de **1,34 % a 1,97 %**. El 1,73 % oficial queda
  dentro del intervalo, así que la semana es representativa del trimestre.
- **Por modelo, 7 días son pocos datos:**
  - «Incluso el modelo con más uso, WDC WUH722222ALE6L4, con 874 años-disco, solo tuvo
    4 fallas: 0,46 %, con un intervalo de 0,12 % a 1,17 %.»
  - «Los modelos con 0 fallas muestran un AFR de 0 %, pero su límite superior llega al 2,7 –
    5,3 %. Que no hayan fallado no significa que sean perfectos.»
  - «57 de los 78 modelos tienen menos de 10.000 días-disco: su AFR no es interpretable.»
  - «Solo dos modelos tienen el intervalo **completo** por encima del promedio global:
    **HGST HUH721212ALN604, con 11,97 %** (IC 7,50 – 18,12 %), y **ST24000NM002H, con 5,03 %**
    (IC 2,30 – 9,55 %). Son los únicos de los que podemos decir con confianza que fallan más.»

> Frase de cierre: «Es como juzgar a un futbolista por un solo partido: con pocos datos, el
> intervalo de confianza nos protege de sacar conclusiones apresuradas.»

---

## 8. La censura: cuánto vive cada disco (1 min)

**Para todos**
«También quisimos saber cuánto vive cada disco. Pero al final de la semana el **99,97 %** de los
discos seguía funcionando: de casi todos solo sabemos que *siguen vivos*, no cuándo se van a
dañar.»

**El detalle técnico**
- «Aplicamos el mismo patrón MapReduce con **clave `serial_number`**: obtenemos los días que se
  observó cada disco, su primera y última fecha y si falló.»
- «Un disco que no falló está **censurado por la derecha**: solo sabemos que vivió *al menos*
  esos días. Es el problema clásico del análisis de supervivencia.»
- «Además hay **truncamiento por la izquierda**: los discos ya funcionaban antes del 1 de abril.
  En los que fallaron, los días observados son el día de la falla dentro de la semana, no su
  vida útil. Para conocer su edad real habría que usar `smart_9_raw`, las horas encendido.»
- **Control:** «la suma de los días observados por serial debe ser igual al total de
  días-disco, y lo es.»
- «El archivo `vida_por_serial.csv` queda listo como entrada para un estimador de
  **Kaplan-Meier**.»

---

## 9. Salidas y transición (15 s)

«Los resultados quedan en `output/part_a/`: el AFR por modelo con su intervalo, la vida por
serial, la censura por modelo y un `resumen.json` con totales, la verificación y los tiempos.

Ya sabemos contar bien. La pregunta siguiente es si el trabajo está bien repartido entre los
computadores. Eso lo explica Jairo.»

---

## Preguntas que podría hacer el profesor

**¿Por qué RDD y no solo DataFrames?**
«El enunciado pide un MapReduce explícito, y con RDD se ven claramente las fases de *map* y
*reduce*. El DataFrame lo usamos como verificación independiente: optimiza el plan de otra
manera, así que si los dos coinciden es poco probable que haya un error.»

**¿Por qué `reduceByKey` y no `groupByKey`?**
«`groupByKey` envía por la red todos los valores de cada clave y los junta en un solo nodo.
`reduceByKey` suma primero dentro de cada partición y solo envía los totales parciales: mucho
menos tráfico en el *shuffle* y menos memoria.»

**¿Por qué el intervalo de Poisson y no una aproximación normal?**
«Con pocas fallas, por ejemplo 0 o 4, la aproximación normal da intervalos simétricos que
pueden incluir valores negativos. El intervalo exacto de Poisson es asimétrico y siempre
válido, incluso con 0 fallas.»

**¿Por qué solo 7 días?**
«El enunciado recomienda de 7 a 14 días, y así el proyecto corre en pocos minutos. Se amplía
cambiando `DS_DAYS` en `.env`, y solo se descargan los días nuevos.»

**¿Qué pasa si un archivo se descarga mal?**
«El CRC32 no coincide con el del índice del ZIP, el archivo se marca como inválido y se vuelve
a descargar. Si no hay red y falta un archivo, el job no arranca: preferimos no calcular a
calcular con datos incompletos.»

**¿Cómo estimarían la vida útil real?**
«Con una ventana más larga y la columna `smart_9_raw` para conocer la edad de cada disco.
Después, con `vida_por_serial.csv`, se aplica Kaplan-Meier, que está pensado justamente para
datos censurados.»
