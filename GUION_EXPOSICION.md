# Guion: «¿Cuánto viven los discos?»

**Duración:** unos 17 minutos más preguntas.

---

## 1. Juan José: introducción y Parte A (~6 min)

**Apertura**
«Buenas. Somos Juan José, Jairo y Antonio. Nuestro proyecto responde una pregunta: **¿cuánto viven los discos duros y qué modelos fallan más?** Usamos datos reales de Backblaze, que publica cada día el estado de todos sus discos. Lo resolvimos en cuatro partes: MapReduce, balanceo de carga, streaming con Kafka y MongoDB.»

**Los datos y la métrica**
«Cada CSV diario tiene una fila por disco: fecha, serial, modelo y si falló ese día. Cada fila es un **día-disco**: contar filas nos da la exposición al riesgo y sumar la columna `failure` nos da las fallas. Usamos una semana, del 1 al 7 de abril de 2026: 346.921 discos de 78 modelos.
La métrica es la tasa anualizada de fallas: **AFR = fallas ÷ (días-disco ÷ 365) × 100**, con un intervalo de confianza del 95 %. Para comprobar la fórmula usamos las cifras oficiales de Backblaze: da 1,73 %, igual que lo que ellos publican.»

**Arquitectura**
«Todo corre con **un solo comando, `docker compose up`**. Levanta Spark con un master y dos workers, Kafka y MongoDB, y ejecuta las cuatro partes en cadena. Si una etapa falla, el pipeline se detiene ahí.
Para los datos, no bajamos el archivo completo del trimestre, que pesa 2,1 GB: leemos su índice y descargamos solo los días que faltan, y verificamos cada archivo con un checksum.»

**Parte A: MapReduce**
«Por cada fila emitimos el par *(modelo, (1, falla))* y lo sumamos con `reduceByKey`. La primera suma son los días-disco y la segunda las fallas. Para verificarlo, calculamos lo mismo con DataFrames: coincidieron los 78 modelos.
Resultado: **2.419.966 días-disco y 108 fallas, una AFR global de 1,63 %**, con un intervalo que incluye el 1,73 % oficial. Pero una semana son pocos datos por modelo: 57 de los 78 modelos no tienen suficiente exposición para interpretar su tasa. Solo dos están claramente por encima del promedio: HGST HUH721212ALN604, con 11,97 %, y ST24000NM002H, con 5,03 %.»

**Censura**
«El 99,97 % de los discos sigue vivo al final de la semana. Esto se llama **censura**: solo sabemos que vivieron *al menos* esos días.»

**Transición:** «El cálculo es correcto; ahora, ¿está bien repartido entre los workers? Jairo.»

---

## 2. Jairo: Parte B y Parte C (~6 min)

**Parte B: el experimento**
«Repetimos el MapReduce con **2, 8, 32 y 200 particiones**, cinco veces cada una. Antes de cronometrar dejamos los datos en caché para medir solo el cálculo. También comparamos dos formas de repartir: por modelo y sin clave.»

**Tiempos**
«Con 2 particiones tardó [__] s, con 8 [__] s, con 32 [__] s y con 200 [__] s. Más particiones no siempre es más rápido: con pocas no se aprovechan los núcleos, y con demasiadas pesa el costo de coordinar muchas tareas pequeñas.»
*(Estos tiempos no están en el repositorio: ejecuta `docker compose run --rm part-b` y llena los huecos con los resultados.)*

**El sesgo por clave**
«Si repartimos por modelo, todas las filas de un modelo caen en la misma partición. El modelo más grande tiene el **13,2 %** de las filas, así que con 32 particiones la más cargada tiene al menos 4 veces lo que le tocaría, y con 200, más de 26 veces. Además, con 200 particiones y 78 modelos, al menos 122 quedan vacías. La tarea más lenta marca el ritmo de todo el trabajo.»

**Transición:** «Esta lección la aplicamos en Kafka.»

**Parte C: el flujo**
«Un producer publica en Kafka los registros de **un día cada 10 segundos**, de 5 modelos, unos 120.000 eventos por día. La clave del mensaje es el **serial**, no el modelo, así que las 4 particiones quedaron casi iguales. Con el modelo como clave, una sola partición habría recibido el 38 % de los eventos. Además, un 2 % de los discos llega con un día de retraso, para simular datos desordenados.»

**Ventanas y watermark**
«Spark agrupa por **fecha del evento**, no por la hora en que llega, en ventanas de 7 días. El **watermark** es la fecha más reciente vista menos 2 días: significa "ya no espero eventos anteriores a esto". Cuando el watermark pasa el final de una ventana, Spark la cierra y la publica como definitiva.
La AFR en vivo cambia mucho al principio: la del modelo HGST pasó por 15,98 % y 3,84 % antes de quedar en 11,97 %. Al final comparamos el streaming con el cálculo sobre los CSV: **coinciden los 5 modelos**, incluidos los 16.610 eventos que llegaron tarde.»

**Eventos tardíos**
«Probamos con ventanas de 3 días. Con 4 días de retraso, los eventos llegaron en el mismo lote que cerró su ventana y se contaron. Con 5 días llegaron un lote después y se descartaron **2.373 eventos**. Lo que llega dentro del umbral siempre se cuenta; lo que llega después puede perderse.»

**Transición:** «Ya tenemos resultados por ventana; falta guardarlos y consultarlos. Antonio.»

---

## 3. Antonio: Parte D y cierre (~5 min)

**Parte D: qué guardamos**
«Spark calcula los días-disco, las fallas y la AFR de cada modelo en cada ventana de 7 días, y guarda **un documento por modelo y ventana** en MongoDB. Un índice único impide duplicados.
No lo hacemos desde el streaming porque ahí solo hay 5 modelos, y un top 5 entre 5 no dice nada; aquí entran los 78. Y si no hay datos, el proceso termina con error y no escribe nada en la base.»

**La consulta**
«Con el pipeline de agregación de MongoDB: primero filtramos los modelos con al menos 100 días-disco, después agrupamos por ventana y nos quedamos con los 5 de mayor AFR, y al final ordenamos por fecha.
El resultado: HGST HUH721212ALN604 (11,97 %), WDC WUH721816ALE6L0 (5,27 %), ST24000NM002H (5,03 %), ST8000DM002 (3,05 %) y ST14000NM001G (3,00 %). Si sumamos las ventanas de cada modelo, obtenemos exactamente las cifras de la Parte A.»

**La lección del umbral**
«Con ventanas de 3 días, la última tiene un solo día de datos, y un modelo entra tercero al top con **una sola falla**. Esa es justo la conclusión engañosa que el umbral debe evitar: con ventanas cortas hay que subirlo y mirar los intervalos de confianza.»

**Calidad del proyecto**
«Cada parte se verifica contra otra: RDD contra DataFrame, streaming contra el cálculo sobre los CSV, y Mongo contra la Parte A. Si algo no cuadra, el pipeline se detiene. Además tenemos pruebas automáticas, y todo se reproduce con un solo comando.»

**Conclusiones**
«Tres ideas para cerrar:
1. **Muchos datos no garantizan confianza**: 2,4 millones de filas, pero solo 108 fallas.
2. **Cómo se reparten los datos importa tanto como el cálculo**, en Spark y en Kafka.
3. **En streaming, el watermark es un compromiso** entre aceptar datos tardíos y publicar a tiempo.

Gracias. ¿Preguntas?»
