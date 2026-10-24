# Guion: «¿Cuánto viven los discos?»

**Público:** personas sin formación técnica.
**Duración:** unos 15 minutos más preguntas.

---

## 1. Juan José: la pregunta y el primer conteo (~5 min)

**Apertura**
«Buenas. Somos Juan José, Jairo y Antonio. Todos guardamos fotos, documentos y correos en *la nube*. Pero la nube no es algo etéreo: son edificios llenos de miles de discos duros, y los discos, como cualquier aparato, se dañan. Nuestra pregunta fue sencilla: **¿qué tan seguido se dañan los discos, y hay marcas o modelos que fallan más que otros?**»

**De dónde salen los datos**
«Una empresa llamada Backblaze, que guarda copias de seguridad de millones de personas, publica todos los días una lista de todos sus discos: cuál es, de qué modelo es y si se dañó ese día. Tomamos una semana de abril de 2026: casi **347.000 discos** de 78 modelos distintos.»

**Cómo medimos**
«No basta con contar cuántos discos se dañaron. Imaginen dos marcas de bombillos: de una se dañaron 10 y de otra 2. ¿Cuál es peor? Depende: si de la primera tenemos 10.000 bombillos y de la segunda solo 20, la segunda es mucho peor.
Por eso contamos **días de uso**: un disco funcionando un día es un *día-disco*. Con eso calculamos qué porcentaje de discos se dañaría en un año. Es como el promedio de bateo en el béisbol: no importa cuántos *hits* tienes, sino cuántos por cada turno al bate.»

**Cómo lo calculamos**
«Eran casi 2,5 millones de registros. Para contarlos rápido usamos una técnica llamada *MapReduce*, que funciona como un escrutinio de elecciones: cada mesa cuenta sus votos por separado y al final se suman los resultados de todas las mesas. Aquí, varios computadores cuentan cada uno una parte, y luego se juntan los totales.
Para estar seguros, lo contamos **dos veces, de dos formas diferentes**, y los resultados coincidieron en los 78 modelos.»

**Qué encontramos**
«En esa semana hubo **108 discos dañados** entre 2,4 millones de días de uso. Eso equivale a que se dañaría cerca del **1,6 % de los discos en un año**, es decir, unos 16 de cada mil. Backblaze publica un 1,7 % para el trimestre completo: nuestro resultado es muy parecido.
Encontramos un modelo que falla mucho más: casi el **12 %** al año, siete veces el promedio.
Pero hay una advertencia: una semana es poco tiempo. Para muchos modelos tenemos tan pocos discos que no podemos decir nada con seguridad. Sería como juzgar a un futbolista por un solo partido.»

**Una curiosidad**
«Al final de la semana, el 99,97 % de los discos seguía funcionando. O sea, de casi todos solo sabemos que *siguen vivos*, no cuánto van a durar. Por eso esta pregunta necesita observar mucho más tiempo.»

**Transición:** «Ya sabemos contar. Pero cuando varios computadores trabajan juntos, ¿cómo repartimos el trabajo? Eso lo explica Jairo.»

---

## 2. Jairo: repartir bien el trabajo y recibir datos en vivo (~5 min)

**Repartir el trabajo**
«Piensen en un supermercado. Si hay un solo cajero, la fila es eterna. Si abrimos 200 cajas, pero solo llegan unos pocos clientes, la mayoría de los cajeros está sin hacer nada y coordinarlos también cuesta.
Hicimos ese experimento con los computadores: dividimos el trabajo en 2, 8, 32 y 200 partes y medimos cuánto tardaba cada vez. [Completar con lo que se observó en la medición.] La lección es que **dividir más no siempre es más rápido**: hay un punto justo.»

**El problema de la fila desigual**
«Hay otro detalle. Si en el supermercado mandamos a cada cliente a la caja según su apellido, y medio mundo se apellida *García*, esa caja tendrá una fila enorme mientras las demás están vacías. Y la tienda no cierra hasta que atiendan al último García.
Eso pasa si repartimos los datos por modelo de disco: un solo modelo tiene el **13 %** de todos los registros, y el computador que le toca lo tiene que procesar todo él solo, mientras otros quedan sin trabajo. La solución es repartir de una forma más pareja, por ejemplo, por el número de serie de cada disco, que es único.»

**Datos que llegan en vivo**
«Hasta aquí teníamos todos los datos guardados de antemano. Pero en la vida real los datos llegan poco a poco, como los pedidos en una cocina de restaurante.
Simulamos eso: un programa envía los datos de un día cada 10 segundos, como si la semana pasara en poco más de un minuto. Usamos una herramienta llamada **Kafka**, que funciona como una banda transportadora: un lado pone los datos y el otro los va recogiendo a su ritmo, sin que ninguno tenga que esperar al otro. Y aplicamos la lección anterior: repartimos los datos de forma pareja.»

**Los datos que llegan tarde**
«En la vida real algunos reportes llegan tarde. Simulamos que un 2 % de los discos reportaba con un día de retraso.
Entonces, ¿cuándo damos un resultado por terminado? Es como un concurso que dice: "se reciben trabajos hasta dos días después de la fecha límite". Pasado ese plazo, el jurado publica los ganadores y ya no acepta nada más.
Así lo hicimos: esperamos dos días de margen. Los reportes con un día de retraso se contaron todos, y el resultado fue **idéntico** al cálculo con todos los datos completos.
Luego probamos con reportes mucho más atrasados, de cinco días: esos llegaron cuando *el concurso ya había cerrado* y quedaron fuera. Eso nos enseñó que hay que elegir bien el margen: si esperamos poco, perdemos datos; si esperamos mucho, los resultados tardan más en salir.»

**Transición:** «Ya tenemos los resultados. Falta guardarlos de forma ordenada para poder consultarlos. Antonio.»

---

## 3. Antonio: guardar y consultar los resultados, y conclusiones (~5 min)

**Guardar los resultados**
«Los resultados los guardamos en una base de datos llamada **MongoDB**. Piénsenla como un archivador: cada ficha tiene un modelo de disco, una semana, cuántos días de uso tuvo, cuántos se dañaron y su porcentaje de fallas.
El archivador tiene una regla: no puede haber dos fichas para el mismo modelo en la misma semana. Así evitamos contar algo dos veces. Y si un día no hay datos, el sistema se detiene en vez de inventar resultados.»

**Hacerle preguntas**
«Con el archivador listo le podemos preguntar, por ejemplo: **¿cuáles son los cinco modelos que más fallan en cada semana?**
La respuesta: el peor modelo tiene casi un 12 % de fallas al año, y los siguientes están entre el 3 % y el 5 %, todos por encima del promedio.
Para comprobar, sumamos las fichas de cada modelo y obtuvimos exactamente los mismos totales que Juan José calculó al principio.»

**Cuidado con las conclusiones apresuradas**
«Aquí aprendimos algo importante. Cuando miramos periodos muy cortos, de un solo día, apareció en el top un modelo con **una sola falla**. ¿Es un mal modelo? No necesariamente: es como juzgar un restaurante por una sola reseña.
Por eso le pusimos un filtro a la pregunta: solo entran al ranking los modelos con suficientes días de uso. Y cuando los periodos son cortos, ese mínimo debe ser más alto.»

**Cómo nos aseguramos de que todo esté bien**
«En cada paso comprobamos el resultado de dos formas distintas, y si no coinciden, el sistema se detiene. Además, cualquier persona puede repetir todo el proyecto con un solo comando y obtener los mismos resultados.»

**Conclusiones**
«Nos quedamos con tres ideas:
1. **Tener muchos datos no es lo mismo que tener certeza.** Revisamos casi 2,5 millones de registros, pero solo hubo 108 fallas. Con pocos casos, hay que ser prudentes.
2. **Repartir bien el trabajo importa tanto como hacerlo.** Una mala división deja a unos sobrecargados y a otros sin hacer nada, igual que en un equipo de personas.
3. **Con datos en vivo hay que elegir entre rapidez y exactitud.** Esperar más da resultados más completos, pero llegan más tarde.

Gracias. ¿Preguntas?»

---

## Preguntas que podrían hacer

- **¿Entonces qué disco compro?** *(Antonio)* «Con una semana de datos no podemos recomendar una marca. Algunos modelos sí se destacan como más frágiles, pero haría falta observar meses para estar seguros.»
- **¿Por qué no usaron todo el año?** *(Juan José)* «Para que el proyecto corriera en pocos minutos en un computador normal. El sistema permite usar más días cambiando una sola configuración.»
- **¿Para qué sirve esto en la vida real?** *(Jairo)* «Los centros de datos usan este tipo de análisis para saber cuántos discos de repuesto comprar y cuáles cambiar antes de que fallen, y así no perder información.»
