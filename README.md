2. Implementación en Spark: Desarrollar aplicaciones en Spark (utilizando Python) que realicen las siguientes tareas:

* Procesamiento en Batch:
    * Cargar el conjunto de datos seleccionados desde la fuente original.
    * Realizar operaciones de limpieza, transformación y análisis exploratorio de datos (EDA) utilizando RDDs o DataFrames.
    * Almacenar los resultados procesados.

Instrucciones de uso:
1. Para la implementación de dicho código se creo un archivo llamado Tarea3, por medio del comando nano Tarea3.py donde se agregó todo el código anterior.

2. Luego probamos que el correcto funcionamiento del código implementando en Spark, primero ejecutamos con usuario hadoop el spark por medio del comando, ¨pyspark¨.

3. Ejecutamos mi archivo Python por medio del comando ¨python3 Tarea3.py¨. para que podamos observar el correcto funcionamiento.

-----------------------------------------------------

* Procesamiento en tiempo real (Spark Streaming & Kafka):
    * Configurar un topic en Kafka para simular la llegada de datos en tiempo real (usar un generador de datos).
    * Implementar una aplicación Spark Streaming que consuma datos del topic de Kafka.
    *  Realizar algún tipo de procesamiento o análisis sobre los datos en tiempo real (contar eventos, calcular estadísticas, etc.).
    * Visualizar los resultados del procesamiento en tiempo real.

Instrucciones de uso:
1. Para empezar, ejecutar los servidores: ZooKeeper y Kafka.
   En caso de presentarse un error de permisos, estos se pueden ejecutar en primer plano, pero para esto, cada una de las ejecuciones
   se debe hacer en una terminal independiente a las demás, una terminal para cada servidor (total de 2).

EN OTRA TERMINAL->

2. Segundo, en una terminal independiente, se ejecuta el script Productor con: 'python3 kafka_producer.py'
   Es importante que primero se ejecute el script de productor antes que el de consumidor. 

3. Tercero, en otra terminal independiente, se ejecuta el script Consumidor con:
   'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py'
   Es importante que ambos scripts se ejecuten en una terminal diferente, pues al ser procesos en primer plano que se mantienen en
   constante ejecución, el prompt del sistema ya no aparece para ejecutar más código. 

CONSIDERACIONES->

Los procesos de Productor y Consumidor muestran sus resultados en consola. Sin embargo, se puede comprobar el resultado en la web usando
el enlace:
http://ip_propio:4040/
Donde:
ip_propio: es el número ip que te muestra la maquina virtual al iniciarla. Este cambia con cada reinicio de la maquina, así que revisalo.
