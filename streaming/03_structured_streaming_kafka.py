# Requiere: docker/01_launch.sh (servicio kafka) y, antes o en paralelo,
# streaming/02_kafka_productor.py enviando mensajes al topic "frases" (si
# no hay nada en el topic, este script se queda esperando indefinidamente,
# es streaming de verdad — no un batch que termina solo).
#
# Kafka como fuente de Structured Streaming — la fuente de datos "real"
# más habitual en streaming (a diferencia de "rate", que se la inventa
# Spark solo, ver 01_structured_streaming_rate.py): un sistema de
# mensajería en el que unos procesos ("productores") escriben mensajes en
# un topic, y otros ("consumidores", aquí Spark) los leen a medida que
# llegan. Varios consumidores pueden leer el mismo topic de forma
# independiente, y los mensajes no se borran al leerlos (a diferencia de
# una cola clásica).
#
# El conector de Kafka para Spark es un jar aparte (igual que con
# GraphFrames/Delta/el driver JDBC de Postgres): hay que indicárselo a
# Spark como coordenada Maven antes de crear la SparkSession.
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0-preview5 pyspark-shell'

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('structured-streaming-kafka').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Cada mensaje de Kafka llega con varias columnas (key, value, topic,
# partition, offset, timestamp...) — "value" es el contenido del mensaje
# en bruto, como bytes ("binary" en Spark), no como texto: hay que
# convertirlo explícitamente con .cast("string").
mensajes = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'frases') \
    .option('startingOffsets', 'earliest') \
    .load()

texto = mensajes.select(mensajes.value.cast('string').alias('frase'))

# Mismo conteo de palabras que en el ejemplo de fuente "rate", pero ahora
# sobre datos que llegan de verdad desde otro proceso (kafka_productor.py)
palabras = texto.select(F.explode(F.split(texto.frase, ' ')).alias('palabra'))
conteo = palabras.groupBy('palabra').count()

# outputMode("complete"): el contador de una palabra ya vista puede
# cambiar con cada mensaje nuevo, así que hay que reescribir la tabla de
# resultados entera en cada micro-batch (ver el mismo comentario, más
# detallado, en el ejemplo de la fuente "rate").
consulta = conteo.writeStream.format('console').outputMode('complete').start()

consulta.awaitTermination()
