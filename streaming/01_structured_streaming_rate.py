# Requiere: ninguno (Spark local, sin servicios docker, sin nada externo).
#
# Structured Streaming (la API moderna de streaming de Spark, sobre
# DataFrames) — a diferencia de la API antigua ("Spark Streaming"/DStream,
# `pyspark.streaming.StreamingContext`, ya en desuso desde hace años),
# Structured Streaming trata un flujo de datos como una tabla que no para
# de crecer: se escriben las mismas transformaciones (`select`, `groupBy`,
# `filter`...) que con un DataFrame normal, y Spark se encarga de ir
# aplicándolas a los datos nuevos que van llegando, en "micro-batches".
#
# Este ejemplo usa la fuente "rate" (genera filas él solo, a un ritmo
# fijo) para no depender de nada externo — es la forma más simple de
# probar Structured Streaming sin montar nada aparte. Para un ejemplo con
# una fuente de datos real (Kafka), ver 03_structured_streaming_kafka.py.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('structured-streaming-rate').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# La fuente "rate" genera automáticamente dos columnas: "timestamp"
# (cuándo se generó la fila) y "value" (un contador que crece de 1 en 1) —
# solo sirve para pruebas/demos, nunca para un caso real (rowsPerSecond
# controla la velocidad de generación).
datos = spark.readStream.format('rate').option('rowsPerSecond', 5).load()

# "memory" es un sink (destino) pensado también solo para pruebas: guarda
# los resultados en una tabla en memoria consultable con spark.sql(...) —
# en un caso real el sink sería un fichero, Kafka, una BBDD... nunca
# memoria (se perdería todo al reiniciar el proceso).
# outputMode("append") significa que cada fila nueva se añade tal cual,
# sin reescribir resultados anteriores — es el modo más simple, pero no
# vale para consultas con agregaciones que cambian con el tiempo (para
# eso hace falta "complete" o "update", ver 03_structured_streaming_kafka.py).
consulta = datos.writeStream.format('memory').queryName('datos_generados').outputMode('append').start()

# awaitTermination(timeout) espera hasta que la consulta termine por sí
# sola O hasta que pase ese tiempo (lo que ocurra antes) — no es un
# sleep(): si la consulta fallase antes de los 5 segundos, devuelve el
# control enseguida en vez de bloquear el tiempo completo.
consulta.awaitTermination(5)
consulta.stop()

print('Filas generadas en 5 segundos (a 5 filas/segundo, deberían ser ~25):')
spark.sql('SELECT count(*) AS num_filas FROM datos_generados').show()

spark.stop()
