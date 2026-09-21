# Requiere: ninguno (Spark local, sin servicios docker) + conexión a
# internet la primera vez (descarga el jar del conector Avro vía Maven,
# se queda cacheado en ~/.ivy2 para las siguientes ejecuciones).
#
# Avro — un formato binario con schema explícito (como Parquet), pero
# pensado sobre todo para streaming/mensajería (Kafka lo usa mucho) más
# que para almacenamiento analítico — a diferencia de Parquet, guarda el
# schema completo dentro de cada fichero de forma autocontenida, lo que
# lo hace cómodo para pasar datos entre sistemas que no comparten
# metastore. Como GraphFrames/Delta/Kafka/el driver JDBC de Postgres, el
# conector Avro es un jar aparte que hay que indicarle a Spark como
# coordenada Maven antes de crear la SparkSession — no viene incluido en
# Spark por defecto (a diferencia de CSV/JSON/Parquet).
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.13:4.2.0-preview5 pyspark-shell'

import pyspark

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo avro").getOrCreate()

# files/users.avro trae, además de columnas normales, un array anidado
# (favorite_numbers) — Avro conserva ese tipo de estructura igual que
# Parquet o JSON (ver 05_spark_sql_12_datos_anidados.py).
df = spark.read.format("avro").load("files/users.avro")
df.printSchema()
df.show()

# Escritura: igual que con cualquier otro formato (ver
# 05_spark_sql_15_escritura_formatos.py), solo cambia el valor de
# .format(). mode("overwrite") para que el ejemplo se pueda repetir sin
# fallar la segunda vez (el valor por defecto de mode es "error" si la
# ruta ya existe).
ruta_salida = "spark-warehouse/salida_avro"
df.write.format("avro").mode("overwrite").save(ruta_salida)

# Releer confirma que el schema (incluido el array anidado) se conserva
# tal cual tras el viaje de ida y vuelta.
releido = spark.read.format("avro").load(ruta_salida)
print("--- Releído tras escribir/leer en Avro: mismo contenido ---")
releido.orderBy("name").show()
