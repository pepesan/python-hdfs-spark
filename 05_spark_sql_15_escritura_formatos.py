# Requiere: ninguno (Spark local, sin servicios docker).
#
# Escritura real a disco y conversión entre formatos — hasta ahora todos
# los ejemplos del proyecto solo LEEN datos; aquí se escriben de verdad,
# en varios formatos, para ver las diferencias entre ellos.
import os
import shutil

import pyspark

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo escritura de formatos").getOrCreate()

clientes = spark.read.option("header", "true").option("inferSchema", "true").csv("files/sql.csv")
clientes.printSchema()

# Todo lo que se escribe en este ejemplo va a spark-warehouse/ (ya está en
# .gitignore, igual que se hizo para la tabla Delta de
# 08_spark_final_practice.py) — nunca a files/, que es solo para los
# datasets de ENTRADA versionados del proyecto.
ruta_base = "spark-warehouse/escritura_formatos"
shutil.rmtree(ruta_base, ignore_errors=True)  # limpio antes de empezar, para que el ejemplo sea repetible

# partitionBy("pais"): en vez de un único fichero, Spark crea una carpeta
# por cada valor distinto de "pais" (pais=España/, pais=México/...), y
# dentro de cada una solo las filas de ese país — muy habitual para datos
# que casi siempre se van a consultar filtrando por esa columna (así
# Spark puede saltarse carpetas enteras sin ni siquiera abrirlas, en vez
# de leer todo el fichero y filtrar después).
ruta_parquet = f"{ruta_base}/clientes_parquet"
clientes.write.partitionBy("pais").mode("overwrite").parquet(ruta_parquet)

print(f"--- Carpetas creadas dentro de {ruta_parquet} (una por país) ---")
# Se filtran los ficheros de control que Spark añade además de las
# carpetas de datos (_SUCCESS marca que la escritura terminó bien; los
# .crc son checksums) — no son parte del contenido, solo metadata interna
# del propio proceso de escritura.
for nombre in sorted(os.listdir(ruta_parquet)):
    if not nombre.startswith((".", "_")):
        print(nombre)

# Al releer un parquet particionado, Spark reconstruye la columna "pais"
# a partir del NOMBRE de las carpetas (pais=España -> pais="España") —
# no hace falta que el valor esté también dentro del fichero.
releido_parquet = spark.read.parquet(ruta_parquet)
print("--- Releído desde parquet: mismo contenido, con \"pais\" reconstruido del nombre de carpeta ---")
releido_parquet.printSchema()
releido_parquet.orderBy("id").show()

# mode("overwrite"): si la ruta de destino ya existe, la borra y la
# vuelve a escribir entera — SIN esto (el valor por defecto es "error"),
# escribir dos veces en la misma ruta lanza una excepción
# (AnalysisException: path already exists) en vez de sobreescribir. Otros
# valores: "append" (añade sin borrar lo que había) y "ignore" (si ya
# existe, no hace nada y no falla).
ruta_json = f"{ruta_base}/clientes_json"
clientes.write.mode("overwrite").json(ruta_json)

# Diferencia clave entre formatos al releer: un CSV (como el de entrada)
# no guarda el tipo de cada columna — hace falta inferSchema/option para
# recuperarlo, y aun así puede adivinar mal. Parquet y JSON, en cambio, SÍ
# guardan el tipo de cada columna dentro del propio fichero (parquet de
# forma binaria y estricta; JSON infiriéndolo de los valores al leer,
# sin necesidad de "inferSchema" porque no es ambiguo como en un CSV).
releido_json = spark.read.json(ruta_json)
print("--- Releído desde JSON: el schema se recupera solo, sin inferSchema ---")
releido_json.printSchema()
releido_json.orderBy("id").show()
