# Requiere: ninguno (Spark local, sin servicios docker).
#
# Primer ejemplo del proyecto: crear una sesión de Spark en modo "local"
# (todo corre en un único proceso, sin cluster real) y usarla mínimamente
# — solo para comprobar que la instalación funciona antes de pasar a
# ejemplos con datos de verdad.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('First App').master('local').getOrCreate()
print(spark)
