# Requiere: ninguno (Spark local, sin servicios docker).
#
# Introducción a Spark SQL / DataFrames: cargar datos (aquí desde pandas,
# con un schema definido a mano), y las operaciones más básicas —
# filtrar, agrupar y agregar. Ver `05_spark_sql_02.py` en adelante para
# leer directamente de CSV/JSON y operaciones más avanzadas.
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('spark-sql-intro').getOrCreate()

# Cargamos el CSV con pandas primero (en vez de spark.read.csv) solo para
# poder mostrar cómo se pasa un pandas DataFrame ya existente a Spark con
# un schema explícito, en vez de dejar que Spark lo infiera solo.
import pandas as pd
data = pd.read_csv("files/sql.csv")

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("pais", StringType(), True),
])
df = spark.createDataFrame(data, schema=schema)
print(df.head())
df.show()

# Filtrar: igual que un WHERE de SQL, se queda solo con las filas que
# cumplen la condición.
df = df.filter(df.edad >= 30)
df.show()

# Agregar: media de "edad" por cada valor distinto de "pais" — equivalente
# a un GROUP BY ... AVG(edad) de SQL.
df = df.groupBy("pais").agg(F.avg("edad"))
df.show()
