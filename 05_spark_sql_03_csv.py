import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Crear un SparkSession
spark = pyspark.sql.SparkSession.builder.appName("Ejemplo 1").getOrCreate()

# Leer el archivo CSV
df = spark.read.csv("files/sql.csv")
# fijate en los nombres de las columnas
df.show()

# Leer el archivo CSV con cabecera
df = spark.read.csv("files/sql.csv", header=True)

# Mostrar los datos
df.show()

# Leer el archivo CSV con cabecera a infiriendo el esquema
df = spark.read.csv("files/sql.csv", header=True, inferSchema=True)

# Mostrar los datos
df.show()

# Mostrar los tipos de datos
print(df.dtypes)

# Mostrar el esquema
df.printSchema()

