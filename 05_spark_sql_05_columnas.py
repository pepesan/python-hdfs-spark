import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType

# Crear un SparkSession
spark = pyspark.sql.SparkSession.builder.appName("Ejemplo").getOrCreate()

# Read multiline json file
df = spark.read.option("multiline","true") \
      .json("files/example.json")
# Mostrar los datos
df.show()

df.printSchema()

# Seleccionar las columnas "nombre" y "edad"
df_filtrado = df.select("nombre", "edad")

# Mostrar los datos
df_filtrado.show()

# Filtrar los datos para que solo queden las personas mayores de 25 años
df_filtrado = df.filter(df.edad > 25)

# Mostrar los datos
df_filtrado.show()

# Filtrar los datos para que solo queden las personas mayores de 25 años
df_filtrado = df.where(df.edad > 25)

# Mostrar los datos
df_filtrado.show()

# Filtrar los datos para que solo queden las personas mayores de 25 años que viven en España
df_filtrado = df.where((df.edad > 25) & (df.pais == "España"))

# Mostrar los datos
df_filtrado.show()


# Filtrar los datos para que solo queden las personas de México o España
df_filtrado = df.where((df["pais"] == "España") | (df["pais"] == "México"))

# Mostrar los datos
df_filtrado.show()

# Ordenar los datos por edad, de menor a mayor
df_ordenado = df.sort(df.edad.asc())

# Mostrar los datos
df_ordenado.show()

# Ordenar los datos por edad, de menor a mayor
df_ordenado = df.sort(df.edad.desc())

# Mostrar los datos
df_ordenado.show()

# Agrupar los datos por país y calcular la media de la edad
df_agrupado = df.groupBy("pais").agg(F.avg("edad"))

# Mostrar los datos
df_agrupado.show()

# Crear una nueva columna basada en los valores de otra columna
df_modificada = df.withColumn("edad_masiva", df.edad * 2)

# Mostrar el DataFrame resultante
df_modificada.show()
df_modificada.printSchema()
# quito una columna
df_modificada = df_modificada.drop("edad_masiva")
# Mostrar el DataFrame resultante
df_modificada.show()
df_modificada.printSchema()







