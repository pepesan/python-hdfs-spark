# Requiere: ninguno (Spark local, sin servicios docker).
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
df_filtrado = df.filter(df["edad"] > 25)
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

# Renombrar una columna
df_renombrada = df.withColumnRenamed("nombre", "nombre_completo")
df_renombrada.show()

# Cambiar el tipo de una columna (cast)
# aquí no hace falta (edad ya es numérico en el JSON), pero es habitual
# cuando el dato viene como texto — ver 05_spark_sql_07_limpieza_datos.py
df_cast = df.withColumn("edad", df.edad.cast(DoubleType()))
df_cast.printSchema()

# Columna calculada con condición (equivalente a un CASE WHEN de SQL)
df_categorias = df.withColumn(
    "categoria_edad",
    F.when(df.edad < 30, "joven")
     .when(df.edad < 40, "adulto")
     .otherwise("mayor")
)
df_categorias.show()

# Quitar filas duplicadas (por todas las columnas, o solo por algunas)
df_sin_duplicados = df.dropDuplicates()
df_sin_duplicados_pais = df.dropDuplicates(["pais"])
df_sin_duplicados_pais.show()

# Varios agregados a la vez, con alias para cada columna resultado
df_resumen = df.groupBy("pais").agg(
    F.count("*").alias("num_personas"),
    F.avg("edad").alias("edad_media"),
    F.min("edad").alias("edad_min"),
    F.max("edad").alias("edad_max"),
)
df_resumen.show()

# selectExpr: seleccionar/transformar columnas con expresiones SQL en texto,
# alternativa a encadenar select()/withColumn() con la API de columnas
df_expr = df.selectExpr("nombre", "edad", "edad * 2 AS edad_doble", "upper(pais) AS pais_mayusculas")
df_expr.show()









