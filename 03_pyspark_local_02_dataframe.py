# Requiere: ninguno (Spark local, sin servicios docker).
#
# Tres formas equivalentes de crear un DataFrame de Spark a mano (sin leer
# de ningún fichero) — útil para prototipar o para los propios ejemplos de
# este proyecto, donde interesa más mostrar la API que depender de datos
# externos:
#   1. una lista de `Row` (cada Row es una fila con sus columnas nombradas).
#   2. una lista de tuplas + un `schema` explícito en formato DDL
#      ("a long, b double, ...").
#   3. a partir de un DataFrame de pandas ya existente.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('dataframe-local').getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df_desde_rows = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
])

df_desde_tuplas = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0)),
], schema='a long, b double, c string, d date, e timestamp')

pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)],
})
df = spark.createDataFrame(pandas_df)

df.show()
df.printSchema()
df.describe().show()

# collect() trae TODAS las filas del cluster al proceso driver, como una
# lista de Row en memoria Python — con un DataFrame grande puede agotar la
# memoria del driver (aquí es inofensivo, solo 3 filas).
print(df.collect())

# toPandas() hace lo mismo pero devolviendo un DataFrame de pandas en vez
# de una lista de Row — mismo riesgo de memoria con datasets grandes.
print(df.toPandas())
