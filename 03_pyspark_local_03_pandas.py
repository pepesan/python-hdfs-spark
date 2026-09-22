# Requiere: ninguno (Spark local, sin servicios docker).
#
# "pandas API on Spark" (`pyspark.pandas`, alias habitual "ps"): la misma
# API de pandas (Series, DataFrame, groupby...) pero ejecutada de forma
# distribuida sobre Spark por debajo — para poder reutilizar código/
# conocimiento de pandas sobre datos que ya no caben en la memoria de una
# sola máquina, sin reescribir todo con la API de Spark SQL. No es un
# sustituto 1:1 (algunas operaciones de pandas no tienen sentido
# distribuidas, como el orden posicional de las filas), pero cubre la
# mayoría del día a día.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pandas-on-spark').getOrCreate()

import pandas as pd
import numpy as np
import pyspark.pandas as ps

# Una Series de pandas-on-Spark, igual que se crearía una de pandas normal
# (con un hueco == NaN, valor que representa un dato ausente):
s = ps.Series([1, 3, 5, np.nan, 6, 8])
print(s)

# Un DataFrame de pandas-on-Spark, a partir de un dict de columnas:
psdf = ps.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])
print(psdf)

# Conversión en ambas direcciones entre pandas "normal" (en memoria, un
# solo proceso) y pandas-on-Spark (distribuido):
dates = pd.date_range('20130101', periods=6)
pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
psdf = ps.from_pandas(pdf)  # pandas -> pandas-on-Spark
print(type(psdf))

# También se puede partir de un DataFrame de Spark "normal" (creado con
# spark.createDataFrame) y pasarlo a la API de pandas con .pandas_api():
sdf = spark.createDataFrame(pdf)
sdf.show()
psdf = sdf.pandas_api()
print(psdf.dtypes)
print(psdf.head())
print(psdf.index)
print(psdf.columns)
print(psdf.to_numpy())  # trae los datos al driver como array de numpy

print(psdf.describe())
print(psdf.sort_index(ascending=False))
print(psdf.sort_values(by='B'))

# Datos ausentes: igual que en pandas, se representan con np.nan y se
# excluyen por defecto de los cálculos (medias, sumas...).
pdf1 = pdf.reindex(index=dates[0:4], columns=list(pdf.columns) + ['E'])
pdf1.loc[dates[0]:dates[1], 'E'] = 1
psdf1 = ps.from_pandas(pdf1)
print(psdf1)
print(psdf1.dropna(how='any'))    # quita las filas con algún NaN
print(psdf1.fillna(value=5))      # rellena los NaN con un valor fijo

print(psdf.mean())

# Agrupaciones: misma API que pandas (groupby + agregación), ejecutada de
# forma distribuida sobre Spark por debajo.
psdf = ps.DataFrame({
    'A': ['foo', 'bar', 'foo', 'bar', 'foo', 'bar', 'foo', 'foo'],
    'B': ['one', 'one', 'two', 'three', 'two', 'two', 'one', 'three'],
    'C': np.random.randn(8),
    'D': np.random.randn(8),
})
print(psdf.groupby('A').sum())
