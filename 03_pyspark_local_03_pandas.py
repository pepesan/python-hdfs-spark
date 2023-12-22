
import pyspark
# conexión a spark "local"
# sólo se usa la biblioteca para acceder a las funciones de spark
from pyspark import SparkContext
sc = SparkContext("local", "First App")
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
import pandas as pd
import numpy as np
import pyspark.pandas as ps
# Creating a pandas-on-Spark Series by passing a list of values, letting pandas API on Spark create a default integer index:
s = ps.Series([1, 3, 5, np.nan, 6, 8])

# Creating a pandas-on-Spark DataFrame by passing a dict of objects that can be converted to series-like.
psdf = ps.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])
# Creating a pandas DataFrame by passing a numpy array, with a datetime index and labeled columns:
dates = pd.date_range('20130101', periods=6)
# carga de datos aleatorios
pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
# carga de datos desde pandas en spark
psdf = ps.from_pandas(pdf)
print(type(psdf))

# creado dataframe desde pandas
sdf = spark.createDataFrame(pdf)
sdf.show()
# Creating pandas-on-Spark DataFrame from Spark DataFrame.
psdf = sdf.pandas_api()
print(psdf.dtypes)
psdf.head()
print(psdf.index)
print(psdf.columns)
# convertir a array de numpy
psdf.to_numpy()

psdf.describe()

print(psdf.sort_index(ascending=False))
print(psdf.sort_values(by='B'))

# Missing data
# Pandas API on Spark primarily uses the value np.nan to represent missing data. It is by default not included in computations.
pdf1 = pdf.reindex(index=dates[0:4], columns=list(pdf.columns) + ['E'])
pdf1.loc[dates[0]:dates[1], 'E'] = 1
psdf1 = ps.from_pandas(pdf1)
print(psdf1)

# quitamos los na
print(psdf1.dropna(how='any'))
# ponemos un contenido por na
print(psdf1.fillna(value=5))

# media
print(psdf.mean())

# agrupaciones

psdf = ps.DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
                          'foo', 'bar', 'foo', 'foo'],
                    'B': ['one', 'one', 'two', 'three',
                          'two', 'two', 'one', 'three'],
                    'C': np.random.randn(8),
                    'D': np.random.randn(8)})

print(psdf.groupby('A').sum())
