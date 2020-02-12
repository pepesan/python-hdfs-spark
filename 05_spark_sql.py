# para abrir sesiones
from pyspark.sql import SparkSession
# Load the Pandas libraries with alias 'pd'
import pandas as pd
# Read data from file 'filename.csv'
# (in the same directory that your python process is based)
# Control delimiters, rows, column names with read_csv (see later)
# leo los datos del csv con pandas
data = pd.read_csv("./files/con_cabecera.csv")
# shape es la forma (filas,columnas)
print(data.shape)
# Preview the first 5 lines of the loaded data
print(data.head())
print(data.keys())
print(data.keys()[0])

from pyspark.sql.types import *
# como no se que datos van en casa sitio me monto yo la estructura
# aqui van los campos del DF
mySchema = StructType([ StructField("firstName", StringType(), True)\
                       ,StructField("lastName", StringType(), True)\
                       ,StructField("email", StringType(), True)\
                       ,StructField("phoneNumber", LongType(), True)\
                       ,StructField("age", IntegerType(), True)\
                    ])
# creo la sesión de spark
sparkSession = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
#uso la sesión para crear un DF desde los datos de pandas
df = sparkSession.createDataFrame(data, schema=mySchema)
print(df.head())
# Quita duplicados, aqui ya se modifica
df = df.dropDuplicates()

from pyspark.sql import SQLContext
sqlContext = SQLContext(sparkSession)
# creo una tabla temporal en spark con los datos del DF
df.registerTempTable("usuarios")
# dataframe consutable por sql de manera paralela
# aqui devuelve todos los datos del DF
res = sqlContext.sql("Select * from usuarios")
res.show()
res = sqlContext.sql("Select firstName from usuarios")
res.show()
res = sqlContext.sql("Select firstName from usuarios where lastName='Doe'")
res.show()

res = sqlContext.sql("Select * from usuarios")
res.show()

res1 = res.select("*")
res1.show()
res1 = res.select("firstName")
res1.show()

from pyspark.sql import functions as F
# en este caso devolverá dos campos
# el nombre y el otro depende de la edad
# 1 en el caso que la edad sea mayor que 30
# y 0 en el resto de los casos
# almaceno el resultado en otrodf
otrodf=df.select("firstName",
          F.when(df.age > 30, 1).otherwise(0))
# aqui estamos filtrando lo que el apellido sea Bond
otrodf=df.select("firstName", df.lastName.like("Bond")).show()
"""
df.sort("age", ascending=True).show()

dfs = df.withColumnRenamed('telePhoneNumber', 'phoneNumber')
dfs.show()
dfs = dfs.drop("address", "phoneNumber")
dfs.show()


# salvando DF's
#res.write.save('./files/salida-csv', format='csv')
#res.write.save('./files/salida-json', format='json')
#res.write.save("./files/nameAndCity.parquet")
# cerrando a sesión
sparkSession.stop()

"""