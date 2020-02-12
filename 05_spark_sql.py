# para abrir sesiones
from pyspark.sql import SparkSession
# Load the Pandas libraries with alias 'pd'
import pandas as pd
# Read data from file 'filename.csv'
# (in the same directory that your python process is based)
# Control delimiters, rows, column names with read_csv (see later)
data = pd.read_csv("./files/con_cabecera.csv")
print(data.shape)
# Preview the first 5 lines of the loaded data
print(data.head())
print(data.keys())
print(data.keys()[0])

from pyspark.sql.types import *

mySchema = StructType([ StructField("firstName", StringType(), True)\
                       ,StructField("lastName", StringType(), True)\
                       ,StructField("email", StringType(), True)\
                       ,StructField("phoneNumber", LongType(), True)\
                       ,StructField("age", IntegerType(), True)\
                    ])
sparkSession = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

df = sparkSession.createDataFrame(data, schema=mySchema)
print(df.head())
# Quita duplicados
df = df.dropDuplicates()

from pyspark.sql import SQLContext
sqlContext = SQLContext(sparkSession)

df.registerTempTable("usuarios")

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

df.select("firstName",
          F.when(df.age > 30, 1).otherwise(0)).show()

df.select("firstName", df.lastName.like("Bond")).show()

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

