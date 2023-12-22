import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Crear un SparkSession
spark = pyspark.sql.SparkSession.builder.appName("Ejemplo 1").getOrCreate()

# Load the Pandas libraries with alias 'pd'
import pandas as pd
# Read data from file 'filename.csv'
# (in the same directory that your python process is based)
# Control delimiters, rows, column names with read_csv (see later)
# leo los datos del csv con pandas
data = pd.read_csv("files/sql.csv")

from pyspark.sql.types import *
# como no se que datos van en casa sitio me monto yo la estructura
# aqui van los campos del DF
mySchema = StructType([ StructField("id", IntegerType(), True)\
                       ,StructField("nombre", StringType(), True)\
                       ,StructField("edad", IntegerType(), True)\
                       ,StructField("pais", StringType(), True)\
                    ])
# creo la sesión de spark
sparkSession = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
#uso la sesión para crear un DF desde los datos de pandas
df = sparkSession.createDataFrame(data, schema=mySchema)
print(df.head())
# Mostrar los datos
df.show()
df.printSchema()

# Filtrar los datos
df = df.filter(df.edad >= 30)

# Mostrar los datos
df.show()

# Agregar los datos
df = df.groupBy("pais").agg(F.avg("edad"))

# Mostrar los datos
df.show()