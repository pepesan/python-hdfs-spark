import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType

# Crear un SparkSession
spark = pyspark.sql.SparkSession.builder.appName("Ejemplo").getOrCreate()

# Read multiline json file
# Necesitamos pasar el jar
#df = spark.read.format("avro").load("files/user.avro")
# Mostrar los datos
#df.show()

#df.printSchema()

# volcado a fichero
#df.write.json("files/salida")



