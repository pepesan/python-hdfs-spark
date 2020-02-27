
import pyspark
# conexión a spark "local"
# sólo se usa la biblioteca para acceder a las funciones de spark
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark final example") \
    .getOrCreate()

# File location and type
file_location = "./files/1500000_Sales_Records.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

print(df.head(2))

# Create a view or table

#temp_table_name = "ventas"

#df.createOrReplaceTempView(temp_table_name)

# Cambiando el nombre de las columnas
df=df.withColumnRenamed("Item Type","ItemType")
df=df.withColumnRenamed("Sales Channel","SalesChannel")
df=df.withColumnRenamed("Order Priority","OrderPriority")
df=df.withColumnRenamed("Order Date","OrderDate")
df=df.withColumnRenamed("Order ID","OrderID")
df=df.withColumnRenamed("Ship Date","ShipDate")
df=df.withColumnRenamed("Units Sold","UnitsSold")
df=df.withColumnRenamed("Unit Price","UnitPrice")
df=df.withColumnRenamed("Unit Cost","UnitCost")
df=df.withColumnRenamed("Total Revenue","TotalRevenue")
df=df.withColumnRenamed("Total Cost","TotalCost")
df=df.withColumnRenamed("Total Profit","TotalProfit")
# Schema del DF
df.printSchema()

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

#permanent_table_name = "ventas3"

#df.write.format("parquet").saveAsTable(permanent_table_name)

#permanent_table_name = "ventas3d"

#df.write.format("delta").saveAsTable(permanent_table_name)
# Particionado de tabla delta
dfpart=df.write.partitionBy("ItemType").format("delta").save("/mnt/delta/ventasp")
dfpart.printSchema()


#from pyspark.sql.types import DateType
#df = df.withColumn("OrderDate", df["OrderDate"].cast(DateType()))
#df = df.withColumn("ShipDate", df["ShipDate"].cast(DateType()))

#Normalizando los datos

from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp
# Creamos un DF con la columna recalculada
df2 = df.select(from_unixtime(unix_timestamp('OrderDate', 'MM/dd/yyyy')).alias('OrderDate'))
# Borramos las columna original
df = df.drop('OrderDate')
# Creamos una columna en ambos DF llamado id
from pyspark.sql.functions import monotonically_increasing_id
df = df.withColumn("id", monotonically_increasing_id())

df2 = df2.withColumn("id", monotonically_increasing_id())
# Unimos los DF en uno
df = df.join(df2, "id", "outer").drop("id")
# Renombramos la columna
# df = df.withColumnRenamed("OrderDateP","OrderDate")
print(df.head(2))

#Hacemos lo mismo con ShipDate
df3= df.select(from_unixtime(unix_timestamp('ShipDate', 'MM/dd/yyy')).alias('ShipDate'))
df = df.drop('ShipDate')
from pyspark.sql.functions import monotonically_increasing_id
df = df.withColumn("id", monotonically_increasing_id())
df3 = df3.withColumn("id", monotonically_increasing_id())
df = df.join(df3, "id", "outer").drop("id")
print(df.head(2))

#permanent_table_name = "ventasp"

#df.write.format("delta").saveAsTable(permanent_table_name)
