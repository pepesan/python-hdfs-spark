# Requiere: ninguno (Spark local, sin servicios docker) + el fichero de datos
# files/1500000_Sales_Records.csv, que NO va en el repo (~187 MB, ver
# .gitignore). Descargar y colocar así:
#   curl -Lo /tmp/sales.zip "https://excelbianalytics.com/wp/wp-content/uploads/2017/07/1500000%20Sales%20Records.zip"
#   unzip -o /tmp/sales.zip -d /tmp/sales
#   mv "/tmp/sales/1500000 Sales Records.csv" files/1500000_Sales_Records.csv
#
# Delta Lake: a diferencia de Parquet/CSV/JSON (formatos que Spark sabe leer
# y escribir de fábrica), Delta es un formato de tabla que añade encima de
# Parquet un log de transacciones (ACID, versionado, "time travel") — para
# poder usar .format("delta") hace falta el paquete "delta-spark" (que trae
# el conector Python) Y decirle a Spark, al crear la SparkSession, que
# cargue las clases Java/Scala correspondientes (spark.sql.extensions +
# spark.sql.catalog.spark_catalog). configure_spark_with_delta_pip() hace
# esa segunda parte por nosotros; la primera vez que se ejecuta, descarga
# el jar de Delta Lake vía Maven (necesita conexión a internet una vez, se
# queda cacheado en ~/.ivy2 para las siguientes ejecuciones).
import pyspark
# conexión a spark "local"
# sólo se usa la biblioteca para acceder a las funciones de spark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# spark.driver.memory: el valor por defecto de Spark en local (1g) se queda
# corto para este dataset (1.5 millones de filas, ~187MB en CSV) al llegar
# a los joins de más abajo (recalculamos OrderDate/ShipDate en DataFrames
# aparte y los volvemos a unir) — sin subir la memoria, el driver revienta
# con OutOfMemoryError a mitad del cálculo. En un cluster real esto se
# ajusta con --driver-memory en spark-submit, no aquí en el código.
builder = SparkSession.builder \
    .appName("Python Spark final example") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
# OJO: df.write...save(...) escribe a disco pero devuelve None (no un
# DataFrame) — es un error habitual pensar que se puede encadenar
# .printSchema() sobre ese resultado. Para inspeccionar lo que se acaba de
# escribir hay que releerlo con spark.read, igual que se haría en una
# sesión distinta que no tuviera ya el DataFrame en memoria.
ruta_delta = "spark-warehouse/delta/ventasp"
df.write.partitionBy("ItemType").format("delta").mode("overwrite").save(ruta_delta)
dfpart = spark.read.format("delta").load(ruta_delta)
dfpart.printSchema()


#from pyspark.sql.types import DateType
#df = df.withColumn("OrderDate", df["OrderDate"].cast(DateType()))
#df = df.withColumn("ShipDate", df["ShipDate"].cast(DateType()))

#Normalizando los datos

from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp
# Creamos un DF con la columna recalculada
# El patrón de fecha debe coincidir EXACTAMENTE con el formato del dato de
# origen: este CSV trae días/meses sin cero inicial (p. ej. "11/6/2015"),
# así que el patrón tiene que ser "M/d/yyyy" (un solo símbolo), no
# "MM/dd/yyyy" (dos símbolos, que exige cero inicial) — si no, Spark lanza
# CANNOT_PARSE_TIMESTAMP en cuanto encuentra la primera fecha sin ceros.
df2 = df.select(from_unixtime(unix_timestamp('OrderDate', 'M/d/yyyy')).alias('OrderDate'))
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

#Hacemos lo mismo con ShipDate (mismo motivo: "M/d/yyyy", no "MM/dd/yyyy")
df3= df.select(from_unixtime(unix_timestamp('ShipDate', 'M/d/yyyy')).alias('ShipDate'))
df = df.drop('ShipDate')
from pyspark.sql.functions import monotonically_increasing_id
df = df.withColumn("id", monotonically_increasing_id())
df3 = df3.withColumn("id", monotonically_increasing_id())
df = df.join(df3, "id", "outer").drop("id")
print(df.head(2))

#permanent_table_name = "ventasp"

#df.write.format("delta").saveAsTable(permanent_table_name)
