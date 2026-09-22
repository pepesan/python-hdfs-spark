# Requiere: ninguno (Spark local, sin servicios docker) + el fichero de datos
# files/1500000_Sales_Records.csv, que NO va en el repo (~187 MB, ver
# .gitignore). Descargar y colocar así:
#   curl -Lo /tmp/sales.zip "https://excelbianalytics.com/wp/wp-content/uploads/2017/07/1500000%20Sales%20Records.zip"
#   unzip -o /tmp/sales.zip -d /tmp/sales
#   mv "/tmp/sales/1500000 Sales Records.csv" files/1500000_Sales_Records.csv
#
# Ejemplo "de repaso final": une varias piezas ya vistas en otros scripts
# (lectura de CSV, limpieza de nombres de columnas, parseo de fechas,
# escritura/relectura particionada) sobre un dataset más grande de lo
# habitual (1.5 millones de filas) y en formato Delta Lake.
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
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# spark.driver.memory: el valor por defecto de Spark en local (1g) se queda
# corto para este dataset (1.5 millones de filas, ~187MB en CSV) al llegar
# a los joins de más abajo (recalculamos OrderDate/ShipDate en DataFrames
# aparte y los volvemos a unir) — sin subir la memoria, el driver revienta
# con OutOfMemoryError a mitad del cálculo. En un cluster real esto se
# ajusta con --driver-memory en spark-submit, no aquí en el código.
builder = SparkSession.builder \
    .appName("practica-final-ventas") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

ruta_csv = "./files/1500000_Sales_Records.csv"

df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("sep", ",") \
    .load(ruta_csv)

print(df.head(2))

# El CSV trae los nombres de columna con espacios ("Item Type") — se
# renombran a CamelCase sin espacios, más cómodo de usar en el resto del
# script (df.ItemType en vez de df["Item Type"]).
df = df.withColumnRenamed("Item Type", "ItemType")
df = df.withColumnRenamed("Sales Channel", "SalesChannel")
df = df.withColumnRenamed("Order Priority", "OrderPriority")
df = df.withColumnRenamed("Order Date", "OrderDate")
df = df.withColumnRenamed("Order ID", "OrderID")
df = df.withColumnRenamed("Ship Date", "ShipDate")
df = df.withColumnRenamed("Units Sold", "UnitsSold")
df = df.withColumnRenamed("Unit Price", "UnitPrice")
df = df.withColumnRenamed("Unit Cost", "UnitCost")
df = df.withColumnRenamed("Total Revenue", "TotalRevenue")
df = df.withColumnRenamed("Total Cost", "TotalCost")
df = df.withColumnRenamed("Total Profit", "TotalProfit")
df.printSchema()

# Escritura particionada en Delta Lake: cada valor distinto de "ItemType"
# va a su propia carpeta bajo ruta_delta — igual que el particionado de
# Parquet/CSV normal, Delta añade encima el log de transacciones.
# OJO: df.write...save(...) escribe a disco pero devuelve None (no un
# DataFrame) — es un error habitual pensar que se puede encadenar
# .printSchema() sobre ese resultado. Para inspeccionar lo que se acaba de
# escribir hay que releerlo con spark.read, igual que se haría en una
# sesión distinta que no tuviera ya el DataFrame en memoria.
ruta_delta = "spark-warehouse/delta/ventasp"
df.write.partitionBy("ItemType").format("delta").mode("overwrite").save(ruta_delta)
dfpart = spark.read.format("delta").load(ruta_delta)
dfpart.printSchema()

# Normalizando las fechas: llegan como texto ("7/27/2012"), hay que
# convertirlas a timestamp de verdad para poder ordenarlas/filtrarlas
# como fechas más adelante.
from pyspark.sql.functions import from_unixtime, unix_timestamp, monotonically_increasing_id

# El patrón de fecha debe coincidir EXACTAMENTE con el formato del dato de
# origen: este CSV trae días/meses sin cero inicial (p. ej. "7/27/2012"),
# así que el patrón tiene que ser "M/d/yyyy" (un solo símbolo), no
# "MM/dd/yyyy" (dos símbolos, que exige cero inicial) — si no, Spark lanza
# CANNOT_PARSE_TIMESTAMP en cuanto encuentra la primera fecha sin ceros.
df_order_date = df.select(from_unixtime(unix_timestamp('OrderDate', 'M/d/yyyy')).alias('OrderDate'))
df = df.drop('OrderDate')

# Al no compartir ninguna columna, hace falta un id artificial para poder
# volver a unir df (sin OrderDate) con df_order_date (solo OrderDate ya
# convertida) fila a fila — monotonically_increasing_id() genera un id
# único y creciente por fila, válido aquí porque ambos DataFrames vienen
# del mismo df original y conservan el mismo orden relativo.
df = df.withColumn("id", monotonically_increasing_id())
df_order_date = df_order_date.withColumn("id", monotonically_increasing_id())
df = df.join(df_order_date, "id", "outer").drop("id")
print(df.head(2))

# Mismo tratamiento para ShipDate (mismo motivo: "M/d/yyyy", no "MM/dd/yyyy").
df_ship_date = df.select(from_unixtime(unix_timestamp('ShipDate', 'M/d/yyyy')).alias('ShipDate'))
df = df.drop('ShipDate')
df = df.withColumn("id", monotonically_increasing_id())
df_ship_date = df_ship_date.withColumn("id", monotonically_increasing_id())
df = df.join(df_ship_date, "id", "outer").drop("id")
print(df.head(2))
