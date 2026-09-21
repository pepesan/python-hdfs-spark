# Requiere: ninguno (Spark local, sin servicios docker).
#
# Funciones de fecha — ninguno de los datasets del proyecto trae fechas
# reales, así que aquí se generan a mano un puñado de pedidos con fecha de
# compra y de entrega (como texto, que es como suelen llegar de un CSV/una
# API — casi nunca ya como tipo fecha).
import pyspark
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo fechas").getOrCreate()

pedidos = spark.createDataFrame([
    (1, "2024-01-15", "2024-01-18"),
    (2, "2024-03-02", "2024-03-02"),   # entrega el mismo día
    (3, "2024-06-20", "2024-06-25"),
    (4, "2024-11-08", "2024-11-20"),
    (5, "2024-12-24", "2025-01-03"),   # cruza de año
], ["id_pedido", "fecha_compra", "fecha_entrega"])

pedidos.printSchema()

# to_date() convierte el texto ("2024-01-15") a un tipo Date de verdad —
# imprescindible antes de poder usar el resto de funciones de fecha
# (datediff, year, month...), que no funcionan sobre texto.
pedidos = pedidos.withColumn("fecha_compra", F.to_date("fecha_compra")) \
    .withColumn("fecha_entrega", F.to_date("fecha_entrega"))
pedidos.printSchema()

# datediff(fin, inicio): número de días entre dos fechas (fin - inicio,
# en ese orden — invertirlo da el mismo número en negativo).
con_plazo = pedidos.withColumn(
    "dias_hasta_entrega", F.datediff(pedidos.fecha_entrega, pedidos.fecha_compra)
)
print("--- Días que tardó cada pedido en entregarse ---")
con_plazo.select("id_pedido", "fecha_compra", "fecha_entrega", "dias_hasta_entrega").show()

# year()/month()/dayofweek(): extraer partes de una fecha. dayofweek()
# devuelve 1=domingo, 2=lunes... 7=sábado (convención de Spark, no
# empieza en lunes como cabría esperar).
desglose = pedidos.withColumn("anio_compra", F.year("fecha_compra")) \
    .withColumn("mes_compra", F.month("fecha_compra")) \
    .withColumn("dia_semana_compra", F.dayofweek("fecha_compra"))
print("--- Año/mes/día de la semana de cada compra ---")
desglose.select("id_pedido", "fecha_compra", "anio_compra", "mes_compra", "dia_semana_compra").show()

# date_add(fecha, n): suma n días a una fecha (date_sub para restar). Aquí
# se usa para calcular una fecha límite de devolución (30 días desde la
# entrega) — un cálculo típico de negocio con fechas.
con_devolucion = pedidos.withColumn(
    "fecha_limite_devolucion", F.date_add(pedidos.fecha_entrega, 30)
)
print("--- Fecha límite de devolución (30 días desde la entrega) ---")
con_devolucion.select("id_pedido", "fecha_entrega", "fecha_limite_devolucion").show()

# date_format(fecha, patrón): convierte una fecha a texto con el formato
# que se quiera — lo contrario de to_date(). Útil para mostrar la fecha
# en un informe con un formato concreto (aquí, "día/mes/año").
con_formato = pedidos.withColumn(
    "fecha_compra_es", F.date_format(pedidos.fecha_compra, "dd/MM/yyyy")
)
print("--- Fecha de compra en formato dd/MM/yyyy ---")
con_formato.select("id_pedido", "fecha_compra", "fecha_compra_es").show()
