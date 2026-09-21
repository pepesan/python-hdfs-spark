# Requiere: ninguno (Spark local, sin servicios docker).
#
# Window functions (funciones de ventana) — a diferencia de groupBy(), que
# colapsa varias filas en una sola por grupo, una window function calcula
# un valor PARA CADA FILA mirando un conjunto de filas relacionadas con
# ella (su "ventana": p. ej. "las demás filas de su mismo país", o "las
# filas anteriores según un orden") sin perder el detalle fila a fila.
# Es la herramienta para preguntas como "¿en qué puesto queda cada fila
# dentro de su grupo?" o "¿cuál es el acumulado hasta esta fila?", que un
# groupBy() normal no puede responder.
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo window functions").getOrCreate()

clientes = spark.read.option("header", "true").option("inferSchema", "true").csv("files/sql.csv")
clientes.show()

# Una ventana se define con partitionBy() (agrupa, como groupBy, pero SIN
# colapsar filas) y/o orderBy() (dentro de cada grupo, en qué orden se
# recorren las filas). Aquí: "las filas se ordenan por edad, dentro de
# cada país por separado".
ventana_por_pais = Window.partitionBy("pais").orderBy("edad")

# row_number(): la posición de cada fila dentro de su ventana (1, 2, 3...
# sin repetir nunca un número, ni siquiera con empates).
# rank(): igual, pero los empates comparten puesto y se salta el
# siguiente número (si dos filas empatan en el puesto 1, la siguiente es
# la 3, no la 2).
# dense_rank(): como rank(), pero SIN saltar números tras un empate (si
# dos empatan en el puesto 1, la siguiente es la 2).
resultado = clientes.withColumn("fila_num", F.row_number().over(ventana_por_pais)) \
    .withColumn("puesto", F.rank().over(ventana_por_pais)) \
    .withColumn("puesto_denso", F.dense_rank().over(ventana_por_pais))

print("--- row_number / rank / dense_rank, por país ordenado por edad ---")
resultado.select("pais", "nombre", "edad", "fila_num", "puesto", "puesto_denso") \
    .orderBy("pais", "edad").show()

# lag(columna, n): el valor de esa columna n filas ANTES en la ventana
# (NULL si no hay fila anterior, p. ej. la primera de cada país).
# lead(columna, n): el valor n filas DESPUÉS (NULL si no hay fila siguiente).
# Útil para comparar cada fila con la anterior/siguiente sin un self-join.
comparativa = clientes.withColumn(
    "edad_anterior_mismo_pais", F.lag("edad", 1).over(ventana_por_pais)
).withColumn(
    "edad_siguiente_mismo_pais", F.lead("edad", 1).over(ventana_por_pais)
)
print("--- lag/lead: edad de la persona anterior/siguiente del mismo país ---")
comparativa.select("pais", "nombre", "edad", "edad_anterior_mismo_pais", "edad_siguiente_mismo_pais") \
    .orderBy("pais", "edad").show()

# Total acumulado (running total): una ventana SIN partitionBy (una sola
# "ventana" para todo el DataFrame) ordenada por edad, sumando desde la
# primera fila hasta la fila actual — rowsBetween(unboundedPreceding,
# currentRow) es justo esa definición ("desde el principio hasta aquí").
ventana_acumulada = Window.orderBy("edad").rowsBetween(Window.unboundedPreceding, Window.currentRow)
acumulado = clientes.withColumn("suma_edades_acumulada", F.sum("edad").over(ventana_acumulada))
print("--- Suma acumulada de edades, ordenado de menor a mayor edad ---")
acumulado.select("nombre", "edad", "suma_edades_acumulada").orderBy("edad").show()
