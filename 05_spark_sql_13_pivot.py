# Requiere: ninguno (Spark local, sin servicios docker).
#
# Pivot — convertir valores de una columna en columnas nuevas, con una
# agregación en cada celda. Es el mismo concepto que una "tabla dinámica"
# de una hoja de cálculo: aquí se parte de ventas en formato "largo" (una
# fila por vendedor y trimestre) y se pasa a formato "ancho" (una fila por
# vendedor, una columna por trimestre) — mucho más fácil de leer para un
# informe, aunque menos cómodo para seguir haciendo más transformaciones
# con Spark (por eso se suele dejar como el ÚLTIMO paso, justo antes de
# mostrar o exportar el resultado).
import pyspark
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo pivot").getOrCreate()

ventas = spark.createDataFrame([
    ("Ana", "Q1", 1000),
    ("Ana", "Q2", 1500),
    ("Ana", "Q3", 1200),
    ("Bruno", "Q1", 800),
    ("Bruno", "Q3", 1200),
    ("Carla", "Q2", 900),
    ("Carla", "Q4", 1100),
    ("Carla", "Q4", 300),   # una segunda venta de Carla en Q4, para ver que se SUMAN
], ["vendedor", "trimestre", "importe"])

print("--- Ventas en formato \"largo\" (una fila por vendedor y trimestre) ---")
ventas.show()

# groupBy(...).pivot(columna).agg(...): agrupa por vendedor (como un
# groupBy normal) y, DENTRO de cada grupo, convierte cada valor distinto
# de "trimestre" en una columna nueva — el valor de cada celda es el
# resultado de aplicar la agregación (aquí, sum) a las filas de ese
# vendedor y ese trimestre. Si un vendedor no tiene ventas en un
# trimestre (Ana en Q4, Bruno en Q2/Q4), la celda queda a NULL — no a 0.
print("--- Pivotado: una columna por trimestre, suma de importes ---")
ventas.groupBy("vendedor").pivot("trimestre").agg(F.sum("importe")).orderBy("vendedor").show()

# Pasarle a pivot() la lista de valores esperados (en vez de dejar que
# Spark los descubra solos) es más rápido en datasets grandes: sin la
# lista, Spark necesita hacer una pasada extra sobre los datos solo para
# averiguar qué valores distintos tiene la columna antes de poder
# construir las columnas — con la lista, se salta ese paso. También sirve
# para fijar el ORDEN de las columnas resultantes (sin lista, salen en
# orden alfabético) y para quedarse solo con un subconjunto de valores.
print("--- Mismo pivot, con la lista de trimestres fijada a mano (más rápido y en orden) ---")
ventas.groupBy("vendedor").pivot("trimestre", ["Q1", "Q2", "Q3", "Q4"]).agg(F.sum("importe")) \
    .orderBy("vendedor").show()

# NULL vs 0: si para un informe interesa ver 0 en vez de NULL en los
# trimestres sin ventas, hay que rellenarlo explícitamente después del
# pivot — pivot() por sí solo no lo hace (NULL significa "no hay dato",
# 0 significaría "hubo ventas por importe cero", son cosas distintas).
print("--- Igual, pero con 0 en vez de NULL donde no hubo ventas ---")
ventas.groupBy("vendedor").pivot("trimestre", ["Q1", "Q2", "Q3", "Q4"]).agg(F.sum("importe")) \
    .na.fill(0).orderBy("vendedor").show()
