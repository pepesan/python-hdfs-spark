# Requiere: ninguno (Spark local, sin servicios docker).
#
# Operaciones de conjuntos — combinar o comparar dos DataFrames CON EL
# MISMO NÚMERO DE COLUMNAS (aunque no necesariamente el mismo orden, ver
# más abajo), tratándolos como conjuntos de filas.
import pyspark

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo operaciones de conjuntos").getOrCreate()

clientes_madrid = spark.createDataFrame([
    (1, "Ana", "Madrid"),
    (2, "Bruno", "Madrid"),
    (3, "Carla", "Madrid"),
], ["id", "nombre", "ciudad"])

# Carla (id=3) está en las dos listas a propósito — es clienta de las dos
# tiendas — para poder ver la diferencia entre "juntar todo" (union) y
# "solo lo que tienen en común"/"solo lo que no comparten" (intersect/except).
clientes_barcelona = spark.createDataFrame([
    (3, "Carla", "Madrid"),
    (4, "David", "Barcelona"),
    (5, "Elena", "Barcelona"),
], ["id", "nombre", "ciudad"])

print("--- Clientes de la tienda de Madrid ---")
clientes_madrid.show()
print("--- Clientes de la tienda de Barcelona ---")
clientes_barcelona.show()

# union(): junta las filas de los dos DataFrames, SIN quitar duplicados
# (a pesar de que en SQL "UNION" sí los quita — es una diferencia real
# con el UNION de SQL que sorprende a quien viene de ahí). Carla sale
# DOS veces en el resultado, una por cada DataFrame de origen.
print("--- union(): las filas de Carla se duplican (union NO quita duplicados) ---")
union_simple = clientes_madrid.union(clientes_barcelona)
union_simple.show()
print(f"Filas totales: {union_simple.count()} (3 + 3 = 6, sin deduplicar)")

# Para un union que sí elimine duplicados (como el UNION de SQL) hay que
# encadenar distinct() explícitamente.
print("--- union().distinct(): ahora Carla sale una sola vez ---")
union_sin_duplicados = clientes_madrid.union(clientes_barcelona).distinct()
union_sin_duplicados.show()
print(f"Filas totales: {union_sin_duplicados.count()} (6 - 1 duplicado exacto = 5)")

# intersect(): solo las filas que existen EN AMBOS DataFrames (comparando
# la fila entera, no solo una columna) — aquí, solo Carla.
print("--- intersect(): filas que están en ambas tiendas ---")
clientes_madrid.intersect(clientes_barcelona).show()

# exceptAll(): las filas del primer DataFrame que NO están en el segundo
# — el "resta de conjuntos". Aquí, los clientes exclusivos de Madrid.
print("--- exceptAll(): clientes exclusivos de Madrid (no están en Barcelona) ---")
clientes_madrid.exceptAll(clientes_barcelona).show()

# --- union() vs unionByName(): el orden de las columnas SÍ importa ---
# clientes_valencia tiene las mismas 3 columnas, pero con "nombre" y
# "ciudad" INTERCAMBIADAS de posición — algo que pasa fácilmente si los
# dos DataFrames vienen de fuentes distintas (dos CSV con las columnas en
# distinto orden, por ejemplo). "id" se deja en su sitio a propósito: si
# el desajuste fuera de tipo (texto contra número), Spark con ANSI mode
# (activo por defecto) lo detecta y lanza un error de cast — el caso
# realmente peligroso es este otro, donde los dos lados que se cruzan son
# del MISMO tipo (dos columnas de texto), así que Spark no tiene manera
# de darse cuenta de que están mal emparejadas.
clientes_valencia = spark.createDataFrame([
    (6, "Valencia", "Fran"),
], ["id", "ciudad", "nombre"])

# union() junta por POSICIÓN, no por nombre de columna: la 2ª columna de
# cada lado se junta con la 2ª del otro lado, la 3ª con la 3ª... aunque se
# llamen distinto. Aquí "nombre" (clientes_madrid) se junta con "ciudad"
# (clientes_valencia) y viceversa — el resultado queda con "Valencia" en
# la columna "nombre" y "Fran" en la columna "ciudad", AL REVÉS, sin
# ningún error ni aviso: los tipos coinciden (ambas son texto), así que
# Spark no tiene forma de detectar el problema.
print("--- union() con columnas en distinto orden: mal mezclado, SIN error ---")
clientes_madrid.union(clientes_valencia).show()

# unionByName() junta por NOMBRE de columna, no por posición — el
# resultado sale bien colocado, con independencia del orden de columnas
# de cada lado. Es la opción más segura salvo que se sepa con certeza que
# el orden de columnas coincide siempre.
print("--- unionByName(): mismo caso, pero bien colocado por nombre de columna ---")
clientes_madrid.unionByName(clientes_valencia).show()
