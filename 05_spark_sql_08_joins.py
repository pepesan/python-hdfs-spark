# Requiere: ninguno (Spark local, sin servicios docker).
#
# Joins — combinar dos DataFrames por una columna en común, igual que un
# JOIN de SQL. Es la operación que más veces se hace mal si no se entiende
# bien la diferencia entre cada tipo: aquí se ve el mismo par de tablas
# con los 6 tipos de join más habituales, para poder comparar el
# resultado exacto de cada uno con los mismos datos.
#
# Datos pensados a propósito para que cada tipo de join dé un resultado
# distinto y visible:
# - files/sql.csv: 6 clientes (id 1 a 6).
# - files/pedidos.csv: 5 pedidos — el cliente 1 tiene 2 pedidos, los
#   clientes 2 y 3 tienen 1 cada uno, los clientes 4/5/6 no tienen
#   ninguno, y hay un pedido con id_cliente=99 (un cliente que NO existe
#   en clientes) — un caso real habitual: un pedido "huérfano" por un
#   error de datos, un cliente borrado, etc.
import pyspark

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo joins").getOrCreate()

clientes = spark.read.option("header", "true").option("inferSchema", "true").csv("files/sql.csv")
pedidos = spark.read.option("header", "true").option("inferSchema", "true").csv("files/pedidos.csv")

print("--- Clientes ---")
clientes.show()
print("--- Pedidos ---")
pedidos.show()

condicion = clientes.id == pedidos.id_cliente

# INNER JOIN (por defecto si no se indica "how"): solo las filas que
# encajan en AMBOS lados — ni los clientes sin pedidos (4, 5, 6) ni el
# pedido huérfano (id_cliente=99) aparecen aquí.
print("--- INNER: solo clientes CON pedidos, sin el pedido huérfano ---")
clientes.join(pedidos, condicion, "inner").select(
    "id", "nombre", "id_pedido", "producto"
).orderBy("id").show()

# LEFT (OUTER) JOIN: TODAS las filas del lado izquierdo (clientes),
# aunque no tengan pedido — para los clientes sin pedidos, las columnas
# de "pedidos" quedan a NULL. Es el join más habitual para "quiero ver
# todos los X, tengan o no relación con Y".
print("--- LEFT: TODOS los clientes, con o sin pedidos (NULL si no tienen) ---")
clientes.join(pedidos, condicion, "left").select(
    "id", "nombre", "id_pedido", "producto"
).orderBy("id").show()

# RIGHT (OUTER) JOIN: el espejo del anterior — TODAS las filas del lado
# derecho (pedidos), aunque el cliente no exista. Aquí es donde aparece
# el pedido huérfano (id_cliente=99), con "nombre" a NULL.
print("--- RIGHT: TODOS los pedidos, con o sin cliente válido ---")
clientes.join(pedidos, condicion, "right").select(
    "id_cliente", "nombre", "id_pedido", "producto"
).orderBy("id_pedido").show()

# FULL OUTER JOIN: la unión de LEFT y RIGHT — todo lo de ambos lados,
# encajen o no. Aquí aparecen a la vez los clientes sin pedidos Y el
# pedido huérfano.
print("--- FULL OUTER: todo de ambos lados, encajen o no ---")
clientes.join(pedidos, condicion, "full_outer").select(
    "id", "nombre", "id_cliente", "id_pedido", "producto"
).orderBy("id").show()

# LEFT SEMI JOIN: parecido a un INNER, pero solo con las columnas del
# lado IZQUIERDO (nunca añade columnas del derecho) y sin duplicar filas
# aunque haya varias coincidencias — responde a "¿qué clientes TIENEN al
# menos un pedido?" (el cliente 1 sale una sola vez, no dos, aunque tenga
# 2 pedidos).
print("--- LEFT SEMI: qué clientes tienen al menos un pedido (sin duplicar) ---")
clientes.join(pedidos, condicion, "left_semi").orderBy("id").show()

# LEFT ANTI JOIN: el opuesto exacto del semi join — solo las filas del
# lado izquierdo que NO tienen ninguna coincidencia. Responde a "¿qué
# clientes NO tienen ningún pedido?" (4, 5 y 6).
print("--- LEFT ANTI: qué clientes NO tienen ningún pedido ---")
clientes.join(pedidos, condicion, "left_anti").orderBy("id").show()
