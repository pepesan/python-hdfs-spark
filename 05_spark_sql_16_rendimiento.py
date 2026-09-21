# Requiere: ninguno (Spark local, sin servicios docker).
#
# Rendimiento — herramientas para entender y ajustar CÓMO ejecuta Spark
# una consulta, no solo el resultado. Nivel más avanzado que el resto de
# la familia `05_spark_sql_*`: no hace falta dominarlo para escribir
# consultas correctas, pero ayuda a entender por qué una consulta va
# lenta o consume más memoria de la esperada.
import pyspark
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo rendimiento").getOrCreate()

clientes = spark.read.option("header", "true").option("inferSchema", "true").csv("files/sql.csv")
pedidos = spark.read.option("header", "true").option("inferSchema", "true").csv("files/pedidos.csv")

# --- explain(): ver el PLAN de ejecución sin ejecutar nada todavía ---
# Spark no ejecuta una transformación en el momento de escribirla
# (select, filter, join... son "perezosas" — lazy): arma un plan y solo
# lo ejecuta de verdad al llegar a una acción (show, count, collect...).
# explain() enseña ese plan sin disparar la ejecución — útil para
# comprobar, antes de lanzar algo pesado, qué va a hacer Spark realmente.
consulta = clientes.join(pedidos, clientes.id == pedidos.id_cliente).filter(clientes.edad > 25)

print("--- explain() por defecto: plan físico (lo que se va a ejecutar) ---")
consulta.explain()

# explain(True) (o explain("extended")) añade también el plan LÓGICO
# (antes y después de las optimizaciones del optimizador de Spark, el
# "Catalyst") — mucho más detalle, normalmente solo hace falta para
# depurar algo raro, no para el día a día.
print("--- explain(True): plan completo, lógico + físico ---")
consulta.explain(True)

# --- cache() / persist(): guardar un resultado intermedio en memoria ---
# Por defecto, si un DataFrame se usa dos veces, Spark lo RECALCULA desde
# el origen las dos veces (nada se guarda automáticamente entre
# acciones). cache() marca un DataFrame para que, tras la PRIMERA vez que
# se materialice (con una acción), quede guardado en memoria — las
# siguientes veces que se use, se reutiliza en vez de recalcular. persist()
# es lo mismo, pero permite elegir DÓNDE guardarlo (memoria, disco, o
# ambos) en vez del nivel por defecto de cache().
clientes_mayores = clientes.filter(clientes.edad > 25).cache()
clientes_mayores.count()  # esta acción "materializa" la caché de verdad:
                          # a partir de aquí, clientes_mayores ya está en memoria
print(f"--- Filas de clientes_mayores (servidas desde caché, no recalculadas): {clientes_mayores.count()} ---")
clientes_mayores.unpersist()  # liberar la memoria cuando ya no hace falta

# --- repartition() / coalesce(): cuántas particiones tiene un DataFrame ---
# Spark divide un DataFrame en "particiones" (trozos que se procesan en
# paralelo, potencialmente en máquinas distintas de un cluster real).
print(f"--- Particiones de clientes antes de tocar nada: {clientes.rdd.getNumPartitions()} ---")

# repartition(n): reparte los datos en exactamente n particiones,
# haciendo un shuffle completo (mueve datos entre particiones) — sirve
# tanto para aumentar como para disminuir el número de particiones.
mas_particiones = clientes.repartition(4)
print(f"Tras repartition(4): {mas_particiones.rdd.getNumPartitions()} particiones")

# coalesce(n): reduce el número de particiones SIN shuffle completo (solo
# fusiona particiones vecinas) — más barato que repartition() para
# reducir, pero solo sirve para reducir, no para aumentar (coalesce a un
# número mayor de particiones de las que ya hay no hace nada).
menos_particiones = mas_particiones.coalesce(2)
print(f"Tras coalesce(2): {menos_particiones.rdd.getNumPartitions()} particiones")

# --- broadcast join: para cuando un lado del join es pequeño ---
# En un join normal, si NINGÚN lado es pequeño, Spark reparte (shuffle)
# los datos de AMBOS lados para que las filas con la misma clave acaben
# en la misma máquina (SortMergeJoin) — caro en un cluster real. Si un
# lado SÍ es pequeño (cabe de sobra en memoria), es mucho más barato
# copiarlo ENTERO a todas las máquinas (BroadcastHashJoin) que repartir
# los dos lados.
#
# Con datasets tan pequeños como los de este ejemplo, Spark YA elige
# BroadcastHashJoin él solo, sin pedírselo (por debajo de
# spark.sql.autoBroadcastJoinThreshold, 10MB por defecto, decide
# automáticamente qué lado emisora — aquí "clientes", el más pequeño de
# los dos, "BuildLeft" en el plan). F.broadcast() no cambia el TIPO de
# join aquí (ya era broadcast), pero sí FUERZA qué lado se copia: fíjate
# en "BuildRight" en el segundo plan (ahora es "pedidos" el que se
# copia, porque se lo hemos pedido explícitamente) — útil en un cluster
# real cuando una tabla es más grande que el umbral automático pero se
# sabe, por otra vía, que igualmente cabe en memoria de sobra.
print("--- Plan de un join normal (Spark ya elige broadcast solo, por ser tablas pequeñas) ---")
clientes.join(pedidos, clientes.id == pedidos.id_cliente).explain()

print("--- Plan del mismo join, forzando qué lado se copia con F.broadcast(pedidos) ---")
clientes.join(F.broadcast(pedidos), clientes.id == pedidos.id_cliente).explain()
