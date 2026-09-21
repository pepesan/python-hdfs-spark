# Requiere: ninguno (Spark local, sin servicios docker) + conexión a
# internet la primera vez (descarga el jar de GraphFrames vía Maven, se
# queda cacheado en ~/.ivy2 para las siguientes ejecuciones).
#
# GraphX vs GraphFrames: "GraphX" es la API de grafos original de Spark,
# basada en RDDs — de bajo nivel y solo disponible desde Scala/Java, sin
# API en Python. "GraphFrames" es la librería (de terceros, no viene con
# Spark) que expone grafos sobre DataFrames, con API en Python — es lo que
# usa este script (a pesar del nombre del fichero, heredado de la versión
# anterior del ejemplo). Un grafo en GraphFrames son dos DataFrames: uno de
# "vertices" (nodos, con una columna "id" obligatoria) y otro de "edges"
# (aristas, con columnas "src"/"dst" obligatorias que apuntan a ids de
# vertices).
#
# El paquete io.graphframes:graphframes-spark4_2.13 hay que indicárselo a
# Spark como coordenada Maven ANTES de crear la SparkSession (variable de
# entorno PYSPARK_SUBMIT_ARGS) — no basta con "pip install graphframes-py"
# (eso solo trae el envoltorio Python, que llama a clases Java/Scala que
# vienen en el jar). El nombre del artefacto codifica la versión de Spark
# (spark4) y de Scala (2.13) con las que es compatible — usar el artefacto
# equivocado (p. ej. el de Spark 2.3/Scala 2.11 de versiones antiguas de
# este script) falla en tiempo de carga del jar, no al importar el paquete
# Python.
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.graphframes:graphframes-spark4_2.13:0.12.2 pyspark-shell'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('graphframes-example').getOrCreate()

from graphframes import GraphFrame

# Grafo de ejemplo propio, en vez de un dataset externo sin procedencia
# clara: dos grupos de personas COMPLETAMENTE conectadas entre sí dentro de
# cada grupo ("a,b,c,d" y "e,f,g,h"), unidos solo por una relación entre
# "c" y "e" — con esta densidad, labelPropagation (más abajo) converge de
# forma fiable a 2 comunidades, una por grupo (con grupos menos densos, el
# algoritmo puede quedarse a medias y encontrar subgrupos de más).
# Guardado en Parquet (files/graph_vertices.snappy.parquet y
# files/graph_edges.snappy.parquet) para poder reutilizarlo también desde
# otros ejemplos del proyecto, no solo este. Cada arista se guardó ya en
# ambos sentidos (GraphFrame es un grafo DIRIGIDO: para representar una
# relación "sin dirección" — si Ana conoce a Bruno, Bruno conoce a Ana —
# hay que añadir la arista en los dos sentidos explícitamente, GraphFrames
# no lo hace solo).
vertices = spark.read.parquet("files/graph_vertices.snappy.parquet")
edges = spark.read.parquet("files/graph_edges.snappy.parquet")

graph = GraphFrame(vertices, edges)
print(graph)

# --- Label Propagation Algorithm (LPA): detección de comunidades ---
# Cada nodo empieza con su propia "etiqueta" (label, un número interno sin
# significado propio) y, en cada iteración, adopta la etiqueta más
# frecuente entre sus vecinos. Con este grafo (2 grupos muy conectados +
# 1 puente) sería intuitivo esperar exactamente 2 comunidades — pero
# ejecutándolo se ve que NO siempre es así (probado con maxIter de 5 a 50,
# siempre da 3): "e,f,g,h" sí converge a una sola etiqueta, pero "a,b,c,d"
# se divide en "a,b" y "c,d". Es un comportamiento real del algoritmo, no
# un bug del ejemplo: LPA es síncrono (todos los nodos actualizan a la vez
# en cada ronda, mirando el estado de la ronda anterior) y, en un subgrafo
# muy simétrico como un grupo totalmente conectado, puede quedar "empatado"
# entre varias etiquetas igual de frecuentes — el desempate depende del
# VALOR numérico de la etiqueta (arbitrario), así que el resultado exacto
# no es predecible a simple vista ni siquiera en grafos pequeños y
# aparentemente obvios. Lección: no dar por hecho el resultado de un
# algoritmo de grafos sin ejecutarlo — con LPA en particular, más
# iteraciones no arreglan un empate estructural como este.
comunidades = graph.labelPropagation(maxIter=10)
comunidades.orderBy("id").show()
print(f"Comunidades encontradas: {comunidades.select('label').distinct().count()}")

# --- Grado de entrada (inDegrees): cuántas relaciones "entrantes" tiene cada nodo ---
# Al haber añadido cada relación en ambos sentidos, el grado de entrada
# coincide aquí con el número total de conexiones de cada persona.
graph.inDegrees.join(vertices, on="id") \
    .orderBy("inDegree", ascending=False).show()

# --- PageRank: importancia de cada nodo según cuántos (y cuán importantes)
# son los nodos que apuntan a él --- el algoritmo que hizo famoso a Google
# para ordenar páginas web por relevancia, aplicable a cualquier grafo.
resultado = graph.pageRank(resetProbability=0.15, maxIter=10)
resultado.vertices.select("id", "name", "pagerank") \
    .orderBy("pagerank", ascending=False).show()

spark.stop()
