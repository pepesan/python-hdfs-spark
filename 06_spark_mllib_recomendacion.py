# Requiere: ninguno (Spark local, sin servicios docker).
#
# Sistemas de recomendación (filtrado colaborativo) — un tipo de problema
# de ML distinto a los otros 4 ejemplos de la familia `06_spark_mllib_*`:
# no se predice una categoría (clasificación) ni un número a partir de
# características propias de cada fila (regresión), sino que se rellenan
# los huecos de una matriz usuario × producto a partir de las valoraciones
# que SÍ existen — la idea de fondo es "a usuarios que se parecen les
# gustan cosas parecidas", sin necesidad de saber NADA sobre el contenido
# de cada producto (ni género, ni actores, ni palabras clave: solo qué
# usuario valoró qué producto y con qué nota). Es el algoritmo detrás de
# "recomendado para ti" en cualquier plataforma de streaming/tienda online.
#
# ALS (Alternating Least Squares) es el algoritmo de filtrado colaborativo
# que trae MLlib — pensado específicamente para que escale bien con
# millones de usuarios/productos en un cluster distribuido (a diferencia
# de otros enfoques de recomendación que no paralelizan tan bien).
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName('recomendacion-als').getOrCreate()

# Películas de ejemplo, con dos "gustos" claramente diferenciados a
# propósito (ciencia ficción vs. romance) para que las recomendaciones
# finales se puedan verificar a ojo, no solo confiar en una métrica.
peliculas = {
    0: "Matrix", 1: "Inception", 2: "Interstellar",       # ciencia ficción
    3: "Titanic", 4: "El Diario de Noa", 5: "La La Land",  # romance
}

# (usuario, pelicula, valoracion de 1 a 5) — los usuarios 0/1/2 prefieren
# ciencia ficción, los usuarios 3/4/5 prefieren romance. A propósito
# faltan valoraciones (ninguna matriz de valoraciones real está
# completa) — en particular, el usuario 0 (fan de ciencia ficción) NUNCA
# ha valorado "Interstellar" (película 2): es la que se va a comprobar
# que el modelo recomienda al final.
valoraciones = spark.createDataFrame([
    (0, 0, 5.0), (0, 1, 4.0),                          # usuario 0: no ha visto la 2
    (1, 0, 4.0), (1, 1, 5.0), (1, 2, 5.0),
    (2, 0, 5.0), (2, 2, 4.0),
    (0, 3, 1.0), (1, 4, 2.0), (2, 5, 1.0),             # a los de ciencia ficción no les gusta el romance
    (3, 3, 5.0), (3, 4, 4.0),
    (4, 4, 5.0), (4, 5, 5.0), (4, 3, 4.0),
    (5, 5, 4.0), (5, 3, 5.0),
    (3, 0, 1.0), (4, 1, 2.0), (5, 2, 1.0),             # a los de romance no les gusta la ciencia ficción
], ["usuario", "pelicula", "valoracion"])

(entrenamiento, prueba) = valoraciones.randomSplit([0.8, 0.2], seed=42)

# coldStartStrategy="drop": ALS predice NaN para un usuario o producto que
# no ha visto NUNCA en el conjunto de entrenamiento (no tiene con qué
# aprender sus gustos) — "drop" descarta esas filas al evaluar, en vez de
# dejar que las NaN rompan el cálculo del error.
als = ALS(
    userCol="usuario", itemCol="pelicula", ratingCol="valoracion",
    rank=4, maxIter=10, regParam=0.1, seed=42,
    coldStartStrategy="drop",
)
modelo = als.fit(entrenamiento)

predicciones = modelo.transform(prueba)
evaluador = RegressionEvaluator(metricName="rmse", labelCol="valoracion", predictionCol="prediction")
rmse = evaluador.evaluate(predicciones)
print(f"RMSE en el conjunto de prueba: {rmse:.3f}")

# Recomendaciones: para cada usuario, las 2 películas con mejor
# predicción de valoración ENTRE LAS QUE NO HA VALORADO TODAVÍA — esto lo
# calcula ALS automáticamente (no hay que filtrar a mano las ya vistas).
recomendaciones = modelo.recommendForAllUsers(2)
print("Top-2 recomendaciones por usuario (id de película, valoración predicha):")
recomendaciones.orderBy("usuario").show(truncate=False)

# Comprobación concreta: el usuario 0 (fan de ciencia ficción, nunca vio
# "Interstellar") debería tener esa película entre sus recomendaciones,
# con una valoración predicha alta — es la comprobación de que el modelo
# ha aprendido el patrón real de gustos, no solo memorizado datos.
recs_usuario_0 = recomendaciones.filter(recomendaciones.usuario == 0).first().recommendations
print("Recomendaciones para el usuario 0:")
for r in recs_usuario_0:
    print(f"  {peliculas[r['pelicula']]}: {r['rating']:.2f}")
