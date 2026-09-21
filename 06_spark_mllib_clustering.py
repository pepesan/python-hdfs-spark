# Requiere: ninguno (Spark local, sin servicios docker). Abre ventanas de
# matplotlib (plt.show()); en un entorno sin pantalla usar MPLBACKEND=Agg.
#
# Clusterización (aprendizaje NO supervisado) — a diferencia de
# `06_spark_mllib.py` (clasificación) y `06_spark_mllib_boston.py`
# (regresión), que son aprendizaje SUPERVISADO (el dataset trae la
# respuesta correcta — la especie de flor, el precio de la casa — y el
# modelo aprende a predecirla), aquí NO se le da al algoritmo ninguna
# etiqueta: K-Means solo ve las medidas de cada flor y tiene que agrupar
# por su cuenta las que se parecen entre sí. Es el tipo de problema que se
# usa cuando no se sabe de antemano en qué grupos deberían caer los datos
# (segmentación de clientes, agrupar documentos por tema...).
#
# Mismo dataset que 06_spark_mllib.py (Iris) para poder comparar: aquí se
# esconden las etiquetas reales durante el entrenamiento, y solo se usan
# al final para comprobar qué tal ha acertado K-Means "a ciegas".
from sklearn import datasets
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder.appName('clustering-iris').getOrCreate()

iris = datasets.load_iris()
iris_df = pd.DataFrame(iris.data, columns=iris.feature_names)
# guardamos la especie real APARTE — K-Means no la va a ver durante el
# entrenamiento, solo se usa al final para comparar
iris_df['especie_real'] = iris.target

df = spark.createDataFrame(iris_df)

# Igual que en clasificación/regresión, hay que juntar las columnas
# numéricas en una sola columna "features" (un vector) antes de
# entrenar cualquier algoritmo de MLlib — es el formato que espera toda
# la librería, no una particularidad de K-Means.
vector_assembler = VectorAssembler(
    inputCols=iris.feature_names,
    outputCol="features")
df_features = vector_assembler.transform(df)

# k=3: le decimos a K-Means CUÁNTOS grupos buscar (a diferencia de la
# clasificación, donde el número de clases lo trae el propio dataset).
# Aquí "hacemos trampa" un poco porque ya sabemos que hay 3 especies —
# en un caso real sin etiquetas, elegir k es un problema en sí mismo
# (el "método del codo": probar varios valores de k y quedarse con el
# que mejor equilibrio da entre pocos grupos y grupos compactos).
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=3, seed=42)
modelo = kmeans.fit(df_features)

resultado = modelo.transform(df_features)
resultado.select("especie_real", "cluster").show(15)

# silhouette score: mide qué tan bien separados están los clusters (entre
# -1 y 1; cerca de 1 es bueno, cerca de 0 significa clusters solapados,
# negativo significa que muchos puntos probablemente están en el cluster
# equivocado) — es la métrica estándar para evaluar clustering CUANDO NO
# hay etiquetas reales con las que comparar (aquí sí las tenemos, pero se
# usa igualmente para mostrar cómo se evaluaría en el caso general).
evaluador = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
silhouette = evaluador.evaluate(resultado)
print(f"Silhouette score: {silhouette:.3f}")

# Como SÍ tenemos la especie real (algo que no pasaría en un caso real
# sin etiquetas), podemos comprobar qué tal ha ido de verdad: una tabla
# cruzada especie_real x cluster. Si K-Means ha hecho bien su trabajo,
# cada especie debería caer mayoritariamente en un único cluster (aunque
# el NÚMERO de cada cluster es arbitrario — el cluster "0" de K-Means no
# tiene por qué coincidir con la especie "0" del dataset).
print("Tabla cruzada especie real vs. cluster asignado:")
resultado.groupBy("especie_real", "cluster").count().orderBy("especie_real", "cluster").show()

# Centros de los 3 clusters encontrados — el "prototipo" de cada grupo,
# en el mismo espacio de 4 dimensiones (las 4 medidas de la flor)
print("Centros de los clusters:")
for i, centro in enumerate(modelo.clusterCenters()):
    print(f"  cluster {i}: {centro}")
