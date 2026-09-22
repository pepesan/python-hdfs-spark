# Requiere: ninguno (Spark local, sin servicios docker). Abre ventanas de
# matplotlib (plt.show()); en un entorno sin pantalla usar MPLBACKEND=Agg.
#
# Clasificación (aprendizaje SUPERVISADO) — el dataset trae la respuesta
# correcta para cada fila (la especie de cada flor, "target") y el modelo
# aprende a predecirla a partir de sus características (medidas del
# sépalo y del pétalo). Dataset clásico Iris de scikit-learn: 3 especies,
# 4 medidas por flor. Ver `06_spark_mllib_clustering.py` para el mismo
# dataset resuelto SIN usar las etiquetas (aprendizaje no supervisado).
from sklearn import datasets

iris = datasets.load_iris()
print("Características: " + str(iris.data))
print("Nombre de características: " + str(iris.feature_names))
print("Etiquetas: " + str(iris.target))
print("Nombres de etiquetas: " + str(iris.target_names))

# pandas es solo para explorar los datos cómodamente antes de pasarlos a
# Spark — el DataFrame "de verdad" (el que entrena el modelo) es el de
# Spark, creado más abajo a partir de este.
import pandas as pd

iris_df = pd.DataFrame(iris.data, columns=iris.feature_names)
iris_df['target'] = iris.target
print(iris_df.head())

# Un histograma por medida, para ver cómo se distribuye cada una antes de
# entrenar nada — puramente exploratorio, no afecta al modelo.
import matplotlib.pyplot as plt

for columna in iris.feature_names:
    iris_df[columna].hist(bins=90)
    plt.title(columna)
    plt.show()

# Dispersión de la primera medida (longitud del sépalo) contra cada una
# de las otras tres, coloreada por especie (c=target) — sirve para ver a
# ojo qué combinaciones de medidas separan mejor las 3 especies.
plt.figure(figsize=(12, 5))
for i, columna in enumerate(iris.feature_names[1:], start=1):
    plt.subplot(1, 3, i)
    plt.scatter(iris.data[:, 0], iris.data[:, i], c=iris.target)
    plt.xlabel(iris.feature_names[0])
    plt.ylabel(columna)
plt.show()

# MLlib exige juntar todas las columnas de entrada en una sola columna
# "features" (un vector) antes de entrenar cualquier algoritmo — es el
# formato que espera toda la librería, no una particularidad de este
# clasificador en concreto.
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('clasificacion-iris').getOrCreate()
df = spark.createDataFrame(iris_df)

vector_assembler = VectorAssembler(
    inputCols=iris.feature_names,
    outputCol="features")
df = vector_assembler.transform(df).select("features", "target")
df.show(3)

# Split train/test: se entrena SOLO con trainingData y se evalúa SOLO con
# testData, que el modelo no ha visto durante el entrenamiento — es la
# única forma honesta de saber si ha aprendido el patrón general o si
# solo ha memorizado los datos de entrenamiento.
trainingData, testData = df.randomSplit([0.7, 0.3])

# Árbol de decisión: parte el espacio de características con preguntas
# tipo "¿petal length < 2.5?" hasta separar bien las 3 especies —
# elegible entre otros muchos algoritmos de clasificación de MLlib
# (regresión logística, random forest...); este es de los más fáciles de
# interpretar (se puede dibujar el árbol de decisiones resultante).
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

dt = DecisionTreeClassifier(labelCol="target", featuresCol="features", maxDepth=8)
modelo = dt.fit(trainingData)
predicciones = modelo.transform(testData)
predicciones.select("prediction", "target").show(5)

# El accuracy (proporción de aciertos) es la métrica de evaluación
# estándar en clasificación con clases equilibradas (aquí las 3 especies
# tienen 50 flores cada una) — con clases muy desequilibradas haría falta
# otra métrica (precision/recall/F1), porque un modelo que siempre
# predijera la clase mayoritaria tendría un accuracy engañosamente alto.
evaluator = MulticlassClassificationEvaluator(
    labelCol="target",
    predictionCol="prediction",
    metricName="accuracy")
accuracy = evaluator.evaluate(predicciones)
error = 1.0 - accuracy
print("Test Acierto = " + str(accuracy))
print("Test Error = " + str(error))

# Guardar/cargar el modelo entrenado (para reutilizarlo sin reentrenar):
# dt.save("dt_model.model")
# modelo_cargado = DecisionTreeClassifier.load("dt_model.model")
