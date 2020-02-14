from pyspark import SparkContext
sc =SparkContext()
# cargamos los datos de iris
from sklearn import datasets
# esto es un dataset que cargamos desde sklearn
iris = datasets.load_iris()
print("Características: "+str(iris.data))
print("Nombre de Características: " + str(iris.feature_names))
print("Etiquetas: " + str(iris.target))
print("Nombres de etiquetas: " + str(iris.target_names))

# invocamos pandas para crear un array de datos y un DF
# un DF de pandas XD
import pandas as pd
# iris.data son los datos de las características
# las medidas de las flores
# feature_name son los nombres de cada medida
iris_df = pd.DataFrame(iris.data, columns = iris.feature_names)

# imprimimos los primeros registros del DF, las primeras rows
print(iris_df.head())
print(iris.target)
iris_df['target'] = iris.target
print(iris_df)
#print(iris_df.show())

"""
import matplotlib.pyplot as plt   #Load the pyplot visualization library
iris_df['sepal length (cm)'].hist(bins=90)
plt.show()

data = iris.data
target = iris.target
# Resize the figure for better viewing
plt.figure(figsize=(12,5))

# First subplot
plt.subplot(131)

# Visualize the first two columns of data:
plt.scatter(data[:,0], data[:,1], c=target)
# Second subplot
plt.subplot(132)

# Visualize the last two columns of data:
plt.scatter(data[:,0], data[:,2], c=target)
# Second subplot
plt.subplot(133)

# Visualize the last two columns of data:
plt.scatter(data[:,0], data[:,3], c=target)
plt.show()

"""
#Haciendo transformaciones
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# esto es una vectorización
# es meter todas las características en un sólo campo de tipo array
# así funciona más rápido
#Creando campos de entrada y salida
vector_assembler = VectorAssembler(
    inputCols=["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"],
    outputCol="features")

from pyspark.sql import SparkSession
sparkSession =SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
#creado el DF de Spark
df = sparkSession.createDataFrame(iris_df)
#aplicando transformación
df_temp = vector_assembler.transform(df)
#mostrando 3
df_temp.show(3)
#Quitamos las columnas no interesantes
df = df_temp.drop("sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)")
df.show(3)
#Dividimos los datos en entrenamiento y pruebas
(trainingData, testData) = df.randomSplit([0.7, 0.3])
trainingData.show(3)
testData.show(3)
# en este caso escogemos el algoritmo de clasificación de árboles de decision
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#Creamos el modelo con un clasificador de árboles de decisión
# Escogemos un algoritmo, en este caso DecisionTreeClassifier
# pero podría ser otro por ejemplo regresión linear
# target son las etiquetas, features son las características
dt = DecisionTreeClassifier(labelCol="target", featuresCol="features", maxDepth=8)
# creamos o entrenamos el modelo
model = dt.fit(trainingData)
# probamos el modelo a ver si es bueno o no
predictions = model.transform(testData)
# mostramos las 5 primeras predicciones, como ejemplo
predictions.select("prediction", "target").show(5)
# lo importante es que usamos un evaluador de esas predicciones
evaluator = MulticlassClassificationEvaluator(
    labelCol="target",
    predictionCol="prediction",
    metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
error_precision=1.0 - accuracy
print("Test Acierto = " + str(accuracy))
print("Test Error = " + str(error_precision))

#guardar modelo
#dt.save("dt_model.model")
#cargar modelo
#model2 = DecisionTreeClassifier.load("dt_model.model")
