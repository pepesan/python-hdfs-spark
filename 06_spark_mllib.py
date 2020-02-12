from pyspark import SparkContext
sc =SparkContext()

from sklearn import datasets
iris = datasets.load_iris()
#print(iris)
import pandas as pd
iris_df = pd.DataFrame(iris.data, columns = iris.feature_names)
print(iris_df.head())
print(iris.target)
iris_df['target'] = iris.target
print(iris_df)
#print(iris_df.show())
import matplotlib.pyplot as plt   #Load the pyplot visualization library
iris_df['sepal length (cm)'].hist(bins=30)
plt.show()

data = iris.data
target = iris.target
# Resize the figure for better viewing
plt.figure(figsize=(12,5))

# First subplot
plt.subplot(121)

# Visualize the first two columns of data:
plt.scatter(data[:,0], data[:,1], c=target)
# Second subplot
plt.subplot(122)

# Visualize the last two columns of data:
plt.scatter(data[:,2], data[:,3], c=target)
plt.show()


#Haciendo transformaciones
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

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

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#Creamos el modelo con un clasificador de árboles de decisión
dt = DecisionTreeClassifier(labelCol="target", featuresCol="features")
model = dt.fit(trainingData)
predictions = model.transform(testData)
predictions.select("prediction", "target").show(5)

evaluator = MulticlassClassificationEvaluator(
    labelCol="target",
    predictionCol="prediction",
    metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
precision=1.0 - accuracy
print("Test Acierto = " + str(accuracy))
print("Test Error = " + str(precision))
