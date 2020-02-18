from pyspark import SparkContext
sc =SparkContext()
# cargamos los datos de cancerDataSet
from sklearn import datasets
# esto es un dataset que cargamos desde sklearn
cancerDataSet = datasets.load_breast_cancer()
#print("Características: " + str(cancerDataSet.data))
#print("Nombre de Características: " + str(cancerDataSet.feature_names))
#print("Etiquetas: " + str(cancerDataSet.target))
#print("Nombres de etiquetas: " + str(cancerDataSet.target_names))
#print("Descripción: "+cancerDataSet.DESCR)

# invocamos pandas para crear un array de datos y un DF
# un DF de pandas XD
import pandas as pd
# cancerDataSet.data son los datos de las características
# las medidas de las flores
# feature_name son los nombres de cada medida
cancer_df = pd.DataFrame(
    cancerDataSet.data, columns = cancerDataSet.feature_names)

# imprimimos los primeros registros del DF, las primeras rows
#print(cancer_df.head())
#print(cancerDataSet.target)
cancer_df['target'] = cancerDataSet.target
#print(cancer_df)
#print(cancer_df.show())
"""
import matplotlib.pyplot as plt   #Load the pyplot visualization library
cancer_df['mean radius'].hist(bins=90)
plt.show()

data = cancerDataSet.data
target = cancerDataSet.target
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
    inputCols=[
'mean radius' ,'mean texture' ,'mean perimeter' ,'mean area',
 'mean smoothness', 'mean compactness', 'mean concavity',
 'mean concave points', 'mean symmetry' ,'mean fractal dimension',
 'radius error', 'texture error', 'perimeter error', 'area error',
 'smoothness error' ,'compactness error', 'concavity error',
 'concave points error' ,'symmetry error' ,'fractal dimension error',
 'worst radius', 'worst texture' ,'worst perimeter', 'worst area',
 'worst smoothness' ,'worst compactness', 'worst concavity',
 'worst concave points' ,'worst symmetry' ,'worst fractal dimension'
    ],
    outputCol="features")

from pyspark.sql import SparkSession
sparkSession =SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
#creado el DF de Spark
df = sparkSession.createDataFrame(cancer_df)
#aplicando transformación
df_temp = vector_assembler.transform(df)
#mostrando 3
#df_temp.show(3)
#Quitamos las columnas no interesantes
df = df_temp.drop('mean radius' ,'mean texture' ,'mean perimeter' ,'mean area',
 'mean smoothness', 'mean compactness', 'mean concavity',
 'mean concave points', 'mean symmetry' ,'mean fractal dimension',
 'radius error', 'texture error', 'perimeter error', 'area error',
 'smoothness error' ,'compactness error', 'concavity error',
 'concave points error' ,'symmetry error' ,'fractal dimension error',
 'worst radius', 'worst texture' ,'worst perimeter', 'worst area',
 'worst smoothness' ,'worst compactness', 'worst concavity',
 'worst concave points' ,'worst symmetry' ,'worst fractal dimension')
#df.show(3)

def prueba(random,maxDepth):
    print("Depth: " + str(maxDepth) + ", Random: " + str(random))
    #Dividimos los datos en entrenamiento y pruebas
    (trainingData, testData) = df.randomSplit([random, 1-random], seed=3)
    #trainingData.show(3)
    #testData.show(3)
    # en este caso escogemos el algoritmo de clasificación de árboles de decision
    from pyspark.ml.classification import DecisionTreeClassifier
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    #Creamos el modelo con un clasificador de árboles de decisión
    # Escogemos un algoritmo, en este caso DecisionTreeClassifier
    # pero podría ser otro por ejemplo regresión linear
    # target son las etiquetas, features son las características
    dt = DecisionTreeClassifier(
        labelCol="target",
        featuresCol="features", seed=3, maxDepth=maxDepth)
    # creamos o entrenamos el modelo
    model = dt.fit(trainingData)
    # probamos el modelo a ver si es bueno o no
    predictions = model.transform(testData)
    # mostramos las 5 primeras predicciones, como ejemplo
    #predictions.select("prediction", "target").show(5)
    # lo importante es que usamos un evaluador de esas predicciones
    evaluator = MulticlassClassificationEvaluator(
        labelCol="target",
        predictionCol="prediction",
        metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    error_precision=1.0 - accuracy
    print("Test Acierto = " + str(accuracy))
    #print("Test Error = " + str(error_precision))
    return accuracy
    #guardar modelo
    #dt.save("dt_model.model")
    #cargar modelo
    #model2 = DecisionTreeClassifier.load("dt_model.model")

randomArray = [0.7, 0.8, 0.9]
maxDepthArray = [3, 4, 5, 6, 7, 8, 9, 10, 11]
acierto = 0
for random in randomArray:
    for depth in maxDepthArray:
        aciertoD = prueba(random,depth)
        if (aciertoD>acierto):
            acierto = aciertoD
            print("Se ha encontrado un modelo mejor maxDepth=" + str(acierto) + " y random: " + str(random))