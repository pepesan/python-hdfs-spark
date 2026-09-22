# Requiere: ninguno (Spark local, sin servicios docker).
#
# Clasificación (aprendizaje SUPERVISADO) — mismo tipo de problema que
# `06_spark_mllib.py` (Iris), pero aquí con un caso binario real:
# clasificar tumores de mama como malignos o benignos ("target") a partir
# de 30 medidas del tumor (radio, textura, perímetro...). Dataset clásico
# "Breast Cancer Wisconsin" de scikit-learn.
#
# A diferencia de `06_spark_mllib.py`, aquí además se barre un rango de
# hiperparámetros a mano (una búsqueda en rejilla muy simple) para ver
# cómo cambia la precisión del árbol de decisión según:
#   - random: qué proporción de datos se usa para entrenar (0.7/0.8/0.9)
#     frente a probar (el resto).
#   - maxDepth: la profundidad máxima del árbol — más profundidad capta
#     patrones más finos, pero también aumenta el riesgo de sobreajustar
#     (memorizar el ruido del conjunto de entrenamiento en vez de aprender
#     el patrón real).
from sklearn import datasets
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

cancer = datasets.load_breast_cancer()
cancer_df = pd.DataFrame(cancer.data, columns=cancer.feature_names)
cancer_df['target'] = cancer.target  # 0 = maligno, 1 = benigno

spark = SparkSession.builder.appName('clasificacion-cancer').getOrCreate()
df = spark.createDataFrame(cancer_df)

# MLlib exige juntar todas las columnas de entrada en una sola columna
# "features" (un vector) antes de entrenar cualquier algoritmo — aquí son
# las 30 medidas del tumor.
columnas_features = list(cancer.feature_names)
vector_assembler = VectorAssembler(inputCols=columnas_features, outputCol="features")
df = vector_assembler.transform(df).select("features", "target")
df.show(3)


def entrena_y_evalua(proporcion_train: float, max_depth: int) -> float:
    """Entrena un árbol de decisión con un split y una profundidad
    concretos, y devuelve el accuracy sobre el conjunto de prueba."""
    print(f"Depth: {max_depth}, Random: {proporcion_train}")
    train_df, test_df = df.randomSplit([proporcion_train, 1 - proporcion_train], seed=3)

    dt = DecisionTreeClassifier(
        labelCol="target", featuresCol="features", seed=3, maxDepth=max_depth,
    )
    modelo = dt.fit(train_df)
    predicciones = modelo.transform(test_df)

    evaluador = MulticlassClassificationEvaluator(
        labelCol="target", predictionCol="prediction", metricName="accuracy",
    )
    accuracy = evaluador.evaluate(predicciones)
    print("Test Acierto = " + str(accuracy))
    return accuracy


# Búsqueda en rejilla simple: prueba todas las combinaciones de las dos
# listas de abajo y se queda con la de mejor accuracy — el equivalente
# manual de lo que haría `pyspark.ml.tuning.ParamGridBuilder` con más
# ceremonia (aquí se deja explícito el bucle para que se vea qué hace por
# debajo).
proporciones_train = [0.7, 0.8, 0.9]
profundidades = [3, 4, 5, 6, 7, 8, 9, 10, 11]

mejor_accuracy = 0.0
for proporcion in proporciones_train:
    for profundidad in profundidades:
        accuracy = entrena_y_evalua(proporcion, profundidad)
        if accuracy > mejor_accuracy:
            mejor_accuracy = accuracy
            print(
                f"Se ha encontrado un modelo mejor: maxDepth={profundidad}, "
                f"random={proporcion}, accuracy={mejor_accuracy}"
            )
