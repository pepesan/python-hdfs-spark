# Requiere: ninguno (Spark local, sin servicios docker); usa files/boston.csv.
#
# Regresión (aprendizaje SUPERVISADO) — a diferencia de la clasificación
# (`06_spark_mllib.py`, `06_spark_mllib_cancer.py`), aquí lo que se predice
# no es una categoría sino un número real: el precio medio de una vivienda
# (columna "MV", en miles de dólares) a partir de sus características
# (criminalidad de la zona, número de habitaciones, distancia al centro...).
# Dataset clásico de Boston Housing.
#
# Se entrenan y comparan 3 algoritmos de regresión distintos sobre los
# mismos datos, de más simple a más sofisticado:
#   - LinearRegression: ajusta una única combinación lineal de las
#     características — rápido y fácil de interpretar, pero solo capta
#     relaciones lineales.
#   - DecisionTreeRegressor: parte el espacio de características en
#     regiones (a base de preguntas tipo "¿RM > 6?") y predice la media de
#     cada región — capta relaciones no lineales, pero un único árbol
#     tiende a sobreajustar.
#   - GBTRegressor (Gradient-Boosted Trees): entrena muchos árboles
#     pequeños en secuencia, cada uno corrigiendo los errores del anterior
#     — normalmente el que mejor RMSE saca de los tres, a costa de ser el
#     más lento de entrenar.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('regresion-boston').getOrCreate()

house_df = spark.read.csv('files/boston.csv', header=True, inferSchema=True)
house_df.printSchema()
house_df.show(3)

# MLlib exige juntar todas las columnas de entrada en una sola columna
# "features" (un vector) — igual que en clasificación/clustering, es el
# formato que espera cualquier algoritmo de la librería, no algo propio de
# la regresión.
from pyspark.ml.feature import VectorAssembler
columnas_features = [
    'CRIM', 'ZN', 'INDUS', 'CHAS', 'NOX', 'RM', 'AGE',
    'DIS', 'RAD', 'TAX', 'PT', 'B', 'LSTAT',
]
vector_assembler = VectorAssembler(inputCols=columnas_features, outputCol='features')
vhouse_df = vector_assembler.transform(house_df).select('features', 'MV')
vhouse_df.show(3)

# seed fija para que el split train/test sea siempre el mismo entre
# ejecuciones (reproducibilidad) — ver la nota sobre randomSplit()/particiones
# en CLAUDE.md si esto se compara alguna vez entre local y el cluster docker.
train_df, test_df = vhouse_df.randomSplit([0.7, 0.3], seed=3)

# --- Modelo 1: regresión lineal ------------------------------------------
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

lr = LinearRegression(
    featuresCol='features', labelCol='MV',
    maxIter=10, regParam=0.3, elasticNetParam=0.8,
)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

# summary trae las métricas calculadas sobre el propio conjunto de
# entrenamiento (útil para ver si el modelo ha convergido bien), distinto
# de evaluar sobre test_df más abajo (lo que importa de verdad: cómo de
# bien generaliza a datos que no ha visto).
training_summary = lr_model.summary
print("RMSE (train): %f" % training_summary.rootMeanSquaredError)
print("r2 (train): %f" % training_summary.r2)

lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction", "MV", "features").show(5)

lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="MV", metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

# --- Modelo 2: árbol de decisión ------------------------------------------
from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor(featuresCol='features', labelCol='MV')
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)

dt_evaluator = RegressionEvaluator(labelCol="MV", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# --- Modelo 3: Gradient-Boosted Trees -------------------------------------
from pyspark.ml.regression import GBTRegressor

gbt = GBTRegressor(featuresCol='features', labelCol='MV', maxIter=10)
gbt_model = gbt.fit(train_df)
gbt_predictions = gbt_model.transform(test_df)
gbt_predictions.select('prediction', 'MV', 'features').show(5)

gbt_evaluator = RegressionEvaluator(labelCol="MV", predictionCol="prediction", metricName="rmse")
rmse = gbt_evaluator.evaluate(gbt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

print("Comparando los 3 RMSE de arriba: normalmente GBTRegressor < DecisionTreeRegressor "
      "< LinearRegression, aunque con un dataset tan pequeño (~150 filas de test) "
      "el orden exacto puede variar entre ejecuciones.")
