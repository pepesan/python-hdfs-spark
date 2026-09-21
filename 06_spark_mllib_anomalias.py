# Requiere: ninguno (Spark local, sin servicios docker).
#
# Detección de anomalías — encontrar los pocos datos que NO se parecen a
# la mayoría (fraude en tarjetas, un sensor que empieza a fallar, tráfico
# de red raro...). A diferencia de clasificación/regresión, normalmente no
# hay ejemplos etiquetados de "esto es una anomalía" para entrenar con
# ellos (las anomalías son raras casi por definición) — así que se suele
# abordar como un problema NO supervisado, apoyándose en algo que SÍ se
# puede medir sin etiquetas: qué tan "raro" es un punto respecto al resto.
#
# MLlib no trae un algoritmo específico de detección de anomalías (como sí
# tiene para clasificación/regresión/clustering) — aquí se construye uno
# con una técnica real y muy usada en la práctica: calcular dónde está
# "el centro de la normalidad" (la media de los datos) y usar la
# DISTANCIA de cada punto a ese centro como "puntuación de rareza" —
# cuanto más lejos, más sospechoso. Con varios grupos normales distintos
# (no uno solo) se haría lo mismo pero con K-Means de verdad (k>=2,
# ver `06_spark_mllib_clustering.py`) y la distancia al centro de SU
# cluster asignado, en vez de a un único centro global.
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('anomalias').getOrCreate()

# Datos sintéticos con un problema real detrás: lecturas de dos sensores
# de una máquina (temperatura, vibración) — la inmensa mayoría son
# lecturas normales agrupadas en torno a un punto de funcionamiento
# habitual, y unas pocas son anomalías claras (la máquina calentándose de
# más, o vibrando muchísimo). Semilla fija para que el ejemplo sea
# reproducible.
rng = np.random.default_rng(42)
normales = rng.normal(loc=[50.0, 2.0], scale=[3.0, 0.3], size=(200, 2))
anomalias_reales = np.array([
    [95.0, 2.1],   # temperatura disparada
    [51.0, 9.0],   # vibración disparada
    [88.0, 8.5],   # las dos cosas a la vez
])
datos = np.vstack([normales, anomalias_reales])
es_anomalia_real = np.array([False] * len(normales) + [True] * len(anomalias_reales))

pdf = pd.DataFrame(datos, columns=["temperatura", "vibracion"])
pdf["es_anomalia_real"] = es_anomalia_real
df = spark.createDataFrame(pdf)

# "Centro de la normalidad": la media de cada sensor sobre TODOS los
# datos. Con solo 3 anomalías entre 203 puntos, la media apenas se ve
# afectada por ellas — sigue representando bien el comportamiento normal
# (con una proporción de anomalías mucho más alta, habría que calcular
# la media solo sobre un conjunto de datos ya sabido normal, o usar una
# medida más robusta como la mediana).
centro = df.select(F.mean("temperatura").alias("t"), F.mean("vibracion").alias("v")).first()
print(f"Centro de \"funcionamiento normal\": temperatura={centro['t']:.2f}, vibracion={centro['v']:.2f}")

# Distancia euclídea de cada punto al centro — cuanto mayor, más
# "anómalo".
distancia = F.sqrt(
    F.pow(df.temperatura - centro["t"], 2) +
    F.pow(df.vibracion - centro["v"], 2)
)
resultado = df.withColumn("distancia_al_centro", distancia)

# Umbral: media + 3 desviaciones típicas de la distancia — una regla
# estadística clásica ("regla de las 3 sigma"): en una distribución
# normal, menos del 0.3% de los puntos caen a más de 3 desviaciones de
# la media, así que un punto tan lejos es sospechoso de no pertenecer a
# esa distribución.
stats = resultado.select(
    F.mean("distancia_al_centro").alias("media"),
    F.stddev("distancia_al_centro").alias("desviacion"),
).first()
umbral = stats["media"] + 3 * stats["desviacion"]
print(f"Umbral de distancia (media + 3 desviaciones): {umbral:.2f}")

resultado = resultado.withColumn("es_anomalia_detectada", resultado.distancia_al_centro > umbral)

print("Puntos marcados como anomalía:")
resultado.filter(resultado.es_anomalia_detectada) \
    .select("temperatura", "vibracion", "distancia_al_centro", "es_anomalia_real") \
    .orderBy(F.desc("distancia_al_centro")).show()

# Comprobación: cuántas anomalías reales se han detectado, y cuántos
# falsos positivos (puntos normales marcados como anómalos sin serlo)
detectadas_correctas = resultado.filter(
    resultado.es_anomalia_detectada & resultado.es_anomalia_real
).count()
falsos_positivos = resultado.filter(
    resultado.es_anomalia_detectada & ~resultado.es_anomalia_real
).count()
print(f"Anomalías reales detectadas: {detectadas_correctas} de {len(anomalias_reales)}")
print(f"Falsos positivos: {falsos_positivos}")

# Nota real (no un fallo del ejemplo): el punto [51.0, 9.0] (vibración muy
# alta, temperatura normal) se queda SIN detectar con este umbral — la
# distancia euclídea sin más está dominada por la escala de "temperatura"
# (varía en decenas) frente a la de "vibracion" (varía en unidades), así
# que una desviación grande en vibración pesa poco en la distancia total.
# La solución habitual es ESTANDARIZAR las columnas antes de calcular
# distancias (restar la media y dividir por la desviación típica de cada
# una, con `pyspark.ml.feature.StandardScaler`) para que todas pesen por
# igual — no se hace aquí para mantener el ejemplo simple, pero es la
# primera mejora a probar si esto fuera un caso real.
