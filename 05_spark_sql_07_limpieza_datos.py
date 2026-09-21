# Requiere: ninguno (Spark local, sin servicios docker).
#
# Limpieza de datos con Spark SQL/DataFrame: en la práctica, los datos que
# llegan de un sistema externo casi nunca están "limpios" — traen filas
# duplicadas, campos en blanco, valores de tipo incorrecto o fuera de rango.
# Este ejemplo usa un CSV creado a propósito con esos 4 problemas
# (files/datos_sucios.csv) y muestra cómo detectarlos y corregirlos uno a
# uno, en vez de descartar la fila entera a la primera.
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

spark = pyspark.sql.SparkSession.builder.appName("Limpieza de datos").getOrCreate()

# Leemos el CSV tal cual, SIN inferSchema: si dejáramos que Spark adivinara
# el tipo de "edad", al encontrar el valor "cuarenta" (texto) en una columna
# que en el resto de filas parece numérica, Spark la infiere entera como
# "string" para toda la columna — perderíamos de un plumazo la información
# de qué valores son números válidos y cuáles no. Es mejor leer todo como
# texto y decidir nosotros, columna a columna, cómo convertir cada una.
df = spark.read.option("header", "true").csv("files/datos_sucios.csv")
print("--- Datos tal cual llegan ---")
df.show()
df.printSchema()

# ---------------------------------------------------------------------
# 1) FILAS DUPLICADAS
# ---------------------------------------------------------------------
# files/datos_sucios.csv tiene la fila "4,Luis,35,España" repetida dos
# veces de forma EXACTA (mismo id, mismo valor en todas las columnas) —
# el caso más simple de duplicado, típico de una carga que se ha hecho dos
# veces por error. dropDuplicates() sin argumentos compara TODAS las
# columnas y se queda con una sola copia de cada combinación distinta.
duplicados = df.groupBy(df.columns).count().filter(F.col("count") > 1)
print("--- Filas que aparecen más de una vez ---")
duplicados.show()

df_sin_duplicados = df.dropDuplicates()
print(f"Filas antes: {df.count()}, filas después de quitar duplicados exactos: {df_sin_duplicados.count()}")

# ---------------------------------------------------------------------
# 2) VALORES EN BLANCO / NULOS
# ---------------------------------------------------------------------
# En un CSV, un campo vacío se lee como cadena vacía "" (no como NULL de
# SQL) — así que antes de poder usar isNull()/isNotNull() o na.fill()
# tenemos que convertir explícitamente las cadenas vacías (y las que solo
# tienen espacios) en NULL de verdad. Sin este paso, isNull() no
# detectaría el nombre en blanco de la fila con id=5.
for columna in df_sin_duplicados.columns:
    df_sin_duplicados = df_sin_duplicados.withColumn(
        columna,
        F.when(F.trim(F.col(columna)) == "", None).otherwise(F.col(columna))
    )

# Contar cuántos NULL hay en cada columna es el primer diagnóstico antes de
# decidir qué hacer con ellos (¿se puede rellenar con un valor por
# defecto? ¿hay que descartar la fila?).
print("--- Número de valores en blanco/nulos por columna ---")
df_sin_duplicados.select(
    [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_sin_duplicados.columns]
).show()

# Dos estrategias distintas según la columna:
# - "nombre" en blanco (fila id=5): no tiene sentido inventar un nombre,
#   así que rellenamos con un valor que dice claramente "esto faltaba" en
#   vez de dejarlo NULL (un NULL se puede colar sin más en un informe).
# - "edad" en blanco (fila id=6): si más adelante calculamos una media de
#   edad, es mejor dejarlo NULL (Spark ignora los NULL en avg()) que
#   inventar un número que distorsionaría el cálculo.
df_rellenado = df_sin_duplicados.na.fill({"nombre": "DESCONOCIDO"})

# ---------------------------------------------------------------------
# 3) TIPO DE DATO INCORRECTO
# ---------------------------------------------------------------------
# La fila id=3 tiene "cuarenta" en la columna "edad": un cast() normal
# (.cast(IntegerType())) NO lanza ningún error en ese caso, simplemente
# convierte el valor en NULL en silencio — fácil de confundir con un campo
# que ya venía en blanco. try_cast (función de SQL, sin equivalente directo
# en pyspark.sql.functions, así que se usa vía F.expr) hace exactamente lo
# mismo en cuanto al resultado (también da NULL si no puede convertir),
# pero aquí lo usamos junto a una comparación para PODER DISTINGUIR entre
# "el valor original ya era NULL" y "el valor original no se pudo
# convertir" — ese matiz es el que de verdad importa para depurar el
# origen de los datos.
df_tipos = df_rellenado.withColumn("edad_numerica", F.expr("try_cast(edad as int)"))

valores_no_convertibles = df_tipos.filter(
    df_tipos.edad.isNotNull() & df_tipos.edad_numerica.isNull()
)
print("--- Valores de 'edad' que no se pueden convertir a número ---")
valores_no_convertibles.select("id", "nombre", "edad").show()

# ---------------------------------------------------------------------
# 4) VALORES FUERA DE RANGO (dato del tipo correcto, pero imposible)
# ---------------------------------------------------------------------
# Las filas id=7 (edad -5) e id=8 (edad 150) ya son números válidos tras el
# cast, así que el paso anterior no las detecta — hace falta una regla de
# negocio explícita ("una edad de persona está entre 0 y 120") para
# encontrarlas. Este tipo de fallo (dato bien formado pero sin sentido) es
# el más fácil de pasar por alto si solo se valida el tipo de dato.
rango_valido = (F.col("edad_numerica") >= 0) & (F.col("edad_numerica") <= 120)
fuera_de_rango = df_tipos.filter(F.col("edad_numerica").isNotNull() & ~rango_valido)
print("--- Edades fuera de rango razonable (< 0 o > 120) ---")
fuera_de_rango.select("id", "nombre", "edad_numerica").show()

# ---------------------------------------------------------------------
# Dataset final limpio: combinamos las 4 correcciones anteriores en una
# sola pasada. Descartamos (poniendo a NULL) tanto los valores que no se
# pudieron convertir como los que están fuera de rango — así la columna
# final "edad" solo contiene números creíbles o NULL, nunca basura.
# ---------------------------------------------------------------------
df_limpio = df_tipos.withColumn(
    "edad", F.when(rango_valido, F.col("edad_numerica")).otherwise(None)
).drop("edad_numerica")

# También normalizamos texto: quitamos espacios sobrantes al principio/
# final (la fila id=9 llega como " Pedro ") y unificamos mayúsculas en
# "pais" (la fila id=10 llega como "ESPAÑA" en vez de "España") — sin esto,
# un groupBy("pais") contaría "España" y "ESPAÑA" como países distintos.
df_limpio = df_limpio.withColumn("nombre", F.trim(F.col("nombre")))
df_limpio = df_limpio.withColumn("pais", F.initcap(F.lower(F.col("pais"))))

print("--- Dataset limpio ---")
df_limpio.orderBy("id").show()
df_limpio.printSchema()
