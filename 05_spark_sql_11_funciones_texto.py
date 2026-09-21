# Requiere: ninguno (Spark local, sin servicios docker).
#
# Funciones de texto — sobre files/addresses.csv (sin cabecera, con
# espacios sueltos delante de "estado"/"cp" y comas/comillas dentro de
# algunos campos, tal cual vendría de un export real y descuidado).
import pyspark
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo funciones de texto").getOrCreate()

# option("escape", "\""): el CSV usa el escapado estándar RFC 4180 para
# comillas dentro de un campo (una comilla se duplica: "" significa un
# único " literal, y puede haber comas dentro de un campo entrecomillado
# sin que cuenten como separador — ver la fila 6, con una coma dentro del
# nombre). El valor por defecto de Spark para "escape" es "\" (barra
# invertida), NO comillas dobles — sin fijar esta opción, Spark corta mal
# los campos que usan comillas dobles como escape (los deja con las
# comillas literales sin quitar, o separa por comas que en realidad
# estaban protegidas dentro de un campo).
personas = spark.read.option("escape", "\"").csv("files/addresses.csv").toDF(
    "nombre", "apellido", "direccion", "ciudad", "estado", "cp"
)
personas.show(truncate=False)

# trim() quita espacios sobrantes al principio/final — imprescindible
# aquí: "estado" y "cp" traen un espacio delante (" NJ", " 08075") porque
# el CSV original los escribió así después de la coma. Sin este paso,
# comparar o agrupar por "estado" trataría " NJ" y "NJ" como valores
# distintos.
personas = personas.withColumn("estado", F.trim("estado")).withColumn("cp", F.trim("cp"))

# upper()/lower()/initcap(): cambiar mayúsculas/minúsculas.
# concat_ws(separador, columnas...): une varias columnas de texto con un
# separador — a diferencia de concat() (sin separador), y sin liarse con
# NULLs (concat_ws ignora los NULL, concat() los propaga a toda la fila).
con_nombre_completo = personas.withColumn(
    "nombre_completo", F.concat_ws(" ", F.initcap("nombre"), F.upper("apellido"))
)
print("--- Nombre completo (nombre en mayúscula inicial + apellido en mayúsculas) ---")
con_nombre_completo.select("nombre", "apellido", "nombre_completo").show(truncate=False)

# split(texto, patrón): separa un texto en un array de trozos según un
# patrón (aquí, un espacio) — el primer trozo de la dirección suele ser
# el número de la calle. getItem(0) coge el primer elemento del array.
con_numero = personas.withColumn(
    "primer_trozo_direccion", F.split(personas.direccion, " ").getItem(0)
)
print("--- Primer \"trozo\" de la dirección (normalmente el número) ---")
con_numero.select("direccion", "primer_trozo_direccion").show(truncate=False)

# regexp_extract(texto, patrón, grupo): saca la parte de un texto que
# coincide con un grupo de una expresión regular — aquí, los primeros 5
# dígitos de "cp" (por si algún código postal trajera algo más pegado).
con_cp_limpio = personas.withColumn(
    "cp_5_digitos", F.regexp_extract(personas.cp, r"(\d{5})", 1)
)
print("--- Código postal, solo los 5 primeros dígitos ---")
con_cp_limpio.select("cp", "cp_5_digitos").show()

# regexp_replace(texto, patrón, reemplazo): sustituye todas las
# coincidencias de un patrón — aquí, quita las comillas dobles que trae
# el nombre de la fila 3 ('John "Da Man"' -> 'John Da Man').
sin_comillas = personas.withColumn(
    "nombre_sin_comillas", F.regexp_replace(personas.nombre, '"', "")
)
print("--- Nombre sin comillas dobles ---")
sin_comillas.select("nombre", "nombre_sin_comillas").show(truncate=False)

# substring(texto, inicio, longitud): un trozo de texto por posición (el
# índice empieza en 1, no en 0, a diferencia de Python) — aquí, para
# sacar solo los 2 primeros caracteres del estado como "abreviatura",
# aunque ya vengan abreviados en este dataset (sirve igual como ejemplo).
con_abreviatura = personas.withColumn(
    "estado_2_letras", F.substring(personas.estado, 1, 2)
)
print("--- Primeros 2 caracteres del estado ---")
con_abreviatura.select("estado", "estado_2_letras").show()
