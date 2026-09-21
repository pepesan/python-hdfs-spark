# Requiere: ninguno (Spark local, sin servicios docker).
#
# Datos anidados — arrays y structs. Muy habitual al leer JSON (a
# diferencia de un CSV, que solo tiene columnas planas): aquí cada persona
# trae una lista de teléfonos (array) y una dirección con varios campos
# dentro (struct), en vez de columnas sueltas "telefono1"/"telefono2" o
# "direccion_calle"/"direccion_ciudad".
import pyspark
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.appName("Ejemplo datos anidados").getOrCreate()

personas = spark.read.option("multiline", "true").json("files/personas_anidado.json")

# El schema deja ver la estructura anidada: "telefonos" es un
# array<string>, "direccion" es un struct con sus propios campos dentro
# (cada uno con su propio tipo) — no columnas planas.
personas.printSchema()
personas.show(truncate=False)

# Acceso a un campo de un struct: con notación de punto (df.direccion.ciudad)
# o con df["direccion"]["ciudad"] — igual que acceder a un atributo
# anidado en Python, no hace falta "aplanar" el struct primero.
print("--- Acceso directo a un campo del struct \"direccion\" ---")
personas.select("nombre", personas.direccion.ciudad, personas.direccion.cp).show()

# explode(array): convierte un array en VARIAS filas, una por elemento —
# la persona con 3 teléfonos (Luis) genera 3 filas, cada una con un
# teléfono distinto; Pedro, con el array vacío, DESAPARECE del resultado
# (explode() de un array vacío no produce ninguna fila — para mantenerlo
# con un NULL habría que usar explode_outer() en su lugar).
print("--- explode: una fila por cada teléfono (Pedro desaparece, array vacío) ---")
personas.select("nombre", F.explode("telefonos").alias("telefono")).show()

# posexplode(array): como explode(), pero además da la posición (índice,
# empezando en 0) de cada elemento dentro del array original.
print("--- posexplode: con la posición del teléfono en la lista original ---")
personas.select("nombre", F.posexplode("telefonos").alias("posicion", "telefono")).show()

# explode_outer(array): como explode(), pero SÍ conserva las filas cuyo
# array está vacío o es NULL (con el valor explotado a NULL) — aquí Pedro
# vuelve a aparecer, con "telefono" a NULL.
print("--- explode_outer: Pedro reaparece, con telefono=NULL ---")
personas.select("nombre", F.explode_outer("telefonos").alias("telefono")).show()

# struct(): lo contrario de acceder a un campo — agrupa varias columnas
# planas en una sola columna anidada. Aquí se construye una columna nueva
# "resumen" combinando nombre y ciudad, como ejemplo de crear un struct
# desde cero (no viene de un JSON, se construye en el propio script).
con_resumen = personas.withColumn(
    "resumen", F.struct(personas.nombre, personas.direccion.ciudad.alias("ciudad"))
)
print("--- Columna nueva creada como struct (nombre + ciudad) ---")
con_resumen.select("resumen").show(truncate=False)

# size(array): cuántos elementos tiene el array — sin necesidad de
# explotarlo primero, útil para filtrar (p. ej. "personas sin teléfono").
print("--- Número de teléfonos por persona ---")
personas.select("nombre", F.size("telefonos").alias("num_telefonos")).show()
