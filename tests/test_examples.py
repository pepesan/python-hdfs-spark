"""Validación automatizada de los scripts de ejemplo del proyecto.

Ejecuta cada script tal cual lo haría un usuario desde la raíz del repo
(mismo intérprete que crea `uv venv`), y comprueba:
- que termina sin traceback (exit code 0), y
- cuando es posible, que el resultado calculado por pyspark es el esperado
  (no solo que "no ha explotado").

Los que dependen del docker/compose.yaml (spark-master:7077, namenode:9870)
se saltan automáticamente si el entorno no está levantado.

Ejecutar con:
    source .venv/bin/activate
    pytest tests/ -v
"""

import pytest


# ---------------------------------------------------------------------------
# Scripts que solo necesitan Spark local (sin docker)
# ---------------------------------------------------------------------------

LOCAL_OK_SCRIPTS = [
    "02_hdfs.py",  # no-op: todo el cuerpo está comentado
    "03_pyspark_local.py",
    "03_pyspark_local_02_dataframe.py",
    "03_pyspark_local_03_pandas.py",
    "05_spark_sql.py",
    "05_spark_sql_02.py",
    "05_spark_sql_03_csv.py",
    "05_spark_sql_04_json.py",
    "05_spark_sql_05_columnas.py",
    "06_spark_mllib.py",
    "06_spark_mllib_boston.py",
    "06_spark_mllib_cancer.py",
    "05_spark_sql_06_funciones.py",
]


@pytest.mark.parametrize("script", LOCAL_OK_SCRIPTS)
def test_script_runs_without_error(run_script, script):
    result = run_script(script)
    assert result.returncode == 0, (
        f"{script} terminó con código {result.returncode}\n"
        f"--- stdout ---\n{result.stdout[-4000:]}\n"
        f"--- stderr ---\n{result.stderr[-4000:]}"
    )


# ---------------------------------------------------------------------------
# Comprobación de contenido: no solo "no falla", sino "calcula lo correcto"
# ---------------------------------------------------------------------------

def test_05_spark_sql_02_agrega_edad_media_por_pais(run_script):
    """files/sql.csv es fijo: comprueba la media de edad por país calculada
    por Spark (groupBy + avg) contra el valor calculado a mano."""
    result = run_script("05_spark_sql_02.py")
    assert result.returncode == 0
    assert "|   México|     30.0|" in result.stdout
    assert "|Argentina|     37.5|" in result.stdout
    assert "|   España|     36.0|" in result.stdout


def test_05_spark_sql_07_limpieza_datos(run_script):
    """files/datos_sucios.csv es fijo: comprueba que el dataset final queda
    sin el duplicado exacto (id=4 aparecía dos veces), con el nombre en
    blanco relleno, la edad no numérica ("cuarenta") y las fuera de rango
    (-5, 150) puestas a NULL, y el país normalizado (ESPAÑA -> España)."""
    result = run_script("05_spark_sql_07_limpieza_datos.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "Filas antes: 11, filas después de quitar duplicados exactos: 10" in result.stdout
    assert "|  3| Pedro|cuarenta|" in result.stdout  # detectado como no convertible
    assert "|  8| Elena|          150|" in result.stdout  # detectado fuera de rango
    assert "|  5|DESCONOCIDO|  28|   México|" in result.stdout  # nombre en blanco relleno
    assert "| 10|      Sofia|  22|   España|" in result.stdout  # ESPAÑA normalizado


def test_05_spark_sql_08_joins(run_script):
    """files/sql.csv (6 clientes) + files/pedidos.csv (5 pedidos, con un
    id_cliente=99 huérfano y 3 clientes sin ningún pedido) son fijos:
    comprueba el resultado exacto de los 6 tipos de join sobre los mismos
    datos (inner/left/right/full_outer/left_semi/left_anti)."""
    result = run_script("05_spark_sql_08_joins.py")
    assert result.returncode == 0, result.stderr[-4000:]
    # INNER: el pedido huérfano y los clientes sin pedidos no aparecen
    seccion_inner = result.stdout.split("--- INNER:")[1].split("--- LEFT:")[0]
    assert "Producto fantasma" not in seccion_inner
    assert "Patricia" not in seccion_inner
    # LEFT: los 3 clientes sin pedidos aparecen con NULL
    seccion_left = result.stdout.split("--- LEFT:")[1].split("--- RIGHT:")[0]
    assert "|  4|Patricia|     NULL|    NULL|" in seccion_left
    # RIGHT: el pedido huérfano (id_cliente=99) aparece con nombre NULL
    seccion_right = result.stdout.split("--- RIGHT:")[1].split("--- FULL OUTER:")[0]
    assert "|        99|  NULL|        5|Producto fantasma|" in seccion_right
    # LEFT ANTI: exactamente los 3 clientes sin pedidos, nadie más
    seccion_anti = result.stdout.split("--- LEFT ANTI:")[1]
    assert "Patricia" in seccion_anti and "Jose" in seccion_anti and "Miguel" in seccion_anti
    assert "Juan" not in seccion_anti


def test_05_spark_sql_09_window_functions(run_script):
    """files/sql.csv es fijo: comprueba row_number/rank/dense_rank por
    país+edad, lag/lead dentro del mismo país, y la suma acumulada de
    edades ordenado de menor a mayor."""
    result = run_script("05_spark_sql_09_window_functions.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "|   España|Patricia|  40|       3|     3|           3|" in result.stdout
    assert "|Argentina|   Pedro|  40|                      35|                     NULL|" in result.stdout
    assert "|Patricia|  40|                  202|" in result.stdout


def test_05_spark_sql_10_fechas(run_script):
    """Datos fijos (creados inline en el script): comprueba datediff (el
    pedido 5 cruza de año, 10 días), date_add (+30 días) y date_format
    (dd/MM/yyyy)."""
    result = run_script("05_spark_sql_10_fechas.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "|        5|  2024-12-24|   2025-01-03|                10|" in result.stdout
    assert "|        1|   2024-01-18|             2024-02-17|" in result.stdout
    assert "|        1|  2024-01-15|     15/01/2024|" in result.stdout


def test_05_spark_sql_11_funciones_texto(run_script):
    """files/addresses.csv es fijo: comprueba trim (estado sin espacios
    sobrantes), regexp_replace (comillas quitadas) y split/getItem (primer
    trozo de la dirección)."""
    result = run_script("05_spark_sql_11_funciones_texto.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "|    NJ|             NJ|" in result.stdout
    assert "|Joan \"the bone\", Anne|Joan the bone, Anne|" in result.stdout
    assert "|120 jefferson st.               |120                   |" in result.stdout


def test_05_spark_sql_12_datos_anidados(run_script):
    """files/personas_anidado.json es fijo: comprueba que explode() hace
    desaparecer a Pedro (array de teléfonos vacío) mientras que
    explode_outer() lo conserva con NULL."""
    result = run_script("05_spark_sql_12_datos_anidados.py")
    assert result.returncode == 0, result.stderr[-4000:]
    seccion_explode = result.stdout.split("--- explode:")[1].split("--- posexplode:")[0]
    assert "| Pedro|" not in seccion_explode
    seccion_explode_outer = result.stdout.split("--- explode_outer:")[1]
    assert "| Pedro|     NULL|" in seccion_explode_outer


def test_05_spark_sql_13_pivot(run_script):
    """Datos fijos (inline): comprueba que Carla suma sus dos ventas de Q4
    (1100+300=1400) y que los trimestres sin ventas quedan NULL en el
    pivot normal y 0 tras el na.fill(0)."""
    result = run_script("05_spark_sql_13_pivot.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "|   Carla|NULL| 900|NULL|1400|" in result.stdout
    assert "|   Carla|   0| 900|   0|1400|" in result.stdout


def test_05_spark_sql_14_operaciones_conjuntos(run_script):
    """Datos fijos (inline): comprueba que union() NO deduplica (Carla
    sale 2 veces, 6 filas totales), que intersect() encuentra solo a
    Carla, y que union() con columnas en distinto orden mezcla
    "nombre"/"ciudad" al revés mientras que unionByName() lo coloca bien."""
    result = run_script("05_spark_sql_14_operaciones_conjuntos.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "Filas totales: 6 (3 + 3 = 6, sin deduplicar)" in result.stdout
    assert "Filas totales: 5 (6 - 1 duplicado exacto = 5)" in result.stdout
    # union() mal mezclado: "Valencia" acaba en la columna "nombre"
    seccion_union_mal = result.stdout.split("distinto orden: mal mezclado")[1].split("unionByName()")[0]
    assert "|  6|Valencia|  Fran|" in seccion_union_mal
    # unionByName() bien colocado: "Fran" en nombre, "Valencia" en ciudad
    seccion_union_bien = result.stdout.split("unionByName(): mismo caso")[1]
    assert "|  6|  Fran|Valencia|" in seccion_union_bien


def test_05_spark_sql_15_escritura_formatos(run_script):
    """files/sql.csv es fijo: comprueba que la escritura particionada por
    "pais" crea una carpeta por país, y que el schema se recupera bien al
    releer tanto de parquet como de JSON (sin inferSchema)."""
    result = run_script("05_spark_sql_15_escritura_formatos.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "pais=España" in result.stdout
    assert "pais=México" in result.stdout
    assert "pais=Argentina" in result.stdout
    assert "|  4|Patricia|  40|   España|" in result.stdout
    assert "|  40|  4|Patricia|   España|" in result.stdout


def test_05_spark_sql_16_rendimiento(run_script):
    """files/sql.csv (6 filas) + files/pedidos.csv son fijos: comprueba
    que repartition(4)/coalesce(2) dan el número de particiones esperado,
    y que F.broadcast(pedidos) fuerza BuildRight en el plan (frente a
    BuildLeft cuando Spark decide solo, por ser "clientes" más pequeño)."""
    result = run_script("05_spark_sql_16_rendimiento.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "Tras repartition(4): 4 particiones" in result.stdout
    assert "Tras coalesce(2): 2 particiones" in result.stdout
    assert "Filas de clientes_mayores (servidas desde caché, no recalculadas): 5" in result.stdout
    seccion_normal = result.stdout.split("Spark ya elige broadcast solo")[1].split("forzando qué lado")[0]
    assert "BuildLeft" in seccion_normal
    seccion_forzado = result.stdout.split("F.broadcast(pedidos)")[-1]
    assert "BuildRight" in seccion_forzado


def test_05_spark_sql_05_avro(run_script):
    """files/users.avro es fijo: comprueba que se lee bien (incluido el
    array anidado favorite_numbers) y que sobrevive a un ciclo de
    escritura/relectura en Avro."""
    result = run_script("05_spark_sql_05_avro.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "|Alyssa|          NULL|  [3, 9, 15, 20]|" in result.stdout
    seccion_releida = result.stdout.split("Releído tras escribir")[1]
    assert "|Alyssa|          NULL|  [3, 9, 15, 20]|" in seccion_releida


def test_06_spark_mllib_entrena_y_evalua_iris(run_script):
    """Comprueba que el pipeline de MLlib sobre Iris llega a entrenar y
    evaluar un modelo (el split train/test no es determinista, así que solo
    se valida que se imprime una métrica, no un valor exacto)."""
    result = run_script("06_spark_mllib.py")
    assert result.returncode == 0
    assert "Test Acierto = 0." in result.stdout


def test_06_spark_mllib_boston_calcula_rmse(run_script):
    result = run_script("06_spark_mllib_boston.py")
    assert result.returncode == 0
    assert "Root Mean Squared Error (RMSE) on test data" in result.stdout


def test_06_spark_mllib_clustering_agrupa_iris(run_script):
    """Iris (mismo dataset que 06_spark_mllib.py, pero sin usar las
    etiquetas para entrenar) es determinista con seed=42: comprueba que
    la especie 0 (setosa) queda perfectamente separada en un único
    cluster, que es el resultado real y conocido de K-Means sobre Iris
    (las especies 1 y 2 sí se solapan algo, no se exige que salgan
    perfectas)."""
    result = run_script("06_spark_mllib_clustering.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "|           0|      1|   50|" in result.stdout
    assert "Silhouette score: 0." in result.stdout


def test_06_spark_mllib_anomalias_detecta_outliers(run_script):
    """Datos sintéticos con semilla fija (42): comprueba que se detectan
    2 de las 3 anomalías inyectadas sin ningún falso positivo — el
    resultado real de la regla de las 3 sigma sobre distancia euclídea
    sin estandarizar (la tercera anomalía, con la vibración disparada
    pero temperatura normal, queda por debajo del umbral porque la escala
    de "temperatura" domina la distancia — comportamiento esperado, no un
    fallo del ejemplo, explicado en el propio script)."""
    result = run_script("06_spark_mllib_anomalias.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "Anomalías reales detectadas: 2 de 3" in result.stdout
    assert "Falsos positivos: 0" in result.stdout


def test_06_spark_mllib_recomendacion_als(run_script):
    """Datos fijos (inline) con dos gustos claramente diferenciados
    (ciencia ficción / romance) y semilla fija (42): comprueba que ALS
    recomienda al usuario 0 (fan de ciencia ficción) la película
    "Interstellar", que nunca valoró, y ninguna de romance."""
    result = run_script("06_spark_mllib_recomendacion.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "RMSE en el conjunto de prueba: " in result.stdout
    seccion_usuario_0 = result.stdout.split("Recomendaciones para el usuario 0:")[1]
    assert "Interstellar" in seccion_usuario_0
    assert "Titanic" not in seccion_usuario_0
    assert "La La Land" not in seccion_usuario_0


# ---------------------------------------------------------------------------
# Scripts que dependen de un jar externo (GraphFrames) o de un dataset
# grande no versionado (08_spark_final_practice.py) — antes documentados
# como xfail, arreglados de verdad el 2026-09-21 (ver PLAN.md/CLAUDE.md).
# ---------------------------------------------------------------------------

def test_07_spark_graphx(run_script):
    """Usa io.graphframes:graphframes-spark4_2.13:0.12.2 (compatible con
    Spark 4.2.0/Scala 2.13) sobre un grafo propio en
    files/graph_{vertices,edges}.snappy.parquet — descarga el jar de Maven
    la primera vez, por eso el timeout más alto."""
    result = run_script("07_spark_graphx.py", timeout=180)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "Comunidades encontradas: " in result.stdout


def test_08_spark_final_practice(run_script, sales_dataset_present):
    """Requiere delta-spark (añadido a pyproject.toml) y spark.driver.memory
    subido a 4g (el 1g por defecto no basta para el join final sobre 1.5M
    filas). Comprueba que las fechas quedan bien parseadas (antes fallaban
    con CANNOT_PARSE_TIMESTAMP: el patrón "MM/dd/yyyy" no admite días/meses
    sin cero inicial como "7/27/2012", hace falta "M/d/yyyy")."""
    result = run_script("08_spark_final_practice.py", timeout=180)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "OrderDate='2012-07-27 00:00:00'" in result.stdout


# ---------------------------------------------------------------------------
# Scripts que dependen del entorno docker/compose.yaml
# ---------------------------------------------------------------------------

def test_04_spark_remote_contra_cluster_docker(run_script, spark_cluster_up):
    """Ejecuta el job en el cluster Spark standalone del docker/compose.yaml
    (spark://127.0.0.1:7077, tal y como está hardcodeado en el script) y
    comprueba que el resultado del RDD es el esperado."""
    result = run_script("04_spark_remote.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "['uno', 'dos', 'dos', 'tres', 'cuatro']" in result.stdout


def test_01_connect_hdfs_conexion_y_listado(run_script, hdfs_up):
    """Conexión básica: crea /user/admin (si no existe) y lo lista."""
    result = run_script("01_connect_hdfs.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "listado: " in result.stdout


def test_01_connect_hdfs_02_ficheros_sube_lee_borra(run_script, hdfs_up):
    """Comprueba que el cluster HDFS del docker/compose.yaml recibe datos de
    verdad: sube un fichero de files/, lo relee, escribe uno nuevo directo en
    HDFS, lo relee y borra ambos."""
    result = run_script("01_connect_hdfs_02_ficheros.py", timeout=180)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "b'Hello, world!'" in result.stdout


def test_01_connect_hdfs_03_conteo_palabras(run_script, hdfs_up):
    """Sube el Quijote a HDFS y cuenta palabras leyéndolo directamente desde
    ahí (sin pasar por Spark)."""
    result = run_script("01_connect_hdfs_03_conteo_palabras.py", timeout=180)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "DON QUIJOTE DE LA MANCHA" in result.stdout
    assert "'Quijote': 310" in result.stdout


def test_01_connect_hdfs_04_rpc_nativo_pyarrow(run_script_in_spark_master, spark_cluster_up, hdfs_up):
    """RPC nativo (puerto 8020) vía pyarrow.fs.HadoopFileSystem — solo puede
    ejecutarse dentro de spark-master (libhdfs.so no está en el venv del
    host), así que corre vía docker/04_exec_spark.sh en vez de run_script."""
    result = run_script_in_spark_master("01_connect_hdfs_04_rpc_nativo_pyarrow.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "b'hola desde pyarrow.fs.HadoopFileSystem (RPC nativo)\\n'" in result.stdout


def test_01_connect_s3_05_conexion_y_listado(run_script, s3_up):
    """Conexión básica a SeaweedFS (S3): crea el bucket "prueba" (si no
    existe) y lo lista."""
    result = run_script("01_connect_s3_05.py")
    assert result.returncode == 0, result.stderr[-4000:]
    assert "listado: " in result.stdout


def test_01_connect_s3_06_ficheros_sube_lee_borra(run_script, s3_up):
    """Comprueba que el servicio S3 (SeaweedFS) del docker/compose.yaml
    recibe datos de verdad: sube un fichero de files/, lo relee, escribe
    uno nuevo directo en S3, lo relee y borra ambos."""
    result = run_script("01_connect_s3_06_ficheros.py", timeout=180)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "b'Hello, world!'" in result.stdout


def test_01_connect_s3_08_modificar(run_script, s3_up):
    """Comprueba el ciclo subir -> descargar -> modificar -> resubir
    (sobrescribiendo la misma clave): el objeto final en S3 debe ser la
    versión en mayúsculas con la línea añadida, no el original."""
    result = run_script("01_connect_s3_08_modificar.py", timeout=180)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "SADAHSKDJ" in result.stdout
    assert "--- MODIFICADO ---" in result.stdout


def test_01_connect_postgres_07_lee_via_jdbc(run_script, postgres_up):
    """La tabla "empleados" la crea docker/postgres/init/01_empleados.sql
    al arrancar el contenedor por primera vez (datos fijos) — comprueba
    que Spark la lee de verdad vía JDBC y calcula bien el salario medio
    por departamento."""
    result = run_script("01_connect_postgres_07.py", timeout=180)
    assert result.returncode == 0, result.stderr[-4000:]
    assert "|  Ingeniería|            3| 46166.666667|" in result.stdout
