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
    "05_spark_sql_05_avro.py",
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


# ---------------------------------------------------------------------------
# Scripts con bugs conocidos / datos de entrada que faltan en el repo:
# se documentan como xfail (no se corrigen aquí) para que la suite deje
# constancia del hallazgo sin marcar el resto en rojo.
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    reason=(
        "Falta el fichero de datos files/*.snappy.parquet en el repo (dataset "
        "propio de enlaces entre TLDs, sin fuente pública identificada). "
        "Además usa graphframes:graphframes:0.6.0-spark2.3-s_2.11, empaquetado "
        "para Spark 2.3/Scala 2.11: incompatible con nuestro Spark 4.2.0/Scala "
        "2.13 aunque se resolviera el dato."
    ),
    strict=True,
)
def test_07_spark_graphx(run_script):
    result = run_script("07_spark_graphx.py")
    assert result.returncode == 0


@pytest.mark.xfail(
    reason=(
        "files/1500000_Sales_Records.csv ya está descargado (dataset público "
        "de excelbianalytics.com), pero el script usa .format('delta') sin "
        "tener el paquete delta-spark instalado/configurado "
        "(SparkClassNotFoundException: Failed to find the data source: delta). "
        "Además, aunque se instalara, 'dfpart = df.write...save(...)' guarda "
        "el resultado de .save() (que devuelve None) y luego llama a "
        "dfpart.printSchema(), lo que fallaría igualmente."
    ),
    strict=True,
)
def test_08_spark_final_practice(run_script):
    result = run_script("08_spark_final_practice.py")
    assert result.returncode == 0


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
