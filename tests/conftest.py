import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def spark_cluster_up():
    """El cluster Spark standalone del docker/compose.yaml está arriba."""
    if not _port_open("localhost", 7077):
        pytest.skip("El cluster Spark de docker/ no está levantado (docker/01_launch.sh)")


@pytest.fixture(scope="session")
def hdfs_up():
    """El namenode/datanode del docker/compose.yaml están arriba y accesibles."""
    if not _port_open("localhost", 9870):
        pytest.skip("HDFS (namenode) de docker/ no está levantado (docker/01_launch.sh)")
    if not _port_open("localhost", 9864):
        pytest.skip("HDFS (datanode) de docker/ no está levantado (docker/01_launch.sh)")
    if not _port_open("datanode", 9864):
        pytest.skip(
            "El host no resuelve 'datanode' (falta la entrada en /etc/hosts, "
            "ver README.md)"
        )


@pytest.fixture(scope="session")
def s3_up():
    """El servicio seaweedfs del docker/compose.yaml está arriba."""
    if not _port_open("localhost", 8333):
        pytest.skip("SeaweedFS de docker/ no está levantado (docker/01_launch.sh)")
    if not (PROJECT_ROOT / "docker/seaweedfs/s3-config/s3.json").exists():
        pytest.skip(
            "Faltan las credenciales S3 (docker/00_init.sh no se ha ejecutado, "
            "ver README.md)"
        )


@pytest.fixture(scope="session")
def postgres_up():
    """El servicio postgres del docker/compose.yaml está arriba."""
    if not _port_open("localhost", 5432):
        pytest.skip("Postgres de docker/ no está levantado (docker/01_launch.sh)")
    if not (PROJECT_ROOT / "docker/postgres/.env").exists():
        pytest.skip(
            "Falta la contraseña de postgres (docker/00_init.sh no se ha "
            "ejecutado, ver README.md)"
        )


@pytest.fixture(scope="session")
def kafka_up():
    """El servicio kafka del docker/compose.yaml está arriba."""
    if not _port_open("localhost", 9092):
        pytest.skip("Kafka de docker/ no está levantado (docker/01_launch.sh)")


@pytest.fixture(scope="session")
def sales_dataset_present():
    """files/1500000_Sales_Records.csv no va en el repo (~187MB, ver
    .gitignore) — hay que descargarlo a mano (ver README.md)."""
    if not (PROJECT_ROOT / "files/1500000_Sales_Records.csv").exists():
        pytest.skip(
            "Falta files/1500000_Sales_Records.csv (no va en el repo por "
            "tamaño, ver README.md para descargarlo)"
        )


@pytest.fixture
def run_script():
    """Ejecuta un script del proyecto tal cual lo haría el usuario desde la
    raíz del repo, con el intérprete del venv (uv) y las variables de
    entorno necesarias para que pyspark no falle por versión de Python."""

    def _run(script_name: str, timeout: int = 120) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
        env["PYSPARK_PYTHON"] = sys.executable
        env["MPLBACKEND"] = "Agg"
        return subprocess.run(
            [sys.executable, script_name],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    return _run


@pytest.fixture
def run_script_in_spark_master():
    """Ejecuta un script DENTRO del contenedor spark-master vía spark-submit
    (docker/04_exec_spark.sh), para los scripts que no pueden correr desde
    el venv del host (p. ej. los que dependen de libhdfs.so, solo presente
    en esa imagen — ver 01_connect_hdfs_04_rpc_nativo_pyarrow.py)."""

    def _run(script_name: str, timeout: int = 180) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["./04_exec_spark.sh", script_name],
            cwd=PROJECT_ROOT / "docker",
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    return _run
