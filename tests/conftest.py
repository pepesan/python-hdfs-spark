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
