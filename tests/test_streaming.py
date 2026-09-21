"""streaming/*.py usan Structured Streaming (la API moderna sobre
DataFrames), no la antigua API de DStream. Tienen su propio fichero de
test porque no encajan en el patrón "ejecuta y espera exit 0" de
tests/test_examples.py: 03_structured_streaming_kafka.py no termina por sí
solo (awaitTermination() sin timeout, pensado para streaming de verdad) —
hace falta lanzarlo como subproceso, darle tiempo a procesar y matarlo."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STREAMING_DIR = PROJECT_ROOT / "streaming"


def _env():
    env = dict(os.environ)
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
    env["PYSPARK_PYTHON"] = sys.executable
    return env


def test_01_structured_streaming_rate():
    """La fuente "rate" genera datos ella sola (5 filas/segundo durante 5
    segundos) — sin depender de nada externo, comprueba que se generaron
    filas de verdad (con margen: la velocidad exacta puede variar un poco
    según la carga de la máquina)."""
    result = subprocess.run(
        [sys.executable, "01_structured_streaming_rate.py"],
        cwd=STREAMING_DIR,
        env=_env(),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    assert "num_filas" in result.stdout

    import re
    match = re.search(r"\|\s*(\d+)\s*\|", result.stdout.split("num_filas")[1])
    assert match is not None, result.stdout
    # a 5 filas/s durante 5s deberían ser ~25; se exige solo un mínimo
    # razonable para no depender de la velocidad exacta de la máquina
    assert int(match.group(1)) >= 10


def test_02_y_03_kafka_productor_y_structured_streaming(kafka_up):
    """Vacía el topic "frases" (por si quedan mensajes de una ejecución
    manual anterior), lanza 02_kafka_productor.py (termina solo, envía 3
    frases fijas) y luego 03_structured_streaming_kafka.py como subproceso
    (no termina solo — se le da tiempo a procesar y se mata), comprobando
    en su salida por consola que el conteo de palabras es el esperado."""
    subprocess.run(
        [
            "docker", "exec", "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--delete", "--topic", "frases",
        ],
        capture_output=True,
        timeout=30,
    )  # no pasa nada si el topic no existía todavía (primera vez)

    productor = subprocess.run(
        [sys.executable, "02_kafka_productor.py"],
        cwd=STREAMING_DIR,
        env=_env(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert productor.returncode == 0, productor.stderr[-4000:]

    consumidor = subprocess.Popen(
        [sys.executable, "03_structured_streaming_kafka.py"],
        cwd=STREAMING_DIR,
        env=_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        salida, _ = consumidor.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        consumidor.terminate()
        salida, _ = consumidor.communicate(timeout=15)
    finally:
        if consumidor.poll() is None:
            consumidor.kill()

    assert "|    spark|    3|" in salida, salida[-4000:]
    assert "|  example|    2|" in salida, salida[-4000:]
