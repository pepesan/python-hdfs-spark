"""label_propagation/ es un ejemplo aparte del resto del proyecto: no usa
pyspark, es una detección de comunidades pura en Python con networkx —
sirve de comparación frente al labelPropagation de GraphFrames en
07_spark_graphx.py. A diferencia de los demás scripts, hay que ejecutarlo
con cwd=label_propagation/ (rutas relativas propias, ver
label_propagation.py), por eso tiene su propio fichero de test en vez de
usar la fixture run_script (pensada para la raíz del proyecto)."""

import json
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LABEL_PROPAGATION_DIR = PROJECT_ROOT / "label_propagation"


def test_label_propagation_facebook_politicians(tmp_path):
    """files/politician_edges.csv es fijo (dataset real, red de páginas de
    Facebook de políticos, ~5900 nodos) — con la semilla por defecto
    (seed=42) el resultado es determinista: comprueba que converge y that
    la modularidad de la partición encontrada es razonable (una red con
    estructura de comunidades real da modularidad bien por encima de 0,
    valores cercanos a 0 indicarían que no se detectó nada)."""
    salida = tmp_path / "asignacion.json"
    result = subprocess.run(
        [sys.executable, "label_propagation.py", "--assignment-output", str(salida)],
        cwd=LABEL_PROPAGATION_DIR,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    assert "Modularity is: 0.8" in result.stdout

    asignacion = json.loads(salida.read_text())
    assert len(asignacion) == 5908
    assert len(set(asignacion.values())) > 1  # más de una comunidad
