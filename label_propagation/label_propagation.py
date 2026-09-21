# Requiere: ejecutar desde DENTRO de esta carpeta (no desde la raíz del
# proyecto): "cd label_propagation && python label_propagation.py" — las
# rutas por defecto de --input/--assignment-output (ver param_parser.py)
# son relativas a esta carpeta ("../files/..."), no a la raíz del repo.
# Sin servicios docker, sin pyspark: es una detección de comunidades pura
# en Python con networkx, para comparar con el mismo tipo de algoritmo
# (labelPropagation) hecho con Spark/GraphFrames en ../07_spark_graphx.py
# — aquí sobre un dataset real (red de páginas de Facebook de políticos,
# ../files/politician_edges.csv, ~5900 nodos) en vez del grafo de juguete
# de ese otro ejemplo.
"""Running label propagation."""

from model import LabelPropagator
from param_parser import parameter_parser
from print_and_read import graph_reader, argument_printer

def create_and_run_model(args):
    """
    Method to run the model.
    :param args: Arguments object.
    """
    graph = graph_reader(args.input)
    model = LabelPropagator(graph, args)
    model.do_a_series_of_propagations()

if __name__ == "__main__":
    args = parameter_parser()
    argument_printer(args)
    create_and_run_model(args)