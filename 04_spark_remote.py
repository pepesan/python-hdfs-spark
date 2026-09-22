# Requiere: docker/01_launch.sh (spark-master, spark-worker) — se conecta a
# spark://127.0.0.1:7077, el puerto RPC del master publicado por el docker.
#
# A diferencia de los ejemplos anteriores (modo "local", todo en un único
# proceso), aquí el driver (este script, corriendo en el host) se conecta
# a un cluster Spark standalone REAL — el `spark-master`/`spark-worker`
# levantados por docker/compose.yaml — y les manda el trabajo para que lo
# ejecuten ellos. `setMaster('spark://...')` es el equivalente a apuntar
# un cliente SQL a un servidor remoto en vez de a una BBDD local: en una
# empresa real, esa URL sería la del cluster Spark compartido.
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf() \
        .setAppName('nombre aplicacion') \
        .setMaster('spark://127.0.0.1:7077') \
        .setSparkHome('/opt/spark/')
    sc = SparkContext(conf=conf)

    # parallelize() reparte una lista de Python entre los executors del
    # cluster, convirtiéndola en un RDD (Resilient Distributed Dataset) —
    # la estructura de datos distribuida más básica de Spark, sobre la que
    # se construyen DataFrames y el resto de APIs de más alto nivel.
    x = ['spark', 'rdd', 'example', 'sample', 'example']
    y = sc.parallelize(x)
    y.collect()

    lista = ['uno', 'dos', 'dos', 'tres', 'cuatro']
    listardd = sc.parallelize(lista)
    # El segundo argumento de parallelize() fija en cuántas particiones se
    # reparte el RDD (aquí 4) — cada partición se procesa en un executor
    # distinto, es la unidad de paralelismo de Spark.
    listardd = sc.parallelize(lista, 4)
    print(listardd.collect())  # trae todos los datos de vuelta al driver
