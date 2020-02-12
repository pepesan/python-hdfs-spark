from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    # acceso remoto a spark
    # aquí se dice donde está el servidor master
    # el server lo monta cada empresa en un nombre y puerto
    # esto es un builder :)
    conf = SparkConf()\
        .setAppName('nombre apliacacion')\
        .setMaster('spark://127.0.0.1:7077')\
        .setSparkHome('/opt/spark/')
    # es una conexión a Spark
    # como una conexión a una BBDD MySQL pero a Spark
    # conf es el parámetro del contructor de SparkContenxt
    sc = SparkContext(conf=conf)
    # listado normal python con datos chorras
    # pero podría ser un fichero super complejo
    x = ['spark', 'rdd', 'example', 'sample', 'example']
    # enviamos los datos a spark
    # crea un RDD en spark, un listado de datos en spark
    # para luego trabajar con ellos
    y = sc.parallelize(x)

    y.collect()
    lista = ['uno', 'dos', 'dos', 'tres', 'cuatro']
    listardd = sc.parallelize(lista)
    listardd = sc.parallelize(lista, 4)  # Incluir el número de cluster en lo que dividir el RDD
    print(listardd.collect())  # Visualizar la colección RDD