from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName('hello').setMaster('spark://127.0.0.1:7077').setSparkHome('/opt/spark/')
    sc = SparkContext(conf=conf)
    x = ['spark', 'rdd', 'example', 'sample', 'example']
    y = sc.parallelize(x)
    y.collect()
    lista = ['uno', 'dos', 'dos', 'tres', 'cuatro']
    listardd = sc.parallelize(lista)
    listardd = sc.parallelize(lista, 4)  # Incluir el número de cluster en lo que dividir el RDD
    print(listardd.collect())  # Visualizar la colección RDD