from pyspark import SparkContext

import sys

if __name__ == '__main__':
    """
    fname = sys.argv[1]
    search1 = sys.argv[2].lower()
    search2 = sys.argv[3].lower()

    sc = SparkContext("local", appName="Line Count")
    data = sc.textFile(fname)

    # Transformations
    filtered_data1 = data.filter(lambda s: search1 in s.lower())
    filtered_data2 = data.filter(lambda s: search2 in s.lower())
    # Actions
    num1 = filtered_data1.count()
    num2 = filtered_data2.count()

    print('Lines with "%s": %i, lines with "%s": %i' % (search1, num1, search2, num2))
    """
    # conectar a un servidor remoto
