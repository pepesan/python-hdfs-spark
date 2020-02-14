import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell'

import pyspark
from pyspark.sql import *
from pyspark.sql.functions import udf


sc = pyspark.SparkContext("local[*]")
spark = SparkSession.builder.appName('notebook').getOrCreate()



from graphframes import *
import hashlib

# This dataset is already filtered to include only links between TLDs, not within TLDs.
# I also filtered out common sites and resources for a cleaner sample graph.
raw_data = spark.read.parquet("files/*.snappy.parquet")
print(raw_data.count())

# Rename columns to something decent.
df = raw_data.withColumnRenamed("_c0", "parent")\
.withColumnRenamed("_c1", "parentTLD")\
.withColumnRenamed("_c2", "childTLD")\
.withColumnRenamed("_c3", "child")\
.filter("parentTLD is not null and childTLD is not null")

df.show(5)

# Select set of parents and children TLDs (your nodes) to assign id for each node.

aggcodes = df.select("parentTLD","childTLD").rdd.flatMap(lambda x: x).distinct()
print(aggcodes.count())

def hashnode(x):

    try:
        sha1bar = hashlib.sha1(x.encode("UTF-8"))
        ret = sha1bar.hexdigest()[:8]
        return ret
    except:
        pass

hashnode_udf = udf(hashnode)

vertices = aggcodes.map(lambda x: (hashnode(x), x)).toDF(["id","name"])

vertices.show(5)

edges = df.select("parentTLD","childTLD")\
.withColumn("src", hashnode_udf("parentTLD"))\
.withColumn("dst", hashnode_udf("childTLD"))\
.select("src","dst")

edges.show(5)


# create GraphFrame
graph = GraphFrame(vertices, edges)

print(graph)

# Run LPA
communities = graph.labelPropagation(maxIter=5)

communities.persist().show(10)

print (f"There are {communities.select('label').distinct().count()} communities in sample graph.")


# Count nodes by number of in-degrees

graph.inDegrees.join(vertices, on="id")\
.orderBy("inDegree", ascending=False).show(10)


results = graph.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank")\
.join(vertices, on="id").orderBy("pagerank", ascending=False)\
.show(10)