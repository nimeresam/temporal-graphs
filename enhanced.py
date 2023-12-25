from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from graphframes import *

from utils import readRawDF, toGraph

conf = SparkConf().setAppName('graph_processing') \
                  .set('spark.jars.packages', 'graphframes:graphframes:0.8.1-spark3.0-s_2.12')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

raw = readRawDF(spark)

years = [2014, 2015, 2016, 2017]

rdds = list()
for y in years:
  df =  raw.filter(raw.YEAR == y)
  graph = toGraph(df)
  rdds.append(graph.edges.rdd)
  
def getLeftEdges(rdds):
  left_rdd = rdds[0]
  left_edges = left_rdd.collect()
  output = {}
  for [src, dst, _] in left_edges:
    if not output.get(src):
      output[src] = {}
    srcDict = output[src]
    srcDict[dst] = 1
  return output

left_edges = getLeftEdges(rdds)

for y in range(1, len(years)):
  right_edges = rdds[y].collect()
  for re in right_edges:
    [src, dst, _] = re
    if not left_edges.get(src):
      left_edges[src] = {}
    if not left_edges[src].get(dst):
      left_edges[src][dst] = 1
    else:
      left_edges[src][dst] += 1

output = []
for [src, value] in left_edges.items():
  for (dst, count) in value.items():
    output.append([src, dst, count])
df = sc.parallelize(output).toDF(['src', 'dst', 'count'])
# Stop SparkSession
spark.stop()
