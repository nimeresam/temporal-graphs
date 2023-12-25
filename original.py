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
  print(y, graph.edges.count())

def getLeftEdges(rdds):
  left_rdd = rdds[0]
  left_edges = left_rdd.collect()
  output = []
  for [src, dst, _] in left_edges:
    output.append([src, dst, 1])
  return output

left_edges = getLeftEdges(rdds)

for y in range(1, len(years)):
  right_edges = rdds[y].collect()
  output = list()
  for le in left_edges:
    index = 0
    end = len(right_edges)
    while index < end:
      re = right_edges[index]
      if le[0] == re[0] and le[1] == le[1]:
        output.append([ le[0], le[1], le[2] + 1 ])
        right_edges.pop(index)
        end -= 1
      else:
        index += 1
  # add rest
  for re in right_edges:
    output.append([ re[0], re[1], 1 ])
  left_edges = output
  
df = sc.parallelize(left_edges).toDF(['src', 'dst', 'count'])
# Stop SparkSession
spark.stop()
