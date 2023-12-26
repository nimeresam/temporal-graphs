from pyspark.sql.functions import col, year, to_timestamp
from graphframes import *

def readRawDF(spark):
    # Vertices DataFrame
    raw_1 = spark.read.option("header", True)\
    .option("delimiter", "\t")\
    .csv("./dataset/soc-redditHyperlinks-title.tsv")

    raw_2 = spark.read.option("header", True)\
    .option("delimiter", "\t")\
    .csv("./dataset/soc-redditHyperlinks-body.tsv")

    return raw_1.unionAll(raw_2)\
    .withColumn("TIMESTAMP", to_timestamp(col("TIMESTAMP")))\
    .withColumn("YEAR", year(col("TIMESTAMP")))

def toGraph(df):
  vertices = df.select(
    col('SOURCE_SUBREDDIT').alias('id'),
    col('POST_ID').alias('post'),
  )
  edges = df.select(
    col('SOURCE_SUBREDDIT').alias('src'),
    col('TARGET_SUBREDDIT').alias('dst')
  ).groupBy('src', 'dst').count().filter('count > 5')
  return GraphFrame(vertices, edges).dropIsolatedVertices()