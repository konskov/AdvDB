from pyspark.sql import SparkSession
from io import StringIO 
import csv
import sys
spark = SparkSession.builder.appName("q3-rdd").getOrCreate()

sc = spark.sparkContext

def map1(x):
    fields = x.split(",")
    movie_id = fields[1]
    rating = float(fields[2])
    return (movie_id, (rating,1))

# def map2(x):
#     genre =

ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv")
genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv")

g = genres.map(lambda x: (x.split(",")[0], x.split(",")[1]))
# (movie_id, avg_rating)
r = ratings.map(lambda x: map1(x)). \
reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])). \
map(lambda x: (x[0],x[1][0]/x[1][1]))

# (movie_id, (avg_rating, genre))
res = r.join(g).map(lambda x: (x[1][1],(x[1][0],1))). \
reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])). \
map(lambda x: (x[0],(x[1][0]/x[1][1],x[1][1]))).sortByKey(ascending=True)

for i in res.collect():
    print(i)
