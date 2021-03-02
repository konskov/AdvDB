from pyspark.sql import SparkSession
from io import StringIO 
import csv
import sys
import time

spark = SparkSession.builder.appName("q5-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def map1(x):
    y = x.split(",")
    user_id = y[0]
    movie_id = y[1]
    return (movie_id, user_id)

def map3(x):
    y = split_complex(x)
    title = y[1]
    movie_id = y[0]
    popularity = y[7]
    return (movie_id, (title, popularity))

movies = sc.textFile("hdfs://master:9000/movies/movies.csv")
genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv")
ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv")
 
start_time = time.time() 
r = ratings.map(lambda x: map1(x))
m = movies.map(lambda x: map3(x))
g = genres.map(lambda x: (x.split(",")[0], x.split(",")[1]))
# (movie_id, (user_id, genre))
res1 = r.join(g)

# map returns ((genre, user_id), 1)
res1 = res1. \
map(lambda x: ((x[1][1], x[1][0]), 1)). \
reduceByKey(lambda x,y: (x + y))

res2 = res1.map(lambda x: (x[0][0],(x[0][1],x[1]))). \
reduceByKey(lambda x,y: ((x[0],x[1]) if (x[1] > y[1]) else (y[0], y[1])))

res = res2.sortByKey(ascending=True)

# m.join(g).map(lambda x: (x[0], (x[1][1], x[1][0][0], x[1][0][1])))
# m = movies.map(lambda x: map3(x))
# mur = r.join(m)

user_genre = res.map(lambda x: (x[1][0],x[0])).join


for i in res.collect():
    print(i)

print("--- %s seconds ---" % (time.time() - start_time))