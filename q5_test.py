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
    rating = float(y[2])
    return (movie_id, (user_id, rating))

def map2(x):
    y = x.split(",")
    user_id = y[0]
    movie_id = y[1]
    rating = float(y[2])
    return (user_id, (movie_id, rating))


def map3(x):
    y = split_complex(x)
    title = y[1]
    movie_id = y[0]
    popularity = float(y[7])
    return (movie_id, (title, popularity))

def reduce_max(x,y):
    if (x[2] > y[2]):
        return (x[0],x[1],x[2])
    elif (x[2] < y[2]):
        return (y[0],y[1],y[2])    
    else:
        if (x[1] >= y[1]):
            return (x[0],x[1],x[2])
        else:
            return (y[0],y[1],y[2])

def reduce_min(x,y):
    if (x[2] < y[2]):
        return (x[0],x[1],x[2])
    elif (x[2] > y[2]):
        return (y[0],y[1],y[2])   
    else:
        if (x[1] <= y[1]):
            return (x[0],x[1],x[2])
        else:
            return (y[0],y[1],y[2])

movies = sc.textFile("hdfs://master:9000/movies/movies.csv")
genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv")
ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv")
 
start_time = time.time() 
# (movie, user)
r = ratings.map(lambda x: map1(x))
m = movies.map(lambda x: map3(x))
g = genres.map(lambda x: (x.split(",")[0], x.split(",")[1]))
# (movie_id, (user_id, genre))

res = m.join(g).map(lambda x: (x[0], (x[1][1], x[1][0][0], x[1][0][1])))
res = r.join(res).map(lambda x: ((x[1][1][0], x[1][0][0]), (x[1][1][1], x[1][1][2], x[1][0][1], 1)))
res1 = res.reduceByKey(lambda x,y: reduce_max(x,y))
# res_max = res.reduceByKey(lambda x,y: reduce_max(x,y))
# res_min = res.reduceByKey(lambda x,y: reduce_min(x,y))
# res1 = r.join(m)
# res1 = ratings.map(lambda x: map2(x)). \
# reduceByKey(lambda x,y: min((x,y), key=lambda k: k[1]))
for i in res1.sample(False, 0.01).collect():
    print(i)

print("--- %s seconds ---" % (time.time() - start_time))
