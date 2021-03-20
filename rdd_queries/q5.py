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

def map2(x):
    y = x.split(",")
    user_id = y[0]
    movie_id = y[1]
    rating = float(y[2])
    return (movie_id, (user_id,rating))

def map4(x, l):
    y = x.split(",")
    user_id = y[0]
    movie_id = y[1]
    rating = float(y[2])
    if user_id in l:
        return (user_id, (user_id,movie_id,rating))
    else:
        return ('axrhsto',('axrhsto',movie_id,rating))

def map3(x):
    y = split_complex(x)
    title = y[1]
    movie_id = y[0]
    popularity = float(y[7])
    return (movie_id, (title, popularity))

def reduce_max(x,y):
    if (x[0] > y[0]):
        return (x[0],x[1],x[2])
    elif (x[0] < y[0]):
        return (y[0],y[1],y[2])    
    else:
        if (x[2] >= y[2]):
            return (x[0],x[1],x[2])
        else:
            return (y[0],y[1],y[2])

def reduce_min(x,y):
    if (x[0] < y[0]):
        return (x[0],x[1],x[2])
    elif (x[0] > y[0]):
        return (y[0],y[1],y[2])    
    else:
        if (x[2] >= y[2]):
            return (x[0],x[1],x[2])
        else:
            return (y[0],y[1],y[2])

start_time = time.time() 
movies = sc.textFile("hdfs://master:9000/movies/movies.csv")
genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv")
ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv")
 
r = ratings.map(lambda x: map1(x))
m = movies.map(lambda x: map3(x))
g = genres.map(lambda x: (x.split(",")[0], x.split(",")[1]))
r2 = ratings.map(lambda x: map2(x))

# (movie_id, (user_id, genre))
res0 = r.join(g)

# map returns ((genre, user_id), 1)
res1 = res0. \
map(lambda x: ((x[1][1], x[1][0]), 1)). \
reduceByKey(lambda x,y: (x + y))

res = res1.map(lambda x: (x[0][0],(x[0][1],x[1]))). \
reduceByKey(lambda x,y: ((x[0],x[1]) if (x[1] > y[1]) else (y[0], y[1]))).sortByKey(ascending=True)
# m.join(g).map(lambda x: (x[0], (x[1][1], x[1][0][0], x[1][0][1])))

genre_user = res.map(lambda x: (x[0],x[1][0]))
ulist = list(set([x[1] for x in genre_user.collect()]))
gu_r = res.map(lambda x: (((x[0],x[1][0]),x[1][1])))
# print(ulist)
# for i in res.collect():
#     print(i)

########## Ayto to kommati einai pou vriskei ((genre, user),(rating, fav, pop), (rating, least_fav, pop))
m_gtp = m.join(g).map(lambda x: (x[0], (x[1][1], x[1][0][0], x[1][0][1])))
# ulist = ['8659','45811']
r4 = ratings.map(lambda x: map4(x,ulist)).filter(lambda x: x[0] in ulist). \
map(lambda x: ((x[1][1],(x[0],x[1][2])))).join(m_gtp). \
map(lambda x: (((x[1][1][0],x[1][0][0]),(x[1][0][1],x[1][1][1],x[1][1][2]))))

res_max = r4.reduceByKey(lambda x,y: reduce_max(x,y))

res_min = r4.reduceByKey(lambda x,y: reduce_min(x,y))

res_min_max = res_max.join(res_min)

res_final = gu_r.join(res_min_max).sortBy(lambda x: x[0][0])
for i in res_final.collect():
    print(i)
print("--- %s seconds ---" % (time.time() - start_time))
# 213 seconds