from pyspark.sql import SparkSession
from io import StringIO 
import csv
import sys
spark = SparkSession.builder.appName("q3-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def summary_length_chars(summary):
    if (summary):
        return len(summary)
    return 0    

def summary_length_words(summary):
    if (summary):
        return len(summary.split(" "))
    return 0    

def get_year(timestamp):
    return int(timestamp[0:4])

def map1(x):
    y = split_complex(x)
    movie_id = y[0]
    year = 0
    if y[3] != '' and y[3]:
        year = get_year(y[3])
    if year >= 2000 and year < 2005:
        period = '2000-2004'
    elif year >= 2005 and year < 2010:
        period = '2005-2009'
    elif year >= 2010 and year < 2015:
        period = '2010-2014'
    elif year >= 2015 and year < 2020:
        period = '2015-2019'
    elif year >= 2020:
        period = '2020+' 
    else:
        period = '1000'
    summary = y[2]
    
    w = summary_length_words(summary)
    c = summary_length_chars(summary)

    return (movie_id, (period, c, w, 1))

def map2(x):
    genre = x[1][0]
    period = x[1][1][0]
    if "Drama" not in genre:
        period = '1000'
    return (period, (x[1][1][1], x[1][1][2], x[1][1][3]))
    

movies = sc.textFile("hdfs://master:9000/movies/movies.csv")
genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv")

m = movies.map(lambda x: map1(x))

g = genres.map(lambda x: (x.split(",")[0], x.split(",")[1]))
# (movie_id, (Genre, (period, chars, words, 1))) to apotelesma tou join
res = g.join(m). \
map(lambda x: map2(x)). \
reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1], x[2] + y[2])). \
map(lambda x: (x[0],(x[1][0]/x[1][2], x[1][1]/x[1][2]))).sortByKey(ascending=True)

for i in res.collect():
    print(i)