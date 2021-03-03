from pyspark.sql import SparkSession
from io import StringIO 
import csv
import sys
spark = SparkSession.builder.appName("q1-rdd").getOrCreate()

sc = spark.sparkContext
def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def get_year(timestamp):
    return int(timestamp[0:4])

def map1(x):
    y = split_complex(x)
    title = y[1]
    year = 0
    if y[3] != '' and y[3]:
        year = get_year(y[3])
    if year < 2000:
        year = 0     
    gross = int(y[6])
    production_cost = int(y[5]) 
    profit = 0
    if (production_cost != 0) and (gross != 0):
        profit = (gross - production_cost)/production_cost*100
    else:
        year = 0 # sta azhthta
    return (year, (profit, title))

def reduce1(x):
    pass

lines = sc.textFile("hdfs://master:9000/movies/movies.csv")
res1 = lines. \
map(lambda x: map1(x)). \
reduceByKey(lambda x,y: ((x[0],x[1]) if (x[0] > y[0]) else (y[0], y[1]))).sortByKey(ascending=False) # . \
# map(lambda x: (x[1],x[0])).sortByKey(ascending=True)

for i in res1.collect():
    if i[0] != 0:
        print('(',i[0],',',i[1][1],',', i[1][0],')')

# res =\
# 	sc.textFile("hdfs://master:9000/movies/movies.csv"). \
# 	map(lambda x :(int(x.split(",")[2])/12,
# 		format_name(x.split(",")[1]))). \
# 	sortByKey(ascending=False). \
# 	map(lambda x :(x[1], x[0])). \
# 	take(5)

