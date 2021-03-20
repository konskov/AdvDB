from pyspark.sql import SparkSession
from io import StringIO 
import csv
import time
spark = SparkSession.builder.appName("q2-rdd").getOrCreate()

sc = spark.sparkContext

def map1(x):
    fields = x.split(",")
    user_id = fields[0]
    rating = float(fields[2])
    return (user_id, (rating,1))

start_time = time.time()
lines = sc.textFile("hdfs://master:9000/movies/ratings.csv")
# total_users = lines.map(lambda x: (x.split(",")[0], 1)).reduceByKey(lambda x, y: )
total_users = lines.map(lambda x: map1(x)). \
reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])). \
map(lambda x: ((1,1) if (x[1][0]/x[1][1] > 3) else (0,1))). \
reduceByKey(lambda x,y: (x+y)).sortByKey(ascending=True)

under3 = total_users.collect()[0][1]
over3 = total_users.collect()[1][1]
percentage = over3/(under3 + over3)*100
print(percentage)
print("--- %s seconds ---" % (time.time() - start_time))
