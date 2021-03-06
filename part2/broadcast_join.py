from pyspark.sql import SparkSession
import pyspark
import time


def map1(x, join_key):
    fields = x.split(",")
    return (fields.pop(join_key),fields)

def map2(x):
    '''x is a record from L '''
    key = x[0]
    newrecs = []
    if key in broadcast_small_dict.value.keys():
        for r_match in broadcast_small_dict.value[key]:
            newrecs.append(([key] + r_match + x[1]))

    if newrecs:
        return newrecs
    else:
        return []

    

if __name__ == '__main__':
    spark = SparkSession.builder.appName('broadcastJoin').getOrCreate()
    sc = spark.sparkContext
    rdd1 = sc.textFile("hdfs://master:9000/movies/100lines.csv")
    rdd2 = sc.textFile("hdfs://master:9000/movies/ratings.csv")
    start_time = time.time()
    # rdd1 = rdd1.map(lambda x: (x.split(",")[0], x.split(","[1])))
    # rdd2 = rdd2.map(lambda x: map1(x))
    # broadcast_join(rdd2, 1, rdd1, 0)
    join_key1 = 0
    join_key2 = 1
    if min(rdd1.count(),rdd2.count()) == rdd1.count():
        small = rdd1
        jks = join_key1
        big = rdd2
        jkb = join_key2
    else:
        small = rdd2
        big = rdd1
        jks = join_key2
        jkb = join_key1

    small = small.map(lambda x: map1(x,jks))
    big = big.map(lambda x: map1(x,jkb))
    small_dict = {}

    for i in small.collect():
        key = i[0]
        value = i[1]
        if key in small_dict.keys():
            small_dict[key].append(value)
        else:
            small_dict[key] = []
            small_dict[key].append(value)
    # we'll be broadcasting a dict
    broadcast_small_dict = sc.broadcast(small_dict)   
    
    big = big.flatMap(lambda x: map2(x))
    for i in big.take(300):
        print(i)
    
    print("Took %s seconds" %(time.time() - start_time))

