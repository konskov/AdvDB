from pyspark.sql import SparkSession
import pyspark
import time

# Init()
# if R not in local storage:
#     remotely retrive R 
#     partition into p chunks R1...Rp
#     save R1...Rp to local storage

# spark = SparkSession.builder.appName("broadcast-rdd").getOrCreate()
# sc = spark.sparkContext

# data1 =  

def map1(x, join_key):
    fields = x.split(",")
    return (fields.pop(join_key),fields)


def broadcast_join(rdd1, join_key1, rdd2, join_key2):
    ''' it is assumed that the two rdds are 
    in the acceptable format to perform a join
    (k,v), (k,w)'''
    
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
    # we'll be broadcasting a list
    # broadcast_small = sc.broadcast(small.collect())   
    # # print(type(broadcast_small))

    # s_list = broadcast_small.value
    # # print(s_list)
    # s_rdd = sc.parallelize(s_list)
    # # for i in s_rdd.collect():
    # #     print(i)

    # res = big.join(s_rdd)
    res = big.join(small)
    for i in res.take(120):
        print(i)
    

if __name__ == '__main__':
    spark = SparkSession.builder.appName('broadcastJoin').getOrCreate()
    sc = spark.sparkContext
    rdd1 = sc.textFile("hdfs://master:9000/movies/100lines.csv")
    rdd2 = sc.textFile("hdfs://master:9000/movies/ratings.csv")
    start_time = time.time()
    # rdd1 = rdd1.map(lambda x: (x.split(",")[0], x.split(","[1])))
    # rdd2 = rdd2.map(lambda x: map1(x))
    broadcast_join(rdd2, 1, rdd1, 0)
    print("Took %s seconds" %(time.time() - start_time))

