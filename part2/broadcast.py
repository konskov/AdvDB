from pyspark.sql import SparkSession
import pyspark
import time


def map1(x, join_key):
    fields = x.split(",")
    return (fields.pop(join_key),fields)


def broadcast_join(file1, join_key1, file2, join_key2):
    ''' inputs: two pairs (filename, join_key). filename (ratings.csv) which is in the directory hdfs://master:9000/movies
    and a column number which corresponds to the column on which we join. Only one-column joins are supported
    (But this is a feature of course, not a bug :P'''

    def map2(x):
        '''x is a record from L, x[0] is the key, x[1] the values'''
        key = x[0]
        newrecs = []
        if key in broadcast_small_dictionary.value.keys():
            for r_match in broadcast_small_dictionary.value[key]:
                newrecs.append(([key] + r_match + x[1]))

        if newrecs:
            return newrecs
        else:
            return []
   
    spark = SparkSession.builder.appName('broadcastJoin').getOrCreate()
    sc = spark.sparkContext
    rdd1 = sc.textFile("hdfs://master:9000/movies/" + file1)
    rdd2 = sc.textFile("hdfs://master:9000/movies/" + file2)

    start_time = time.time()

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
    small_dictionary = {}

    for i in small.collect():
        key = i[0]
        value = i[1]
        if key in small_dictionary.keys():
            small_dictionary[key].append(value)
        else:
            small_dictionary[key] = []
            small_dictionary[key].append(value)
    # we'll be broadcasting a dict
    broadcast_small_dictionary = sc.broadcast(small_dictionary)   
   
    big = big.flatMap(lambda x: map2(x))
    for i in big.take(120):
        print(i)

    print("Took %s seconds" %(time.time() - start_time))

if __name__ == '__main__':
    
    # rdd1 = rdd1.map(lambda x: (x.split(",")[0], x.split(","[1])))
    # rdd2 = rdd2.map(lambda x: map1(x))
    file2 = "ratings.csv"
    file1 = "100lines.csv"
    broadcast_join(file2, 1, file1, 0)
    

