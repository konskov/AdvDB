from pyspark.sql import SparkSession
import pyspark
import time

#### mono gia to mikro dataset mporei na ginei douleia 
#### gia to megalo logw tou collect exoume provlhma skaei

def repatition_join(rdd1, join_key1, rdd2, join_key2):
    ''' it is assumed that the two rdds are 
    in the acceptable format to perform a join
    (k,v), (k,w)'''

    def map1(x, join_key, tag):
        fields = x.split(",")
        res = (fields.pop(join_key),(fields,tag))
        # this does nothing
        # l.append(res[1])      
        return res

    def flatMap1(seq):
        l1 = []
        l2 = []
        for (fields, tag) in seq:
            if tag == 1:
                l1.append(fields)
            else:
                l2.append(fields)
        return [(l,r) for l in l1 for r in l2]


    rdd1 = rdd1.map(lambda x: map1(x,join_key1,1))
    rdd2 = rdd2.map(lambda x: map1(x,join_key2,2))

    # for i in rdd2.take(20):
    #     print(i)
    # l1 = [(key, rec, 1) for (key,rec) in rdd1.collect()]
    # l2 = [(key, rec, 2) for (key,rec) in rdd2.collect()]
    # print(l2[0], len(l2))
    # emit_list = []
    # for r in l1:
    #     for l in l2:
    #         if r[0] == l[0]:
    #             emit_list.append([r[0]] + r[1] + l[1])
    # print(emit_list)
    unioned = rdd1.union(rdd2)
    # for i in unioned.take(400):
    #     print(i)
    grouped = unioned.groupByKey().flatMapValues(lambda x: flatMap1(x))
    for i in grouped.take(300):
        print(i)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('repartionJoin').getOrCreate()
    sc = spark.sparkContext
    rdd1 = sc.textFile("hdfs://master:9000/movies/100lines.csv")
    rdd2 = sc.textFile("hdfs://master:9000/movies/ratings.csv")
    start_time = time.time()

    repatition_join(rdd1, 0, rdd2, 1)
    print("Took %s seconds" %(time.time() - start_time))