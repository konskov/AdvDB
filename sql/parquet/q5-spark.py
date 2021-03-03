from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("q3-spark").getOrCreate()
ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")
genres = spark.read.parquet("hdfs://master:9000/movies/genres.parquet") 
movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")
movies.registerTempTable("movies")

# ta arxidia mou pairnw me to akoloutho, sq1.genre is not an aggregate function
# sqlString = "select s.genre, t.user_id, s.reviews from " + \
# "(select genre, max(reviews) as reviews from " + \
# "    (select g._c1 as genre, r._c0 as user_id, count(*) as reviews " + \
# "    from " + \
# "    ratings as r join genres as g on g._c0 = r._c1 " + \
# "    group by g._c1, r._c0 order by genre, reviews desc) as sq1) as s, " + \
# "( " + \
# "select g._c1 as genre, r._c0 as user_id, count(*) as reviews " + \
# "from " + \
# "ratings as r join genres as g on g._c0 = r._c1 " + \
# "group by g._c1, r._c0 order by genre, reviews desc " + \
# ") as t " + \
# "where s.genre = t.genre and s.reviews = t.reviews " + \
# "order by s.genre " 

start_time = time.time()

str2 = "select genre, max(reviews) as reviews " + \
    "from " + \
    "(select g._c1 as genre, r._c0 as user_id, count(*) as reviews " + \
    "from " + \
    "ratings as r join genres as g on g._c0 = r._c1 " + \
    "group by g._c1, r._c0 order by genre, reviews desc) as sq1 " + \
    "group by genre order by genre" 

# for every genre, find user with most reviews in this genre, and his/her favorite and least favorite movie
str1 = "select g._c1 as genre, r._c0 as user_id, count(*) as reviews " + \
    "from " + \
    "ratings as r join genres as g on g._c0 = r._c1 " + \
    "group by g._c1, r._c0 order by genre, reviews desc"

res1 = spark.sql(str1)
res2 = spark.sql(str2)

res1.registerTempTable("genre_uid_rev")
res2.registerTempTable("genre_max_rev")

# res1.show()
# res2.show()
sqlString = "select s.genre, t.user_id, s.reviews " + \
    "from " + \
    "genre_uid_rev as t, genre_max_rev as s " + \
    "where t.genre = s.genre and t.reviews = s.reviews order by s.genre"    

res = spark.sql(sqlString)    
# res.show()
res.registerTempTable("g_u_mr")

select 

print("--- %s seconds ---" % (time.time() - start_time))