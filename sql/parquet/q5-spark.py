from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("q3-spark").getOrCreate()
start_time = time.time()

ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")
genres = spark.read.parquet("hdfs://master:9000/movies/genres.parquet") 
movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")
movies.registerTempTable("movies")


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

res1.createOrReplaceTempView("genre_uid_rev")
res2.createOrReplaceTempView("genre_max_rev")

# res1.show()
# res2.show()
sqlString = "select s.genre as genre, t.user_id as user_id, s.reviews as reviews " + \
    "from " + \
    "genre_uid_rev as t, genre_max_rev as s " + \
    "where t.genre = s.genre and t.reviews = s.reviews order by s.genre"    

res = spark.sql(sqlString)    
# res.show()
res.createOrReplaceTempView("g_u_mr")

rmax = "select r._c0 as user_id, g._c1 as genre, m._c0 as movie_id, m._c1 as title, m._c7 as pop, r._c2 as rating " + \
"from ratings as r, movies as m, genres as g " + \
"where m._c0 = r._c1 and m._c0 = g._c0 " + \
"and r._c0 IN (select distinct user_id from g_u_mr) " + \
"and (g._c1, r._c0, r._c2) IN " + \
"(select g._c1 as genre, r._c0 as user_id, max(r._c2) " + \
"from " + \
"ratings as r join genres as g on g._c0 = r._c1 " + \
"where r._c0 IN (select distinct user_id from g_u_mr) group by genre, user_id order by genre) " 

rmin = "select r._c0 as user_id, g._c1 as genre, m._c0 as movie_id, m._c1 as title, m._c7 as pop, r._c2 as rating " + \
"from ratings as r, movies as m, genres as g " + \
"where m._c0 = r._c1 and m._c0 = g._c0 " + \
"and r._c0 IN (select distinct user_id from g_u_mr) " + \
"and (g._c1, r._c0, r._c2) IN " + \
"(select g._c1 as genre, r._c0 as user_id, min(r._c2) " + \
"from " + \
"ratings as r join genres as g on g._c0 = r._c1 " + \
"where r._c0 IN (select distinct user_id from g_u_mr) group by genre, user_id order by genre) " 

res_max = spark.sql(rmax)
res_min = spark.sql(rmin)
res_max.createOrReplaceTempView("res_max")
res_min.createOrReplaceTempView("res_min")

most_fav = "select * from res_max as r " + \
"where (r.genre, r.user_id, r.pop) in (select genre, user_id, max(pop) from res_max group by genre, user_id)"
least_fav = "select * from res_min as r " + \
"where (r.genre, r.user_id, r.pop) in (select genre, user_id, max(pop) from res_min group by genre, user_id)"
mf = spark.sql(most_fav)
lf = spark.sql(least_fav)
mf.createOrReplaceTempView("most_fav")
lf.createOrReplaceTempView("least_fav")

final = "select g.genre, g.user_id, g.reviews, t.title, t.pop, t.rating, " + \
"s.title, s.pop, s.rating " + \
"from g_u_mr as g, most_fav as t, least_fav as s " + \
"where s.genre = t.genre and s.genre = g.genre " + \
"and s.user_id = t.user_id and s.user_id = g.user_id " + \
"order by g.genre"
 
f = spark.sql(final)
f.show()
print("--- %s seconds ---" % (time.time() - start_time))