from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("q3-spark").getOrCreate()
ratings = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/movies/ratings.csv")

genres = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/movies/movie_genres.csv")    

movies = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/movies/movies.csv")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")
movies.registerTempTable("movies")

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

s = "select s.genre as genre, s.user_id as user_id, " + \
"rmax.title as fav_title, rmax.pop as fav_pop, rmax.rating as fav_rating, " + \
"rmin.title as least_fav_title, rmin.pop as least_fav_pop, rmin.rating as least_fav_rating " + \
"from g_u_mr as s, res_max as rmax, res_min as rmin " + \
"where s.genre = rmin.genre and s.genre = rmax.genre " + \
"and s.user_id = rmin.user_id and s.user_id = rmax.user_id "
res = spark.sql(s)
# res.show()

res.createOrReplaceTempView("s")

final = "select g.genre, g.user_id, g.reviews, t.fav_title, t.fav_pop, t.fav_rating, " + \
"t.least_fav_title, t.least_fav_pop, t.least_fav_rating " + \
"from g_u_mr as g join " + \
"( " + \
"select * from s " + \
"where (genre, user_id, fav_pop, least_fav_pop) in " + \
"(select genre, user_id, max(fav_pop), max(least_fav_pop) " + \
"from s " + \
"group by genre, user_id) " + \
") as t on t.genre = g.genre and t.user_id = t.user_id order by genre"

f = spark.sql(final)
f.show()
print("--- %s seconds ---" % (time.time() - start_time))
# trexei se 6 lepta