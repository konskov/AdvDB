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

# select m.title, m.popularity
# from 
# movies as m, genres as g, ratings as r
# where r.uid in (45811,8659) and m.id = g.id, r.mid = m.id
# and m.rating in (select max(r._c2) from 
# ratings as r join genres as g )

s = "select r._c0 as user, g._c1 as genre, m._c0 as movie_id, m._c1 as title, m._c7 as pop, r._c2 as rating " + \
"from ratings as r, movies as m, genres as g " + \
"where m._c0 = r._c1 and m._c0 = g._c0 " + \
"and r._c0 IN (45811,8659) " + \
"and (g._c1, r._c0, r._c2) IN " + \
"(select g._c1 as genre, r._c0 as user_id, max(r._c2) " + \
"from " + \
"ratings as r join genres as g on g._c0 = r._c1 " + \
"where r._c0 IN (45811,8659) group by genre, user_id order by genre) " 

# s = "select r._c0 as user, g._c1 as genre, m._c0 as movie_id, m._c1 as title, m._c7 as pop, r._c2 as rating " + \
# "from ratings as r, movies as m, genres as g " + \
# "where m._c0 = r._c1 and m._c0 = g._c0 " + \
# "and r._c0 IN (45811,8659) " + \
# "and (g._c1, r._c0, r._c2) IN " + \
# "(select g._c1 as genre, r._c0 as user_id, max(r._c2) " + \
# "from " + \
# "ratings as r join genres as g on g._c0 = r._c1 " + \
# "where r._c0 IN (45811,8659) group by genre, user_id order by genre) " 

# s = "select r._c0 as user_id, g._c1 as genre, max(r._c2), min(r._c2) " + \
# "from "  + \
# "ratings as r join genres as g on g._c0 = r._c1 " + \
# "where r._c0 IN (45811,8659) group by genre, user_id order by genre "
res = spark.sql(s)
res.show()
print("--- %s seconds ---" % (time.time() - start_time))