from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("q4-spark").getOrCreate()

def summary_length_chars(summary):
    if (summary):
        return len(summary)
    return 0    

def summary_length_words(summary):
    if (summary):
        return len(summary.split(" "))
    return 0    

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
spark.udf.register("chars", summary_length_chars)
spark.udf.register("words", summary_length_words)

start_time = time.time()
sqlString = "select timeframe, avg(characters) as average_chars, avg(wordies) as average_words from " + \
    "(select m._c0 as movie_id, chars(m._c2) as characters, words(m._c2) as wordies, " + \
    "( " + \
    "case " + \
    "when year(m._c3) between 2000 and 2004 then '2000-2004' " + \
    "when year(m._c3) between 2005 and 2009 then '2005-2009' " + \
    "when year(m._c3) between 2010 and 2014 then '2010-2014' " + \
    "when year(m._c3) between 2015 and 2019 then '2015-2019' " + \
    "when year(m._c3) > 2019 then '2020+' " + \
    "end) as timeframe " + \
    "from movies as m, genres as g " + \
    "where year(m._c3) >= 2000 and m._c0 = g._c0 and g._c1 like '%Drama%') as sq group by timeframe"

res = spark.sql(sqlString)
res.show()

# sqlString = "select m._c0 as movie_id, chars(m._c2) as characters, words(m._c2) as wordies, " + \
#     "( " + \
#     "case " + \
#     "when year(m._c3) between 2000 and 2004 then '2000-2004' " + \
#     "when year(m._c3) between 2005 and 2009 then '2005-2009' " + \
#     "when year(m._c3) between 2010 and 2014 then '2010-2014' " + \
#     "when year(m._c3) between 2015 and 2019 then '2015-2019' " + \
#     "when year(m._c3) > 2019 then '2020+' " + \
#     "end) as timeframe " + \
#     "from movies as m, genres as g " + \
#     "where year(m._c3) >= 2000 and m._c0 = g._c0 and g._c1 like '%Drama%'"

# res = spark.sql(sqlString)
# res.registerTempTable("temp_table")
# string = "select timeframe, avg(characters) as average_characters, avg(wordies) as average_words " + \
#     "from temp_table group by timeframe"

# res2 = spark.sql(string)
# res2.show()    

print("--- %s seconds ---" % (time.time() - start_time))