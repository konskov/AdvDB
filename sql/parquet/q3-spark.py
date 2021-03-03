from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q3-spark").getOrCreate()
ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")

genres = spark.read.parquet("hdfs://master:9000/movies/genres.parquet")
          

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

sqlString = "select g._c1 as genre, avg(avg_rating) as average_rating, count(*) as num_movies " + \
"from genres as g join ( " + \
	"select r._c1 as movie_id, avg(r._c2) as avg_rating " + \
	"from " + \
	"ratings as r " + \
	"group by r._c1 " + \
") as sq1 on sq1.movie_id = g._c0 "  + \
"group by g._c1 order by g._c1"

res = spark.sql(sqlString)
res.show()