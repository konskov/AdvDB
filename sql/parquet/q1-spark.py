from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("q1-spark").getOrCreate()
start_time = time.time()

movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet")

movies.registerTempTable("movies")

str1 = "select m._c0 as id, m._c1 as title, year(m._c3) as year, (m._c6 - m._c5)/m._c5*100 as profit " + \
	"from movies as m " + \
	"where m._c5 > 0 and m._c6 > 0 and m._c3 is not null " + \
	"and year(m._c3) >= 2000 order by profit desc"

res1 = spark.sql(str1)
# res1.show()

res1.registerTempTable("mov_prof")
str2 = "SELECT title, year, profit " + \
	"FROM mov_prof WHERE (year,profit) IN " + \
	"( SELECT year, MAX(profit) " + \
	"FROM mov_prof GROUP BY year) order by year"
res2 = spark.sql(str2)
res2.show()	
print("--- %s seconds ---" % (time.time() - start_time))
