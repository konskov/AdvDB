from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("q2-spark").getOrCreate()
ratings = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/movies/ratings.csv")

ratings.registerTempTable("ratings")
start_time = time.time()
# find % of users that have given an average rating of movies >= 3.0
# pragmati douleuei ki etsi alla nomizw oti argei perissotero 
str1 = \
	"select s.users_avg_3/t.total_users*100 as percentage_over_3 " + \
	"from "	+ \
	"(select count(user_id) as users_avg_3 from ( "  + \
	"	select r._c0 as user_id, avg(r._c2) as avg_rating " + \
	"	from ratings as r " + \
	"	group by r._c0 " + \
	") as sq1 " + \
	"where avg_rating > 3) as s cross join " + \
	"(select count(user_id) as total_users from " + \
	"(select distinct(_c0) as user_id from ratings)) as t" 

# str1 = \
# 	"select count(user_id) as users_avg_3 from ( "  + \
# 	"	select r._c0 as user_id, avg(r._c2) as avg_rating " + \
# 	"	from ratings as r " + \
# 	"	group by r._c0 " + \
# 	") as sq1 " + \
# 	"where avg_rating >= 3"

str2 = "select count(user_id) as total_users from " + \
	"(select distinct(_c0) as user_id from ratings)"

res1 = spark.sql(str1)	
res1. show()

res2 = spark.sql(str2)	
res2. show()

print("--- %s seconds ---" % (time.time() - start_time))
