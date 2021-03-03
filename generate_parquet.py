from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("parquet").getOrCreate()
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

ratings.write.parquet("ratings.parquet")
movies.write.parquet("movies.parquet")
genres.write.parquet("genres.parquet")

parquetRatings = spark.read.parquet("ratings.parquet")