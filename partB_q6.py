from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, count, desc, round  

file1  = "/user/root/lab/movies.csv"
file2 = "/user/root/lab/ratings.csv"
output_directory = "/user/root/lab/partB_q6_out"

def main(sc):
    sqlContext = SQLContext(sc)

    movies = (sqlContext.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(file1)
              .select("movieId", "title"))

    ratings = (sqlContext.read
               .option("header", "true")
               .option("inferSchema", "true")
               .csv(file2)
               .select("movieId", "rating"))

    average = (ratings
               .groupBy("movieId")
               .agg(count("*").alias("total_rating"),
                    avg("rating").alias("average_rating")))
    movie_title = (average
                   .where(col("total_rating") >= 100)
                   .join(movies, on="movieId", how="inner")
                   .select("movieId", "title", "average_rating", "total_rating"))

    highest_rated = (movie_title
                     .orderBy(desc("average_rating"))
                     .limit(5)
                     .withColumn("average_rating", round(col("average_rating"), 2)))

    highest_rated.write.option("header", "true").csv(output_directory)

if __name__ == "__main__":
    conf = SparkConf().setAppName("partB_q6_out")
    sc = SparkContext(conf=conf)
    main(sc)
    sc.stop()
