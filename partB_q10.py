from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, min, max, count, round, desc

file_movies  = "/user/root/lab/movies.csv"
file_ratings = "/user/root/lab/ratings.csv"
output_directory = "/user/root/lab/partB_q10_out"

def main(sc):
    sqlContext = SQLContext(sc)

    movies = (sqlContext.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(file_movies)
              .select("movieId", "title")
              .where(col("movieId").isNotNull() & col("title").isNotNull()))

    ratings = (sqlContext.read
               .option("header", "true")
               .option("inferSchema", "true")
               .csv(file_ratings)
               .select("movieId", "rating")
               .where(col("movieId").isNotNull() & col("rating").isNotNull()))

    joined_dataset = movies.join(ratings, on="movieId", how="inner")

    rating_range_count = (joined_dataset
                   .groupBy("movieId", "title")
                   .agg(
                       round(min("rating"), 2).alias("min_rating"),
                       round(max("rating"), 2).alias("max_rating"),
                       count("rating").alias("total_ratings")
                   )
                   .withColumn("widest_rating_range", round(col("max_rating") - col("min_rating"), 2)))

    output = rating_range_count.orderBy(desc("widest_rating_range"), desc("total_ratings"), "title")

    output.write.option("header", "true").csv(output_directory)

if __name__ == "__main__":
    conf = SparkConf().setAppName("partB_q10_out")
    sc = SparkContext(conf=conf)
    main(sc)
    sc.stop()
