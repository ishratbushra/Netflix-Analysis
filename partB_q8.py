from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, split, explode, avg, desc, round

file_movies  = "/user/root/lab/movies.csv"
file_ratings = "/user/root/lab/ratings.csv"
output_directory = "/user/root/lab/partB_q8_out"

def main(sc):
    sqlContext = SQLContext(sc)
    movies = (sqlContext.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(file_movies)
              .select("movieId", "genres")
              .where(col("movieId").isNotNull() & col("genres").isNotNull()))

    ratings = (sqlContext.read
               .option("header", "true")
               .option("inferSchema", "true")
               .csv(file_ratings)
               .select("movieId", "rating")
               .where(col("movieId").isNotNull() & col("rating").isNotNull()))

    joined_dataset = movies.join(ratings, on="movieId", how="inner")

    genres = (joined_dataset
                .withColumn("genre", explode(split(col("genres"), "\\|")))
                .select("genre", "rating")
                .where(col("genre").isNotNull()))

    average_rating_per_genre = (genres
                 .groupBy("genre")
                 .agg(round(avg("rating"), 2).alias("average_rating")))

    top_genres = average_rating_per_genre.orderBy(desc("average_rating")).limit(3)

    top_genres.write.option("header", "true").csv(output_directory)

if __name__ == "__main__":
    conf = SparkConf().setAppName("partB_q8_out")
    sc = SparkContext(conf=conf)
    main(sc)
    sc.stop()
