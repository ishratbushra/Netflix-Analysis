from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, desc, sum  

file_ratings = "/user/root/lab/ratings.csv"
file_tags    = "/user/root/lab/tags.csv"
output_directory = "/user/root/lab/partB_q9_out"

def main(sc):
    sqlContext = SQLContext(sc)

    ratings = (sqlContext.read
               .option("header", "true")
               .option("inferSchema", "true")
               .csv(file_ratings)
               .select("userId", "movieId", "rating")
               .where(col("userId").isNotNull() &
                      col("movieId").isNotNull() &
                      col("rating").isNotNull()))

    tags = (sqlContext.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file_tags)
            .select("userId", "movieId", "tag")
            .where(col("userId").isNotNull() &
                   col("movieId").isNotNull() &
                   col("tag").isNotNull()))

    filtered_ratings = ratings.where(col("rating") >= 4.5)
    joined_dataset = tags.join(filtered_ratings, on=["userId", "movieId"], how="inner")

    per_tag_count = (joined_dataset
                         .groupBy("tag", "rating")
                         .count()
                         .withColumnRenamed("count", "tag_count_per_rating"))

    total = (per_tag_count
                   .groupBy("tag")
                   .agg(sum("tag_count_per_rating").alias("total_tag_count_high_ratings"))
                   .join(per_tag_count, on="tag", how="inner"))

    output = total.orderBy(desc("total_tag_count_high_ratings"), "tag", desc("rating"))

    output.write.option("header", "true").csv(output_directory)

if __name__ == "__main__":
    conf = SparkConf().setAppName("partB_q9_out")
    sc = SparkContext(conf=conf)
    main(sc)
    sc.stop()
