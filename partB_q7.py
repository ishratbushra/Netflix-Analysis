from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, desc 

file1 = "/user/root/lab/movies.csv"
file2   = "/user/root/lab/tags.csv"      
output_directory = "/user/root/lab/partB_q7_out"

def main(sc):
    sqlContext = SQLContext(sc)

    movies = (sqlContext.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(file1)
              .select("movieId", "title"))

    tags = (sqlContext.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file2)
            .select("movieId", "userId")
            .where(col("movieId").isNotNull() & col("userId").isNotNull()))

    total_unique_user_counts = (tags
        .select("movieId", "userId").distinct()
        .groupBy("movieId").count()
        .withColumnRenamed("count", "total_unique_user"))

    result = (total_unique_user_counts
                   .join(movies, on="movieId", how="inner")
                   .select("movieId", "title", "total_unique_user"))

    output = result.orderBy(desc("total_unique_user"))

    output.write.option("header", "true").csv(output_directory)

if __name__ == "__main__":
    conf = SparkConf().setAppName("partB_q7_out")
    sc = SparkContext(conf=conf)
    main(sc)
    sc.stop()
