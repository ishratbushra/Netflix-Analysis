from __future__ import print_function

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F, types as T

input_path        = "/user/root/lab/netflix_titles.csv"
output_directory  = "/user/root/lab/project_test_q1_out"

def main(sc):
    sqlContext = SQLContext(sc)

    schema = T.StructType([
        T.StructField("show_id",      T.StringType(),  True),
        T.StructField("type",         T.StringType(),  True),
        T.StructField("title",        T.StringType(),  True),
        T.StructField("director",     T.StringType(),  True),
        T.StructField("cast",         T.StringType(),  True),
        T.StructField("country",      T.StringType(),  True),
        T.StructField("date_added",   T.StringType(),  True),
        T.StructField("release_year", T.IntegerType(), True),
        T.StructField("rating",       T.StringType(),  True),
        T.StructField("duration",     T.StringType(),  True),
        T.StructField("listed_in",    T.StringType(),  True),
        T.StructField("description",  T.StringType(),  True),
    ])

    df = (sqlContext.read
          .option("header", "true")
          .option("multiLine", "true")
          .option("quote", '"')
          .option("escape", '"')
          .option("mode", "PERMISSIVE")
          .schema(schema)
          .csv(input_path))

    def nz(colname):
        c = F.col(colname)
        return F.when(F.length(F.trim(c)) == 0, None).otherwise(F.trim(c))

    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    for c in string_cols:
        df = df.withColumn(c, nz(c))
    df = df.withColumn("release_year", F.col("release_year").cast("int"))

    row_count = df.count()
    dup_count = df.count() - df.dropDuplicates().count()

    df_q1 = df.filter(F.col("type").isin("Movie", "TV Show"))

    duration_counts = (df_q1.filter(F.col("duration").isNotNull())
                       .groupBy("type", "duration")
                       .count())

    max_counts = (duration_counts.groupBy("type")
                  .agg(F.max("count").alias("max_count")))

    duration_mode = (duration_counts.join(max_counts, on="type", how="inner")
                                   .filter(F.col("count") == F.col("max_count"))
                                   .select("type", F.col("duration").alias("mode_duration"))
                                   .dropDuplicates(["type"]))

    df_final = (df_q1.join(duration_mode, on="type", how="left")
                    .withColumn("duration",
                                F.when(F.col("duration").isNull(), F.col("mode_duration"))
                                 .otherwise(F.col("duration")))
                    .drop("mode_duration"))

    missing_before = df_q1.filter(F.col("duration").isNull()).count()
    missing_after  = df_final.filter(F.col("duration").isNull()).count()
    filled_count   = missing_before - missing_after

    df_dur = df_final.withColumn(
        "duration_num",
        F.when(F.col("duration").like("%min%"),
               F.regexp_extract(F.col("duration"), r"(\d+)", 1).cast("int"))
         .when(F.col("duration").like("%Season%"),
               F.regexp_extract(F.col("duration"), r"(\d+)", 1).cast("int"))
         .otherwise(F.lit(None).cast("int"))
    )

    avg_dur = (df_dur.groupBy("type")
               .agg(F.round(F.avg("duration_num"), 2).alias("avg_duration")))

    movie_med_list = (df_dur
                      .filter((F.col("type") == "Movie") & F.col("duration_num").isNotNull())
                      .stat.approxQuantile("duration_num", [0.5], 0.01))
    tv_med_list = (df_dur
                   .filter((F.col("type") == "TV Show") & F.col("duration_num").isNotNull())
                   .stat.approxQuantile("duration_num", [0.5], 0.01))

    movie_med = int(round(movie_med_list[0], 0)) if movie_med_list else 0
    tv_med    = int(round(tv_med_list[0], 0)) if tv_med_list else 0

    avg_map = {}
    for r in avg_dur.collect():
        avg_map[r["type"]] = float(r["avg_duration"]) if r["avg_duration"] is not None else 0.0

    movie_mean = int(round(avg_map.get("Movie", 0.0), 0))
    tv_mean    = int(round(avg_map.get("TV Show", 0.0), 0))

    print("\nMode durations used for imputation:")
    for r in duration_mode.collect():
        print("%s: %s" % (r["type"], r["mode_duration"]))

    print("\nNETFLIX DATA SUMMARY:")
    print("Total rows:", row_count)
    print("Duplicate rows removed:", dup_count)
    print("Missing duration before fill:", missing_before)
    print("Missing duration after fill:", missing_after)
    print("Durations filled using mode:", filled_count)

    print("\nDURATION SUMMARY:")
    print("Movies: mean ~ %d min, median = %d min" % (movie_mean, movie_med))
    print("TV Shows: mean ~ %d seasons, median = %d seasons" % (tv_mean, tv_med))

    duration_rows = [
        ("Movies",  movie_mean, movie_med, "minutes"),
        ("TV Shows", tv_mean,   tv_med,    "seasons"),
    ]
    duration_df = sqlContext.createDataFrame(duration_rows, ["Type", "Mean", "Median", "Unit"])
    duration_df.write.mode("overwrite").option("header", "true") \
        .csv(output_directory + "/duration_summary_csv")

if __name__ == "__main__":
    conf = SparkConf().setAppName("project_test_q1_out")
    sc = SparkContext(conf=conf)
    try:
        main(sc)
    finally:
        sc.stop()
