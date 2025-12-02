from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F, types as T, Window

INPUT_PATH   = "/user/root/lab/netflix_titles.csv"
OUTPUT_BASE  = "/user/root/lab/project_test_q2_out"
OUT_QUALITY  = OUTPUT_BASE + "/data_quality_summary_csv"
OUT_YEARLY   = OUTPUT_BASE + "/yearly_genre_prevalence_csv"
OUT_MOVIES   = OUTPUT_BASE + "/duration_trends_movies_csv"
OUT_TV       = OUTPUT_BASE + "/duration_trends_tv_csv"

def main(sc):
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

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
          .csv(INPUT_PATH))

    def nz(colname):
        c = F.col(colname)
        return F.when(F.length(F.trim(c)) == 0, None).otherwise(F.trim(c))

    for f in df.schema.fields:
        if isinstance(f.dataType, T.StringType):
            df = df.withColumn(f.name, nz(f.name))

    df = (df.withColumn("release_year", F.col("release_year").cast("int"))
            .filter(F.col("type").isin("Movie", "TV Show")))

    total_rows = df.count()

    quality_schema = T.StructType([
        T.StructField("column",     T.StringType(),  True),
        T.StructField("null_count", T.IntegerType(), True),
        T.StructField("null_pct",   T.DoubleType(),  True),
    ])

    agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    null_counts_row = df.agg(*agg_exprs).collect()[0]

    null_rows = []
    for i, colname in enumerate(df.columns):
        n = int(null_counts_row[i])
        pct = (n / float(total_rows) * 100.0) if total_rows > 0 else 0.0
        null_rows.append((colname, n, round(pct, 2)))

    null_summary_df = sqlContext.createDataFrame(null_rows, schema=quality_schema)

    distinct_all = df.dropDuplicates().count()
    duplicate_rows = total_rows - distinct_all
    distinct_ids = df.select("show_id").dropDuplicates().count()
    duplicate_show_ids = total_rows - distinct_ids

    dup_rows = [
        ("TOTAL_ROWS",           int(total_rows),         None),
        ("EXACT_DUPLICATE_ROWS", int(duplicate_rows),     None),
        ("DUPLICATE_SHOW_ID",    int(duplicate_show_ids), None),
    ]
    dup_rows_df = sqlContext.createDataFrame(dup_rows, schema=quality_schema)

    quality_out = null_summary_df.unionByName(dup_rows_df)
    quality_out.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT_QUALITY)

    def parse_date(col):
        pats = ["MMMM d, yyyy", "MMM d, yyyy", "d-MMM-yy", "d-MMM-yyyy", "M/d/yyyy"]
        return F.coalesce(*[F.to_date(F.to_timestamp(col, p)) for p in pats])

    df = (df.withColumn("date_added_dt", parse_date(F.col("date_added")))
            .withColumn("year_added",  F.year("date_added_dt"))
            .withColumn("month_added", F.month("date_added_dt"))
            .filter(F.col("date_added_dt").isNotNull()))

    dur_counts = (df.filter(F.col("duration").isNotNull())
                    .groupBy("type", "duration").count())
    max_counts  = dur_counts.groupBy("type").agg(F.max("count").alias("max_count"))
    dur_mode = (dur_counts.join(max_counts, "type")
                          .filter(F.col("count") == F.col("max_count"))
                          .select("type", F.col("duration").alias("mode_duration"))
                          .dropDuplicates(["type"]))

    df = (df.join(dur_mode, "type", "left")
            .withColumn("duration_filled",
                        F.when(F.col("duration").isNull(), F.col("mode_duration"))
                         .otherwise(F.col("duration")))
            .drop("mode_duration"))

    df = df.withColumn("duration_num",
                       F.regexp_extract("duration_filled", r"(\d+)", 1).cast("int"))

    split = F.split(F.col("listed_in"), "\\s*,\\s*")
    dfg = (df.withColumn("genres", split)
             .withColumn("k", F.size("genres"))
             .withColumn("genre", F.explode("genres"))
             .filter(F.col("genre").isNotNull())
             .withColumn("weight", F.when(F.col("k") > 0, 1.0 / F.col("k")).otherwise(F.lit(0.0))))

    yearly_genre = (dfg.groupBy("year_added", "genre")
                       .agg(F.round(F.sum("weight"), 3).alias("approx_title_count")))

    year_totals = (df.select("show_id", "year_added")
                     .dropDuplicates()
                     .groupBy("year_added")
                     .agg(F.count("*").alias("titles_in_year")))

    yg = yearly_genre.withColumnRenamed("year_added", "yg_year")
    yt = year_totals.withColumnRenamed("year_added", "yt_year")

    yearly_prev = (yg.join(yt, yg.yg_year == yt.yt_year, "inner")
                     .select(F.col("yg_year").alias("year_added"),
                             "genre",
                             "approx_title_count",
                             "titles_in_year")
                     .withColumn("share",
                         F.round(F.col("approx_title_count") / F.col("titles_in_year"), 4)))

    w_year = Window.partitionBy("year_added").orderBy(F.col("approx_title_count").desc(), F.col("genre"))
    yearly_top10 = (yearly_prev.withColumn("rank", F.row_number().over(w_year))
                               .filter(F.col("rank") <= 10)
                               .select("year_added", "rank", "genre", "approx_title_count", "share")
                               .orderBy("year_added", "rank"))

    yearly_top10.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT_YEARLY)

    dfg_dur = dfg.select("show_id", "type", "year_added", "genre", "duration_num")

    movies_trend = (dfg_dur.filter((F.col("type") == "Movie") & F.col("duration_num").isNotNull())
        .groupBy("year_added", "genre")
        .agg(F.countDistinct("show_id").alias("titles"),
             F.round(F.avg("duration_num"), 1).alias("mean_minutes"),
             F.expr("percentile_approx(duration_num, 0.5)").alias("median_minutes"))
        .orderBy("year_added", F.desc("titles"), "genre"))

    movies_trend.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT_MOVIES)

    tv_trend = (dfg_dur.filter((F.col("type") == "TV Show") & F.col("duration_num").isNotNull())
        .groupBy("year_added", "genre")
        .agg(F.countDistinct("show_id").alias("titles"),
             F.round(F.avg("duration_num"), 1).alias("mean_seasons"),
             F.expr("percentile_approx(duration_num, 0.5)").alias("median_seasons"))
        .orderBy("year_added", F.desc("titles"), "genre"))

    tv_trend.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT_TV)

if __name__ == "__main__":
    conf = SparkConf().setAppName("project_test_q2_out")
    sc = SparkContext(conf=conf)
    try:
        main(sc)
    finally:
        sc.stop()
