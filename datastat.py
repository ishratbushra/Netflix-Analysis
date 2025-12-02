
from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F, types as T

INPUT_PATH  = "/user/root/lab/netflix_titles.csv"
OUT_BASE    = "/user/root/lab/project_stats_out"

OUT_OVERVIEW        = OUT_BASE + "/overview_csv"
OUT_COLUMN_SUMMARY  = OUT_BASE + "/column_summary_csv"
OUT_NUM_RELEASEYEAR = OUT_BASE + "/numeric_release_year_csv"
OUT_NUM_MOVIES      = OUT_BASE + "/numeric_movies_minutes_csv"
OUT_NUM_TV          = OUT_BASE + "/numeric_tv_seasons_csv"
OUT_NUM_MEDIANS     = OUT_BASE + "/numeric_medians_csv"
OUT_TOP_RATINGS     = OUT_BASE + "/top10_ratings_csv"
OUT_TOP_GENRES      = OUT_BASE + "/top20_genres_csv"

def main(sc):
    sql = SQLContext(sc)
    spark = sql.sparkSession
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

    df = (sql.read
          .option("header", "true")
          .option("multiLine", "true")
          .option("quote", '"').option("escape", '"')
          .option("mode", "PERMISSIVE")
          .schema(schema)
          .csv(INPUT_PATH))

    def nz(colname):
        c = F.col(colname)
        return F.when(F.length(F.trim(c)) == 0, None).otherwise(F.trim(c))

    for f in df.schema.fields:
        if isinstance(f.dataType, T.StringType):
            df = df.withColumn(f.name, nz(f.name))

    df = df.withColumn("release_year", F.col("release_year").cast("int"))

    df = df.withColumn(
        "duration_num",
        F.when(F.col("duration").isNotNull(),
               F.regexp_extract(F.col("duration"), r"(\d+)", 1).cast("int"))
    )

    total_rows = df.count()
    sql.createDataFrame(
        [("netflix_titles.csv", int(total_rows), len(df.columns))],
        "source string, rows int, cols int"
    ).coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT_OVERVIEW)

    col_rows = []
    for c in df.columns:
        dtype = str(df.schema[c].dataType)
        nulls = df.where(F.col(c).isNull()).count()
        distinct = df.select(c).distinct().count()
        top_row = (df.where(F.col(c).isNotNull())
                    .groupBy(c).count()
                    .orderBy(F.desc("count"))
                    .limit(1).collect())
        top_val, top_cnt = (top_row[0][0], int(top_row[0][1])) if top_row else (None, 0)
        col_rows.append((c, dtype, int(nulls),
                         round(100.0 * nulls / float(max(1, total_rows)), 2),
                         int(distinct),
                         None if top_val is None else str(top_val),
                         int(top_cnt)))
    (sql.createDataFrame(col_rows,
        "column string, dtype string, null_count int, null_pct double, distinct_count int, top_value string, top_count int")
     .coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT_COLUMN_SUMMARY))

    def numeric_stats(df_num, label, col="duration_num"):
        base = df_num.select(col).na.drop()
        cnt = base.count()
        if cnt == 0:
            return sql.createDataFrame(
                [(label, 0, None, None, None, None, None, None, None, None, None, None)],
                "feature string, count int, mean double, sd double, min double, q1 double, median double, q3 double, max double, iqr double, lower_bound double, upper_bound double"
            )
        q1, median, q3 = base.approxQuantile(col, [0.25, 0.5, 0.75], 0.01)
        iqr = q3 - q1
        lb = q1 - 1.5 * iqr
        ub = q3 + 1.5 * iqr
        row = [(
            label,
            int(cnt),
            float(base.select(F.mean(col)).first()[0]),
            float(base.select(F.stddev(col)).first()[0]),
            float(base.select(F.min(col)).first()[0]),
            float(q1),
            float(median),
            float(q3),
            float(base.select(F.max(col)).first()[0]),
            float(iqr), float(lb), float(ub)
        )]
        return sql.createDataFrame(row,
            "feature string, count int, mean double, sd double, min double, q1 double, median double, q3 double, max double, iqr double, lower_bound double, upper_bound double")

    df_ry = df.select(F.col("release_year").alias("num")).na.drop()
    (numeric_stats(df_ry.withColumnRenamed("num", "duration_num"), "release_year", "duration_num")
        .coalesce(1).write.mode("overwrite").option("header","true").csv(OUT_NUM_RELEASEYEAR))

    movies = df.where((F.col("type")=="Movie") & F.col("duration_num").isNotNull())
    (numeric_stats(movies, "movie_duration_minutes", "duration_num")
        .coalesce(1).write.mode("overwrite").option("header","true").csv(OUT_NUM_MOVIES))

    tv = df.where((F.col("type")=="TV Show") & F.col("duration_num").isNotNull())
    (numeric_stats(tv, "tv_seasons", "duration_num")
        .coalesce(1).write.mode("overwrite").option("header","true").csv(OUT_NUM_TV))

    med_rows = []
    med_ry = df_ry.approxQuantile("num", [0.5], 0.01)
    med_rows.append(("Release year", float(med_ry[0]) if med_ry else None))
    med_m = movies.approxQuantile("duration_num", [0.5], 0.01)
    med_rows.append(("Movie duration (minutes)", float(med_m[0]) if med_m else None))
    med_t = tv.approxQuantile("duration_num", [0.5], 0.01)
    med_rows.append(("TV seasons (count)", float(med_t[0]) if med_t else None))

    (sql.createDataFrame(med_rows, "feature string, median double")
        .coalesce(1).write.mode("overwrite").option("header","true").csv(OUT_NUM_MEDIANS))

    (df.groupBy("rating").count().orderBy(F.desc("count")).limit(10)
       .coalesce(1).write.mode("overwrite").option("header","true").csv(OUT_TOP_RATINGS))

    (df.withColumn("genre", F.explode(F.split(F.col("listed_in"), "\\s*,\\s*")))
       .where(F.col("genre").isNotNull())
       .groupBy("genre").count().orderBy(F.desc("count")).limit(20)
       .coalesce(1).write.mode("overwrite").option("header","true").csv(OUT_TOP_GENRES))

if __name__ == "__main__":
    conf = SparkConf().setAppName("project_stats_only")
    sc = SparkContext(conf=conf)
    try:
        main(sc)
    finally:
        sc.stop()
