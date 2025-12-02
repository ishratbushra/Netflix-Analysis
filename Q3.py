import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

spark = (
    SparkSession.builder
    .appName("DS8003_Project_Q3")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

INPUT = "hdfs:///user/maria_dev/Q3/netflix_titles.csv"
OUTPUT_DIR = "hdfs:///user/maria_dev/Q3/q3_country_contribution_out"

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

df = (
    spark.read.option('header', 'true')
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .schema(schema)
    .csv(INPUT)
)

def clean_str(c):
    return F.when(F.length(F.trim(F.col(c))) ==0, None).otherwise(F.trim(F.col(c)))

for c in [f.name for f in df.schema.fields if isinstance(f.dataType,T.StringType)]:
    df= df.withColumn(c, clean_str(c))

df =df.withColumn("country", F.when(F.col("country").isNull(), "Unknown").otherwise(F.col("country")))
df_valid= df.filter(F.col("type").isin("Movie", "TV Show"))
df_exploded = df_valid.withColumn("country", F.explode(F.split(F.col("country"), ",\\s*")))

country_counts= (
    df_exploded.groupBy('country')
    .agg(F.count("*").alias("total_titles"))
    .orderBy(F.desc('total_titles'))
)
country_counts.write.mode("overwrite").option('header', 'true').csv(OUTPUT_DIR + "/country_contribution")
country_counts.show(10,truncate = False)

country_by_type=(
    df_exploded.groupBy('country', "type")
    .agg(F.count("*").alias("total_titles"))
    .orderBy(F.desc("total_titles"))
)
country_by_type.write.mode("overwrite").option("header", "true").csv(OUTPUT_DIR + "/country_by_type")
country_by_type.show(10, truncate = False)

spark.stop()
