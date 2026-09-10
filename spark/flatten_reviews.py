import ast
import gzip
import os
from pathlib import Path
import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import traceback

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BUCKET_NAME = "steam-data-pipeline-507614-steam-data-bucket"
PROJECT_ID = "steam-data-pipeline-507614"
BQ_DATASET = "bronze_dataset"


def init_spark():
    spark = (
        SparkSession.builder
        .appName("SteamBronze")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,"
            "com.google.cloud.spark:spark-3.5-bigquery:0.36.0,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22"
        )
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_postgres_games(spark):
    return (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/steam_metadata")
        .option("dbtable", "games")
        .option("user", "root")
        .option("password", "root")
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def load_raw_reviews(spark, file_path):

    reviews_schema = T.StructType([
    T.StructField("date", T.StringType(), True),
    T.StructField("early_access", T.BooleanType(), True),
    T.StructField("hours", T.DoubleType(), True),
    T.StructField("page", T.LongType(), True),
    T.StructField("page_order", T.LongType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("products", T.LongType(), True),
    T.StructField("text", T.StringType(), True),
    T.StructField("username", T.StringType(), True),
    ])

    rdd = (
    spark.sparkContext.textFile(file_path)
    .repartition(64)                    
    .map(safe_parse)                    
    .filter(lambda row: row is not None)
    )

    return spark.createDataFrame(rdd, schema=reviews_schema)



def safe_parse(line):
    line = line.strip()
    if not line:
        return None
    try:
        parsed = ast.literal_eval(line)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def shutdown():
    subprocess.run("taskkill /F /IM java.exe /T", shell=True, capture_output=True)
    os._exit(0)

## {"id": "774276", 
# "url": "http://store.steampowered.com/app/774276/SNOW__All_Access_Basic_Pass/", 
## "tags": ["Free to Play", "Indie", "Simulation", "Sports"], 
## "price": 9.99, 
## "specs": ["Single-player", "Multi-player", "Online Multi-Player", "Cross-Platform Multiplayer", "Downloadable Content", "Steam Achievements", "Full controller support", "Steam Trading Cards", "In-App Purchases", "Steam Cloud", "Steam Leaderboards"], 
## "title": "SNOW - All Access Basic Pass", 
## "genres": ["Free to Play", "Indie", "Simulation", "Sports"], 
## "app_name": "SNOW - All Access Basic Pass", 
## "developer": "Poppermost Productions", 
# "reviews_url": "http://steamcommunity.com/app/774276/reviews/?browsefilter=mostrecent&p=1", 
## "early_access": false, 
## "release_date": "2018-01-04"}

def payload_to_table(postgres_df):

    games_schema = T.StructType([
        T.StructField("id", T.StringType(), True),               
        T.StructField("title", T.StringType(), True),
        T.StructField("app_name", T.StringType(), True),
        T.StructField("developer", T.StringType(), True),
        T.StructField("price", T.StringType(), True),    
        T.StructField("release_date", T.StringType(), True),      
        T.StructField("early_access", T.BooleanType(), True),
        T.StructField("tags", T.ArrayType(T.StringType()), True),
        T.StructField("genres", T.ArrayType(T.StringType()), True),
        T.StructField("specs", T.ArrayType(T.StringType()), True),
        T.StructField("url", T.StringType(), True),
        T.StructField("reviews_url", T.StringType(), True),
    ])
    return postgres_df.select(
        F.from_json(F.col("payload"), games_schema).alias("data")
    ).select("data.*")

def transform_games(spark,games_df):
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    clean_price = F.regexp_replace(F.col("price"), r"[^0-9.]", "")
    games_df = games_df.withColumn(
        "price",
        F.when(F.col("price").ilike("%free%"), F.lit("0.00"))
         .when(clean_price != "", clean_price)
         .otherwise(F.lit(None))
         .cast(T.DecimalType(10, 2))
    )

    is_invalid_title = (
        F.col("title").isNull() | 
        (F.trim(F.col("title")) == "") | 
        F.col("title").ilike("null")
    )
    games_df = games_df.withColumn(
        "title",
        F.when(is_invalid_title, F.col("app_name")).otherwise(F.col("title"))
    )

    games_df = games_df.withColumn(
        "release_date",
        F.coalesce(
            F.to_date(F.col("release_date"), "yyyy-MM-dd"),
            F.to_date(F.col("release_date"), "MMM d, yyyy"),
            F.to_date(F.concat(F.col("release_date"), F.lit(" 01")), "MMM yyyy dd"),
            F.to_date(F.concat(F.col("release_date"), F.lit(" 01")), "MMMM yyyy dd"),
            F.to_date(F.col("release_date"), "d MMM, yyyy"),
            F.to_date(F.col("release_date"), "dd.MM.yyyy")
        )
    )

    games_df = games_df.withColumn("id", F.trim(F.col("id")))
    games_df = games_df.dropDuplicates(["id"])
    games_df = games_df.filter(F.col("id").isNotNull())

    games_df.printSchema()

    return games_df

def transform_reviews(spark,reviews_df):
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    is_valid_text = F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0)

    reviews_df = reviews_df.filter(is_valid_text)

    reviews_df = reviews_df.dropDuplicates(["username","product_id"])

    reviews_df = reviews_df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    reviews_df = reviews_df.withColumn("page", F.col("page").cast("int"))
    reviews_df = reviews_df.withColumn("page_order", F.col("page_order").cast("int"))
    reviews_df = reviews_df.withColumn("products", F.col("products").cast("int"))
    reviews_df = reviews_df.withColumn("hours", F.col("hours").cast("float"))
    reviews_df = reviews_df.withColumn("product_id", F.trim(F.col("product_id")))
    reviews_df = reviews_df.withColumn("username", F.trim(F.col("username")))
    reviews_df.printSchema()

    return reviews_df


def main():
    spark = init_spark()

    try:
        postgres_df = load_postgres_games(spark)

        review_file_path = f"gs://{BUCKET_NAME}/raw/steam_reviews.json.gz"

        reviews_df = load_raw_reviews(spark, review_file_path)
        games_df = payload_to_table(postgres_df)
        games_df = transform_games(spark,games_df)

        games_df.write \
            .format("bigquery") \
            .option("table", f"{PROJECT_ID}.{BQ_DATASET}.games") \
            .option("temporaryGcsBucket", BUCKET_NAME) \
            .mode("overwrite") \
            .save()

        reviews_df.write \
            .format("bigquery") \
            .option("table", f"{PROJECT_ID}.{BQ_DATASET}.reviews") \
            .option("temporaryGcsBucket", BUCKET_NAME) \
            .mode("overwrite") \
            .save()



        


    except Exception as e:
        print("\n" + "="*50)
        print("ERROR:")
        traceback.print_exc()
        print("="*50 + "\n")
    finally:
        shutdown()


if __name__ == "__main__":
    main()