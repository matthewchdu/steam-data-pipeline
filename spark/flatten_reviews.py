import ast
import gzip
import os
from pathlib import Path
import subprocess
import sys

#Pyspark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Error Tracing
import traceback

# Points spark's driver and executor to current python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Constant variables, must be changed
BUCKET_NAME = "steam-data-pipeline-507614-steam-data-bucket"
PROJECT_ID = "steam-data-pipeline-507614"
BQ_DATASET = "bronze_dataset"
IP = "34.173.54.255"

# Sets up spark for steam bronze
def init_spark():
    spark = SparkSession.builder.appName("SteamBronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Connects to the Postgre database using JDBC in the cloud and loads the database
def load_postgres_games(spark):
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{IP}:5432/steam_metadata")
        .option("dbtable", "games")
        .option("user", "postgres")
        .option("password", "root")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

# Loads reviews file from GCS bucket and parses it
def load_raw_reviews(spark, file_path):
    # Sets the schema
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

    #RDD necessary since the file is not in valid JSON
    #The file is in Python2 format
    rdd = (
    spark.sparkContext.textFile(file_path)
    .repartition(64) #Allows 64 cores to process each safe_parse        
    .map(safe_parse) # .map = runs it on each line
    .filter(lambda row: row is not None) #Empty filter
    )

    # Converts RDD into DF following the target schema
    return spark.createDataFrame(rdd, schema=reviews_schema)

def safe_parse(line):
    line = line.strip()

    if not line:
        #Returns None if line is empty
        return None
    try:
        # json.loads() doesn't work here, ast.literal_eval needed to turn it into a Python dictionary
        parsed = ast.literal_eval(line)
        # If the record isn't corructed it returns the data
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None

def payload_to_table(postgres_df):
    games_schema = T.StructType([
        T.StructField("id", T.StringType(), True),               
        T.StructField("title", T.StringType(), True),
        T.StructField("app_name", T.StringType(), True),
        T.StructField("developer", T.StringType(), True),
        T.StructField("price", T.StringType(), True),    
        T.StructField("release_date", T.StringType(), True),      
        T.StructField("early_access", T.BooleanType(), True),
        T.StructField("url", T.StringType(), True),
        T.StructField("reviews_url", T.StringType(), True),
        T.StructField("tags", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
        T.StructField("specs", T.StringType(), True),
    ])

    return postgres_df.select(
        F.from_json(F.col("payload"), games_schema).alias("data")
    ).select("data.*")

# Cleaning up the data in the "games" table
def transform_games(spark,games_df):
    # Necessary confirm for coalesce logic
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    for col_name in ["tags", "genres", "specs"]:
            # F.translate is bulletproof. It universally deletes these 4 characters without regex confusion.
            cleaned = F.translate(F.col(col_name), "[]'\"", "")
            trimmed = F.trim(cleaned)

            games_df = games_df.withColumn(
                col_name,
                F.when(
                    trimmed.isNull() | (trimmed == "") | (trimmed.ilike("null")),
                    F.array().cast(T.ArrayType(T.StringType()))
                ).otherwise(
                    # Splits on commas and strips any whitespace around them
                    F.array_remove(F.split(trimmed, r"\s*,\s*"), "")
                )
            )
    # Nulls invalid data, all "free" values are turned to decimal
    clean_price = F.regexp_replace(F.col("price"), r"[^0-9.]", "")
    games_df = games_df.withColumn(
        "price",
        F.when(F.col("price").ilike("%free%"), F.lit("0.00"))
         .when(clean_price != "", clean_price)
         .otherwise(F.lit(None))
         .cast(T.DecimalType(10, 2))
    )

    # Turns null titles into app_name if it has it
    is_invalid_title = (
        F.col("title").isNull() | 
        (F.trim(F.col("title")) == "") | 
        F.col("title").ilike("null")
    )
    games_df = games_df.withColumn(
        "title",
        F.when(is_invalid_title, F.col("app_name")).otherwise(F.col("title"))
    )

    #Parses release_dates, only allows precise data
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
    games_df = games_df.withColumn("release_date", F.col("release_date").cast(T.StringType()))
    games_df = games_df.withColumn("price", F.col("price").cast(T.StringType()))

    #Further filtering, deleting duplicate and null entries
    games_df = games_df.withColumn("id", F.trim(F.col("id")))
    games_df = games_df.dropDuplicates(["id"])
    games_df = games_df.filter(F.col("id").isNotNull())

    return games_df

#Cleans up the data in the "reviews" table
def transform_reviews(spark,reviews_df):
    # Necessary confirm for coalesce logic
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    #Gets rid of empty reviews
    is_valid_text = F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0)
    reviews_df = reviews_df.filter(is_valid_text)

    # Gets rid of duplicates
    reviews_df = reviews_df.dropDuplicates(["username","product_id"])

    #Casts the data
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

        # Load and tranform "reviews" table
        review_file_path = f"gs://{BUCKET_NAME}/steam_reviews.json.gz"
        reviews_df = load_raw_reviews(spark, review_file_path)
        reviews_df = transform_reviews(spark,reviews_df)

        # Load and transform "games" table
        postgres_df = load_postgres_games(spark)
        games_df = payload_to_table(postgres_df)
        games_df = transform_games(spark,games_df)

        # Write "games" to BigQuery
        games_df.write \
            .format("bigquery") \
            .option("table", f"{PROJECT_ID}.{BQ_DATASET}.games") \
            .option("temporaryGcsBucket", BUCKET_NAME) \
            .option("intermediateFormat", "orc") \
            .mode("overwrite") \
            .save()

        # Write "reviews" to BigQuery
        reviews_df.write \
            .format("bigquery") \
            .option("table", f"{PROJECT_ID}.{BQ_DATASET}.reviews") \
            .option("temporaryGcsBucket", BUCKET_NAME) \
            .option("intermediateFormat", "orc") \
            .mode("overwrite") \
            .save()

    # Used for debugging
    except Exception as e:
        print("\n" + "="*50)
        print("ERROR:")
        traceback.print_exc()
        print("="*50 + "\n")

    # Termination
    finally:
        spark.stop()


if __name__ == "__main__":
    main()