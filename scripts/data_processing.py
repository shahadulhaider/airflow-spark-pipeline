from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import sys
import os

def process_book_data(input_path, output_path):
    spark = SparkSession.builder.appName("BookDataTransformation").getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    transformed_df = df.select(
        col("key").alias("book_key"),
        col("title"),
        when(col("author_name").isNotNull(), col("author_name")).otherwise(lit("Unknown")).alias("authors"),
        col("first_publish_year").cast("integer").alias("publish_year"),
        when(col("language").isNotNull(), col("language")).otherwise(lit("Unknown")).alias("languages"),
        col("number_of_pages_median").cast("integer").alias("pages")
    )

    transformed_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    # Rename the output file
    output_files = [f for f in os.listdir(output_path) if f.startswith("part-")]
    if output_files:
        os.rename(os.path.join(output_path, output_files[0]), os.path.join(output_path, "transformed_book_data.csv"))

    spark.stop()

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    process_book_data(input_path, output_path)
