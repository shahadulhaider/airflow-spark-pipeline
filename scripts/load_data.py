from pyspark.sql import SparkSession
import os
import sys


def load_book_data(input_path):
    jdbc_jar_path = os.path.join(os.environ.get('PROJECT_ROOT', ''), 'jars', 'postgresql-42.7.3.jar')

    spark = SparkSession.builder \
        .appName("BookDataLoading") \
        .config("spark.jars", jdbc_jar_path) \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
        .option("dbtable", "open_library_books") \
        .option("user", "msh") \
        .option("password", "habijabi") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()

if __name__ == "__main__":
    input_path = sys.argv[1]
    load_book_data(input_path)
