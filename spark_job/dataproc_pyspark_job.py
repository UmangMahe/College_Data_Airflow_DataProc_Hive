import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys

# Initialize Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main(gcs_bucket):
    spark = None
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("DataProcessingandHiveTable") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.sql.warehouse.dir", f"gs://{gcs_bucket}/airflow-project-2/hive_data/") \
            .enableHiveSupport() \
            .getOrCreate()

        logger.info("Spark session initialized.")

        # Resolve GCS path based on the environment
        input_path = f"gs://{gcs_bucket}/airflow-project-2/data"
        logger.info(f"Input path resolved: {input_path}")

        college_data = None
        # Read the data from GCS
        college_data = spark.read.csv(input_path, header=True, inferSchema=True)

        if college_data is None:
            logger.error("Failed to load data. Please check the input path and file extension.")
            sys.exit(1)
        
        logger.info("Data read from GCS.")

        # Data transformations
        logger.info("Starting data transformations.")

        sql_query = "CREATE DATABASE IF NOT EXISTS airflow_assignment_1"
        spark.sql(sql_query)

        #1 Total Placements by College IDs

        total_placements = college_data.groupBy("college_id").agg(
            count(when(col("Placement") == "Yes", 1)).alias("total_placements"), 
            count(when(col("Placement") == "No", 1)).alias("total_non_placements"),
            col("total_placements") / (col("total_placements") + col("total_non_placements")).alias("placement_rate")
        )

        logger.info("Data transformations completed.")

        # Write transformed data to Hive
        
        total_placements.write.format("hive").mode("append").saveAsTable("airflow_assignment_1.total_placements")

        logger.info("Data written to Hive successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

    finally:
        # Stop Spark session
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process Data and write to Hive Table.")
    parser.add_argument("--gcs_bucket", required=True, help="GCS bucket name")

    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(
        gcs_bucket=args.gcs_bucket
    )