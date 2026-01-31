from pyspark.sql import SparkSession
import logging
import os
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
import base64

# ================= LOGGING SETUP =================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"sample_sql_json_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.info(f"Starting JSON schema inspection job. Log file: {log_filename}")

# ================= SPARK SESSION =================
spark = SparkSession.builder \
    .appName("SampleJsonAndExplodeFunctions") \
    .getOrCreate()
logger.info("Spark session created")

spark.sparkContext.setLogLevel("ERROR")

def load_base64_json_to_df(json_path: str):

    try:
        logger.info(f"Reading Base64 JSON from: {json_path}")
        with open(json_path, "rb") as f:
            raw_bytes = f.read()

        decoded_bytes = base64.b64decode(raw_bytes)
        decoded_str = decoded_bytes.decode("utf-8")
        parsed_json = json.loads(decoded_str)
        logger.info("Base64 JSON loaded successfully.")

        records = parsed_json.get("data", [])
        if not records:
            raise ValueError("No data records found in JSON")
        logger.info(f"Number of records: {len(records)}")

        # Extract column names from metadata
        columns_meta = parsed_json["meta"]["view"]["columns"]
        column_names = [col["name"] for col in columns_meta]
        schema = StructType([StructField(col, StringType(), True) for col in column_names])

        # Create Spark DataFrame
        df = spark.createDataFrame(records, schema=schema)
        logger.info("DataFrame created successfully.")

        return df

    except Exception as e:
        logger.error(f"Error loading JSON: {e}")
        raise

if __name__ == "__main__":
    json_path = "./data/baby_names.json"

    try:
        # Load JSON as DataFrame
        json_df = load_base64_json_to_df(json_path)
        print(f"Total rows loaded: {json_df.printSchema()}")
        logger.info(f"Total rows loaded: {json_df.count()}")
        json_df.show(5, truncate=True)

        # Create temp view to use DF as temp table for analysis
        json_df.createOrReplaceTempView("baby_names_table")

        # Example SQL query
        spark_sql_df1 = spark.sql(""" SELECT COUNT(*) AS total_counts FROM baby_names_table """)
        spark_sql_df1.show(truncate=False)
        
        logger.info("finding most popular baby name for each year")
        spark_sql_df2 = spark.sql(""" 
        WITH total_counts_data AS (
            SELECT  
                year,
                `First Name` AS name,
                SUM(count) AS total_counts
            FROM    
                baby_names_table
            GROUP BY year, `First Name`
            ),
            ranking_names AS (
            SELECT
                year,
                name,
                ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_counts DESC) AS rnk
            FROM
                total_counts_data
            )
            SELECT
                year,
                name
            FROM
                ranking_names
            WHERE rnk = 1
            ORDER BY year
        """)
        spark_sql_df2.show(100,truncate=False)
        logger.info("Query ran and you can see output in validation script.")

    except Exception as e:
        print("Error loading JSON: {e}")
        logger.error(f"Unexpected error in main execution: {e}")

    finally:
        spark.stop()
        logger.info("Spark session stopped")
