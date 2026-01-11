from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws
from datetime import datetime
import logging
import os

# ================= LOGGING SETUP =================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Variable for log filename – can change per script or per run
log_filename = f"scd_type2_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Full path for the log file
log_path = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create logger
logger = logging.getLogger(__name__)
logger.info(f"Starting SCD Type 2 PySpark Job. Log file: {log_filename}")

# ================= SPARK SESSION =================
spark = SparkSession.builder.appName("SCD_Type_2_Example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logger.info("Spark session created")

# ================= TARGET DATA =================
target_data = [
    (1, "John",  "New York", "john@gmail.com",  "2024-01-01", "9999-12-31", "Y"),
    (2, "Alice", "Chicago",  "alice@gmail.com", "2024-01-01", "9999-12-31", "Y"),
    (3, "Bob",   "Seattle",  "bob@gmail.com",   "2024-01-01", "9999-12-31", "Y"),
    (4, "Emma",  "Boston",   "emma@gmail.com",  "2024-01-01", "9999-12-31", "Y"),
    (5, "Mike",  "Dallas",   "mike@gmail.com",  "2024-01-01", "9999-12-31", "Y"),
    (6, "Sara",  "Austin",   "sara@gmail.com",  "2024-01-01", "9999-12-31", "Y")
]

target_cols = ["customer_id", "name", "city", "email", "start_date", "end_date", "is_current"]
target_df = spark.createDataFrame(target_data, target_cols)

print("==== TABLE CREATED ====")
target_df.show(truncate=False)

logger.info("Target table created with %d records", target_df.count())

# ================= SOURCE DATA =================
source_data = [
    (1, "John",  "Boston",   "john@gmail.com",       "2024-02-01"),
    (2, "Alice", "Chicago",  "alice_new@gmail.com",  "2024-02-01"),
    (3, "Bob",   "Seattle",  "bob@gmail.com",        "2024-02-01"),
    (5, "Mike",  "Dallas",   "mike_new@gmail.com",   "2024-02-01"),
    (7, "Tom",   "Denver",   "tom@gmail.com",        "2024-02-01")
]

source_cols = ["customer_id", "name", "city", "email", "update_date"]
source_df = spark.createDataFrame(source_data, source_cols)

print("==== INCOMING SOURCE DATA ====")
source_df.show(truncate=False)

logger.info("Source data loaded with %d records", source_df.count())

# ================= HASHING =================
scd_columns = ["name", "city", "email"]

target_hashed = target_df.withColumn(
    "hash_value",
    sha2(concat_ws("||", *[col(c) for c in scd_columns]), 256)
)

source_hashed = source_df.withColumn(
    "hash_value",
    sha2(concat_ws("||", *[col(c) for c in scd_columns]), 256)
)

# ================= CHANGED RECORDS =================
changed_df = source_hashed.alias("s").join(
    target_hashed.alias("t"),
    (col("s.customer_id") == col("t.customer_id")) &
    (col("t.is_current") == "Y") &
    (col("s.hash_value") != col("t.hash_value")),
    "inner"
)

print("==== CHANGED RECORDS ====")
changed_df.select("s.customer_id", "s.name", "s.city", "s.email").show(truncate=False)

logger.info("Changed records count: %d", changed_df.count())

# ================= EXPIRED RECORDS =================
expired_df = changed_df.select(
    col("t.customer_id"),
    col("t.name"),
    col("t.city"),
    col("t.email"),
    col("t.start_date"),
    col("s.update_date").alias("end_date"),
    lit("N").alias("is_current")
)

print("==== EXPIRED RECORDS ====")
expired_df.show(truncate=False)

# ================= NEW RECORDS =================
new_records_df = source_df.select(
    col("customer_id"),
    col("name"),
    col("city"),
    col("email"),
    col("update_date").alias("start_date"),
    lit("9999-12-31").alias("end_date"),
    lit("Y").alias("is_current")
)

print("==== NEW RECORDS ====")
new_records_df.show(truncate=False)

# ================= UNCHANGED RECORDS =================
unchanged_target_df = target_df.alias("t").join(
    source_df.alias("s"),
    (col("t.customer_id") == col("s.customer_id")) &
    (col("t.is_current") == "Y"),
    "left_anti"
)

print("==== UNCHANGED TARGET RECORDS ====")
unchanged_target_df.show(truncate=False)

# ================= FINAL TABLE =================
final_df = unchanged_target_df.union(expired_df).union(new_records_df)

print("==== FINAL DIMENSION TABLE (SCD TYPE 2) ====")
final_df.orderBy("customer_id", "start_date").show(truncate=False)

logger.info("Final dimension count: %d", final_df.count())
logger.info("SCD Type 2 job completed successfully")

spark.stop()
logger.info("Spark session stopped")
