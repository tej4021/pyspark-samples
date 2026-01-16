from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from datetime import datetime
import logging

# ================= LOGGING SETUP =================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Variable for log filename – can change per script or per run
log_filename = f"spark_sql_toll_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Full path for the log file
log_path = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create logger
logger = logging.getLogger(__name__)
logger.info(f"Starting Toll Collection PySpark SQL Job. Log file: {log_filename}")

# ================= SPARK SESSION =================
spark = SparkSession.builder.appName("TollCollectionSQLStyle").getOrCreate()

# 2. Dummy data
data = [
    (1,  "DL01AA1111", "car",        "2026-01-10 08:00"),
    (2,  "DL02BB2222", "truck",     "2026-01-10 09:00"),
    (3,  "DL03CC3333", "bus",       "2026-01-10 10:00"),
    (4,  "DL01AA1111", "car",        "2026-01-10 11:00"),
    (5,  "DL04DD4444", "motorcycle","2026-01-10 11:00"),
    (6,  "DL03CC3333", "bus",       "2026-01-10 12:00"),
    (7,  "DL05EE5555", "car",        "2026-01-10 14:00"),
    (8,  "DL02BB2222", "truck",     "2026-01-10 16:00"),
    (9,  "DL01AA1111", "car",        "2026-01-10 18:00"),
    (10, "DL05EE5555", "car",        "2026-01-10 17:00"),
    (11, "DL08HH8888", "car",        "2026-01-11 07:00"),
    (12, "DL06FF6666", "bus",       "2026-01-11 08:00"),
    (13, "DL07GG7777", "truck",     "2026-01-11 09:00"),
    (14, "DL08HH8888", "car",        "2026-01-11 09:00"),
    (15, "DL06FF6666", "bus",       "2026-01-11 10:00"),
    (16, "DL09II9999", "motorcycle","2026-01-11 12:00"),
    (17, "DL07GG7777", "truck",     "2026-01-11 15:00"),
    (18, "DL10JJ0000", "truck",     "2026-01-11 18:00"),
    (19, "DL10JJ0000", "truck",     "2026-01-11 20:00"),
    (20, "DL11KK1111", "car",        "2026-01-11 21:00")
]

schema = StructType([
    StructField("txn_id", IntegerType(), True),
    StructField("vehicle_no", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("crossing_time", StringType(), True)
])

df = spark.createDataFrame(data, schema) \
    .withColumn("crossing_time", to_timestamp("crossing_time"))

df.printSchema()
df.show(truncate=False)
# temp view

df.createOrReplaceTempView("toll_log")
logger.info("TOLL TABLE CREATED WITH SOURCE DATA")

"""
Write a query which calculates the total toll collected per day from vehicle crossing logs by applying vehicle-specific toll rules and a 4-hour discount window.
It orders crossings per vehicle and checks the previous crossing time.
- Motorcycles are always free.
- Cars, buses, and trucks pay a reduced toll if they cross again within 4 hours of their previous crossing. Otherwise, the full toll is charged.
- Finally, it aggregates (sums) the toll amounts by crossing date and returns the daily total toll collected
"""
final_df = spark.sql("""
WITH ordered_logs AS (
    SELECT
        txn_id,
        vehicle_no,
        vehicle_type,
        crossing_time,
        DATE(crossing_time) AS crossing_date,
        LAG(crossing_time) OVER (
            PARTITION BY vehicle_no
            ORDER BY crossing_time
        ) AS prev_crossing_time
    FROM toll_log
),
toll_calculation AS (
    SELECT
        crossing_date,
        CASE
            WHEN vehicle_type = 'motorcycle' THEN 0

            WHEN prev_crossing_time IS NOT NULL
             AND crossing_time <= prev_crossing_time + INTERVAL 4 HOURS
            THEN
                CASE
                    WHEN vehicle_type = 'car' THEN 20
                    WHEN vehicle_type = 'bus' THEN 30
                    WHEN vehicle_type = 'truck' THEN 40
                END

            ELSE
                CASE
                    WHEN vehicle_type = 'car' THEN 40
                    WHEN vehicle_type = 'bus' THEN 70
                    WHEN vehicle_type = 'truck' THEN 80
                END
        END AS toll_amount
    FROM ordered_logs
)

SELECT
    crossing_date,
    SUM(toll_amount) AS total_toll
FROM toll_calculation
GROUP BY crossing_date
ORDER BY crossing_date
""")

logger.info("QUERY TO FIND TOLL PER DAY IS EXECUTED PLEASE LOOK INTO VALIDATION FOLDER")
final_df.show(truncate=False)

logger.info("CLOSING THE SPARK SESSION")
spark.stop()