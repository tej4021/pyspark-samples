from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import logging
import os
from datetime import datetime

# ================= LOGGING SETUP =================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Variable for log filename – can change per script or per run
log_filename = f"nosql_sample_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
spark = SparkSession.builder \
    .appName("DocumentToNormalizedDF") \
    .getOrCreate()
logger.info("Spark session created")

# Sample Document-Based (NoSQL-style) Data
data = [
    {
        "order_id": "O1001",
        "order_date": "2025-01-10",
        "customer": {
            "customer_id": "C001",
            "name": "Alice",
            "email": "alice@example.com"
        },
        "items": [
            {"product_id": "P01", "product_name": "Laptop", "price": 1200, "quantity": 1},
            {"product_id": "P02", "product_name": "Mouse", "price": 25, "quantity": 2}
        ]
    },
    {
        "order_id": "O1002",
        "order_date": "2025-01-11",
        "customer": {
            "customer_id": "C002",
            "name": "Bob",
            "email": "bob@example.com"
        },
        "items": [
            {"product_id": "P03", "product_name": "Keyboard", "price": 75, "quantity": 1}
        ]
    }
]

# Create main DataFrame (document style)
doc_df = spark.createDataFrame(data)
logger.info("Source DataFrame created with %d records", doc_df.count())
logger.info("Schema of Sample Document File : ",doc_df.printSchema())

print("DOCUMENT DATA LOADED")
doc_df.show(truncate=False)

# Create CUSTOMER DataFrame
customers_df = doc_df.select(
    col("customer.customer_id").alias("customer_id"),
    col("customer.name").alias("customer_name"),
    col("customer.email").alias("customer_email")
)
logger.info("Target Customer DataFrame created with %d records", customers_df.count())

print("CUSTOMERS DATAFRAME CREATED")
customers_df.show(truncate=False)

# Create ORDERS DataFrame
orders_df = doc_df.select(
    col("order_id"),
    col("order_date"),
    col("customer.customer_id").alias("customer_id")
)
logger.info("Target Order DataFrame created with %d records", orders_df.count())

print("ORDERS DATAFRAME CREATED")
orders_df.show(truncate=False)

# Create ORDER ITEMS DataFrame
order_items_df = doc_df.select(
    col("order_id"),
    explode("items").alias("item")
).select(
    col("order_id"),
    col("item.product_id").alias("product_id"),
    col("item.product_name").alias("product_name"),
    col("item.price").alias("price"),
    col("item.quantity").alias("quantity")
)
logger.info("Target Order Item DataFrame created with %d records", order_items_df.count())

print("ORDER ITEMS DATAFRAME CREATED")
order_items_df.show(truncate=False)

spark.stop()
