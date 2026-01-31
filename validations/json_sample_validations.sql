"""
Validation : Load and analyze Base64-encoded JSON data for baby names.
Description : Script that reads a Base64-encoded JSON file, decodes it, and converts it into a Spark DataFrame using schema inferred from Socrata-style metadata.
It allows analysis both with PySpark DataFrame API and Spark SQL.
- Explodes nested fields if needed (currently flat JSON arrays under 'data').
- Provides functionality to count rows, show schema, and preview data.
- Supports querying popular baby names per year using SQL with window functions (ROW_NUMBER for ranking).
- Designed for exploratory data analysis and ETL workflow validation of NoSQL/document-style datasets.
"""

-- Schema of JSON FILE after reading
root
 |-- sid: string (nullable = true)
 |-- id: string (nullable = true)
 |-- position: string (nullable = true)
 |-- created_at: string (nullable = true)
 |-- created_meta: string (nullable = true)
 |-- updated_at: string (nullable = true)
 |-- updated_meta: string (nullable = true)
 |-- meta: string (nullable = true)
 |-- Year: string (nullable = true)
 |-- First Name: string (nullable = true)
 |-- County: string (nullable = true)
 |-- Sex: string (nullable = true)
 |-- Count: string (nullable = true)

-- Total number of records
Total rows loaded: 87899

-- top 5 records
+------------------+--------------------+--------+----------+------------+----------+------------+----+----+----------+------+---+-----+
|               sid|                  id|position|created_at|created_meta|updated_at|updated_meta|meta|Year|First Name|County|Sex|Count|
+------------------+--------------------+--------+----------+------------+----------+------------+----+----+----------+------+---+-----+
|row-brkm-7izk-trjm|00000000-0000-000...|       0|1611674742|        NULL|1611674742|        NULL| { }|2018|    OLIVIA|Albany|  F|   17|
|row-2m5x_rpr2.gwvc|00000000-0000-000...|       0|1611674742|        NULL|1611674742|        NULL| { }|2018|       AVA|Albany|  F|   17|
|row-xcx9~hw65_ib5p|00000000-0000-000...|       0|1611674742|        NULL|1611674742|        NULL| { }|2018|  ISABELLA|Albany|  F|   15|
|row-684m~4agu.tevb|00000000-0000-000...|       0|1611674742|        NULL|1611674742|        NULL| { }|2018|      EMMA|Albany|  F|   14|
|row-jgz3~57z7_bxvk|00000000-0000-000...|       0|1611674742|        NULL|1611674742|        NULL| { }|2018|      ELLA|Albany|  F|   14|
+------------------+--------------------+--------+----------+------------+----------+------------+----+----+----------+------+---+-----+
only showing top 5 rows

'''
NOTE : OUTPUT FOR SPARK SQL TABLE - BABY_NAME_TABLE WE CREATED IN JOB
'''
-- QUERY 1 : Number of records present in temp table : baby_name_table
+-----------+
|total_count|
+-----------+
|87899      |
+-----------+

-- QUERY 2 : finding most popular baby name for each year
+----+--------+
|year|name    |
+----+--------+
|2007|MICHAEL |
|2008|MICHAEL |
|2009|MICHAEL |
|2010|ISABELLA|
|2011|MICHAEL |
|2012|SOPHIA  |
|2013|SOPHIA  |
|2014|JACOB   |
|2015|LIAM    |
|2016|LIAM    |
|2017|LIAM    |
|2018|LIAM    |
|2019|LIAM    |
|2020|LIAM    |
+----+--------+