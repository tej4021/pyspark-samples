"""
Validation : SCD 2 (Slowly Changing Dimension)
Description : Updates a customer dimension table using SCD Type 2 in PySpark, tracking changes and preserving historical records.
"""

-- ==== INITIAL DATA/SOURCE DATA ====
+-----------+-----+--------+---------------+----------+----------+----------+
|customer_id|name |city    |email          |start_date|end_date  |is_current|
+-----------+-----+--------+---------------+----------+----------+----------+
|1          |John |New York|john@gmail.com |2024-01-01|9999-12-31|Y         |
|2          |Alice|Chicago |alice@gmail.com|2024-01-01|9999-12-31|Y         |
|3          |Bob  |Seattle |bob@gmail.com  |2024-01-01|9999-12-31|Y         |
|4          |Emma |Boston  |emma@gmail.com |2024-01-01|9999-12-31|Y         |
|5          |Mike |Dallas  |mike@gmail.com |2024-01-01|9999-12-31|Y         |
|6          |Sara |Austin  |sara@gmail.com |2024-01-01|9999-12-31|Y         |
+-----------+-----+--------+---------------+----------+----------+----------+

-- ==== INCOMING SOURCE DATA/NEW LOAD ====
+-----------+-----+-------+-------------------+-----------+
|customer_id|name |city   |email              |update_date|
+-----------+-----+-------+-------------------+-----------+
|1          |John |Boston |john@gmail.com     |2024-02-01 |
|2          |Alice|Chicago|alice_new@gmail.com|2024-02-01 |
|3          |Bob  |Seattle|bob@gmail.com      |2024-02-01 |
|5          |Mike |Dallas |mike_new@gmail.com |2024-02-01 |
|7          |Tom  |Denver |tom@gmail.com      |2024-02-01 |
+-----------+-----+-------+-------------------+-----------+

-- ==== UPDATED RECORDS ====
+-----------+-----+-------+-------------------+
|customer_id|name |city   |email              |
+-----------+-----+-------+-------------------+
|1          |John |Boston |john@gmail.com     |
|2          |Alice|Chicago|alice_new@gmail.com|
|5          |Mike |Dallas |mike_new@gmail.com |
+-----------+-----+-------+-------------------+

-- ==== EXPIRED RECORDS WITH FLAG N ====
+-----------+-----+--------+---------------+----------+----------+----------+
|customer_id|name |city    |email          |start_date|end_date  |is_current|
+-----------+-----+--------+---------------+----------+----------+----------+
|1          |John |New York|john@gmail.com |2024-01-01|2024-02-01|N         |
|2          |Alice|Chicago |alice@gmail.com|2024-01-01|2024-02-01|N         |
|5          |Mike |Dallas  |mike@gmail.com |2024-01-01|2024-02-01|N         |
+-----------+-----+--------+---------------+----------+----------+----------+

-- ==== NEW RECORDS TO BE LOADED/UPDATED WITH FLAG Y ====
+-----------+-----+-------+-------------------+----------+----------+----------+
|customer_id|name |city   |email              |start_date|end_date  |is_current|
+-----------+-----+-------+-------------------+----------+----------+----------+
|1          |John |Boston |john@gmail.com     |2024-02-01|9999-12-31|Y         |
|2          |Alice|Chicago|alice_new@gmail.com|2024-02-01|9999-12-31|Y         |
|3          |Bob  |Seattle|bob@gmail.com      |2024-02-01|9999-12-31|Y         |
|5          |Mike |Dallas |mike_new@gmail.com |2024-02-01|9999-12-31|Y         |
|7          |Tom  |Denver |tom@gmail.com      |2024-02-01|9999-12-31|Y         |
+-----------+-----+-------+-------------------+----------+----------+----------+

-- ==== UNCHANGED TARGET RECORDS ====
+-----------+----+------+--------------+----------+----------+----------+
|customer_id|name|city  |email         |start_date|end_date  |is_current|
+-----------+----+------+--------------+----------+----------+----------+
|4          |Emma|Boston|emma@gmail.com|2024-01-01|9999-12-31|Y         |
|6          |Sara|Austin|sara@gmail.com|2024-01-01|9999-12-31|Y         |
+-----------+----+------+--------------+----------+----------+----------+

-- ==== FINAL DIMENSION TABLE (SCD TYPE 2) ====
+-----------+-----+--------+-------------------+----------+----------+----------+
|customer_id|name |city    |email              |start_date|end_date  |is_current|
+-----------+-----+--------+-------------------+----------+----------+----------+
|1          |John |New York|john@gmail.com     |2024-01-01|2024-02-01|N         |
|1          |John |Boston  |john@gmail.com     |2024-02-01|9999-12-31|Y         |
|2          |Alice|Chicago |alice@gmail.com    |2024-01-01|2024-02-01|N         |
|2          |Alice|Chicago |alice_new@gmail.com|2024-02-01|9999-12-31|Y         |
|3          |Bob  |Seattle |bob@gmail.com      |2024-02-01|9999-12-31|Y         |
|4          |Emma |Boston  |emma@gmail.com     |2024-01-01|9999-12-31|Y         |
|5          |Mike |Dallas  |mike@gmail.com     |2024-01-01|2024-02-01|N         |
|5          |Mike |Dallas  |mike_new@gmail.com |2024-02-01|9999-12-31|Y         |
|6          |Sara |Austin  |sara@gmail.com     |2024-01-01|9999-12-31|Y         |
|7          |Tom  |Denver  |tom@gmail.com      |2024-02-01|9999-12-31|Y         |
+-----------+-----+--------+-------------------+----------+----------+----------+
