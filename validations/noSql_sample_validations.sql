"""
Validation : Document Based(NoSql with Spark) 2 (Slowly Changing Dimension)
Description : Process document based data and un-nest it or explode it and make it readable.
"""

-- Schema of Coming document
root
 |-- customer: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- items: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: string (valueContainsNull = true)
 |-- order_date: string (nullable = true)
 |-- order_id: string (nullable = true)
 
-- DOCUMENT DATA LOADED
+----------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+----------+--------+
|customer                                                        |items                                                                                                                                              |order_date|order_id|
+----------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+----------+--------+
|{name -> Alice, customer_id -> C001, email -> alice@example.com}|[{quantity -> 1, product_name -> Laptop, price -> 1200, product_id -> P01}, {quantity -> 2, product_name -> Mouse, price -> 25, product_id -> P02}]|2025-01-10|O1001   |
|{name -> Bob, customer_id -> C002, email -> bob@example.com}    |[{quantity -> 1, product_name -> Keyboard, price -> 75, product_id -> P03}]                                                                        |2025-01-11|O1002   |
+----------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+----------+--------+

-- CUSTOMERS DATAFRAME CREATED
+-----------+-------------+-----------------+
|customer_id|customer_name|customer_email   |
+-----------+-------------+-----------------+
|C001       |Alice        |alice@example.com|
|C002       |Bob          |bob@example.com  |
+-----------+-------------+-----------------+

-- ORDERS DATAFRAME CREATED
+--------+----------+-----------+
|order_id|order_date|customer_id|
+--------+----------+-----------+
|O1001   |2025-01-10|C001       |
|O1002   |2025-01-11|C002       |
+--------+----------+-----------+

-- ORDER ITEMS DATAFRAME CREATED
+--------+----------+------------+-----+--------+
|order_id|product_id|product_name|price|quantity|
+--------+----------+------------+-----+--------+
|O1001   |P01       |Laptop      |1200 |1       |
|O1001   |P02       |Mouse       |25   |2       |
|O1002   |P03       |Keyboard    |75   |1       |
+--------+----------+------------+-----+--------+