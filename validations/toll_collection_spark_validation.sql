"""
Validation : Query to calculate the toll for daily basis.
Description : Query which calculates the total toll collected per day from vehicle crossing logs by applying vehicle-specific toll rules and a 4-hour discount window.
It orders crossings per vehicle and checks the previous crossing time.
- Motorcycles are always free.
- Cars, buses, and trucks pay a reduced toll if they cross again within 4 hours of their previous crossing. Otherwise, the full toll is charged.
- Finally, it aggregates (sums) the toll amounts by crossing date and returns the daily total toll collected
"""

-- Schema of incoming data/Sample data.
root
 |-- txn_id: integer (nullable = true)
 |-- vehicle_no: string (nullable = true)
 |-- vehicle_type: string (nullable = true)
 |-- crossing_time: timestamp (nullable = true)
 
-- Sample data looks like this
+------+----------+------------+-------------------+
|txn_id|vehicle_no|vehicle_type|crossing_time      |
+------+----------+------------+-------------------+
|1     |DL01AA1111|car         |2026-01-10 08:00:00|
|2     |DL02BB2222|truck       |2026-01-10 09:00:00|
|3     |DL03CC3333|bus         |2026-01-10 10:00:00|
|4     |DL01AA1111|car         |2026-01-10 11:00:00|
|5     |DL04DD4444|motorcycle  |2026-01-10 11:00:00|
|6     |DL03CC3333|bus         |2026-01-10 12:00:00|
|7     |DL05EE5555|car         |2026-01-10 14:00:00|
|8     |DL02BB2222|truck       |2026-01-10 16:00:00|
|9     |DL01AA1111|car         |2026-01-10 18:00:00|
|10    |DL05EE5555|car         |2026-01-10 17:00:00|
|11    |DL08HH8888|car         |2026-01-11 07:00:00|
|12    |DL06FF6666|bus         |2026-01-11 08:00:00|
|13    |DL07GG7777|truck       |2026-01-11 09:00:00|
|14    |DL08HH8888|car         |2026-01-11 09:00:00|
|15    |DL06FF6666|bus         |2026-01-11 10:00:00|
|16    |DL09II9999|motorcycle  |2026-01-11 12:00:00|
|17    |DL07GG7777|truck       |2026-01-11 15:00:00|
|18    |DL10JJ0000|truck       |2026-01-11 18:00:00|
|19    |DL10JJ0000|truck       |2026-01-11 20:00:00|
|20    |DL11KK1111|car         |2026-01-11 21:00:00|
+------+----------+------------+-------------------+

-- Query Output gives this
+-------------+----------+
|crossing_date|total_toll|
+-------------+----------+
|2026-01-10   |420       |
|2026-01-11   |480       |
+-------------+----------+