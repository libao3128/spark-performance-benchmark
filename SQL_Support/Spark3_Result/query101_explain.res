Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583527745
== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Project ['d_date, 'make_date(2022, 'd_moy, 'd_dom) AS new_date_format#0, 'timestamp_millis('unix_timestamp()) AS ts_millis#1]
      +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
d_date: string, new_date_format: date, ts_millis: timestamp
GlobalLimit 10
+- LocalLimit 10
   +- Project [d_date#9, make_date(2022, d_moy#15, d_dom#16, false) AS new_date_format#0, timestamp_millis(unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss, Some(America/Los_Angeles), false)) AS ts_millis#1]
      +- SubqueryAlias spark_catalog.tpcds.date_dim
         +- Relation spark_catalog.tpcds.date_dim[d_date_sk#7,d_date_id#8,d_date#9,d_month_seq#10,d_week_seq#11,d_quarter_seq#12,d_year#13,d_dow#14,d_moy#15,d_dom#16,d_qoy#17,d_fy_year#18,d_fy_quarter_seq#19,d_fy_week_seq#20,d_day_name#21,d_quarter_name#22,d_holiday#23,d_weekend#24,d_following_holiday#25,d_first_dom#26,d_last_dom#27,d_same_day_ly#28,d_same_day_lq#29,d_current_day#30,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Project [d_date#9, make_date(2022, d_moy#15, d_dom#16, false) AS new_date_format#0, 1970-01-20 19:46:23.535 AS ts_millis#1]
      +- Relation spark_catalog.tpcds.date_dim[d_date_sk#7,d_date_id#8,d_date#9,d_month_seq#10,d_week_seq#11,d_quarter_seq#12,d_year#13,d_dow#14,d_moy#15,d_dom#16,d_qoy#17,d_fy_year#18,d_fy_quarter_seq#19,d_fy_week_seq#20,d_day_name#21,d_quarter_name#22,d_holiday#23,d_weekend#24,d_following_holiday#25,d_first_dom#26,d_last_dom#27,d_same_day_ly#28,d_same_day_lq#29,d_current_day#30,... 4 more fields] parquet

== Physical Plan ==
CollectLimit 10
+- *(1) Project [d_date#9, make_date(2022, d_moy#15, d_dom#16, false) AS new_date_format#0, 1970-01-20 19:46:23.535 AS ts_millis#1]
   +- *(1) ColumnarToRow
      +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#9,d_moy#15,d_dom#16] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<d_date:string,d_moy:int,d_dom:int>

Time taken: 1.565 seconds, Fetched 1 row(s)
