Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581223984
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_store_name ASC NULLS FIRST, 's_store_id ASC NULLS FIRST, 'sun_sales ASC NULLS FIRST, 'mon_sales ASC NULLS FIRST, 'tue_sales ASC NULLS FIRST, 'wed_sales ASC NULLS FIRST, 'thu_sales ASC NULLS FIRST, 'fri_sales ASC NULLS FIRST, 'sat_sales ASC NULLS FIRST], true
      +- 'Aggregate ['s_store_name, 's_store_id], ['s_store_name, 's_store_id, 'sum(CASE WHEN ('d_day_name = Sunday) THEN 'ss_sales_price ELSE null END) AS sun_sales#0, 'sum(CASE WHEN ('d_day_name = Monday) THEN 'ss_sales_price ELSE null END) AS mon_sales#1, 'sum(CASE WHEN ('d_day_name = Tuesday) THEN 'ss_sales_price ELSE null END) AS tue_sales#2, 'sum(CASE WHEN ('d_day_name = Wednesday) THEN 'ss_sales_price ELSE null END) AS wed_sales#3, 'sum(CASE WHEN ('d_day_name = Thursday) THEN 'ss_sales_price ELSE null END) AS thu_sales#4, 'sum(CASE WHEN ('d_day_name = Friday) THEN 'ss_sales_price ELSE null END) AS fri_sales#5, 'sum(CASE WHEN ('d_day_name = Saturday) THEN 'ss_sales_price ELSE null END) AS sat_sales#6]
         +- 'Filter ((('d_date_sk = 'ss_sold_date_sk) AND ('s_store_sk = 'ss_store_sk)) AND (('s_gmt_offset = -5) AND ('d_year = 2000)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [store_sales], [], false
               +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
s_store_name: string, s_store_id: string, sun_sales: double, mon_sales: double, tue_sales: double, wed_sales: double, thu_sales: double, fri_sales: double, sat_sales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#68 ASC NULLS FIRST, s_store_id#64 ASC NULLS FIRST, sun_sales#0 ASC NULLS FIRST, mon_sales#1 ASC NULLS FIRST, tue_sales#2 ASC NULLS FIRST, wed_sales#3 ASC NULLS FIRST, thu_sales#4 ASC NULLS FIRST, fri_sales#5 ASC NULLS FIRST, sat_sales#6 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#68, s_store_id#64], [s_store_name#68, s_store_id#64, sum(CASE WHEN (d_day_name#26 = Sunday) THEN ss_sales_price#53 ELSE cast(null as double) END) AS sun_sales#0, sum(CASE WHEN (d_day_name#26 = Monday) THEN ss_sales_price#53 ELSE cast(null as double) END) AS mon_sales#1, sum(CASE WHEN (d_day_name#26 = Tuesday) THEN ss_sales_price#53 ELSE cast(null as double) END) AS tue_sales#2, sum(CASE WHEN (d_day_name#26 = Wednesday) THEN ss_sales_price#53 ELSE cast(null as double) END) AS wed_sales#3, sum(CASE WHEN (d_day_name#26 = Thursday) THEN ss_sales_price#53 ELSE cast(null as double) END) AS thu_sales#4, sum(CASE WHEN (d_day_name#26 = Friday) THEN ss_sales_price#53 ELSE cast(null as double) END) AS fri_sales#5, sum(CASE WHEN (d_day_name#26 = Saturday) THEN ss_sales_price#53 ELSE cast(null as double) END) AS sat_sales#6]
         +- Filter (((d_date_sk#12 = ss_sold_date_sk#40) AND (s_store_sk#63 = ss_store_sk#47)) AND ((s_gmt_offset#90 = cast(-5 as double)) AND (d_year#18 = 2000)))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#12,d_date_id#13,d_date#14,d_month_seq#15,d_week_seq#16,d_quarter_seq#17,d_year#18,d_dow#19,d_moy#20,d_dom#21,d_qoy#22,d_fy_year#23,d_fy_quarter_seq#24,d_fy_week_seq#25,d_day_name#26,d_quarter_name#27,d_holiday#28,d_weekend#29,d_following_holiday#30,d_first_dom#31,d_last_dom#32,d_same_day_ly#33,d_same_day_lq#34,d_current_day#35,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.store_sales
               :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
               +- SubqueryAlias spark_catalog.tpcds.store
                  +- Relation spark_catalog.tpcds.store[s_store_sk#63,s_store_id#64,s_rec_start_date#65,s_rec_end_date#66,s_closed_date_sk#67,s_store_name#68,s_number_employees#69,s_floor_space#70,s_hours#71,s_manager#72,s_market_id#73,s_geography_class#74,s_market_desc#75,s_market_manager#76,s_division_id#77,s_division_name#78,s_company_id#79,s_company_name#80,s_street_number#81,s_street_name#82,s_street_type#83,s_suite_number#84,s_city#85,s_county#86,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#68 ASC NULLS FIRST, s_store_id#64 ASC NULLS FIRST, sun_sales#0 ASC NULLS FIRST, mon_sales#1 ASC NULLS FIRST, tue_sales#2 ASC NULLS FIRST, wed_sales#3 ASC NULLS FIRST, thu_sales#4 ASC NULLS FIRST, fri_sales#5 ASC NULLS FIRST, sat_sales#6 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#68, s_store_id#64], [s_store_name#68, s_store_id#64, sum(CASE WHEN (d_day_name#26 = Sunday) THEN ss_sales_price#53 END) AS sun_sales#0, sum(CASE WHEN (d_day_name#26 = Monday) THEN ss_sales_price#53 END) AS mon_sales#1, sum(CASE WHEN (d_day_name#26 = Tuesday) THEN ss_sales_price#53 END) AS tue_sales#2, sum(CASE WHEN (d_day_name#26 = Wednesday) THEN ss_sales_price#53 END) AS wed_sales#3, sum(CASE WHEN (d_day_name#26 = Thursday) THEN ss_sales_price#53 END) AS thu_sales#4, sum(CASE WHEN (d_day_name#26 = Friday) THEN ss_sales_price#53 END) AS fri_sales#5, sum(CASE WHEN (d_day_name#26 = Saturday) THEN ss_sales_price#53 END) AS sat_sales#6]
         +- Project [d_day_name#26, ss_sales_price#53, s_store_id#64, s_store_name#68]
            +- Join Inner, (s_store_sk#63 = ss_store_sk#47)
               :- Project [d_day_name#26, ss_store_sk#47, ss_sales_price#53]
               :  +- Join Inner, (d_date_sk#12 = ss_sold_date_sk#40)
               :     :- Project [d_date_sk#12, d_day_name#26]
               :     :  +- Filter ((isnotnull(d_year#18) AND (d_year#18 = 2000)) AND isnotnull(d_date_sk#12))
               :     :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#12,d_date_id#13,d_date#14,d_month_seq#15,d_week_seq#16,d_quarter_seq#17,d_year#18,d_dow#19,d_moy#20,d_dom#21,d_qoy#22,d_fy_year#23,d_fy_quarter_seq#24,d_fy_week_seq#25,d_day_name#26,d_quarter_name#27,d_holiday#28,d_weekend#29,d_following_holiday#30,d_first_dom#31,d_last_dom#32,d_same_day_ly#33,d_same_day_lq#34,d_current_day#35,... 4 more fields] parquet
               :     +- Project [ss_sold_date_sk#40, ss_store_sk#47, ss_sales_price#53]
               :        +- Filter (isnotnull(ss_sold_date_sk#40) AND isnotnull(ss_store_sk#47))
               :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
               +- Project [s_store_sk#63, s_store_id#64, s_store_name#68]
                  +- Filter ((isnotnull(s_gmt_offset#90) AND (s_gmt_offset#90 = -5.0)) AND isnotnull(s_store_sk#63))
                     +- Relation spark_catalog.tpcds.store[s_store_sk#63,s_store_id#64,s_rec_start_date#65,s_rec_end_date#66,s_closed_date_sk#67,s_store_name#68,s_number_employees#69,s_floor_space#70,s_hours#71,s_manager#72,s_market_id#73,s_geography_class#74,s_market_desc#75,s_market_manager#76,s_division_id#77,s_division_name#78,s_company_id#79,s_company_name#80,s_street_number#81,s_street_name#82,s_street_type#83,s_suite_number#84,s_city#85,s_county#86,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[s_store_name#68 ASC NULLS FIRST,s_store_id#64 ASC NULLS FIRST,sun_sales#0 ASC NULLS FIRST,mon_sales#1 ASC NULLS FIRST,tue_sales#2 ASC NULLS FIRST,wed_sales#3 ASC NULLS FIRST,thu_sales#4 ASC NULLS FIRST,fri_sales#5 ASC NULLS FIRST,sat_sales#6 ASC NULLS FIRST], output=[s_store_name#68,s_store_id#64,sun_sales#0,mon_sales#1,tue_sales#2,wed_sales#3,thu_sales#4,fri_sales#5,sat_sales#6])
   +- HashAggregate(keys=[s_store_name#68, s_store_id#64], functions=[sum(CASE WHEN (d_day_name#26 = Sunday) THEN ss_sales_price#53 END), sum(CASE WHEN (d_day_name#26 = Monday) THEN ss_sales_price#53 END), sum(CASE WHEN (d_day_name#26 = Tuesday) THEN ss_sales_price#53 END), sum(CASE WHEN (d_day_name#26 = Wednesday) THEN ss_sales_price#53 END), sum(CASE WHEN (d_day_name#26 = Thursday) THEN ss_sales_price#53 END), sum(CASE WHEN (d_day_name#26 = Friday) THEN ss_sales_price#53 END), sum(CASE WHEN (d_day_name#26 = Saturday) THEN ss_sales_price#53 END)], output=[s_store_name#68, s_store_id#64, sun_sales#0, mon_sales#1, tue_sales#2, wed_sales#3, thu_sales#4, fri_sales#5, sat_sales#6])
      +- Exchange hashpartitioning(s_store_name#68, s_store_id#64, 200), ENSURE_REQUIREMENTS, [plan_id=72]
         +- HashAggregate(keys=[s_store_name#68, s_store_id#64], functions=[partial_sum(CASE WHEN (d_day_name#26 = Sunday) THEN ss_sales_price#53 END), partial_sum(CASE WHEN (d_day_name#26 = Monday) THEN ss_sales_price#53 END), partial_sum(CASE WHEN (d_day_name#26 = Tuesday) THEN ss_sales_price#53 END), partial_sum(CASE WHEN (d_day_name#26 = Wednesday) THEN ss_sales_price#53 END), partial_sum(CASE WHEN (d_day_name#26 = Thursday) THEN ss_sales_price#53 END), partial_sum(CASE WHEN (d_day_name#26 = Friday) THEN ss_sales_price#53 END), partial_sum(CASE WHEN (d_day_name#26 = Saturday) THEN ss_sales_price#53 END)], output=[s_store_name#68, s_store_id#64, sum#109, sum#110, sum#111, sum#112, sum#113, sum#114, sum#115])
            +- Project [d_day_name#26, ss_sales_price#53, s_store_id#64, s_store_name#68]
               +- BroadcastHashJoin [ss_store_sk#47], [s_store_sk#63], Inner, BuildRight, false
                  :- Project [d_day_name#26, ss_store_sk#47, ss_sales_price#53]
                  :  +- BroadcastHashJoin [d_date_sk#12], [ss_sold_date_sk#40], Inner, BuildLeft, false
                  :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=63]
                  :     :  +- Project [d_date_sk#12, d_day_name#26]
                  :     :     +- Filter ((isnotnull(d_year#18) AND (d_year#18 = 2000)) AND isnotnull(d_date_sk#12))
                  :     :        +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#12,d_year#18,d_day_name#26] Batched: true, DataFilters: [isnotnull(d_year#18), (d_year#18 = 2000), isnotnull(d_date_sk#12)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_day_name:string>
                  :     +- Filter (isnotnull(ss_sold_date_sk#40) AND isnotnull(ss_store_sk#47))
                  :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#40,ss_store_sk#47,ss_sales_price#53] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#40), isnotnull(ss_store_sk#47)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_sales_price:double>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=67]
                     +- Project [s_store_sk#63, s_store_id#64, s_store_name#68]
                        +- Filter ((isnotnull(s_gmt_offset#90) AND (s_gmt_offset#90 = -5.0)) AND isnotnull(s_store_sk#63))
                           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#63,s_store_id#64,s_store_name#68,s_gmt_offset#90] Batched: true, DataFilters: [isnotnull(s_gmt_offset#90), (s_gmt_offset#90 = -5.0), isnotnull(s_store_sk#63)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_gmt_offset), EqualTo(s_gmt_offset,-5.0), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string,s_store_name:string,s_gmt_offset:double>

Time taken: 2.426 seconds, Fetched 1 row(s)
