Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581935211
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('w_warehouse_name, 1, 20) ASC NULLS FIRST, 'sm_type ASC NULLS FIRST, 'web_name ASC NULLS FIRST], true
      +- 'Aggregate ['substr('w_warehouse_name, 1, 20), 'sm_type, 'web_name], [unresolvedalias('substr('w_warehouse_name, 1, 20), None), 'sm_type, 'web_name, 'sum(CASE WHEN (('ws_ship_date_sk - 'ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END) AS 30_days#0, 'sum(CASE WHEN ((('ws_ship_date_sk - 'ws_sold_date_sk) > 30) AND (('ws_ship_date_sk - 'ws_sold_date_sk) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1, 'sum(CASE WHEN ((('ws_ship_date_sk - 'ws_sold_date_sk) > 60) AND (('ws_ship_date_sk - 'ws_sold_date_sk) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2, 'sum(CASE WHEN ((('ws_ship_date_sk - 'ws_sold_date_sk) > 90) AND (('ws_ship_date_sk - 'ws_sold_date_sk) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3, 'sum(CASE WHEN (('ws_ship_date_sk - 'ws_sold_date_sk) > 120) THEN 1 ELSE 0 END) AS above120_days#4]
         +- 'Filter ((((('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))) AND ('ws_ship_date_sk = 'd_date_sk)) AND ('ws_warehouse_sk = 'w_warehouse_sk)) AND (('ws_ship_mode_sk = 'sm_ship_mode_sk) AND ('ws_web_site_sk = 'web_site_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation [web_sales], [], false
               :  :  :  +- 'UnresolvedRelation [warehouse], [], false
               :  :  +- 'UnresolvedRelation [ship_mode], [], false
               :  +- 'UnresolvedRelation [web_site], [], false
               +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
substr(w_warehouse_name, 1, 20): string, sm_type: string, web_name: string, 30_days: bigint, 31_60_days: bigint, 61_90_days: bigint, 91_120_days: bigint, above120_days: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substr(w_warehouse_name, 1, 20)#123 ASC NULLS FIRST, sm_type#60 ASC NULLS FIRST, web_name#68 ASC NULLS FIRST], true
      +- Aggregate [substr(w_warehouse_name#46, 1, 20), sm_type#60, web_name#68], [substr(w_warehouse_name#46, 1, 20) AS substr(w_warehouse_name, 1, 20)#123, sm_type#60, web_name#68, sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END) AS 30_days#0L, sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 30) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1L, sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 60) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2L, sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 90) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3L, sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 120) THEN 1 ELSE 0 END) AS above120_days#4L]
         +- Filter (((((d_month_seq#93 >= 1200) AND (d_month_seq#93 <= (1200 + 11))) AND (ws_ship_date_sk#12 = d_date_sk#90)) AND (ws_warehouse_sk#25 = w_warehouse_sk#44)) AND ((ws_ship_mode_sk#24 = sm_ship_mode_sk#58) AND (ws_web_site_sk#23 = web_site_sk#64)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
               :  :  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.warehouse
               :  :  :     +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#44,w_warehouse_id#45,w_warehouse_name#46,w_warehouse_sq_ft#47,w_street_number#48,w_street_name#49,w_street_type#50,w_suite_number#51,w_city#52,w_county#53,w_state#54,w_zip#55,w_country#56,w_gmt_offset#57] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.ship_mode
               :  :     +- Relation spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#58,sm_ship_mode_id#59,sm_type#60,sm_code#61,sm_carrier#62,sm_contract#63] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.web_site
               :     +- Relation spark_catalog.tpcds.web_site[web_site_sk#64,web_site_id#65,web_rec_start_date#66,web_rec_end_date#67,web_name#68,web_open_date_sk#69,web_close_date_sk#70,web_class#71,web_manager#72,web_mkt_id#73,web_mkt_class#74,web_mkt_desc#75,web_market_manager#76,web_company_id#77,web_company_name#78,web_street_number#79,web_street_name#80,web_street_type#81,web_suite_number#82,web_city#83,web_county#84,web_state#85,web_zip#86,web_country#87,... 2 more fields] parquet
               +- SubqueryAlias spark_catalog.tpcds.date_dim
                  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substr(w_warehouse_name, 1, 20)#123 ASC NULLS FIRST, sm_type#60 ASC NULLS FIRST, web_name#68 ASC NULLS FIRST], true
      +- Aggregate [_groupingexpression#129, sm_type#60, web_name#68], [_groupingexpression#129 AS substr(w_warehouse_name, 1, 20)#123, sm_type#60, web_name#68, sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END) AS 30_days#0L, sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 30) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1L, sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 60) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2L, sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 90) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3L, sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 120) THEN 1 ELSE 0 END) AS above120_days#4L]
         +- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, sm_type#60, web_name#68, substr(w_warehouse_name#46, 1, 20) AS _groupingexpression#129]
            +- Join Inner, (ws_ship_date_sk#12 = d_date_sk#90)
               :- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, w_warehouse_name#46, sm_type#60, web_name#68]
               :  +- Join Inner, (ws_web_site_sk#23 = web_site_sk#64)
               :     :- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, ws_web_site_sk#23, w_warehouse_name#46, sm_type#60]
               :     :  +- Join Inner, (ws_ship_mode_sk#24 = sm_ship_mode_sk#58)
               :     :     :- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, ws_web_site_sk#23, ws_ship_mode_sk#24, w_warehouse_name#46]
               :     :     :  +- Join Inner, (ws_warehouse_sk#25 = w_warehouse_sk#44)
               :     :     :     :- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, ws_web_site_sk#23, ws_ship_mode_sk#24, ws_warehouse_sk#25]
               :     :     :     :  +- Filter ((isnotnull(ws_warehouse_sk#25) AND isnotnull(ws_ship_mode_sk#24)) AND (isnotnull(ws_web_site_sk#23) AND isnotnull(ws_ship_date_sk#12)))
               :     :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
               :     :     :     +- Project [w_warehouse_sk#44, w_warehouse_name#46]
               :     :     :        +- Filter isnotnull(w_warehouse_sk#44)
               :     :     :           +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#44,w_warehouse_id#45,w_warehouse_name#46,w_warehouse_sq_ft#47,w_street_number#48,w_street_name#49,w_street_type#50,w_suite_number#51,w_city#52,w_county#53,w_state#54,w_zip#55,w_country#56,w_gmt_offset#57] parquet
               :     :     +- Project [sm_ship_mode_sk#58, sm_type#60]
               :     :        +- Filter isnotnull(sm_ship_mode_sk#58)
               :     :           +- Relation spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#58,sm_ship_mode_id#59,sm_type#60,sm_code#61,sm_carrier#62,sm_contract#63] parquet
               :     +- Project [web_site_sk#64, web_name#68]
               :        +- Filter isnotnull(web_site_sk#64)
               :           +- Relation spark_catalog.tpcds.web_site[web_site_sk#64,web_site_id#65,web_rec_start_date#66,web_rec_end_date#67,web_name#68,web_open_date_sk#69,web_close_date_sk#70,web_class#71,web_manager#72,web_mkt_id#73,web_mkt_class#74,web_mkt_desc#75,web_market_manager#76,web_company_id#77,web_company_name#78,web_street_number#79,web_street_name#80,web_street_type#81,web_suite_number#82,web_city#83,web_county#84,web_state#85,web_zip#86,web_country#87,... 2 more fields] parquet
               +- Project [d_date_sk#90]
                  +- Filter ((isnotnull(d_month_seq#93) AND ((d_month_seq#93 >= 1200) AND (d_month_seq#93 <= 1211))) AND isnotnull(d_date_sk#90))
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[substr(w_warehouse_name, 1, 20)#123 ASC NULLS FIRST,sm_type#60 ASC NULLS FIRST,web_name#68 ASC NULLS FIRST], output=[substr(w_warehouse_name, 1, 20)#123,sm_type#60,web_name#68,30_days#0L,31_60_days#1L,61_90_days#2L,91_120_days#3L,above120_days#4L])
   +- HashAggregate(keys=[_groupingexpression#129, sm_type#60, web_name#68], functions=[sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END), sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 30) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END), sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 60) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END), sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 90) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END), sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 120) THEN 1 ELSE 0 END)], output=[substr(w_warehouse_name, 1, 20)#123, sm_type#60, web_name#68, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L])
      +- Exchange hashpartitioning(_groupingexpression#129, sm_type#60, web_name#68, 200), ENSURE_REQUIREMENTS, [plan_id=116]
         +- HashAggregate(keys=[_groupingexpression#129, sm_type#60, web_name#68], functions=[partial_sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 30) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 60) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 90) AND ((ws_ship_date_sk#12 - ws_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN ((ws_ship_date_sk#12 - ws_sold_date_sk#10) > 120) THEN 1 ELSE 0 END)], output=[_groupingexpression#129, sm_type#60, web_name#68, sum#135L, sum#136L, sum#137L, sum#138L, sum#139L])
            +- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, sm_type#60, web_name#68, substr(w_warehouse_name#46, 1, 20) AS _groupingexpression#129]
               +- BroadcastHashJoin [ws_ship_date_sk#12], [d_date_sk#90], Inner, BuildRight, false
                  :- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, w_warehouse_name#46, sm_type#60, web_name#68]
                  :  +- BroadcastHashJoin [ws_web_site_sk#23], [web_site_sk#64], Inner, BuildRight, false
                  :     :- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, ws_web_site_sk#23, w_warehouse_name#46, sm_type#60]
                  :     :  +- BroadcastHashJoin [ws_ship_mode_sk#24], [sm_ship_mode_sk#58], Inner, BuildRight, false
                  :     :     :- Project [ws_sold_date_sk#10, ws_ship_date_sk#12, ws_web_site_sk#23, ws_ship_mode_sk#24, w_warehouse_name#46]
                  :     :     :  +- BroadcastHashJoin [ws_warehouse_sk#25], [w_warehouse_sk#44], Inner, BuildRight, false
                  :     :     :     :- Filter (((isnotnull(ws_warehouse_sk#25) AND isnotnull(ws_ship_mode_sk#24)) AND isnotnull(ws_web_site_sk#23)) AND isnotnull(ws_ship_date_sk#12))
                  :     :     :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_ship_date_sk#12,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25] Batched: true, DataFilters: [isnotnull(ws_warehouse_sk#25), isnotnull(ws_ship_mode_sk#24), isnotnull(ws_web_site_sk#23), isno..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_warehouse_sk), IsNotNull(ws_ship_mode_sk), IsNotNull(ws_web_site_sk), IsNotNull(ws_..., ReadSchema: struct<ws_sold_date_sk:int,ws_ship_date_sk:int,ws_web_site_sk:int,ws_ship_mode_sk:int,ws_warehous...
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=99]
                  :     :     :        +- Filter isnotnull(w_warehouse_sk#44)
                  :     :     :           +- FileScan parquet spark_catalog.tpcds.warehouse[w_warehouse_sk#44,w_warehouse_name#46] Batched: true, DataFilters: [isnotnull(w_warehouse_sk#44)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=103]
                  :     :        +- Filter isnotnull(sm_ship_mode_sk#58)
                  :     :           +- FileScan parquet spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#58,sm_type#60] Batched: true, DataFilters: [isnotnull(sm_ship_mode_sk#58)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sm_ship_mode_sk)], ReadSchema: struct<sm_ship_mode_sk:int,sm_type:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=107]
                  :        +- Filter isnotnull(web_site_sk#64)
                  :           +- FileScan parquet spark_catalog.tpcds.web_site[web_site_sk#64,web_name#68] Batched: true, DataFilters: [isnotnull(web_site_sk#64)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_name:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=111]
                     +- Project [d_date_sk#90]
                        +- Filter (((isnotnull(d_month_seq#93) AND (d_month_seq#93 >= 1200)) AND (d_month_seq#93 <= 1211)) AND isnotnull(d_date_sk#90))
                           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#90,d_month_seq#93] Batched: true, DataFilters: [isnotnull(d_month_seq#93), (d_month_seq#93 >= 1200), (d_month_seq#93 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>

Time taken: 2.614 seconds, Fetched 1 row(s)
