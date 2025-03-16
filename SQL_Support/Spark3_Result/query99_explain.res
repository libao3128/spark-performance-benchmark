Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583452305
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('w_warehouse_name, 1, 20) ASC NULLS FIRST, 'sm_type ASC NULLS FIRST, 'cc_name ASC NULLS FIRST], true
      +- 'Aggregate ['substr('w_warehouse_name, 1, 20), 'sm_type, 'cc_name], [unresolvedalias('substr('w_warehouse_name, 1, 20), None), 'sm_type, 'cc_name, 'sum(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END) AS 30_days#0, 'sum(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 30) AND (('cs_ship_date_sk - 'cs_sold_date_sk) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1, 'sum(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 60) AND (('cs_ship_date_sk - 'cs_sold_date_sk) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2, 'sum(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 90) AND (('cs_ship_date_sk - 'cs_sold_date_sk) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3, 'sum(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) > 120) THEN 1 ELSE 0 END) AS above120_days#4]
         +- 'Filter ((((('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))) AND ('cs_ship_date_sk = 'd_date_sk)) AND ('cs_warehouse_sk = 'w_warehouse_sk)) AND (('cs_ship_mode_sk = 'sm_ship_mode_sk) AND ('cs_call_center_sk = 'cc_call_center_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation [catalog_sales], [], false
               :  :  :  +- 'UnresolvedRelation [warehouse], [], false
               :  :  +- 'UnresolvedRelation [ship_mode], [], false
               :  +- 'UnresolvedRelation [call_center], [], false
               +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
substr(w_warehouse_name, 1, 20): string, sm_type: string, cc_name: string, 30_days: bigint, 31_60_days: bigint, 61_90_days: bigint, 91_120_days: bigint, above120_days: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substr(w_warehouse_name, 1, 20)#128 ASC NULLS FIRST, sm_type#60 ASC NULLS FIRST, cc_name#70 ASC NULLS FIRST], true
      +- Aggregate [substr(w_warehouse_name#46, 1, 20), sm_type#60, cc_name#70], [substr(w_warehouse_name#46, 1, 20) AS substr(w_warehouse_name, 1, 20)#128, sm_type#60, cc_name#70, sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END) AS 30_days#0L, sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 30) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1L, sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 60) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2L, sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 90) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3L, sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 120) THEN 1 ELSE 0 END) AS above120_days#4L]
         +- Filter (((((d_month_seq#98 >= 1200) AND (d_month_seq#98 <= (1200 + 11))) AND (cs_ship_date_sk#12 = d_date_sk#95)) AND (cs_warehouse_sk#24 = w_warehouse_sk#44)) AND ((cs_ship_mode_sk#23 = sm_ship_mode_sk#58) AND (cs_call_center_sk#21 = cc_call_center_sk#64)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#10,cs_sold_time_sk#11,cs_ship_date_sk#12,cs_bill_customer_sk#13,cs_bill_cdemo_sk#14,cs_bill_hdemo_sk#15,cs_bill_addr_sk#16,cs_ship_customer_sk#17,cs_ship_cdemo_sk#18,cs_ship_hdemo_sk#19,cs_ship_addr_sk#20,cs_call_center_sk#21,cs_catalog_page_sk#22,cs_ship_mode_sk#23,cs_warehouse_sk#24,cs_item_sk#25,cs_promo_sk#26,cs_order_number#27,cs_quantity#28,cs_wholesale_cost#29,cs_list_price#30,cs_sales_price#31,cs_ext_discount_amt#32,cs_ext_sales_price#33,... 10 more fields] parquet
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.warehouse
               :  :  :     +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#44,w_warehouse_id#45,w_warehouse_name#46,w_warehouse_sq_ft#47,w_street_number#48,w_street_name#49,w_street_type#50,w_suite_number#51,w_city#52,w_county#53,w_state#54,w_zip#55,w_country#56,w_gmt_offset#57] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.ship_mode
               :  :     +- Relation spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#58,sm_ship_mode_id#59,sm_type#60,sm_code#61,sm_carrier#62,sm_contract#63] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.call_center
               :     +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#64,cc_call_center_id#65,cc_rec_start_date#66,cc_rec_end_date#67,cc_closed_date_sk#68,cc_open_date_sk#69,cc_name#70,cc_class#71,cc_employees#72,cc_sq_ft#73,cc_hours#74,cc_manager#75,cc_mkt_id#76,cc_mkt_class#77,cc_mkt_desc#78,cc_market_manager#79,cc_division#80,cc_division_name#81,cc_company#82,cc_company_name#83,cc_street_number#84,cc_street_name#85,cc_street_type#86,cc_suite_number#87,... 7 more fields] parquet
               +- SubqueryAlias spark_catalog.tpcds.date_dim
                  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#95,d_date_id#96,d_date#97,d_month_seq#98,d_week_seq#99,d_quarter_seq#100,d_year#101,d_dow#102,d_moy#103,d_dom#104,d_qoy#105,d_fy_year#106,d_fy_quarter_seq#107,d_fy_week_seq#108,d_day_name#109,d_quarter_name#110,d_holiday#111,d_weekend#112,d_following_holiday#113,d_first_dom#114,d_last_dom#115,d_same_day_ly#116,d_same_day_lq#117,d_current_day#118,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substr(w_warehouse_name, 1, 20)#128 ASC NULLS FIRST, sm_type#60 ASC NULLS FIRST, cc_name#70 ASC NULLS FIRST], true
      +- Aggregate [_groupingexpression#134, sm_type#60, cc_name#70], [_groupingexpression#134 AS substr(w_warehouse_name, 1, 20)#128, sm_type#60, cc_name#70, sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END) AS 30_days#0L, sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 30) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1L, sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 60) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2L, sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 90) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3L, sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 120) THEN 1 ELSE 0 END) AS above120_days#4L]
         +- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, sm_type#60, cc_name#70, substr(w_warehouse_name#46, 1, 20) AS _groupingexpression#134]
            +- Join Inner, (cs_ship_date_sk#12 = d_date_sk#95)
               :- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, w_warehouse_name#46, sm_type#60, cc_name#70]
               :  +- Join Inner, (cs_call_center_sk#21 = cc_call_center_sk#64)
               :     :- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, cs_call_center_sk#21, w_warehouse_name#46, sm_type#60]
               :     :  +- Join Inner, (cs_ship_mode_sk#23 = sm_ship_mode_sk#58)
               :     :     :- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, cs_call_center_sk#21, cs_ship_mode_sk#23, w_warehouse_name#46]
               :     :     :  +- Join Inner, (cs_warehouse_sk#24 = w_warehouse_sk#44)
               :     :     :     :- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, cs_call_center_sk#21, cs_ship_mode_sk#23, cs_warehouse_sk#24]
               :     :     :     :  +- Filter ((isnotnull(cs_warehouse_sk#24) AND isnotnull(cs_ship_mode_sk#23)) AND (isnotnull(cs_call_center_sk#21) AND isnotnull(cs_ship_date_sk#12)))
               :     :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#10,cs_sold_time_sk#11,cs_ship_date_sk#12,cs_bill_customer_sk#13,cs_bill_cdemo_sk#14,cs_bill_hdemo_sk#15,cs_bill_addr_sk#16,cs_ship_customer_sk#17,cs_ship_cdemo_sk#18,cs_ship_hdemo_sk#19,cs_ship_addr_sk#20,cs_call_center_sk#21,cs_catalog_page_sk#22,cs_ship_mode_sk#23,cs_warehouse_sk#24,cs_item_sk#25,cs_promo_sk#26,cs_order_number#27,cs_quantity#28,cs_wholesale_cost#29,cs_list_price#30,cs_sales_price#31,cs_ext_discount_amt#32,cs_ext_sales_price#33,... 10 more fields] parquet
               :     :     :     +- Project [w_warehouse_sk#44, w_warehouse_name#46]
               :     :     :        +- Filter isnotnull(w_warehouse_sk#44)
               :     :     :           +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#44,w_warehouse_id#45,w_warehouse_name#46,w_warehouse_sq_ft#47,w_street_number#48,w_street_name#49,w_street_type#50,w_suite_number#51,w_city#52,w_county#53,w_state#54,w_zip#55,w_country#56,w_gmt_offset#57] parquet
               :     :     +- Project [sm_ship_mode_sk#58, sm_type#60]
               :     :        +- Filter isnotnull(sm_ship_mode_sk#58)
               :     :           +- Relation spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#58,sm_ship_mode_id#59,sm_type#60,sm_code#61,sm_carrier#62,sm_contract#63] parquet
               :     +- Project [cc_call_center_sk#64, cc_name#70]
               :        +- Filter isnotnull(cc_call_center_sk#64)
               :           +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#64,cc_call_center_id#65,cc_rec_start_date#66,cc_rec_end_date#67,cc_closed_date_sk#68,cc_open_date_sk#69,cc_name#70,cc_class#71,cc_employees#72,cc_sq_ft#73,cc_hours#74,cc_manager#75,cc_mkt_id#76,cc_mkt_class#77,cc_mkt_desc#78,cc_market_manager#79,cc_division#80,cc_division_name#81,cc_company#82,cc_company_name#83,cc_street_number#84,cc_street_name#85,cc_street_type#86,cc_suite_number#87,... 7 more fields] parquet
               +- Project [d_date_sk#95]
                  +- Filter ((isnotnull(d_month_seq#98) AND ((d_month_seq#98 >= 1200) AND (d_month_seq#98 <= 1211))) AND isnotnull(d_date_sk#95))
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#95,d_date_id#96,d_date#97,d_month_seq#98,d_week_seq#99,d_quarter_seq#100,d_year#101,d_dow#102,d_moy#103,d_dom#104,d_qoy#105,d_fy_year#106,d_fy_quarter_seq#107,d_fy_week_seq#108,d_day_name#109,d_quarter_name#110,d_holiday#111,d_weekend#112,d_following_holiday#113,d_first_dom#114,d_last_dom#115,d_same_day_ly#116,d_same_day_lq#117,d_current_day#118,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[substr(w_warehouse_name, 1, 20)#128 ASC NULLS FIRST,sm_type#60 ASC NULLS FIRST,cc_name#70 ASC NULLS FIRST], output=[substr(w_warehouse_name, 1, 20)#128,sm_type#60,cc_name#70,30_days#0L,31_60_days#1L,61_90_days#2L,91_120_days#3L,above120_days#4L])
   +- HashAggregate(keys=[_groupingexpression#134, sm_type#60, cc_name#70], functions=[sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END), sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 30) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END), sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 60) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END), sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 90) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END), sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 120) THEN 1 ELSE 0 END)], output=[substr(w_warehouse_name, 1, 20)#128, sm_type#60, cc_name#70, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L])
      +- Exchange hashpartitioning(_groupingexpression#134, sm_type#60, cc_name#70, 200), ENSURE_REQUIREMENTS, [plan_id=116]
         +- HashAggregate(keys=[_groupingexpression#134, sm_type#60, cc_name#70], functions=[partial_sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 30) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 30) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 60)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 60) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 90)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 90) AND ((cs_ship_date_sk#12 - cs_sold_date_sk#10) <= 120)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN ((cs_ship_date_sk#12 - cs_sold_date_sk#10) > 120) THEN 1 ELSE 0 END)], output=[_groupingexpression#134, sm_type#60, cc_name#70, sum#140L, sum#141L, sum#142L, sum#143L, sum#144L])
            +- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, sm_type#60, cc_name#70, substr(w_warehouse_name#46, 1, 20) AS _groupingexpression#134]
               +- BroadcastHashJoin [cs_ship_date_sk#12], [d_date_sk#95], Inner, BuildRight, false
                  :- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, w_warehouse_name#46, sm_type#60, cc_name#70]
                  :  +- BroadcastHashJoin [cs_call_center_sk#21], [cc_call_center_sk#64], Inner, BuildRight, false
                  :     :- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, cs_call_center_sk#21, w_warehouse_name#46, sm_type#60]
                  :     :  +- BroadcastHashJoin [cs_ship_mode_sk#23], [sm_ship_mode_sk#58], Inner, BuildRight, false
                  :     :     :- Project [cs_sold_date_sk#10, cs_ship_date_sk#12, cs_call_center_sk#21, cs_ship_mode_sk#23, w_warehouse_name#46]
                  :     :     :  +- BroadcastHashJoin [cs_warehouse_sk#24], [w_warehouse_sk#44], Inner, BuildRight, false
                  :     :     :     :- Filter (((isnotnull(cs_warehouse_sk#24) AND isnotnull(cs_ship_mode_sk#23)) AND isnotnull(cs_call_center_sk#21)) AND isnotnull(cs_ship_date_sk#12))
                  :     :     :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#10,cs_ship_date_sk#12,cs_call_center_sk#21,cs_ship_mode_sk#23,cs_warehouse_sk#24] Batched: true, DataFilters: [isnotnull(cs_warehouse_sk#24), isnotnull(cs_ship_mode_sk#23), isnotnull(cs_call_center_sk#21), i..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_warehouse_sk), IsNotNull(cs_ship_mode_sk), IsNotNull(cs_call_center_sk), IsNotNull(..., ReadSchema: struct<cs_sold_date_sk:int,cs_ship_date_sk:int,cs_call_center_sk:int,cs_ship_mode_sk:int,cs_wareh...
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=99]
                  :     :     :        +- Filter isnotnull(w_warehouse_sk#44)
                  :     :     :           +- FileScan parquet spark_catalog.tpcds.warehouse[w_warehouse_sk#44,w_warehouse_name#46] Batched: true, DataFilters: [isnotnull(w_warehouse_sk#44)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=103]
                  :     :        +- Filter isnotnull(sm_ship_mode_sk#58)
                  :     :           +- FileScan parquet spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#58,sm_type#60] Batched: true, DataFilters: [isnotnull(sm_ship_mode_sk#58)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sm_ship_mode_sk)], ReadSchema: struct<sm_ship_mode_sk:int,sm_type:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=107]
                  :        +- Filter isnotnull(cc_call_center_sk#64)
                  :           +- FileScan parquet spark_catalog.tpcds.call_center[cc_call_center_sk#64,cc_name#70] Batched: true, DataFilters: [isnotnull(cc_call_center_sk#64)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=111]
                     +- Project [d_date_sk#95]
                        +- Filter (((isnotnull(d_month_seq#98) AND (d_month_seq#98 >= 1200)) AND (d_month_seq#98 <= 1211)) AND isnotnull(d_date_sk#95))
                           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#95,d_month_seq#98] Batched: true, DataFilters: [isnotnull(d_month_seq#98), (d_month_seq#98 >= 1200), (d_month_seq#98 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>

Time taken: 3.316 seconds, Fetched 1 row(s)
