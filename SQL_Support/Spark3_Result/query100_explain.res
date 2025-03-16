Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583490515
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('w_warehouse_name, 1, 20) ASC NULLS FIRST, 'sm_type ASC NULLS FIRST, 'cc_name ASC NULLS FIRST], true
      +- 'Aggregate ['substr('w_warehouse_name, 1, 20), 'sm_type, 'cc_name], ['substr('w_warehouse_name, 1, 20) AS warehouse_name#0, 'sm_type, 'cc_name, 'SUM(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END) AS days_30#1, 'SUM(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 30) AND (('cs_ship_date_sk - 'cs_sold_date_sk) <= 60)) THEN 1 ELSE 0 END) AS days_31_60#2, 'SUM(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 60) AND (('cs_ship_date_sk - 'cs_sold_date_sk) <= 90)) THEN 1 ELSE 0 END) AS days_61_90#3, 'SUM(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 90) AND (('cs_ship_date_sk - 'cs_sold_date_sk) <= 120)) THEN 1 ELSE 0 END) AS days_91_120#4, 'SUM(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) > 120) THEN 1 ELSE 0 END) AS above120_days#5]
         +- 'Join Inner, (('cs.cs_ship_date_sk = 'd.d_date_sk) AND (('d_month_seq >= 1200) AND ('d_month_seq <= 1211)))
            :- 'Join Inner, ('cs.cs_call_center_sk = 'cc.cc_call_center_sk)
            :  :- 'Join Inner, ('cs.cs_ship_mode_sk = 'sm.sm_ship_mode_sk)
            :  :  :- 'Join Inner, ('cs.cs_warehouse_sk = 'w.w_warehouse_sk)
            :  :  :  :- 'SubqueryAlias cs
            :  :  :  :  +- 'UnresolvedRelation [catalog_sales], [], false
            :  :  :  +- 'SubqueryAlias w
            :  :  :     +- 'UnresolvedRelation [warehouse], [], false
            :  :  +- 'SubqueryAlias sm
            :  :     +- 'UnresolvedRelation [ship_mode], [], false
            :  +- 'SubqueryAlias cc
            :     +- 'UnresolvedRelation [call_center], [], false
            +- 'SubqueryAlias d
               +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
warehouse_name: string, sm_type: string, cc_name: string, days_30: bigint, days_31_60: bigint, days_61_90: bigint, days_91_120: bigint, above120_days: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [warehouse_name#0 ASC NULLS FIRST, sm_type#61 ASC NULLS FIRST, cc_name#71 ASC NULLS FIRST], true
      +- Aggregate [substr(w_warehouse_name#47, 1, 20), sm_type#61, cc_name#71], [substr(w_warehouse_name#47, 1, 20) AS warehouse_name#0, sm_type#61, cc_name#71, sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 30) THEN 1 ELSE 0 END) AS days_30#1L, sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 30) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 60)) THEN 1 ELSE 0 END) AS days_31_60#2L, sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 60) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 90)) THEN 1 ELSE 0 END) AS days_61_90#3L, sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 90) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 120)) THEN 1 ELSE 0 END) AS days_91_120#4L, sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 120) THEN 1 ELSE 0 END) AS above120_days#5L]
         +- Join Inner, ((cs_ship_date_sk#13 = d_date_sk#96) AND ((d_month_seq#99 >= 1200) AND (d_month_seq#99 <= 1211)))
            :- Join Inner, (cs_call_center_sk#22 = cc_call_center_sk#65)
            :  :- Join Inner, (cs_ship_mode_sk#24 = sm_ship_mode_sk#59)
            :  :  :- Join Inner, (cs_warehouse_sk#25 = w_warehouse_sk#45)
            :  :  :  :- SubqueryAlias cs
            :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_sales
            :  :  :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#11,cs_sold_time_sk#12,cs_ship_date_sk#13,cs_bill_customer_sk#14,cs_bill_cdemo_sk#15,cs_bill_hdemo_sk#16,cs_bill_addr_sk#17,cs_ship_customer_sk#18,cs_ship_cdemo_sk#19,cs_ship_hdemo_sk#20,cs_ship_addr_sk#21,cs_call_center_sk#22,cs_catalog_page_sk#23,cs_ship_mode_sk#24,cs_warehouse_sk#25,cs_item_sk#26,cs_promo_sk#27,cs_order_number#28,cs_quantity#29,cs_wholesale_cost#30,cs_list_price#31,cs_sales_price#32,cs_ext_discount_amt#33,cs_ext_sales_price#34,... 10 more fields] parquet
            :  :  :  +- SubqueryAlias w
            :  :  :     +- SubqueryAlias spark_catalog.tpcds.warehouse
            :  :  :        +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#45,w_warehouse_id#46,w_warehouse_name#47,w_warehouse_sq_ft#48,w_street_number#49,w_street_name#50,w_street_type#51,w_suite_number#52,w_city#53,w_county#54,w_state#55,w_zip#56,w_country#57,w_gmt_offset#58] parquet
            :  :  +- SubqueryAlias sm
            :  :     +- SubqueryAlias spark_catalog.tpcds.ship_mode
            :  :        +- Relation spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#59,sm_ship_mode_id#60,sm_type#61,sm_code#62,sm_carrier#63,sm_contract#64] parquet
            :  +- SubqueryAlias cc
            :     +- SubqueryAlias spark_catalog.tpcds.call_center
            :        +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#65,cc_call_center_id#66,cc_rec_start_date#67,cc_rec_end_date#68,cc_closed_date_sk#69,cc_open_date_sk#70,cc_name#71,cc_class#72,cc_employees#73,cc_sq_ft#74,cc_hours#75,cc_manager#76,cc_mkt_id#77,cc_mkt_class#78,cc_mkt_desc#79,cc_market_manager#80,cc_division#81,cc_division_name#82,cc_company#83,cc_company_name#84,cc_street_number#85,cc_street_name#86,cc_street_type#87,cc_suite_number#88,... 7 more fields] parquet
            +- SubqueryAlias d
               +- SubqueryAlias spark_catalog.tpcds.date_dim
                  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#96,d_date_id#97,d_date#98,d_month_seq#99,d_week_seq#100,d_quarter_seq#101,d_year#102,d_dow#103,d_moy#104,d_dom#105,d_qoy#106,d_fy_year#107,d_fy_quarter_seq#108,d_fy_week_seq#109,d_day_name#110,d_quarter_name#111,d_holiday#112,d_weekend#113,d_following_holiday#114,d_first_dom#115,d_last_dom#116,d_same_day_ly#117,d_same_day_lq#118,d_current_day#119,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [warehouse_name#0 ASC NULLS FIRST, sm_type#61 ASC NULLS FIRST, cc_name#71 ASC NULLS FIRST], true
      +- Aggregate [_groupingexpression#134, sm_type#61, cc_name#71], [_groupingexpression#134 AS warehouse_name#0, sm_type#61, cc_name#71, sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 30) THEN 1 ELSE 0 END) AS days_30#1L, sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 30) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 60)) THEN 1 ELSE 0 END) AS days_31_60#2L, sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 60) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 90)) THEN 1 ELSE 0 END) AS days_61_90#3L, sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 90) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 120)) THEN 1 ELSE 0 END) AS days_91_120#4L, sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 120) THEN 1 ELSE 0 END) AS above120_days#5L]
         +- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, sm_type#61, cc_name#71, substr(w_warehouse_name#47, 1, 20) AS _groupingexpression#134]
            +- Join Inner, (cs_ship_date_sk#13 = d_date_sk#96)
               :- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, w_warehouse_name#47, sm_type#61, cc_name#71]
               :  +- Join Inner, (cs_call_center_sk#22 = cc_call_center_sk#65)
               :     :- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, cs_call_center_sk#22, w_warehouse_name#47, sm_type#61]
               :     :  +- Join Inner, (cs_ship_mode_sk#24 = sm_ship_mode_sk#59)
               :     :     :- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, cs_call_center_sk#22, cs_ship_mode_sk#24, w_warehouse_name#47]
               :     :     :  +- Join Inner, (cs_warehouse_sk#25 = w_warehouse_sk#45)
               :     :     :     :- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, cs_call_center_sk#22, cs_ship_mode_sk#24, cs_warehouse_sk#25]
               :     :     :     :  +- Filter ((isnotnull(cs_warehouse_sk#25) AND isnotnull(cs_ship_mode_sk#24)) AND (isnotnull(cs_call_center_sk#22) AND isnotnull(cs_ship_date_sk#13)))
               :     :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#11,cs_sold_time_sk#12,cs_ship_date_sk#13,cs_bill_customer_sk#14,cs_bill_cdemo_sk#15,cs_bill_hdemo_sk#16,cs_bill_addr_sk#17,cs_ship_customer_sk#18,cs_ship_cdemo_sk#19,cs_ship_hdemo_sk#20,cs_ship_addr_sk#21,cs_call_center_sk#22,cs_catalog_page_sk#23,cs_ship_mode_sk#24,cs_warehouse_sk#25,cs_item_sk#26,cs_promo_sk#27,cs_order_number#28,cs_quantity#29,cs_wholesale_cost#30,cs_list_price#31,cs_sales_price#32,cs_ext_discount_amt#33,cs_ext_sales_price#34,... 10 more fields] parquet
               :     :     :     +- Project [w_warehouse_sk#45, w_warehouse_name#47]
               :     :     :        +- Filter isnotnull(w_warehouse_sk#45)
               :     :     :           +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#45,w_warehouse_id#46,w_warehouse_name#47,w_warehouse_sq_ft#48,w_street_number#49,w_street_name#50,w_street_type#51,w_suite_number#52,w_city#53,w_county#54,w_state#55,w_zip#56,w_country#57,w_gmt_offset#58] parquet
               :     :     +- Project [sm_ship_mode_sk#59, sm_type#61]
               :     :        +- Filter isnotnull(sm_ship_mode_sk#59)
               :     :           +- Relation spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#59,sm_ship_mode_id#60,sm_type#61,sm_code#62,sm_carrier#63,sm_contract#64] parquet
               :     +- Project [cc_call_center_sk#65, cc_name#71]
               :        +- Filter isnotnull(cc_call_center_sk#65)
               :           +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#65,cc_call_center_id#66,cc_rec_start_date#67,cc_rec_end_date#68,cc_closed_date_sk#69,cc_open_date_sk#70,cc_name#71,cc_class#72,cc_employees#73,cc_sq_ft#74,cc_hours#75,cc_manager#76,cc_mkt_id#77,cc_mkt_class#78,cc_mkt_desc#79,cc_market_manager#80,cc_division#81,cc_division_name#82,cc_company#83,cc_company_name#84,cc_street_number#85,cc_street_name#86,cc_street_type#87,cc_suite_number#88,... 7 more fields] parquet
               +- Project [d_date_sk#96]
                  +- Filter ((isnotnull(d_month_seq#99) AND ((d_month_seq#99 >= 1200) AND (d_month_seq#99 <= 1211))) AND isnotnull(d_date_sk#96))
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#96,d_date_id#97,d_date#98,d_month_seq#99,d_week_seq#100,d_quarter_seq#101,d_year#102,d_dow#103,d_moy#104,d_dom#105,d_qoy#106,d_fy_year#107,d_fy_quarter_seq#108,d_fy_week_seq#109,d_day_name#110,d_quarter_name#111,d_holiday#112,d_weekend#113,d_following_holiday#114,d_first_dom#115,d_last_dom#116,d_same_day_ly#117,d_same_day_lq#118,d_current_day#119,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[warehouse_name#0 ASC NULLS FIRST,sm_type#61 ASC NULLS FIRST,cc_name#71 ASC NULLS FIRST], output=[warehouse_name#0,sm_type#61,cc_name#71,days_30#1L,days_31_60#2L,days_61_90#3L,days_91_120#4L,above120_days#5L])
   +- HashAggregate(keys=[_groupingexpression#134, sm_type#61, cc_name#71], functions=[sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 30) THEN 1 ELSE 0 END), sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 30) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 60)) THEN 1 ELSE 0 END), sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 60) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 90)) THEN 1 ELSE 0 END), sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 90) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 120)) THEN 1 ELSE 0 END), sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 120) THEN 1 ELSE 0 END)], output=[warehouse_name#0, sm_type#61, cc_name#71, days_30#1L, days_31_60#2L, days_61_90#3L, days_91_120#4L, above120_days#5L])
      +- Exchange hashpartitioning(_groupingexpression#134, sm_type#61, cc_name#71, 200), ENSURE_REQUIREMENTS, [plan_id=116]
         +- HashAggregate(keys=[_groupingexpression#134, sm_type#61, cc_name#71], functions=[partial_sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 30) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 30) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 60)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 60) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 90)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 90) AND ((cs_ship_date_sk#13 - cs_sold_date_sk#11) <= 120)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN ((cs_ship_date_sk#13 - cs_sold_date_sk#11) > 120) THEN 1 ELSE 0 END)], output=[_groupingexpression#134, sm_type#61, cc_name#71, sum#140L, sum#141L, sum#142L, sum#143L, sum#144L])
            +- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, sm_type#61, cc_name#71, substr(w_warehouse_name#47, 1, 20) AS _groupingexpression#134]
               +- BroadcastHashJoin [cs_ship_date_sk#13], [d_date_sk#96], Inner, BuildRight, false
                  :- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, w_warehouse_name#47, sm_type#61, cc_name#71]
                  :  +- BroadcastHashJoin [cs_call_center_sk#22], [cc_call_center_sk#65], Inner, BuildRight, false
                  :     :- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, cs_call_center_sk#22, w_warehouse_name#47, sm_type#61]
                  :     :  +- BroadcastHashJoin [cs_ship_mode_sk#24], [sm_ship_mode_sk#59], Inner, BuildRight, false
                  :     :     :- Project [cs_sold_date_sk#11, cs_ship_date_sk#13, cs_call_center_sk#22, cs_ship_mode_sk#24, w_warehouse_name#47]
                  :     :     :  +- BroadcastHashJoin [cs_warehouse_sk#25], [w_warehouse_sk#45], Inner, BuildRight, false
                  :     :     :     :- Filter (((isnotnull(cs_warehouse_sk#25) AND isnotnull(cs_ship_mode_sk#24)) AND isnotnull(cs_call_center_sk#22)) AND isnotnull(cs_ship_date_sk#13))
                  :     :     :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#11,cs_ship_date_sk#13,cs_call_center_sk#22,cs_ship_mode_sk#24,cs_warehouse_sk#25] Batched: true, DataFilters: [isnotnull(cs_warehouse_sk#25), isnotnull(cs_ship_mode_sk#24), isnotnull(cs_call_center_sk#22), i..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_warehouse_sk), IsNotNull(cs_ship_mode_sk), IsNotNull(cs_call_center_sk), IsNotNull(..., ReadSchema: struct<cs_sold_date_sk:int,cs_ship_date_sk:int,cs_call_center_sk:int,cs_ship_mode_sk:int,cs_wareh...
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=99]
                  :     :     :        +- Filter isnotnull(w_warehouse_sk#45)
                  :     :     :           +- FileScan parquet spark_catalog.tpcds.warehouse[w_warehouse_sk#45,w_warehouse_name#47] Batched: true, DataFilters: [isnotnull(w_warehouse_sk#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=103]
                  :     :        +- Filter isnotnull(sm_ship_mode_sk#59)
                  :     :           +- FileScan parquet spark_catalog.tpcds.ship_mode[sm_ship_mode_sk#59,sm_type#61] Batched: true, DataFilters: [isnotnull(sm_ship_mode_sk#59)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sm_ship_mode_sk)], ReadSchema: struct<sm_ship_mode_sk:int,sm_type:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=107]
                  :        +- Filter isnotnull(cc_call_center_sk#65)
                  :           +- FileScan parquet spark_catalog.tpcds.call_center[cc_call_center_sk#65,cc_name#71] Batched: true, DataFilters: [isnotnull(cc_call_center_sk#65)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=111]
                     +- Project [d_date_sk#96]
                        +- Filter (((isnotnull(d_month_seq#99) AND (d_month_seq#99 >= 1200)) AND (d_month_seq#99 <= 1211)) AND isnotnull(d_date_sk#96))
                           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#96,d_month_seq#99] Batched: true, DataFilters: [isnotnull(d_month_seq#99), (d_month_seq#99 >= 1200), (d_month_seq#99 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>

Time taken: 2.931 seconds, Fetched 1 row(s)
