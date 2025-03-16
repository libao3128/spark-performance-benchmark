== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('w_warehouse_name, 1, 20) ASC NULLS FIRST, 'sm_type ASC NULLS FIRST, 'cc_name ASC NULLS FIRST], true
      +- 'Aggregate ['substr('w_warehouse_name, 1, 20), 'sm_type, 'cc_name], ['substr('w_warehouse_name, 1, 20) AS warehouse_name#0, 'sm_type, 'cc_name, 'SUM(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END) AS days_30#1, 'SUM(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 30) && (('cs_ship_date_sk - 'cs_sold_date_sk) <= 60)) THEN 1 ELSE 0 END) AS days_31_60#2, 'SUM(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 60) && (('cs_ship_date_sk - 'cs_sold_date_sk) <= 90)) THEN 1 ELSE 0 END) AS days_61_90#3, 'SUM(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 90) && (('cs_ship_date_sk - 'cs_sold_date_sk) <= 120)) THEN 1 ELSE 0 END) AS days_91_120#4, 'SUM(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) > 120) THEN 1 ELSE 0 END) AS above120_days#5]
         +- 'Join Inner, (('cs.cs_ship_date_sk = 'd.d_date_sk) && (('d_month_seq >= 1200) && ('d_month_seq <= 1211)))
            :- 'Join Inner, ('cs.cs_call_center_sk = 'cc.cc_call_center_sk)
            :  :- 'Join Inner, ('cs.cs_ship_mode_sk = 'sm.sm_ship_mode_sk)
            :  :  :- 'Join Inner, ('cs.cs_warehouse_sk = 'w.w_warehouse_sk)
            :  :  :  :- 'SubqueryAlias `cs`
            :  :  :  :  +- 'UnresolvedRelation `catalog_sales`
            :  :  :  +- 'SubqueryAlias `w`
            :  :  :     +- 'UnresolvedRelation `warehouse`
            :  :  +- 'SubqueryAlias `sm`
            :  :     +- 'UnresolvedRelation `ship_mode`
            :  +- 'SubqueryAlias `cc`
            :     +- 'UnresolvedRelation `call_center`
            +- 'SubqueryAlias `d`
               +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
warehouse_name: string, sm_type: string, cc_name: string, days_30: bigint, days_31_60: bigint, days_61_90: bigint, days_91_120: bigint, above120_days: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Project [warehouse_name#0, sm_type#58, cc_name#68, days_30#1L, days_31_60#2L, days_61_90#3L, days_91_120#4L, above120_days#5L]
      +- Sort [warehouse_name#0 ASC NULLS FIRST, sm_type#58 ASC NULLS FIRST, cc_name#68 ASC NULLS FIRST], true
         +- Aggregate [substring(w_warehouse_name#44, 1, 20), sm_type#58, cc_name#68], [substring(w_warehouse_name#44, 1, 20) AS warehouse_name#0, sm_type#58, cc_name#68, sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 30) THEN 1 ELSE 0 END as bigint)) AS days_30#1L, sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 30) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 60)) THEN 1 ELSE 0 END as bigint)) AS days_31_60#2L, sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 60) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 90)) THEN 1 ELSE 0 END as bigint)) AS days_61_90#3L, sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 90) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 120)) THEN 1 ELSE 0 END as bigint)) AS days_91_120#4L, sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#5L]
            +- Join Inner, ((cs_ship_date_sk#10 = d_date_sk#93) && ((d_month_seq#96 >= 1200) && (d_month_seq#96 <= 1211)))
               :- Join Inner, (cs_call_center_sk#19 = cc_call_center_sk#62)
               :  :- Join Inner, (cs_ship_mode_sk#21 = sm_ship_mode_sk#56)
               :  :  :- Join Inner, (cs_warehouse_sk#22 = w_warehouse_sk#42)
               :  :  :  :- SubqueryAlias `cs`
               :  :  :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
               :  :  :  :     +- Relation[cs_sold_date_sk#8,cs_sold_time_sk#9,cs_ship_date_sk#10,cs_bill_customer_sk#11,cs_bill_cdemo_sk#12,cs_bill_hdemo_sk#13,cs_bill_addr_sk#14,cs_ship_customer_sk#15,cs_ship_cdemo_sk#16,cs_ship_hdemo_sk#17,cs_ship_addr_sk#18,cs_call_center_sk#19,cs_catalog_page_sk#20,cs_ship_mode_sk#21,cs_warehouse_sk#22,cs_item_sk#23,cs_promo_sk#24,cs_order_number#25,cs_quantity#26,cs_wholesale_cost#27,cs_list_price#28,cs_sales_price#29,cs_ext_discount_amt#30,cs_ext_sales_price#31,... 10 more fields] parquet
               :  :  :  +- SubqueryAlias `w`
               :  :  :     +- SubqueryAlias `tpcds`.`warehouse`
               :  :  :        +- Relation[w_warehouse_sk#42,w_warehouse_id#43,w_warehouse_name#44,w_warehouse_sq_ft#45,w_street_number#46,w_street_name#47,w_street_type#48,w_suite_number#49,w_city#50,w_county#51,w_state#52,w_zip#53,w_country#54,w_gmt_offset#55] parquet
               :  :  +- SubqueryAlias `sm`
               :  :     +- SubqueryAlias `tpcds`.`ship_mode`
               :  :        +- Relation[sm_ship_mode_sk#56,sm_ship_mode_id#57,sm_type#58,sm_code#59,sm_carrier#60,sm_contract#61] parquet
               :  +- SubqueryAlias `cc`
               :     +- SubqueryAlias `tpcds`.`call_center`
               :        +- Relation[cc_call_center_sk#62,cc_call_center_id#63,cc_rec_start_date#64,cc_rec_end_date#65,cc_closed_date_sk#66,cc_open_date_sk#67,cc_name#68,cc_class#69,cc_employees#70,cc_sq_ft#71,cc_hours#72,cc_manager#73,cc_mkt_id#74,cc_mkt_class#75,cc_mkt_desc#76,cc_market_manager#77,cc_division#78,cc_division_name#79,cc_company#80,cc_company_name#81,cc_street_number#82,cc_street_name#83,cc_street_type#84,cc_suite_number#85,... 7 more fields] parquet
               +- SubqueryAlias `d`
                  +- SubqueryAlias `tpcds`.`date_dim`
                     +- Relation[d_date_sk#93,d_date_id#94,d_date#95,d_month_seq#96,d_week_seq#97,d_quarter_seq#98,d_year#99,d_dow#100,d_moy#101,d_dom#102,d_qoy#103,d_fy_year#104,d_fy_quarter_seq#105,d_fy_week_seq#106,d_day_name#107,d_quarter_name#108,d_holiday#109,d_weekend#110,d_following_holiday#111,d_first_dom#112,d_last_dom#113,d_same_day_ly#114,d_same_day_lq#115,d_current_day#116,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [warehouse_name#0 ASC NULLS FIRST, sm_type#58 ASC NULLS FIRST, cc_name#68 ASC NULLS FIRST], true
      +- Aggregate [substring(w_warehouse_name#44, 1, 20), sm_type#58, cc_name#68], [substring(w_warehouse_name#44, 1, 20) AS warehouse_name#0, sm_type#58, cc_name#68, sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 30) THEN 1 ELSE 0 END as bigint)) AS days_30#1L, sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 30) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 60)) THEN 1 ELSE 0 END as bigint)) AS days_31_60#2L, sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 60) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 90)) THEN 1 ELSE 0 END as bigint)) AS days_61_90#3L, sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 90) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 120)) THEN 1 ELSE 0 END as bigint)) AS days_91_120#4L, sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#5L]
         +- Project [cs_sold_date_sk#8, cs_ship_date_sk#10, w_warehouse_name#44, sm_type#58, cc_name#68]
            +- Join Inner, (cs_ship_date_sk#10 = d_date_sk#93)
               :- Project [cs_sold_date_sk#8, cs_ship_date_sk#10, w_warehouse_name#44, sm_type#58, cc_name#68]
               :  +- Join Inner, (cs_call_center_sk#19 = cc_call_center_sk#62)
               :     :- Project [cs_sold_date_sk#8, cs_ship_date_sk#10, cs_call_center_sk#19, w_warehouse_name#44, sm_type#58]
               :     :  +- Join Inner, (cs_ship_mode_sk#21 = sm_ship_mode_sk#56)
               :     :     :- Project [cs_sold_date_sk#8, cs_ship_date_sk#10, cs_call_center_sk#19, cs_ship_mode_sk#21, w_warehouse_name#44]
               :     :     :  +- Join Inner, (cs_warehouse_sk#22 = w_warehouse_sk#42)
               :     :     :     :- Project [cs_sold_date_sk#8, cs_ship_date_sk#10, cs_call_center_sk#19, cs_ship_mode_sk#21, cs_warehouse_sk#22]
               :     :     :     :  +- Filter (((isnotnull(cs_warehouse_sk#22) && isnotnull(cs_ship_mode_sk#21)) && isnotnull(cs_call_center_sk#19)) && isnotnull(cs_ship_date_sk#10))
               :     :     :     :     +- Relation[cs_sold_date_sk#8,cs_sold_time_sk#9,cs_ship_date_sk#10,cs_bill_customer_sk#11,cs_bill_cdemo_sk#12,cs_bill_hdemo_sk#13,cs_bill_addr_sk#14,cs_ship_customer_sk#15,cs_ship_cdemo_sk#16,cs_ship_hdemo_sk#17,cs_ship_addr_sk#18,cs_call_center_sk#19,cs_catalog_page_sk#20,cs_ship_mode_sk#21,cs_warehouse_sk#22,cs_item_sk#23,cs_promo_sk#24,cs_order_number#25,cs_quantity#26,cs_wholesale_cost#27,cs_list_price#28,cs_sales_price#29,cs_ext_discount_amt#30,cs_ext_sales_price#31,... 10 more fields] parquet
               :     :     :     +- Project [w_warehouse_sk#42, w_warehouse_name#44]
               :     :     :        +- Filter isnotnull(w_warehouse_sk#42)
               :     :     :           +- Relation[w_warehouse_sk#42,w_warehouse_id#43,w_warehouse_name#44,w_warehouse_sq_ft#45,w_street_number#46,w_street_name#47,w_street_type#48,w_suite_number#49,w_city#50,w_county#51,w_state#52,w_zip#53,w_country#54,w_gmt_offset#55] parquet
               :     :     +- Project [sm_ship_mode_sk#56, sm_type#58]
               :     :        +- Filter isnotnull(sm_ship_mode_sk#56)
               :     :           +- Relation[sm_ship_mode_sk#56,sm_ship_mode_id#57,sm_type#58,sm_code#59,sm_carrier#60,sm_contract#61] parquet
               :     +- Project [cc_call_center_sk#62, cc_name#68]
               :        +- Filter isnotnull(cc_call_center_sk#62)
               :           +- Relation[cc_call_center_sk#62,cc_call_center_id#63,cc_rec_start_date#64,cc_rec_end_date#65,cc_closed_date_sk#66,cc_open_date_sk#67,cc_name#68,cc_class#69,cc_employees#70,cc_sq_ft#71,cc_hours#72,cc_manager#73,cc_mkt_id#74,cc_mkt_class#75,cc_mkt_desc#76,cc_market_manager#77,cc_division#78,cc_division_name#79,cc_company#80,cc_company_name#81,cc_street_number#82,cc_street_name#83,cc_street_type#84,cc_suite_number#85,... 7 more fields] parquet
               +- Project [d_date_sk#93]
                  +- Filter (((isnotnull(d_month_seq#96) && (d_month_seq#96 >= 1200)) && (d_month_seq#96 <= 1211)) && isnotnull(d_date_sk#93))
                     +- Relation[d_date_sk#93,d_date_id#94,d_date#95,d_month_seq#96,d_week_seq#97,d_quarter_seq#98,d_year#99,d_dow#100,d_moy#101,d_dom#102,d_qoy#103,d_fy_year#104,d_fy_quarter_seq#105,d_fy_week_seq#106,d_day_name#107,d_quarter_name#108,d_holiday#109,d_weekend#110,d_following_holiday#111,d_first_dom#112,d_last_dom#113,d_same_day_ly#114,d_same_day_lq#115,d_current_day#116,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[warehouse_name#0 ASC NULLS FIRST,sm_type#58 ASC NULLS FIRST,cc_name#68 ASC NULLS FIRST], output=[warehouse_name#0,sm_type#58,cc_name#68,days_30#1L,days_31_60#2L,days_61_90#3L,days_91_120#4L,above120_days#5L])
+- *(6) HashAggregate(keys=[substring(w_warehouse_name#44, 1, 20)#129, sm_type#58, cc_name#68], functions=[sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 30) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 30) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 60)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 60) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 90)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 90) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 120)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 120) THEN 1 ELSE 0 END as bigint))], output=[warehouse_name#0, sm_type#58, cc_name#68, days_30#1L, days_31_60#2L, days_61_90#3L, days_91_120#4L, above120_days#5L])
   +- Exchange hashpartitioning(substring(w_warehouse_name#44, 1, 20)#129, sm_type#58, cc_name#68, 200)
      +- *(5) HashAggregate(keys=[substring(w_warehouse_name#44, 1, 20) AS substring(w_warehouse_name#44, 1, 20)#129, sm_type#58, cc_name#68], functions=[partial_sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 30) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 30) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 60)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 60) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 90)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 90) && ((cs_ship_date_sk#10 - cs_sold_date_sk#8) <= 120)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN ((cs_ship_date_sk#10 - cs_sold_date_sk#8) > 120) THEN 1 ELSE 0 END as bigint))], output=[substring(w_warehouse_name#44, 1, 20)#129, sm_type#58, cc_name#68, sum#135L, sum#136L, sum#137L, sum#138L, sum#139L])
         +- *(5) Project [cs_sold_date_sk#8, cs_ship_date_sk#10, w_warehouse_name#44, sm_type#58, cc_name#68]
            +- *(5) BroadcastHashJoin [cs_ship_date_sk#10], [d_date_sk#93], Inner, BuildRight
               :- *(5) Project [cs_sold_date_sk#8, cs_ship_date_sk#10, w_warehouse_name#44, sm_type#58, cc_name#68]
               :  +- *(5) BroadcastHashJoin [cs_call_center_sk#19], [cc_call_center_sk#62], Inner, BuildRight
               :     :- *(5) Project [cs_sold_date_sk#8, cs_ship_date_sk#10, cs_call_center_sk#19, w_warehouse_name#44, sm_type#58]
               :     :  +- *(5) BroadcastHashJoin [cs_ship_mode_sk#21], [sm_ship_mode_sk#56], Inner, BuildRight
               :     :     :- *(5) Project [cs_sold_date_sk#8, cs_ship_date_sk#10, cs_call_center_sk#19, cs_ship_mode_sk#21, w_warehouse_name#44]
               :     :     :  +- *(5) BroadcastHashJoin [cs_warehouse_sk#22], [w_warehouse_sk#42], Inner, BuildRight
               :     :     :     :- *(5) Project [cs_sold_date_sk#8, cs_ship_date_sk#10, cs_call_center_sk#19, cs_ship_mode_sk#21, cs_warehouse_sk#22]
               :     :     :     :  +- *(5) Filter (((isnotnull(cs_warehouse_sk#22) && isnotnull(cs_ship_mode_sk#21)) && isnotnull(cs_call_center_sk#19)) && isnotnull(cs_ship_date_sk#10))
               :     :     :     :     +- *(5) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#8,cs_ship_date_sk#10,cs_call_center_sk#19,cs_ship_mode_sk#21,cs_warehouse_sk#22] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_warehouse_sk), IsNotNull(cs_ship_mode_sk), IsNotNull(cs_call_center_sk), IsNotNull(..., ReadSchema: struct<cs_sold_date_sk:int,cs_ship_date_sk:int,cs_call_center_sk:int,cs_ship_mode_sk:int,cs_wareh...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(1) Project [w_warehouse_sk#42, w_warehouse_name#44]
               :     :     :           +- *(1) Filter isnotnull(w_warehouse_sk#42)
               :     :     :              +- *(1) FileScan parquet tpcds.warehouse[w_warehouse_sk#42,w_warehouse_name#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(2) Project [sm_ship_mode_sk#56, sm_type#58]
               :     :           +- *(2) Filter isnotnull(sm_ship_mode_sk#56)
               :     :              +- *(2) FileScan parquet tpcds.ship_mode[sm_ship_mode_sk#56,sm_type#58] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sm_ship_mode_sk)], ReadSchema: struct<sm_ship_mode_sk:int,sm_type:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(3) Project [cc_call_center_sk#62, cc_name#68]
               :           +- *(3) Filter isnotnull(cc_call_center_sk#62)
               :              +- *(3) FileScan parquet tpcds.call_center[cc_call_center_sk#62,cc_name#68] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(4) Project [d_date_sk#93]
                     +- *(4) Filter (((isnotnull(d_month_seq#96) && (d_month_seq#96 >= 1200)) && (d_month_seq#96 <= 1211)) && isnotnull(d_date_sk#93))
                        +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#93,d_month_seq#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
Time taken: 3.819 seconds, Fetched 1 row(s)
