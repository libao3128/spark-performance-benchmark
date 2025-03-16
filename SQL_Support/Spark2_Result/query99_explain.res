== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('w_warehouse_name, 1, 20) ASC NULLS FIRST, 'sm_type ASC NULLS FIRST, 'cc_name ASC NULLS FIRST], true
      +- 'Aggregate ['substr('w_warehouse_name, 1, 20), 'sm_type, 'cc_name], [unresolvedalias('substr('w_warehouse_name, 1, 20), None), 'sm_type, 'cc_name, 'sum(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END) AS 30_days#0, 'sum(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 30) && (('cs_ship_date_sk - 'cs_sold_date_sk) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1, 'sum(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 60) && (('cs_ship_date_sk - 'cs_sold_date_sk) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2, 'sum(CASE WHEN ((('cs_ship_date_sk - 'cs_sold_date_sk) > 90) && (('cs_ship_date_sk - 'cs_sold_date_sk) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3, 'sum(CASE WHEN (('cs_ship_date_sk - 'cs_sold_date_sk) > 120) THEN 1 ELSE 0 END) AS above120_days#4]
         +- 'Filter ((((('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))) && ('cs_ship_date_sk = 'd_date_sk)) && ('cs_warehouse_sk = 'w_warehouse_sk)) && (('cs_ship_mode_sk = 'sm_ship_mode_sk) && ('cs_call_center_sk = 'cc_call_center_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation `catalog_sales`
               :  :  :  +- 'UnresolvedRelation `warehouse`
               :  :  +- 'UnresolvedRelation `ship_mode`
               :  +- 'UnresolvedRelation `call_center`
               +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
substring(w_warehouse_name, 1, 20): string, sm_type: string, cc_name: string, 30_days: bigint, 31_60_days: bigint, 61_90_days: bigint, 91_120_days: bigint, above120_days: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Project [substring(w_warehouse_name, 1, 20)#125, sm_type#57, cc_name#67, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L]
      +- Sort [substring(w_warehouse_name, 1, 20)#125 ASC NULLS FIRST, sm_type#57 ASC NULLS FIRST, cc_name#67 ASC NULLS FIRST], true
         +- Aggregate [substring(w_warehouse_name#43, 1, 20), sm_type#57, cc_name#67], [substring(w_warehouse_name#43, 1, 20) AS substring(w_warehouse_name, 1, 20)#125, sm_type#57, cc_name#67, sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)) AS 30_days#0L, sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 30) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)) AS 31_60_days#1L, sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 60) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)) AS 61_90_days#2L, sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 90) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)) AS 91_120_days#3L, sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#4L]
            +- Filter (((((d_month_seq#95 >= 1200) && (d_month_seq#95 <= (1200 + 11))) && (cs_ship_date_sk#9 = d_date_sk#92)) && (cs_warehouse_sk#21 = w_warehouse_sk#41)) && ((cs_ship_mode_sk#20 = sm_ship_mode_sk#55) && (cs_call_center_sk#18 = cc_call_center_sk#61)))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
                  :  :  :  :  +- Relation[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`warehouse`
                  :  :  :     +- Relation[w_warehouse_sk#41,w_warehouse_id#42,w_warehouse_name#43,w_warehouse_sq_ft#44,w_street_number#45,w_street_name#46,w_street_type#47,w_suite_number#48,w_city#49,w_county#50,w_state#51,w_zip#52,w_country#53,w_gmt_offset#54] parquet
                  :  :  +- SubqueryAlias `tpcds`.`ship_mode`
                  :  :     +- Relation[sm_ship_mode_sk#55,sm_ship_mode_id#56,sm_type#57,sm_code#58,sm_carrier#59,sm_contract#60] parquet
                  :  +- SubqueryAlias `tpcds`.`call_center`
                  :     +- Relation[cc_call_center_sk#61,cc_call_center_id#62,cc_rec_start_date#63,cc_rec_end_date#64,cc_closed_date_sk#65,cc_open_date_sk#66,cc_name#67,cc_class#68,cc_employees#69,cc_sq_ft#70,cc_hours#71,cc_manager#72,cc_mkt_id#73,cc_mkt_class#74,cc_mkt_desc#75,cc_market_manager#76,cc_division#77,cc_division_name#78,cc_company#79,cc_company_name#80,cc_street_number#81,cc_street_name#82,cc_street_type#83,cc_suite_number#84,... 7 more fields] parquet
                  +- SubqueryAlias `tpcds`.`date_dim`
                     +- Relation[d_date_sk#92,d_date_id#93,d_date#94,d_month_seq#95,d_week_seq#96,d_quarter_seq#97,d_year#98,d_dow#99,d_moy#100,d_dom#101,d_qoy#102,d_fy_year#103,d_fy_quarter_seq#104,d_fy_week_seq#105,d_day_name#106,d_quarter_name#107,d_holiday#108,d_weekend#109,d_following_holiday#110,d_first_dom#111,d_last_dom#112,d_same_day_ly#113,d_same_day_lq#114,d_current_day#115,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substring(w_warehouse_name, 1, 20)#125 ASC NULLS FIRST, sm_type#57 ASC NULLS FIRST, cc_name#67 ASC NULLS FIRST], true
      +- Aggregate [substring(w_warehouse_name#43, 1, 20), sm_type#57, cc_name#67], [substring(w_warehouse_name#43, 1, 20) AS substring(w_warehouse_name, 1, 20)#125, sm_type#57, cc_name#67, sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)) AS 30_days#0L, sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 30) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)) AS 31_60_days#1L, sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 60) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)) AS 61_90_days#2L, sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 90) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)) AS 91_120_days#3L, sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#4L]
         +- Project [cs_sold_date_sk#7, cs_ship_date_sk#9, w_warehouse_name#43, sm_type#57, cc_name#67]
            +- Join Inner, (cs_ship_date_sk#9 = d_date_sk#92)
               :- Project [cs_sold_date_sk#7, cs_ship_date_sk#9, w_warehouse_name#43, sm_type#57, cc_name#67]
               :  +- Join Inner, (cs_call_center_sk#18 = cc_call_center_sk#61)
               :     :- Project [cs_sold_date_sk#7, cs_ship_date_sk#9, cs_call_center_sk#18, w_warehouse_name#43, sm_type#57]
               :     :  +- Join Inner, (cs_ship_mode_sk#20 = sm_ship_mode_sk#55)
               :     :     :- Project [cs_sold_date_sk#7, cs_ship_date_sk#9, cs_call_center_sk#18, cs_ship_mode_sk#20, w_warehouse_name#43]
               :     :     :  +- Join Inner, (cs_warehouse_sk#21 = w_warehouse_sk#41)
               :     :     :     :- Project [cs_sold_date_sk#7, cs_ship_date_sk#9, cs_call_center_sk#18, cs_ship_mode_sk#20, cs_warehouse_sk#21]
               :     :     :     :  +- Filter (((isnotnull(cs_warehouse_sk#21) && isnotnull(cs_ship_mode_sk#20)) && isnotnull(cs_call_center_sk#18)) && isnotnull(cs_ship_date_sk#9))
               :     :     :     :     +- Relation[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
               :     :     :     +- Project [w_warehouse_sk#41, w_warehouse_name#43]
               :     :     :        +- Filter isnotnull(w_warehouse_sk#41)
               :     :     :           +- Relation[w_warehouse_sk#41,w_warehouse_id#42,w_warehouse_name#43,w_warehouse_sq_ft#44,w_street_number#45,w_street_name#46,w_street_type#47,w_suite_number#48,w_city#49,w_county#50,w_state#51,w_zip#52,w_country#53,w_gmt_offset#54] parquet
               :     :     +- Project [sm_ship_mode_sk#55, sm_type#57]
               :     :        +- Filter isnotnull(sm_ship_mode_sk#55)
               :     :           +- Relation[sm_ship_mode_sk#55,sm_ship_mode_id#56,sm_type#57,sm_code#58,sm_carrier#59,sm_contract#60] parquet
               :     +- Project [cc_call_center_sk#61, cc_name#67]
               :        +- Filter isnotnull(cc_call_center_sk#61)
               :           +- Relation[cc_call_center_sk#61,cc_call_center_id#62,cc_rec_start_date#63,cc_rec_end_date#64,cc_closed_date_sk#65,cc_open_date_sk#66,cc_name#67,cc_class#68,cc_employees#69,cc_sq_ft#70,cc_hours#71,cc_manager#72,cc_mkt_id#73,cc_mkt_class#74,cc_mkt_desc#75,cc_market_manager#76,cc_division#77,cc_division_name#78,cc_company#79,cc_company_name#80,cc_street_number#81,cc_street_name#82,cc_street_type#83,cc_suite_number#84,... 7 more fields] parquet
               +- Project [d_date_sk#92]
                  +- Filter (((isnotnull(d_month_seq#95) && (d_month_seq#95 >= 1200)) && (d_month_seq#95 <= 1211)) && isnotnull(d_date_sk#92))
                     +- Relation[d_date_sk#92,d_date_id#93,d_date#94,d_month_seq#95,d_week_seq#96,d_quarter_seq#97,d_year#98,d_dow#99,d_moy#100,d_dom#101,d_qoy#102,d_fy_year#103,d_fy_quarter_seq#104,d_fy_week_seq#105,d_day_name#106,d_quarter_name#107,d_holiday#108,d_weekend#109,d_following_holiday#110,d_first_dom#111,d_last_dom#112,d_same_day_ly#113,d_same_day_lq#114,d_current_day#115,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[substring(w_warehouse_name, 1, 20)#125 ASC NULLS FIRST,sm_type#57 ASC NULLS FIRST,cc_name#67 ASC NULLS FIRST], output=[substring(w_warehouse_name, 1, 20)#125,sm_type#57,cc_name#67,30_days#0L,31_60_days#1L,61_90_days#2L,91_120_days#3L,above120_days#4L])
+- *(6) HashAggregate(keys=[substring(w_warehouse_name#43, 1, 20)#129, sm_type#57, cc_name#67], functions=[sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 30) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 60) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 90) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint))], output=[substring(w_warehouse_name, 1, 20)#125, sm_type#57, cc_name#67, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L])
   +- Exchange hashpartitioning(substring(w_warehouse_name#43, 1, 20)#129, sm_type#57, cc_name#67, 200)
      +- *(5) HashAggregate(keys=[substring(w_warehouse_name#43, 1, 20) AS substring(w_warehouse_name#43, 1, 20)#129, sm_type#57, cc_name#67], functions=[partial_sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 30) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 60) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 90) && ((cs_ship_date_sk#9 - cs_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN ((cs_ship_date_sk#9 - cs_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint))], output=[substring(w_warehouse_name#43, 1, 20)#129, sm_type#57, cc_name#67, sum#135L, sum#136L, sum#137L, sum#138L, sum#139L])
         +- *(5) Project [cs_sold_date_sk#7, cs_ship_date_sk#9, w_warehouse_name#43, sm_type#57, cc_name#67]
            +- *(5) BroadcastHashJoin [cs_ship_date_sk#9], [d_date_sk#92], Inner, BuildRight
               :- *(5) Project [cs_sold_date_sk#7, cs_ship_date_sk#9, w_warehouse_name#43, sm_type#57, cc_name#67]
               :  +- *(5) BroadcastHashJoin [cs_call_center_sk#18], [cc_call_center_sk#61], Inner, BuildRight
               :     :- *(5) Project [cs_sold_date_sk#7, cs_ship_date_sk#9, cs_call_center_sk#18, w_warehouse_name#43, sm_type#57]
               :     :  +- *(5) BroadcastHashJoin [cs_ship_mode_sk#20], [sm_ship_mode_sk#55], Inner, BuildRight
               :     :     :- *(5) Project [cs_sold_date_sk#7, cs_ship_date_sk#9, cs_call_center_sk#18, cs_ship_mode_sk#20, w_warehouse_name#43]
               :     :     :  +- *(5) BroadcastHashJoin [cs_warehouse_sk#21], [w_warehouse_sk#41], Inner, BuildRight
               :     :     :     :- *(5) Project [cs_sold_date_sk#7, cs_ship_date_sk#9, cs_call_center_sk#18, cs_ship_mode_sk#20, cs_warehouse_sk#21]
               :     :     :     :  +- *(5) Filter (((isnotnull(cs_warehouse_sk#21) && isnotnull(cs_ship_mode_sk#20)) && isnotnull(cs_call_center_sk#18)) && isnotnull(cs_ship_date_sk#9))
               :     :     :     :     +- *(5) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#7,cs_ship_date_sk#9,cs_call_center_sk#18,cs_ship_mode_sk#20,cs_warehouse_sk#21] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_warehouse_sk), IsNotNull(cs_ship_mode_sk), IsNotNull(cs_call_center_sk), IsNotNull(..., ReadSchema: struct<cs_sold_date_sk:int,cs_ship_date_sk:int,cs_call_center_sk:int,cs_ship_mode_sk:int,cs_wareh...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(1) Project [w_warehouse_sk#41, w_warehouse_name#43]
               :     :     :           +- *(1) Filter isnotnull(w_warehouse_sk#41)
               :     :     :              +- *(1) FileScan parquet tpcds.warehouse[w_warehouse_sk#41,w_warehouse_name#43] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(2) Project [sm_ship_mode_sk#55, sm_type#57]
               :     :           +- *(2) Filter isnotnull(sm_ship_mode_sk#55)
               :     :              +- *(2) FileScan parquet tpcds.ship_mode[sm_ship_mode_sk#55,sm_type#57] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sm_ship_mode_sk)], ReadSchema: struct<sm_ship_mode_sk:int,sm_type:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(3) Project [cc_call_center_sk#61, cc_name#67]
               :           +- *(3) Filter isnotnull(cc_call_center_sk#61)
               :              +- *(3) FileScan parquet tpcds.call_center[cc_call_center_sk#61,cc_name#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(4) Project [d_date_sk#92]
                     +- *(4) Filter (((isnotnull(d_month_seq#95) && (d_month_seq#95 >= 1200)) && (d_month_seq#95 <= 1211)) && isnotnull(d_date_sk#92))
                        +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#92,d_month_seq#95] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
Time taken: 3.804 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 99 in stream 0 using template query99.tpl
------------------------------------------------------^^^

