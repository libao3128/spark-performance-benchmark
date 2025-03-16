== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['count('cs_order_number) ASC NULLS FIRST], true
      +- 'Project ['count('cs_order_number) AS order_count#0, 'sum('cs_ext_ship_cost) AS total_shipping_cost#1, 'sum('cs_net_profit) AS total_net_profit#2]
         +- 'Filter ((((('d_date >= cast(2002-2-01 as date)) && ('d_date <= 'date_add(cast(2002-2-01 as date), 60))) && ('cs1.cs_ship_date_sk = 'd_date_sk)) && (('cs1.cs_ship_addr_sk = 'ca_address_sk) && ('ca_state = GA))) && ((('cs1.cs_call_center_sk = 'cc_call_center_sk) && 'cc_county IN (Williamson County,Williamson County,Williamson County,Williamson County,Williamson County)) && (exists#3 [] && NOT exists#4 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter (('cs1.cs_order_number = 'cs2.cs_order_number) && NOT ('cs1.cs_warehouse_sk = 'cs2.cs_warehouse_sk))
            :  :     +- 'SubqueryAlias `cs2`
            :  :        +- 'UnresolvedRelation `catalog_sales`
            :  +- 'Project [*]
            :     +- 'Filter ('cs1.cs_order_number = 'cr1.cr_order_number)
            :        +- 'SubqueryAlias `cr1`
            :           +- 'UnresolvedRelation `catalog_returns`
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'SubqueryAlias `cs1`
               :  :  :  +- 'UnresolvedRelation `catalog_sales`
               :  :  +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `customer_address`
               +- 'UnresolvedRelation `call_center`

== Analyzed Logical Plan ==
order_count: bigint, total_shipping_cost: double, total_net_profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [order_count#0L, total_shipping_cost#1, total_net_profit#2]
      +- Sort [order_count#0L ASC NULLS FIRST], true
         +- Aggregate [count(distinct cs_order_number#24) AS order_count#0L, sum(cs_ext_ship_cost#35) AS total_shipping_cost#1, sum(cs_net_profit#40) AS total_net_profit#2]
            +- Filter (((((d_date#43 >= cast(cast(2002-2-01 as date) as string)) && (d_date#43 <= cast(date_add(cast(2002-2-01 as date), 60) as string))) && (cs_ship_date_sk#9 = d_date_sk#41)) && ((cs_ship_addr_sk#17 = ca_address_sk#69) && (ca_state#77 = GA))) && (((cs_call_center_sk#18 = cc_call_center_sk#82) && cc_county#107 IN (Williamson County,Williamson County,Williamson County,Williamson County,Williamson County)) && (exists#3 [cs_order_number#24 && cs_warehouse_sk#21] && NOT exists#4 [cs_order_number#24])))
               :  :- Project [cs_sold_date_sk#7, cs_sold_time_sk#8, cs_ship_date_sk#9, cs_bill_customer_sk#10, cs_bill_cdemo_sk#11, cs_bill_hdemo_sk#12, cs_bill_addr_sk#13, cs_ship_customer_sk#14, cs_ship_cdemo_sk#15, cs_ship_hdemo_sk#16, cs_ship_addr_sk#17, cs_call_center_sk#18, cs_catalog_page_sk#19, cs_ship_mode_sk#20, cs_warehouse_sk#21, cs_item_sk#22, cs_promo_sk#23, cs_order_number#24, cs_quantity#25, cs_wholesale_cost#26, cs_list_price#27, cs_sales_price#28, cs_ext_discount_amt#29, cs_ext_sales_price#30, ... 10 more fields]
               :  :  +- Filter ((outer(cs_order_number#24) = cs_order_number#24) && NOT (outer(cs_warehouse_sk#21) = cs_warehouse_sk#21))
               :  :     +- SubqueryAlias `cs2`
               :  :        +- SubqueryAlias `tpcds`.`catalog_sales`
               :  :           +- Relation[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
               :  +- Project [cr_returned_date_sk#113, cr_returned_time_sk#114, cr_item_sk#115, cr_refunded_customer_sk#116, cr_refunded_cdemo_sk#117, cr_refunded_hdemo_sk#118, cr_refunded_addr_sk#119, cr_returning_customer_sk#120, cr_returning_cdemo_sk#121, cr_returning_hdemo_sk#122, cr_returning_addr_sk#123, cr_call_center_sk#124, cr_catalog_page_sk#125, cr_ship_mode_sk#126, cr_warehouse_sk#127, cr_reason_sk#128, cr_order_number#129, cr_return_quantity#130, cr_return_amount#131, cr_return_tax#132, cr_return_amt_inc_tax#133, cr_fee#134, cr_return_ship_cost#135, cr_refunded_cash#136, ... 3 more fields]
               :     +- Filter (outer(cs_order_number#24) = cr_order_number#129)
               :        +- SubqueryAlias `cr1`
               :           +- SubqueryAlias `tpcds`.`catalog_returns`
               :              +- Relation[cr_returned_date_sk#113,cr_returned_time_sk#114,cr_item_sk#115,cr_refunded_customer_sk#116,cr_refunded_cdemo_sk#117,cr_refunded_hdemo_sk#118,cr_refunded_addr_sk#119,cr_returning_customer_sk#120,cr_returning_cdemo_sk#121,cr_returning_hdemo_sk#122,cr_returning_addr_sk#123,cr_call_center_sk#124,cr_catalog_page_sk#125,cr_ship_mode_sk#126,cr_warehouse_sk#127,cr_reason_sk#128,cr_order_number#129,cr_return_quantity#130,cr_return_amount#131,cr_return_tax#132,cr_return_amt_inc_tax#133,cr_fee#134,cr_return_ship_cost#135,cr_refunded_cash#136,... 3 more fields] parquet
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias `cs1`
                  :  :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
                  :  :  :     +- Relation[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
                  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :     +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :     +- Relation[ca_address_sk#69,ca_address_id#70,ca_street_number#71,ca_street_name#72,ca_street_type#73,ca_suite_number#74,ca_city#75,ca_county#76,ca_state#77,ca_zip#78,ca_country#79,ca_gmt_offset#80,ca_location_type#81] parquet
                  +- SubqueryAlias `tpcds`.`call_center`
                     +- Relation[cc_call_center_sk#82,cc_call_center_id#83,cc_rec_start_date#84,cc_rec_end_date#85,cc_closed_date_sk#86,cc_open_date_sk#87,cc_name#88,cc_class#89,cc_employees#90,cc_sq_ft#91,cc_hours#92,cc_manager#93,cc_mkt_id#94,cc_mkt_class#95,cc_mkt_desc#96,cc_market_manager#97,cc_division#98,cc_division_name#99,cc_company#100,cc_company_name#101,cc_street_number#102,cc_street_name#103,cc_street_type#104,cc_suite_number#105,... 7 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [order_count#0L ASC NULLS FIRST], true
      +- Aggregate [count(distinct cs_order_number#24) AS order_count#0L, sum(cs_ext_ship_cost#35) AS total_shipping_cost#1, sum(cs_net_profit#40) AS total_net_profit#2]
         +- Project [cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
            +- Join Inner, (cs_call_center_sk#18 = cc_call_center_sk#82)
               :- Project [cs_call_center_sk#18, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
               :  +- Join Inner, (cs_ship_addr_sk#17 = ca_address_sk#69)
               :     :- Project [cs_ship_addr_sk#17, cs_call_center_sk#18, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
               :     :  +- Join Inner, (cs_ship_date_sk#9 = d_date_sk#41)
               :     :     :- Join LeftAnti, (cs_order_number#24 = cr_order_number#129)
               :     :     :  :- Project [cs_ship_date_sk#9, cs_ship_addr_sk#17, cs_call_center_sk#18, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
               :     :     :  :  +- Join LeftSemi, ((cs_order_number#24 = cs_order_number#24#155) && NOT (cs_warehouse_sk#21 = cs_warehouse_sk#21#167))
               :     :     :  :     :- Project [cs_ship_date_sk#9, cs_ship_addr_sk#17, cs_call_center_sk#18, cs_warehouse_sk#21, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
               :     :     :  :     :  +- Filter ((isnotnull(cs_ship_date_sk#9) && isnotnull(cs_ship_addr_sk#17)) && isnotnull(cs_call_center_sk#18))
               :     :     :  :     :     +- Relation[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
               :     :     :  :     +- Project [cs_warehouse_sk#21 AS cs_warehouse_sk#21#167, cs_order_number#24 AS cs_order_number#24#155]
               :     :     :  :        +- Relation[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
               :     :     :  +- Project [cr_order_number#129]
               :     :     :     +- Relation[cr_returned_date_sk#113,cr_returned_time_sk#114,cr_item_sk#115,cr_refunded_customer_sk#116,cr_refunded_cdemo_sk#117,cr_refunded_hdemo_sk#118,cr_refunded_addr_sk#119,cr_returning_customer_sk#120,cr_returning_cdemo_sk#121,cr_returning_hdemo_sk#122,cr_returning_addr_sk#123,cr_call_center_sk#124,cr_catalog_page_sk#125,cr_ship_mode_sk#126,cr_warehouse_sk#127,cr_reason_sk#128,cr_order_number#129,cr_return_quantity#130,cr_return_amount#131,cr_return_tax#132,cr_return_amt_inc_tax#133,cr_fee#134,cr_return_ship_cost#135,cr_refunded_cash#136,... 3 more fields] parquet
               :     :     +- Project [d_date_sk#41]
               :     :        +- Filter (((isnotnull(d_date#43) && (d_date#43 >= 2002-02-01)) && (d_date#43 <= 2002-04-02)) && isnotnull(d_date_sk#41))
               :     :           +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
               :     +- Project [ca_address_sk#69]
               :        +- Filter ((isnotnull(ca_state#77) && (ca_state#77 = GA)) && isnotnull(ca_address_sk#69))
               :           +- Relation[ca_address_sk#69,ca_address_id#70,ca_street_number#71,ca_street_name#72,ca_street_type#73,ca_suite_number#74,ca_city#75,ca_county#76,ca_state#77,ca_zip#78,ca_country#79,ca_gmt_offset#80,ca_location_type#81] parquet
               +- Project [cc_call_center_sk#82]
                  +- Filter ((isnotnull(cc_county#107) && (cc_county#107 = Williamson County)) && isnotnull(cc_call_center_sk#82))
                     +- Relation[cc_call_center_sk#82,cc_call_center_id#83,cc_rec_start_date#84,cc_rec_end_date#85,cc_closed_date_sk#86,cc_open_date_sk#87,cc_name#88,cc_class#89,cc_employees#90,cc_sq_ft#91,cc_hours#92,cc_manager#93,cc_mkt_id#94,cc_mkt_class#95,cc_mkt_desc#96,cc_market_manager#97,cc_division#98,cc_division_name#99,cc_company#100,cc_company_name#101,cc_street_number#102,cc_street_name#103,cc_street_type#104,cc_suite_number#105,... 7 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[order_count#0L ASC NULLS FIRST], output=[order_count#0L,total_shipping_cost#1,total_net_profit#2])
+- *(8) HashAggregate(keys=[], functions=[sum(cs_ext_ship_cost#35), sum(cs_net_profit#40), count(distinct cs_order_number#24)], output=[order_count#0L, total_shipping_cost#1, total_net_profit#2])
   +- Exchange SinglePartition
      +- *(7) HashAggregate(keys=[], functions=[merge_sum(cs_ext_ship_cost#35), merge_sum(cs_net_profit#40), partial_count(distinct cs_order_number#24)], output=[sum#180, sum#182, count#185L])
         +- *(7) HashAggregate(keys=[cs_order_number#24], functions=[merge_sum(cs_ext_ship_cost#35), merge_sum(cs_net_profit#40)], output=[cs_order_number#24, sum#180, sum#182])
            +- Exchange hashpartitioning(cs_order_number#24, 200)
               +- *(6) HashAggregate(keys=[cs_order_number#24], functions=[partial_sum(cs_ext_ship_cost#35), partial_sum(cs_net_profit#40)], output=[cs_order_number#24, sum#180, sum#182])
                  +- *(6) Project [cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
                     +- *(6) BroadcastHashJoin [cs_call_center_sk#18], [cc_call_center_sk#82], Inner, BuildRight
                        :- *(6) Project [cs_call_center_sk#18, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
                        :  +- *(6) BroadcastHashJoin [cs_ship_addr_sk#17], [ca_address_sk#69], Inner, BuildRight
                        :     :- *(6) Project [cs_ship_addr_sk#17, cs_call_center_sk#18, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
                        :     :  +- *(6) BroadcastHashJoin [cs_ship_date_sk#9], [d_date_sk#41], Inner, BuildRight
                        :     :     :- *(6) BroadcastHashJoin [cs_order_number#24], [cr_order_number#129], LeftAnti, BuildRight
                        :     :     :  :- *(6) Project [cs_ship_date_sk#9, cs_ship_addr_sk#17, cs_call_center_sk#18, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
                        :     :     :  :  +- *(6) BroadcastHashJoin [cs_order_number#24], [cs_order_number#24#155], LeftSemi, BuildRight, NOT (cs_warehouse_sk#21 = cs_warehouse_sk#21#167)
                        :     :     :  :     :- *(6) Project [cs_ship_date_sk#9, cs_ship_addr_sk#17, cs_call_center_sk#18, cs_warehouse_sk#21, cs_order_number#24, cs_ext_ship_cost#35, cs_net_profit#40]
                        :     :     :  :     :  +- *(6) Filter ((isnotnull(cs_ship_date_sk#9) && isnotnull(cs_ship_addr_sk#17)) && isnotnull(cs_call_center_sk#18))
                        :     :     :  :     :     +- *(6) FileScan parquet tpcds.catalog_sales[cs_ship_date_sk#9,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_warehouse_sk#21,cs_order_number#24,cs_ext_ship_cost#35,cs_net_profit#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_ship_date_sk), IsNotNull(cs_ship_addr_sk), IsNotNull(cs_call_center_sk)], ReadSchema: struct<cs_ship_date_sk:int,cs_ship_addr_sk:int,cs_call_center_sk:int,cs_warehouse_sk:int,cs_order...
                        :     :     :  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, true] as bigint)))
                        :     :     :  :        +- *(1) Project [cs_warehouse_sk#21 AS cs_warehouse_sk#21#167, cs_order_number#24 AS cs_order_number#24#155]
                        :     :     :  :           +- *(1) FileScan parquet tpcds.catalog_sales[cs_warehouse_sk#21,cs_order_number#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<cs_warehouse_sk:int,cs_order_number:int>
                        :     :     :  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     :     :     +- *(2) FileScan parquet tpcds.catalog_returns[cr_order_number#129] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<cr_order_number:int>
                        :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     :        +- *(3) Project [d_date_sk#41]
                        :     :           +- *(3) Filter (((isnotnull(d_date#43) && (d_date#43 >= 2002-02-01)) && (d_date#43 <= 2002-04-02)) && isnotnull(d_date_sk#41))
                        :     :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#41,d_date#43] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2002-02-01), LessThanOrEqual(d_date,2002-04-02), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :        +- *(4) Project [ca_address_sk#69]
                        :           +- *(4) Filter ((isnotnull(ca_state#77) && (ca_state#77 = GA)) && isnotnull(ca_address_sk#69))
                        :              +- *(4) FileScan parquet tpcds.customer_address[ca_address_sk#69,ca_state#77] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,GA), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                           +- *(5) Project [cc_call_center_sk#82]
                              +- *(5) Filter ((isnotnull(cc_county#107) && (cc_county#107 = Williamson County)) && isnotnull(cc_call_center_sk#82))
                                 +- *(5) FileScan parquet tpcds.call_center[cc_call_center_sk#82,cc_county#107] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_county), EqualTo(cc_county,Williamson County), IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_county:string>
Time taken: 4.228 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 16 in stream 0 using template query16.tpl
------------------------------------------------------^^^

