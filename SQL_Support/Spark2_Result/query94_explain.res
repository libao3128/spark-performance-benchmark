== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['count('ws_order_number) ASC NULLS FIRST], true
      +- 'Project ['count('ws_order_number) AS order_count#0, 'sum('ws_ext_ship_cost) AS total_shipping_cost#1, 'sum('ws_net_profit) AS total_net_profit#2]
         +- 'Filter ((((('d_date >= cast(1999-2-01 as date)) && ('d_date <= 'date_add(cast(1999-2-01 as date), 60))) && ('ws1.ws_ship_date_sk = 'd_date_sk)) && (('ws1.ws_ship_addr_sk = 'ca_address_sk) && ('ca_state = IL))) && ((('ws1.ws_web_site_sk = 'web_site_sk) && ('web_company_name = pri)) && (exists#3 [] && NOT exists#4 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter (('ws1.ws_order_number = 'ws2.ws_order_number) && NOT ('ws1.ws_warehouse_sk = 'ws2.ws_warehouse_sk))
            :  :     +- 'SubqueryAlias `ws2`
            :  :        +- 'UnresolvedRelation `web_sales`
            :  +- 'Project [*]
            :     +- 'Filter ('ws1.ws_order_number = 'wr1.wr_order_number)
            :        +- 'SubqueryAlias `wr1`
            :           +- 'UnresolvedRelation `web_returns`
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'SubqueryAlias `ws1`
               :  :  :  +- 'UnresolvedRelation `web_sales`
               :  :  +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `customer_address`
               +- 'UnresolvedRelation `web_site`

== Analyzed Logical Plan ==
order_count: bigint, total_shipping_cost: double, total_net_profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [order_count#0L, total_shipping_cost#1, total_net_profit#2]
      +- Sort [order_count#0L ASC NULLS FIRST], true
         +- Aggregate [count(distinct ws_order_number#24) AS order_count#0L, sum(ws_ext_ship_cost#35) AS total_shipping_cost#1, sum(ws_net_profit#40) AS total_net_profit#2]
            +- Filter (((((d_date#43 >= cast(cast(1999-2-01 as date) as string)) && (d_date#43 <= cast(date_add(cast(1999-2-01 as date), 60) as string))) && (ws_ship_date_sk#9 = d_date_sk#41)) && ((ws_ship_addr_sk#18 = ca_address_sk#69) && (ca_state#77 = IL))) && (((ws_web_site_sk#20 = web_site_sk#82) && (web_company_name#96 = pri)) && (exists#3 [ws_order_number#24 && ws_warehouse_sk#22] && NOT exists#4 [ws_order_number#24])))
               :  :- Project [ws_sold_date_sk#7, ws_sold_time_sk#8, ws_ship_date_sk#9, ws_item_sk#10, ws_bill_customer_sk#11, ws_bill_cdemo_sk#12, ws_bill_hdemo_sk#13, ws_bill_addr_sk#14, ws_ship_customer_sk#15, ws_ship_cdemo_sk#16, ws_ship_hdemo_sk#17, ws_ship_addr_sk#18, ws_web_page_sk#19, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22, ws_promo_sk#23, ws_order_number#24, ws_quantity#25, ws_wholesale_cost#26, ws_list_price#27, ws_sales_price#28, ws_ext_discount_amt#29, ws_ext_sales_price#30, ... 10 more fields]
               :  :  +- Filter ((outer(ws_order_number#24) = ws_order_number#24) && NOT (outer(ws_warehouse_sk#22) = ws_warehouse_sk#22))
               :  :     +- SubqueryAlias `ws2`
               :  :        +- SubqueryAlias `tpcds`.`web_sales`
               :  :           +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
               :  +- Project [wr_returned_date_sk#108, wr_returned_time_sk#109, wr_item_sk#110, wr_refunded_customer_sk#111, wr_refunded_cdemo_sk#112, wr_refunded_hdemo_sk#113, wr_refunded_addr_sk#114, wr_returning_customer_sk#115, wr_returning_cdemo_sk#116, wr_returning_hdemo_sk#117, wr_returning_addr_sk#118, wr_web_page_sk#119, wr_reason_sk#120, wr_order_number#121, wr_return_quantity#122, wr_return_amt#123, wr_return_tax#124, wr_return_amt_inc_tax#125, wr_fee#126, wr_return_ship_cost#127, wr_refunded_cash#128, wr_reversed_charge#129, wr_account_credit#130, wr_net_loss#131]
               :     +- Filter (outer(ws_order_number#24) = wr_order_number#121)
               :        +- SubqueryAlias `wr1`
               :           +- SubqueryAlias `tpcds`.`web_returns`
               :              +- Relation[wr_returned_date_sk#108,wr_returned_time_sk#109,wr_item_sk#110,wr_refunded_customer_sk#111,wr_refunded_cdemo_sk#112,wr_refunded_hdemo_sk#113,wr_refunded_addr_sk#114,wr_returning_customer_sk#115,wr_returning_cdemo_sk#116,wr_returning_hdemo_sk#117,wr_returning_addr_sk#118,wr_web_page_sk#119,wr_reason_sk#120,wr_order_number#121,wr_return_quantity#122,wr_return_amt#123,wr_return_tax#124,wr_return_amt_inc_tax#125,wr_fee#126,wr_return_ship_cost#127,wr_refunded_cash#128,wr_reversed_charge#129,wr_account_credit#130,wr_net_loss#131] parquet
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias `ws1`
                  :  :  :  +- SubqueryAlias `tpcds`.`web_sales`
                  :  :  :     +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :     +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :     +- Relation[ca_address_sk#69,ca_address_id#70,ca_street_number#71,ca_street_name#72,ca_street_type#73,ca_suite_number#74,ca_city#75,ca_county#76,ca_state#77,ca_zip#78,ca_country#79,ca_gmt_offset#80,ca_location_type#81] parquet
                  +- SubqueryAlias `tpcds`.`web_site`
                     +- Relation[web_site_sk#82,web_site_id#83,web_rec_start_date#84,web_rec_end_date#85,web_name#86,web_open_date_sk#87,web_close_date_sk#88,web_class#89,web_manager#90,web_mkt_id#91,web_mkt_class#92,web_mkt_desc#93,web_market_manager#94,web_company_id#95,web_company_name#96,web_street_number#97,web_street_name#98,web_street_type#99,web_suite_number#100,web_city#101,web_county#102,web_state#103,web_zip#104,web_country#105,... 2 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [order_count#0L ASC NULLS FIRST], true
      +- Aggregate [count(distinct ws_order_number#24) AS order_count#0L, sum(ws_ext_ship_cost#35) AS total_shipping_cost#1, sum(ws_net_profit#40) AS total_net_profit#2]
         +- Project [ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
            +- Join Inner, (ws_web_site_sk#20 = web_site_sk#82)
               :- Project [ws_web_site_sk#20, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
               :  +- Join Inner, (ws_ship_addr_sk#18 = ca_address_sk#69)
               :     :- Project [ws_ship_addr_sk#18, ws_web_site_sk#20, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
               :     :  +- Join Inner, (ws_ship_date_sk#9 = d_date_sk#41)
               :     :     :- Join LeftAnti, (ws_order_number#24 = wr_order_number#121)
               :     :     :  :- Project [ws_ship_date_sk#9, ws_ship_addr_sk#18, ws_web_site_sk#20, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
               :     :     :  :  +- Join LeftSemi, ((ws_order_number#24 = ws_order_number#24#158) && NOT (ws_warehouse_sk#22 = ws_warehouse_sk#22#166))
               :     :     :  :     :- Project [ws_ship_date_sk#9, ws_ship_addr_sk#18, ws_web_site_sk#20, ws_warehouse_sk#22, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
               :     :     :  :     :  +- Filter ((isnotnull(ws_ship_date_sk#9) && isnotnull(ws_ship_addr_sk#18)) && isnotnull(ws_web_site_sk#20))
               :     :     :  :     :     +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
               :     :     :  :     +- Project [ws_warehouse_sk#22 AS ws_warehouse_sk#22#166, ws_order_number#24 AS ws_order_number#24#158]
               :     :     :  :        +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
               :     :     :  +- Project [wr_order_number#121]
               :     :     :     +- Relation[wr_returned_date_sk#108,wr_returned_time_sk#109,wr_item_sk#110,wr_refunded_customer_sk#111,wr_refunded_cdemo_sk#112,wr_refunded_hdemo_sk#113,wr_refunded_addr_sk#114,wr_returning_customer_sk#115,wr_returning_cdemo_sk#116,wr_returning_hdemo_sk#117,wr_returning_addr_sk#118,wr_web_page_sk#119,wr_reason_sk#120,wr_order_number#121,wr_return_quantity#122,wr_return_amt#123,wr_return_tax#124,wr_return_amt_inc_tax#125,wr_fee#126,wr_return_ship_cost#127,wr_refunded_cash#128,wr_reversed_charge#129,wr_account_credit#130,wr_net_loss#131] parquet
               :     :     +- Project [d_date_sk#41]
               :     :        +- Filter (((isnotnull(d_date#43) && (d_date#43 >= 1999-02-01)) && (d_date#43 <= 1999-04-02)) && isnotnull(d_date_sk#41))
               :     :           +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
               :     +- Project [ca_address_sk#69]
               :        +- Filter ((isnotnull(ca_state#77) && (ca_state#77 = IL)) && isnotnull(ca_address_sk#69))
               :           +- Relation[ca_address_sk#69,ca_address_id#70,ca_street_number#71,ca_street_name#72,ca_street_type#73,ca_suite_number#74,ca_city#75,ca_county#76,ca_state#77,ca_zip#78,ca_country#79,ca_gmt_offset#80,ca_location_type#81] parquet
               +- Project [web_site_sk#82]
                  +- Filter ((isnotnull(web_company_name#96) && (web_company_name#96 = pri)) && isnotnull(web_site_sk#82))
                     +- Relation[web_site_sk#82,web_site_id#83,web_rec_start_date#84,web_rec_end_date#85,web_name#86,web_open_date_sk#87,web_close_date_sk#88,web_class#89,web_manager#90,web_mkt_id#91,web_mkt_class#92,web_mkt_desc#93,web_market_manager#94,web_company_id#95,web_company_name#96,web_street_number#97,web_street_name#98,web_street_type#99,web_suite_number#100,web_city#101,web_county#102,web_state#103,web_zip#104,web_country#105,... 2 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[order_count#0L ASC NULLS FIRST], output=[order_count#0L,total_shipping_cost#1,total_net_profit#2])
+- *(8) HashAggregate(keys=[], functions=[sum(ws_ext_ship_cost#35), sum(ws_net_profit#40), count(distinct ws_order_number#24)], output=[order_count#0L, total_shipping_cost#1, total_net_profit#2])
   +- Exchange SinglePartition
      +- *(7) HashAggregate(keys=[], functions=[merge_sum(ws_ext_ship_cost#35), merge_sum(ws_net_profit#40), partial_count(distinct ws_order_number#24)], output=[sum#172, sum#174, count#177L])
         +- *(7) HashAggregate(keys=[ws_order_number#24], functions=[merge_sum(ws_ext_ship_cost#35), merge_sum(ws_net_profit#40)], output=[ws_order_number#24, sum#172, sum#174])
            +- Exchange hashpartitioning(ws_order_number#24, 200)
               +- *(6) HashAggregate(keys=[ws_order_number#24], functions=[partial_sum(ws_ext_ship_cost#35), partial_sum(ws_net_profit#40)], output=[ws_order_number#24, sum#172, sum#174])
                  +- *(6) Project [ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
                     +- *(6) BroadcastHashJoin [ws_web_site_sk#20], [web_site_sk#82], Inner, BuildRight
                        :- *(6) Project [ws_web_site_sk#20, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
                        :  +- *(6) BroadcastHashJoin [ws_ship_addr_sk#18], [ca_address_sk#69], Inner, BuildRight
                        :     :- *(6) Project [ws_ship_addr_sk#18, ws_web_site_sk#20, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
                        :     :  +- *(6) BroadcastHashJoin [ws_ship_date_sk#9], [d_date_sk#41], Inner, BuildRight
                        :     :     :- *(6) BroadcastHashJoin [ws_order_number#24], [wr_order_number#121], LeftAnti, BuildRight
                        :     :     :  :- *(6) Project [ws_ship_date_sk#9, ws_ship_addr_sk#18, ws_web_site_sk#20, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
                        :     :     :  :  +- *(6) BroadcastHashJoin [ws_order_number#24], [ws_order_number#24#158], LeftSemi, BuildRight, NOT (ws_warehouse_sk#22 = ws_warehouse_sk#22#166)
                        :     :     :  :     :- *(6) Project [ws_ship_date_sk#9, ws_ship_addr_sk#18, ws_web_site_sk#20, ws_warehouse_sk#22, ws_order_number#24, ws_ext_ship_cost#35, ws_net_profit#40]
                        :     :     :  :     :  +- *(6) Filter ((isnotnull(ws_ship_date_sk#9) && isnotnull(ws_ship_addr_sk#18)) && isnotnull(ws_web_site_sk#20))
                        :     :     :  :     :     +- *(6) FileScan parquet tpcds.web_sales[ws_ship_date_sk#9,ws_ship_addr_sk#18,ws_web_site_sk#20,ws_warehouse_sk#22,ws_order_number#24,ws_ext_ship_cost#35,ws_net_profit#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_date_sk), IsNotNull(ws_ship_addr_sk), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_ship_date_sk:int,ws_ship_addr_sk:int,ws_web_site_sk:int,ws_warehouse_sk:int,ws_order_nu...
                        :     :     :  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, true] as bigint)))
                        :     :     :  :        +- *(1) Project [ws_warehouse_sk#22 AS ws_warehouse_sk#22#166, ws_order_number#24 AS ws_order_number#24#158]
                        :     :     :  :           +- *(1) FileScan parquet tpcds.web_sales[ws_warehouse_sk#22,ws_order_number#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                        :     :     :  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     :     :     +- *(2) FileScan parquet tpcds.web_returns[wr_order_number#121] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<wr_order_number:int>
                        :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     :        +- *(3) Project [d_date_sk#41]
                        :     :           +- *(3) Filter (((isnotnull(d_date#43) && (d_date#43 >= 1999-02-01)) && (d_date#43 <= 1999-04-02)) && isnotnull(d_date_sk#41))
                        :     :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#41,d_date#43] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,1999-02-01), LessThanOrEqual(d_date,1999-04-02), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :        +- *(4) Project [ca_address_sk#69]
                        :           +- *(4) Filter ((isnotnull(ca_state#77) && (ca_state#77 = IL)) && isnotnull(ca_address_sk#69))
                        :              +- *(4) FileScan parquet tpcds.customer_address[ca_address_sk#69,ca_state#77] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,IL), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                           +- *(5) Project [web_site_sk#82]
                              +- *(5) Filter ((isnotnull(web_company_name#96) && (web_company_name#96 = pri)) && isnotnull(web_site_sk#82))
                                 +- *(5) FileScan parquet tpcds.web_site[web_site_sk#82,web_company_name#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(web_company_name), EqualTo(web_company_name,pri), IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_company_name:string>
Time taken: 4.136 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 94 in stream 0 using template query94.tpl
------------------------------------------------------^^^

