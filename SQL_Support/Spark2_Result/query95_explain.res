== Parsed Logical Plan ==
CTE [ws_wh]
:  +- 'SubqueryAlias `ws_wh`
:     +- 'Project ['ws1.ws_order_number, 'ws1.ws_warehouse_sk AS wh1#5, 'ws2.ws_warehouse_sk AS wh2#6]
:        +- 'Filter (('ws1.ws_order_number = 'ws2.ws_order_number) && NOT ('ws1.ws_warehouse_sk = 'ws2.ws_warehouse_sk))
:           +- 'Join Inner
:              :- 'SubqueryAlias `ws1`
:              :  +- 'UnresolvedRelation `web_sales`
:              +- 'SubqueryAlias `ws2`
:                 +- 'UnresolvedRelation `web_sales`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['count('ws_order_number) ASC NULLS FIRST], true
         +- 'Project ['count('ws_order_number) AS order_count#0, 'sum('ws_ext_ship_cost) AS total_shipping_cost#1, 'sum('ws_net_profit) AS total_net_profit#2]
            +- 'Filter ((((('d_date >= 1999-2-01) && ('d_date <= 'date_add(cast(1999-2-01 as date), 60))) && ('ws1.ws_ship_date_sk = 'd_date_sk)) && (('ws1.ws_ship_addr_sk = 'ca_address_sk) && ('ca_state = IL))) && ((('ws1.ws_web_site_sk = 'web_site_sk) && ('web_company_name = pri)) && ('ws1.ws_order_number IN (list#3 []) && 'ws1.ws_order_number IN (list#4 []))))
               :  :- 'Project ['ws_order_number]
               :  :  +- 'UnresolvedRelation `ws_wh`
               :  +- 'Project ['wr_order_number]
               :     +- 'Filter ('wr_order_number = 'ws_wh.ws_order_number)
               :        +- 'Join Inner
               :           :- 'UnresolvedRelation `web_returns`
               :           +- 'UnresolvedRelation `ws_wh`
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
         +- Aggregate [count(distinct ws_order_number#26) AS order_count#0L, sum(ws_ext_ship_cost#37) AS total_shipping_cost#1, sum(ws_net_profit#42) AS total_net_profit#2]
            +- Filter (((((d_date#79 >= 1999-2-01) && (d_date#79 <= cast(date_add(cast(1999-2-01 as date), 60) as string))) && (ws_ship_date_sk#11 = d_date_sk#77)) && ((ws_ship_addr_sk#20 = ca_address_sk#105) && (ca_state#113 = IL))) && (((ws_web_site_sk#22 = web_site_sk#118) && (web_company_name#132 = pri)) && (ws_order_number#26 IN (list#3 []) && ws_order_number#26 IN (list#4 []))))
               :  :- Project [ws_order_number#26]
               :  :  +- SubqueryAlias `ws_wh`
               :  :     +- Project [ws_order_number#26, ws_warehouse_sk#24 AS wh1#5, ws_warehouse_sk#58 AS wh2#6]
               :  :        +- Filter ((ws_order_number#26 = ws_order_number#60) && NOT (ws_warehouse_sk#24 = ws_warehouse_sk#58))
               :  :           +- Join Inner
               :  :              :- SubqueryAlias `ws1`
               :  :              :  +- SubqueryAlias `tpcds`.`web_sales`
               :  :              :     +- Relation[ws_sold_date_sk#9,ws_sold_time_sk#10,ws_ship_date_sk#11,ws_item_sk#12,ws_bill_customer_sk#13,ws_bill_cdemo_sk#14,ws_bill_hdemo_sk#15,ws_bill_addr_sk#16,ws_ship_customer_sk#17,ws_ship_cdemo_sk#18,ws_ship_hdemo_sk#19,ws_ship_addr_sk#20,ws_web_page_sk#21,ws_web_site_sk#22,ws_ship_mode_sk#23,ws_warehouse_sk#24,ws_promo_sk#25,ws_order_number#26,ws_quantity#27,ws_wholesale_cost#28,ws_list_price#29,ws_sales_price#30,ws_ext_discount_amt#31,ws_ext_sales_price#32,... 10 more fields] parquet
               :  :              +- SubqueryAlias `ws2`
               :  :                 +- SubqueryAlias `tpcds`.`web_sales`
               :  :                    +- Relation[ws_sold_date_sk#43,ws_sold_time_sk#44,ws_ship_date_sk#45,ws_item_sk#46,ws_bill_customer_sk#47,ws_bill_cdemo_sk#48,ws_bill_hdemo_sk#49,ws_bill_addr_sk#50,ws_ship_customer_sk#51,ws_ship_cdemo_sk#52,ws_ship_hdemo_sk#53,ws_ship_addr_sk#54,ws_web_page_sk#55,ws_web_site_sk#56,ws_ship_mode_sk#57,ws_warehouse_sk#58,ws_promo_sk#59,ws_order_number#60,ws_quantity#61,ws_wholesale_cost#62,ws_list_price#63,ws_sales_price#64,ws_ext_discount_amt#65,ws_ext_sales_price#66,... 10 more fields] parquet
               :  +- Project [wr_order_number#157]
               :     +- Filter (wr_order_number#157 = ws_order_number#26)
               :        +- Join Inner
               :           :- SubqueryAlias `tpcds`.`web_returns`
               :           :  +- Relation[wr_returned_date_sk#144,wr_returned_time_sk#145,wr_item_sk#146,wr_refunded_customer_sk#147,wr_refunded_cdemo_sk#148,wr_refunded_hdemo_sk#149,wr_refunded_addr_sk#150,wr_returning_customer_sk#151,wr_returning_cdemo_sk#152,wr_returning_hdemo_sk#153,wr_returning_addr_sk#154,wr_web_page_sk#155,wr_reason_sk#156,wr_order_number#157,wr_return_quantity#158,wr_return_amt#159,wr_return_tax#160,wr_return_amt_inc_tax#161,wr_fee#162,wr_return_ship_cost#163,wr_refunded_cash#164,wr_reversed_charge#165,wr_account_credit#166,wr_net_loss#167] parquet
               :           +- SubqueryAlias `ws_wh`
               :              +- Project [ws_order_number#26, ws_warehouse_sk#24 AS wh1#5, ws_warehouse_sk#58 AS wh2#6]
               :                 +- Filter ((ws_order_number#26 = ws_order_number#60) && NOT (ws_warehouse_sk#24 = ws_warehouse_sk#58))
               :                    +- Join Inner
               :                       :- SubqueryAlias `ws1`
               :                       :  +- SubqueryAlias `tpcds`.`web_sales`
               :                       :     +- Relation[ws_sold_date_sk#9,ws_sold_time_sk#10,ws_ship_date_sk#11,ws_item_sk#12,ws_bill_customer_sk#13,ws_bill_cdemo_sk#14,ws_bill_hdemo_sk#15,ws_bill_addr_sk#16,ws_ship_customer_sk#17,ws_ship_cdemo_sk#18,ws_ship_hdemo_sk#19,ws_ship_addr_sk#20,ws_web_page_sk#21,ws_web_site_sk#22,ws_ship_mode_sk#23,ws_warehouse_sk#24,ws_promo_sk#25,ws_order_number#26,ws_quantity#27,ws_wholesale_cost#28,ws_list_price#29,ws_sales_price#30,ws_ext_discount_amt#31,ws_ext_sales_price#32,... 10 more fields] parquet
               :                       +- SubqueryAlias `ws2`
               :                          +- SubqueryAlias `tpcds`.`web_sales`
               :                             +- Relation[ws_sold_date_sk#43,ws_sold_time_sk#44,ws_ship_date_sk#45,ws_item_sk#46,ws_bill_customer_sk#47,ws_bill_cdemo_sk#48,ws_bill_hdemo_sk#49,ws_bill_addr_sk#50,ws_ship_customer_sk#51,ws_ship_cdemo_sk#52,ws_ship_hdemo_sk#53,ws_ship_addr_sk#54,ws_web_page_sk#55,ws_web_site_sk#56,ws_ship_mode_sk#57,ws_warehouse_sk#58,ws_promo_sk#59,ws_order_number#60,ws_quantity#61,ws_wholesale_cost#62,ws_list_price#63,ws_sales_price#64,ws_ext_discount_amt#65,ws_ext_sales_price#66,... 10 more fields] parquet
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias `ws1`
                  :  :  :  +- SubqueryAlias `tpcds`.`web_sales`
                  :  :  :     +- Relation[ws_sold_date_sk#9,ws_sold_time_sk#10,ws_ship_date_sk#11,ws_item_sk#12,ws_bill_customer_sk#13,ws_bill_cdemo_sk#14,ws_bill_hdemo_sk#15,ws_bill_addr_sk#16,ws_ship_customer_sk#17,ws_ship_cdemo_sk#18,ws_ship_hdemo_sk#19,ws_ship_addr_sk#20,ws_web_page_sk#21,ws_web_site_sk#22,ws_ship_mode_sk#23,ws_warehouse_sk#24,ws_promo_sk#25,ws_order_number#26,ws_quantity#27,ws_wholesale_cost#28,ws_list_price#29,ws_sales_price#30,ws_ext_discount_amt#31,ws_ext_sales_price#32,... 10 more fields] parquet
                  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :     +- Relation[d_date_sk#77,d_date_id#78,d_date#79,d_month_seq#80,d_week_seq#81,d_quarter_seq#82,d_year#83,d_dow#84,d_moy#85,d_dom#86,d_qoy#87,d_fy_year#88,d_fy_quarter_seq#89,d_fy_week_seq#90,d_day_name#91,d_quarter_name#92,d_holiday#93,d_weekend#94,d_following_holiday#95,d_first_dom#96,d_last_dom#97,d_same_day_ly#98,d_same_day_lq#99,d_current_day#100,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :     +- Relation[ca_address_sk#105,ca_address_id#106,ca_street_number#107,ca_street_name#108,ca_street_type#109,ca_suite_number#110,ca_city#111,ca_county#112,ca_state#113,ca_zip#114,ca_country#115,ca_gmt_offset#116,ca_location_type#117] parquet
                  +- SubqueryAlias `tpcds`.`web_site`
                     +- Relation[web_site_sk#118,web_site_id#119,web_rec_start_date#120,web_rec_end_date#121,web_name#122,web_open_date_sk#123,web_close_date_sk#124,web_class#125,web_manager#126,web_mkt_id#127,web_mkt_class#128,web_mkt_desc#129,web_market_manager#130,web_company_id#131,web_company_name#132,web_street_number#133,web_street_name#134,web_street_type#135,web_suite_number#136,web_city#137,web_county#138,web_state#139,web_zip#140,web_country#141,... 2 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [order_count#0L ASC NULLS FIRST], true
      +- Aggregate [count(distinct ws_order_number#26) AS order_count#0L, sum(ws_ext_ship_cost#37) AS total_shipping_cost#1, sum(ws_net_profit#42) AS total_net_profit#2]
         +- Project [ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
            +- Join Inner, (ws_web_site_sk#22 = web_site_sk#118)
               :- Project [ws_web_site_sk#22, ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
               :  +- Join Inner, (ws_ship_addr_sk#20 = ca_address_sk#105)
               :     :- Project [ws_ship_addr_sk#20, ws_web_site_sk#22, ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
               :     :  +- Join Inner, (ws_ship_date_sk#11 = d_date_sk#77)
               :     :     :- Join LeftSemi, (ws_order_number#26 = wr_order_number#157)
               :     :     :  :- Join LeftSemi, (ws_order_number#26 = ws_order_number#26#173)
               :     :     :  :  :- Project [ws_ship_date_sk#11, ws_ship_addr_sk#20, ws_web_site_sk#22, ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
               :     :     :  :  :  +- Filter ((isnotnull(ws_ship_date_sk#11) && isnotnull(ws_ship_addr_sk#20)) && isnotnull(ws_web_site_sk#22))
               :     :     :  :  :     +- Relation[ws_sold_date_sk#9,ws_sold_time_sk#10,ws_ship_date_sk#11,ws_item_sk#12,ws_bill_customer_sk#13,ws_bill_cdemo_sk#14,ws_bill_hdemo_sk#15,ws_bill_addr_sk#16,ws_ship_customer_sk#17,ws_ship_cdemo_sk#18,ws_ship_hdemo_sk#19,ws_ship_addr_sk#20,ws_web_page_sk#21,ws_web_site_sk#22,ws_ship_mode_sk#23,ws_warehouse_sk#24,ws_promo_sk#25,ws_order_number#26,ws_quantity#27,ws_wholesale_cost#28,ws_list_price#29,ws_sales_price#30,ws_ext_discount_amt#31,ws_ext_sales_price#32,... 10 more fields] parquet
               :     :     :  :  +- Project [ws_order_number#26 AS ws_order_number#26#173]
               :     :     :  :     +- Join Inner, ((ws_order_number#26 = ws_order_number#60) && NOT (ws_warehouse_sk#24 = ws_warehouse_sk#58))
               :     :     :  :        :- Project [ws_warehouse_sk#24, ws_order_number#26]
               :     :     :  :        :  +- Filter (isnotnull(ws_order_number#26) && isnotnull(ws_warehouse_sk#24))
               :     :     :  :        :     +- Relation[ws_sold_date_sk#9,ws_sold_time_sk#10,ws_ship_date_sk#11,ws_item_sk#12,ws_bill_customer_sk#13,ws_bill_cdemo_sk#14,ws_bill_hdemo_sk#15,ws_bill_addr_sk#16,ws_ship_customer_sk#17,ws_ship_cdemo_sk#18,ws_ship_hdemo_sk#19,ws_ship_addr_sk#20,ws_web_page_sk#21,ws_web_site_sk#22,ws_ship_mode_sk#23,ws_warehouse_sk#24,ws_promo_sk#25,ws_order_number#26,ws_quantity#27,ws_wholesale_cost#28,ws_list_price#29,ws_sales_price#30,ws_ext_discount_amt#31,ws_ext_sales_price#32,... 10 more fields] parquet
               :     :     :  :        +- Project [ws_warehouse_sk#58, ws_order_number#60]
               :     :     :  :           +- Filter (isnotnull(ws_order_number#60) && isnotnull(ws_warehouse_sk#58))
               :     :     :  :              +- Relation[ws_sold_date_sk#43,ws_sold_time_sk#44,ws_ship_date_sk#45,ws_item_sk#46,ws_bill_customer_sk#47,ws_bill_cdemo_sk#48,ws_bill_hdemo_sk#49,ws_bill_addr_sk#50,ws_ship_customer_sk#51,ws_ship_cdemo_sk#52,ws_ship_hdemo_sk#53,ws_ship_addr_sk#54,ws_web_page_sk#55,ws_web_site_sk#56,ws_ship_mode_sk#57,ws_warehouse_sk#58,ws_promo_sk#59,ws_order_number#60,ws_quantity#61,ws_wholesale_cost#62,ws_list_price#63,ws_sales_price#64,ws_ext_discount_amt#65,ws_ext_sales_price#66,... 10 more fields] parquet
               :     :     :  +- Project [wr_order_number#157]
               :     :     :     +- Join Inner, (wr_order_number#157 = ws_order_number#26)
               :     :     :        :- Project [wr_order_number#157]
               :     :     :        :  +- Filter isnotnull(wr_order_number#157)
               :     :     :        :     +- Relation[wr_returned_date_sk#144,wr_returned_time_sk#145,wr_item_sk#146,wr_refunded_customer_sk#147,wr_refunded_cdemo_sk#148,wr_refunded_hdemo_sk#149,wr_refunded_addr_sk#150,wr_returning_customer_sk#151,wr_returning_cdemo_sk#152,wr_returning_hdemo_sk#153,wr_returning_addr_sk#154,wr_web_page_sk#155,wr_reason_sk#156,wr_order_number#157,wr_return_quantity#158,wr_return_amt#159,wr_return_tax#160,wr_return_amt_inc_tax#161,wr_fee#162,wr_return_ship_cost#163,wr_refunded_cash#164,wr_reversed_charge#165,wr_account_credit#166,wr_net_loss#167] parquet
               :     :     :        +- Project [ws_order_number#26]
               :     :     :           +- Join Inner, ((ws_order_number#26 = ws_order_number#60) && NOT (ws_warehouse_sk#24 = ws_warehouse_sk#58))
               :     :     :              :- Project [ws_warehouse_sk#24, ws_order_number#26]
               :     :     :              :  +- Filter (isnotnull(ws_order_number#26) && isnotnull(ws_warehouse_sk#24))
               :     :     :              :     +- Relation[ws_sold_date_sk#9,ws_sold_time_sk#10,ws_ship_date_sk#11,ws_item_sk#12,ws_bill_customer_sk#13,ws_bill_cdemo_sk#14,ws_bill_hdemo_sk#15,ws_bill_addr_sk#16,ws_ship_customer_sk#17,ws_ship_cdemo_sk#18,ws_ship_hdemo_sk#19,ws_ship_addr_sk#20,ws_web_page_sk#21,ws_web_site_sk#22,ws_ship_mode_sk#23,ws_warehouse_sk#24,ws_promo_sk#25,ws_order_number#26,ws_quantity#27,ws_wholesale_cost#28,ws_list_price#29,ws_sales_price#30,ws_ext_discount_amt#31,ws_ext_sales_price#32,... 10 more fields] parquet
               :     :     :              +- Project [ws_warehouse_sk#58, ws_order_number#60]
               :     :     :                 +- Filter (isnotnull(ws_order_number#60) && isnotnull(ws_warehouse_sk#58))
               :     :     :                    +- Relation[ws_sold_date_sk#43,ws_sold_time_sk#44,ws_ship_date_sk#45,ws_item_sk#46,ws_bill_customer_sk#47,ws_bill_cdemo_sk#48,ws_bill_hdemo_sk#49,ws_bill_addr_sk#50,ws_ship_customer_sk#51,ws_ship_cdemo_sk#52,ws_ship_hdemo_sk#53,ws_ship_addr_sk#54,ws_web_page_sk#55,ws_web_site_sk#56,ws_ship_mode_sk#57,ws_warehouse_sk#58,ws_promo_sk#59,ws_order_number#60,ws_quantity#61,ws_wholesale_cost#62,ws_list_price#63,ws_sales_price#64,ws_ext_discount_amt#65,ws_ext_sales_price#66,... 10 more fields] parquet
               :     :     +- Project [d_date_sk#77]
               :     :        +- Filter (((isnotnull(d_date#79) && (d_date#79 >= 1999-2-01)) && (d_date#79 <= 1999-04-02)) && isnotnull(d_date_sk#77))
               :     :           +- Relation[d_date_sk#77,d_date_id#78,d_date#79,d_month_seq#80,d_week_seq#81,d_quarter_seq#82,d_year#83,d_dow#84,d_moy#85,d_dom#86,d_qoy#87,d_fy_year#88,d_fy_quarter_seq#89,d_fy_week_seq#90,d_day_name#91,d_quarter_name#92,d_holiday#93,d_weekend#94,d_following_holiday#95,d_first_dom#96,d_last_dom#97,d_same_day_ly#98,d_same_day_lq#99,d_current_day#100,... 4 more fields] parquet
               :     +- Project [ca_address_sk#105]
               :        +- Filter ((isnotnull(ca_state#113) && (ca_state#113 = IL)) && isnotnull(ca_address_sk#105))
               :           +- Relation[ca_address_sk#105,ca_address_id#106,ca_street_number#107,ca_street_name#108,ca_street_type#109,ca_suite_number#110,ca_city#111,ca_county#112,ca_state#113,ca_zip#114,ca_country#115,ca_gmt_offset#116,ca_location_type#117] parquet
               +- Project [web_site_sk#118]
                  +- Filter ((isnotnull(web_company_name#132) && (web_company_name#132 = pri)) && isnotnull(web_site_sk#118))
                     +- Relation[web_site_sk#118,web_site_id#119,web_rec_start_date#120,web_rec_end_date#121,web_name#122,web_open_date_sk#123,web_close_date_sk#124,web_class#125,web_manager#126,web_mkt_id#127,web_mkt_class#128,web_mkt_desc#129,web_market_manager#130,web_company_id#131,web_company_name#132,web_street_number#133,web_street_name#134,web_street_type#135,web_suite_number#136,web_city#137,web_county#138,web_state#139,web_zip#140,web_country#141,... 2 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[order_count#0L ASC NULLS FIRST], output=[order_count#0L,total_shipping_cost#1,total_net_profit#2])
+- *(14) HashAggregate(keys=[], functions=[sum(ws_ext_ship_cost#37), sum(ws_net_profit#42), count(distinct ws_order_number#26)], output=[order_count#0L, total_shipping_cost#1, total_net_profit#2])
   +- Exchange SinglePartition
      +- *(13) HashAggregate(keys=[], functions=[merge_sum(ws_ext_ship_cost#37), merge_sum(ws_net_profit#42), partial_count(distinct ws_order_number#26)], output=[sum#175, sum#177, count#180L])
         +- *(13) HashAggregate(keys=[ws_order_number#26], functions=[merge_sum(ws_ext_ship_cost#37), merge_sum(ws_net_profit#42)], output=[ws_order_number#26, sum#175, sum#177])
            +- *(13) HashAggregate(keys=[ws_order_number#26], functions=[partial_sum(ws_ext_ship_cost#37), partial_sum(ws_net_profit#42)], output=[ws_order_number#26, sum#175, sum#177])
               +- *(13) Project [ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
                  +- *(13) BroadcastHashJoin [ws_web_site_sk#22], [web_site_sk#118], Inner, BuildRight
                     :- *(13) Project [ws_web_site_sk#22, ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
                     :  +- *(13) BroadcastHashJoin [ws_ship_addr_sk#20], [ca_address_sk#105], Inner, BuildRight
                     :     :- *(13) Project [ws_ship_addr_sk#20, ws_web_site_sk#22, ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
                     :     :  +- *(13) BroadcastHashJoin [ws_ship_date_sk#11], [d_date_sk#77], Inner, BuildRight
                     :     :     :- SortMergeJoin [ws_order_number#26], [wr_order_number#157], LeftSemi
                     :     :     :  :- SortMergeJoin [ws_order_number#26], [ws_order_number#26#173], LeftSemi
                     :     :     :  :  :- *(2) Sort [ws_order_number#26 ASC NULLS FIRST], false, 0
                     :     :     :  :  :  +- Exchange hashpartitioning(ws_order_number#26, 200)
                     :     :     :  :  :     +- *(1) Project [ws_ship_date_sk#11, ws_ship_addr_sk#20, ws_web_site_sk#22, ws_order_number#26, ws_ext_ship_cost#37, ws_net_profit#42]
                     :     :     :  :  :        +- *(1) Filter ((isnotnull(ws_ship_date_sk#11) && isnotnull(ws_ship_addr_sk#20)) && isnotnull(ws_web_site_sk#22))
                     :     :     :  :  :           +- *(1) FileScan parquet tpcds.web_sales[ws_ship_date_sk#11,ws_ship_addr_sk#20,ws_web_site_sk#22,ws_order_number#26,ws_ext_ship_cost#37,ws_net_profit#42] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_date_sk), IsNotNull(ws_ship_addr_sk), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_ship_date_sk:int,ws_ship_addr_sk:int,ws_web_site_sk:int,ws_order_number:int,ws_ext_ship...
                     :     :     :  :  +- *(5) Sort [ws_order_number#26#173 ASC NULLS FIRST], false, 0
                     :     :     :  :     +- Exchange hashpartitioning(ws_order_number#26#173, 200)
                     :     :     :  :        +- *(4) Project [ws_order_number#26 AS ws_order_number#26#173]
                     :     :     :  :           +- *(4) BroadcastHashJoin [ws_order_number#26], [ws_order_number#60], Inner, BuildRight, NOT (ws_warehouse_sk#24 = ws_warehouse_sk#58)
                     :     :     :  :              :- *(4) Project [ws_warehouse_sk#24, ws_order_number#26]
                     :     :     :  :              :  +- *(4) Filter (isnotnull(ws_order_number#26) && isnotnull(ws_warehouse_sk#24))
                     :     :     :  :              :     +- *(4) FileScan parquet tpcds.web_sales[ws_warehouse_sk#24,ws_order_number#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_order_number), IsNotNull(ws_warehouse_sk)], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                     :     :     :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, true] as bigint)))
                     :     :     :  :                 +- *(3) Project [ws_warehouse_sk#58, ws_order_number#60]
                     :     :     :  :                    +- *(3) Filter (isnotnull(ws_order_number#60) && isnotnull(ws_warehouse_sk#58))
                     :     :     :  :                       +- *(3) FileScan parquet tpcds.web_sales[ws_warehouse_sk#58,ws_order_number#60] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_order_number), IsNotNull(ws_warehouse_sk)], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                     :     :     :  +- *(9) Sort [wr_order_number#157 ASC NULLS FIRST], false, 0
                     :     :     :     +- Exchange hashpartitioning(wr_order_number#157, 200)
                     :     :     :        +- *(8) Project [wr_order_number#157]
                     :     :     :           +- *(8) BroadcastHashJoin [wr_order_number#157], [ws_order_number#26], Inner, BuildLeft
                     :     :     :              :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :     :     :              :  +- *(6) Project [wr_order_number#157]
                     :     :     :              :     +- *(6) Filter isnotnull(wr_order_number#157)
                     :     :     :              :        +- *(6) FileScan parquet tpcds.web_returns[wr_order_number#157] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_order_number)], ReadSchema: struct<wr_order_number:int>
                     :     :     :              +- *(8) Project [ws_order_number#26]
                     :     :     :                 +- *(8) BroadcastHashJoin [ws_order_number#26], [ws_order_number#60], Inner, BuildRight, NOT (ws_warehouse_sk#24 = ws_warehouse_sk#58)
                     :     :     :                    :- *(8) Project [ws_warehouse_sk#24, ws_order_number#26]
                     :     :     :                    :  +- *(8) Filter (isnotnull(ws_order_number#26) && isnotnull(ws_warehouse_sk#24))
                     :     :     :                    :     +- *(8) FileScan parquet tpcds.web_sales[ws_warehouse_sk#24,ws_order_number#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_order_number), IsNotNull(ws_warehouse_sk)], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                     :     :     :                    +- ReusedExchange [ws_warehouse_sk#58, ws_order_number#60], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, true] as bigint)))
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :     :        +- *(10) Project [d_date_sk#77]
                     :     :           +- *(10) Filter (((isnotnull(d_date#79) && (d_date#79 >= 1999-2-01)) && (d_date#79 <= 1999-04-02)) && isnotnull(d_date_sk#77))
                     :     :              +- *(10) FileScan parquet tpcds.date_dim[d_date_sk#77,d_date#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,1999-2-01), LessThanOrEqual(d_date,1999-04-02), IsN..., ReadSchema: struct<d_date_sk:int,d_date:string>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :        +- *(11) Project [ca_address_sk#105]
                     :           +- *(11) Filter ((isnotnull(ca_state#113) && (ca_state#113 = IL)) && isnotnull(ca_address_sk#105))
                     :              +- *(11) FileScan parquet tpcds.customer_address[ca_address_sk#105,ca_state#113] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,IL), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        +- *(12) Project [web_site_sk#118]
                           +- *(12) Filter ((isnotnull(web_company_name#132) && (web_company_name#132 = pri)) && isnotnull(web_site_sk#118))
                              +- *(12) FileScan parquet tpcds.web_site[web_site_sk#118,web_company_name#132] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(web_company_name), EqualTo(web_company_name,pri), IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_company_name:string>
Time taken: 4.129 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 95 in stream 0 using template query95.tpl
------------------------------------------------------^^^

