== Parsed Logical Plan ==
CTE [ws, cs, ss]
:  :- 'SubqueryAlias `ws`
:  :  +- 'Aggregate ['d_year, 'ws_item_sk, 'ws_bill_customer_sk], ['d_year AS ws_sold_year#7, 'ws_item_sk, 'ws_bill_customer_sk AS ws_customer_sk#8, 'sum('ws_quantity) AS ws_qty#9, 'sum('ws_wholesale_cost) AS ws_wc#10, 'sum('ws_sales_price) AS ws_sp#11]
:  :     +- 'Filter isnull('wr_order_number)
:  :        +- 'Join Inner, ('ws_sold_date_sk = 'd_date_sk)
:  :           :- 'Join LeftOuter, (('wr_order_number = 'ws_order_number) && ('ws_item_sk = 'wr_item_sk))
:  :           :  :- 'UnresolvedRelation `web_sales`
:  :           :  +- 'UnresolvedRelation `web_returns`
:  :           +- 'UnresolvedRelation `date_dim`
:  :- 'SubqueryAlias `cs`
:  :  +- 'Aggregate ['d_year, 'cs_item_sk, 'cs_bill_customer_sk], ['d_year AS cs_sold_year#12, 'cs_item_sk, 'cs_bill_customer_sk AS cs_customer_sk#13, 'sum('cs_quantity) AS cs_qty#14, 'sum('cs_wholesale_cost) AS cs_wc#15, 'sum('cs_sales_price) AS cs_sp#16]
:  :     +- 'Filter isnull('cr_order_number)
:  :        +- 'Join Inner, ('cs_sold_date_sk = 'd_date_sk)
:  :           :- 'Join LeftOuter, (('cr_order_number = 'cs_order_number) && ('cs_item_sk = 'cr_item_sk))
:  :           :  :- 'UnresolvedRelation `catalog_sales`
:  :           :  +- 'UnresolvedRelation `catalog_returns`
:  :           +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `ss`
:     +- 'Aggregate ['d_year, 'ss_item_sk, 'ss_customer_sk], ['d_year AS ss_sold_year#17, 'ss_item_sk, 'ss_customer_sk, 'sum('ss_quantity) AS ss_qty#18, 'sum('ss_wholesale_cost) AS ss_wc#19, 'sum('ss_sales_price) AS ss_sp#20]
:        +- 'Filter isnull('sr_ticket_number)
:           +- 'Join Inner, ('ss_sold_date_sk = 'd_date_sk)
:              :- 'Join LeftOuter, (('sr_ticket_number = 'ss_ticket_number) && ('ss_item_sk = 'sr_item_sk))
:              :  :- 'UnresolvedRelation `store_sales`
:              :  +- 'UnresolvedRelation `store_returns`
:              +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['ss_sold_year ASC NULLS FIRST, 'ss_item_sk ASC NULLS FIRST, 'ss_customer_sk ASC NULLS FIRST, 'ss_qty DESC NULLS LAST, 'ss_wc DESC NULLS LAST, 'ss_sp DESC NULLS LAST, 'other_chan_qty ASC NULLS FIRST, 'other_chan_wholesale_cost ASC NULLS FIRST, 'other_chan_sales_price ASC NULLS FIRST, 'round(('ss_qty / 'coalesce(('ws_qty + 'cs_qty), 1)), 2) ASC NULLS FIRST], true
         +- 'Project ['ss_sold_year, 'ss_item_sk, 'ss_customer_sk, 'round(('ss_qty / ('coalesce('ws_qty, 0) + 'coalesce('cs_qty, 0))), 2) AS ratio#0, 'ss_qty AS store_qty#1, 'ss_wc AS store_wholesale_cost#2, 'ss_sp AS store_sales_price#3, ('coalesce('ws_qty, 0) + 'coalesce('cs_qty, 0)) AS other_chan_qty#4, ('coalesce('ws_wc, 0) + 'coalesce('cs_wc, 0)) AS other_chan_wholesale_cost#5, ('coalesce('ws_sp, 0) + 'coalesce('cs_sp, 0)) AS other_chan_sales_price#6]
            +- 'Filter ((('coalesce('ws_qty, 0) > 0) || ('coalesce('cs_qty, 0) > 0)) && ('ss_sold_year = 2000))
               +- 'Join LeftOuter, ((('cs_sold_year = 'ss_sold_year) && ('cs_item_sk = 'ss_item_sk)) && ('cs_customer_sk = 'ss_customer_sk))
                  :- 'Join LeftOuter, ((('ws_sold_year = 'ss_sold_year) && ('ws_item_sk = 'ss_item_sk)) && ('ws_customer_sk = 'ss_customer_sk))
                  :  :- 'UnresolvedRelation `ss`
                  :  +- 'UnresolvedRelation `ws`
                  +- 'UnresolvedRelation `cs`

== Analyzed Logical Plan ==
ss_sold_year: int, ss_item_sk: int, ss_customer_sk: int, ratio: double, store_qty: bigint, store_wholesale_cost: double, store_sales_price: double, other_chan_qty: bigint, other_chan_wholesale_cost: double, other_chan_sales_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, ratio#0, store_qty#1L, store_wholesale_cost#2, store_sales_price#3, other_chan_qty#4L, other_chan_wholesale_cost#5, other_chan_sales_price#6]
      +- Sort [ss_sold_year#17 ASC NULLS FIRST, ss_item_sk#178 ASC NULLS FIRST, ss_customer_sk#179 ASC NULLS FIRST, ss_qty#18L DESC NULLS LAST, ss_wc#19 DESC NULLS LAST, ss_sp#20 DESC NULLS LAST, other_chan_qty#4L ASC NULLS FIRST, other_chan_wholesale_cost#5 ASC NULLS FIRST, other_chan_sales_price#6 ASC NULLS FIRST, round((cast(ss_qty#18L as double) / cast(coalesce((ws_qty#9L + cs_qty#14L), cast(1 as bigint)) as double)), 2) ASC NULLS FIRST], true
         +- Project [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, round((cast(ss_qty#18L as double) / cast((coalesce(ws_qty#9L, cast(0 as bigint)) + coalesce(cs_qty#14L, cast(0 as bigint))) as double)), 2) AS ratio#0, ss_qty#18L AS store_qty#1L, ss_wc#19 AS store_wholesale_cost#2, ss_sp#20 AS store_sales_price#3, (coalesce(ws_qty#9L, cast(0 as bigint)) + coalesce(cs_qty#14L, cast(0 as bigint))) AS other_chan_qty#4L, (coalesce(ws_wc#10, cast(0 as double)) + coalesce(cs_wc#15, cast(0 as double))) AS other_chan_wholesale_cost#5, (coalesce(ws_sp#11, cast(0 as double)) + coalesce(cs_sp#16, cast(0 as double))) AS other_chan_sales_price#6, ss_qty#18L, ss_sp#20, cs_qty#14L, ss_wc#19, ws_qty#9L]
            +- Filter (((coalesce(ws_qty#9L, cast(0 as bigint)) > cast(0 as bigint)) || (coalesce(cs_qty#14L, cast(0 as bigint)) > cast(0 as bigint))) && (ss_sold_year#17 = 2000))
               +- Join LeftOuter, (((cs_sold_year#12 = ss_sold_year#17) && (cs_item_sk#127 = ss_item_sk#178)) && (cs_customer_sk#13 = ss_customer_sk#179))
                  :- Join LeftOuter, (((ws_sold_year#7 = ss_sold_year#17) && (ws_item_sk#26 = ss_item_sk#178)) && (ws_customer_sk#8 = ss_customer_sk#179))
                  :  :- SubqueryAlias `ss`
                  :  :  +- Aggregate [d_year#87, ss_item_sk#178, ss_customer_sk#179], [d_year#87 AS ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, sum(cast(ss_quantity#186 as bigint)) AS ss_qty#18L, sum(ss_wholesale_cost#187) AS ss_wc#19, sum(ss_sales_price#189) AS ss_sp#20]
                  :  :     +- Filter isnull(sr_ticket_number#208)
                  :  :        +- Join Inner, (ss_sold_date_sk#176 = d_date_sk#81)
                  :  :           :- Join LeftOuter, ((sr_ticket_number#208 = ss_ticket_number#185) && (ss_item_sk#178 = sr_item_sk#201))
                  :  :           :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :           :  :  +- Relation[ss_sold_date_sk#176,ss_sold_time_sk#177,ss_item_sk#178,ss_customer_sk#179,ss_cdemo_sk#180,ss_hdemo_sk#181,ss_addr_sk#182,ss_store_sk#183,ss_promo_sk#184,ss_ticket_number#185,ss_quantity#186,ss_wholesale_cost#187,ss_list_price#188,ss_sales_price#189,ss_ext_discount_amt#190,ss_ext_sales_price#191,ss_ext_wholesale_cost#192,ss_ext_list_price#193,ss_ext_tax#194,ss_coupon_amt#195,ss_net_paid#196,ss_net_paid_inc_tax#197,ss_net_profit#198] parquet
                  :  :           :  +- SubqueryAlias `tpcds`.`store_returns`
                  :  :           :     +- Relation[sr_returned_date_sk#199,sr_return_time_sk#200,sr_item_sk#201,sr_customer_sk#202,sr_cdemo_sk#203,sr_hdemo_sk#204,sr_addr_sk#205,sr_store_sk#206,sr_reason_sk#207,sr_ticket_number#208,sr_return_quantity#209,sr_return_amt#210,sr_return_tax#211,sr_return_amt_inc_tax#212,sr_fee#213,sr_return_ship_cost#214,sr_refunded_cash#215,sr_reversed_charge#216,sr_store_credit#217,sr_net_loss#218] parquet
                  :  :           +- SubqueryAlias `tpcds`.`date_dim`
                  :  :              +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet
                  :  +- SubqueryAlias `ws`
                  :     +- Aggregate [d_year#87, ws_item_sk#26, ws_bill_customer_sk#27], [d_year#87 AS ws_sold_year#7, ws_item_sk#26, ws_bill_customer_sk#27 AS ws_customer_sk#8, sum(cast(ws_quantity#41 as bigint)) AS ws_qty#9L, sum(ws_wholesale_cost#42) AS ws_wc#10, sum(ws_sales_price#44) AS ws_sp#11]
                  :        +- Filter isnull(wr_order_number#70)
                  :           +- Join Inner, (ws_sold_date_sk#23 = d_date_sk#81)
                  :              :- Join LeftOuter, ((wr_order_number#70 = ws_order_number#40) && (ws_item_sk#26 = wr_item_sk#59))
                  :              :  :- SubqueryAlias `tpcds`.`web_sales`
                  :              :  :  +- Relation[ws_sold_date_sk#23,ws_sold_time_sk#24,ws_ship_date_sk#25,ws_item_sk#26,ws_bill_customer_sk#27,ws_bill_cdemo_sk#28,ws_bill_hdemo_sk#29,ws_bill_addr_sk#30,ws_ship_customer_sk#31,ws_ship_cdemo_sk#32,ws_ship_hdemo_sk#33,ws_ship_addr_sk#34,ws_web_page_sk#35,ws_web_site_sk#36,ws_ship_mode_sk#37,ws_warehouse_sk#38,ws_promo_sk#39,ws_order_number#40,ws_quantity#41,ws_wholesale_cost#42,ws_list_price#43,ws_sales_price#44,ws_ext_discount_amt#45,ws_ext_sales_price#46,... 10 more fields] parquet
                  :              :  +- SubqueryAlias `tpcds`.`web_returns`
                  :              :     +- Relation[wr_returned_date_sk#57,wr_returned_time_sk#58,wr_item_sk#59,wr_refunded_customer_sk#60,wr_refunded_cdemo_sk#61,wr_refunded_hdemo_sk#62,wr_refunded_addr_sk#63,wr_returning_customer_sk#64,wr_returning_cdemo_sk#65,wr_returning_hdemo_sk#66,wr_returning_addr_sk#67,wr_web_page_sk#68,wr_reason_sk#69,wr_order_number#70,wr_return_quantity#71,wr_return_amt#72,wr_return_tax#73,wr_return_amt_inc_tax#74,wr_fee#75,wr_return_ship_cost#76,wr_refunded_cash#77,wr_reversed_charge#78,wr_account_credit#79,wr_net_loss#80] parquet
                  :              +- SubqueryAlias `tpcds`.`date_dim`
                  :                 +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet
                  +- SubqueryAlias `cs`
                     +- Aggregate [d_year#87, cs_item_sk#127, cs_bill_customer_sk#115], [d_year#87 AS cs_sold_year#12, cs_item_sk#127, cs_bill_customer_sk#115 AS cs_customer_sk#13, sum(cast(cs_quantity#130 as bigint)) AS cs_qty#14L, sum(cs_wholesale_cost#131) AS cs_wc#15, sum(cs_sales_price#133) AS cs_sp#16]
                        +- Filter isnull(cr_order_number#162)
                           +- Join Inner, (cs_sold_date_sk#112 = d_date_sk#81)
                              :- Join LeftOuter, ((cr_order_number#162 = cs_order_number#129) && (cs_item_sk#127 = cr_item_sk#148))
                              :  :- SubqueryAlias `tpcds`.`catalog_sales`
                              :  :  +- Relation[cs_sold_date_sk#112,cs_sold_time_sk#113,cs_ship_date_sk#114,cs_bill_customer_sk#115,cs_bill_cdemo_sk#116,cs_bill_hdemo_sk#117,cs_bill_addr_sk#118,cs_ship_customer_sk#119,cs_ship_cdemo_sk#120,cs_ship_hdemo_sk#121,cs_ship_addr_sk#122,cs_call_center_sk#123,cs_catalog_page_sk#124,cs_ship_mode_sk#125,cs_warehouse_sk#126,cs_item_sk#127,cs_promo_sk#128,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_list_price#132,cs_sales_price#133,cs_ext_discount_amt#134,cs_ext_sales_price#135,... 10 more fields] parquet
                              :  +- SubqueryAlias `tpcds`.`catalog_returns`
                              :     +- Relation[cr_returned_date_sk#146,cr_returned_time_sk#147,cr_item_sk#148,cr_refunded_customer_sk#149,cr_refunded_cdemo_sk#150,cr_refunded_hdemo_sk#151,cr_refunded_addr_sk#152,cr_returning_customer_sk#153,cr_returning_cdemo_sk#154,cr_returning_hdemo_sk#155,cr_returning_addr_sk#156,cr_call_center_sk#157,cr_catalog_page_sk#158,cr_ship_mode_sk#159,cr_warehouse_sk#160,cr_reason_sk#161,cr_order_number#162,cr_return_quantity#163,cr_return_amount#164,cr_return_tax#165,cr_return_amt_inc_tax#166,cr_fee#167,cr_return_ship_cost#168,cr_refunded_cash#169,... 3 more fields] parquet
                              +- SubqueryAlias `tpcds`.`date_dim`
                                 +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Project [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, ratio#0, store_qty#1L, store_wholesale_cost#2, store_sales_price#3, other_chan_qty#4L, other_chan_wholesale_cost#5, other_chan_sales_price#6]
      +- Sort [ss_sold_year#17 ASC NULLS FIRST, ss_item_sk#178 ASC NULLS FIRST, ss_customer_sk#179 ASC NULLS FIRST, ss_qty#18L DESC NULLS LAST, ss_wc#19 DESC NULLS LAST, ss_sp#20 DESC NULLS LAST, other_chan_qty#4L ASC NULLS FIRST, other_chan_wholesale_cost#5 ASC NULLS FIRST, other_chan_sales_price#6 ASC NULLS FIRST, round((cast(ss_qty#18L as double) / cast(coalesce((ws_qty#9L + cs_qty#14L), 1) as double)), 2) ASC NULLS FIRST], true
         +- Project [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, round((cast(ss_qty#18L as double) / cast((coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) as double)), 2) AS ratio#0, ss_qty#18L AS store_qty#1L, ss_wc#19 AS store_wholesale_cost#2, ss_sp#20 AS store_sales_price#3, (coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) AS other_chan_qty#4L, (coalesce(ws_wc#10, 0.0) + coalesce(cs_wc#15, 0.0)) AS other_chan_wholesale_cost#5, (coalesce(ws_sp#11, 0.0) + coalesce(cs_sp#16, 0.0)) AS other_chan_sales_price#6, ss_qty#18L, ss_sp#20, cs_qty#14L, ss_wc#19, ws_qty#9L]
            +- Filter ((coalesce(ws_qty#9L, 0) > 0) || (coalesce(cs_qty#14L, 0) > 0))
               +- Join LeftOuter, (((cs_sold_year#12 = ss_sold_year#17) && (cs_item_sk#127 = ss_item_sk#178)) && (cs_customer_sk#13 = ss_customer_sk#179))
                  :- Project [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, ss_qty#18L, ss_wc#19, ss_sp#20, ws_qty#9L, ws_wc#10, ws_sp#11]
                  :  +- Join LeftOuter, (((ws_sold_year#7 = ss_sold_year#17) && (ws_item_sk#26 = ss_item_sk#178)) && (ws_customer_sk#8 = ss_customer_sk#179))
                  :     :- Aggregate [d_year#87, ss_item_sk#178, ss_customer_sk#179], [d_year#87 AS ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, sum(cast(ss_quantity#186 as bigint)) AS ss_qty#18L, sum(ss_wholesale_cost#187) AS ss_wc#19, sum(ss_sales_price#189) AS ss_sp#20]
                  :     :  +- Project [ss_item_sk#178, ss_customer_sk#179, ss_quantity#186, ss_wholesale_cost#187, ss_sales_price#189, d_year#87]
                  :     :     +- Join Inner, (ss_sold_date_sk#176 = d_date_sk#81)
                  :     :        :- Project [ss_sold_date_sk#176, ss_item_sk#178, ss_customer_sk#179, ss_quantity#186, ss_wholesale_cost#187, ss_sales_price#189]
                  :     :        :  +- Filter isnull(sr_ticket_number#208)
                  :     :        :     +- Join LeftOuter, ((sr_ticket_number#208 = ss_ticket_number#185) && (ss_item_sk#178 = sr_item_sk#201))
                  :     :        :        :- Project [ss_sold_date_sk#176, ss_item_sk#178, ss_customer_sk#179, ss_ticket_number#185, ss_quantity#186, ss_wholesale_cost#187, ss_sales_price#189]
                  :     :        :        :  +- Filter isnotnull(ss_sold_date_sk#176)
                  :     :        :        :     +- Relation[ss_sold_date_sk#176,ss_sold_time_sk#177,ss_item_sk#178,ss_customer_sk#179,ss_cdemo_sk#180,ss_hdemo_sk#181,ss_addr_sk#182,ss_store_sk#183,ss_promo_sk#184,ss_ticket_number#185,ss_quantity#186,ss_wholesale_cost#187,ss_list_price#188,ss_sales_price#189,ss_ext_discount_amt#190,ss_ext_sales_price#191,ss_ext_wholesale_cost#192,ss_ext_list_price#193,ss_ext_tax#194,ss_coupon_amt#195,ss_net_paid#196,ss_net_paid_inc_tax#197,ss_net_profit#198] parquet
                  :     :        :        +- Project [sr_item_sk#201, sr_ticket_number#208]
                  :     :        :           +- Filter (isnotnull(sr_ticket_number#208) && isnotnull(sr_item_sk#201))
                  :     :        :              +- Relation[sr_returned_date_sk#199,sr_return_time_sk#200,sr_item_sk#201,sr_customer_sk#202,sr_cdemo_sk#203,sr_hdemo_sk#204,sr_addr_sk#205,sr_store_sk#206,sr_reason_sk#207,sr_ticket_number#208,sr_return_quantity#209,sr_return_amt#210,sr_return_tax#211,sr_return_amt_inc_tax#212,sr_fee#213,sr_return_ship_cost#214,sr_refunded_cash#215,sr_reversed_charge#216,sr_store_credit#217,sr_net_loss#218] parquet
                  :     :        +- Project [d_date_sk#81, d_year#87]
                  :     :           +- Filter ((isnotnull(d_year#87) && (d_year#87 = 2000)) && isnotnull(d_date_sk#81))
                  :     :              +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet
                  :     +- Aggregate [d_year#87, ws_item_sk#26, ws_bill_customer_sk#27], [d_year#87 AS ws_sold_year#7, ws_item_sk#26, ws_bill_customer_sk#27 AS ws_customer_sk#8, sum(cast(ws_quantity#41 as bigint)) AS ws_qty#9L, sum(ws_wholesale_cost#42) AS ws_wc#10, sum(ws_sales_price#44) AS ws_sp#11]
                  :        +- Project [ws_item_sk#26, ws_bill_customer_sk#27, ws_quantity#41, ws_wholesale_cost#42, ws_sales_price#44, d_year#87]
                  :           +- Join Inner, (ws_sold_date_sk#23 = d_date_sk#81)
                  :              :- Project [ws_sold_date_sk#23, ws_item_sk#26, ws_bill_customer_sk#27, ws_quantity#41, ws_wholesale_cost#42, ws_sales_price#44]
                  :              :  +- Filter isnull(wr_order_number#70)
                  :              :     +- Join LeftOuter, ((wr_order_number#70 = ws_order_number#40) && (ws_item_sk#26 = wr_item_sk#59))
                  :              :        :- Project [ws_sold_date_sk#23, ws_item_sk#26, ws_bill_customer_sk#27, ws_order_number#40, ws_quantity#41, ws_wholesale_cost#42, ws_sales_price#44]
                  :              :        :  +- Filter ((isnotnull(ws_sold_date_sk#23) && isnotnull(ws_item_sk#26)) && isnotnull(ws_bill_customer_sk#27))
                  :              :        :     +- Relation[ws_sold_date_sk#23,ws_sold_time_sk#24,ws_ship_date_sk#25,ws_item_sk#26,ws_bill_customer_sk#27,ws_bill_cdemo_sk#28,ws_bill_hdemo_sk#29,ws_bill_addr_sk#30,ws_ship_customer_sk#31,ws_ship_cdemo_sk#32,ws_ship_hdemo_sk#33,ws_ship_addr_sk#34,ws_web_page_sk#35,ws_web_site_sk#36,ws_ship_mode_sk#37,ws_warehouse_sk#38,ws_promo_sk#39,ws_order_number#40,ws_quantity#41,ws_wholesale_cost#42,ws_list_price#43,ws_sales_price#44,ws_ext_discount_amt#45,ws_ext_sales_price#46,... 10 more fields] parquet
                  :              :        +- Project [wr_item_sk#59, wr_order_number#70]
                  :              :           +- Filter (isnotnull(wr_order_number#70) && isnotnull(wr_item_sk#59))
                  :              :              +- Relation[wr_returned_date_sk#57,wr_returned_time_sk#58,wr_item_sk#59,wr_refunded_customer_sk#60,wr_refunded_cdemo_sk#61,wr_refunded_hdemo_sk#62,wr_refunded_addr_sk#63,wr_returning_customer_sk#64,wr_returning_cdemo_sk#65,wr_returning_hdemo_sk#66,wr_returning_addr_sk#67,wr_web_page_sk#68,wr_reason_sk#69,wr_order_number#70,wr_return_quantity#71,wr_return_amt#72,wr_return_tax#73,wr_return_amt_inc_tax#74,wr_fee#75,wr_return_ship_cost#76,wr_refunded_cash#77,wr_reversed_charge#78,wr_account_credit#79,wr_net_loss#80] parquet
                  :              +- Project [d_date_sk#81, d_year#87]
                  :                 +- Filter ((isnotnull(d_date_sk#81) && (d_year#87 = 2000)) && isnotnull(d_year#87))
                  :                    +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet
                  +- Aggregate [d_year#87, cs_item_sk#127, cs_bill_customer_sk#115], [d_year#87 AS cs_sold_year#12, cs_item_sk#127, cs_bill_customer_sk#115 AS cs_customer_sk#13, sum(cast(cs_quantity#130 as bigint)) AS cs_qty#14L, sum(cs_wholesale_cost#131) AS cs_wc#15, sum(cs_sales_price#133) AS cs_sp#16]
                     +- Project [cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133, d_year#87]
                        +- Join Inner, (cs_sold_date_sk#112 = d_date_sk#81)
                           :- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133]
                           :  +- Filter isnull(cr_order_number#162)
                           :     +- Join LeftOuter, ((cr_order_number#162 = cs_order_number#129) && (cs_item_sk#127 = cr_item_sk#148))
                           :        :- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_order_number#129, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133]
                           :        :  +- Filter ((isnotnull(cs_sold_date_sk#112) && isnotnull(cs_bill_customer_sk#115)) && isnotnull(cs_item_sk#127))
                           :        :     +- Relation[cs_sold_date_sk#112,cs_sold_time_sk#113,cs_ship_date_sk#114,cs_bill_customer_sk#115,cs_bill_cdemo_sk#116,cs_bill_hdemo_sk#117,cs_bill_addr_sk#118,cs_ship_customer_sk#119,cs_ship_cdemo_sk#120,cs_ship_hdemo_sk#121,cs_ship_addr_sk#122,cs_call_center_sk#123,cs_catalog_page_sk#124,cs_ship_mode_sk#125,cs_warehouse_sk#126,cs_item_sk#127,cs_promo_sk#128,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_list_price#132,cs_sales_price#133,cs_ext_discount_amt#134,cs_ext_sales_price#135,... 10 more fields] parquet
                           :        +- Project [cr_item_sk#148, cr_order_number#162]
                           :           +- Filter (isnotnull(cr_order_number#162) && isnotnull(cr_item_sk#148))
                           :              +- Relation[cr_returned_date_sk#146,cr_returned_time_sk#147,cr_item_sk#148,cr_refunded_customer_sk#149,cr_refunded_cdemo_sk#150,cr_refunded_hdemo_sk#151,cr_refunded_addr_sk#152,cr_returning_customer_sk#153,cr_returning_cdemo_sk#154,cr_returning_hdemo_sk#155,cr_returning_addr_sk#156,cr_call_center_sk#157,cr_catalog_page_sk#158,cr_ship_mode_sk#159,cr_warehouse_sk#160,cr_reason_sk#161,cr_order_number#162,cr_return_quantity#163,cr_return_amount#164,cr_return_tax#165,cr_return_amt_inc_tax#166,cr_fee#167,cr_return_ship_cost#168,cr_refunded_cash#169,... 3 more fields] parquet
                           +- Project [d_date_sk#81, d_year#87]
                              +- Filter ((isnotnull(d_date_sk#81) && isnotnull(d_year#87)) && (d_year#87 = 2000))
                                 +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[ss_sold_year#17 ASC NULLS FIRST,ss_item_sk#178 ASC NULLS FIRST,ss_customer_sk#179 ASC NULLS FIRST,ss_qty#18L DESC NULLS LAST,ss_wc#19 DESC NULLS LAST,ss_sp#20 DESC NULLS LAST,other_chan_qty#4L ASC NULLS FIRST,other_chan_wholesale_cost#5 ASC NULLS FIRST,other_chan_sales_price#6 ASC NULLS FIRST,round((cast(ss_qty#18L as double) / cast(coalesce((ws_qty#9L + cs_qty#14L), 1) as double)), 2) ASC NULLS FIRST], output=[ss_sold_year#17,ss_item_sk#178,ss_customer_sk#179,ratio#0,store_qty#1L,store_wholesale_cost#2,store_sales_price#3,other_chan_qty#4L,other_chan_wholesale_cost#5,other_chan_sales_price#6])
+- *(17) Project [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, round((cast(ss_qty#18L as double) / cast((coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) as double)), 2) AS ratio#0, ss_qty#18L AS store_qty#1L, ss_wc#19 AS store_wholesale_cost#2, ss_sp#20 AS store_sales_price#3, (coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) AS other_chan_qty#4L, (coalesce(ws_wc#10, 0.0) + coalesce(cs_wc#15, 0.0)) AS other_chan_wholesale_cost#5, (coalesce(ws_sp#11, 0.0) + coalesce(cs_sp#16, 0.0)) AS other_chan_sales_price#6, ss_qty#18L, ss_sp#20, cs_qty#14L, ss_wc#19, ws_qty#9L]
   +- *(17) Filter ((coalesce(ws_qty#9L, 0) > 0) || (coalesce(cs_qty#14L, 0) > 0))
      +- SortMergeJoin [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179], [cs_sold_year#12, cs_item_sk#127, cs_customer_sk#13], LeftOuter
         :- *(11) Project [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, ss_qty#18L, ss_wc#19, ss_sp#20, ws_qty#9L, ws_wc#10, ws_sp#11]
         :  +- SortMergeJoin [ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179], [ws_sold_year#7, ws_item_sk#26, ws_customer_sk#8], LeftOuter
         :     :- *(5) Sort [ss_sold_year#17 ASC NULLS FIRST, ss_item_sk#178 ASC NULLS FIRST, ss_customer_sk#179 ASC NULLS FIRST], false, 0
         :     :  +- Exchange hashpartitioning(ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, 200)
         :     :     +- *(4) HashAggregate(keys=[d_year#87, ss_item_sk#178, ss_customer_sk#179], functions=[sum(cast(ss_quantity#186 as bigint)), sum(ss_wholesale_cost#187), sum(ss_sales_price#189)], output=[ss_sold_year#17, ss_item_sk#178, ss_customer_sk#179, ss_qty#18L, ss_wc#19, ss_sp#20])
         :     :        +- Exchange hashpartitioning(d_year#87, ss_item_sk#178, ss_customer_sk#179, 200)
         :     :           +- *(3) HashAggregate(keys=[d_year#87, ss_item_sk#178, ss_customer_sk#179], functions=[partial_sum(cast(ss_quantity#186 as bigint)), partial_sum(ss_wholesale_cost#187), partial_sum(ss_sales_price#189)], output=[d_year#87, ss_item_sk#178, ss_customer_sk#179, sum#225L, sum#226, sum#227])
         :     :              +- *(3) Project [ss_item_sk#178, ss_customer_sk#179, ss_quantity#186, ss_wholesale_cost#187, ss_sales_price#189, d_year#87]
         :     :                 +- *(3) BroadcastHashJoin [ss_sold_date_sk#176], [d_date_sk#81], Inner, BuildRight
         :     :                    :- *(3) Project [ss_sold_date_sk#176, ss_item_sk#178, ss_customer_sk#179, ss_quantity#186, ss_wholesale_cost#187, ss_sales_price#189]
         :     :                    :  +- *(3) Filter isnull(sr_ticket_number#208)
         :     :                    :     +- *(3) BroadcastHashJoin [ss_ticket_number#185, ss_item_sk#178], [sr_ticket_number#208, sr_item_sk#201], LeftOuter, BuildRight
         :     :                    :        :- *(3) Project [ss_sold_date_sk#176, ss_item_sk#178, ss_customer_sk#179, ss_ticket_number#185, ss_quantity#186, ss_wholesale_cost#187, ss_sales_price#189]
         :     :                    :        :  +- *(3) Filter isnotnull(ss_sold_date_sk#176)
         :     :                    :        :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#176,ss_item_sk#178,ss_customer_sk#179,ss_ticket_number#185,ss_quantity#186,ss_wholesale_cost#187,ss_sales_price#189] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_ticket_number:int,ss_quantity:int...
         :     :                    :        +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
         :     :                    :           +- *(1) Project [sr_item_sk#201, sr_ticket_number#208]
         :     :                    :              +- *(1) Filter (isnotnull(sr_ticket_number#208) && isnotnull(sr_item_sk#201))
         :     :                    :                 +- *(1) FileScan parquet tpcds.store_returns[sr_item_sk#201,sr_ticket_number#208] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_ticket_number), IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int>
         :     :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :                       +- *(2) Project [d_date_sk#81, d_year#87]
         :     :                          +- *(2) Filter ((isnotnull(d_year#87) && (d_year#87 = 2000)) && isnotnull(d_date_sk#81))
         :     :                             +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#81,d_year#87] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     +- *(10) Sort [ws_sold_year#7 ASC NULLS FIRST, ws_item_sk#26 ASC NULLS FIRST, ws_customer_sk#8 ASC NULLS FIRST], false, 0
         :        +- Exchange hashpartitioning(ws_sold_year#7, ws_item_sk#26, ws_customer_sk#8, 200)
         :           +- *(9) HashAggregate(keys=[d_year#87, ws_item_sk#26, ws_bill_customer_sk#27], functions=[sum(cast(ws_quantity#41 as bigint)), sum(ws_wholesale_cost#42), sum(ws_sales_price#44)], output=[ws_sold_year#7, ws_item_sk#26, ws_customer_sk#8, ws_qty#9L, ws_wc#10, ws_sp#11])
         :              +- Exchange hashpartitioning(d_year#87, ws_item_sk#26, ws_bill_customer_sk#27, 200)
         :                 +- *(8) HashAggregate(keys=[d_year#87, ws_item_sk#26, ws_bill_customer_sk#27], functions=[partial_sum(cast(ws_quantity#41 as bigint)), partial_sum(ws_wholesale_cost#42), partial_sum(ws_sales_price#44)], output=[d_year#87, ws_item_sk#26, ws_bill_customer_sk#27, sum#231L, sum#232, sum#233])
         :                    +- *(8) Project [ws_item_sk#26, ws_bill_customer_sk#27, ws_quantity#41, ws_wholesale_cost#42, ws_sales_price#44, d_year#87]
         :                       +- *(8) BroadcastHashJoin [ws_sold_date_sk#23], [d_date_sk#81], Inner, BuildRight
         :                          :- *(8) Project [ws_sold_date_sk#23, ws_item_sk#26, ws_bill_customer_sk#27, ws_quantity#41, ws_wholesale_cost#42, ws_sales_price#44]
         :                          :  +- *(8) Filter isnull(wr_order_number#70)
         :                          :     +- *(8) BroadcastHashJoin [ws_order_number#40, ws_item_sk#26], [wr_order_number#70, wr_item_sk#59], LeftOuter, BuildRight
         :                          :        :- *(8) Project [ws_sold_date_sk#23, ws_item_sk#26, ws_bill_customer_sk#27, ws_order_number#40, ws_quantity#41, ws_wholesale_cost#42, ws_sales_price#44]
         :                          :        :  +- *(8) Filter ((isnotnull(ws_sold_date_sk#23) && isnotnull(ws_item_sk#26)) && isnotnull(ws_bill_customer_sk#27))
         :                          :        :     +- *(8) FileScan parquet tpcds.web_sales[ws_sold_date_sk#23,ws_item_sk#26,ws_bill_customer_sk#27,ws_order_number#40,ws_quantity#41,ws_wholesale_cost#42,ws_sales_price#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk), IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int,ws_order_number:int,ws_quantity...
         :                          :        +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
         :                          :           +- *(6) Project [wr_item_sk#59, wr_order_number#70]
         :                          :              +- *(6) Filter (isnotnull(wr_order_number#70) && isnotnull(wr_item_sk#59))
         :                          :                 +- *(6) FileScan parquet tpcds.web_returns[wr_item_sk#59,wr_order_number#70] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_order_number), IsNotNull(wr_item_sk)], ReadSchema: struct<wr_item_sk:int,wr_order_number:int>
         :                          +- ReusedExchange [d_date_sk#81, d_year#87], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         +- *(16) Sort [cs_sold_year#12 ASC NULLS FIRST, cs_item_sk#127 ASC NULLS FIRST, cs_customer_sk#13 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(cs_sold_year#12, cs_item_sk#127, cs_customer_sk#13, 200)
               +- *(15) HashAggregate(keys=[d_year#87, cs_item_sk#127, cs_bill_customer_sk#115], functions=[sum(cast(cs_quantity#130 as bigint)), sum(cs_wholesale_cost#131), sum(cs_sales_price#133)], output=[cs_sold_year#12, cs_item_sk#127, cs_customer_sk#13, cs_qty#14L, cs_wc#15, cs_sp#16])
                  +- Exchange hashpartitioning(d_year#87, cs_item_sk#127, cs_bill_customer_sk#115, 200)
                     +- *(14) HashAggregate(keys=[d_year#87, cs_item_sk#127, cs_bill_customer_sk#115], functions=[partial_sum(cast(cs_quantity#130 as bigint)), partial_sum(cs_wholesale_cost#131), partial_sum(cs_sales_price#133)], output=[d_year#87, cs_item_sk#127, cs_bill_customer_sk#115, sum#237L, sum#238, sum#239])
                        +- *(14) Project [cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133, d_year#87]
                           +- *(14) BroadcastHashJoin [cs_sold_date_sk#112], [d_date_sk#81], Inner, BuildRight
                              :- *(14) Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133]
                              :  +- *(14) Filter isnull(cr_order_number#162)
                              :     +- *(14) BroadcastHashJoin [cs_order_number#129, cs_item_sk#127], [cr_order_number#162, cr_item_sk#148], LeftOuter, BuildRight
                              :        :- *(14) Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_order_number#129, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133]
                              :        :  +- *(14) Filter ((isnotnull(cs_sold_date_sk#112) && isnotnull(cs_bill_customer_sk#115)) && isnotnull(cs_item_sk#127))
                              :        :     +- *(14) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#112,cs_bill_customer_sk#115,cs_item_sk#127,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_sales_price#133] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_bill_customer_sk), IsNotNull(cs_item_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_order_number:int,cs_quantity...
                              :        +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
                              :           +- *(12) Project [cr_item_sk#148, cr_order_number#162]
                              :              +- *(12) Filter (isnotnull(cr_order_number#162) && isnotnull(cr_item_sk#148))
                              :                 +- *(12) FileScan parquet tpcds.catalog_returns[cr_item_sk#148,cr_order_number#162] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_order_number), IsNotNull(cr_item_sk)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int>
                              +- ReusedExchange [d_date_sk#81, d_year#87], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.29 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 78 in stream 0 using template query78.tpl
------------------------------------------------------^^^

