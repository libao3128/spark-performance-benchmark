== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort [1 ASC NULLS FIRST, 4 ASC NULLS FIRST, 5 ASC NULLS FIRST], true
      +- 'Distinct
         +- 'Union
            :- 'Distinct
            :  +- 'Union
            :     :- 'Project [web AS channel#5, 'web.item, 'web.return_ratio, 'web.return_rank, 'web.currency_rank]
            :     :  +- 'Filter (('web.return_rank <= 10) || ('web.currency_rank <= 10))
            :     :     +- 'SubqueryAlias `web`
            :     :        +- 'Project ['item, 'return_ratio, 'currency_ratio, 'rank() windowspecdefinition('return_ratio ASC NULLS FIRST, unspecifiedframe$()) AS return_rank#3, 'rank() windowspecdefinition('currency_ratio ASC NULLS FIRST, unspecifiedframe$()) AS currency_rank#4]
            :     :           +- 'SubqueryAlias `in_web`
            :     :              +- 'Aggregate ['ws.ws_item_sk], ['ws.ws_item_sk AS item#0, (cast('sum('coalesce('wr.wr_return_quantity, 0)) as decimal(15,4)) / cast('sum('coalesce('ws.ws_quantity, 0)) as decimal(15,4))) AS return_ratio#1, (cast('sum('coalesce('wr.wr_return_amt, 0)) as decimal(15,4)) / cast('sum('coalesce('ws.ws_net_paid, 0)) as decimal(15,4))) AS currency_ratio#2]
            :     :                 +- 'Filter (((('wr.wr_return_amt > 10000) && ('ws.ws_net_profit > 1)) && (('ws.ws_net_paid > 0) && ('ws.ws_quantity > 0))) && ((('ws_sold_date_sk = 'd_date_sk) && ('d_year = 2001)) && ('d_moy = 12)))
            :     :                    +- 'Join Inner
            :     :                       :- 'Join LeftOuter, (('ws.ws_order_number = 'wr.wr_order_number) && ('ws.ws_item_sk = 'wr.wr_item_sk))
            :     :                       :  :- 'SubqueryAlias `ws`
            :     :                       :  :  +- 'UnresolvedRelation `web_sales`
            :     :                       :  +- 'SubqueryAlias `wr`
            :     :                       :     +- 'UnresolvedRelation `web_returns`
            :     :                       +- 'UnresolvedRelation `date_dim`
            :     +- 'Project [catalog AS channel#11, 'catalog.item, 'catalog.return_ratio, 'catalog.return_rank, 'catalog.currency_rank]
            :        +- 'Filter (('catalog.return_rank <= 10) || ('catalog.currency_rank <= 10))
            :           +- 'SubqueryAlias `catalog`
            :              +- 'Project ['item, 'return_ratio, 'currency_ratio, 'rank() windowspecdefinition('return_ratio ASC NULLS FIRST, unspecifiedframe$()) AS return_rank#9, 'rank() windowspecdefinition('currency_ratio ASC NULLS FIRST, unspecifiedframe$()) AS currency_rank#10]
            :                 +- 'SubqueryAlias `in_cat`
            :                    +- 'Aggregate ['cs.cs_item_sk], ['cs.cs_item_sk AS item#6, (cast('sum('coalesce('cr.cr_return_quantity, 0)) as decimal(15,4)) / cast('sum('coalesce('cs.cs_quantity, 0)) as decimal(15,4))) AS return_ratio#7, (cast('sum('coalesce('cr.cr_return_amount, 0)) as decimal(15,4)) / cast('sum('coalesce('cs.cs_net_paid, 0)) as decimal(15,4))) AS currency_ratio#8]
            :                       +- 'Filter (((('cr.cr_return_amount > 10000) && ('cs.cs_net_profit > 1)) && (('cs.cs_net_paid > 0) && ('cs.cs_quantity > 0))) && ((('cs_sold_date_sk = 'd_date_sk) && ('d_year = 2001)) && ('d_moy = 12)))
            :                          +- 'Join Inner
            :                             :- 'Join LeftOuter, (('cs.cs_order_number = 'cr.cr_order_number) && ('cs.cs_item_sk = 'cr.cr_item_sk))
            :                             :  :- 'SubqueryAlias `cs`
            :                             :  :  +- 'UnresolvedRelation `catalog_sales`
            :                             :  +- 'SubqueryAlias `cr`
            :                             :     +- 'UnresolvedRelation `catalog_returns`
            :                             +- 'UnresolvedRelation `date_dim`
            +- 'Project [store AS channel#17, 'store.item, 'store.return_ratio, 'store.return_rank, 'store.currency_rank]
               +- 'Filter (('store.return_rank <= 10) || ('store.currency_rank <= 10))
                  +- 'SubqueryAlias `store`
                     +- 'Project ['item, 'return_ratio, 'currency_ratio, 'rank() windowspecdefinition('return_ratio ASC NULLS FIRST, unspecifiedframe$()) AS return_rank#15, 'rank() windowspecdefinition('currency_ratio ASC NULLS FIRST, unspecifiedframe$()) AS currency_rank#16]
                        +- 'SubqueryAlias `in_store`
                           +- 'Aggregate ['sts.ss_item_sk], ['sts.ss_item_sk AS item#12, (cast('sum('coalesce('sr.sr_return_quantity, 0)) as decimal(15,4)) / cast('sum('coalesce('sts.ss_quantity, 0)) as decimal(15,4))) AS return_ratio#13, (cast('sum('coalesce('sr.sr_return_amt, 0)) as decimal(15,4)) / cast('sum('coalesce('sts.ss_net_paid, 0)) as decimal(15,4))) AS currency_ratio#14]
                              +- 'Filter (((('sr.sr_return_amt > 10000) && ('sts.ss_net_profit > 1)) && (('sts.ss_net_paid > 0) && ('sts.ss_quantity > 0))) && ((('ss_sold_date_sk = 'd_date_sk) && ('d_year = 2001)) && ('d_moy = 12)))
                                 +- 'Join Inner
                                    :- 'Join LeftOuter, (('sts.ss_ticket_number = 'sr.sr_ticket_number) && ('sts.ss_item_sk = 'sr.sr_item_sk))
                                    :  :- 'SubqueryAlias `sts`
                                    :  :  +- 'UnresolvedRelation `store_sales`
                                    :  +- 'SubqueryAlias `sr`
                                    :     +- 'UnresolvedRelation `store_returns`
                                    +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
channel: string, item: int, return_ratio: decimal(35,20), return_rank: int, currency_rank: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#5 ASC NULLS FIRST, return_rank#3 ASC NULLS FIRST, currency_rank#4 ASC NULLS FIRST], true
      +- Distinct
         +- Union
            :- Distinct
            :  +- Union
            :     :- Project [web AS channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
            :     :  +- Filter ((return_rank#3 <= 10) || (currency_rank#4 <= 10))
            :     :     +- SubqueryAlias `web`
            :     :        +- Project [item#0, return_ratio#1, currency_ratio#2, return_rank#3, currency_rank#4]
            :     :           +- Project [item#0, return_ratio#1, currency_ratio#2, currency_rank#4, return_rank#3, return_rank#3, currency_rank#4]
            :     :              +- Window [rank(return_ratio#1) windowspecdefinition(return_ratio#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#3], [return_ratio#1 ASC NULLS FIRST]
            :     :                 +- Window [rank(currency_ratio#2) windowspecdefinition(currency_ratio#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#4], [currency_ratio#2 ASC NULLS FIRST]
            :     :                    +- Project [item#0, return_ratio#1, currency_ratio#2]
            :     :                       +- SubqueryAlias `in_web`
            :     :                          +- Aggregate [ws_item_sk#35], [ws_item_sk#35 AS item#0, CheckOverflow((promote_precision(cast(cast(sum(cast(coalesce(wr_return_quantity#80, 0) as bigint)) as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(sum(cast(coalesce(ws_quantity#50, 0) as bigint)) as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) AS return_ratio#1, CheckOverflow((promote_precision(cast(cast(sum(coalesce(wr_return_amt#81, cast(0 as double))) as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(sum(coalesce(ws_net_paid#61, cast(0 as double))) as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) AS currency_ratio#2]
            :     :                             +- Filter ((((wr_return_amt#81 > cast(10000 as double)) && (ws_net_profit#65 > cast(1 as double))) && ((ws_net_paid#61 > cast(0 as double)) && (ws_quantity#50 > 0))) && (((ws_sold_date_sk#32 = d_date_sk#90) && (d_year#96 = 2001)) && (d_moy#98 = 12)))
            :     :                                +- Join Inner
            :     :                                   :- Join LeftOuter, ((ws_order_number#49 = wr_order_number#79) && (ws_item_sk#35 = wr_item_sk#68))
            :     :                                   :  :- SubqueryAlias `ws`
            :     :                                   :  :  +- SubqueryAlias `tpcds`.`web_sales`
            :     :                                   :  :     +- Relation[ws_sold_date_sk#32,ws_sold_time_sk#33,ws_ship_date_sk#34,ws_item_sk#35,ws_bill_customer_sk#36,ws_bill_cdemo_sk#37,ws_bill_hdemo_sk#38,ws_bill_addr_sk#39,ws_ship_customer_sk#40,ws_ship_cdemo_sk#41,ws_ship_hdemo_sk#42,ws_ship_addr_sk#43,ws_web_page_sk#44,ws_web_site_sk#45,ws_ship_mode_sk#46,ws_warehouse_sk#47,ws_promo_sk#48,ws_order_number#49,ws_quantity#50,ws_wholesale_cost#51,ws_list_price#52,ws_sales_price#53,ws_ext_discount_amt#54,ws_ext_sales_price#55,... 10 more fields] parquet
            :     :                                   :  +- SubqueryAlias `wr`
            :     :                                   :     +- SubqueryAlias `tpcds`.`web_returns`
            :     :                                   :        +- Relation[wr_returned_date_sk#66,wr_returned_time_sk#67,wr_item_sk#68,wr_refunded_customer_sk#69,wr_refunded_cdemo_sk#70,wr_refunded_hdemo_sk#71,wr_refunded_addr_sk#72,wr_returning_customer_sk#73,wr_returning_cdemo_sk#74,wr_returning_hdemo_sk#75,wr_returning_addr_sk#76,wr_web_page_sk#77,wr_reason_sk#78,wr_order_number#79,wr_return_quantity#80,wr_return_amt#81,wr_return_tax#82,wr_return_amt_inc_tax#83,wr_fee#84,wr_return_ship_cost#85,wr_refunded_cash#86,wr_reversed_charge#87,wr_account_credit#88,wr_net_loss#89] parquet
            :     :                                   +- SubqueryAlias `tpcds`.`date_dim`
            :     :                                      +- Relation[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet
            :     +- Project [catalog AS channel#11, item#6, return_ratio#7, return_rank#9, currency_rank#10]
            :        +- Filter ((return_rank#9 <= 10) || (currency_rank#10 <= 10))
            :           +- SubqueryAlias `catalog`
            :              +- Project [item#6, return_ratio#7, currency_ratio#8, return_rank#9, currency_rank#10]
            :                 +- Project [item#6, return_ratio#7, currency_ratio#8, return_rank#9, currency_rank#10, return_rank#9, currency_rank#10]
            :                    +- Window [rank(currency_ratio#8) windowspecdefinition(currency_ratio#8 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#10], [currency_ratio#8 ASC NULLS FIRST]
            :                       +- Window [rank(return_ratio#7) windowspecdefinition(return_ratio#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#9], [return_ratio#7 ASC NULLS FIRST]
            :                          +- Project [item#6, return_ratio#7, currency_ratio#8]
            :                             +- SubqueryAlias `in_cat`
            :                                +- Aggregate [cs_item_sk#133], [cs_item_sk#133 AS item#6, CheckOverflow((promote_precision(cast(cast(sum(cast(coalesce(cr_return_quantity#169, 0) as bigint)) as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(sum(cast(coalesce(cs_quantity#136, 0) as bigint)) as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) AS return_ratio#7, CheckOverflow((promote_precision(cast(cast(sum(coalesce(cr_return_amount#170, cast(0 as double))) as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(sum(coalesce(cs_net_paid#147, cast(0 as double))) as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) AS currency_ratio#8]
            :                                   +- Filter ((((cr_return_amount#170 > cast(10000 as double)) && (cs_net_profit#151 > cast(1 as double))) && ((cs_net_paid#147 > cast(0 as double)) && (cs_quantity#136 > 0))) && (((cs_sold_date_sk#118 = d_date_sk#90) && (d_year#96 = 2001)) && (d_moy#98 = 12)))
            :                                      +- Join Inner
            :                                         :- Join LeftOuter, ((cs_order_number#135 = cr_order_number#168) && (cs_item_sk#133 = cr_item_sk#154))
            :                                         :  :- SubqueryAlias `cs`
            :                                         :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
            :                                         :  :     +- Relation[cs_sold_date_sk#118,cs_sold_time_sk#119,cs_ship_date_sk#120,cs_bill_customer_sk#121,cs_bill_cdemo_sk#122,cs_bill_hdemo_sk#123,cs_bill_addr_sk#124,cs_ship_customer_sk#125,cs_ship_cdemo_sk#126,cs_ship_hdemo_sk#127,cs_ship_addr_sk#128,cs_call_center_sk#129,cs_catalog_page_sk#130,cs_ship_mode_sk#131,cs_warehouse_sk#132,cs_item_sk#133,cs_promo_sk#134,cs_order_number#135,cs_quantity#136,cs_wholesale_cost#137,cs_list_price#138,cs_sales_price#139,cs_ext_discount_amt#140,cs_ext_sales_price#141,... 10 more fields] parquet
            :                                         :  +- SubqueryAlias `cr`
            :                                         :     +- SubqueryAlias `tpcds`.`catalog_returns`
            :                                         :        +- Relation[cr_returned_date_sk#152,cr_returned_time_sk#153,cr_item_sk#154,cr_refunded_customer_sk#155,cr_refunded_cdemo_sk#156,cr_refunded_hdemo_sk#157,cr_refunded_addr_sk#158,cr_returning_customer_sk#159,cr_returning_cdemo_sk#160,cr_returning_hdemo_sk#161,cr_returning_addr_sk#162,cr_call_center_sk#163,cr_catalog_page_sk#164,cr_ship_mode_sk#165,cr_warehouse_sk#166,cr_reason_sk#167,cr_order_number#168,cr_return_quantity#169,cr_return_amount#170,cr_return_tax#171,cr_return_amt_inc_tax#172,cr_fee#173,cr_return_ship_cost#174,cr_refunded_cash#175,... 3 more fields] parquet
            :                                         +- SubqueryAlias `tpcds`.`date_dim`
            :                                            +- Relation[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet
            +- Project [store AS channel#17, item#12, return_ratio#13, return_rank#15, currency_rank#16]
               +- Filter ((return_rank#15 <= 10) || (currency_rank#16 <= 10))
                  +- SubqueryAlias `store`
                     +- Project [item#12, return_ratio#13, currency_ratio#14, return_rank#15, currency_rank#16]
                        +- Project [item#12, return_ratio#13, currency_ratio#14, currency_rank#16, return_rank#15, return_rank#15, currency_rank#16]
                           +- Window [rank(return_ratio#13) windowspecdefinition(return_ratio#13 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#15], [return_ratio#13 ASC NULLS FIRST]
                              +- Window [rank(currency_ratio#14) windowspecdefinition(currency_ratio#14 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#16], [currency_ratio#14 ASC NULLS FIRST]
                                 +- Project [item#12, return_ratio#13, currency_ratio#14]
                                    +- SubqueryAlias `in_store`
                                       +- Aggregate [ss_item_sk#181], [ss_item_sk#181 AS item#12, CheckOverflow((promote_precision(cast(cast(sum(cast(coalesce(sr_return_quantity#212, 0) as bigint)) as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(sum(cast(coalesce(ss_quantity#189, 0) as bigint)) as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) AS return_ratio#13, CheckOverflow((promote_precision(cast(cast(sum(coalesce(sr_return_amt#213, cast(0 as double))) as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(sum(coalesce(ss_net_paid#199, cast(0 as double))) as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) AS currency_ratio#14]
                                          +- Filter ((((sr_return_amt#213 > cast(10000 as double)) && (ss_net_profit#201 > cast(1 as double))) && ((ss_net_paid#199 > cast(0 as double)) && (ss_quantity#189 > 0))) && (((ss_sold_date_sk#179 = d_date_sk#90) && (d_year#96 = 2001)) && (d_moy#98 = 12)))
                                             +- Join Inner
                                                :- Join LeftOuter, ((ss_ticket_number#188 = sr_ticket_number#211) && (ss_item_sk#181 = sr_item_sk#204))
                                                :  :- SubqueryAlias `sts`
                                                :  :  +- SubqueryAlias `tpcds`.`store_sales`
                                                :  :     +- Relation[ss_sold_date_sk#179,ss_sold_time_sk#180,ss_item_sk#181,ss_customer_sk#182,ss_cdemo_sk#183,ss_hdemo_sk#184,ss_addr_sk#185,ss_store_sk#186,ss_promo_sk#187,ss_ticket_number#188,ss_quantity#189,ss_wholesale_cost#190,ss_list_price#191,ss_sales_price#192,ss_ext_discount_amt#193,ss_ext_sales_price#194,ss_ext_wholesale_cost#195,ss_ext_list_price#196,ss_ext_tax#197,ss_coupon_amt#198,ss_net_paid#199,ss_net_paid_inc_tax#200,ss_net_profit#201] parquet
                                                :  +- SubqueryAlias `sr`
                                                :     +- SubqueryAlias `tpcds`.`store_returns`
                                                :        +- Relation[sr_returned_date_sk#202,sr_return_time_sk#203,sr_item_sk#204,sr_customer_sk#205,sr_cdemo_sk#206,sr_hdemo_sk#207,sr_addr_sk#208,sr_store_sk#209,sr_reason_sk#210,sr_ticket_number#211,sr_return_quantity#212,sr_return_amt#213,sr_return_tax#214,sr_return_amt_inc_tax#215,sr_fee#216,sr_return_ship_cost#217,sr_refunded_cash#218,sr_reversed_charge#219,sr_store_credit#220,sr_net_loss#221] parquet
                                                +- SubqueryAlias `tpcds`.`date_dim`
                                                   +- Relation[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#5 ASC NULLS FIRST, return_rank#3 ASC NULLS FIRST, currency_rank#4 ASC NULLS FIRST], true
      +- Aggregate [channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4], [channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
         +- Union
            :- Project [web AS channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
            :  +- Filter ((return_rank#3 <= 10) || (currency_rank#4 <= 10))
            :     +- Window [rank(return_ratio#1) windowspecdefinition(return_ratio#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#3], [return_ratio#1 ASC NULLS FIRST]
            :        +- Project [item#0, return_ratio#1, currency_rank#4]
            :           +- Window [rank(currency_ratio#2) windowspecdefinition(currency_ratio#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#4], [currency_ratio#2 ASC NULLS FIRST]
            :              +- Aggregate [ws_item_sk#35], [ws_item_sk#35 AS item#0, CheckOverflow((promote_precision(cast(sum(cast(coalesce(wr_return_quantity#80, 0) as bigint)) as decimal(15,4))) / promote_precision(cast(sum(cast(coalesce(ws_quantity#50, 0) as bigint)) as decimal(15,4)))), DecimalType(35,20)) AS return_ratio#1, CheckOverflow((promote_precision(cast(sum(coalesce(wr_return_amt#81, 0.0)) as decimal(15,4))) / promote_precision(cast(sum(coalesce(ws_net_paid#61, 0.0)) as decimal(15,4)))), DecimalType(35,20)) AS currency_ratio#2]
            :                 +- Project [ws_item_sk#35, ws_quantity#50, ws_net_paid#61, wr_return_quantity#80, wr_return_amt#81]
            :                    +- Join Inner, (ws_sold_date_sk#32 = d_date_sk#90)
            :                       :- Project [ws_sold_date_sk#32, ws_item_sk#35, ws_quantity#50, ws_net_paid#61, wr_return_quantity#80, wr_return_amt#81]
            :                       :  +- Join Inner, ((ws_order_number#49 = wr_order_number#79) && (ws_item_sk#35 = wr_item_sk#68))
            :                       :     :- Project [ws_sold_date_sk#32, ws_item_sk#35, ws_order_number#49, ws_quantity#50, ws_net_paid#61]
            :                       :     :  +- Filter ((((((((isnotnull(ws_net_paid#61) && isnotnull(ws_net_profit#65)) && isnotnull(ws_quantity#50)) && (ws_net_profit#65 > 1.0)) && (ws_net_paid#61 > 0.0)) && (ws_quantity#50 > 0)) && isnotnull(ws_item_sk#35)) && isnotnull(ws_order_number#49)) && isnotnull(ws_sold_date_sk#32))
            :                       :     :     +- Relation[ws_sold_date_sk#32,ws_sold_time_sk#33,ws_ship_date_sk#34,ws_item_sk#35,ws_bill_customer_sk#36,ws_bill_cdemo_sk#37,ws_bill_hdemo_sk#38,ws_bill_addr_sk#39,ws_ship_customer_sk#40,ws_ship_cdemo_sk#41,ws_ship_hdemo_sk#42,ws_ship_addr_sk#43,ws_web_page_sk#44,ws_web_site_sk#45,ws_ship_mode_sk#46,ws_warehouse_sk#47,ws_promo_sk#48,ws_order_number#49,ws_quantity#50,ws_wholesale_cost#51,ws_list_price#52,ws_sales_price#53,ws_ext_discount_amt#54,ws_ext_sales_price#55,... 10 more fields] parquet
            :                       :     +- Project [wr_item_sk#68, wr_order_number#79, wr_return_quantity#80, wr_return_amt#81]
            :                       :        +- Filter (((isnotnull(wr_return_amt#81) && (wr_return_amt#81 > 10000.0)) && isnotnull(wr_order_number#79)) && isnotnull(wr_item_sk#68))
            :                       :           +- Relation[wr_returned_date_sk#66,wr_returned_time_sk#67,wr_item_sk#68,wr_refunded_customer_sk#69,wr_refunded_cdemo_sk#70,wr_refunded_hdemo_sk#71,wr_refunded_addr_sk#72,wr_returning_customer_sk#73,wr_returning_cdemo_sk#74,wr_returning_hdemo_sk#75,wr_returning_addr_sk#76,wr_web_page_sk#77,wr_reason_sk#78,wr_order_number#79,wr_return_quantity#80,wr_return_amt#81,wr_return_tax#82,wr_return_amt_inc_tax#83,wr_fee#84,wr_return_ship_cost#85,wr_refunded_cash#86,wr_reversed_charge#87,wr_account_credit#88,wr_net_loss#89] parquet
            :                       +- Project [d_date_sk#90]
            :                          +- Filter ((((isnotnull(d_year#96) && isnotnull(d_moy#98)) && (d_year#96 = 2001)) && (d_moy#98 = 12)) && isnotnull(d_date_sk#90))
            :                             +- Relation[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet
            :- Project [catalog AS channel#11, item#6, return_ratio#7, return_rank#9, currency_rank#10]
            :  +- Filter ((return_rank#9 <= 10) || (currency_rank#10 <= 10))
            :     +- Window [rank(currency_ratio#8) windowspecdefinition(currency_ratio#8 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#10], [currency_ratio#8 ASC NULLS FIRST]
            :        +- Window [rank(return_ratio#7) windowspecdefinition(return_ratio#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#9], [return_ratio#7 ASC NULLS FIRST]
            :           +- Aggregate [cs_item_sk#133], [cs_item_sk#133 AS item#6, CheckOverflow((promote_precision(cast(sum(cast(coalesce(cr_return_quantity#169, 0) as bigint)) as decimal(15,4))) / promote_precision(cast(sum(cast(coalesce(cs_quantity#136, 0) as bigint)) as decimal(15,4)))), DecimalType(35,20)) AS return_ratio#7, CheckOverflow((promote_precision(cast(sum(coalesce(cr_return_amount#170, 0.0)) as decimal(15,4))) / promote_precision(cast(sum(coalesce(cs_net_paid#147, 0.0)) as decimal(15,4)))), DecimalType(35,20)) AS currency_ratio#8]
            :              +- Project [cs_item_sk#133, cs_quantity#136, cs_net_paid#147, cr_return_quantity#169, cr_return_amount#170]
            :                 +- Join Inner, (cs_sold_date_sk#118 = d_date_sk#90)
            :                    :- Project [cs_sold_date_sk#118, cs_item_sk#133, cs_quantity#136, cs_net_paid#147, cr_return_quantity#169, cr_return_amount#170]
            :                    :  +- Join Inner, ((cs_order_number#135 = cr_order_number#168) && (cs_item_sk#133 = cr_item_sk#154))
            :                    :     :- Project [cs_sold_date_sk#118, cs_item_sk#133, cs_order_number#135, cs_quantity#136, cs_net_paid#147]
            :                    :     :  +- Filter ((((((((isnotnull(cs_net_profit#151) && isnotnull(cs_quantity#136)) && isnotnull(cs_net_paid#147)) && (cs_net_profit#151 > 1.0)) && (cs_net_paid#147 > 0.0)) && (cs_quantity#136 > 0)) && isnotnull(cs_item_sk#133)) && isnotnull(cs_order_number#135)) && isnotnull(cs_sold_date_sk#118))
            :                    :     :     +- Relation[cs_sold_date_sk#118,cs_sold_time_sk#119,cs_ship_date_sk#120,cs_bill_customer_sk#121,cs_bill_cdemo_sk#122,cs_bill_hdemo_sk#123,cs_bill_addr_sk#124,cs_ship_customer_sk#125,cs_ship_cdemo_sk#126,cs_ship_hdemo_sk#127,cs_ship_addr_sk#128,cs_call_center_sk#129,cs_catalog_page_sk#130,cs_ship_mode_sk#131,cs_warehouse_sk#132,cs_item_sk#133,cs_promo_sk#134,cs_order_number#135,cs_quantity#136,cs_wholesale_cost#137,cs_list_price#138,cs_sales_price#139,cs_ext_discount_amt#140,cs_ext_sales_price#141,... 10 more fields] parquet
            :                    :     +- Project [cr_item_sk#154, cr_order_number#168, cr_return_quantity#169, cr_return_amount#170]
            :                    :        +- Filter (((isnotnull(cr_return_amount#170) && (cr_return_amount#170 > 10000.0)) && isnotnull(cr_item_sk#154)) && isnotnull(cr_order_number#168))
            :                    :           +- Relation[cr_returned_date_sk#152,cr_returned_time_sk#153,cr_item_sk#154,cr_refunded_customer_sk#155,cr_refunded_cdemo_sk#156,cr_refunded_hdemo_sk#157,cr_refunded_addr_sk#158,cr_returning_customer_sk#159,cr_returning_cdemo_sk#160,cr_returning_hdemo_sk#161,cr_returning_addr_sk#162,cr_call_center_sk#163,cr_catalog_page_sk#164,cr_ship_mode_sk#165,cr_warehouse_sk#166,cr_reason_sk#167,cr_order_number#168,cr_return_quantity#169,cr_return_amount#170,cr_return_tax#171,cr_return_amt_inc_tax#172,cr_fee#173,cr_return_ship_cost#174,cr_refunded_cash#175,... 3 more fields] parquet
            :                    +- Project [d_date_sk#90]
            :                       +- Filter ((((isnotnull(d_year#96) && isnotnull(d_moy#98)) && (d_year#96 = 2001)) && (d_moy#98 = 12)) && isnotnull(d_date_sk#90))
            :                          +- Relation[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet
            +- Project [store AS channel#17, item#12, return_ratio#13, return_rank#15, currency_rank#16]
               +- Filter ((return_rank#15 <= 10) || (currency_rank#16 <= 10))
                  +- Window [rank(return_ratio#13) windowspecdefinition(return_ratio#13 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#15], [return_ratio#13 ASC NULLS FIRST]
                     +- Project [item#12, return_ratio#13, currency_rank#16]
                        +- Window [rank(currency_ratio#14) windowspecdefinition(currency_ratio#14 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#16], [currency_ratio#14 ASC NULLS FIRST]
                           +- Aggregate [ss_item_sk#181], [ss_item_sk#181 AS item#12, CheckOverflow((promote_precision(cast(sum(cast(coalesce(sr_return_quantity#212, 0) as bigint)) as decimal(15,4))) / promote_precision(cast(sum(cast(coalesce(ss_quantity#189, 0) as bigint)) as decimal(15,4)))), DecimalType(35,20)) AS return_ratio#13, CheckOverflow((promote_precision(cast(sum(coalesce(sr_return_amt#213, 0.0)) as decimal(15,4))) / promote_precision(cast(sum(coalesce(ss_net_paid#199, 0.0)) as decimal(15,4)))), DecimalType(35,20)) AS currency_ratio#14]
                              +- Project [ss_item_sk#181, ss_quantity#189, ss_net_paid#199, sr_return_quantity#212, sr_return_amt#213]
                                 +- Join Inner, (ss_sold_date_sk#179 = d_date_sk#90)
                                    :- Project [ss_sold_date_sk#179, ss_item_sk#181, ss_quantity#189, ss_net_paid#199, sr_return_quantity#212, sr_return_amt#213]
                                    :  +- Join Inner, ((ss_ticket_number#188 = sr_ticket_number#211) && (ss_item_sk#181 = sr_item_sk#204))
                                    :     :- Project [ss_sold_date_sk#179, ss_item_sk#181, ss_ticket_number#188, ss_quantity#189, ss_net_paid#199]
                                    :     :  +- Filter ((((((((isnotnull(ss_net_paid#199) && isnotnull(ss_net_profit#201)) && isnotnull(ss_quantity#189)) && (ss_net_profit#201 > 1.0)) && (ss_net_paid#199 > 0.0)) && (ss_quantity#189 > 0)) && isnotnull(ss_ticket_number#188)) && isnotnull(ss_item_sk#181)) && isnotnull(ss_sold_date_sk#179))
                                    :     :     +- Relation[ss_sold_date_sk#179,ss_sold_time_sk#180,ss_item_sk#181,ss_customer_sk#182,ss_cdemo_sk#183,ss_hdemo_sk#184,ss_addr_sk#185,ss_store_sk#186,ss_promo_sk#187,ss_ticket_number#188,ss_quantity#189,ss_wholesale_cost#190,ss_list_price#191,ss_sales_price#192,ss_ext_discount_amt#193,ss_ext_sales_price#194,ss_ext_wholesale_cost#195,ss_ext_list_price#196,ss_ext_tax#197,ss_coupon_amt#198,ss_net_paid#199,ss_net_paid_inc_tax#200,ss_net_profit#201] parquet
                                    :     +- Project [sr_item_sk#204, sr_ticket_number#211, sr_return_quantity#212, sr_return_amt#213]
                                    :        +- Filter (((isnotnull(sr_return_amt#213) && (sr_return_amt#213 > 10000.0)) && isnotnull(sr_item_sk#204)) && isnotnull(sr_ticket_number#211))
                                    :           +- Relation[sr_returned_date_sk#202,sr_return_time_sk#203,sr_item_sk#204,sr_customer_sk#205,sr_cdemo_sk#206,sr_hdemo_sk#207,sr_addr_sk#208,sr_store_sk#209,sr_reason_sk#210,sr_ticket_number#211,sr_return_quantity#212,sr_return_amt#213,sr_return_tax#214,sr_return_amt_inc_tax#215,sr_fee#216,sr_return_ship_cost#217,sr_refunded_cash#218,sr_reversed_charge#219,sr_store_credit#220,sr_net_loss#221] parquet
                                    +- Project [d_date_sk#90]
                                       +- Filter ((((isnotnull(d_year#96) && isnotnull(d_moy#98)) && (d_year#96 = 2001)) && (d_moy#98 = 12)) && isnotnull(d_date_sk#90))
                                          +- Relation[d_date_sk#90,d_date_id#91,d_date#92,d_month_seq#93,d_week_seq#94,d_quarter_seq#95,d_year#96,d_dow#97,d_moy#98,d_dom#99,d_qoy#100,d_fy_year#101,d_fy_quarter_seq#102,d_fy_week_seq#103,d_day_name#104,d_quarter_name#105,d_holiday#106,d_weekend#107,d_following_holiday#108,d_first_dom#109,d_last_dom#110,d_same_day_ly#111,d_same_day_lq#112,d_current_day#113,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[channel#5 ASC NULLS FIRST,return_rank#3 ASC NULLS FIRST,currency_rank#4 ASC NULLS FIRST], output=[channel#5,item#0,return_ratio#1,return_rank#3,currency_rank#4])
+- *(23) HashAggregate(keys=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4], functions=[], output=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4])
   +- Exchange hashpartitioning(channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4, 200)
      +- *(22) HashAggregate(keys=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4], functions=[], output=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4])
         +- Union
            :- *(7) Project [web AS channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
            :  +- *(7) Filter ((return_rank#3 <= 10) || (currency_rank#4 <= 10))
            :     +- Window [rank(return_ratio#1) windowspecdefinition(return_ratio#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#3], [return_ratio#1 ASC NULLS FIRST]
            :        +- *(6) Sort [return_ratio#1 ASC NULLS FIRST], false, 0
            :           +- *(6) Project [item#0, return_ratio#1, currency_rank#4]
            :              +- Window [rank(currency_ratio#2) windowspecdefinition(currency_ratio#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#4], [currency_ratio#2 ASC NULLS FIRST]
            :                 +- *(5) Sort [currency_ratio#2 ASC NULLS FIRST], false, 0
            :                    +- Exchange SinglePartition
            :                       +- *(4) HashAggregate(keys=[ws_item_sk#35], functions=[sum(cast(coalesce(wr_return_quantity#80, 0) as bigint)), sum(cast(coalesce(ws_quantity#50, 0) as bigint)), sum(coalesce(wr_return_amt#81, 0.0)), sum(coalesce(ws_net_paid#61, 0.0))], output=[item#0, return_ratio#1, currency_ratio#2])
            :                          +- Exchange hashpartitioning(ws_item_sk#35, 200)
            :                             +- *(3) HashAggregate(keys=[ws_item_sk#35], functions=[partial_sum(cast(coalesce(wr_return_quantity#80, 0) as bigint)), partial_sum(cast(coalesce(ws_quantity#50, 0) as bigint)), partial_sum(coalesce(wr_return_amt#81, 0.0)), partial_sum(coalesce(ws_net_paid#61, 0.0))], output=[ws_item_sk#35, sum#292L, sum#293L, sum#294, sum#295])
            :                                +- *(3) Project [ws_item_sk#35, ws_quantity#50, ws_net_paid#61, wr_return_quantity#80, wr_return_amt#81]
            :                                   +- *(3) BroadcastHashJoin [ws_sold_date_sk#32], [d_date_sk#90], Inner, BuildRight
            :                                      :- *(3) Project [ws_sold_date_sk#32, ws_item_sk#35, ws_quantity#50, ws_net_paid#61, wr_return_quantity#80, wr_return_amt#81]
            :                                      :  +- *(3) BroadcastHashJoin [ws_order_number#49, ws_item_sk#35], [wr_order_number#79, wr_item_sk#68], Inner, BuildRight
            :                                      :     :- *(3) Project [ws_sold_date_sk#32, ws_item_sk#35, ws_order_number#49, ws_quantity#50, ws_net_paid#61]
            :                                      :     :  +- *(3) Filter ((((((((isnotnull(ws_net_paid#61) && isnotnull(ws_net_profit#65)) && isnotnull(ws_quantity#50)) && (ws_net_profit#65 > 1.0)) && (ws_net_paid#61 > 0.0)) && (ws_quantity#50 > 0)) && isnotnull(ws_item_sk#35)) && isnotnull(ws_order_number#49)) && isnotnull(ws_sold_date_sk#32))
            :                                      :     :     +- *(3) FileScan parquet tpcds.web_sales[ws_sold_date_sk#32,ws_item_sk#35,ws_order_number#49,ws_quantity#50,ws_net_paid#61,ws_net_profit#65] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_net_paid), IsNotNull(ws_net_profit), IsNotNull(ws_quantity), GreaterThan(ws_net_pro..., ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_order_number:int,ws_quantity:int,ws_net_paid:double,...
            :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
            :                                      :        +- *(1) Project [wr_item_sk#68, wr_order_number#79, wr_return_quantity#80, wr_return_amt#81]
            :                                      :           +- *(1) Filter (((isnotnull(wr_return_amt#81) && (wr_return_amt#81 > 10000.0)) && isnotnull(wr_order_number#79)) && isnotnull(wr_item_sk#68))
            :                                      :              +- *(1) FileScan parquet tpcds.web_returns[wr_item_sk#68,wr_order_number#79,wr_return_quantity#80,wr_return_amt#81] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_return_amt), GreaterThan(wr_return_amt,10000.0), IsNotNull(wr_order_number), IsNotN..., ReadSchema: struct<wr_item_sk:int,wr_order_number:int,wr_return_quantity:int,wr_return_amt:double>
            :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :                                         +- *(2) Project [d_date_sk#90]
            :                                            +- *(2) Filter ((((isnotnull(d_year#96) && isnotnull(d_moy#98)) && (d_year#96 = 2001)) && (d_moy#98 = 12)) && isnotnull(d_date_sk#90))
            :                                               +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#90,d_year#96,d_moy#98] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,12), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
            :- *(14) Project [catalog AS channel#11, item#6, return_ratio#7, return_rank#9, currency_rank#10]
            :  +- *(14) Filter ((return_rank#9 <= 10) || (currency_rank#10 <= 10))
            :     +- Window [rank(currency_ratio#8) windowspecdefinition(currency_ratio#8 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#10], [currency_ratio#8 ASC NULLS FIRST]
            :        +- *(13) Sort [currency_ratio#8 ASC NULLS FIRST], false, 0
            :           +- Window [rank(return_ratio#7) windowspecdefinition(return_ratio#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#9], [return_ratio#7 ASC NULLS FIRST]
            :              +- *(12) Sort [return_ratio#7 ASC NULLS FIRST], false, 0
            :                 +- Exchange SinglePartition
            :                    +- *(11) HashAggregate(keys=[cs_item_sk#133], functions=[sum(cast(coalesce(cr_return_quantity#169, 0) as bigint)), sum(cast(coalesce(cs_quantity#136, 0) as bigint)), sum(coalesce(cr_return_amount#170, 0.0)), sum(coalesce(cs_net_paid#147, 0.0))], output=[item#6, return_ratio#7, currency_ratio#8])
            :                       +- Exchange hashpartitioning(cs_item_sk#133, 200)
            :                          +- *(10) HashAggregate(keys=[cs_item_sk#133], functions=[partial_sum(cast(coalesce(cr_return_quantity#169, 0) as bigint)), partial_sum(cast(coalesce(cs_quantity#136, 0) as bigint)), partial_sum(coalesce(cr_return_amount#170, 0.0)), partial_sum(coalesce(cs_net_paid#147, 0.0))], output=[cs_item_sk#133, sum#300L, sum#301L, sum#302, sum#303])
            :                             +- *(10) Project [cs_item_sk#133, cs_quantity#136, cs_net_paid#147, cr_return_quantity#169, cr_return_amount#170]
            :                                +- *(10) BroadcastHashJoin [cs_sold_date_sk#118], [d_date_sk#90], Inner, BuildRight
            :                                   :- *(10) Project [cs_sold_date_sk#118, cs_item_sk#133, cs_quantity#136, cs_net_paid#147, cr_return_quantity#169, cr_return_amount#170]
            :                                   :  +- *(10) BroadcastHashJoin [cs_order_number#135, cs_item_sk#133], [cr_order_number#168, cr_item_sk#154], Inner, BuildRight
            :                                   :     :- *(10) Project [cs_sold_date_sk#118, cs_item_sk#133, cs_order_number#135, cs_quantity#136, cs_net_paid#147]
            :                                   :     :  +- *(10) Filter ((((((((isnotnull(cs_net_profit#151) && isnotnull(cs_quantity#136)) && isnotnull(cs_net_paid#147)) && (cs_net_profit#151 > 1.0)) && (cs_net_paid#147 > 0.0)) && (cs_quantity#136 > 0)) && isnotnull(cs_item_sk#133)) && isnotnull(cs_order_number#135)) && isnotnull(cs_sold_date_sk#118))
            :                                   :     :     +- *(10) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#118,cs_item_sk#133,cs_order_number#135,cs_quantity#136,cs_net_paid#147,cs_net_profit#151] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_net_profit), IsNotNull(cs_quantity), IsNotNull(cs_net_paid), GreaterThan(cs_net_pro..., ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_order_number:int,cs_quantity:int,cs_net_paid:double,...
            :                                   :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
            :                                   :        +- *(8) Project [cr_item_sk#154, cr_order_number#168, cr_return_quantity#169, cr_return_amount#170]
            :                                   :           +- *(8) Filter (((isnotnull(cr_return_amount#170) && (cr_return_amount#170 > 10000.0)) && isnotnull(cr_item_sk#154)) && isnotnull(cr_order_number#168))
            :                                   :              +- *(8) FileScan parquet tpcds.catalog_returns[cr_item_sk#154,cr_order_number#168,cr_return_quantity#169,cr_return_amount#170] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_return_amount), GreaterThan(cr_return_amount,10000.0), IsNotNull(cr_item_sk), IsNot..., ReadSchema: struct<cr_item_sk:int,cr_order_number:int,cr_return_quantity:int,cr_return_amount:double>
            :                                   +- ReusedExchange [d_date_sk#90], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            +- *(21) Project [store AS channel#17, item#12, return_ratio#13, return_rank#15, currency_rank#16]
               +- *(21) Filter ((return_rank#15 <= 10) || (currency_rank#16 <= 10))
                  +- Window [rank(return_ratio#13) windowspecdefinition(return_ratio#13 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#15], [return_ratio#13 ASC NULLS FIRST]
                     +- *(20) Sort [return_ratio#13 ASC NULLS FIRST], false, 0
                        +- *(20) Project [item#12, return_ratio#13, currency_rank#16]
                           +- Window [rank(currency_ratio#14) windowspecdefinition(currency_ratio#14 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#16], [currency_ratio#14 ASC NULLS FIRST]
                              +- *(19) Sort [currency_ratio#14 ASC NULLS FIRST], false, 0
                                 +- Exchange SinglePartition
                                    +- *(18) HashAggregate(keys=[ss_item_sk#181], functions=[sum(cast(coalesce(sr_return_quantity#212, 0) as bigint)), sum(cast(coalesce(ss_quantity#189, 0) as bigint)), sum(coalesce(sr_return_amt#213, 0.0)), sum(coalesce(ss_net_paid#199, 0.0))], output=[item#12, return_ratio#13, currency_ratio#14])
                                       +- Exchange hashpartitioning(ss_item_sk#181, 200)
                                          +- *(17) HashAggregate(keys=[ss_item_sk#181], functions=[partial_sum(cast(coalesce(sr_return_quantity#212, 0) as bigint)), partial_sum(cast(coalesce(ss_quantity#189, 0) as bigint)), partial_sum(coalesce(sr_return_amt#213, 0.0)), partial_sum(coalesce(ss_net_paid#199, 0.0))], output=[ss_item_sk#181, sum#308L, sum#309L, sum#310, sum#311])
                                             +- *(17) Project [ss_item_sk#181, ss_quantity#189, ss_net_paid#199, sr_return_quantity#212, sr_return_amt#213]
                                                +- *(17) BroadcastHashJoin [ss_sold_date_sk#179], [d_date_sk#90], Inner, BuildRight
                                                   :- *(17) Project [ss_sold_date_sk#179, ss_item_sk#181, ss_quantity#189, ss_net_paid#199, sr_return_quantity#212, sr_return_amt#213]
                                                   :  +- *(17) BroadcastHashJoin [ss_ticket_number#188, ss_item_sk#181], [sr_ticket_number#211, sr_item_sk#204], Inner, BuildRight
                                                   :     :- *(17) Project [ss_sold_date_sk#179, ss_item_sk#181, ss_ticket_number#188, ss_quantity#189, ss_net_paid#199]
                                                   :     :  +- *(17) Filter ((((((((isnotnull(ss_net_paid#199) && isnotnull(ss_net_profit#201)) && isnotnull(ss_quantity#189)) && (ss_net_profit#201 > 1.0)) && (ss_net_paid#199 > 0.0)) && (ss_quantity#189 > 0)) && isnotnull(ss_ticket_number#188)) && isnotnull(ss_item_sk#181)) && isnotnull(ss_sold_date_sk#179))
                                                   :     :     +- *(17) FileScan parquet tpcds.store_sales[ss_sold_date_sk#179,ss_item_sk#181,ss_ticket_number#188,ss_quantity#189,ss_net_paid#199,ss_net_profit#201] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_net_paid), IsNotNull(ss_net_profit), IsNotNull(ss_quantity), GreaterThan(ss_net_pro..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ticket_number:int,ss_quantity:int,ss_net_paid:double...
                                                   :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
                                                   :        +- *(15) Project [sr_item_sk#204, sr_ticket_number#211, sr_return_quantity#212, sr_return_amt#213]
                                                   :           +- *(15) Filter (((isnotnull(sr_return_amt#213) && (sr_return_amt#213 > 10000.0)) && isnotnull(sr_item_sk#204)) && isnotnull(sr_ticket_number#211))
                                                   :              +- *(15) FileScan parquet tpcds.store_returns[sr_item_sk#204,sr_ticket_number#211,sr_return_quantity#212,sr_return_amt#213] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_return_amt), GreaterThan(sr_return_amt,10000.0), IsNotNull(sr_item_sk), IsNotNull(s..., ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int,sr_return_quantity:int,sr_return_amt:double>
                                                   +- ReusedExchange [d_date_sk#90], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.339 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 49 in stream 0 using template query49.tpl
------------------------------------------------------^^^

