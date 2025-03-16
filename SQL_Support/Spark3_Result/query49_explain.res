Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581446111
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort [1 ASC NULLS FIRST, 4 ASC NULLS FIRST, 5 ASC NULLS FIRST], true
      +- 'Distinct
         +- 'Union false, false
            :- 'Distinct
            :  +- 'Union false, false
            :     :- 'Project [web AS channel#5, 'web.item, 'web.return_ratio, 'web.return_rank, 'web.currency_rank]
            :     :  +- 'Filter (('web.return_rank <= 10) OR ('web.currency_rank <= 10))
            :     :     +- 'SubqueryAlias web
            :     :        +- 'Project ['item, 'return_ratio, 'currency_ratio, 'rank() windowspecdefinition('return_ratio ASC NULLS FIRST, unspecifiedframe$()) AS return_rank#3, 'rank() windowspecdefinition('currency_ratio ASC NULLS FIRST, unspecifiedframe$()) AS currency_rank#4]
            :     :           +- 'SubqueryAlias in_web
            :     :              +- 'Aggregate ['ws.ws_item_sk], ['ws.ws_item_sk AS item#0, (cast('sum('coalesce('wr.wr_return_quantity, 0)) as decimal(15,4)) / cast('sum('coalesce('ws.ws_quantity, 0)) as decimal(15,4))) AS return_ratio#1, (cast('sum('coalesce('wr.wr_return_amt, 0)) as decimal(15,4)) / cast('sum('coalesce('ws.ws_net_paid, 0)) as decimal(15,4))) AS currency_ratio#2]
            :     :                 +- 'Filter (((('wr.wr_return_amt > 10000) AND ('ws.ws_net_profit > 1)) AND (('ws.ws_net_paid > 0) AND ('ws.ws_quantity > 0))) AND ((('ws_sold_date_sk = 'd_date_sk) AND ('d_year = 2001)) AND ('d_moy = 12)))
            :     :                    +- 'Join Inner
            :     :                       :- 'Join LeftOuter, (('ws.ws_order_number = 'wr.wr_order_number) AND ('ws.ws_item_sk = 'wr.wr_item_sk))
            :     :                       :  :- 'SubqueryAlias ws
            :     :                       :  :  +- 'UnresolvedRelation [web_sales], [], false
            :     :                       :  +- 'SubqueryAlias wr
            :     :                       :     +- 'UnresolvedRelation [web_returns], [], false
            :     :                       +- 'UnresolvedRelation [date_dim], [], false
            :     +- 'Project [catalog AS channel#11, 'catalog.item, 'catalog.return_ratio, 'catalog.return_rank, 'catalog.currency_rank]
            :        +- 'Filter (('catalog.return_rank <= 10) OR ('catalog.currency_rank <= 10))
            :           +- 'SubqueryAlias catalog
            :              +- 'Project ['item, 'return_ratio, 'currency_ratio, 'rank() windowspecdefinition('return_ratio ASC NULLS FIRST, unspecifiedframe$()) AS return_rank#9, 'rank() windowspecdefinition('currency_ratio ASC NULLS FIRST, unspecifiedframe$()) AS currency_rank#10]
            :                 +- 'SubqueryAlias in_cat
            :                    +- 'Aggregate ['cs.cs_item_sk], ['cs.cs_item_sk AS item#6, (cast('sum('coalesce('cr.cr_return_quantity, 0)) as decimal(15,4)) / cast('sum('coalesce('cs.cs_quantity, 0)) as decimal(15,4))) AS return_ratio#7, (cast('sum('coalesce('cr.cr_return_amount, 0)) as decimal(15,4)) / cast('sum('coalesce('cs.cs_net_paid, 0)) as decimal(15,4))) AS currency_ratio#8]
            :                       +- 'Filter (((('cr.cr_return_amount > 10000) AND ('cs.cs_net_profit > 1)) AND (('cs.cs_net_paid > 0) AND ('cs.cs_quantity > 0))) AND ((('cs_sold_date_sk = 'd_date_sk) AND ('d_year = 2001)) AND ('d_moy = 12)))
            :                          +- 'Join Inner
            :                             :- 'Join LeftOuter, (('cs.cs_order_number = 'cr.cr_order_number) AND ('cs.cs_item_sk = 'cr.cr_item_sk))
            :                             :  :- 'SubqueryAlias cs
            :                             :  :  +- 'UnresolvedRelation [catalog_sales], [], false
            :                             :  +- 'SubqueryAlias cr
            :                             :     +- 'UnresolvedRelation [catalog_returns], [], false
            :                             +- 'UnresolvedRelation [date_dim], [], false
            +- 'Project [store AS channel#17, 'store.item, 'store.return_ratio, 'store.return_rank, 'store.currency_rank]
               +- 'Filter (('store.return_rank <= 10) OR ('store.currency_rank <= 10))
                  +- 'SubqueryAlias store
                     +- 'Project ['item, 'return_ratio, 'currency_ratio, 'rank() windowspecdefinition('return_ratio ASC NULLS FIRST, unspecifiedframe$()) AS return_rank#15, 'rank() windowspecdefinition('currency_ratio ASC NULLS FIRST, unspecifiedframe$()) AS currency_rank#16]
                        +- 'SubqueryAlias in_store
                           +- 'Aggregate ['sts.ss_item_sk], ['sts.ss_item_sk AS item#12, (cast('sum('coalesce('sr.sr_return_quantity, 0)) as decimal(15,4)) / cast('sum('coalesce('sts.ss_quantity, 0)) as decimal(15,4))) AS return_ratio#13, (cast('sum('coalesce('sr.sr_return_amt, 0)) as decimal(15,4)) / cast('sum('coalesce('sts.ss_net_paid, 0)) as decimal(15,4))) AS currency_ratio#14]
                              +- 'Filter (((('sr.sr_return_amt > 10000) AND ('sts.ss_net_profit > 1)) AND (('sts.ss_net_paid > 0) AND ('sts.ss_quantity > 0))) AND ((('ss_sold_date_sk = 'd_date_sk) AND ('d_year = 2001)) AND ('d_moy = 12)))
                                 +- 'Join Inner
                                    :- 'Join LeftOuter, (('sts.ss_ticket_number = 'sr.sr_ticket_number) AND ('sts.ss_item_sk = 'sr.sr_item_sk))
                                    :  :- 'SubqueryAlias sts
                                    :  :  +- 'UnresolvedRelation [store_sales], [], false
                                    :  +- 'SubqueryAlias sr
                                    :     +- 'UnresolvedRelation [store_returns], [], false
                                    +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
channel: string, item: int, return_ratio: decimal(35,20), return_rank: int, currency_rank: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#5 ASC NULLS FIRST, return_rank#3 ASC NULLS FIRST, currency_rank#4 ASC NULLS FIRST], true
      +- Distinct
         +- Union false, false
            :- Distinct
            :  +- Union false, false
            :     :- Project [web AS channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
            :     :  +- Filter ((return_rank#3 <= 10) OR (currency_rank#4 <= 10))
            :     :     +- SubqueryAlias web
            :     :        +- Project [item#0, return_ratio#1, currency_ratio#2, return_rank#3, currency_rank#4]
            :     :           +- Project [item#0, return_ratio#1, currency_ratio#2, return_rank#3, currency_rank#4, return_rank#3, currency_rank#4]
            :     :              +- Window [rank(currency_ratio#2) windowspecdefinition(currency_ratio#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#4], [currency_ratio#2 ASC NULLS FIRST]
            :     :                 +- Window [rank(return_ratio#1) windowspecdefinition(return_ratio#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#3], [return_ratio#1 ASC NULLS FIRST]
            :     :                    +- Project [item#0, return_ratio#1, currency_ratio#2]
            :     :                       +- SubqueryAlias in_web
            :     :                          +- Aggregate [ws_item_sk#38], [ws_item_sk#38 AS item#0, (cast(sum(coalesce(wr_return_quantity#83, 0)) as decimal(15,4)) / cast(sum(coalesce(ws_quantity#53, 0)) as decimal(15,4))) AS return_ratio#1, (cast(sum(coalesce(wr_return_amt#84, cast(0 as double))) as decimal(15,4)) / cast(sum(coalesce(ws_net_paid#64, cast(0 as double))) as decimal(15,4))) AS currency_ratio#2]
            :     :                             +- Filter ((((wr_return_amt#84 > cast(10000 as double)) AND (ws_net_profit#68 > cast(1 as double))) AND ((ws_net_paid#64 > cast(0 as double)) AND (ws_quantity#53 > 0))) AND (((ws_sold_date_sk#35 = d_date_sk#93) AND (d_year#99 = 2001)) AND (d_moy#101 = 12)))
            :     :                                +- Join Inner
            :     :                                   :- Join LeftOuter, ((ws_order_number#52 = wr_order_number#82) AND (ws_item_sk#38 = wr_item_sk#71))
            :     :                                   :  :- SubqueryAlias ws
            :     :                                   :  :  +- SubqueryAlias spark_catalog.tpcds.web_sales
            :     :                                   :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#35,ws_sold_time_sk#36,ws_ship_date_sk#37,ws_item_sk#38,ws_bill_customer_sk#39,ws_bill_cdemo_sk#40,ws_bill_hdemo_sk#41,ws_bill_addr_sk#42,ws_ship_customer_sk#43,ws_ship_cdemo_sk#44,ws_ship_hdemo_sk#45,ws_ship_addr_sk#46,ws_web_page_sk#47,ws_web_site_sk#48,ws_ship_mode_sk#49,ws_warehouse_sk#50,ws_promo_sk#51,ws_order_number#52,ws_quantity#53,ws_wholesale_cost#54,ws_list_price#55,ws_sales_price#56,ws_ext_discount_amt#57,ws_ext_sales_price#58,... 10 more fields] parquet
            :     :                                   :  +- SubqueryAlias wr
            :     :                                   :     +- SubqueryAlias spark_catalog.tpcds.web_returns
            :     :                                   :        +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#69,wr_returned_time_sk#70,wr_item_sk#71,wr_refunded_customer_sk#72,wr_refunded_cdemo_sk#73,wr_refunded_hdemo_sk#74,wr_refunded_addr_sk#75,wr_returning_customer_sk#76,wr_returning_cdemo_sk#77,wr_returning_hdemo_sk#78,wr_returning_addr_sk#79,wr_web_page_sk#80,wr_reason_sk#81,wr_order_number#82,wr_return_quantity#83,wr_return_amt#84,wr_return_tax#85,wr_return_amt_inc_tax#86,wr_fee#87,wr_return_ship_cost#88,wr_refunded_cash#89,wr_reversed_charge#90,wr_account_credit#91,wr_net_loss#92] parquet
            :     :                                   +- SubqueryAlias spark_catalog.tpcds.date_dim
            :     :                                      +- Relation spark_catalog.tpcds.date_dim[d_date_sk#93,d_date_id#94,d_date#95,d_month_seq#96,d_week_seq#97,d_quarter_seq#98,d_year#99,d_dow#100,d_moy#101,d_dom#102,d_qoy#103,d_fy_year#104,d_fy_quarter_seq#105,d_fy_week_seq#106,d_day_name#107,d_quarter_name#108,d_holiday#109,d_weekend#110,d_following_holiday#111,d_first_dom#112,d_last_dom#113,d_same_day_ly#114,d_same_day_lq#115,d_current_day#116,... 4 more fields] parquet
            :     +- Project [catalog AS channel#11, item#6, return_ratio#7, return_rank#9, currency_rank#10]
            :        +- Filter ((return_rank#9 <= 10) OR (currency_rank#10 <= 10))
            :           +- SubqueryAlias catalog
            :              +- Project [item#6, return_ratio#7, currency_ratio#8, return_rank#9, currency_rank#10]
            :                 +- Project [item#6, return_ratio#7, currency_ratio#8, return_rank#9, currency_rank#10, return_rank#9, currency_rank#10]
            :                    +- Window [rank(currency_ratio#8) windowspecdefinition(currency_ratio#8 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#10], [currency_ratio#8 ASC NULLS FIRST]
            :                       +- Window [rank(return_ratio#7) windowspecdefinition(return_ratio#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#9], [return_ratio#7 ASC NULLS FIRST]
            :                          +- Project [item#6, return_ratio#7, currency_ratio#8]
            :                             +- SubqueryAlias in_cat
            :                                +- Aggregate [cs_item_sk#136], [cs_item_sk#136 AS item#6, (cast(sum(coalesce(cr_return_quantity#172, 0)) as decimal(15,4)) / cast(sum(coalesce(cs_quantity#139, 0)) as decimal(15,4))) AS return_ratio#7, (cast(sum(coalesce(cr_return_amount#173, cast(0 as double))) as decimal(15,4)) / cast(sum(coalesce(cs_net_paid#150, cast(0 as double))) as decimal(15,4))) AS currency_ratio#8]
            :                                   +- Filter ((((cr_return_amount#173 > cast(10000 as double)) AND (cs_net_profit#154 > cast(1 as double))) AND ((cs_net_paid#150 > cast(0 as double)) AND (cs_quantity#139 > 0))) AND (((cs_sold_date_sk#121 = d_date_sk#225) AND (d_year#231 = 2001)) AND (d_moy#233 = 12)))
            :                                      +- Join Inner
            :                                         :- Join LeftOuter, ((cs_order_number#138 = cr_order_number#171) AND (cs_item_sk#136 = cr_item_sk#157))
            :                                         :  :- SubqueryAlias cs
            :                                         :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_sales
            :                                         :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#121,cs_sold_time_sk#122,cs_ship_date_sk#123,cs_bill_customer_sk#124,cs_bill_cdemo_sk#125,cs_bill_hdemo_sk#126,cs_bill_addr_sk#127,cs_ship_customer_sk#128,cs_ship_cdemo_sk#129,cs_ship_hdemo_sk#130,cs_ship_addr_sk#131,cs_call_center_sk#132,cs_catalog_page_sk#133,cs_ship_mode_sk#134,cs_warehouse_sk#135,cs_item_sk#136,cs_promo_sk#137,cs_order_number#138,cs_quantity#139,cs_wholesale_cost#140,cs_list_price#141,cs_sales_price#142,cs_ext_discount_amt#143,cs_ext_sales_price#144,... 10 more fields] parquet
            :                                         :  +- SubqueryAlias cr
            :                                         :     +- SubqueryAlias spark_catalog.tpcds.catalog_returns
            :                                         :        +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#155,cr_returned_time_sk#156,cr_item_sk#157,cr_refunded_customer_sk#158,cr_refunded_cdemo_sk#159,cr_refunded_hdemo_sk#160,cr_refunded_addr_sk#161,cr_returning_customer_sk#162,cr_returning_cdemo_sk#163,cr_returning_hdemo_sk#164,cr_returning_addr_sk#165,cr_call_center_sk#166,cr_catalog_page_sk#167,cr_ship_mode_sk#168,cr_warehouse_sk#169,cr_reason_sk#170,cr_order_number#171,cr_return_quantity#172,cr_return_amount#173,cr_return_tax#174,cr_return_amt_inc_tax#175,cr_fee#176,cr_return_ship_cost#177,cr_refunded_cash#178,... 3 more fields] parquet
            :                                         +- SubqueryAlias spark_catalog.tpcds.date_dim
            :                                            +- Relation spark_catalog.tpcds.date_dim[d_date_sk#225,d_date_id#226,d_date#227,d_month_seq#228,d_week_seq#229,d_quarter_seq#230,d_year#231,d_dow#232,d_moy#233,d_dom#234,d_qoy#235,d_fy_year#236,d_fy_quarter_seq#237,d_fy_week_seq#238,d_day_name#239,d_quarter_name#240,d_holiday#241,d_weekend#242,d_following_holiday#243,d_first_dom#244,d_last_dom#245,d_same_day_ly#246,d_same_day_lq#247,d_current_day#248,... 4 more fields] parquet
            +- Project [store AS channel#17, item#12, return_ratio#13, return_rank#15, currency_rank#16]
               +- Filter ((return_rank#15 <= 10) OR (currency_rank#16 <= 10))
                  +- SubqueryAlias store
                     +- Project [item#12, return_ratio#13, currency_ratio#14, return_rank#15, currency_rank#16]
                        +- Project [item#12, return_ratio#13, currency_ratio#14, return_rank#15, currency_rank#16, return_rank#15, currency_rank#16]
                           +- Window [rank(currency_ratio#14) windowspecdefinition(currency_ratio#14 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#16], [currency_ratio#14 ASC NULLS FIRST]
                              +- Window [rank(return_ratio#13) windowspecdefinition(return_ratio#13 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#15], [return_ratio#13 ASC NULLS FIRST]
                                 +- Project [item#12, return_ratio#13, currency_ratio#14]
                                    +- SubqueryAlias in_store
                                       +- Aggregate [ss_item_sk#184], [ss_item_sk#184 AS item#12, (cast(sum(coalesce(sr_return_quantity#215, 0)) as decimal(15,4)) / cast(sum(coalesce(ss_quantity#192, 0)) as decimal(15,4))) AS return_ratio#13, (cast(sum(coalesce(sr_return_amt#216, cast(0 as double))) as decimal(15,4)) / cast(sum(coalesce(ss_net_paid#202, cast(0 as double))) as decimal(15,4))) AS currency_ratio#14]
                                          +- Filter ((((sr_return_amt#216 > cast(10000 as double)) AND (ss_net_profit#204 > cast(1 as double))) AND ((ss_net_paid#202 > cast(0 as double)) AND (ss_quantity#192 > 0))) AND (((ss_sold_date_sk#182 = d_date_sk#253) AND (d_year#259 = 2001)) AND (d_moy#261 = 12)))
                                             +- Join Inner
                                                :- Join LeftOuter, ((ss_ticket_number#191 = sr_ticket_number#214) AND (ss_item_sk#184 = sr_item_sk#207))
                                                :  :- SubqueryAlias sts
                                                :  :  +- SubqueryAlias spark_catalog.tpcds.store_sales
                                                :  :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#182,ss_sold_time_sk#183,ss_item_sk#184,ss_customer_sk#185,ss_cdemo_sk#186,ss_hdemo_sk#187,ss_addr_sk#188,ss_store_sk#189,ss_promo_sk#190,ss_ticket_number#191,ss_quantity#192,ss_wholesale_cost#193,ss_list_price#194,ss_sales_price#195,ss_ext_discount_amt#196,ss_ext_sales_price#197,ss_ext_wholesale_cost#198,ss_ext_list_price#199,ss_ext_tax#200,ss_coupon_amt#201,ss_net_paid#202,ss_net_paid_inc_tax#203,ss_net_profit#204] parquet
                                                :  +- SubqueryAlias sr
                                                :     +- SubqueryAlias spark_catalog.tpcds.store_returns
                                                :        +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#205,sr_return_time_sk#206,sr_item_sk#207,sr_customer_sk#208,sr_cdemo_sk#209,sr_hdemo_sk#210,sr_addr_sk#211,sr_store_sk#212,sr_reason_sk#213,sr_ticket_number#214,sr_return_quantity#215,sr_return_amt#216,sr_return_tax#217,sr_return_amt_inc_tax#218,sr_fee#219,sr_return_ship_cost#220,sr_refunded_cash#221,sr_reversed_charge#222,sr_store_credit#223,sr_net_loss#224] parquet
                                                +- SubqueryAlias spark_catalog.tpcds.date_dim
                                                   +- Relation spark_catalog.tpcds.date_dim[d_date_sk#253,d_date_id#254,d_date#255,d_month_seq#256,d_week_seq#257,d_quarter_seq#258,d_year#259,d_dow#260,d_moy#261,d_dom#262,d_qoy#263,d_fy_year#264,d_fy_quarter_seq#265,d_fy_week_seq#266,d_day_name#267,d_quarter_name#268,d_holiday#269,d_weekend#270,d_following_holiday#271,d_first_dom#272,d_last_dom#273,d_same_day_ly#274,d_same_day_lq#275,d_current_day#276,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#5 ASC NULLS FIRST, return_rank#3 ASC NULLS FIRST, currency_rank#4 ASC NULLS FIRST], true
      +- Aggregate [channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4], [channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
         +- Union false, false
            :- Project [web AS channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
            :  +- Filter ((return_rank#3 <= 10) OR (currency_rank#4 <= 10))
            :     +- Window [rank(currency_ratio#2) windowspecdefinition(currency_ratio#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#4], [currency_ratio#2 ASC NULLS FIRST]
            :        +- Window [rank(return_ratio#1) windowspecdefinition(return_ratio#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#3], [return_ratio#1 ASC NULLS FIRST]
            :           +- Aggregate [ws_item_sk#38], [ws_item_sk#38 AS item#0, (cast(sum(coalesce(wr_return_quantity#83, 0)) as decimal(15,4)) / cast(sum(coalesce(ws_quantity#53, 0)) as decimal(15,4))) AS return_ratio#1, (cast(sum(coalesce(wr_return_amt#84, 0.0)) as decimal(15,4)) / cast(sum(coalesce(ws_net_paid#64, 0.0)) as decimal(15,4))) AS currency_ratio#2]
            :              +- Project [ws_item_sk#38, ws_quantity#53, ws_net_paid#64, wr_return_quantity#83, wr_return_amt#84]
            :                 +- Join Inner, (ws_sold_date_sk#35 = d_date_sk#93)
            :                    :- Project [ws_sold_date_sk#35, ws_item_sk#38, ws_quantity#53, ws_net_paid#64, wr_return_quantity#83, wr_return_amt#84]
            :                    :  +- Join Inner, ((ws_order_number#52 = wr_order_number#82) AND (ws_item_sk#38 = wr_item_sk#71))
            :                    :     :- Project [ws_sold_date_sk#35, ws_item_sk#38, ws_order_number#52, ws_quantity#53, ws_net_paid#64]
            :                    :     :  +- Filter ((((isnotnull(ws_net_profit#68) AND isnotnull(ws_net_paid#64)) AND isnotnull(ws_quantity#53)) AND (((ws_net_profit#68 > 1.0) AND (ws_net_paid#64 > 0.0)) AND (ws_quantity#53 > 0))) AND ((isnotnull(ws_order_number#52) AND isnotnull(ws_item_sk#38)) AND isnotnull(ws_sold_date_sk#35)))
            :                    :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#35,ws_sold_time_sk#36,ws_ship_date_sk#37,ws_item_sk#38,ws_bill_customer_sk#39,ws_bill_cdemo_sk#40,ws_bill_hdemo_sk#41,ws_bill_addr_sk#42,ws_ship_customer_sk#43,ws_ship_cdemo_sk#44,ws_ship_hdemo_sk#45,ws_ship_addr_sk#46,ws_web_page_sk#47,ws_web_site_sk#48,ws_ship_mode_sk#49,ws_warehouse_sk#50,ws_promo_sk#51,ws_order_number#52,ws_quantity#53,ws_wholesale_cost#54,ws_list_price#55,ws_sales_price#56,ws_ext_discount_amt#57,ws_ext_sales_price#58,... 10 more fields] parquet
            :                    :     +- Project [wr_item_sk#71, wr_order_number#82, wr_return_quantity#83, wr_return_amt#84]
            :                    :        +- Filter ((isnotnull(wr_return_amt#84) AND (wr_return_amt#84 > 10000.0)) AND (isnotnull(wr_order_number#82) AND isnotnull(wr_item_sk#71)))
            :                    :           +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#69,wr_returned_time_sk#70,wr_item_sk#71,wr_refunded_customer_sk#72,wr_refunded_cdemo_sk#73,wr_refunded_hdemo_sk#74,wr_refunded_addr_sk#75,wr_returning_customer_sk#76,wr_returning_cdemo_sk#77,wr_returning_hdemo_sk#78,wr_returning_addr_sk#79,wr_web_page_sk#80,wr_reason_sk#81,wr_order_number#82,wr_return_quantity#83,wr_return_amt#84,wr_return_tax#85,wr_return_amt_inc_tax#86,wr_fee#87,wr_return_ship_cost#88,wr_refunded_cash#89,wr_reversed_charge#90,wr_account_credit#91,wr_net_loss#92] parquet
            :                    +- Project [d_date_sk#93]
            :                       +- Filter (((isnotnull(d_year#99) AND isnotnull(d_moy#101)) AND ((d_year#99 = 2001) AND (d_moy#101 = 12))) AND isnotnull(d_date_sk#93))
            :                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#93,d_date_id#94,d_date#95,d_month_seq#96,d_week_seq#97,d_quarter_seq#98,d_year#99,d_dow#100,d_moy#101,d_dom#102,d_qoy#103,d_fy_year#104,d_fy_quarter_seq#105,d_fy_week_seq#106,d_day_name#107,d_quarter_name#108,d_holiday#109,d_weekend#110,d_following_holiday#111,d_first_dom#112,d_last_dom#113,d_same_day_ly#114,d_same_day_lq#115,d_current_day#116,... 4 more fields] parquet
            :- Project [catalog AS channel#11, item#6, return_ratio#7, return_rank#9, currency_rank#10]
            :  +- Filter ((return_rank#9 <= 10) OR (currency_rank#10 <= 10))
            :     +- Window [rank(currency_ratio#8) windowspecdefinition(currency_ratio#8 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#10], [currency_ratio#8 ASC NULLS FIRST]
            :        +- Window [rank(return_ratio#7) windowspecdefinition(return_ratio#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#9], [return_ratio#7 ASC NULLS FIRST]
            :           +- Aggregate [cs_item_sk#136], [cs_item_sk#136 AS item#6, (cast(sum(coalesce(cr_return_quantity#172, 0)) as decimal(15,4)) / cast(sum(coalesce(cs_quantity#139, 0)) as decimal(15,4))) AS return_ratio#7, (cast(sum(coalesce(cr_return_amount#173, 0.0)) as decimal(15,4)) / cast(sum(coalesce(cs_net_paid#150, 0.0)) as decimal(15,4))) AS currency_ratio#8]
            :              +- Project [cs_item_sk#136, cs_quantity#139, cs_net_paid#150, cr_return_quantity#172, cr_return_amount#173]
            :                 +- Join Inner, (cs_sold_date_sk#121 = d_date_sk#225)
            :                    :- Project [cs_sold_date_sk#121, cs_item_sk#136, cs_quantity#139, cs_net_paid#150, cr_return_quantity#172, cr_return_amount#173]
            :                    :  +- Join Inner, ((cs_order_number#138 = cr_order_number#171) AND (cs_item_sk#136 = cr_item_sk#157))
            :                    :     :- Project [cs_sold_date_sk#121, cs_item_sk#136, cs_order_number#138, cs_quantity#139, cs_net_paid#150]
            :                    :     :  +- Filter ((((isnotnull(cs_net_profit#154) AND isnotnull(cs_net_paid#150)) AND isnotnull(cs_quantity#139)) AND (((cs_net_profit#154 > 1.0) AND (cs_net_paid#150 > 0.0)) AND (cs_quantity#139 > 0))) AND ((isnotnull(cs_order_number#138) AND isnotnull(cs_item_sk#136)) AND isnotnull(cs_sold_date_sk#121)))
            :                    :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#121,cs_sold_time_sk#122,cs_ship_date_sk#123,cs_bill_customer_sk#124,cs_bill_cdemo_sk#125,cs_bill_hdemo_sk#126,cs_bill_addr_sk#127,cs_ship_customer_sk#128,cs_ship_cdemo_sk#129,cs_ship_hdemo_sk#130,cs_ship_addr_sk#131,cs_call_center_sk#132,cs_catalog_page_sk#133,cs_ship_mode_sk#134,cs_warehouse_sk#135,cs_item_sk#136,cs_promo_sk#137,cs_order_number#138,cs_quantity#139,cs_wholesale_cost#140,cs_list_price#141,cs_sales_price#142,cs_ext_discount_amt#143,cs_ext_sales_price#144,... 10 more fields] parquet
            :                    :     +- Project [cr_item_sk#157, cr_order_number#171, cr_return_quantity#172, cr_return_amount#173]
            :                    :        +- Filter ((isnotnull(cr_return_amount#173) AND (cr_return_amount#173 > 10000.0)) AND (isnotnull(cr_order_number#171) AND isnotnull(cr_item_sk#157)))
            :                    :           +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#155,cr_returned_time_sk#156,cr_item_sk#157,cr_refunded_customer_sk#158,cr_refunded_cdemo_sk#159,cr_refunded_hdemo_sk#160,cr_refunded_addr_sk#161,cr_returning_customer_sk#162,cr_returning_cdemo_sk#163,cr_returning_hdemo_sk#164,cr_returning_addr_sk#165,cr_call_center_sk#166,cr_catalog_page_sk#167,cr_ship_mode_sk#168,cr_warehouse_sk#169,cr_reason_sk#170,cr_order_number#171,cr_return_quantity#172,cr_return_amount#173,cr_return_tax#174,cr_return_amt_inc_tax#175,cr_fee#176,cr_return_ship_cost#177,cr_refunded_cash#178,... 3 more fields] parquet
            :                    +- Project [d_date_sk#225]
            :                       +- Filter (((isnotnull(d_year#231) AND isnotnull(d_moy#233)) AND ((d_year#231 = 2001) AND (d_moy#233 = 12))) AND isnotnull(d_date_sk#225))
            :                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#225,d_date_id#226,d_date#227,d_month_seq#228,d_week_seq#229,d_quarter_seq#230,d_year#231,d_dow#232,d_moy#233,d_dom#234,d_qoy#235,d_fy_year#236,d_fy_quarter_seq#237,d_fy_week_seq#238,d_day_name#239,d_quarter_name#240,d_holiday#241,d_weekend#242,d_following_holiday#243,d_first_dom#244,d_last_dom#245,d_same_day_ly#246,d_same_day_lq#247,d_current_day#248,... 4 more fields] parquet
            +- Project [store AS channel#17, item#12, return_ratio#13, return_rank#15, currency_rank#16]
               +- Filter ((return_rank#15 <= 10) OR (currency_rank#16 <= 10))
                  +- Window [rank(currency_ratio#14) windowspecdefinition(currency_ratio#14 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#16], [currency_ratio#14 ASC NULLS FIRST]
                     +- Window [rank(return_ratio#13) windowspecdefinition(return_ratio#13 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#15], [return_ratio#13 ASC NULLS FIRST]
                        +- Aggregate [ss_item_sk#184], [ss_item_sk#184 AS item#12, (cast(sum(coalesce(sr_return_quantity#215, 0)) as decimal(15,4)) / cast(sum(coalesce(ss_quantity#192, 0)) as decimal(15,4))) AS return_ratio#13, (cast(sum(coalesce(sr_return_amt#216, 0.0)) as decimal(15,4)) / cast(sum(coalesce(ss_net_paid#202, 0.0)) as decimal(15,4))) AS currency_ratio#14]
                           +- Project [ss_item_sk#184, ss_quantity#192, ss_net_paid#202, sr_return_quantity#215, sr_return_amt#216]
                              +- Join Inner, (ss_sold_date_sk#182 = d_date_sk#253)
                                 :- Project [ss_sold_date_sk#182, ss_item_sk#184, ss_quantity#192, ss_net_paid#202, sr_return_quantity#215, sr_return_amt#216]
                                 :  +- Join Inner, ((ss_ticket_number#191 = sr_ticket_number#214) AND (ss_item_sk#184 = sr_item_sk#207))
                                 :     :- Project [ss_sold_date_sk#182, ss_item_sk#184, ss_ticket_number#191, ss_quantity#192, ss_net_paid#202]
                                 :     :  +- Filter ((((isnotnull(ss_net_profit#204) AND isnotnull(ss_net_paid#202)) AND isnotnull(ss_quantity#192)) AND (((ss_net_profit#204 > 1.0) AND (ss_net_paid#202 > 0.0)) AND (ss_quantity#192 > 0))) AND ((isnotnull(ss_ticket_number#191) AND isnotnull(ss_item_sk#184)) AND isnotnull(ss_sold_date_sk#182)))
                                 :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#182,ss_sold_time_sk#183,ss_item_sk#184,ss_customer_sk#185,ss_cdemo_sk#186,ss_hdemo_sk#187,ss_addr_sk#188,ss_store_sk#189,ss_promo_sk#190,ss_ticket_number#191,ss_quantity#192,ss_wholesale_cost#193,ss_list_price#194,ss_sales_price#195,ss_ext_discount_amt#196,ss_ext_sales_price#197,ss_ext_wholesale_cost#198,ss_ext_list_price#199,ss_ext_tax#200,ss_coupon_amt#201,ss_net_paid#202,ss_net_paid_inc_tax#203,ss_net_profit#204] parquet
                                 :     +- Project [sr_item_sk#207, sr_ticket_number#214, sr_return_quantity#215, sr_return_amt#216]
                                 :        +- Filter ((isnotnull(sr_return_amt#216) AND (sr_return_amt#216 > 10000.0)) AND (isnotnull(sr_ticket_number#214) AND isnotnull(sr_item_sk#207)))
                                 :           +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#205,sr_return_time_sk#206,sr_item_sk#207,sr_customer_sk#208,sr_cdemo_sk#209,sr_hdemo_sk#210,sr_addr_sk#211,sr_store_sk#212,sr_reason_sk#213,sr_ticket_number#214,sr_return_quantity#215,sr_return_amt#216,sr_return_tax#217,sr_return_amt_inc_tax#218,sr_fee#219,sr_return_ship_cost#220,sr_refunded_cash#221,sr_reversed_charge#222,sr_store_credit#223,sr_net_loss#224] parquet
                                 +- Project [d_date_sk#253]
                                    +- Filter (((isnotnull(d_year#259) AND isnotnull(d_moy#261)) AND ((d_year#259 = 2001) AND (d_moy#261 = 12))) AND isnotnull(d_date_sk#253))
                                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#253,d_date_id#254,d_date#255,d_month_seq#256,d_week_seq#257,d_quarter_seq#258,d_year#259,d_dow#260,d_moy#261,d_dom#262,d_qoy#263,d_fy_year#264,d_fy_quarter_seq#265,d_fy_week_seq#266,d_day_name#267,d_quarter_name#268,d_holiday#269,d_weekend#270,d_following_holiday#271,d_first_dom#272,d_last_dom#273,d_same_day_ly#274,d_same_day_lq#275,d_current_day#276,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[channel#5 ASC NULLS FIRST,return_rank#3 ASC NULLS FIRST,currency_rank#4 ASC NULLS FIRST], output=[channel#5,item#0,return_ratio#1,return_rank#3,currency_rank#4])
   +- HashAggregate(keys=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4], functions=[], output=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4])
      +- Exchange hashpartitioning(channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4, 200), ENSURE_REQUIREMENTS, [plan_id=306]
         +- HashAggregate(keys=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4], functions=[], output=[channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4])
            +- Union
               :- Project [web AS channel#5, item#0, return_ratio#1, return_rank#3, currency_rank#4]
               :  +- Filter ((return_rank#3 <= 10) OR (currency_rank#4 <= 10))
               :     +- Window [rank(currency_ratio#2) windowspecdefinition(currency_ratio#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#4], [currency_ratio#2 ASC NULLS FIRST]
               :        +- Sort [currency_ratio#2 ASC NULLS FIRST], false, 0
               :           +- Window [rank(return_ratio#1) windowspecdefinition(return_ratio#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#3], [return_ratio#1 ASC NULLS FIRST]
               :              +- Sort [return_ratio#1 ASC NULLS FIRST], false, 0
               :                 +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=255]
               :                    +- HashAggregate(keys=[ws_item_sk#38], functions=[sum(coalesce(wr_return_quantity#83, 0)), sum(coalesce(ws_quantity#53, 0)), sum(coalesce(wr_return_amt#84, 0.0)), sum(coalesce(ws_net_paid#64, 0.0))], output=[item#0, return_ratio#1, currency_ratio#2])
               :                       +- Exchange hashpartitioning(ws_item_sk#38, 200), ENSURE_REQUIREMENTS, [plan_id=252]
               :                          +- HashAggregate(keys=[ws_item_sk#38], functions=[partial_sum(coalesce(wr_return_quantity#83, 0)), partial_sum(coalesce(ws_quantity#53, 0)), partial_sum(coalesce(wr_return_amt#84, 0.0)), partial_sum(coalesce(ws_net_paid#64, 0.0))], output=[ws_item_sk#38, sum#372L, sum#373L, sum#374, sum#375])
               :                             +- Project [ws_item_sk#38, ws_quantity#53, ws_net_paid#64, wr_return_quantity#83, wr_return_amt#84]
               :                                +- BroadcastHashJoin [ws_sold_date_sk#35], [d_date_sk#93], Inner, BuildRight, false
               :                                   :- Project [ws_sold_date_sk#35, ws_item_sk#38, ws_quantity#53, ws_net_paid#64, wr_return_quantity#83, wr_return_amt#84]
               :                                   :  +- BroadcastHashJoin [ws_order_number#52, ws_item_sk#38], [wr_order_number#82, wr_item_sk#71], Inner, BuildRight, false
               :                                   :     :- Project [ws_sold_date_sk#35, ws_item_sk#38, ws_order_number#52, ws_quantity#53, ws_net_paid#64]
               :                                   :     :  +- Filter ((((((((isnotnull(ws_net_profit#68) AND isnotnull(ws_net_paid#64)) AND isnotnull(ws_quantity#53)) AND (ws_net_profit#68 > 1.0)) AND (ws_net_paid#64 > 0.0)) AND (ws_quantity#53 > 0)) AND isnotnull(ws_order_number#52)) AND isnotnull(ws_item_sk#38)) AND isnotnull(ws_sold_date_sk#35))
               :                                   :     :     +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#35,ws_item_sk#38,ws_order_number#52,ws_quantity#53,ws_net_paid#64,ws_net_profit#68] Batched: true, DataFilters: [isnotnull(ws_net_profit#68), isnotnull(ws_net_paid#64), isnotnull(ws_quantity#53), (ws_net_profi..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_net_profit), IsNotNull(ws_net_paid), IsNotNull(ws_quantity), GreaterThan(ws_net_pro..., ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_order_number:int,ws_quantity:int,ws_net_paid:double,...
               :                                   :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=243]
               :                                   :        +- Filter (((isnotnull(wr_return_amt#84) AND (wr_return_amt#84 > 10000.0)) AND isnotnull(wr_order_number#82)) AND isnotnull(wr_item_sk#71))
               :                                   :           +- FileScan parquet spark_catalog.tpcds.web_returns[wr_item_sk#71,wr_order_number#82,wr_return_quantity#83,wr_return_amt#84] Batched: true, DataFilters: [isnotnull(wr_return_amt#84), (wr_return_amt#84 > 10000.0), isnotnull(wr_order_number#82), isnotn..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_return_amt), GreaterThan(wr_return_amt,10000.0), IsNotNull(wr_order_number), IsNotN..., ReadSchema: struct<wr_item_sk:int,wr_order_number:int,wr_return_quantity:int,wr_return_amt:double>
               :                                   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=247]
               :                                      +- Project [d_date_sk#93]
               :                                         +- Filter ((((isnotnull(d_year#99) AND isnotnull(d_moy#101)) AND (d_year#99 = 2001)) AND (d_moy#101 = 12)) AND isnotnull(d_date_sk#93))
               :                                            +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#93,d_year#99,d_moy#101] Batched: true, DataFilters: [isnotnull(d_year#99), isnotnull(d_moy#101), (d_year#99 = 2001), (d_moy#101 = 12), isnotnull(d_da..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,12), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :- Project [catalog AS channel#11, item#6, return_ratio#7, return_rank#9, currency_rank#10]
               :  +- Filter ((return_rank#9 <= 10) OR (currency_rank#10 <= 10))
               :     +- Window [rank(currency_ratio#8) windowspecdefinition(currency_ratio#8 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#10], [currency_ratio#8 ASC NULLS FIRST]
               :        +- Sort [currency_ratio#8 ASC NULLS FIRST], false, 0
               :           +- Window [rank(return_ratio#7) windowspecdefinition(return_ratio#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#9], [return_ratio#7 ASC NULLS FIRST]
               :              +- Sort [return_ratio#7 ASC NULLS FIRST], false, 0
               :                 +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=275]
               :                    +- HashAggregate(keys=[cs_item_sk#136], functions=[sum(coalesce(cr_return_quantity#172, 0)), sum(coalesce(cs_quantity#139, 0)), sum(coalesce(cr_return_amount#173, 0.0)), sum(coalesce(cs_net_paid#150, 0.0))], output=[item#6, return_ratio#7, currency_ratio#8])
               :                       +- Exchange hashpartitioning(cs_item_sk#136, 200), ENSURE_REQUIREMENTS, [plan_id=272]
               :                          +- HashAggregate(keys=[cs_item_sk#136], functions=[partial_sum(coalesce(cr_return_quantity#172, 0)), partial_sum(coalesce(cs_quantity#139, 0)), partial_sum(coalesce(cr_return_amount#173, 0.0)), partial_sum(coalesce(cs_net_paid#150, 0.0))], output=[cs_item_sk#136, sum#380L, sum#381L, sum#382, sum#383])
               :                             +- Project [cs_item_sk#136, cs_quantity#139, cs_net_paid#150, cr_return_quantity#172, cr_return_amount#173]
               :                                +- BroadcastHashJoin [cs_sold_date_sk#121], [d_date_sk#225], Inner, BuildRight, false
               :                                   :- Project [cs_sold_date_sk#121, cs_item_sk#136, cs_quantity#139, cs_net_paid#150, cr_return_quantity#172, cr_return_amount#173]
               :                                   :  +- BroadcastHashJoin [cs_order_number#138, cs_item_sk#136], [cr_order_number#171, cr_item_sk#157], Inner, BuildRight, false
               :                                   :     :- Project [cs_sold_date_sk#121, cs_item_sk#136, cs_order_number#138, cs_quantity#139, cs_net_paid#150]
               :                                   :     :  +- Filter ((((((((isnotnull(cs_net_profit#154) AND isnotnull(cs_net_paid#150)) AND isnotnull(cs_quantity#139)) AND (cs_net_profit#154 > 1.0)) AND (cs_net_paid#150 > 0.0)) AND (cs_quantity#139 > 0)) AND isnotnull(cs_order_number#138)) AND isnotnull(cs_item_sk#136)) AND isnotnull(cs_sold_date_sk#121))
               :                                   :     :     +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#121,cs_item_sk#136,cs_order_number#138,cs_quantity#139,cs_net_paid#150,cs_net_profit#154] Batched: true, DataFilters: [isnotnull(cs_net_profit#154), isnotnull(cs_net_paid#150), isnotnull(cs_quantity#139), (cs_net_pr..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_net_profit), IsNotNull(cs_net_paid), IsNotNull(cs_quantity), GreaterThan(cs_net_pro..., ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_order_number:int,cs_quantity:int,cs_net_paid:double,...
               :                                   :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=263]
               :                                   :        +- Filter (((isnotnull(cr_return_amount#173) AND (cr_return_amount#173 > 10000.0)) AND isnotnull(cr_order_number#171)) AND isnotnull(cr_item_sk#157))
               :                                   :           +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_item_sk#157,cr_order_number#171,cr_return_quantity#172,cr_return_amount#173] Batched: true, DataFilters: [isnotnull(cr_return_amount#173), (cr_return_amount#173 > 10000.0), isnotnull(cr_order_number#171..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_return_amount), GreaterThan(cr_return_amount,10000.0), IsNotNull(cr_order_number), ..., ReadSchema: struct<cr_item_sk:int,cr_order_number:int,cr_return_quantity:int,cr_return_amount:double>
               :                                   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=267]
               :                                      +- Project [d_date_sk#225]
               :                                         +- Filter ((((isnotnull(d_year#231) AND isnotnull(d_moy#233)) AND (d_year#231 = 2001)) AND (d_moy#233 = 12)) AND isnotnull(d_date_sk#225))
               :                                            +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#225,d_year#231,d_moy#233] Batched: true, DataFilters: [isnotnull(d_year#231), isnotnull(d_moy#233), (d_year#231 = 2001), (d_moy#233 = 12), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,12), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               +- Project [store AS channel#17, item#12, return_ratio#13, return_rank#15, currency_rank#16]
                  +- Filter ((return_rank#15 <= 10) OR (currency_rank#16 <= 10))
                     +- Window [rank(currency_ratio#14) windowspecdefinition(currency_ratio#14 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS currency_rank#16], [currency_ratio#14 ASC NULLS FIRST]
                        +- Sort [currency_ratio#14 ASC NULLS FIRST], false, 0
                           +- Window [rank(return_ratio#13) windowspecdefinition(return_ratio#13 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS return_rank#15], [return_ratio#13 ASC NULLS FIRST]
                              +- Sort [return_ratio#13 ASC NULLS FIRST], false, 0
                                 +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=295]
                                    +- HashAggregate(keys=[ss_item_sk#184], functions=[sum(coalesce(sr_return_quantity#215, 0)), sum(coalesce(ss_quantity#192, 0)), sum(coalesce(sr_return_amt#216, 0.0)), sum(coalesce(ss_net_paid#202, 0.0))], output=[item#12, return_ratio#13, currency_ratio#14])
                                       +- Exchange hashpartitioning(ss_item_sk#184, 200), ENSURE_REQUIREMENTS, [plan_id=292]
                                          +- HashAggregate(keys=[ss_item_sk#184], functions=[partial_sum(coalesce(sr_return_quantity#215, 0)), partial_sum(coalesce(ss_quantity#192, 0)), partial_sum(coalesce(sr_return_amt#216, 0.0)), partial_sum(coalesce(ss_net_paid#202, 0.0))], output=[ss_item_sk#184, sum#388L, sum#389L, sum#390, sum#391])
                                             +- Project [ss_item_sk#184, ss_quantity#192, ss_net_paid#202, sr_return_quantity#215, sr_return_amt#216]
                                                +- BroadcastHashJoin [ss_sold_date_sk#182], [d_date_sk#253], Inner, BuildRight, false
                                                   :- Project [ss_sold_date_sk#182, ss_item_sk#184, ss_quantity#192, ss_net_paid#202, sr_return_quantity#215, sr_return_amt#216]
                                                   :  +- BroadcastHashJoin [ss_ticket_number#191, ss_item_sk#184], [sr_ticket_number#214, sr_item_sk#207], Inner, BuildRight, false
                                                   :     :- Project [ss_sold_date_sk#182, ss_item_sk#184, ss_ticket_number#191, ss_quantity#192, ss_net_paid#202]
                                                   :     :  +- Filter ((((((((isnotnull(ss_net_profit#204) AND isnotnull(ss_net_paid#202)) AND isnotnull(ss_quantity#192)) AND (ss_net_profit#204 > 1.0)) AND (ss_net_paid#202 > 0.0)) AND (ss_quantity#192 > 0)) AND isnotnull(ss_ticket_number#191)) AND isnotnull(ss_item_sk#184)) AND isnotnull(ss_sold_date_sk#182))
                                                   :     :     +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#182,ss_item_sk#184,ss_ticket_number#191,ss_quantity#192,ss_net_paid#202,ss_net_profit#204] Batched: true, DataFilters: [isnotnull(ss_net_profit#204), isnotnull(ss_net_paid#202), isnotnull(ss_quantity#192), (ss_net_pr..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_net_profit), IsNotNull(ss_net_paid), IsNotNull(ss_quantity), GreaterThan(ss_net_pro..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ticket_number:int,ss_quantity:int,ss_net_paid:double...
                                                   :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=283]
                                                   :        +- Filter (((isnotnull(sr_return_amt#216) AND (sr_return_amt#216 > 10000.0)) AND isnotnull(sr_ticket_number#214)) AND isnotnull(sr_item_sk#207))
                                                   :           +- FileScan parquet spark_catalog.tpcds.store_returns[sr_item_sk#207,sr_ticket_number#214,sr_return_quantity#215,sr_return_amt#216] Batched: true, DataFilters: [isnotnull(sr_return_amt#216), (sr_return_amt#216 > 10000.0), isnotnull(sr_ticket_number#214), is..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_return_amt), GreaterThan(sr_return_amt,10000.0), IsNotNull(sr_ticket_number), IsNot..., ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int,sr_return_quantity:int,sr_return_amt:double>
                                                   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=287]
                                                      +- Project [d_date_sk#253]
                                                         +- Filter ((((isnotnull(d_year#259) AND isnotnull(d_moy#261)) AND (d_year#259 = 2001)) AND (d_moy#261 = 12)) AND isnotnull(d_date_sk#253))
                                                            +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#253,d_year#259,d_moy#261] Batched: true, DataFilters: [isnotnull(d_year#259), isnotnull(d_moy#261), (d_year#259 = 2001), (d_moy#261 = 12), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,12), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>

Time taken: 4.052 seconds, Fetched 1 row(s)
