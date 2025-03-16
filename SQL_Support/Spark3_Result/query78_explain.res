Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582563397
== Parsed Logical Plan ==
CTE [ws, cs, ss]
:  :- 'SubqueryAlias ws
:  :  +- 'Aggregate ['d_year, 'ws_item_sk, 'ws_bill_customer_sk], ['d_year AS ws_sold_year#7, 'ws_item_sk, 'ws_bill_customer_sk AS ws_customer_sk#8, 'sum('ws_quantity) AS ws_qty#9, 'sum('ws_wholesale_cost) AS ws_wc#10, 'sum('ws_sales_price) AS ws_sp#11]
:  :     +- 'Filter isnull('wr_order_number)
:  :        +- 'Join Inner, ('ws_sold_date_sk = 'd_date_sk)
:  :           :- 'Join LeftOuter, (('wr_order_number = 'ws_order_number) AND ('ws_item_sk = 'wr_item_sk))
:  :           :  :- 'UnresolvedRelation [web_sales], [], false
:  :           :  +- 'UnresolvedRelation [web_returns], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  :- 'SubqueryAlias cs
:  :  +- 'Aggregate ['d_year, 'cs_item_sk, 'cs_bill_customer_sk], ['d_year AS cs_sold_year#12, 'cs_item_sk, 'cs_bill_customer_sk AS cs_customer_sk#13, 'sum('cs_quantity) AS cs_qty#14, 'sum('cs_wholesale_cost) AS cs_wc#15, 'sum('cs_sales_price) AS cs_sp#16]
:  :     +- 'Filter isnull('cr_order_number)
:  :        +- 'Join Inner, ('cs_sold_date_sk = 'd_date_sk)
:  :           :- 'Join LeftOuter, (('cr_order_number = 'cs_order_number) AND ('cs_item_sk = 'cr_item_sk))
:  :           :  :- 'UnresolvedRelation [catalog_sales], [], false
:  :           :  +- 'UnresolvedRelation [catalog_returns], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias ss
:     +- 'Aggregate ['d_year, 'ss_item_sk, 'ss_customer_sk], ['d_year AS ss_sold_year#17, 'ss_item_sk, 'ss_customer_sk, 'sum('ss_quantity) AS ss_qty#18, 'sum('ss_wholesale_cost) AS ss_wc#19, 'sum('ss_sales_price) AS ss_sp#20]
:        +- 'Filter isnull('sr_ticket_number)
:           +- 'Join Inner, ('ss_sold_date_sk = 'd_date_sk)
:              :- 'Join LeftOuter, (('sr_ticket_number = 'ss_ticket_number) AND ('ss_item_sk = 'sr_item_sk))
:              :  :- 'UnresolvedRelation [store_sales], [], false
:              :  +- 'UnresolvedRelation [store_returns], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['ss_sold_year ASC NULLS FIRST, 'ss_item_sk ASC NULLS FIRST, 'ss_customer_sk ASC NULLS FIRST, 'ss_qty DESC NULLS LAST, 'ss_wc DESC NULLS LAST, 'ss_sp DESC NULLS LAST, 'other_chan_qty ASC NULLS FIRST, 'other_chan_wholesale_cost ASC NULLS FIRST, 'other_chan_sales_price ASC NULLS FIRST, 'round(('ss_qty / 'coalesce(('ws_qty + 'cs_qty), 1)), 2) ASC NULLS FIRST], true
         +- 'Project ['ss_sold_year, 'ss_item_sk, 'ss_customer_sk, 'round(('ss_qty / ('coalesce('ws_qty, 0) + 'coalesce('cs_qty, 0))), 2) AS ratio#0, 'ss_qty AS store_qty#1, 'ss_wc AS store_wholesale_cost#2, 'ss_sp AS store_sales_price#3, ('coalesce('ws_qty, 0) + 'coalesce('cs_qty, 0)) AS other_chan_qty#4, ('coalesce('ws_wc, 0) + 'coalesce('cs_wc, 0)) AS other_chan_wholesale_cost#5, ('coalesce('ws_sp, 0) + 'coalesce('cs_sp, 0)) AS other_chan_sales_price#6]
            +- 'Filter ((('coalesce('ws_qty, 0) > 0) OR ('coalesce('cs_qty, 0) > 0)) AND ('ss_sold_year = 2000))
               +- 'Join LeftOuter, ((('cs_sold_year = 'ss_sold_year) AND ('cs_item_sk = 'ss_item_sk)) AND ('cs_customer_sk = 'ss_customer_sk))
                  :- 'Join LeftOuter, ((('ws_sold_year = 'ss_sold_year) AND ('ws_item_sk = 'ss_item_sk)) AND ('ws_customer_sk = 'ss_customer_sk))
                  :  :- 'UnresolvedRelation [ss], [], false
                  :  +- 'UnresolvedRelation [ws], [], false
                  +- 'UnresolvedRelation [cs], [], false

== Analyzed Logical Plan ==
ss_sold_year: int, ss_item_sk: int, ss_customer_sk: int, ratio: double, store_qty: bigint, store_wholesale_cost: double, store_sales_price: double, other_chan_qty: bigint, other_chan_wholesale_cost: double, other_chan_sales_price: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ws
:     +- Aggregate [d_year#90, ws_item_sk#29, ws_bill_customer_sk#30], [d_year#90 AS ws_sold_year#7, ws_item_sk#29, ws_bill_customer_sk#30 AS ws_customer_sk#8, sum(ws_quantity#44) AS ws_qty#9L, sum(ws_wholesale_cost#45) AS ws_wc#10, sum(ws_sales_price#47) AS ws_sp#11]
:        +- Filter isnull(wr_order_number#73)
:           +- Join Inner, (ws_sold_date_sk#26 = d_date_sk#84)
:              :- Join LeftOuter, ((wr_order_number#73 = ws_order_number#43) AND (ws_item_sk#29 = wr_item_sk#62))
:              :  :- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#26,ws_sold_time_sk#27,ws_ship_date_sk#28,ws_item_sk#29,ws_bill_customer_sk#30,ws_bill_cdemo_sk#31,ws_bill_hdemo_sk#32,ws_bill_addr_sk#33,ws_ship_customer_sk#34,ws_ship_cdemo_sk#35,ws_ship_hdemo_sk#36,ws_ship_addr_sk#37,ws_web_page_sk#38,ws_web_site_sk#39,ws_ship_mode_sk#40,ws_warehouse_sk#41,ws_promo_sk#42,ws_order_number#43,ws_quantity#44,ws_wholesale_cost#45,ws_list_price#46,ws_sales_price#47,ws_ext_discount_amt#48,ws_ext_sales_price#49,... 10 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.web_returns
:              :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#60,wr_returned_time_sk#61,wr_item_sk#62,wr_refunded_customer_sk#63,wr_refunded_cdemo_sk#64,wr_refunded_hdemo_sk#65,wr_refunded_addr_sk#66,wr_returning_customer_sk#67,wr_returning_cdemo_sk#68,wr_returning_hdemo_sk#69,wr_returning_addr_sk#70,wr_web_page_sk#71,wr_reason_sk#72,wr_order_number#73,wr_return_quantity#74,wr_return_amt#75,wr_return_tax#76,wr_return_amt_inc_tax#77,wr_fee#78,wr_return_ship_cost#79,wr_refunded_cash#80,wr_reversed_charge#81,wr_account_credit#82,wr_net_loss#83] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#84,d_date_id#85,d_date#86,d_month_seq#87,d_week_seq#88,d_quarter_seq#89,d_year#90,d_dow#91,d_moy#92,d_dom#93,d_qoy#94,d_fy_year#95,d_fy_quarter_seq#96,d_fy_week_seq#97,d_day_name#98,d_quarter_name#99,d_holiday#100,d_weekend#101,d_following_holiday#102,d_first_dom#103,d_last_dom#104,d_same_day_ly#105,d_same_day_lq#106,d_current_day#107,... 4 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias cs
:     +- Aggregate [d_year#222, cs_item_sk#127, cs_bill_customer_sk#115], [d_year#222 AS cs_sold_year#12, cs_item_sk#127, cs_bill_customer_sk#115 AS cs_customer_sk#13, sum(cs_quantity#130) AS cs_qty#14L, sum(cs_wholesale_cost#131) AS cs_wc#15, sum(cs_sales_price#133) AS cs_sp#16]
:        +- Filter isnull(cr_order_number#162)
:           +- Join Inner, (cs_sold_date_sk#112 = d_date_sk#216)
:              :- Join LeftOuter, ((cr_order_number#162 = cs_order_number#129) AND (cs_item_sk#127 = cr_item_sk#148))
:              :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#112,cs_sold_time_sk#113,cs_ship_date_sk#114,cs_bill_customer_sk#115,cs_bill_cdemo_sk#116,cs_bill_hdemo_sk#117,cs_bill_addr_sk#118,cs_ship_customer_sk#119,cs_ship_cdemo_sk#120,cs_ship_hdemo_sk#121,cs_ship_addr_sk#122,cs_call_center_sk#123,cs_catalog_page_sk#124,cs_ship_mode_sk#125,cs_warehouse_sk#126,cs_item_sk#127,cs_promo_sk#128,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_list_price#132,cs_sales_price#133,cs_ext_discount_amt#134,cs_ext_sales_price#135,... 10 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.catalog_returns
:              :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#146,cr_returned_time_sk#147,cr_item_sk#148,cr_refunded_customer_sk#149,cr_refunded_cdemo_sk#150,cr_refunded_hdemo_sk#151,cr_refunded_addr_sk#152,cr_returning_customer_sk#153,cr_returning_cdemo_sk#154,cr_returning_hdemo_sk#155,cr_returning_addr_sk#156,cr_call_center_sk#157,cr_catalog_page_sk#158,cr_ship_mode_sk#159,cr_warehouse_sk#160,cr_reason_sk#161,cr_order_number#162,cr_return_quantity#163,cr_return_amount#164,cr_return_tax#165,cr_return_amt_inc_tax#166,cr_fee#167,cr_return_ship_cost#168,cr_refunded_cash#169,... 3 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#216,d_date_id#217,d_date#218,d_month_seq#219,d_week_seq#220,d_quarter_seq#221,d_year#222,d_dow#223,d_moy#224,d_dom#225,d_qoy#226,d_fy_year#227,d_fy_quarter_seq#228,d_fy_week_seq#229,d_day_name#230,d_quarter_name#231,d_holiday#232,d_weekend#233,d_following_holiday#234,d_first_dom#235,d_last_dom#236,d_same_day_ly#237,d_same_day_lq#238,d_current_day#239,... 4 more fields] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias ss
:     +- Aggregate [d_year#250, ss_item_sk#175, ss_customer_sk#176], [d_year#250 AS ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, sum(ss_quantity#183) AS ss_qty#18L, sum(ss_wholesale_cost#184) AS ss_wc#19, sum(ss_sales_price#186) AS ss_sp#20]
:        +- Filter isnull(sr_ticket_number#205)
:           +- Join Inner, (ss_sold_date_sk#173 = d_date_sk#244)
:              :- Join LeftOuter, ((sr_ticket_number#205 = ss_ticket_number#182) AND (ss_item_sk#175 = sr_item_sk#198))
:              :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#173,ss_sold_time_sk#174,ss_item_sk#175,ss_customer_sk#176,ss_cdemo_sk#177,ss_hdemo_sk#178,ss_addr_sk#179,ss_store_sk#180,ss_promo_sk#181,ss_ticket_number#182,ss_quantity#183,ss_wholesale_cost#184,ss_list_price#185,ss_sales_price#186,ss_ext_discount_amt#187,ss_ext_sales_price#188,ss_ext_wholesale_cost#189,ss_ext_list_price#190,ss_ext_tax#191,ss_coupon_amt#192,ss_net_paid#193,ss_net_paid_inc_tax#194,ss_net_profit#195] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.store_returns
:              :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#196,sr_return_time_sk#197,sr_item_sk#198,sr_customer_sk#199,sr_cdemo_sk#200,sr_hdemo_sk#201,sr_addr_sk#202,sr_store_sk#203,sr_reason_sk#204,sr_ticket_number#205,sr_return_quantity#206,sr_return_amt#207,sr_return_tax#208,sr_return_amt_inc_tax#209,sr_fee#210,sr_return_ship_cost#211,sr_refunded_cash#212,sr_reversed_charge#213,sr_store_credit#214,sr_net_loss#215] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#244,d_date_id#245,d_date#246,d_month_seq#247,d_week_seq#248,d_quarter_seq#249,d_year#250,d_dow#251,d_moy#252,d_dom#253,d_qoy#254,d_fy_year#255,d_fy_quarter_seq#256,d_fy_week_seq#257,d_day_name#258,d_quarter_name#259,d_holiday#260,d_weekend#261,d_following_holiday#262,d_first_dom#263,d_last_dom#264,d_same_day_ly#265,d_same_day_lq#266,d_current_day#267,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Project [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, ratio#0, store_qty#1L, store_wholesale_cost#2, store_sales_price#3, other_chan_qty#4L, other_chan_wholesale_cost#5, other_chan_sales_price#6]
         +- Sort [ss_sold_year#17 ASC NULLS FIRST, ss_item_sk#175 ASC NULLS FIRST, ss_customer_sk#176 ASC NULLS FIRST, ss_qty#18L DESC NULLS LAST, ss_wc#19 DESC NULLS LAST, ss_sp#20 DESC NULLS LAST, other_chan_qty#4L ASC NULLS FIRST, other_chan_wholesale_cost#5 ASC NULLS FIRST, other_chan_sales_price#6 ASC NULLS FIRST, round((cast(ss_qty#18L as double) / cast(coalesce((ws_qty#9L + cs_qty#14L), cast(1 as bigint)) as double)), 2) ASC NULLS FIRST], true
            +- Project [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, round((cast(ss_qty#18L as double) / cast((coalesce(ws_qty#9L, cast(0 as bigint)) + coalesce(cs_qty#14L, cast(0 as bigint))) as double)), 2) AS ratio#0, ss_qty#18L AS store_qty#1L, ss_wc#19 AS store_wholesale_cost#2, ss_sp#20 AS store_sales_price#3, (coalesce(ws_qty#9L, cast(0 as bigint)) + coalesce(cs_qty#14L, cast(0 as bigint))) AS other_chan_qty#4L, (coalesce(ws_wc#10, cast(0 as double)) + coalesce(cs_wc#15, cast(0 as double))) AS other_chan_wholesale_cost#5, (coalesce(ws_sp#11, cast(0 as double)) + coalesce(cs_sp#16, cast(0 as double))) AS other_chan_sales_price#6, ss_qty#18L, ss_wc#19, ss_sp#20, ws_qty#9L, cs_qty#14L]
               +- Filter (((coalesce(ws_qty#9L, cast(0 as bigint)) > cast(0 as bigint)) OR (coalesce(cs_qty#14L, cast(0 as bigint)) > cast(0 as bigint))) AND (ss_sold_year#17 = 2000))
                  +- Join LeftOuter, (((cs_sold_year#12 = ss_sold_year#17) AND (cs_item_sk#127 = ss_item_sk#175)) AND (cs_customer_sk#13 = ss_customer_sk#176))
                     :- Join LeftOuter, (((ws_sold_year#7 = ss_sold_year#17) AND (ws_item_sk#29 = ss_item_sk#175)) AND (ws_customer_sk#8 = ss_customer_sk#176))
                     :  :- SubqueryAlias ss
                     :  :  +- CTERelationRef 2, true, [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, ss_qty#18L, ss_wc#19, ss_sp#20], false
                     :  +- SubqueryAlias ws
                     :     +- CTERelationRef 0, true, [ws_sold_year#7, ws_item_sk#29, ws_customer_sk#8, ws_qty#9L, ws_wc#10, ws_sp#11], false
                     +- SubqueryAlias cs
                        +- CTERelationRef 1, true, [cs_sold_year#12, cs_item_sk#127, cs_customer_sk#13, cs_qty#14L, cs_wc#15, cs_sp#16], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Project [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, ratio#0, store_qty#1L, store_wholesale_cost#2, store_sales_price#3, other_chan_qty#4L, other_chan_wholesale_cost#5, other_chan_sales_price#6]
      +- Sort [ss_sold_year#17 ASC NULLS FIRST, ss_item_sk#175 ASC NULLS FIRST, ss_customer_sk#176 ASC NULLS FIRST, ss_qty#18L DESC NULLS LAST, ss_wc#19 DESC NULLS LAST, ss_sp#20 DESC NULLS LAST, other_chan_qty#4L ASC NULLS FIRST, other_chan_wholesale_cost#5 ASC NULLS FIRST, other_chan_sales_price#6 ASC NULLS FIRST, round((cast(ss_qty#18L as double) / cast(coalesce((ws_qty#9L + cs_qty#14L), 1) as double)), 2) ASC NULLS FIRST], true
         +- Project [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, round((cast(ss_qty#18L as double) / cast((coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) as double)), 2) AS ratio#0, ss_qty#18L AS store_qty#1L, ss_wc#19 AS store_wholesale_cost#2, ss_sp#20 AS store_sales_price#3, (coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) AS other_chan_qty#4L, (coalesce(ws_wc#10, 0.0) + coalesce(cs_wc#15, 0.0)) AS other_chan_wholesale_cost#5, (coalesce(ws_sp#11, 0.0) + coalesce(cs_sp#16, 0.0)) AS other_chan_sales_price#6, ss_qty#18L, ss_wc#19, ss_sp#20, ws_qty#9L, cs_qty#14L]
            +- Filter ((coalesce(ws_qty#9L, 0) > 0) OR (coalesce(cs_qty#14L, 0) > 0))
               +- Join LeftOuter, (((cs_sold_year#12 = ss_sold_year#17) AND (cs_item_sk#127 = ss_item_sk#175)) AND (cs_customer_sk#13 = ss_customer_sk#176))
                  :- Project [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, ss_qty#18L, ss_wc#19, ss_sp#20, ws_qty#9L, ws_wc#10, ws_sp#11]
                  :  +- Join LeftOuter, (((ws_sold_year#7 = ss_sold_year#17) AND (ws_item_sk#29 = ss_item_sk#175)) AND (ws_customer_sk#8 = ss_customer_sk#176))
                  :     :- Aggregate [d_year#250, ss_item_sk#175, ss_customer_sk#176], [d_year#250 AS ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, sum(ss_quantity#183) AS ss_qty#18L, sum(ss_wholesale_cost#184) AS ss_wc#19, sum(ss_sales_price#186) AS ss_sp#20]
                  :     :  +- Project [ss_item_sk#175, ss_customer_sk#176, ss_quantity#183, ss_wholesale_cost#184, ss_sales_price#186, d_year#250]
                  :     :     +- Join Inner, (ss_sold_date_sk#173 = d_date_sk#244)
                  :     :        :- Project [ss_sold_date_sk#173, ss_item_sk#175, ss_customer_sk#176, ss_quantity#183, ss_wholesale_cost#184, ss_sales_price#186]
                  :     :        :  +- Filter isnull(sr_ticket_number#205)
                  :     :        :     +- Join LeftOuter, ((sr_ticket_number#205 = ss_ticket_number#182) AND (ss_item_sk#175 = sr_item_sk#198))
                  :     :        :        :- Project [ss_sold_date_sk#173, ss_item_sk#175, ss_customer_sk#176, ss_ticket_number#182, ss_quantity#183, ss_wholesale_cost#184, ss_sales_price#186]
                  :     :        :        :  +- Filter isnotnull(ss_sold_date_sk#173)
                  :     :        :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#173,ss_sold_time_sk#174,ss_item_sk#175,ss_customer_sk#176,ss_cdemo_sk#177,ss_hdemo_sk#178,ss_addr_sk#179,ss_store_sk#180,ss_promo_sk#181,ss_ticket_number#182,ss_quantity#183,ss_wholesale_cost#184,ss_list_price#185,ss_sales_price#186,ss_ext_discount_amt#187,ss_ext_sales_price#188,ss_ext_wholesale_cost#189,ss_ext_list_price#190,ss_ext_tax#191,ss_coupon_amt#192,ss_net_paid#193,ss_net_paid_inc_tax#194,ss_net_profit#195] parquet
                  :     :        :        +- Project [sr_item_sk#198, sr_ticket_number#205]
                  :     :        :           +- Filter (isnotnull(sr_ticket_number#205) AND isnotnull(sr_item_sk#198))
                  :     :        :              +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#196,sr_return_time_sk#197,sr_item_sk#198,sr_customer_sk#199,sr_cdemo_sk#200,sr_hdemo_sk#201,sr_addr_sk#202,sr_store_sk#203,sr_reason_sk#204,sr_ticket_number#205,sr_return_quantity#206,sr_return_amt#207,sr_return_tax#208,sr_return_amt_inc_tax#209,sr_fee#210,sr_return_ship_cost#211,sr_refunded_cash#212,sr_reversed_charge#213,sr_store_credit#214,sr_net_loss#215] parquet
                  :     :        +- Project [d_date_sk#244, d_year#250]
                  :     :           +- Filter ((isnotnull(d_year#250) AND (d_year#250 = 2000)) AND isnotnull(d_date_sk#244))
                  :     :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#244,d_date_id#245,d_date#246,d_month_seq#247,d_week_seq#248,d_quarter_seq#249,d_year#250,d_dow#251,d_moy#252,d_dom#253,d_qoy#254,d_fy_year#255,d_fy_quarter_seq#256,d_fy_week_seq#257,d_day_name#258,d_quarter_name#259,d_holiday#260,d_weekend#261,d_following_holiday#262,d_first_dom#263,d_last_dom#264,d_same_day_ly#265,d_same_day_lq#266,d_current_day#267,... 4 more fields] parquet
                  :     +- Aggregate [d_year#90, ws_item_sk#29, ws_bill_customer_sk#30], [d_year#90 AS ws_sold_year#7, ws_item_sk#29, ws_bill_customer_sk#30 AS ws_customer_sk#8, sum(ws_quantity#44) AS ws_qty#9L, sum(ws_wholesale_cost#45) AS ws_wc#10, sum(ws_sales_price#47) AS ws_sp#11]
                  :        +- Project [ws_item_sk#29, ws_bill_customer_sk#30, ws_quantity#44, ws_wholesale_cost#45, ws_sales_price#47, d_year#90]
                  :           +- Join Inner, (ws_sold_date_sk#26 = d_date_sk#84)
                  :              :- Project [ws_sold_date_sk#26, ws_item_sk#29, ws_bill_customer_sk#30, ws_quantity#44, ws_wholesale_cost#45, ws_sales_price#47]
                  :              :  +- Filter isnull(wr_order_number#73)
                  :              :     +- Join LeftOuter, ((wr_order_number#73 = ws_order_number#43) AND (ws_item_sk#29 = wr_item_sk#62))
                  :              :        :- Project [ws_sold_date_sk#26, ws_item_sk#29, ws_bill_customer_sk#30, ws_order_number#43, ws_quantity#44, ws_wholesale_cost#45, ws_sales_price#47]
                  :              :        :  +- Filter ((isnotnull(ws_sold_date_sk#26) AND isnotnull(ws_item_sk#29)) AND isnotnull(ws_bill_customer_sk#30))
                  :              :        :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#26,ws_sold_time_sk#27,ws_ship_date_sk#28,ws_item_sk#29,ws_bill_customer_sk#30,ws_bill_cdemo_sk#31,ws_bill_hdemo_sk#32,ws_bill_addr_sk#33,ws_ship_customer_sk#34,ws_ship_cdemo_sk#35,ws_ship_hdemo_sk#36,ws_ship_addr_sk#37,ws_web_page_sk#38,ws_web_site_sk#39,ws_ship_mode_sk#40,ws_warehouse_sk#41,ws_promo_sk#42,ws_order_number#43,ws_quantity#44,ws_wholesale_cost#45,ws_list_price#46,ws_sales_price#47,ws_ext_discount_amt#48,ws_ext_sales_price#49,... 10 more fields] parquet
                  :              :        +- Project [wr_item_sk#62, wr_order_number#73]
                  :              :           +- Filter (isnotnull(wr_order_number#73) AND isnotnull(wr_item_sk#62))
                  :              :              +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#60,wr_returned_time_sk#61,wr_item_sk#62,wr_refunded_customer_sk#63,wr_refunded_cdemo_sk#64,wr_refunded_hdemo_sk#65,wr_refunded_addr_sk#66,wr_returning_customer_sk#67,wr_returning_cdemo_sk#68,wr_returning_hdemo_sk#69,wr_returning_addr_sk#70,wr_web_page_sk#71,wr_reason_sk#72,wr_order_number#73,wr_return_quantity#74,wr_return_amt#75,wr_return_tax#76,wr_return_amt_inc_tax#77,wr_fee#78,wr_return_ship_cost#79,wr_refunded_cash#80,wr_reversed_charge#81,wr_account_credit#82,wr_net_loss#83] parquet
                  :              +- Project [d_date_sk#84, d_year#90]
                  :                 +- Filter (isnotnull(d_date_sk#84) AND ((d_year#90 = 2000) AND isnotnull(d_year#90)))
                  :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#84,d_date_id#85,d_date#86,d_month_seq#87,d_week_seq#88,d_quarter_seq#89,d_year#90,d_dow#91,d_moy#92,d_dom#93,d_qoy#94,d_fy_year#95,d_fy_quarter_seq#96,d_fy_week_seq#97,d_day_name#98,d_quarter_name#99,d_holiday#100,d_weekend#101,d_following_holiday#102,d_first_dom#103,d_last_dom#104,d_same_day_ly#105,d_same_day_lq#106,d_current_day#107,... 4 more fields] parquet
                  +- Aggregate [d_year#222, cs_item_sk#127, cs_bill_customer_sk#115], [d_year#222 AS cs_sold_year#12, cs_item_sk#127, cs_bill_customer_sk#115 AS cs_customer_sk#13, sum(cs_quantity#130) AS cs_qty#14L, sum(cs_wholesale_cost#131) AS cs_wc#15, sum(cs_sales_price#133) AS cs_sp#16]
                     +- Project [cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133, d_year#222]
                        +- Join Inner, (cs_sold_date_sk#112 = d_date_sk#216)
                           :- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133]
                           :  +- Filter isnull(cr_order_number#162)
                           :     +- Join LeftOuter, ((cr_order_number#162 = cs_order_number#129) AND (cs_item_sk#127 = cr_item_sk#148))
                           :        :- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_order_number#129, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133]
                           :        :  +- Filter ((isnotnull(cs_sold_date_sk#112) AND isnotnull(cs_item_sk#127)) AND isnotnull(cs_bill_customer_sk#115))
                           :        :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#112,cs_sold_time_sk#113,cs_ship_date_sk#114,cs_bill_customer_sk#115,cs_bill_cdemo_sk#116,cs_bill_hdemo_sk#117,cs_bill_addr_sk#118,cs_ship_customer_sk#119,cs_ship_cdemo_sk#120,cs_ship_hdemo_sk#121,cs_ship_addr_sk#122,cs_call_center_sk#123,cs_catalog_page_sk#124,cs_ship_mode_sk#125,cs_warehouse_sk#126,cs_item_sk#127,cs_promo_sk#128,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_list_price#132,cs_sales_price#133,cs_ext_discount_amt#134,cs_ext_sales_price#135,... 10 more fields] parquet
                           :        +- Project [cr_item_sk#148, cr_order_number#162]
                           :           +- Filter (isnotnull(cr_order_number#162) AND isnotnull(cr_item_sk#148))
                           :              +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#146,cr_returned_time_sk#147,cr_item_sk#148,cr_refunded_customer_sk#149,cr_refunded_cdemo_sk#150,cr_refunded_hdemo_sk#151,cr_refunded_addr_sk#152,cr_returning_customer_sk#153,cr_returning_cdemo_sk#154,cr_returning_hdemo_sk#155,cr_returning_addr_sk#156,cr_call_center_sk#157,cr_catalog_page_sk#158,cr_ship_mode_sk#159,cr_warehouse_sk#160,cr_reason_sk#161,cr_order_number#162,cr_return_quantity#163,cr_return_amount#164,cr_return_tax#165,cr_return_amt_inc_tax#166,cr_fee#167,cr_return_ship_cost#168,cr_refunded_cash#169,... 3 more fields] parquet
                           +- Project [d_date_sk#216, d_year#222]
                              +- Filter (isnotnull(d_date_sk#216) AND ((d_year#222 = 2000) AND isnotnull(d_year#222)))
                                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#216,d_date_id#217,d_date#218,d_month_seq#219,d_week_seq#220,d_quarter_seq#221,d_year#222,d_dow#223,d_moy#224,d_dom#225,d_qoy#226,d_fy_year#227,d_fy_quarter_seq#228,d_fy_week_seq#229,d_day_name#230,d_quarter_name#231,d_holiday#232,d_weekend#233,d_following_holiday#234,d_first_dom#235,d_last_dom#236,d_same_day_ly#237,d_same_day_lq#238,d_current_day#239,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[ss_sold_year#17 ASC NULLS FIRST,ss_item_sk#175 ASC NULLS FIRST,ss_customer_sk#176 ASC NULLS FIRST,ss_qty#18L DESC NULLS LAST,ss_wc#19 DESC NULLS LAST,ss_sp#20 DESC NULLS LAST,other_chan_qty#4L ASC NULLS FIRST,other_chan_wholesale_cost#5 ASC NULLS FIRST,other_chan_sales_price#6 ASC NULLS FIRST,round((cast(ss_qty#18L as double) / cast(coalesce((ws_qty#9L + cs_qty#14L), 1) as double)), 2) ASC NULLS FIRST], output=[ss_sold_year#17,ss_item_sk#175,ss_customer_sk#176,ratio#0,store_qty#1L,store_wholesale_cost#2,store_sales_price#3,other_chan_qty#4L,other_chan_wholesale_cost#5,other_chan_sales_price#6])
   +- Project [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, round((cast(ss_qty#18L as double) / cast((coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) as double)), 2) AS ratio#0, ss_qty#18L AS store_qty#1L, ss_wc#19 AS store_wholesale_cost#2, ss_sp#20 AS store_sales_price#3, (coalesce(ws_qty#9L, 0) + coalesce(cs_qty#14L, 0)) AS other_chan_qty#4L, (coalesce(ws_wc#10, 0.0) + coalesce(cs_wc#15, 0.0)) AS other_chan_wholesale_cost#5, (coalesce(ws_sp#11, 0.0) + coalesce(cs_sp#16, 0.0)) AS other_chan_sales_price#6, ss_qty#18L, ss_wc#19, ss_sp#20, ws_qty#9L, cs_qty#14L]
      +- Filter ((coalesce(ws_qty#9L, 0) > 0) OR (coalesce(cs_qty#14L, 0) > 0))
         +- SortMergeJoin [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176], [cs_sold_year#12, cs_item_sk#127, cs_customer_sk#13], LeftOuter
            :- Project [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, ss_qty#18L, ss_wc#19, ss_sp#20, ws_qty#9L, ws_wc#10, ws_sp#11]
            :  +- SortMergeJoin [ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176], [ws_sold_year#7, ws_item_sk#29, ws_customer_sk#8], LeftOuter
            :     :- Sort [ss_sold_year#17 ASC NULLS FIRST, ss_item_sk#175 ASC NULLS FIRST, ss_customer_sk#176 ASC NULLS FIRST], false, 0
            :     :  +- HashAggregate(keys=[d_year#250, ss_item_sk#175, ss_customer_sk#176], functions=[sum(ss_quantity#183), sum(ss_wholesale_cost#184), sum(ss_sales_price#186)], output=[ss_sold_year#17, ss_item_sk#175, ss_customer_sk#176, ss_qty#18L, ss_wc#19, ss_sp#20])
            :     :     +- Exchange hashpartitioning(d_year#250, ss_item_sk#175, ss_customer_sk#176, 200), ENSURE_REQUIREMENTS, [plan_id=219]
            :     :        +- HashAggregate(keys=[d_year#250, ss_item_sk#175, ss_customer_sk#176], functions=[partial_sum(ss_quantity#183), partial_sum(ss_wholesale_cost#184), partial_sum(ss_sales_price#186)], output=[d_year#250, ss_item_sk#175, ss_customer_sk#176, sum#293L, sum#294, sum#295])
            :     :           +- Project [ss_item_sk#175, ss_customer_sk#176, ss_quantity#183, ss_wholesale_cost#184, ss_sales_price#186, d_year#250]
            :     :              +- BroadcastHashJoin [ss_sold_date_sk#173], [d_date_sk#244], Inner, BuildRight, false
            :     :                 :- Project [ss_sold_date_sk#173, ss_item_sk#175, ss_customer_sk#176, ss_quantity#183, ss_wholesale_cost#184, ss_sales_price#186]
            :     :                 :  +- Filter isnull(sr_ticket_number#205)
            :     :                 :     +- BroadcastHashJoin [ss_ticket_number#182, ss_item_sk#175], [sr_ticket_number#205, sr_item_sk#198], LeftOuter, BuildRight, false
            :     :                 :        :- Filter isnotnull(ss_sold_date_sk#173)
            :     :                 :        :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#173,ss_item_sk#175,ss_customer_sk#176,ss_ticket_number#182,ss_quantity#183,ss_wholesale_cost#184,ss_sales_price#186] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#173)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_ticket_number:int,ss_quantity:int...
            :     :                 :        +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=209]
            :     :                 :           +- Filter (isnotnull(sr_ticket_number#205) AND isnotnull(sr_item_sk#198))
            :     :                 :              +- FileScan parquet spark_catalog.tpcds.store_returns[sr_item_sk#198,sr_ticket_number#205] Batched: true, DataFilters: [isnotnull(sr_ticket_number#205), isnotnull(sr_item_sk#198)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_ticket_number), IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int>
            :     :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=214]
            :     :                    +- Filter ((isnotnull(d_year#250) AND (d_year#250 = 2000)) AND isnotnull(d_date_sk#244))
            :     :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#244,d_year#250] Batched: true, DataFilters: [isnotnull(d_year#250), (d_year#250 = 2000), isnotnull(d_date_sk#244)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
            :     +- Sort [ws_sold_year#7 ASC NULLS FIRST, ws_item_sk#29 ASC NULLS FIRST, ws_customer_sk#8 ASC NULLS FIRST], false, 0
            :        +- HashAggregate(keys=[d_year#90, ws_item_sk#29, ws_bill_customer_sk#30], functions=[sum(ws_quantity#44), sum(ws_wholesale_cost#45), sum(ws_sales_price#47)], output=[ws_sold_year#7, ws_item_sk#29, ws_customer_sk#8, ws_qty#9L, ws_wc#10, ws_sp#11])
            :           +- Exchange hashpartitioning(d_year#90, ws_item_sk#29, ws_bill_customer_sk#30, 200), ENSURE_REQUIREMENTS, [plan_id=231]
            :              +- HashAggregate(keys=[d_year#90, ws_item_sk#29, ws_bill_customer_sk#30], functions=[partial_sum(ws_quantity#44), partial_sum(ws_wholesale_cost#45), partial_sum(ws_sales_price#47)], output=[d_year#90, ws_item_sk#29, ws_bill_customer_sk#30, sum#299L, sum#300, sum#301])
            :                 +- Project [ws_item_sk#29, ws_bill_customer_sk#30, ws_quantity#44, ws_wholesale_cost#45, ws_sales_price#47, d_year#90]
            :                    +- BroadcastHashJoin [ws_sold_date_sk#26], [d_date_sk#84], Inner, BuildRight, false
            :                       :- Project [ws_sold_date_sk#26, ws_item_sk#29, ws_bill_customer_sk#30, ws_quantity#44, ws_wholesale_cost#45, ws_sales_price#47]
            :                       :  +- Filter isnull(wr_order_number#73)
            :                       :     +- BroadcastHashJoin [ws_order_number#43, ws_item_sk#29], [wr_order_number#73, wr_item_sk#62], LeftOuter, BuildRight, false
            :                       :        :- Filter ((isnotnull(ws_sold_date_sk#26) AND isnotnull(ws_item_sk#29)) AND isnotnull(ws_bill_customer_sk#30))
            :                       :        :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#26,ws_item_sk#29,ws_bill_customer_sk#30,ws_order_number#43,ws_quantity#44,ws_wholesale_cost#45,ws_sales_price#47] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#26), isnotnull(ws_item_sk#29), isnotnull(ws_bill_customer_sk#30)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk), IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int,ws_order_number:int,ws_quantity...
            :                       :        +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=221]
            :                       :           +- Filter (isnotnull(wr_order_number#73) AND isnotnull(wr_item_sk#62))
            :                       :              +- FileScan parquet spark_catalog.tpcds.web_returns[wr_item_sk#62,wr_order_number#73] Batched: true, DataFilters: [isnotnull(wr_order_number#73), isnotnull(wr_item_sk#62)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_order_number), IsNotNull(wr_item_sk)], ReadSchema: struct<wr_item_sk:int,wr_order_number:int>
            :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=226]
            :                          +- Filter ((isnotnull(d_date_sk#84) AND (d_year#90 = 2000)) AND isnotnull(d_year#90))
            :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#84,d_year#90] Batched: true, DataFilters: [isnotnull(d_date_sk#84), (d_year#90 = 2000), isnotnull(d_year#90)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), EqualTo(d_year,2000), IsNotNull(d_year)], ReadSchema: struct<d_date_sk:int,d_year:int>
            +- Sort [cs_sold_year#12 ASC NULLS FIRST, cs_item_sk#127 ASC NULLS FIRST, cs_customer_sk#13 ASC NULLS FIRST], false, 0
               +- HashAggregate(keys=[d_year#222, cs_item_sk#127, cs_bill_customer_sk#115], functions=[sum(cs_quantity#130), sum(cs_wholesale_cost#131), sum(cs_sales_price#133)], output=[cs_sold_year#12, cs_item_sk#127, cs_customer_sk#13, cs_qty#14L, cs_wc#15, cs_sp#16])
                  +- Exchange hashpartitioning(d_year#222, cs_item_sk#127, cs_bill_customer_sk#115, 200), ENSURE_REQUIREMENTS, [plan_id=249]
                     +- HashAggregate(keys=[d_year#222, cs_item_sk#127, cs_bill_customer_sk#115], functions=[partial_sum(cs_quantity#130), partial_sum(cs_wholesale_cost#131), partial_sum(cs_sales_price#133)], output=[d_year#222, cs_item_sk#127, cs_bill_customer_sk#115, sum#305L, sum#306, sum#307])
                        +- Project [cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133, d_year#222]
                           +- BroadcastHashJoin [cs_sold_date_sk#112], [d_date_sk#216], Inner, BuildRight, false
                              :- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_wholesale_cost#131, cs_sales_price#133]
                              :  +- Filter isnull(cr_order_number#162)
                              :     +- BroadcastHashJoin [cs_order_number#129, cs_item_sk#127], [cr_order_number#162, cr_item_sk#148], LeftOuter, BuildRight, false
                              :        :- Filter ((isnotnull(cs_sold_date_sk#112) AND isnotnull(cs_item_sk#127)) AND isnotnull(cs_bill_customer_sk#115))
                              :        :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#112,cs_bill_customer_sk#115,cs_item_sk#127,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_sales_price#133] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#112), isnotnull(cs_item_sk#127), isnotnull(cs_bill_customer_sk#115)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_item_sk), IsNotNull(cs_bill_customer_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_order_number:int,cs_quantity...
                              :        +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=239]
                              :           +- Filter (isnotnull(cr_order_number#162) AND isnotnull(cr_item_sk#148))
                              :              +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_item_sk#148,cr_order_number#162] Batched: true, DataFilters: [isnotnull(cr_order_number#162), isnotnull(cr_item_sk#148)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_order_number), IsNotNull(cr_item_sk)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=244]
                                 +- Filter ((isnotnull(d_date_sk#216) AND (d_year#222 = 2000)) AND isnotnull(d_year#222))
                                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#216,d_year#222] Batched: true, DataFilters: [isnotnull(d_date_sk#216), (d_year#222 = 2000), isnotnull(d_year#222)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), EqualTo(d_year,2000), IsNotNull(d_year)], ReadSchema: struct<d_date_sk:int,d_year:int>

Time taken: 3.444 seconds, Fetched 1 row(s)
