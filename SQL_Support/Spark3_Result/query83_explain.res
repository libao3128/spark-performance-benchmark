Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582774124
== Parsed Logical Plan ==
CTE [sr_items, cr_items, wr_items]
:  :- 'SubqueryAlias sr_items
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#4, 'sum('sr_return_quantity) AS sr_item_qty#5]
:  :     +- 'Filter ((('sr_item_sk = 'i_item_sk) AND 'd_date IN (list#7 [])) AND ('sr_returned_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter 'd_week_seq IN (list#6 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter 'd_date IN (2000-06-30,2000-09-27,2000-11-17)
:  :        :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [store_returns], [], false
:  :           :  +- 'UnresolvedRelation [item], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  :- 'SubqueryAlias cr_items
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#8, 'sum('cr_return_quantity) AS cr_item_qty#9]
:  :     +- 'Filter ((('cr_item_sk = 'i_item_sk) AND 'd_date IN (list#11 [])) AND ('cr_returned_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter 'd_week_seq IN (list#10 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter 'd_date IN (2000-06-30,2000-09-27,2000-11-17)
:  :        :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [catalog_returns], [], false
:  :           :  +- 'UnresolvedRelation [item], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias wr_items
:     +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#12, 'sum('wr_return_quantity) AS wr_item_qty#13]
:        +- 'Filter ((('wr_item_sk = 'i_item_sk) AND 'd_date IN (list#15 [])) AND ('wr_returned_date_sk = 'd_date_sk))
:           :  +- 'Project ['d_date]
:           :     +- 'Filter 'd_week_seq IN (list#14 [])
:           :        :  +- 'Project ['d_week_seq]
:           :        :     +- 'Filter 'd_date IN (2000-06-30,2000-09-27,2000-11-17)
:           :        :        +- 'UnresolvedRelation [date_dim], [], false
:           :        +- 'UnresolvedRelation [date_dim], [], false
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation [web_returns], [], false
:              :  +- 'UnresolvedRelation [item], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['sr_items.item_id ASC NULLS FIRST, 'sr_item_qty ASC NULLS FIRST], true
         +- 'Project ['sr_items.item_id, 'sr_item_qty, ((('sr_item_qty / (('sr_item_qty + 'cr_item_qty) + 'wr_item_qty)) / 3.0) * 100) AS sr_dev#0, 'cr_item_qty, ((('cr_item_qty / (('sr_item_qty + 'cr_item_qty) + 'wr_item_qty)) / 3.0) * 100) AS cr_dev#1, 'wr_item_qty, ((('wr_item_qty / (('sr_item_qty + 'cr_item_qty) + 'wr_item_qty)) / 3.0) * 100) AS wr_dev#2, ((('sr_item_qty + 'cr_item_qty) + 'wr_item_qty) / 3.0) AS average#3]
            +- 'Filter (('sr_items.item_id = 'cr_items.item_id) AND ('sr_items.item_id = 'wr_items.item_id))
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'UnresolvedRelation [sr_items], [], false
                  :  +- 'UnresolvedRelation [cr_items], [], false
                  +- 'UnresolvedRelation [wr_items], [], false

== Analyzed Logical Plan ==
item_id: string, sr_item_qty: bigint, sr_dev: double, cr_item_qty: bigint, cr_dev: double, wr_item_qty: bigint, wr_dev: double, average: decimal(27,6)
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias sr_items
:     +- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#4, sum(sr_return_quantity#31) AS sr_item_qty#5L]
:        +- Filter (((sr_item_sk#23 = i_item_sk#41) AND d_date#65 IN (list#7 [])) AND (sr_returned_date_sk#21 = d_date_sk#63))
:           :  +- Project [d_date#337]
:           :     +- Filter d_week_seq#339 IN (list#6 [])
:           :        :  +- Project [d_week_seq#247]
:           :        :     +- Filter d_date#245 IN (2000-06-30,2000-09-27,2000-11-17)
:           :        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#243,d_date_id#244,d_date#245,d_month_seq#246,d_week_seq#247,d_quarter_seq#248,d_year#249,d_dow#250,d_moy#251,d_dom#252,d_qoy#253,d_fy_year#254,d_fy_quarter_seq#255,d_fy_week_seq#256,d_day_name#257,d_quarter_name#258,d_holiday#259,d_weekend#260,d_following_holiday#261,d_first_dom#262,d_last_dom#263,d_same_day_ly#264,d_same_day_lq#265,d_current_day#266,... 4 more fields] parquet
:           :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#335,d_date_id#336,d_date#337,d_month_seq#338,d_week_seq#339,d_quarter_seq#340,d_year#341,d_dow#342,d_moy#343,d_dom#344,d_qoy#345,d_fy_year#346,d_fy_quarter_seq#347,d_fy_week_seq#348,d_day_name#349,d_quarter_name#350,d_holiday#351,d_weekend#352,d_following_holiday#353,d_first_dom#354,d_last_dom#355,d_same_day_ly#356,d_same_day_lq#357,d_current_day#358,... 4 more fields] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.store_returns
:              :  :  +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#21,sr_return_time_sk#22,sr_item_sk#23,sr_customer_sk#24,sr_cdemo_sk#25,sr_hdemo_sk#26,sr_addr_sk#27,sr_store_sk#28,sr_reason_sk#29,sr_ticket_number#30,sr_return_quantity#31,sr_return_amt#32,sr_return_tax#33,sr_return_amt_inc_tax#34,sr_fee#35,sr_return_ship_cost#36,sr_refunded_cash#37,sr_reversed_charge#38,sr_store_credit#39,sr_net_loss#40] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias cr_items
:     +- Aggregate [i_item_id#143], [i_item_id#143 AS item_id#8, sum(cr_return_quantity#108) AS cr_item_qty#9L]
:        +- Filter (((cr_item_sk#93 = i_item_sk#142) AND d_date#166 IN (list#11 [])) AND (cr_returned_date_sk#91 = d_date_sk#164))
:           :  +- Project [d_date#365]
:           :     +- Filter d_week_seq#367 IN (list#10 [])
:           :        :  +- Project [d_week_seq#275]
:           :        :     +- Filter d_date#273 IN (2000-06-30,2000-09-27,2000-11-17)
:           :        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#271,d_date_id#272,d_date#273,d_month_seq#274,d_week_seq#275,d_quarter_seq#276,d_year#277,d_dow#278,d_moy#279,d_dom#280,d_qoy#281,d_fy_year#282,d_fy_quarter_seq#283,d_fy_week_seq#284,d_day_name#285,d_quarter_name#286,d_holiday#287,d_weekend#288,d_following_holiday#289,d_first_dom#290,d_last_dom#291,d_same_day_ly#292,d_same_day_lq#293,d_current_day#294,... 4 more fields] parquet
:           :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#363,d_date_id#364,d_date#365,d_month_seq#366,d_week_seq#367,d_quarter_seq#368,d_year#369,d_dow#370,d_moy#371,d_dom#372,d_qoy#373,d_fy_year#374,d_fy_quarter_seq#375,d_fy_week_seq#376,d_day_name#377,d_quarter_name#378,d_holiday#379,d_weekend#380,d_following_holiday#381,d_first_dom#382,d_last_dom#383,d_same_day_ly#384,d_same_day_lq#385,d_current_day#386,... 4 more fields] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.catalog_returns
:              :  :  +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#91,cr_returned_time_sk#92,cr_item_sk#93,cr_refunded_customer_sk#94,cr_refunded_cdemo_sk#95,cr_refunded_hdemo_sk#96,cr_refunded_addr_sk#97,cr_returning_customer_sk#98,cr_returning_cdemo_sk#99,cr_returning_hdemo_sk#100,cr_returning_addr_sk#101,cr_call_center_sk#102,cr_catalog_page_sk#103,cr_ship_mode_sk#104,cr_warehouse_sk#105,cr_reason_sk#106,cr_order_number#107,cr_return_quantity#108,cr_return_amount#109,cr_return_tax#110,cr_return_amt_inc_tax#111,cr_fee#112,cr_return_ship_cost#113,cr_refunded_cash#114,... 3 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#142,i_item_id#143,i_rec_start_date#144,i_rec_end_date#145,i_item_desc#146,i_current_price#147,i_wholesale_cost#148,i_brand_id#149,i_brand#150,i_class_id#151,i_class#152,i_category_id#153,i_category#154,i_manufact_id#155,i_manufact#156,i_size#157,i_formulation#158,i_color#159,i_units#160,i_container#161,i_manager_id#162,i_product_name#163] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#164,d_date_id#165,d_date#166,d_month_seq#167,d_week_seq#168,d_quarter_seq#169,d_year#170,d_dow#171,d_moy#172,d_dom#173,d_qoy#174,d_fy_year#175,d_fy_quarter_seq#176,d_fy_week_seq#177,d_day_name#178,d_quarter_name#179,d_holiday#180,d_weekend#181,d_following_holiday#182,d_first_dom#183,d_last_dom#184,d_same_day_ly#185,d_same_day_lq#186,d_current_day#187,... 4 more fields] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias wr_items
:     +- Aggregate [i_item_id#193], [i_item_id#193 AS item_id#12, sum(wr_return_quantity#132) AS wr_item_qty#13L]
:        +- Filter (((wr_item_sk#120 = i_item_sk#192) AND d_date#216 IN (list#15 [])) AND (wr_returned_date_sk#118 = d_date_sk#214))
:           :  +- Project [d_date#393]
:           :     +- Filter d_week_seq#395 IN (list#14 [])
:           :        :  +- Project [d_week_seq#303]
:           :        :     +- Filter d_date#301 IN (2000-06-30,2000-09-27,2000-11-17)
:           :        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#299,d_date_id#300,d_date#301,d_month_seq#302,d_week_seq#303,d_quarter_seq#304,d_year#305,d_dow#306,d_moy#307,d_dom#308,d_qoy#309,d_fy_year#310,d_fy_quarter_seq#311,d_fy_week_seq#312,d_day_name#313,d_quarter_name#314,d_holiday#315,d_weekend#316,d_following_holiday#317,d_first_dom#318,d_last_dom#319,d_same_day_ly#320,d_same_day_lq#321,d_current_day#322,... 4 more fields] parquet
:           :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#391,d_date_id#392,d_date#393,d_month_seq#394,d_week_seq#395,d_quarter_seq#396,d_year#397,d_dow#398,d_moy#399,d_dom#400,d_qoy#401,d_fy_year#402,d_fy_quarter_seq#403,d_fy_week_seq#404,d_day_name#405,d_quarter_name#406,d_holiday#407,d_weekend#408,d_following_holiday#409,d_first_dom#410,d_last_dom#411,d_same_day_ly#412,d_same_day_lq#413,d_current_day#414,... 4 more fields] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.web_returns
:              :  :  +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#118,wr_returned_time_sk#119,wr_item_sk#120,wr_refunded_customer_sk#121,wr_refunded_cdemo_sk#122,wr_refunded_hdemo_sk#123,wr_refunded_addr_sk#124,wr_returning_customer_sk#125,wr_returning_cdemo_sk#126,wr_returning_hdemo_sk#127,wr_returning_addr_sk#128,wr_web_page_sk#129,wr_reason_sk#130,wr_order_number#131,wr_return_quantity#132,wr_return_amt#133,wr_return_tax#134,wr_return_amt_inc_tax#135,wr_fee#136,wr_return_ship_cost#137,wr_refunded_cash#138,wr_reversed_charge#139,wr_account_credit#140,wr_net_loss#141] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#192,i_item_id#193,i_rec_start_date#194,i_rec_end_date#195,i_item_desc#196,i_current_price#197,i_wholesale_cost#198,i_brand_id#199,i_brand#200,i_class_id#201,i_class#202,i_category_id#203,i_category#204,i_manufact_id#205,i_manufact#206,i_size#207,i_formulation#208,i_color#209,i_units#210,i_container#211,i_manager_id#212,i_product_name#213] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#214,d_date_id#215,d_date#216,d_month_seq#217,d_week_seq#218,d_quarter_seq#219,d_year#220,d_dow#221,d_moy#222,d_dom#223,d_qoy#224,d_fy_year#225,d_fy_quarter_seq#226,d_fy_week_seq#227,d_day_name#228,d_quarter_name#229,d_holiday#230,d_weekend#231,d_following_holiday#232,d_first_dom#233,d_last_dom#234,d_same_day_ly#235,d_same_day_lq#236,d_current_day#237,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [item_id#4 ASC NULLS FIRST, sr_item_qty#5L ASC NULLS FIRST], true
         +- Project [item_id#4, sr_item_qty#5L, (((cast(sr_item_qty#5L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / cast(3.0 as double)) * cast(100 as double)) AS sr_dev#0, cr_item_qty#9L, (((cast(cr_item_qty#9L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / cast(3.0 as double)) * cast(100 as double)) AS cr_dev#1, wr_item_qty#13L, (((cast(wr_item_qty#13L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / cast(3.0 as double)) * cast(100 as double)) AS wr_dev#2, (cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as decimal(20,0)) / 3.0) AS average#3]
            +- Filter ((item_id#4 = item_id#8) AND (item_id#4 = item_id#12))
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias sr_items
                  :  :  +- CTERelationRef 0, true, [item_id#4, sr_item_qty#5L], false
                  :  +- SubqueryAlias cr_items
                  :     +- CTERelationRef 1, true, [item_id#8, cr_item_qty#9L], false
                  +- SubqueryAlias wr_items
                     +- CTERelationRef 2, true, [item_id#12, wr_item_qty#13L], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_id#4 ASC NULLS FIRST, sr_item_qty#5L ASC NULLS FIRST], true
      +- Project [item_id#4, sr_item_qty#5L, (((cast(sr_item_qty#5L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS sr_dev#0, cr_item_qty#9L, (((cast(cr_item_qty#9L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS cr_dev#1, wr_item_qty#13L, (((cast(wr_item_qty#13L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS wr_dev#2, (cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as decimal(20,0)) / 3.0) AS average#3]
         +- Join Inner, (item_id#4 = item_id#12)
            :- Project [item_id#4, sr_item_qty#5L, cr_item_qty#9L]
            :  +- Join Inner, (item_id#4 = item_id#8)
            :     :- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#4, sum(sr_return_quantity#31) AS sr_item_qty#5L]
            :     :  +- Project [sr_return_quantity#31, i_item_id#42]
            :     :     +- Join Inner, (sr_returned_date_sk#21 = d_date_sk#63)
            :     :        :- Project [sr_returned_date_sk#21, sr_return_quantity#31, i_item_id#42]
            :     :        :  +- Join Inner, (sr_item_sk#23 = i_item_sk#41)
            :     :        :     :- Project [sr_returned_date_sk#21, sr_item_sk#23, sr_return_quantity#31]
            :     :        :     :  +- Filter (isnotnull(sr_item_sk#23) AND isnotnull(sr_returned_date_sk#21))
            :     :        :     :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#21,sr_return_time_sk#22,sr_item_sk#23,sr_customer_sk#24,sr_cdemo_sk#25,sr_hdemo_sk#26,sr_addr_sk#27,sr_store_sk#28,sr_reason_sk#29,sr_ticket_number#30,sr_return_quantity#31,sr_return_amt#32,sr_return_tax#33,sr_return_amt_inc_tax#34,sr_fee#35,sr_return_ship_cost#36,sr_refunded_cash#37,sr_reversed_charge#38,sr_store_credit#39,sr_net_loss#40] parquet
            :     :        :     +- Project [i_item_sk#41, i_item_id#42]
            :     :        :        +- Filter (isnotnull(i_item_sk#41) AND isnotnull(i_item_id#42))
            :     :        :           +- Relation spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
            :     :        +- Project [d_date_sk#63]
            :     :           +- Join LeftSemi, (d_date#65 = d_date#337)
            :     :              :- Project [d_date_sk#63, d_date#65]
            :     :              :  +- Filter isnotnull(d_date_sk#63)
            :     :              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :     :              +- Project [d_date#337]
            :     :                 +- Join LeftSemi, (d_week_seq#339 = d_week_seq#247)
            :     :                    :- Project [d_date#337, d_week_seq#339]
            :     :                    :  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#335,d_date_id#336,d_date#337,d_month_seq#338,d_week_seq#339,d_quarter_seq#340,d_year#341,d_dow#342,d_moy#343,d_dom#344,d_qoy#345,d_fy_year#346,d_fy_quarter_seq#347,d_fy_week_seq#348,d_day_name#349,d_quarter_name#350,d_holiday#351,d_weekend#352,d_following_holiday#353,d_first_dom#354,d_last_dom#355,d_same_day_ly#356,d_same_day_lq#357,d_current_day#358,... 4 more fields] parquet
            :     :                    +- Project [d_week_seq#247]
            :     :                       +- Filter d_date#245 IN (2000-06-30,2000-09-27,2000-11-17)
            :     :                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#243,d_date_id#244,d_date#245,d_month_seq#246,d_week_seq#247,d_quarter_seq#248,d_year#249,d_dow#250,d_moy#251,d_dom#252,d_qoy#253,d_fy_year#254,d_fy_quarter_seq#255,d_fy_week_seq#256,d_day_name#257,d_quarter_name#258,d_holiday#259,d_weekend#260,d_following_holiday#261,d_first_dom#262,d_last_dom#263,d_same_day_ly#264,d_same_day_lq#265,d_current_day#266,... 4 more fields] parquet
            :     +- Aggregate [i_item_id#143], [i_item_id#143 AS item_id#8, sum(cr_return_quantity#108) AS cr_item_qty#9L]
            :        +- Project [cr_return_quantity#108, i_item_id#143]
            :           +- Join Inner, (cr_returned_date_sk#91 = d_date_sk#164)
            :              :- Project [cr_returned_date_sk#91, cr_return_quantity#108, i_item_id#143]
            :              :  +- Join Inner, (cr_item_sk#93 = i_item_sk#142)
            :              :     :- Project [cr_returned_date_sk#91, cr_item_sk#93, cr_return_quantity#108]
            :              :     :  +- Filter (isnotnull(cr_item_sk#93) AND isnotnull(cr_returned_date_sk#91))
            :              :     :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#91,cr_returned_time_sk#92,cr_item_sk#93,cr_refunded_customer_sk#94,cr_refunded_cdemo_sk#95,cr_refunded_hdemo_sk#96,cr_refunded_addr_sk#97,cr_returning_customer_sk#98,cr_returning_cdemo_sk#99,cr_returning_hdemo_sk#100,cr_returning_addr_sk#101,cr_call_center_sk#102,cr_catalog_page_sk#103,cr_ship_mode_sk#104,cr_warehouse_sk#105,cr_reason_sk#106,cr_order_number#107,cr_return_quantity#108,cr_return_amount#109,cr_return_tax#110,cr_return_amt_inc_tax#111,cr_fee#112,cr_return_ship_cost#113,cr_refunded_cash#114,... 3 more fields] parquet
            :              :     +- Project [i_item_sk#142, i_item_id#143]
            :              :        +- Filter (isnotnull(i_item_sk#142) AND isnotnull(i_item_id#143))
            :              :           +- Relation spark_catalog.tpcds.item[i_item_sk#142,i_item_id#143,i_rec_start_date#144,i_rec_end_date#145,i_item_desc#146,i_current_price#147,i_wholesale_cost#148,i_brand_id#149,i_brand#150,i_class_id#151,i_class#152,i_category_id#153,i_category#154,i_manufact_id#155,i_manufact#156,i_size#157,i_formulation#158,i_color#159,i_units#160,i_container#161,i_manager_id#162,i_product_name#163] parquet
            :              +- Project [d_date_sk#164]
            :                 +- Join LeftSemi, (d_date#166 = d_date#365)
            :                    :- Project [d_date_sk#164, d_date#166]
            :                    :  +- Filter isnotnull(d_date_sk#164)
            :                    :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#164,d_date_id#165,d_date#166,d_month_seq#167,d_week_seq#168,d_quarter_seq#169,d_year#170,d_dow#171,d_moy#172,d_dom#173,d_qoy#174,d_fy_year#175,d_fy_quarter_seq#176,d_fy_week_seq#177,d_day_name#178,d_quarter_name#179,d_holiday#180,d_weekend#181,d_following_holiday#182,d_first_dom#183,d_last_dom#184,d_same_day_ly#185,d_same_day_lq#186,d_current_day#187,... 4 more fields] parquet
            :                    +- Project [d_date#365]
            :                       +- Join LeftSemi, (d_week_seq#367 = d_week_seq#275)
            :                          :- Project [d_date#365, d_week_seq#367]
            :                          :  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#363,d_date_id#364,d_date#365,d_month_seq#366,d_week_seq#367,d_quarter_seq#368,d_year#369,d_dow#370,d_moy#371,d_dom#372,d_qoy#373,d_fy_year#374,d_fy_quarter_seq#375,d_fy_week_seq#376,d_day_name#377,d_quarter_name#378,d_holiday#379,d_weekend#380,d_following_holiday#381,d_first_dom#382,d_last_dom#383,d_same_day_ly#384,d_same_day_lq#385,d_current_day#386,... 4 more fields] parquet
            :                          +- Project [d_week_seq#275]
            :                             +- Filter d_date#273 IN (2000-06-30,2000-09-27,2000-11-17)
            :                                +- Relation spark_catalog.tpcds.date_dim[d_date_sk#271,d_date_id#272,d_date#273,d_month_seq#274,d_week_seq#275,d_quarter_seq#276,d_year#277,d_dow#278,d_moy#279,d_dom#280,d_qoy#281,d_fy_year#282,d_fy_quarter_seq#283,d_fy_week_seq#284,d_day_name#285,d_quarter_name#286,d_holiday#287,d_weekend#288,d_following_holiday#289,d_first_dom#290,d_last_dom#291,d_same_day_ly#292,d_same_day_lq#293,d_current_day#294,... 4 more fields] parquet
            +- Aggregate [i_item_id#193], [i_item_id#193 AS item_id#12, sum(wr_return_quantity#132) AS wr_item_qty#13L]
               +- Project [wr_return_quantity#132, i_item_id#193]
                  +- Join Inner, (wr_returned_date_sk#118 = d_date_sk#214)
                     :- Project [wr_returned_date_sk#118, wr_return_quantity#132, i_item_id#193]
                     :  +- Join Inner, (wr_item_sk#120 = i_item_sk#192)
                     :     :- Project [wr_returned_date_sk#118, wr_item_sk#120, wr_return_quantity#132]
                     :     :  +- Filter (isnotnull(wr_item_sk#120) AND isnotnull(wr_returned_date_sk#118))
                     :     :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#118,wr_returned_time_sk#119,wr_item_sk#120,wr_refunded_customer_sk#121,wr_refunded_cdemo_sk#122,wr_refunded_hdemo_sk#123,wr_refunded_addr_sk#124,wr_returning_customer_sk#125,wr_returning_cdemo_sk#126,wr_returning_hdemo_sk#127,wr_returning_addr_sk#128,wr_web_page_sk#129,wr_reason_sk#130,wr_order_number#131,wr_return_quantity#132,wr_return_amt#133,wr_return_tax#134,wr_return_amt_inc_tax#135,wr_fee#136,wr_return_ship_cost#137,wr_refunded_cash#138,wr_reversed_charge#139,wr_account_credit#140,wr_net_loss#141] parquet
                     :     +- Project [i_item_sk#192, i_item_id#193]
                     :        +- Filter (isnotnull(i_item_sk#192) AND isnotnull(i_item_id#193))
                     :           +- Relation spark_catalog.tpcds.item[i_item_sk#192,i_item_id#193,i_rec_start_date#194,i_rec_end_date#195,i_item_desc#196,i_current_price#197,i_wholesale_cost#198,i_brand_id#199,i_brand#200,i_class_id#201,i_class#202,i_category_id#203,i_category#204,i_manufact_id#205,i_manufact#206,i_size#207,i_formulation#208,i_color#209,i_units#210,i_container#211,i_manager_id#212,i_product_name#213] parquet
                     +- Project [d_date_sk#214]
                        +- Join LeftSemi, (d_date#216 = d_date#393)
                           :- Project [d_date_sk#214, d_date#216]
                           :  +- Filter isnotnull(d_date_sk#214)
                           :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#214,d_date_id#215,d_date#216,d_month_seq#217,d_week_seq#218,d_quarter_seq#219,d_year#220,d_dow#221,d_moy#222,d_dom#223,d_qoy#224,d_fy_year#225,d_fy_quarter_seq#226,d_fy_week_seq#227,d_day_name#228,d_quarter_name#229,d_holiday#230,d_weekend#231,d_following_holiday#232,d_first_dom#233,d_last_dom#234,d_same_day_ly#235,d_same_day_lq#236,d_current_day#237,... 4 more fields] parquet
                           +- Project [d_date#393]
                              +- Join LeftSemi, (d_week_seq#395 = d_week_seq#303)
                                 :- Project [d_date#393, d_week_seq#395]
                                 :  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#391,d_date_id#392,d_date#393,d_month_seq#394,d_week_seq#395,d_quarter_seq#396,d_year#397,d_dow#398,d_moy#399,d_dom#400,d_qoy#401,d_fy_year#402,d_fy_quarter_seq#403,d_fy_week_seq#404,d_day_name#405,d_quarter_name#406,d_holiday#407,d_weekend#408,d_following_holiday#409,d_first_dom#410,d_last_dom#411,d_same_day_ly#412,d_same_day_lq#413,d_current_day#414,... 4 more fields] parquet
                                 +- Project [d_week_seq#303]
                                    +- Filter d_date#301 IN (2000-06-30,2000-09-27,2000-11-17)
                                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#299,d_date_id#300,d_date#301,d_month_seq#302,d_week_seq#303,d_quarter_seq#304,d_year#305,d_dow#306,d_moy#307,d_dom#308,d_qoy#309,d_fy_year#310,d_fy_quarter_seq#311,d_fy_week_seq#312,d_day_name#313,d_quarter_name#314,d_holiday#315,d_weekend#316,d_following_holiday#317,d_first_dom#318,d_last_dom#319,d_same_day_ly#320,d_same_day_lq#321,d_current_day#322,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[item_id#4 ASC NULLS FIRST,sr_item_qty#5L ASC NULLS FIRST], output=[item_id#4,sr_item_qty#5L,sr_dev#0,cr_item_qty#9L,cr_dev#1,wr_item_qty#13L,wr_dev#2,average#3])
   +- Project [item_id#4, sr_item_qty#5L, (((cast(sr_item_qty#5L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS sr_dev#0, cr_item_qty#9L, (((cast(cr_item_qty#9L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS cr_dev#1, wr_item_qty#13L, (((cast(wr_item_qty#13L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS wr_dev#2, (cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as decimal(20,0)) / 3.0) AS average#3]
      +- SortMergeJoin [item_id#4], [item_id#12], Inner
         :- Project [item_id#4, sr_item_qty#5L, cr_item_qty#9L]
         :  +- SortMergeJoin [item_id#4], [item_id#8], Inner
         :     :- Sort [item_id#4 ASC NULLS FIRST], false, 0
         :     :  +- HashAggregate(keys=[i_item_id#42], functions=[sum(sr_return_quantity#31)], output=[item_id#4, sr_item_qty#5L])
         :     :     +- Exchange hashpartitioning(i_item_id#42, 200), ENSURE_REQUIREMENTS, [plan_id=295]
         :     :        +- HashAggregate(keys=[i_item_id#42], functions=[partial_sum(sr_return_quantity#31)], output=[i_item_id#42, sum#423L])
         :     :           +- Project [sr_return_quantity#31, i_item_id#42]
         :     :              +- BroadcastHashJoin [sr_returned_date_sk#21], [d_date_sk#63], Inner, BuildRight, false
         :     :                 :- Project [sr_returned_date_sk#21, sr_return_quantity#31, i_item_id#42]
         :     :                 :  +- BroadcastHashJoin [sr_item_sk#23], [i_item_sk#41], Inner, BuildRight, false
         :     :                 :     :- Filter (isnotnull(sr_item_sk#23) AND isnotnull(sr_returned_date_sk#21))
         :     :                 :     :  +- FileScan parquet spark_catalog.tpcds.store_returns[sr_returned_date_sk#21,sr_item_sk#23,sr_return_quantity#31] Batched: true, DataFilters: [isnotnull(sr_item_sk#23), isnotnull(sr_returned_date_sk#21)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_returned_date_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_item_sk:int,sr_return_quantity:int>
         :     :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=279]
         :     :                 :        +- Filter (isnotnull(i_item_sk#41) AND isnotnull(i_item_id#42))
         :     :                 :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42] Batched: true, DataFilters: [isnotnull(i_item_sk#41), isnotnull(i_item_id#42)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
         :     :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=290]
         :     :                    +- Project [d_date_sk#63]
         :     :                       +- BroadcastHashJoin [d_date#65], [d_date#337], LeftSemi, BuildRight, false
         :     :                          :- Filter isnotnull(d_date_sk#63)
         :     :                          :  +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#63,d_date#65] Batched: true, DataFilters: [isnotnull(d_date_sk#63)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
         :     :                          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=286]
         :     :                             +- Project [d_date#337]
         :     :                                +- BroadcastHashJoin [d_week_seq#339], [d_week_seq#247], LeftSemi, BuildRight, false
         :     :                                   :- FileScan parquet spark_catalog.tpcds.date_dim[d_date#337,d_week_seq#339] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<d_date:string,d_week_seq:int>
         :     :                                   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=282]
         :     :                                      +- Project [d_week_seq#247]
         :     :                                         +- Filter d_date#245 IN (2000-06-30,2000-09-27,2000-11-17)
         :     :                                            +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#245,d_week_seq#247] Batched: true, DataFilters: [d_date#245 IN (2000-06-30,2000-09-27,2000-11-17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_date, [2000-06-30,2000-09-27,2000-11-17])], ReadSchema: struct<d_date:string,d_week_seq:int>
         :     +- Sort [item_id#8 ASC NULLS FIRST], false, 0
         :        +- HashAggregate(keys=[i_item_id#143], functions=[sum(cr_return_quantity#108)], output=[item_id#8, cr_item_qty#9L])
         :           +- Exchange hashpartitioning(i_item_id#143, 200), ENSURE_REQUIREMENTS, [plan_id=313]
         :              +- HashAggregate(keys=[i_item_id#143], functions=[partial_sum(cr_return_quantity#108)], output=[i_item_id#143, sum#425L])
         :                 +- Project [cr_return_quantity#108, i_item_id#143]
         :                    +- BroadcastHashJoin [cr_returned_date_sk#91], [d_date_sk#164], Inner, BuildRight, false
         :                       :- Project [cr_returned_date_sk#91, cr_return_quantity#108, i_item_id#143]
         :                       :  +- BroadcastHashJoin [cr_item_sk#93], [i_item_sk#142], Inner, BuildRight, false
         :                       :     :- Filter (isnotnull(cr_item_sk#93) AND isnotnull(cr_returned_date_sk#91))
         :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#91,cr_item_sk#93,cr_return_quantity#108] Batched: true, DataFilters: [isnotnull(cr_item_sk#93), isnotnull(cr_returned_date_sk#91)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_item_sk), IsNotNull(cr_returned_date_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_item_sk:int,cr_return_quantity:int>
         :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=297]
         :                       :        +- Filter (isnotnull(i_item_sk#142) AND isnotnull(i_item_id#143))
         :                       :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#142,i_item_id#143] Batched: true, DataFilters: [isnotnull(i_item_sk#142), isnotnull(i_item_id#143)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
         :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=308]
         :                          +- Project [d_date_sk#164]
         :                             +- BroadcastHashJoin [d_date#166], [d_date#365], LeftSemi, BuildRight, false
         :                                :- Filter isnotnull(d_date_sk#164)
         :                                :  +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#164,d_date#166] Batched: true, DataFilters: [isnotnull(d_date_sk#164)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
         :                                +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=304]
         :                                   +- Project [d_date#365]
         :                                      +- BroadcastHashJoin [d_week_seq#367], [d_week_seq#275], LeftSemi, BuildRight, false
         :                                         :- FileScan parquet spark_catalog.tpcds.date_dim[d_date#365,d_week_seq#367] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<d_date:string,d_week_seq:int>
         :                                         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=300]
         :                                            +- Project [d_week_seq#275]
         :                                               +- Filter d_date#273 IN (2000-06-30,2000-09-27,2000-11-17)
         :                                                  +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#273,d_week_seq#275] Batched: true, DataFilters: [d_date#273 IN (2000-06-30,2000-09-27,2000-11-17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_date, [2000-06-30,2000-09-27,2000-11-17])], ReadSchema: struct<d_date:string,d_week_seq:int>
         +- Sort [item_id#12 ASC NULLS FIRST], false, 0
            +- HashAggregate(keys=[i_item_id#193], functions=[sum(wr_return_quantity#132)], output=[item_id#12, wr_item_qty#13L])
               +- Exchange hashpartitioning(i_item_id#193, 200), ENSURE_REQUIREMENTS, [plan_id=337]
                  +- HashAggregate(keys=[i_item_id#193], functions=[partial_sum(wr_return_quantity#132)], output=[i_item_id#193, sum#427L])
                     +- Project [wr_return_quantity#132, i_item_id#193]
                        +- BroadcastHashJoin [wr_returned_date_sk#118], [d_date_sk#214], Inner, BuildRight, false
                           :- Project [wr_returned_date_sk#118, wr_return_quantity#132, i_item_id#193]
                           :  +- BroadcastHashJoin [wr_item_sk#120], [i_item_sk#192], Inner, BuildRight, false
                           :     :- Filter (isnotnull(wr_item_sk#120) AND isnotnull(wr_returned_date_sk#118))
                           :     :  +- FileScan parquet spark_catalog.tpcds.web_returns[wr_returned_date_sk#118,wr_item_sk#120,wr_return_quantity#132] Batched: true, DataFilters: [isnotnull(wr_item_sk#120), isnotnull(wr_returned_date_sk#118)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_item_sk), IsNotNull(wr_returned_date_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_item_sk:int,wr_return_quantity:int>
                           :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=321]
                           :        +- Filter (isnotnull(i_item_sk#192) AND isnotnull(i_item_id#193))
                           :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#192,i_item_id#193] Batched: true, DataFilters: [isnotnull(i_item_sk#192), isnotnull(i_item_id#193)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
                           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=332]
                              +- Project [d_date_sk#214]
                                 +- BroadcastHashJoin [d_date#216], [d_date#393], LeftSemi, BuildRight, false
                                    :- Filter isnotnull(d_date_sk#214)
                                    :  +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#214,d_date#216] Batched: true, DataFilters: [isnotnull(d_date_sk#214)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                                    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=328]
                                       +- Project [d_date#393]
                                          +- BroadcastHashJoin [d_week_seq#395], [d_week_seq#303], LeftSemi, BuildRight, false
                                             :- FileScan parquet spark_catalog.tpcds.date_dim[d_date#393,d_week_seq#395] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<d_date:string,d_week_seq:int>
                                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=324]
                                                +- Project [d_week_seq#303]
                                                   +- Filter d_date#301 IN (2000-06-30,2000-09-27,2000-11-17)
                                                      +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#301,d_week_seq#303] Batched: true, DataFilters: [d_date#301 IN (2000-06-30,2000-09-27,2000-11-17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_date, [2000-06-30,2000-09-27,2000-11-17])], ReadSchema: struct<d_date:string,d_week_seq:int>

Time taken: 3.735 seconds, Fetched 1 row(s)
