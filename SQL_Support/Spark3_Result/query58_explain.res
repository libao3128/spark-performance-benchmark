Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581784858
== Parsed Logical Plan ==
CTE [ss_items, cs_items, ws_items]
:  :- 'SubqueryAlias ss_items
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#4, 'sum('ss_ext_sales_price) AS ss_item_rev#5]
:  :     +- 'Filter ((('ss_item_sk = 'i_item_sk) AND 'd_date IN (list#7 [])) AND ('ss_sold_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter ('d_week_seq = scalar-subquery#6 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter ('d_date = 2000-01-03)
:  :        :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [store_sales], [], false
:  :           :  +- 'UnresolvedRelation [item], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  :- 'SubqueryAlias cs_items
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#8, 'sum('cs_ext_sales_price) AS cs_item_rev#9]
:  :     +- 'Filter ((('cs_item_sk = 'i_item_sk) AND 'd_date IN (list#11 [])) AND ('cs_sold_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter ('d_week_seq = scalar-subquery#10 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter ('d_date = 2000-01-03)
:  :        :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        :        +- 'UnresolvedRelation [date_dim], [], false
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [catalog_sales], [], false
:  :           :  +- 'UnresolvedRelation [item], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias ws_items
:     +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#12, 'sum('ws_ext_sales_price) AS ws_item_rev#13]
:        +- 'Filter ((('ws_item_sk = 'i_item_sk) AND 'd_date IN (list#15 [])) AND ('ws_sold_date_sk = 'd_date_sk))
:           :  +- 'Project ['d_date]
:           :     +- 'Filter ('d_week_seq = scalar-subquery#14 [])
:           :        :  +- 'Project ['d_week_seq]
:           :        :     +- 'Filter ('d_date = 2000-01-03)
:           :        :        +- 'UnresolvedRelation [date_dim], [], false
:           :        +- 'UnresolvedRelation [date_dim], [], false
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation [web_sales], [], false
:              :  +- 'UnresolvedRelation [item], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['item_id ASC NULLS FIRST, 'ss_item_rev ASC NULLS FIRST], true
         +- 'Project ['ss_items.item_id, 'ss_item_rev, (('ss_item_rev / ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3)) * 100) AS ss_dev#0, 'cs_item_rev, (('cs_item_rev / ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3)) * 100) AS cs_dev#1, 'ws_item_rev, (('ws_item_rev / ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3)) * 100) AS ws_dev#2, ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3) AS average#3]
            +- 'Filter (((('ss_items.item_id = 'cs_items.item_id) AND ('ss_items.item_id = 'ws_items.item_id)) AND ((('ss_item_rev >= (0.9 * 'cs_item_rev)) AND ('ss_item_rev <= (1.1 * 'cs_item_rev))) AND (('ss_item_rev >= (0.9 * 'ws_item_rev)) AND ('ss_item_rev <= (1.1 * 'ws_item_rev))))) AND (((('cs_item_rev >= (0.9 * 'ss_item_rev)) AND ('cs_item_rev <= (1.1 * 'ss_item_rev))) AND (('cs_item_rev >= (0.9 * 'ws_item_rev)) AND ('cs_item_rev <= (1.1 * 'ws_item_rev)))) AND ((('ws_item_rev >= (0.9 * 'ss_item_rev)) AND ('ws_item_rev <= (1.1 * 'ss_item_rev))) AND (('ws_item_rev >= (0.9 * 'cs_item_rev)) AND ('ws_item_rev <= (1.1 * 'cs_item_rev))))))
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'UnresolvedRelation [ss_items], [], false
                  :  +- 'UnresolvedRelation [cs_items], [], false
                  +- 'UnresolvedRelation [ws_items], [], false

== Analyzed Logical Plan ==
item_id: string, ss_item_rev: double, ss_dev: double, cs_item_rev: double, cs_dev: double, ws_item_rev: double, ws_dev: double, average: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ss_items
:     +- Aggregate [i_item_id#45], [i_item_id#45 AS item_id#4, sum(ss_ext_sales_price#36) AS ss_item_rev#5]
:        +- Filter (((ss_item_sk#23 = i_item_sk#44) AND d_date#68 IN (list#7 [])) AND (ss_sold_date_sk#21 = d_date_sk#66))
:           :  +- Project [d_date#357]
:           :     +- Filter (d_week_seq#359 = scalar-subquery#6 [])
:           :        :  +- Project [d_week_seq#267]
:           :        :     +- Filter (d_date#265 = 2000-01-03)
:           :        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#263,d_date_id#264,d_date#265,d_month_seq#266,d_week_seq#267,d_quarter_seq#268,d_year#269,d_dow#270,d_moy#271,d_dom#272,d_qoy#273,d_fy_year#274,d_fy_quarter_seq#275,d_fy_week_seq#276,d_day_name#277,d_quarter_name#278,d_holiday#279,d_weekend#280,d_following_holiday#281,d_first_dom#282,d_last_dom#283,d_same_day_ly#284,d_same_day_lq#285,d_current_day#286,... 4 more fields] parquet
:           :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#355,d_date_id#356,d_date#357,d_month_seq#358,d_week_seq#359,d_quarter_seq#360,d_year#361,d_dow#362,d_moy#363,d_dom#364,d_qoy#365,d_fy_year#366,d_fy_quarter_seq#367,d_fy_week_seq#368,d_day_name#369,d_quarter_name#370,d_holiday#371,d_weekend#372,d_following_holiday#373,d_first_dom#374,d_last_dom#375,d_same_day_ly#376,d_same_day_lq#377,d_current_day#378,... 4 more fields] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#44,i_item_id#45,i_rec_start_date#46,i_rec_end_date#47,i_item_desc#48,i_current_price#49,i_wholesale_cost#50,i_brand_id#51,i_brand#52,i_class_id#53,i_class#54,i_category_id#55,i_category#56,i_manufact_id#57,i_manufact#58,i_size#59,i_formulation#60,i_color#61,i_units#62,i_container#63,i_manager_id#64,i_product_name#65] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#66,d_date_id#67,d_date#68,d_month_seq#69,d_week_seq#70,d_quarter_seq#71,d_year#72,d_dow#73,d_moy#74,d_dom#75,d_qoy#76,d_fy_year#77,d_fy_quarter_seq#78,d_fy_week_seq#79,d_day_name#80,d_quarter_name#81,d_holiday#82,d_weekend#83,d_following_holiday#84,d_first_dom#85,d_last_dom#86,d_same_day_ly#87,d_same_day_lq#88,d_current_day#89,... 4 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias cs_items
:     +- Aggregate [i_item_id#163], [i_item_id#163 AS item_id#8, sum(cs_ext_sales_price#117) AS cs_item_rev#9]
:        +- Filter (((cs_item_sk#109 = i_item_sk#162) AND d_date#186 IN (list#11 [])) AND (cs_sold_date_sk#94 = d_date_sk#184))
:           :  +- Project [d_date#385]
:           :     +- Filter (d_week_seq#387 = scalar-subquery#10 [])
:           :        :  +- Project [d_week_seq#295]
:           :        :     +- Filter (d_date#293 = 2000-01-03)
:           :        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#291,d_date_id#292,d_date#293,d_month_seq#294,d_week_seq#295,d_quarter_seq#296,d_year#297,d_dow#298,d_moy#299,d_dom#300,d_qoy#301,d_fy_year#302,d_fy_quarter_seq#303,d_fy_week_seq#304,d_day_name#305,d_quarter_name#306,d_holiday#307,d_weekend#308,d_following_holiday#309,d_first_dom#310,d_last_dom#311,d_same_day_ly#312,d_same_day_lq#313,d_current_day#314,... 4 more fields] parquet
:           :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#383,d_date_id#384,d_date#385,d_month_seq#386,d_week_seq#387,d_quarter_seq#388,d_year#389,d_dow#390,d_moy#391,d_dom#392,d_qoy#393,d_fy_year#394,d_fy_quarter_seq#395,d_fy_week_seq#396,d_day_name#397,d_quarter_name#398,d_holiday#399,d_weekend#400,d_following_holiday#401,d_first_dom#402,d_last_dom#403,d_same_day_ly#404,d_same_day_lq#405,d_current_day#406,... 4 more fields] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#94,cs_sold_time_sk#95,cs_ship_date_sk#96,cs_bill_customer_sk#97,cs_bill_cdemo_sk#98,cs_bill_hdemo_sk#99,cs_bill_addr_sk#100,cs_ship_customer_sk#101,cs_ship_cdemo_sk#102,cs_ship_hdemo_sk#103,cs_ship_addr_sk#104,cs_call_center_sk#105,cs_catalog_page_sk#106,cs_ship_mode_sk#107,cs_warehouse_sk#108,cs_item_sk#109,cs_promo_sk#110,cs_order_number#111,cs_quantity#112,cs_wholesale_cost#113,cs_list_price#114,cs_sales_price#115,cs_ext_discount_amt#116,cs_ext_sales_price#117,... 10 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#162,i_item_id#163,i_rec_start_date#164,i_rec_end_date#165,i_item_desc#166,i_current_price#167,i_wholesale_cost#168,i_brand_id#169,i_brand#170,i_class_id#171,i_class#172,i_category_id#173,i_category#174,i_manufact_id#175,i_manufact#176,i_size#177,i_formulation#178,i_color#179,i_units#180,i_container#181,i_manager_id#182,i_product_name#183] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#184,d_date_id#185,d_date#186,d_month_seq#187,d_week_seq#188,d_quarter_seq#189,d_year#190,d_dow#191,d_moy#192,d_dom#193,d_qoy#194,d_fy_year#195,d_fy_quarter_seq#196,d_fy_week_seq#197,d_day_name#198,d_quarter_name#199,d_holiday#200,d_weekend#201,d_following_holiday#202,d_first_dom#203,d_last_dom#204,d_same_day_ly#205,d_same_day_lq#206,d_current_day#207,... 4 more fields] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias ws_items
:     +- Aggregate [i_item_id#213], [i_item_id#213 AS item_id#12, sum(ws_ext_sales_price#151) AS ws_item_rev#13]
:        +- Filter (((ws_item_sk#131 = i_item_sk#212) AND d_date#236 IN (list#15 [])) AND (ws_sold_date_sk#128 = d_date_sk#234))
:           :  +- Project [d_date#413]
:           :     +- Filter (d_week_seq#415 = scalar-subquery#14 [])
:           :        :  +- Project [d_week_seq#323]
:           :        :     +- Filter (d_date#321 = 2000-01-03)
:           :        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#319,d_date_id#320,d_date#321,d_month_seq#322,d_week_seq#323,d_quarter_seq#324,d_year#325,d_dow#326,d_moy#327,d_dom#328,d_qoy#329,d_fy_year#330,d_fy_quarter_seq#331,d_fy_week_seq#332,d_day_name#333,d_quarter_name#334,d_holiday#335,d_weekend#336,d_following_holiday#337,d_first_dom#338,d_last_dom#339,d_same_day_ly#340,d_same_day_lq#341,d_current_day#342,... 4 more fields] parquet
:           :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#411,d_date_id#412,d_date#413,d_month_seq#414,d_week_seq#415,d_quarter_seq#416,d_year#417,d_dow#418,d_moy#419,d_dom#420,d_qoy#421,d_fy_year#422,d_fy_quarter_seq#423,d_fy_week_seq#424,d_day_name#425,d_quarter_name#426,d_holiday#427,d_weekend#428,d_following_holiday#429,d_first_dom#430,d_last_dom#431,d_same_day_ly#432,d_same_day_lq#433,d_current_day#434,... 4 more fields] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#128,ws_sold_time_sk#129,ws_ship_date_sk#130,ws_item_sk#131,ws_bill_customer_sk#132,ws_bill_cdemo_sk#133,ws_bill_hdemo_sk#134,ws_bill_addr_sk#135,ws_ship_customer_sk#136,ws_ship_cdemo_sk#137,ws_ship_hdemo_sk#138,ws_ship_addr_sk#139,ws_web_page_sk#140,ws_web_site_sk#141,ws_ship_mode_sk#142,ws_warehouse_sk#143,ws_promo_sk#144,ws_order_number#145,ws_quantity#146,ws_wholesale_cost#147,ws_list_price#148,ws_sales_price#149,ws_ext_discount_amt#150,ws_ext_sales_price#151,... 10 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#212,i_item_id#213,i_rec_start_date#214,i_rec_end_date#215,i_item_desc#216,i_current_price#217,i_wholesale_cost#218,i_brand_id#219,i_brand#220,i_class_id#221,i_class#222,i_category_id#223,i_category#224,i_manufact_id#225,i_manufact#226,i_size#227,i_formulation#228,i_color#229,i_units#230,i_container#231,i_manager_id#232,i_product_name#233] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#234,d_date_id#235,d_date#236,d_month_seq#237,d_week_seq#238,d_quarter_seq#239,d_year#240,d_dow#241,d_moy#242,d_dom#243,d_qoy#244,d_fy_year#245,d_fy_quarter_seq#246,d_fy_week_seq#247,d_day_name#248,d_quarter_name#249,d_holiday#250,d_weekend#251,d_following_holiday#252,d_first_dom#253,d_last_dom#254,d_same_day_ly#255,d_same_day_lq#256,d_current_day#257,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [item_id#4 ASC NULLS FIRST, ss_item_rev#5 ASC NULLS FIRST], true
         +- Project [item_id#4, ss_item_rev#5, ((ss_item_rev#5 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double))) * cast(100 as double)) AS ss_dev#0, cs_item_rev#9, ((cs_item_rev#9 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double))) * cast(100 as double)) AS cs_dev#1, ws_item_rev#13, ((ws_item_rev#13 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double))) * cast(100 as double)) AS ws_dev#2, (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double)) AS average#3]
            +- Filter ((((item_id#4 = item_id#8) AND (item_id#4 = item_id#12)) AND (((ss_item_rev#5 >= (cast(0.9 as double) * cs_item_rev#9)) AND (ss_item_rev#5 <= (cast(1.1 as double) * cs_item_rev#9))) AND ((ss_item_rev#5 >= (cast(0.9 as double) * ws_item_rev#13)) AND (ss_item_rev#5 <= (cast(1.1 as double) * ws_item_rev#13))))) AND ((((cs_item_rev#9 >= (cast(0.9 as double) * ss_item_rev#5)) AND (cs_item_rev#9 <= (cast(1.1 as double) * ss_item_rev#5))) AND ((cs_item_rev#9 >= (cast(0.9 as double) * ws_item_rev#13)) AND (cs_item_rev#9 <= (cast(1.1 as double) * ws_item_rev#13)))) AND (((ws_item_rev#13 >= (cast(0.9 as double) * ss_item_rev#5)) AND (ws_item_rev#13 <= (cast(1.1 as double) * ss_item_rev#5))) AND ((ws_item_rev#13 >= (cast(0.9 as double) * cs_item_rev#9)) AND (ws_item_rev#13 <= (cast(1.1 as double) * cs_item_rev#9))))))
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias ss_items
                  :  :  +- CTERelationRef 0, true, [item_id#4, ss_item_rev#5], false
                  :  +- SubqueryAlias cs_items
                  :     +- CTERelationRef 1, true, [item_id#8, cs_item_rev#9], false
                  +- SubqueryAlias ws_items
                     +- CTERelationRef 2, true, [item_id#12, ws_item_rev#13], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_id#4 ASC NULLS FIRST, ss_item_rev#5 ASC NULLS FIRST], true
      +- Project [item_id#4, ss_item_rev#5, ((ss_item_rev#5 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ss_dev#0, cs_item_rev#9, ((cs_item_rev#9 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS cs_dev#1, ws_item_rev#13, ((ws_item_rev#13 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ws_dev#2, (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0) AS average#3]
         +- Join Inner, (((((((((item_id#4 = item_id#12) AND (ss_item_rev#5 >= (0.9 * ws_item_rev#13))) AND (ss_item_rev#5 <= (1.1 * ws_item_rev#13))) AND (cs_item_rev#9 >= (0.9 * ws_item_rev#13))) AND (cs_item_rev#9 <= (1.1 * ws_item_rev#13))) AND (ws_item_rev#13 >= (0.9 * ss_item_rev#5))) AND (ws_item_rev#13 <= (1.1 * ss_item_rev#5))) AND (ws_item_rev#13 >= (0.9 * cs_item_rev#9))) AND (ws_item_rev#13 <= (1.1 * cs_item_rev#9)))
            :- Project [item_id#4, ss_item_rev#5, cs_item_rev#9]
            :  +- Join Inner, (((((item_id#4 = item_id#8) AND (ss_item_rev#5 >= (0.9 * cs_item_rev#9))) AND (ss_item_rev#5 <= (1.1 * cs_item_rev#9))) AND (cs_item_rev#9 >= (0.9 * ss_item_rev#5))) AND (cs_item_rev#9 <= (1.1 * ss_item_rev#5)))
            :     :- Filter isnotnull(ss_item_rev#5)
            :     :  +- Aggregate [i_item_id#45], [i_item_id#45 AS item_id#4, sum(ss_ext_sales_price#36) AS ss_item_rev#5]
            :     :     +- Project [ss_ext_sales_price#36, i_item_id#45]
            :     :        +- Join Inner, (ss_sold_date_sk#21 = d_date_sk#66)
            :     :           :- Project [ss_sold_date_sk#21, ss_ext_sales_price#36, i_item_id#45]
            :     :           :  +- Join Inner, (ss_item_sk#23 = i_item_sk#44)
            :     :           :     :- Project [ss_sold_date_sk#21, ss_item_sk#23, ss_ext_sales_price#36]
            :     :           :     :  +- Filter (isnotnull(ss_item_sk#23) AND isnotnull(ss_sold_date_sk#21))
            :     :           :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
            :     :           :     +- Project [i_item_sk#44, i_item_id#45]
            :     :           :        +- Filter (isnotnull(i_item_sk#44) AND isnotnull(i_item_id#45))
            :     :           :           +- Relation spark_catalog.tpcds.item[i_item_sk#44,i_item_id#45,i_rec_start_date#46,i_rec_end_date#47,i_item_desc#48,i_current_price#49,i_wholesale_cost#50,i_brand_id#51,i_brand#52,i_class_id#53,i_class#54,i_category_id#55,i_category#56,i_manufact_id#57,i_manufact#58,i_size#59,i_formulation#60,i_color#61,i_units#62,i_container#63,i_manager_id#64,i_product_name#65] parquet
            :     :           +- Project [d_date_sk#66]
            :     :              +- Join LeftSemi, (d_date#68 = d_date#357)
            :     :                 :- Project [d_date_sk#66, d_date#68]
            :     :                 :  +- Filter isnotnull(d_date_sk#66)
            :     :                 :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#66,d_date_id#67,d_date#68,d_month_seq#69,d_week_seq#70,d_quarter_seq#71,d_year#72,d_dow#73,d_moy#74,d_dom#75,d_qoy#76,d_fy_year#77,d_fy_quarter_seq#78,d_fy_week_seq#79,d_day_name#80,d_quarter_name#81,d_holiday#82,d_weekend#83,d_following_holiday#84,d_first_dom#85,d_last_dom#86,d_same_day_ly#87,d_same_day_lq#88,d_current_day#89,... 4 more fields] parquet
            :     :                 +- Project [d_date#357]
            :     :                    +- Filter (isnotnull(d_week_seq#359) AND (d_week_seq#359 = scalar-subquery#6 []))
            :     :                       :  +- Project [d_week_seq#267]
            :     :                       :     +- Filter (isnotnull(d_date#265) AND (d_date#265 = 2000-01-03))
            :     :                       :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#263,d_date_id#264,d_date#265,d_month_seq#266,d_week_seq#267,d_quarter_seq#268,d_year#269,d_dow#270,d_moy#271,d_dom#272,d_qoy#273,d_fy_year#274,d_fy_quarter_seq#275,d_fy_week_seq#276,d_day_name#277,d_quarter_name#278,d_holiday#279,d_weekend#280,d_following_holiday#281,d_first_dom#282,d_last_dom#283,d_same_day_ly#284,d_same_day_lq#285,d_current_day#286,... 4 more fields] parquet
            :     :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#355,d_date_id#356,d_date#357,d_month_seq#358,d_week_seq#359,d_quarter_seq#360,d_year#361,d_dow#362,d_moy#363,d_dom#364,d_qoy#365,d_fy_year#366,d_fy_quarter_seq#367,d_fy_week_seq#368,d_day_name#369,d_quarter_name#370,d_holiday#371,d_weekend#372,d_following_holiday#373,d_first_dom#374,d_last_dom#375,d_same_day_ly#376,d_same_day_lq#377,d_current_day#378,... 4 more fields] parquet
            :     +- Filter isnotnull(cs_item_rev#9)
            :        +- Aggregate [i_item_id#163], [i_item_id#163 AS item_id#8, sum(cs_ext_sales_price#117) AS cs_item_rev#9]
            :           +- Project [cs_ext_sales_price#117, i_item_id#163]
            :              +- Join Inner, (cs_sold_date_sk#94 = d_date_sk#184)
            :                 :- Project [cs_sold_date_sk#94, cs_ext_sales_price#117, i_item_id#163]
            :                 :  +- Join Inner, (cs_item_sk#109 = i_item_sk#162)
            :                 :     :- Project [cs_sold_date_sk#94, cs_item_sk#109, cs_ext_sales_price#117]
            :                 :     :  +- Filter (isnotnull(cs_item_sk#109) AND isnotnull(cs_sold_date_sk#94))
            :                 :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#94,cs_sold_time_sk#95,cs_ship_date_sk#96,cs_bill_customer_sk#97,cs_bill_cdemo_sk#98,cs_bill_hdemo_sk#99,cs_bill_addr_sk#100,cs_ship_customer_sk#101,cs_ship_cdemo_sk#102,cs_ship_hdemo_sk#103,cs_ship_addr_sk#104,cs_call_center_sk#105,cs_catalog_page_sk#106,cs_ship_mode_sk#107,cs_warehouse_sk#108,cs_item_sk#109,cs_promo_sk#110,cs_order_number#111,cs_quantity#112,cs_wholesale_cost#113,cs_list_price#114,cs_sales_price#115,cs_ext_discount_amt#116,cs_ext_sales_price#117,... 10 more fields] parquet
            :                 :     +- Project [i_item_sk#162, i_item_id#163]
            :                 :        +- Filter (isnotnull(i_item_sk#162) AND isnotnull(i_item_id#163))
            :                 :           +- Relation spark_catalog.tpcds.item[i_item_sk#162,i_item_id#163,i_rec_start_date#164,i_rec_end_date#165,i_item_desc#166,i_current_price#167,i_wholesale_cost#168,i_brand_id#169,i_brand#170,i_class_id#171,i_class#172,i_category_id#173,i_category#174,i_manufact_id#175,i_manufact#176,i_size#177,i_formulation#178,i_color#179,i_units#180,i_container#181,i_manager_id#182,i_product_name#183] parquet
            :                 +- Project [d_date_sk#184]
            :                    +- Join LeftSemi, (d_date#186 = d_date#385)
            :                       :- Project [d_date_sk#184, d_date#186]
            :                       :  +- Filter isnotnull(d_date_sk#184)
            :                       :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#184,d_date_id#185,d_date#186,d_month_seq#187,d_week_seq#188,d_quarter_seq#189,d_year#190,d_dow#191,d_moy#192,d_dom#193,d_qoy#194,d_fy_year#195,d_fy_quarter_seq#196,d_fy_week_seq#197,d_day_name#198,d_quarter_name#199,d_holiday#200,d_weekend#201,d_following_holiday#202,d_first_dom#203,d_last_dom#204,d_same_day_ly#205,d_same_day_lq#206,d_current_day#207,... 4 more fields] parquet
            :                       +- Project [d_date#385]
            :                          +- Filter (isnotnull(d_week_seq#387) AND (d_week_seq#387 = scalar-subquery#10 []))
            :                             :  +- Project [d_week_seq#267]
            :                             :     +- Filter (isnotnull(d_date#265) AND (d_date#265 = 2000-01-03))
            :                             :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#263,d_date_id#264,d_date#265,d_month_seq#266,d_week_seq#267,d_quarter_seq#268,d_year#269,d_dow#270,d_moy#271,d_dom#272,d_qoy#273,d_fy_year#274,d_fy_quarter_seq#275,d_fy_week_seq#276,d_day_name#277,d_quarter_name#278,d_holiday#279,d_weekend#280,d_following_holiday#281,d_first_dom#282,d_last_dom#283,d_same_day_ly#284,d_same_day_lq#285,d_current_day#286,... 4 more fields] parquet
            :                             +- Relation spark_catalog.tpcds.date_dim[d_date_sk#383,d_date_id#384,d_date#385,d_month_seq#386,d_week_seq#387,d_quarter_seq#388,d_year#389,d_dow#390,d_moy#391,d_dom#392,d_qoy#393,d_fy_year#394,d_fy_quarter_seq#395,d_fy_week_seq#396,d_day_name#397,d_quarter_name#398,d_holiday#399,d_weekend#400,d_following_holiday#401,d_first_dom#402,d_last_dom#403,d_same_day_ly#404,d_same_day_lq#405,d_current_day#406,... 4 more fields] parquet
            +- Filter isnotnull(ws_item_rev#13)
               +- Aggregate [i_item_id#213], [i_item_id#213 AS item_id#12, sum(ws_ext_sales_price#151) AS ws_item_rev#13]
                  +- Project [ws_ext_sales_price#151, i_item_id#213]
                     +- Join Inner, (ws_sold_date_sk#128 = d_date_sk#234)
                        :- Project [ws_sold_date_sk#128, ws_ext_sales_price#151, i_item_id#213]
                        :  +- Join Inner, (ws_item_sk#131 = i_item_sk#212)
                        :     :- Project [ws_sold_date_sk#128, ws_item_sk#131, ws_ext_sales_price#151]
                        :     :  +- Filter (isnotnull(ws_item_sk#131) AND isnotnull(ws_sold_date_sk#128))
                        :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#128,ws_sold_time_sk#129,ws_ship_date_sk#130,ws_item_sk#131,ws_bill_customer_sk#132,ws_bill_cdemo_sk#133,ws_bill_hdemo_sk#134,ws_bill_addr_sk#135,ws_ship_customer_sk#136,ws_ship_cdemo_sk#137,ws_ship_hdemo_sk#138,ws_ship_addr_sk#139,ws_web_page_sk#140,ws_web_site_sk#141,ws_ship_mode_sk#142,ws_warehouse_sk#143,ws_promo_sk#144,ws_order_number#145,ws_quantity#146,ws_wholesale_cost#147,ws_list_price#148,ws_sales_price#149,ws_ext_discount_amt#150,ws_ext_sales_price#151,... 10 more fields] parquet
                        :     +- Project [i_item_sk#212, i_item_id#213]
                        :        +- Filter (isnotnull(i_item_sk#212) AND isnotnull(i_item_id#213))
                        :           +- Relation spark_catalog.tpcds.item[i_item_sk#212,i_item_id#213,i_rec_start_date#214,i_rec_end_date#215,i_item_desc#216,i_current_price#217,i_wholesale_cost#218,i_brand_id#219,i_brand#220,i_class_id#221,i_class#222,i_category_id#223,i_category#224,i_manufact_id#225,i_manufact#226,i_size#227,i_formulation#228,i_color#229,i_units#230,i_container#231,i_manager_id#232,i_product_name#233] parquet
                        +- Project [d_date_sk#234]
                           +- Join LeftSemi, (d_date#236 = d_date#413)
                              :- Project [d_date_sk#234, d_date#236]
                              :  +- Filter isnotnull(d_date_sk#234)
                              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#234,d_date_id#235,d_date#236,d_month_seq#237,d_week_seq#238,d_quarter_seq#239,d_year#240,d_dow#241,d_moy#242,d_dom#243,d_qoy#244,d_fy_year#245,d_fy_quarter_seq#246,d_fy_week_seq#247,d_day_name#248,d_quarter_name#249,d_holiday#250,d_weekend#251,d_following_holiday#252,d_first_dom#253,d_last_dom#254,d_same_day_ly#255,d_same_day_lq#256,d_current_day#257,... 4 more fields] parquet
                              +- Project [d_date#413]
                                 +- Filter (isnotnull(d_week_seq#415) AND (d_week_seq#415 = scalar-subquery#14 []))
                                    :  +- Project [d_week_seq#267]
                                    :     +- Filter (isnotnull(d_date#265) AND (d_date#265 = 2000-01-03))
                                    :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#263,d_date_id#264,d_date#265,d_month_seq#266,d_week_seq#267,d_quarter_seq#268,d_year#269,d_dow#270,d_moy#271,d_dom#272,d_qoy#273,d_fy_year#274,d_fy_quarter_seq#275,d_fy_week_seq#276,d_day_name#277,d_quarter_name#278,d_holiday#279,d_weekend#280,d_following_holiday#281,d_first_dom#282,d_last_dom#283,d_same_day_ly#284,d_same_day_lq#285,d_current_day#286,... 4 more fields] parquet
                                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#411,d_date_id#412,d_date#413,d_month_seq#414,d_week_seq#415,d_quarter_seq#416,d_year#417,d_dow#418,d_moy#419,d_dom#420,d_qoy#421,d_fy_year#422,d_fy_quarter_seq#423,d_fy_week_seq#424,d_day_name#425,d_quarter_name#426,d_holiday#427,d_weekend#428,d_following_holiday#429,d_first_dom#430,d_last_dom#431,d_same_day_ly#432,d_same_day_lq#433,d_current_day#434,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[item_id#4 ASC NULLS FIRST,ss_item_rev#5 ASC NULLS FIRST], output=[item_id#4,ss_item_rev#5,ss_dev#0,cs_item_rev#9,cs_dev#1,ws_item_rev#13,ws_dev#2,average#3])
   +- Project [item_id#4, ss_item_rev#5, ((ss_item_rev#5 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ss_dev#0, cs_item_rev#9, ((cs_item_rev#9 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS cs_dev#1, ws_item_rev#13, ((ws_item_rev#13 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ws_dev#2, (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0) AS average#3]
      +- SortMergeJoin [item_id#4], [item_id#12], Inner, ((((((((ss_item_rev#5 >= (0.9 * ws_item_rev#13)) AND (ss_item_rev#5 <= (1.1 * ws_item_rev#13))) AND (cs_item_rev#9 >= (0.9 * ws_item_rev#13))) AND (cs_item_rev#9 <= (1.1 * ws_item_rev#13))) AND (ws_item_rev#13 >= (0.9 * ss_item_rev#5))) AND (ws_item_rev#13 <= (1.1 * ss_item_rev#5))) AND (ws_item_rev#13 >= (0.9 * cs_item_rev#9))) AND (ws_item_rev#13 <= (1.1 * cs_item_rev#9)))
         :- Project [item_id#4, ss_item_rev#5, cs_item_rev#9]
         :  +- SortMergeJoin [item_id#4], [item_id#8], Inner, ((((ss_item_rev#5 >= (0.9 * cs_item_rev#9)) AND (ss_item_rev#5 <= (1.1 * cs_item_rev#9))) AND (cs_item_rev#9 >= (0.9 * ss_item_rev#5))) AND (cs_item_rev#9 <= (1.1 * ss_item_rev#5)))
         :     :- Sort [item_id#4 ASC NULLS FIRST], false, 0
         :     :  +- Filter isnotnull(ss_item_rev#5)
         :     :     +- HashAggregate(keys=[i_item_id#45], functions=[sum(ss_ext_sales_price#36)], output=[item_id#4, ss_item_rev#5])
         :     :        +- Exchange hashpartitioning(i_item_id#45, 200), ENSURE_REQUIREMENTS, [plan_id=320]
         :     :           +- HashAggregate(keys=[i_item_id#45], functions=[partial_sum(ss_ext_sales_price#36)], output=[i_item_id#45, sum#443])
         :     :              +- Project [ss_ext_sales_price#36, i_item_id#45]
         :     :                 +- BroadcastHashJoin [ss_sold_date_sk#21], [d_date_sk#66], Inner, BuildRight, false
         :     :                    :- Project [ss_sold_date_sk#21, ss_ext_sales_price#36, i_item_id#45]
         :     :                    :  +- BroadcastHashJoin [ss_item_sk#23], [i_item_sk#44], Inner, BuildRight, false
         :     :                    :     :- Filter (isnotnull(ss_item_sk#23) AND isnotnull(ss_sold_date_sk#21))
         :     :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_item_sk#23,ss_ext_sales_price#36] Batched: true, DataFilters: [isnotnull(ss_item_sk#23), isnotnull(ss_sold_date_sk#21)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
         :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=308]
         :     :                    :        +- Filter (isnotnull(i_item_sk#44) AND isnotnull(i_item_id#45))
         :     :                    :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#44,i_item_id#45] Batched: true, DataFilters: [isnotnull(i_item_sk#44), isnotnull(i_item_id#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
         :     :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=315]
         :     :                       +- Project [d_date_sk#66]
         :     :                          +- BroadcastHashJoin [d_date#68], [d_date#357], LeftSemi, BuildRight, false
         :     :                             :- Filter isnotnull(d_date_sk#66)
         :     :                             :  +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#66,d_date#68] Batched: true, DataFilters: [isnotnull(d_date_sk#66)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
         :     :                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=311]
         :     :                                +- Project [d_date#357]
         :     :                                   +- Filter (isnotnull(d_week_seq#359) AND (d_week_seq#359 = Subquery subquery#6, [id=#240]))
         :     :                                      :  +- Subquery subquery#6, [id=#240]
         :     :                                      :     +- AdaptiveSparkPlan isFinalPlan=false
         :     :                                      :        +- Project [d_week_seq#267]
         :     :                                      :           +- Filter (isnotnull(d_date#265) AND (d_date#265 = 2000-01-03))
         :     :                                      :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#265,d_week_seq#267] Batched: true, DataFilters: [isnotnull(d_date#265), (d_date#265 = 2000-01-03)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
         :     :                                      +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#357,d_week_seq#359] Batched: true, DataFilters: [isnotnull(d_week_seq#359)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_week_seq)], ReadSchema: struct<d_date:string,d_week_seq:int>
         :     +- Sort [item_id#8 ASC NULLS FIRST], false, 0
         :        +- Filter isnotnull(cs_item_rev#9)
         :           +- HashAggregate(keys=[i_item_id#163], functions=[sum(cs_ext_sales_price#117)], output=[item_id#8, cs_item_rev#9])
         :              +- Exchange hashpartitioning(i_item_id#163, 200), ENSURE_REQUIREMENTS, [plan_id=335]
         :                 +- HashAggregate(keys=[i_item_id#163], functions=[partial_sum(cs_ext_sales_price#117)], output=[i_item_id#163, sum#445])
         :                    +- Project [cs_ext_sales_price#117, i_item_id#163]
         :                       +- BroadcastHashJoin [cs_sold_date_sk#94], [d_date_sk#184], Inner, BuildRight, false
         :                          :- Project [cs_sold_date_sk#94, cs_ext_sales_price#117, i_item_id#163]
         :                          :  +- BroadcastHashJoin [cs_item_sk#109], [i_item_sk#162], Inner, BuildRight, false
         :                          :     :- Filter (isnotnull(cs_item_sk#109) AND isnotnull(cs_sold_date_sk#94))
         :                          :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#94,cs_item_sk#109,cs_ext_sales_price#117] Batched: true, DataFilters: [isnotnull(cs_item_sk#109), isnotnull(cs_sold_date_sk#94)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
         :                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=323]
         :                          :        +- Filter (isnotnull(i_item_sk#162) AND isnotnull(i_item_id#163))
         :                          :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#162,i_item_id#163] Batched: true, DataFilters: [isnotnull(i_item_sk#162), isnotnull(i_item_id#163)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
         :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=330]
         :                             +- Project [d_date_sk#184]
         :                                +- BroadcastHashJoin [d_date#186], [d_date#385], LeftSemi, BuildRight, false
         :                                   :- Filter isnotnull(d_date_sk#184)
         :                                   :  +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#184,d_date#186] Batched: true, DataFilters: [isnotnull(d_date_sk#184)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
         :                                   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=326]
         :                                      +- Project [d_date#385]
         :                                         +- Filter (isnotnull(d_week_seq#387) AND (d_week_seq#387 = Subquery subquery#10, [id=#250]))
         :                                            :  +- Subquery subquery#10, [id=#250]
         :                                            :     +- AdaptiveSparkPlan isFinalPlan=false
         :                                            :        +- Project [d_week_seq#267]
         :                                            :           +- Filter (isnotnull(d_date#265) AND (d_date#265 = 2000-01-03))
         :                                            :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#265,d_week_seq#267] Batched: true, DataFilters: [isnotnull(d_date#265), (d_date#265 = 2000-01-03)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
         :                                            +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#385,d_week_seq#387] Batched: true, DataFilters: [isnotnull(d_week_seq#387)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_week_seq)], ReadSchema: struct<d_date:string,d_week_seq:int>
         +- Sort [item_id#12 ASC NULLS FIRST], false, 0
            +- Filter isnotnull(ws_item_rev#13)
               +- HashAggregate(keys=[i_item_id#213], functions=[sum(ws_ext_sales_price#151)], output=[item_id#12, ws_item_rev#13])
                  +- Exchange hashpartitioning(i_item_id#213, 200), ENSURE_REQUIREMENTS, [plan_id=356]
                     +- HashAggregate(keys=[i_item_id#213], functions=[partial_sum(ws_ext_sales_price#151)], output=[i_item_id#213, sum#447])
                        +- Project [ws_ext_sales_price#151, i_item_id#213]
                           +- BroadcastHashJoin [ws_sold_date_sk#128], [d_date_sk#234], Inner, BuildRight, false
                              :- Project [ws_sold_date_sk#128, ws_ext_sales_price#151, i_item_id#213]
                              :  +- BroadcastHashJoin [ws_item_sk#131], [i_item_sk#212], Inner, BuildRight, false
                              :     :- Filter (isnotnull(ws_item_sk#131) AND isnotnull(ws_sold_date_sk#128))
                              :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#128,ws_item_sk#131,ws_ext_sales_price#151] Batched: true, DataFilters: [isnotnull(ws_item_sk#131), isnotnull(ws_sold_date_sk#128)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ext_sales_price:double>
                              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=344]
                              :        +- Filter (isnotnull(i_item_sk#212) AND isnotnull(i_item_id#213))
                              :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#212,i_item_id#213] Batched: true, DataFilters: [isnotnull(i_item_sk#212), isnotnull(i_item_id#213)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=351]
                                 +- Project [d_date_sk#234]
                                    +- BroadcastHashJoin [d_date#236], [d_date#413], LeftSemi, BuildRight, false
                                       :- Filter isnotnull(d_date_sk#234)
                                       :  +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#234,d_date#236] Batched: true, DataFilters: [isnotnull(d_date_sk#234)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                                       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=347]
                                          +- Project [d_date#413]
                                             +- Filter (isnotnull(d_week_seq#415) AND (d_week_seq#415 = Subquery subquery#14, [id=#262]))
                                                :  +- Subquery subquery#14, [id=#262]
                                                :     +- AdaptiveSparkPlan isFinalPlan=false
                                                :        +- Project [d_week_seq#267]
                                                :           +- Filter (isnotnull(d_date#265) AND (d_date#265 = 2000-01-03))
                                                :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#265,d_week_seq#267] Batched: true, DataFilters: [isnotnull(d_date#265), (d_date#265 = 2000-01-03)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
                                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date#413,d_week_seq#415] Batched: true, DataFilters: [isnotnull(d_week_seq#415)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_week_seq)], ReadSchema: struct<d_date:string,d_week_seq:int>

Time taken: 3.54 seconds, Fetched 1 row(s)
