Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579979751
== Parsed Logical Plan ==
CTE [cross_items, avg_sales]
:  :- 'SubqueryAlias cross_items
:  :  +- 'Project ['i_item_sk AS ss_item_sk#18]
:  :     +- 'Filter ((('i_brand_id = 'brand_id) AND ('i_class_id = 'class_id)) AND ('i_category_id = 'category_id))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation [item], [], false
:  :           +- 'SubqueryAlias __auto_generated_subquery_name
:  :              +- 'Intersect false
:  :                 :- 'Intersect false
:  :                 :  :- 'Project ['iss.i_brand_id AS brand_id#15, 'iss.i_class_id AS class_id#16, 'iss.i_category_id AS category_id#17]
:  :                 :  :  +- 'Filter ((('ss_item_sk = 'iss.i_item_sk) AND ('ss_sold_date_sk = 'd1.d_date_sk)) AND (('d1.d_year >= 1999) AND ('d1.d_year <= (1999 + 2))))
:  :                 :  :     +- 'Join Inner
:  :                 :  :        :- 'Join Inner
:  :                 :  :        :  :- 'UnresolvedRelation [store_sales], [], false
:  :                 :  :        :  +- 'SubqueryAlias iss
:  :                 :  :        :     +- 'UnresolvedRelation [item], [], false
:  :                 :  :        +- 'SubqueryAlias d1
:  :                 :  :           +- 'UnresolvedRelation [date_dim], [], false
:  :                 :  +- 'Project ['ics.i_brand_id, 'ics.i_class_id, 'ics.i_category_id]
:  :                 :     +- 'Filter ((('cs_item_sk = 'ics.i_item_sk) AND ('cs_sold_date_sk = 'd2.d_date_sk)) AND (('d2.d_year >= 1999) AND ('d2.d_year <= (1999 + 2))))
:  :                 :        +- 'Join Inner
:  :                 :           :- 'Join Inner
:  :                 :           :  :- 'UnresolvedRelation [catalog_sales], [], false
:  :                 :           :  +- 'SubqueryAlias ics
:  :                 :           :     +- 'UnresolvedRelation [item], [], false
:  :                 :           +- 'SubqueryAlias d2
:  :                 :              +- 'UnresolvedRelation [date_dim], [], false
:  :                 +- 'Project ['iws.i_brand_id, 'iws.i_class_id, 'iws.i_category_id]
:  :                    +- 'Filter ((('ws_item_sk = 'iws.i_item_sk) AND ('ws_sold_date_sk = 'd3.d_date_sk)) AND (('d3.d_year >= 1999) AND ('d3.d_year <= (1999 + 2))))
:  :                       +- 'Join Inner
:  :                          :- 'Join Inner
:  :                          :  :- 'UnresolvedRelation [web_sales], [], false
:  :                          :  +- 'SubqueryAlias iws
:  :                          :     +- 'UnresolvedRelation [item], [], false
:  :                          +- 'SubqueryAlias d3
:  :                             +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias avg_sales
:     +- 'Project ['avg(('quantity * 'list_price)) AS average_sales#25]
:        +- 'SubqueryAlias x
:           +- 'Union false, false
:              :- 'Union false, false
:              :  :- 'Project ['ss_quantity AS quantity#19, 'ss_list_price AS list_price#20]
:              :  :  +- 'Filter (('ss_sold_date_sk = 'd_date_sk) AND (('d_year >= 1999) AND ('d_year <= (1999 + 2))))
:              :  :     +- 'Join Inner
:              :  :        :- 'UnresolvedRelation [store_sales], [], false
:              :  :        +- 'UnresolvedRelation [date_dim], [], false
:              :  +- 'Project ['cs_quantity AS quantity#21, 'cs_list_price AS list_price#22]
:              :     +- 'Filter (('cs_sold_date_sk = 'd_date_sk) AND (('d_year >= 1999) AND ('d_year <= (1999 + 2))))
:              :        +- 'Join Inner
:              :           :- 'UnresolvedRelation [catalog_sales], [], false
:              :           +- 'UnresolvedRelation [date_dim], [], false
:              +- 'Project ['ws_quantity AS quantity#23, 'ws_list_price AS list_price#24]
:                 +- 'Filter (('ws_sold_date_sk = 'd_date_sk) AND (('d_year >= 1999) AND ('d_year <= (1999 + 2))))
:                    +- 'Join Inner
:                       :- 'UnresolvedRelation [web_sales], [], false
:                       +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'i_brand_id ASC NULLS FIRST, 'i_class_id ASC NULLS FIRST, 'i_category_id ASC NULLS FIRST], true
         +- 'Aggregate [rollup(Vector(0), Vector(1), Vector(2), Vector(3), 'channel, 'i_brand_id, 'i_class_id, 'i_category_id)], ['channel, 'i_brand_id, 'i_class_id, 'i_category_id, unresolvedalias('sum('sales), None), unresolvedalias('sum('number_sales), None)]
            +- 'SubqueryAlias y
               +- 'Union false, false
                  :- 'Union false, false
                  :  :- 'UnresolvedHaving ('sum(('ss_quantity * 'ss_list_price)) > scalar-subquery#4 [])
                  :  :  :  +- 'Project ['average_sales]
                  :  :  :     +- 'UnresolvedRelation [avg_sales], [], false
                  :  :  +- 'Aggregate ['i_brand_id, 'i_class_id, 'i_category_id], [store AS channel#0, 'i_brand_id, 'i_class_id, 'i_category_id, 'sum(('ss_quantity * 'ss_list_price)) AS sales#1, 'count(1) AS number_sales#2]
                  :  :     +- 'Filter ((('ss_item_sk IN (list#3 []) AND ('ss_item_sk = 'i_item_sk)) AND ('ss_sold_date_sk = 'd_date_sk)) AND (('d_year = (1999 + 2)) AND ('d_moy = 11)))
                  :  :        :  +- 'Project ['ss_item_sk]
                  :  :        :     +- 'UnresolvedRelation [cross_items], [], false
                  :  :        +- 'Join Inner
                  :  :           :- 'Join Inner
                  :  :           :  :- 'UnresolvedRelation [store_sales], [], false
                  :  :           :  +- 'UnresolvedRelation [item], [], false
                  :  :           +- 'UnresolvedRelation [date_dim], [], false
                  :  +- 'UnresolvedHaving ('sum(('cs_quantity * 'cs_list_price)) > scalar-subquery#9 [])
                  :     :  +- 'Project ['average_sales]
                  :     :     +- 'UnresolvedRelation [avg_sales], [], false
                  :     +- 'Aggregate ['i_brand_id, 'i_class_id, 'i_category_id], [catalog AS channel#5, 'i_brand_id, 'i_class_id, 'i_category_id, 'sum(('cs_quantity * 'cs_list_price)) AS sales#6, 'count(1) AS number_sales#7]
                  :        +- 'Filter ((('cs_item_sk IN (list#8 []) AND ('cs_item_sk = 'i_item_sk)) AND ('cs_sold_date_sk = 'd_date_sk)) AND (('d_year = (1999 + 2)) AND ('d_moy = 11)))
                  :           :  +- 'Project ['ss_item_sk]
                  :           :     +- 'UnresolvedRelation [cross_items], [], false
                  :           +- 'Join Inner
                  :              :- 'Join Inner
                  :              :  :- 'UnresolvedRelation [catalog_sales], [], false
                  :              :  +- 'UnresolvedRelation [item], [], false
                  :              +- 'UnresolvedRelation [date_dim], [], false
                  +- 'UnresolvedHaving ('sum(('ws_quantity * 'ws_list_price)) > scalar-subquery#14 [])
                     :  +- 'Project ['average_sales]
                     :     +- 'UnresolvedRelation [avg_sales], [], false
                     +- 'Aggregate ['i_brand_id, 'i_class_id, 'i_category_id], [web AS channel#10, 'i_brand_id, 'i_class_id, 'i_category_id, 'sum(('ws_quantity * 'ws_list_price)) AS sales#11, 'count(1) AS number_sales#12]
                        +- 'Filter ((('ws_item_sk IN (list#13 []) AND ('ws_item_sk = 'i_item_sk)) AND ('ws_sold_date_sk = 'd_date_sk)) AND (('d_year = (1999 + 2)) AND ('d_moy = 11)))
                           :  +- 'Project ['ss_item_sk]
                           :     +- 'UnresolvedRelation [cross_items], [], false
                           +- 'Join Inner
                              :- 'Join Inner
                              :  :- 'UnresolvedRelation [web_sales], [], false
                              :  +- 'UnresolvedRelation [item], [], false
                              +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
channel: string, i_brand_id: int, i_class_id: int, i_category_id: int, sum(sales): double, sum(number_sales): bigint
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias cross_items
:     +- Project [i_item_sk#34 AS ss_item_sk#18]
:        +- Filter (((i_brand_id#41 = brand_id#15) AND (i_class_id#43 = class_id#16)) AND (i_category_id#45 = category_id#17))
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.item
:              :  +- Relation spark_catalog.tpcds.item[i_item_sk#34,i_item_id#35,i_rec_start_date#36,i_rec_end_date#37,i_item_desc#38,i_current_price#39,i_wholesale_cost#40,i_brand_id#41,i_brand#42,i_class_id#43,i_class#44,i_category_id#45,i_category#46,i_manufact_id#47,i_manufact#48,i_size#49,i_formulation#50,i_color#51,i_units#52,i_container#53,i_manager_id#54,i_product_name#55] parquet
:              +- SubqueryAlias __auto_generated_subquery_name
:                 +- Intersect false
:                    :- Intersect false
:                    :  :- Project [i_brand_id#182 AS brand_id#15, i_class_id#184 AS class_id#16, i_category_id#186 AS category_id#17]
:                    :  :  +- Filter (((ss_item_sk#58 = i_item_sk#175) AND (ss_sold_date_sk#56 = d_date_sk#79)) AND ((d_year#85 >= 1999) AND (d_year#85 <= (1999 + 2))))
:                    :  :     +- Join Inner
:                    :  :        :- Join Inner
:                    :  :        :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:                    :  :        :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#56,ss_sold_time_sk#57,ss_item_sk#58,ss_customer_sk#59,ss_cdemo_sk#60,ss_hdemo_sk#61,ss_addr_sk#62,ss_store_sk#63,ss_promo_sk#64,ss_ticket_number#65,ss_quantity#66,ss_wholesale_cost#67,ss_list_price#68,ss_sales_price#69,ss_ext_discount_amt#70,ss_ext_sales_price#71,ss_ext_wholesale_cost#72,ss_ext_list_price#73,ss_ext_tax#74,ss_coupon_amt#75,ss_net_paid#76,ss_net_paid_inc_tax#77,ss_net_profit#78] parquet
:                    :  :        :  +- SubqueryAlias iss
:                    :  :        :     +- SubqueryAlias spark_catalog.tpcds.item
:                    :  :        :        +- Relation spark_catalog.tpcds.item[i_item_sk#175,i_item_id#176,i_rec_start_date#177,i_rec_end_date#178,i_item_desc#179,i_current_price#180,i_wholesale_cost#181,i_brand_id#182,i_brand#183,i_class_id#184,i_class#185,i_category_id#186,i_category#187,i_manufact_id#188,i_manufact#189,i_size#190,i_formulation#191,i_color#192,i_units#193,i_container#194,i_manager_id#195,i_product_name#196] parquet
:                    :  :        +- SubqueryAlias d1
:                    :  :           +- SubqueryAlias spark_catalog.tpcds.date_dim
:                    :  :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet
:                    :  +- Project [i_brand_id#204, i_class_id#206, i_category_id#208]
:                    :     +- Filter (((cs_item_sk#122 = i_item_sk#197) AND (cs_sold_date_sk#107 = d_date_sk#219)) AND ((d_year#225 >= 1999) AND (d_year#225 <= (1999 + 2))))
:                    :        +- Join Inner
:                    :           :- Join Inner
:                    :           :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:                    :           :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#107,cs_sold_time_sk#108,cs_ship_date_sk#109,cs_bill_customer_sk#110,cs_bill_cdemo_sk#111,cs_bill_hdemo_sk#112,cs_bill_addr_sk#113,cs_ship_customer_sk#114,cs_ship_cdemo_sk#115,cs_ship_hdemo_sk#116,cs_ship_addr_sk#117,cs_call_center_sk#118,cs_catalog_page_sk#119,cs_ship_mode_sk#120,cs_warehouse_sk#121,cs_item_sk#122,cs_promo_sk#123,cs_order_number#124,cs_quantity#125,cs_wholesale_cost#126,cs_list_price#127,cs_sales_price#128,cs_ext_discount_amt#129,cs_ext_sales_price#130,... 10 more fields] parquet
:                    :           :  +- SubqueryAlias ics
:                    :           :     +- SubqueryAlias spark_catalog.tpcds.item
:                    :           :        +- Relation spark_catalog.tpcds.item[i_item_sk#197,i_item_id#198,i_rec_start_date#199,i_rec_end_date#200,i_item_desc#201,i_current_price#202,i_wholesale_cost#203,i_brand_id#204,i_brand#205,i_class_id#206,i_class#207,i_category_id#208,i_category#209,i_manufact_id#210,i_manufact#211,i_size#212,i_formulation#213,i_color#214,i_units#215,i_container#216,i_manager_id#217,i_product_name#218] parquet
:                    :           +- SubqueryAlias d2
:                    :              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                    :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#219,d_date_id#220,d_date#221,d_month_seq#222,d_week_seq#223,d_quarter_seq#224,d_year#225,d_dow#226,d_moy#227,d_dom#228,d_qoy#229,d_fy_year#230,d_fy_quarter_seq#231,d_fy_week_seq#232,d_day_name#233,d_quarter_name#234,d_holiday#235,d_weekend#236,d_following_holiday#237,d_first_dom#238,d_last_dom#239,d_same_day_ly#240,d_same_day_lq#241,d_current_day#242,... 4 more fields] parquet
:                    +- Project [i_brand_id#254, i_class_id#256, i_category_id#258]
:                       +- Filter (((ws_item_sk#144 = i_item_sk#247) AND (ws_sold_date_sk#141 = d_date_sk#269)) AND ((d_year#275 >= 1999) AND (d_year#275 <= (1999 + 2))))
:                          +- Join Inner
:                             :- Join Inner
:                             :  :- SubqueryAlias spark_catalog.tpcds.web_sales
:                             :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#141,ws_sold_time_sk#142,ws_ship_date_sk#143,ws_item_sk#144,ws_bill_customer_sk#145,ws_bill_cdemo_sk#146,ws_bill_hdemo_sk#147,ws_bill_addr_sk#148,ws_ship_customer_sk#149,ws_ship_cdemo_sk#150,ws_ship_hdemo_sk#151,ws_ship_addr_sk#152,ws_web_page_sk#153,ws_web_site_sk#154,ws_ship_mode_sk#155,ws_warehouse_sk#156,ws_promo_sk#157,ws_order_number#158,ws_quantity#159,ws_wholesale_cost#160,ws_list_price#161,ws_sales_price#162,ws_ext_discount_amt#163,ws_ext_sales_price#164,... 10 more fields] parquet
:                             :  +- SubqueryAlias iws
:                             :     +- SubqueryAlias spark_catalog.tpcds.item
:                             :        +- Relation spark_catalog.tpcds.item[i_item_sk#247,i_item_id#248,i_rec_start_date#249,i_rec_end_date#250,i_item_desc#251,i_current_price#252,i_wholesale_cost#253,i_brand_id#254,i_brand#255,i_class_id#256,i_class#257,i_category_id#258,i_category#259,i_manufact_id#260,i_manufact#261,i_size#262,i_formulation#263,i_color#264,i_units#265,i_container#266,i_manager_id#267,i_product_name#268] parquet
:                             +- SubqueryAlias d3
:                                +- SubqueryAlias spark_catalog.tpcds.date_dim
:                                   +- Relation spark_catalog.tpcds.date_dim[d_date_sk#269,d_date_id#270,d_date#271,d_month_seq#272,d_week_seq#273,d_quarter_seq#274,d_year#275,d_dow#276,d_moy#277,d_dom#278,d_qoy#279,d_fy_year#280,d_fy_quarter_seq#281,d_fy_week_seq#282,d_day_name#283,d_quarter_name#284,d_holiday#285,d_weekend#286,d_following_holiday#287,d_first_dom#288,d_last_dom#289,d_same_day_ly#290,d_same_day_lq#291,d_current_day#292,... 4 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias avg_sales
:     +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
:        +- SubqueryAlias x
:           +- Union false, false
:              :- Union false, false
:              :  :- Project [ss_quantity#307 AS quantity#19, ss_list_price#309 AS list_price#20]
:              :  :  +- Filter ((ss_sold_date_sk#297 = d_date_sk#320) AND ((d_year#326 >= 1999) AND (d_year#326 <= (1999 + 2))))
:              :  :     +- Join Inner
:              :  :        :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :        :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#297,ss_sold_time_sk#298,ss_item_sk#299,ss_customer_sk#300,ss_cdemo_sk#301,ss_hdemo_sk#302,ss_addr_sk#303,ss_store_sk#304,ss_promo_sk#305,ss_ticket_number#306,ss_quantity#307,ss_wholesale_cost#308,ss_list_price#309,ss_sales_price#310,ss_ext_discount_amt#311,ss_ext_sales_price#312,ss_ext_wholesale_cost#313,ss_ext_list_price#314,ss_ext_tax#315,ss_coupon_amt#316,ss_net_paid#317,ss_net_paid_inc_tax#318,ss_net_profit#319] parquet
:              :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#320,d_date_id#321,d_date#322,d_month_seq#323,d_week_seq#324,d_quarter_seq#325,d_year#326,d_dow#327,d_moy#328,d_dom#329,d_qoy#330,d_fy_year#331,d_fy_quarter_seq#332,d_fy_week_seq#333,d_day_name#334,d_quarter_name#335,d_holiday#336,d_weekend#337,d_following_holiday#338,d_first_dom#339,d_last_dom#340,d_same_day_ly#341,d_same_day_lq#342,d_current_day#343,... 4 more fields] parquet
:              :  +- Project [cs_quantity#366 AS quantity#21, cs_list_price#368 AS list_price#22]
:              :     +- Filter ((cs_sold_date_sk#348 = d_date_sk#382) AND ((d_year#388 >= 1999) AND (d_year#388 <= (1999 + 2))))
:              :        +- Join Inner
:              :           :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :           :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_sold_time_sk#349,cs_ship_date_sk#350,cs_bill_customer_sk#351,cs_bill_cdemo_sk#352,cs_bill_hdemo_sk#353,cs_bill_addr_sk#354,cs_ship_customer_sk#355,cs_ship_cdemo_sk#356,cs_ship_hdemo_sk#357,cs_ship_addr_sk#358,cs_call_center_sk#359,cs_catalog_page_sk#360,cs_ship_mode_sk#361,cs_warehouse_sk#362,cs_item_sk#363,cs_promo_sk#364,cs_order_number#365,cs_quantity#366,cs_wholesale_cost#367,cs_list_price#368,cs_sales_price#369,cs_ext_discount_amt#370,cs_ext_sales_price#371,... 10 more fields] parquet
:              :           +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#382,d_date_id#383,d_date#384,d_month_seq#385,d_week_seq#386,d_quarter_seq#387,d_year#388,d_dow#389,d_moy#390,d_dom#391,d_qoy#392,d_fy_year#393,d_fy_quarter_seq#394,d_fy_week_seq#395,d_day_name#396,d_quarter_name#397,d_holiday#398,d_weekend#399,d_following_holiday#400,d_first_dom#401,d_last_dom#402,d_same_day_ly#403,d_same_day_lq#404,d_current_day#405,... 4 more fields] parquet
:              +- Project [ws_quantity#428 AS quantity#23, ws_list_price#430 AS list_price#24]
:                 +- Filter ((ws_sold_date_sk#410 = d_date_sk#444) AND ((d_year#450 >= 1999) AND (d_year#450 <= (1999 + 2))))
:                    +- Join Inner
:                       :- SubqueryAlias spark_catalog.tpcds.web_sales
:                       :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#410,ws_sold_time_sk#411,ws_ship_date_sk#412,ws_item_sk#413,ws_bill_customer_sk#414,ws_bill_cdemo_sk#415,ws_bill_hdemo_sk#416,ws_bill_addr_sk#417,ws_ship_customer_sk#418,ws_ship_cdemo_sk#419,ws_ship_hdemo_sk#420,ws_ship_addr_sk#421,ws_web_page_sk#422,ws_web_site_sk#423,ws_ship_mode_sk#424,ws_warehouse_sk#425,ws_promo_sk#426,ws_order_number#427,ws_quantity#428,ws_wholesale_cost#429,ws_list_price#430,ws_sales_price#431,ws_ext_discount_amt#432,ws_ext_sales_price#433,... 10 more fields] parquet
:                       +- SubqueryAlias spark_catalog.tpcds.date_dim
:                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#444,d_date_id#445,d_date#446,d_month_seq#447,d_week_seq#448,d_quarter_seq#449,d_year#450,d_dow#451,d_moy#452,d_dom#453,d_qoy#454,d_fy_year#455,d_fy_quarter_seq#456,d_fy_week_seq#457,d_day_name#458,d_quarter_name#459,d_holiday#460,d_weekend#461,d_following_holiday#462,d_first_dom#463,d_last_dom#464,d_same_day_ly#465,d_same_day_lq#466,d_current_day#467,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [channel#758 ASC NULLS FIRST, i_brand_id#759 ASC NULLS FIRST, i_class_id#760 ASC NULLS FIRST, i_category_id#761 ASC NULLS FIRST], true
         +- Aggregate [channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L], [channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, sum(sales#1) AS sum(sales)#751, sum(number_sales#2L) AS sum(number_sales)#752L]
            +- Expand [[channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L, channel#753, i_brand_id#754, i_class_id#755, i_category_id#756, 0], [channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L, channel#753, i_brand_id#754, i_class_id#755, null, 1], [channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L, channel#753, i_brand_id#754, null, null, 3], [channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L, channel#753, null, null, null, 7], [channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L, null, null, null, null, 15]], [channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L, channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L]
               +- Project [channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L, channel#0 AS channel#753, i_brand_id#502 AS i_brand_id#754, i_class_id#504 AS i_class_id#755, i_category_id#506 AS i_category_id#756]
                  +- SubqueryAlias y
                     +- Union false, false
                        :- Union false, false
                        :  :- Filter (sales#1 > scalar-subquery#4 [])
                        :  :  :  +- Project [average_sales#25]
                        :  :  :     +- SubqueryAlias avg_sales
                        :  :  :        +- CTERelationRef 1, true, [average_sales#25], false
                        :  :  +- Aggregate [i_brand_id#502, i_class_id#504, i_category_id#506], [store AS channel#0, i_brand_id#502, i_class_id#504, i_category_id#506, sum((cast(ss_quantity#482 as double) * ss_list_price#484)) AS sales#1, count(1) AS number_sales#2L]
                        :  :     +- Filter (((ss_item_sk#474 IN (list#3 []) AND (ss_item_sk#474 = i_item_sk#495)) AND (ss_sold_date_sk#472 = d_date_sk#517)) AND ((d_year#523 = (1999 + 2)) AND (d_moy#525 = 11)))
                        :  :        :  +- Project [ss_item_sk#18]
                        :  :        :     +- SubqueryAlias cross_items
                        :  :        :        +- CTERelationRef 0, true, [ss_item_sk#18], false
                        :  :        +- Join Inner
                        :  :           :- Join Inner
                        :  :           :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                        :  :           :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#472,ss_sold_time_sk#473,ss_item_sk#474,ss_customer_sk#475,ss_cdemo_sk#476,ss_hdemo_sk#477,ss_addr_sk#478,ss_store_sk#479,ss_promo_sk#480,ss_ticket_number#481,ss_quantity#482,ss_wholesale_cost#483,ss_list_price#484,ss_sales_price#485,ss_ext_discount_amt#486,ss_ext_sales_price#487,ss_ext_wholesale_cost#488,ss_ext_list_price#489,ss_ext_tax#490,ss_coupon_amt#491,ss_net_paid#492,ss_net_paid_inc_tax#493,ss_net_profit#494] parquet
                        :  :           :  +- SubqueryAlias spark_catalog.tpcds.item
                        :  :           :     +- Relation spark_catalog.tpcds.item[i_item_sk#495,i_item_id#496,i_rec_start_date#497,i_rec_end_date#498,i_item_desc#499,i_current_price#500,i_wholesale_cost#501,i_brand_id#502,i_brand#503,i_class_id#504,i_class#505,i_category_id#506,i_category#507,i_manufact_id#508,i_manufact#509,i_size#510,i_formulation#511,i_color#512,i_units#513,i_container#514,i_manager_id#515,i_product_name#516] parquet
                        :  :           +- SubqueryAlias spark_catalog.tpcds.date_dim
                        :  :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#517,d_date_id#518,d_date#519,d_month_seq#520,d_week_seq#521,d_quarter_seq#522,d_year#523,d_dow#524,d_moy#525,d_dom#526,d_qoy#527,d_fy_year#528,d_fy_quarter_seq#529,d_fy_week_seq#530,d_day_name#531,d_quarter_name#532,d_holiday#533,d_weekend#534,d_following_holiday#535,d_first_dom#536,d_last_dom#537,d_same_day_ly#538,d_same_day_lq#539,d_current_day#540,... 4 more fields] parquet
                        :  +- Filter (sales#6 > scalar-subquery#9 [])
                        :     :  +- Project [average_sales#741]
                        :     :     +- SubqueryAlias avg_sales
                        :     :        +- CTERelationRef 1, true, [average_sales#741], false
                        :     +- Aggregate [i_brand_id#586, i_class_id#588, i_category_id#590], [catalog AS channel#5, i_brand_id#586, i_class_id#588, i_category_id#590, sum((cast(cs_quantity#563 as double) * cs_list_price#565)) AS sales#6, count(1) AS number_sales#7L]
                        :        +- Filter (((cs_item_sk#560 IN (list#8 []) AND (cs_item_sk#560 = i_item_sk#579)) AND (cs_sold_date_sk#545 = d_date_sk#601)) AND ((d_year#607 = (1999 + 2)) AND (d_moy#609 = 11)))
                        :           :  +- Project [ss_item_sk#729]
                        :           :     +- SubqueryAlias cross_items
                        :           :        +- CTERelationRef 0, true, [ss_item_sk#729], false
                        :           +- Join Inner
                        :              :- Join Inner
                        :              :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
                        :              :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#545,cs_sold_time_sk#546,cs_ship_date_sk#547,cs_bill_customer_sk#548,cs_bill_cdemo_sk#549,cs_bill_hdemo_sk#550,cs_bill_addr_sk#551,cs_ship_customer_sk#552,cs_ship_cdemo_sk#553,cs_ship_hdemo_sk#554,cs_ship_addr_sk#555,cs_call_center_sk#556,cs_catalog_page_sk#557,cs_ship_mode_sk#558,cs_warehouse_sk#559,cs_item_sk#560,cs_promo_sk#561,cs_order_number#562,cs_quantity#563,cs_wholesale_cost#564,cs_list_price#565,cs_sales_price#566,cs_ext_discount_amt#567,cs_ext_sales_price#568,... 10 more fields] parquet
                        :              :  +- SubqueryAlias spark_catalog.tpcds.item
                        :              :     +- Relation spark_catalog.tpcds.item[i_item_sk#579,i_item_id#580,i_rec_start_date#581,i_rec_end_date#582,i_item_desc#583,i_current_price#584,i_wholesale_cost#585,i_brand_id#586,i_brand#587,i_class_id#588,i_class#589,i_category_id#590,i_category#591,i_manufact_id#592,i_manufact#593,i_size#594,i_formulation#595,i_color#596,i_units#597,i_container#598,i_manager_id#599,i_product_name#600] parquet
                        :              +- SubqueryAlias spark_catalog.tpcds.date_dim
                        :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#601,d_date_id#602,d_date#603,d_month_seq#604,d_week_seq#605,d_quarter_seq#606,d_year#607,d_dow#608,d_moy#609,d_dom#610,d_qoy#611,d_fy_year#612,d_fy_quarter_seq#613,d_fy_week_seq#614,d_day_name#615,d_quarter_name#616,d_holiday#617,d_weekend#618,d_following_holiday#619,d_first_dom#620,d_last_dom#621,d_same_day_ly#622,d_same_day_lq#623,d_current_day#624,... 4 more fields] parquet
                        +- Filter (sales#11 > scalar-subquery#14 [])
                           :  +- Project [average_sales#742]
                           :     +- SubqueryAlias avg_sales
                           :        +- CTERelationRef 1, true, [average_sales#742], false
                           +- Aggregate [i_brand_id#670, i_class_id#672, i_category_id#674], [web AS channel#10, i_brand_id#670, i_class_id#672, i_category_id#674, sum((cast(ws_quantity#647 as double) * ws_list_price#649)) AS sales#11, count(1) AS number_sales#12L]
                              +- Filter (((ws_item_sk#632 IN (list#13 []) AND (ws_item_sk#632 = i_item_sk#663)) AND (ws_sold_date_sk#629 = d_date_sk#685)) AND ((d_year#691 = (1999 + 2)) AND (d_moy#693 = 11)))
                                 :  +- Project [ss_item_sk#730]
                                 :     +- SubqueryAlias cross_items
                                 :        +- CTERelationRef 0, true, [ss_item_sk#730], false
                                 +- Join Inner
                                    :- Join Inner
                                    :  :- SubqueryAlias spark_catalog.tpcds.web_sales
                                    :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#629,ws_sold_time_sk#630,ws_ship_date_sk#631,ws_item_sk#632,ws_bill_customer_sk#633,ws_bill_cdemo_sk#634,ws_bill_hdemo_sk#635,ws_bill_addr_sk#636,ws_ship_customer_sk#637,ws_ship_cdemo_sk#638,ws_ship_hdemo_sk#639,ws_ship_addr_sk#640,ws_web_page_sk#641,ws_web_site_sk#642,ws_ship_mode_sk#643,ws_warehouse_sk#644,ws_promo_sk#645,ws_order_number#646,ws_quantity#647,ws_wholesale_cost#648,ws_list_price#649,ws_sales_price#650,ws_ext_discount_amt#651,ws_ext_sales_price#652,... 10 more fields] parquet
                                    :  +- SubqueryAlias spark_catalog.tpcds.item
                                    :     +- Relation spark_catalog.tpcds.item[i_item_sk#663,i_item_id#664,i_rec_start_date#665,i_rec_end_date#666,i_item_desc#667,i_current_price#668,i_wholesale_cost#669,i_brand_id#670,i_brand#671,i_class_id#672,i_class#673,i_category_id#674,i_category#675,i_manufact_id#676,i_manufact#677,i_size#678,i_formulation#679,i_color#680,i_units#681,i_container#682,i_manager_id#683,i_product_name#684] parquet
                                    +- SubqueryAlias spark_catalog.tpcds.date_dim
                                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#685,d_date_id#686,d_date#687,d_month_seq#688,d_week_seq#689,d_quarter_seq#690,d_year#691,d_dow#692,d_moy#693,d_dom#694,d_qoy#695,d_fy_year#696,d_fy_quarter_seq#697,d_fy_week_seq#698,d_day_name#699,d_quarter_name#700,d_holiday#701,d_weekend#702,d_following_holiday#703,d_first_dom#704,d_last_dom#705,d_same_day_ly#706,d_same_day_lq#707,d_current_day#708,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#758 ASC NULLS FIRST, i_brand_id#759 ASC NULLS FIRST, i_class_id#760 ASC NULLS FIRST, i_category_id#761 ASC NULLS FIRST], true
      +- Aggregate [channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L], [channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, sum(sales#1) AS sum(sales)#751, sum(number_sales#2L) AS sum(number_sales)#752L]
         +- Expand [[sales#1, number_sales#2L, channel#753, i_brand_id#754, i_class_id#755, i_category_id#756, 0], [sales#1, number_sales#2L, channel#753, i_brand_id#754, i_class_id#755, null, 1], [sales#1, number_sales#2L, channel#753, i_brand_id#754, null, null, 3], [sales#1, number_sales#2L, channel#753, null, null, null, 7], [sales#1, number_sales#2L, null, null, null, null, 15]], [sales#1, number_sales#2L, channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L]
            +- Union false, false
               :- Project [sales#1, number_sales#2L, store AS channel#753, i_brand_id#502 AS i_brand_id#754, i_class_id#504 AS i_class_id#755, i_category_id#506 AS i_category_id#756]
               :  +- Filter (isnotnull(sales#1) AND (sales#1 > scalar-subquery#4 []))
               :     :  +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
               :     :     +- Union false, false
               :     :        :- Project [ss_quantity#307 AS quantity#19, ss_list_price#309 AS list_price#20]
               :     :        :  +- Join Inner, (ss_sold_date_sk#297 = d_date_sk#320)
               :     :        :     :- Project [ss_sold_date_sk#297, ss_quantity#307, ss_list_price#309]
               :     :        :     :  +- Filter isnotnull(ss_sold_date_sk#297)
               :     :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#297,ss_sold_time_sk#298,ss_item_sk#299,ss_customer_sk#300,ss_cdemo_sk#301,ss_hdemo_sk#302,ss_addr_sk#303,ss_store_sk#304,ss_promo_sk#305,ss_ticket_number#306,ss_quantity#307,ss_wholesale_cost#308,ss_list_price#309,ss_sales_price#310,ss_ext_discount_amt#311,ss_ext_sales_price#312,ss_ext_wholesale_cost#313,ss_ext_list_price#314,ss_ext_tax#315,ss_coupon_amt#316,ss_net_paid#317,ss_net_paid_inc_tax#318,ss_net_profit#319] parquet
               :     :        :     +- Project [d_date_sk#320]
               :     :        :        +- Filter ((isnotnull(d_year#326) AND ((d_year#326 >= 1999) AND (d_year#326 <= 2001))) AND isnotnull(d_date_sk#320))
               :     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#320,d_date_id#321,d_date#322,d_month_seq#323,d_week_seq#324,d_quarter_seq#325,d_year#326,d_dow#327,d_moy#328,d_dom#329,d_qoy#330,d_fy_year#331,d_fy_quarter_seq#332,d_fy_week_seq#333,d_day_name#334,d_quarter_name#335,d_holiday#336,d_weekend#337,d_following_holiday#338,d_first_dom#339,d_last_dom#340,d_same_day_ly#341,d_same_day_lq#342,d_current_day#343,... 4 more fields] parquet
               :     :        :- Project [cs_quantity#366 AS quantity#21, cs_list_price#368 AS list_price#22]
               :     :        :  +- Join Inner, (cs_sold_date_sk#348 = d_date_sk#382)
               :     :        :     :- Project [cs_sold_date_sk#348, cs_quantity#366, cs_list_price#368]
               :     :        :     :  +- Filter isnotnull(cs_sold_date_sk#348)
               :     :        :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_sold_time_sk#349,cs_ship_date_sk#350,cs_bill_customer_sk#351,cs_bill_cdemo_sk#352,cs_bill_hdemo_sk#353,cs_bill_addr_sk#354,cs_ship_customer_sk#355,cs_ship_cdemo_sk#356,cs_ship_hdemo_sk#357,cs_ship_addr_sk#358,cs_call_center_sk#359,cs_catalog_page_sk#360,cs_ship_mode_sk#361,cs_warehouse_sk#362,cs_item_sk#363,cs_promo_sk#364,cs_order_number#365,cs_quantity#366,cs_wholesale_cost#367,cs_list_price#368,cs_sales_price#369,cs_ext_discount_amt#370,cs_ext_sales_price#371,... 10 more fields] parquet
               :     :        :     +- Project [d_date_sk#382]
               :     :        :        +- Filter ((isnotnull(d_year#388) AND ((d_year#388 >= 1999) AND (d_year#388 <= 2001))) AND isnotnull(d_date_sk#382))
               :     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#382,d_date_id#383,d_date#384,d_month_seq#385,d_week_seq#386,d_quarter_seq#387,d_year#388,d_dow#389,d_moy#390,d_dom#391,d_qoy#392,d_fy_year#393,d_fy_quarter_seq#394,d_fy_week_seq#395,d_day_name#396,d_quarter_name#397,d_holiday#398,d_weekend#399,d_following_holiday#400,d_first_dom#401,d_last_dom#402,d_same_day_ly#403,d_same_day_lq#404,d_current_day#405,... 4 more fields] parquet
               :     :        +- Project [ws_quantity#428 AS quantity#23, ws_list_price#430 AS list_price#24]
               :     :           +- Join Inner, (ws_sold_date_sk#410 = d_date_sk#444)
               :     :              :- Project [ws_sold_date_sk#410, ws_quantity#428, ws_list_price#430]
               :     :              :  +- Filter isnotnull(ws_sold_date_sk#410)
               :     :              :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#410,ws_sold_time_sk#411,ws_ship_date_sk#412,ws_item_sk#413,ws_bill_customer_sk#414,ws_bill_cdemo_sk#415,ws_bill_hdemo_sk#416,ws_bill_addr_sk#417,ws_ship_customer_sk#418,ws_ship_cdemo_sk#419,ws_ship_hdemo_sk#420,ws_ship_addr_sk#421,ws_web_page_sk#422,ws_web_site_sk#423,ws_ship_mode_sk#424,ws_warehouse_sk#425,ws_promo_sk#426,ws_order_number#427,ws_quantity#428,ws_wholesale_cost#429,ws_list_price#430,ws_sales_price#431,ws_ext_discount_amt#432,ws_ext_sales_price#433,... 10 more fields] parquet
               :     :              +- Project [d_date_sk#444]
               :     :                 +- Filter ((isnotnull(d_year#450) AND ((d_year#450 >= 1999) AND (d_year#450 <= 2001))) AND isnotnull(d_date_sk#444))
               :     :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#444,d_date_id#445,d_date#446,d_month_seq#447,d_week_seq#448,d_quarter_seq#449,d_year#450,d_dow#451,d_moy#452,d_dom#453,d_qoy#454,d_fy_year#455,d_fy_quarter_seq#456,d_fy_week_seq#457,d_day_name#458,d_quarter_name#459,d_holiday#460,d_weekend#461,d_following_holiday#462,d_first_dom#463,d_last_dom#464,d_same_day_ly#465,d_same_day_lq#466,d_current_day#467,... 4 more fields] parquet
               :     +- Aggregate [i_brand_id#502, i_class_id#504, i_category_id#506], [i_brand_id#502, i_class_id#504, i_category_id#506, sum((cast(ss_quantity#482 as double) * ss_list_price#484)) AS sales#1, count(1) AS number_sales#2L]
               :        +- Project [ss_quantity#482, ss_list_price#484, i_brand_id#502, i_class_id#504, i_category_id#506]
               :           +- Join Inner, (ss_sold_date_sk#472 = d_date_sk#517)
               :              :- Project [ss_sold_date_sk#472, ss_quantity#482, ss_list_price#484, i_brand_id#502, i_class_id#504, i_category_id#506]
               :              :  +- Join Inner, (ss_item_sk#474 = i_item_sk#495)
               :              :     :- Join LeftSemi, (ss_item_sk#474 = ss_item_sk#18)
               :              :     :  :- Project [ss_sold_date_sk#472, ss_item_sk#474, ss_quantity#482, ss_list_price#484]
               :              :     :  :  +- Filter (isnotnull(ss_item_sk#474) AND isnotnull(ss_sold_date_sk#472))
               :              :     :  :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#472,ss_sold_time_sk#473,ss_item_sk#474,ss_customer_sk#475,ss_cdemo_sk#476,ss_hdemo_sk#477,ss_addr_sk#478,ss_store_sk#479,ss_promo_sk#480,ss_ticket_number#481,ss_quantity#482,ss_wholesale_cost#483,ss_list_price#484,ss_sales_price#485,ss_ext_discount_amt#486,ss_ext_sales_price#487,ss_ext_wholesale_cost#488,ss_ext_list_price#489,ss_ext_tax#490,ss_coupon_amt#491,ss_net_paid#492,ss_net_paid_inc_tax#493,ss_net_profit#494] parquet
               :              :     :  +- Project [i_item_sk#34 AS ss_item_sk#18]
               :              :     :     +- Join Inner, (((i_brand_id#41 = brand_id#15) AND (i_class_id#43 = class_id#16)) AND (i_category_id#45 = category_id#17))
               :              :     :        :- Project [i_item_sk#34, i_brand_id#41, i_class_id#43, i_category_id#45]
               :              :     :        :  +- Filter ((isnotnull(i_brand_id#41) AND isnotnull(i_class_id#43)) AND isnotnull(i_category_id#45))
               :              :     :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#34,i_item_id#35,i_rec_start_date#36,i_rec_end_date#37,i_item_desc#38,i_current_price#39,i_wholesale_cost#40,i_brand_id#41,i_brand#42,i_class_id#43,i_class#44,i_category_id#45,i_category#46,i_manufact_id#47,i_manufact#48,i_size#49,i_formulation#50,i_color#51,i_units#52,i_container#53,i_manager_id#54,i_product_name#55] parquet
               :              :     :        +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#254) AND (class_id#16 <=> i_class_id#256)) AND (category_id#17 <=> i_category_id#258))
               :              :     :           :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :     :           :  +- Project [i_brand_id#182 AS brand_id#15, i_class_id#184 AS class_id#16, i_category_id#186 AS category_id#17]
               :              :     :           :     +- Join Inner, (ss_sold_date_sk#56 = d_date_sk#79)
               :              :     :           :        :- Project [ss_sold_date_sk#56, i_brand_id#182, i_class_id#184, i_category_id#186]
               :              :     :           :        :  +- Join Inner, (ss_item_sk#58 = i_item_sk#175)
               :              :     :           :        :     :- Project [ss_sold_date_sk#56, ss_item_sk#58]
               :              :     :           :        :     :  +- Filter (isnotnull(ss_item_sk#58) AND isnotnull(ss_sold_date_sk#56))
               :              :     :           :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#56,ss_sold_time_sk#57,ss_item_sk#58,ss_customer_sk#59,ss_cdemo_sk#60,ss_hdemo_sk#61,ss_addr_sk#62,ss_store_sk#63,ss_promo_sk#64,ss_ticket_number#65,ss_quantity#66,ss_wholesale_cost#67,ss_list_price#68,ss_sales_price#69,ss_ext_discount_amt#70,ss_ext_sales_price#71,ss_ext_wholesale_cost#72,ss_ext_list_price#73,ss_ext_tax#74,ss_coupon_amt#75,ss_net_paid#76,ss_net_paid_inc_tax#77,ss_net_profit#78] parquet
               :              :     :           :        :     +- Join LeftSemi, (((i_brand_id#182 <=> i_brand_id#204) AND (i_class_id#184 <=> i_class_id#206)) AND (i_category_id#186 <=> i_category_id#208))
               :              :     :           :        :        :- Project [i_item_sk#175, i_brand_id#182, i_class_id#184, i_category_id#186]
               :              :     :           :        :        :  +- Filter (isnotnull(i_item_sk#175) AND ((isnotnull(i_brand_id#182) AND isnotnull(i_class_id#184)) AND isnotnull(i_category_id#186)))
               :              :     :           :        :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#175,i_item_id#176,i_rec_start_date#177,i_rec_end_date#178,i_item_desc#179,i_current_price#180,i_wholesale_cost#181,i_brand_id#182,i_brand#183,i_class_id#184,i_class#185,i_category_id#186,i_category#187,i_manufact_id#188,i_manufact#189,i_size#190,i_formulation#191,i_color#192,i_units#193,i_container#194,i_manager_id#195,i_product_name#196] parquet
               :              :     :           :        :        +- Project [i_brand_id#204, i_class_id#206, i_category_id#208]
               :              :     :           :        :           +- Join Inner, (cs_sold_date_sk#107 = d_date_sk#219)
               :              :     :           :        :              :- Project [cs_sold_date_sk#107, i_brand_id#204, i_class_id#206, i_category_id#208]
               :              :     :           :        :              :  +- Join Inner, (cs_item_sk#122 = i_item_sk#197)
               :              :     :           :        :              :     :- Project [cs_sold_date_sk#107, cs_item_sk#122]
               :              :     :           :        :              :     :  +- Filter (isnotnull(cs_item_sk#122) AND isnotnull(cs_sold_date_sk#107))
               :              :     :           :        :              :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#107,cs_sold_time_sk#108,cs_ship_date_sk#109,cs_bill_customer_sk#110,cs_bill_cdemo_sk#111,cs_bill_hdemo_sk#112,cs_bill_addr_sk#113,cs_ship_customer_sk#114,cs_ship_cdemo_sk#115,cs_ship_hdemo_sk#116,cs_ship_addr_sk#117,cs_call_center_sk#118,cs_catalog_page_sk#119,cs_ship_mode_sk#120,cs_warehouse_sk#121,cs_item_sk#122,cs_promo_sk#123,cs_order_number#124,cs_quantity#125,cs_wholesale_cost#126,cs_list_price#127,cs_sales_price#128,cs_ext_discount_amt#129,cs_ext_sales_price#130,... 10 more fields] parquet
               :              :     :           :        :              :     +- Project [i_item_sk#197, i_brand_id#204, i_class_id#206, i_category_id#208]
               :              :     :           :        :              :        +- Filter isnotnull(i_item_sk#197)
               :              :     :           :        :              :           +- Relation spark_catalog.tpcds.item[i_item_sk#197,i_item_id#198,i_rec_start_date#199,i_rec_end_date#200,i_item_desc#201,i_current_price#202,i_wholesale_cost#203,i_brand_id#204,i_brand#205,i_class_id#206,i_class#207,i_category_id#208,i_category#209,i_manufact_id#210,i_manufact#211,i_size#212,i_formulation#213,i_color#214,i_units#215,i_container#216,i_manager_id#217,i_product_name#218] parquet
               :              :     :           :        :              +- Project [d_date_sk#219]
               :              :     :           :        :                 +- Filter ((isnotnull(d_year#225) AND ((d_year#225 >= 1999) AND (d_year#225 <= 2001))) AND isnotnull(d_date_sk#219))
               :              :     :           :        :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#219,d_date_id#220,d_date#221,d_month_seq#222,d_week_seq#223,d_quarter_seq#224,d_year#225,d_dow#226,d_moy#227,d_dom#228,d_qoy#229,d_fy_year#230,d_fy_quarter_seq#231,d_fy_week_seq#232,d_day_name#233,d_quarter_name#234,d_holiday#235,d_weekend#236,d_following_holiday#237,d_first_dom#238,d_last_dom#239,d_same_day_ly#240,d_same_day_lq#241,d_current_day#242,... 4 more fields] parquet
               :              :     :           :        +- Project [d_date_sk#79]
               :              :     :           :           +- Filter ((isnotnull(d_year#85) AND ((d_year#85 >= 1999) AND (d_year#85 <= 2001))) AND isnotnull(d_date_sk#79))
               :              :     :           :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet
               :              :     :           +- Project [i_brand_id#254, i_class_id#256, i_category_id#258]
               :              :     :              +- Join Inner, (ws_sold_date_sk#141 = d_date_sk#269)
               :              :     :                 :- Project [ws_sold_date_sk#141, i_brand_id#254, i_class_id#256, i_category_id#258]
               :              :     :                 :  +- Join Inner, (ws_item_sk#144 = i_item_sk#247)
               :              :     :                 :     :- Project [ws_sold_date_sk#141, ws_item_sk#144]
               :              :     :                 :     :  +- Filter (isnotnull(ws_item_sk#144) AND isnotnull(ws_sold_date_sk#141))
               :              :     :                 :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#141,ws_sold_time_sk#142,ws_ship_date_sk#143,ws_item_sk#144,ws_bill_customer_sk#145,ws_bill_cdemo_sk#146,ws_bill_hdemo_sk#147,ws_bill_addr_sk#148,ws_ship_customer_sk#149,ws_ship_cdemo_sk#150,ws_ship_hdemo_sk#151,ws_ship_addr_sk#152,ws_web_page_sk#153,ws_web_site_sk#154,ws_ship_mode_sk#155,ws_warehouse_sk#156,ws_promo_sk#157,ws_order_number#158,ws_quantity#159,ws_wholesale_cost#160,ws_list_price#161,ws_sales_price#162,ws_ext_discount_amt#163,ws_ext_sales_price#164,... 10 more fields] parquet
               :              :     :                 :     +- Project [i_item_sk#247, i_brand_id#254, i_class_id#256, i_category_id#258]
               :              :     :                 :        +- Filter isnotnull(i_item_sk#247)
               :              :     :                 :           +- Relation spark_catalog.tpcds.item[i_item_sk#247,i_item_id#248,i_rec_start_date#249,i_rec_end_date#250,i_item_desc#251,i_current_price#252,i_wholesale_cost#253,i_brand_id#254,i_brand#255,i_class_id#256,i_class#257,i_category_id#258,i_category#259,i_manufact_id#260,i_manufact#261,i_size#262,i_formulation#263,i_color#264,i_units#265,i_container#266,i_manager_id#267,i_product_name#268] parquet
               :              :     :                 +- Project [d_date_sk#269]
               :              :     :                    +- Filter ((isnotnull(d_year#275) AND ((d_year#275 >= 1999) AND (d_year#275 <= 2001))) AND isnotnull(d_date_sk#269))
               :              :     :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#269,d_date_id#270,d_date#271,d_month_seq#272,d_week_seq#273,d_quarter_seq#274,d_year#275,d_dow#276,d_moy#277,d_dom#278,d_qoy#279,d_fy_year#280,d_fy_quarter_seq#281,d_fy_week_seq#282,d_day_name#283,d_quarter_name#284,d_holiday#285,d_weekend#286,d_following_holiday#287,d_first_dom#288,d_last_dom#289,d_same_day_ly#290,d_same_day_lq#291,d_current_day#292,... 4 more fields] parquet
               :              :     +- Join LeftSemi, (i_item_sk#495 = ss_item_sk#18)
               :              :        :- Project [i_item_sk#495, i_brand_id#502, i_class_id#504, i_category_id#506]
               :              :        :  +- Filter isnotnull(i_item_sk#495)
               :              :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#495,i_item_id#496,i_rec_start_date#497,i_rec_end_date#498,i_item_desc#499,i_current_price#500,i_wholesale_cost#501,i_brand_id#502,i_brand#503,i_class_id#504,i_class#505,i_category_id#506,i_category#507,i_manufact_id#508,i_manufact#509,i_size#510,i_formulation#511,i_color#512,i_units#513,i_container#514,i_manager_id#515,i_product_name#516] parquet
               :              :        +- Project [i_item_sk#34 AS ss_item_sk#18]
               :              :           +- Join Inner, (((i_brand_id#41 = brand_id#15) AND (i_class_id#43 = class_id#16)) AND (i_category_id#45 = category_id#17))
               :              :              :- Project [i_item_sk#34, i_brand_id#41, i_class_id#43, i_category_id#45]
               :              :              :  +- Filter ((isnotnull(i_brand_id#41) AND isnotnull(i_class_id#43)) AND isnotnull(i_category_id#45))
               :              :              :     +- Relation spark_catalog.tpcds.item[i_item_sk#34,i_item_id#35,i_rec_start_date#36,i_rec_end_date#37,i_item_desc#38,i_current_price#39,i_wholesale_cost#40,i_brand_id#41,i_brand#42,i_class_id#43,i_class#44,i_category_id#45,i_category#46,i_manufact_id#47,i_manufact#48,i_size#49,i_formulation#50,i_color#51,i_units#52,i_container#53,i_manager_id#54,i_product_name#55] parquet
               :              :              +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#254) AND (class_id#16 <=> i_class_id#256)) AND (category_id#17 <=> i_category_id#258))
               :              :                 :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :                 :  +- Project [i_brand_id#182 AS brand_id#15, i_class_id#184 AS class_id#16, i_category_id#186 AS category_id#17]
               :              :                 :     +- Join Inner, (ss_sold_date_sk#56 = d_date_sk#79)
               :              :                 :        :- Project [ss_sold_date_sk#56, i_brand_id#182, i_class_id#184, i_category_id#186]
               :              :                 :        :  +- Join Inner, (ss_item_sk#58 = i_item_sk#175)
               :              :                 :        :     :- Project [ss_sold_date_sk#56, ss_item_sk#58]
               :              :                 :        :     :  +- Filter (isnotnull(ss_item_sk#58) AND isnotnull(ss_sold_date_sk#56))
               :              :                 :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#56,ss_sold_time_sk#57,ss_item_sk#58,ss_customer_sk#59,ss_cdemo_sk#60,ss_hdemo_sk#61,ss_addr_sk#62,ss_store_sk#63,ss_promo_sk#64,ss_ticket_number#65,ss_quantity#66,ss_wholesale_cost#67,ss_list_price#68,ss_sales_price#69,ss_ext_discount_amt#70,ss_ext_sales_price#71,ss_ext_wholesale_cost#72,ss_ext_list_price#73,ss_ext_tax#74,ss_coupon_amt#75,ss_net_paid#76,ss_net_paid_inc_tax#77,ss_net_profit#78] parquet
               :              :                 :        :     +- Join LeftSemi, (((i_brand_id#182 <=> i_brand_id#204) AND (i_class_id#184 <=> i_class_id#206)) AND (i_category_id#186 <=> i_category_id#208))
               :              :                 :        :        :- Project [i_item_sk#175, i_brand_id#182, i_class_id#184, i_category_id#186]
               :              :                 :        :        :  +- Filter (isnotnull(i_item_sk#175) AND ((isnotnull(i_brand_id#182) AND isnotnull(i_class_id#184)) AND isnotnull(i_category_id#186)))
               :              :                 :        :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#175,i_item_id#176,i_rec_start_date#177,i_rec_end_date#178,i_item_desc#179,i_current_price#180,i_wholesale_cost#181,i_brand_id#182,i_brand#183,i_class_id#184,i_class#185,i_category_id#186,i_category#187,i_manufact_id#188,i_manufact#189,i_size#190,i_formulation#191,i_color#192,i_units#193,i_container#194,i_manager_id#195,i_product_name#196] parquet
               :              :                 :        :        +- Project [i_brand_id#204, i_class_id#206, i_category_id#208]
               :              :                 :        :           +- Join Inner, (cs_sold_date_sk#107 = d_date_sk#219)
               :              :                 :        :              :- Project [cs_sold_date_sk#107, i_brand_id#204, i_class_id#206, i_category_id#208]
               :              :                 :        :              :  +- Join Inner, (cs_item_sk#122 = i_item_sk#197)
               :              :                 :        :              :     :- Project [cs_sold_date_sk#107, cs_item_sk#122]
               :              :                 :        :              :     :  +- Filter (isnotnull(cs_item_sk#122) AND isnotnull(cs_sold_date_sk#107))
               :              :                 :        :              :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#107,cs_sold_time_sk#108,cs_ship_date_sk#109,cs_bill_customer_sk#110,cs_bill_cdemo_sk#111,cs_bill_hdemo_sk#112,cs_bill_addr_sk#113,cs_ship_customer_sk#114,cs_ship_cdemo_sk#115,cs_ship_hdemo_sk#116,cs_ship_addr_sk#117,cs_call_center_sk#118,cs_catalog_page_sk#119,cs_ship_mode_sk#120,cs_warehouse_sk#121,cs_item_sk#122,cs_promo_sk#123,cs_order_number#124,cs_quantity#125,cs_wholesale_cost#126,cs_list_price#127,cs_sales_price#128,cs_ext_discount_amt#129,cs_ext_sales_price#130,... 10 more fields] parquet
               :              :                 :        :              :     +- Project [i_item_sk#197, i_brand_id#204, i_class_id#206, i_category_id#208]
               :              :                 :        :              :        +- Filter isnotnull(i_item_sk#197)
               :              :                 :        :              :           +- Relation spark_catalog.tpcds.item[i_item_sk#197,i_item_id#198,i_rec_start_date#199,i_rec_end_date#200,i_item_desc#201,i_current_price#202,i_wholesale_cost#203,i_brand_id#204,i_brand#205,i_class_id#206,i_class#207,i_category_id#208,i_category#209,i_manufact_id#210,i_manufact#211,i_size#212,i_formulation#213,i_color#214,i_units#215,i_container#216,i_manager_id#217,i_product_name#218] parquet
               :              :                 :        :              +- Project [d_date_sk#219]
               :              :                 :        :                 +- Filter ((isnotnull(d_year#225) AND ((d_year#225 >= 1999) AND (d_year#225 <= 2001))) AND isnotnull(d_date_sk#219))
               :              :                 :        :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#219,d_date_id#220,d_date#221,d_month_seq#222,d_week_seq#223,d_quarter_seq#224,d_year#225,d_dow#226,d_moy#227,d_dom#228,d_qoy#229,d_fy_year#230,d_fy_quarter_seq#231,d_fy_week_seq#232,d_day_name#233,d_quarter_name#234,d_holiday#235,d_weekend#236,d_following_holiday#237,d_first_dom#238,d_last_dom#239,d_same_day_ly#240,d_same_day_lq#241,d_current_day#242,... 4 more fields] parquet
               :              :                 :        +- Project [d_date_sk#79]
               :              :                 :           +- Filter ((isnotnull(d_year#85) AND ((d_year#85 >= 1999) AND (d_year#85 <= 2001))) AND isnotnull(d_date_sk#79))
               :              :                 :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet
               :              :                 +- Project [i_brand_id#254, i_class_id#256, i_category_id#258]
               :              :                    +- Join Inner, (ws_sold_date_sk#141 = d_date_sk#269)
               :              :                       :- Project [ws_sold_date_sk#141, i_brand_id#254, i_class_id#256, i_category_id#258]
               :              :                       :  +- Join Inner, (ws_item_sk#144 = i_item_sk#247)
               :              :                       :     :- Project [ws_sold_date_sk#141, ws_item_sk#144]
               :              :                       :     :  +- Filter (isnotnull(ws_item_sk#144) AND isnotnull(ws_sold_date_sk#141))
               :              :                       :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#141,ws_sold_time_sk#142,ws_ship_date_sk#143,ws_item_sk#144,ws_bill_customer_sk#145,ws_bill_cdemo_sk#146,ws_bill_hdemo_sk#147,ws_bill_addr_sk#148,ws_ship_customer_sk#149,ws_ship_cdemo_sk#150,ws_ship_hdemo_sk#151,ws_ship_addr_sk#152,ws_web_page_sk#153,ws_web_site_sk#154,ws_ship_mode_sk#155,ws_warehouse_sk#156,ws_promo_sk#157,ws_order_number#158,ws_quantity#159,ws_wholesale_cost#160,ws_list_price#161,ws_sales_price#162,ws_ext_discount_amt#163,ws_ext_sales_price#164,... 10 more fields] parquet
               :              :                       :     +- Project [i_item_sk#247, i_brand_id#254, i_class_id#256, i_category_id#258]
               :              :                       :        +- Filter isnotnull(i_item_sk#247)
               :              :                       :           +- Relation spark_catalog.tpcds.item[i_item_sk#247,i_item_id#248,i_rec_start_date#249,i_rec_end_date#250,i_item_desc#251,i_current_price#252,i_wholesale_cost#253,i_brand_id#254,i_brand#255,i_class_id#256,i_class#257,i_category_id#258,i_category#259,i_manufact_id#260,i_manufact#261,i_size#262,i_formulation#263,i_color#264,i_units#265,i_container#266,i_manager_id#267,i_product_name#268] parquet
               :              :                       +- Project [d_date_sk#269]
               :              :                          +- Filter ((isnotnull(d_year#275) AND ((d_year#275 >= 1999) AND (d_year#275 <= 2001))) AND isnotnull(d_date_sk#269))
               :              :                             +- Relation spark_catalog.tpcds.date_dim[d_date_sk#269,d_date_id#270,d_date#271,d_month_seq#272,d_week_seq#273,d_quarter_seq#274,d_year#275,d_dow#276,d_moy#277,d_dom#278,d_qoy#279,d_fy_year#280,d_fy_quarter_seq#281,d_fy_week_seq#282,d_day_name#283,d_quarter_name#284,d_holiday#285,d_weekend#286,d_following_holiday#287,d_first_dom#288,d_last_dom#289,d_same_day_ly#290,d_same_day_lq#291,d_current_day#292,... 4 more fields] parquet
               :              +- Project [d_date_sk#517]
               :                 +- Filter (((isnotnull(d_year#523) AND isnotnull(d_moy#525)) AND ((d_year#523 = 2001) AND (d_moy#525 = 11))) AND isnotnull(d_date_sk#517))
               :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#517,d_date_id#518,d_date#519,d_month_seq#520,d_week_seq#521,d_quarter_seq#522,d_year#523,d_dow#524,d_moy#525,d_dom#526,d_qoy#527,d_fy_year#528,d_fy_quarter_seq#529,d_fy_week_seq#530,d_day_name#531,d_quarter_name#532,d_holiday#533,d_weekend#534,d_following_holiday#535,d_first_dom#536,d_last_dom#537,d_same_day_ly#538,d_same_day_lq#539,d_current_day#540,... 4 more fields] parquet
               :- Project [sales#6, number_sales#7L, catalog AS channel#2522, i_brand_id#586, i_class_id#588, i_category_id#590]
               :  +- Filter (isnotnull(sales#6) AND (sales#6 > scalar-subquery#9 []))
               :     :  +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
               :     :     +- Union false, false
               :     :        :- Project [ss_quantity#307 AS quantity#19, ss_list_price#309 AS list_price#20]
               :     :        :  +- Join Inner, (ss_sold_date_sk#297 = d_date_sk#320)
               :     :        :     :- Project [ss_sold_date_sk#297, ss_quantity#307, ss_list_price#309]
               :     :        :     :  +- Filter isnotnull(ss_sold_date_sk#297)
               :     :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#297,ss_sold_time_sk#298,ss_item_sk#299,ss_customer_sk#300,ss_cdemo_sk#301,ss_hdemo_sk#302,ss_addr_sk#303,ss_store_sk#304,ss_promo_sk#305,ss_ticket_number#306,ss_quantity#307,ss_wholesale_cost#308,ss_list_price#309,ss_sales_price#310,ss_ext_discount_amt#311,ss_ext_sales_price#312,ss_ext_wholesale_cost#313,ss_ext_list_price#314,ss_ext_tax#315,ss_coupon_amt#316,ss_net_paid#317,ss_net_paid_inc_tax#318,ss_net_profit#319] parquet
               :     :        :     +- Project [d_date_sk#320]
               :     :        :        +- Filter ((isnotnull(d_year#326) AND ((d_year#326 >= 1999) AND (d_year#326 <= 2001))) AND isnotnull(d_date_sk#320))
               :     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#320,d_date_id#321,d_date#322,d_month_seq#323,d_week_seq#324,d_quarter_seq#325,d_year#326,d_dow#327,d_moy#328,d_dom#329,d_qoy#330,d_fy_year#331,d_fy_quarter_seq#332,d_fy_week_seq#333,d_day_name#334,d_quarter_name#335,d_holiday#336,d_weekend#337,d_following_holiday#338,d_first_dom#339,d_last_dom#340,d_same_day_ly#341,d_same_day_lq#342,d_current_day#343,... 4 more fields] parquet
               :     :        :- Project [cs_quantity#366 AS quantity#21, cs_list_price#368 AS list_price#22]
               :     :        :  +- Join Inner, (cs_sold_date_sk#348 = d_date_sk#382)
               :     :        :     :- Project [cs_sold_date_sk#348, cs_quantity#366, cs_list_price#368]
               :     :        :     :  +- Filter isnotnull(cs_sold_date_sk#348)
               :     :        :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_sold_time_sk#349,cs_ship_date_sk#350,cs_bill_customer_sk#351,cs_bill_cdemo_sk#352,cs_bill_hdemo_sk#353,cs_bill_addr_sk#354,cs_ship_customer_sk#355,cs_ship_cdemo_sk#356,cs_ship_hdemo_sk#357,cs_ship_addr_sk#358,cs_call_center_sk#359,cs_catalog_page_sk#360,cs_ship_mode_sk#361,cs_warehouse_sk#362,cs_item_sk#363,cs_promo_sk#364,cs_order_number#365,cs_quantity#366,cs_wholesale_cost#367,cs_list_price#368,cs_sales_price#369,cs_ext_discount_amt#370,cs_ext_sales_price#371,... 10 more fields] parquet
               :     :        :     +- Project [d_date_sk#382]
               :     :        :        +- Filter ((isnotnull(d_year#388) AND ((d_year#388 >= 1999) AND (d_year#388 <= 2001))) AND isnotnull(d_date_sk#382))
               :     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#382,d_date_id#383,d_date#384,d_month_seq#385,d_week_seq#386,d_quarter_seq#387,d_year#388,d_dow#389,d_moy#390,d_dom#391,d_qoy#392,d_fy_year#393,d_fy_quarter_seq#394,d_fy_week_seq#395,d_day_name#396,d_quarter_name#397,d_holiday#398,d_weekend#399,d_following_holiday#400,d_first_dom#401,d_last_dom#402,d_same_day_ly#403,d_same_day_lq#404,d_current_day#405,... 4 more fields] parquet
               :     :        +- Project [ws_quantity#428 AS quantity#23, ws_list_price#430 AS list_price#24]
               :     :           +- Join Inner, (ws_sold_date_sk#410 = d_date_sk#444)
               :     :              :- Project [ws_sold_date_sk#410, ws_quantity#428, ws_list_price#430]
               :     :              :  +- Filter isnotnull(ws_sold_date_sk#410)
               :     :              :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#410,ws_sold_time_sk#411,ws_ship_date_sk#412,ws_item_sk#413,ws_bill_customer_sk#414,ws_bill_cdemo_sk#415,ws_bill_hdemo_sk#416,ws_bill_addr_sk#417,ws_ship_customer_sk#418,ws_ship_cdemo_sk#419,ws_ship_hdemo_sk#420,ws_ship_addr_sk#421,ws_web_page_sk#422,ws_web_site_sk#423,ws_ship_mode_sk#424,ws_warehouse_sk#425,ws_promo_sk#426,ws_order_number#427,ws_quantity#428,ws_wholesale_cost#429,ws_list_price#430,ws_sales_price#431,ws_ext_discount_amt#432,ws_ext_sales_price#433,... 10 more fields] parquet
               :     :              +- Project [d_date_sk#444]
               :     :                 +- Filter ((isnotnull(d_year#450) AND ((d_year#450 >= 1999) AND (d_year#450 <= 2001))) AND isnotnull(d_date_sk#444))
               :     :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#444,d_date_id#445,d_date#446,d_month_seq#447,d_week_seq#448,d_quarter_seq#449,d_year#450,d_dow#451,d_moy#452,d_dom#453,d_qoy#454,d_fy_year#455,d_fy_quarter_seq#456,d_fy_week_seq#457,d_day_name#458,d_quarter_name#459,d_holiday#460,d_weekend#461,d_following_holiday#462,d_first_dom#463,d_last_dom#464,d_same_day_ly#465,d_same_day_lq#466,d_current_day#467,... 4 more fields] parquet
               :     +- Aggregate [i_brand_id#586, i_class_id#588, i_category_id#590], [i_brand_id#586, i_class_id#588, i_category_id#590, sum((cast(cs_quantity#563 as double) * cs_list_price#565)) AS sales#6, count(1) AS number_sales#7L]
               :        +- Project [cs_quantity#563, cs_list_price#565, i_brand_id#586, i_class_id#588, i_category_id#590]
               :           +- Join Inner, (cs_sold_date_sk#545 = d_date_sk#601)
               :              :- Project [cs_sold_date_sk#545, cs_quantity#563, cs_list_price#565, i_brand_id#586, i_class_id#588, i_category_id#590]
               :              :  +- Join Inner, (cs_item_sk#560 = i_item_sk#579)
               :              :     :- Join LeftSemi, (cs_item_sk#560 = ss_item_sk#729)
               :              :     :  :- Project [cs_sold_date_sk#545, cs_item_sk#560, cs_quantity#563, cs_list_price#565]
               :              :     :  :  +- Filter (isnotnull(cs_item_sk#560) AND isnotnull(cs_sold_date_sk#545))
               :              :     :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#545,cs_sold_time_sk#546,cs_ship_date_sk#547,cs_bill_customer_sk#548,cs_bill_cdemo_sk#549,cs_bill_hdemo_sk#550,cs_bill_addr_sk#551,cs_ship_customer_sk#552,cs_ship_cdemo_sk#553,cs_ship_hdemo_sk#554,cs_ship_addr_sk#555,cs_call_center_sk#556,cs_catalog_page_sk#557,cs_ship_mode_sk#558,cs_warehouse_sk#559,cs_item_sk#560,cs_promo_sk#561,cs_order_number#562,cs_quantity#563,cs_wholesale_cost#564,cs_list_price#565,cs_sales_price#566,cs_ext_discount_amt#567,cs_ext_sales_price#568,... 10 more fields] parquet
               :              :     :  +- Project [i_item_sk#1642 AS ss_item_sk#729]
               :              :     :     +- Join Inner, (((i_brand_id#1649 = brand_id#15) AND (i_class_id#1651 = class_id#16)) AND (i_category_id#1653 = category_id#17))
               :              :     :        :- Project [i_item_sk#1642, i_brand_id#1649, i_class_id#1651, i_category_id#1653]
               :              :     :        :  +- Filter ((isnotnull(i_brand_id#1649) AND isnotnull(i_class_id#1651)) AND isnotnull(i_category_id#1653))
               :              :     :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#1642,i_item_id#1643,i_rec_start_date#1644,i_rec_end_date#1645,i_item_desc#1646,i_current_price#1647,i_wholesale_cost#1648,i_brand_id#1649,i_brand#1650,i_class_id#1651,i_class#1652,i_category_id#1653,i_category#1654,i_manufact_id#1655,i_manufact#1656,i_size#1657,i_formulation#1658,i_color#1659,i_units#1660,i_container#1661,i_manager_id#1662,i_product_name#1663] parquet
               :              :     :        +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#1862) AND (class_id#16 <=> i_class_id#1864)) AND (category_id#17 <=> i_category_id#1866))
               :              :     :           :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :     :           :  +- Project [i_brand_id#1694 AS brand_id#15, i_class_id#1696 AS class_id#16, i_category_id#1698 AS category_id#17]
               :              :     :           :     +- Join Inner, (ss_sold_date_sk#1664 = d_date_sk#1709)
               :              :     :           :        :- Project [ss_sold_date_sk#1664, i_brand_id#1694, i_class_id#1696, i_category_id#1698]
               :              :     :           :        :  +- Join Inner, (ss_item_sk#1666 = i_item_sk#1687)
               :              :     :           :        :     :- Project [ss_sold_date_sk#1664, ss_item_sk#1666]
               :              :     :           :        :     :  +- Filter (isnotnull(ss_item_sk#1666) AND isnotnull(ss_sold_date_sk#1664))
               :              :     :           :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#1664,ss_sold_time_sk#1665,ss_item_sk#1666,ss_customer_sk#1667,ss_cdemo_sk#1668,ss_hdemo_sk#1669,ss_addr_sk#1670,ss_store_sk#1671,ss_promo_sk#1672,ss_ticket_number#1673,ss_quantity#1674,ss_wholesale_cost#1675,ss_list_price#1676,ss_sales_price#1677,ss_ext_discount_amt#1678,ss_ext_sales_price#1679,ss_ext_wholesale_cost#1680,ss_ext_list_price#1681,ss_ext_tax#1682,ss_coupon_amt#1683,ss_net_paid#1684,ss_net_paid_inc_tax#1685,ss_net_profit#1686] parquet
               :              :     :           :        :     +- Join LeftSemi, (((i_brand_id#1694 <=> i_brand_id#1778) AND (i_class_id#1696 <=> i_class_id#1780)) AND (i_category_id#1698 <=> i_category_id#1782))
               :              :     :           :        :        :- Project [i_item_sk#1687, i_brand_id#1694, i_class_id#1696, i_category_id#1698]
               :              :     :           :        :        :  +- Filter (isnotnull(i_item_sk#1687) AND ((isnotnull(i_brand_id#1694) AND isnotnull(i_class_id#1696)) AND isnotnull(i_category_id#1698)))
               :              :     :           :        :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#1687,i_item_id#1688,i_rec_start_date#1689,i_rec_end_date#1690,i_item_desc#1691,i_current_price#1692,i_wholesale_cost#1693,i_brand_id#1694,i_brand#1695,i_class_id#1696,i_class#1697,i_category_id#1698,i_category#1699,i_manufact_id#1700,i_manufact#1701,i_size#1702,i_formulation#1703,i_color#1704,i_units#1705,i_container#1706,i_manager_id#1707,i_product_name#1708] parquet
               :              :     :           :        :        +- Project [i_brand_id#1778, i_class_id#1780, i_category_id#1782]
               :              :     :           :        :           +- Join Inner, (cs_sold_date_sk#1737 = d_date_sk#1793)
               :              :     :           :        :              :- Project [cs_sold_date_sk#1737, i_brand_id#1778, i_class_id#1780, i_category_id#1782]
               :              :     :           :        :              :  +- Join Inner, (cs_item_sk#1752 = i_item_sk#1771)
               :              :     :           :        :              :     :- Project [cs_sold_date_sk#1737, cs_item_sk#1752]
               :              :     :           :        :              :     :  +- Filter (isnotnull(cs_item_sk#1752) AND isnotnull(cs_sold_date_sk#1737))
               :              :     :           :        :              :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#1737,cs_sold_time_sk#1738,cs_ship_date_sk#1739,cs_bill_customer_sk#1740,cs_bill_cdemo_sk#1741,cs_bill_hdemo_sk#1742,cs_bill_addr_sk#1743,cs_ship_customer_sk#1744,cs_ship_cdemo_sk#1745,cs_ship_hdemo_sk#1746,cs_ship_addr_sk#1747,cs_call_center_sk#1748,cs_catalog_page_sk#1749,cs_ship_mode_sk#1750,cs_warehouse_sk#1751,cs_item_sk#1752,cs_promo_sk#1753,cs_order_number#1754,cs_quantity#1755,cs_wholesale_cost#1756,cs_list_price#1757,cs_sales_price#1758,cs_ext_discount_amt#1759,cs_ext_sales_price#1760,... 10 more fields] parquet
               :              :     :           :        :              :     +- Project [i_item_sk#1771, i_brand_id#1778, i_class_id#1780, i_category_id#1782]
               :              :     :           :        :              :        +- Filter isnotnull(i_item_sk#1771)
               :              :     :           :        :              :           +- Relation spark_catalog.tpcds.item[i_item_sk#1771,i_item_id#1772,i_rec_start_date#1773,i_rec_end_date#1774,i_item_desc#1775,i_current_price#1776,i_wholesale_cost#1777,i_brand_id#1778,i_brand#1779,i_class_id#1780,i_class#1781,i_category_id#1782,i_category#1783,i_manufact_id#1784,i_manufact#1785,i_size#1786,i_formulation#1787,i_color#1788,i_units#1789,i_container#1790,i_manager_id#1791,i_product_name#1792] parquet
               :              :     :           :        :              +- Project [d_date_sk#1793]
               :              :     :           :        :                 +- Filter ((isnotnull(d_year#1799) AND ((d_year#1799 >= 1999) AND (d_year#1799 <= 2001))) AND isnotnull(d_date_sk#1793))
               :              :     :           :        :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1793,d_date_id#1794,d_date#1795,d_month_seq#1796,d_week_seq#1797,d_quarter_seq#1798,d_year#1799,d_dow#1800,d_moy#1801,d_dom#1802,d_qoy#1803,d_fy_year#1804,d_fy_quarter_seq#1805,d_fy_week_seq#1806,d_day_name#1807,d_quarter_name#1808,d_holiday#1809,d_weekend#1810,d_following_holiday#1811,d_first_dom#1812,d_last_dom#1813,d_same_day_ly#1814,d_same_day_lq#1815,d_current_day#1816,... 4 more fields] parquet
               :              :     :           :        +- Project [d_date_sk#1709]
               :              :     :           :           +- Filter ((isnotnull(d_year#1715) AND ((d_year#1715 >= 1999) AND (d_year#1715 <= 2001))) AND isnotnull(d_date_sk#1709))
               :              :     :           :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1709,d_date_id#1710,d_date#1711,d_month_seq#1712,d_week_seq#1713,d_quarter_seq#1714,d_year#1715,d_dow#1716,d_moy#1717,d_dom#1718,d_qoy#1719,d_fy_year#1720,d_fy_quarter_seq#1721,d_fy_week_seq#1722,d_day_name#1723,d_quarter_name#1724,d_holiday#1725,d_weekend#1726,d_following_holiday#1727,d_first_dom#1728,d_last_dom#1729,d_same_day_ly#1730,d_same_day_lq#1731,d_current_day#1732,... 4 more fields] parquet
               :              :     :           +- Project [i_brand_id#1862, i_class_id#1864, i_category_id#1866]
               :              :     :              +- Join Inner, (ws_sold_date_sk#1821 = d_date_sk#1877)
               :              :     :                 :- Project [ws_sold_date_sk#1821, i_brand_id#1862, i_class_id#1864, i_category_id#1866]
               :              :     :                 :  +- Join Inner, (ws_item_sk#1824 = i_item_sk#1855)
               :              :     :                 :     :- Project [ws_sold_date_sk#1821, ws_item_sk#1824]
               :              :     :                 :     :  +- Filter (isnotnull(ws_item_sk#1824) AND isnotnull(ws_sold_date_sk#1821))
               :              :     :                 :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#1821,ws_sold_time_sk#1822,ws_ship_date_sk#1823,ws_item_sk#1824,ws_bill_customer_sk#1825,ws_bill_cdemo_sk#1826,ws_bill_hdemo_sk#1827,ws_bill_addr_sk#1828,ws_ship_customer_sk#1829,ws_ship_cdemo_sk#1830,ws_ship_hdemo_sk#1831,ws_ship_addr_sk#1832,ws_web_page_sk#1833,ws_web_site_sk#1834,ws_ship_mode_sk#1835,ws_warehouse_sk#1836,ws_promo_sk#1837,ws_order_number#1838,ws_quantity#1839,ws_wholesale_cost#1840,ws_list_price#1841,ws_sales_price#1842,ws_ext_discount_amt#1843,ws_ext_sales_price#1844,... 10 more fields] parquet
               :              :     :                 :     +- Project [i_item_sk#1855, i_brand_id#1862, i_class_id#1864, i_category_id#1866]
               :              :     :                 :        +- Filter isnotnull(i_item_sk#1855)
               :              :     :                 :           +- Relation spark_catalog.tpcds.item[i_item_sk#1855,i_item_id#1856,i_rec_start_date#1857,i_rec_end_date#1858,i_item_desc#1859,i_current_price#1860,i_wholesale_cost#1861,i_brand_id#1862,i_brand#1863,i_class_id#1864,i_class#1865,i_category_id#1866,i_category#1867,i_manufact_id#1868,i_manufact#1869,i_size#1870,i_formulation#1871,i_color#1872,i_units#1873,i_container#1874,i_manager_id#1875,i_product_name#1876] parquet
               :              :     :                 +- Project [d_date_sk#1877]
               :              :     :                    +- Filter ((isnotnull(d_year#1883) AND ((d_year#1883 >= 1999) AND (d_year#1883 <= 2001))) AND isnotnull(d_date_sk#1877))
               :              :     :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1877,d_date_id#1878,d_date#1879,d_month_seq#1880,d_week_seq#1881,d_quarter_seq#1882,d_year#1883,d_dow#1884,d_moy#1885,d_dom#1886,d_qoy#1887,d_fy_year#1888,d_fy_quarter_seq#1889,d_fy_week_seq#1890,d_day_name#1891,d_quarter_name#1892,d_holiday#1893,d_weekend#1894,d_following_holiday#1895,d_first_dom#1896,d_last_dom#1897,d_same_day_ly#1898,d_same_day_lq#1899,d_current_day#1900,... 4 more fields] parquet
               :              :     +- Join LeftSemi, (i_item_sk#579 = ss_item_sk#729)
               :              :        :- Project [i_item_sk#579, i_brand_id#586, i_class_id#588, i_category_id#590]
               :              :        :  +- Filter isnotnull(i_item_sk#579)
               :              :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#579,i_item_id#580,i_rec_start_date#581,i_rec_end_date#582,i_item_desc#583,i_current_price#584,i_wholesale_cost#585,i_brand_id#586,i_brand#587,i_class_id#588,i_class#589,i_category_id#590,i_category#591,i_manufact_id#592,i_manufact#593,i_size#594,i_formulation#595,i_color#596,i_units#597,i_container#598,i_manager_id#599,i_product_name#600] parquet
               :              :        +- Project [i_item_sk#1642 AS ss_item_sk#729]
               :              :           +- Join Inner, (((i_brand_id#1649 = brand_id#15) AND (i_class_id#1651 = class_id#16)) AND (i_category_id#1653 = category_id#17))
               :              :              :- Project [i_item_sk#1642, i_brand_id#1649, i_class_id#1651, i_category_id#1653]
               :              :              :  +- Filter ((isnotnull(i_brand_id#1649) AND isnotnull(i_class_id#1651)) AND isnotnull(i_category_id#1653))
               :              :              :     +- Relation spark_catalog.tpcds.item[i_item_sk#1642,i_item_id#1643,i_rec_start_date#1644,i_rec_end_date#1645,i_item_desc#1646,i_current_price#1647,i_wholesale_cost#1648,i_brand_id#1649,i_brand#1650,i_class_id#1651,i_class#1652,i_category_id#1653,i_category#1654,i_manufact_id#1655,i_manufact#1656,i_size#1657,i_formulation#1658,i_color#1659,i_units#1660,i_container#1661,i_manager_id#1662,i_product_name#1663] parquet
               :              :              +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#1862) AND (class_id#16 <=> i_class_id#1864)) AND (category_id#17 <=> i_category_id#1866))
               :              :                 :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :                 :  +- Project [i_brand_id#1694 AS brand_id#15, i_class_id#1696 AS class_id#16, i_category_id#1698 AS category_id#17]
               :              :                 :     +- Join Inner, (ss_sold_date_sk#1664 = d_date_sk#1709)
               :              :                 :        :- Project [ss_sold_date_sk#1664, i_brand_id#1694, i_class_id#1696, i_category_id#1698]
               :              :                 :        :  +- Join Inner, (ss_item_sk#1666 = i_item_sk#1687)
               :              :                 :        :     :- Project [ss_sold_date_sk#1664, ss_item_sk#1666]
               :              :                 :        :     :  +- Filter (isnotnull(ss_item_sk#1666) AND isnotnull(ss_sold_date_sk#1664))
               :              :                 :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#1664,ss_sold_time_sk#1665,ss_item_sk#1666,ss_customer_sk#1667,ss_cdemo_sk#1668,ss_hdemo_sk#1669,ss_addr_sk#1670,ss_store_sk#1671,ss_promo_sk#1672,ss_ticket_number#1673,ss_quantity#1674,ss_wholesale_cost#1675,ss_list_price#1676,ss_sales_price#1677,ss_ext_discount_amt#1678,ss_ext_sales_price#1679,ss_ext_wholesale_cost#1680,ss_ext_list_price#1681,ss_ext_tax#1682,ss_coupon_amt#1683,ss_net_paid#1684,ss_net_paid_inc_tax#1685,ss_net_profit#1686] parquet
               :              :                 :        :     +- Join LeftSemi, (((i_brand_id#1694 <=> i_brand_id#1778) AND (i_class_id#1696 <=> i_class_id#1780)) AND (i_category_id#1698 <=> i_category_id#1782))
               :              :                 :        :        :- Project [i_item_sk#1687, i_brand_id#1694, i_class_id#1696, i_category_id#1698]
               :              :                 :        :        :  +- Filter (isnotnull(i_item_sk#1687) AND ((isnotnull(i_brand_id#1694) AND isnotnull(i_class_id#1696)) AND isnotnull(i_category_id#1698)))
               :              :                 :        :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#1687,i_item_id#1688,i_rec_start_date#1689,i_rec_end_date#1690,i_item_desc#1691,i_current_price#1692,i_wholesale_cost#1693,i_brand_id#1694,i_brand#1695,i_class_id#1696,i_class#1697,i_category_id#1698,i_category#1699,i_manufact_id#1700,i_manufact#1701,i_size#1702,i_formulation#1703,i_color#1704,i_units#1705,i_container#1706,i_manager_id#1707,i_product_name#1708] parquet
               :              :                 :        :        +- Project [i_brand_id#1778, i_class_id#1780, i_category_id#1782]
               :              :                 :        :           +- Join Inner, (cs_sold_date_sk#1737 = d_date_sk#1793)
               :              :                 :        :              :- Project [cs_sold_date_sk#1737, i_brand_id#1778, i_class_id#1780, i_category_id#1782]
               :              :                 :        :              :  +- Join Inner, (cs_item_sk#1752 = i_item_sk#1771)
               :              :                 :        :              :     :- Project [cs_sold_date_sk#1737, cs_item_sk#1752]
               :              :                 :        :              :     :  +- Filter (isnotnull(cs_item_sk#1752) AND isnotnull(cs_sold_date_sk#1737))
               :              :                 :        :              :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#1737,cs_sold_time_sk#1738,cs_ship_date_sk#1739,cs_bill_customer_sk#1740,cs_bill_cdemo_sk#1741,cs_bill_hdemo_sk#1742,cs_bill_addr_sk#1743,cs_ship_customer_sk#1744,cs_ship_cdemo_sk#1745,cs_ship_hdemo_sk#1746,cs_ship_addr_sk#1747,cs_call_center_sk#1748,cs_catalog_page_sk#1749,cs_ship_mode_sk#1750,cs_warehouse_sk#1751,cs_item_sk#1752,cs_promo_sk#1753,cs_order_number#1754,cs_quantity#1755,cs_wholesale_cost#1756,cs_list_price#1757,cs_sales_price#1758,cs_ext_discount_amt#1759,cs_ext_sales_price#1760,... 10 more fields] parquet
               :              :                 :        :              :     +- Project [i_item_sk#1771, i_brand_id#1778, i_class_id#1780, i_category_id#1782]
               :              :                 :        :              :        +- Filter isnotnull(i_item_sk#1771)
               :              :                 :        :              :           +- Relation spark_catalog.tpcds.item[i_item_sk#1771,i_item_id#1772,i_rec_start_date#1773,i_rec_end_date#1774,i_item_desc#1775,i_current_price#1776,i_wholesale_cost#1777,i_brand_id#1778,i_brand#1779,i_class_id#1780,i_class#1781,i_category_id#1782,i_category#1783,i_manufact_id#1784,i_manufact#1785,i_size#1786,i_formulation#1787,i_color#1788,i_units#1789,i_container#1790,i_manager_id#1791,i_product_name#1792] parquet
               :              :                 :        :              +- Project [d_date_sk#1793]
               :              :                 :        :                 +- Filter ((isnotnull(d_year#1799) AND ((d_year#1799 >= 1999) AND (d_year#1799 <= 2001))) AND isnotnull(d_date_sk#1793))
               :              :                 :        :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1793,d_date_id#1794,d_date#1795,d_month_seq#1796,d_week_seq#1797,d_quarter_seq#1798,d_year#1799,d_dow#1800,d_moy#1801,d_dom#1802,d_qoy#1803,d_fy_year#1804,d_fy_quarter_seq#1805,d_fy_week_seq#1806,d_day_name#1807,d_quarter_name#1808,d_holiday#1809,d_weekend#1810,d_following_holiday#1811,d_first_dom#1812,d_last_dom#1813,d_same_day_ly#1814,d_same_day_lq#1815,d_current_day#1816,... 4 more fields] parquet
               :              :                 :        +- Project [d_date_sk#1709]
               :              :                 :           +- Filter ((isnotnull(d_year#1715) AND ((d_year#1715 >= 1999) AND (d_year#1715 <= 2001))) AND isnotnull(d_date_sk#1709))
               :              :                 :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1709,d_date_id#1710,d_date#1711,d_month_seq#1712,d_week_seq#1713,d_quarter_seq#1714,d_year#1715,d_dow#1716,d_moy#1717,d_dom#1718,d_qoy#1719,d_fy_year#1720,d_fy_quarter_seq#1721,d_fy_week_seq#1722,d_day_name#1723,d_quarter_name#1724,d_holiday#1725,d_weekend#1726,d_following_holiday#1727,d_first_dom#1728,d_last_dom#1729,d_same_day_ly#1730,d_same_day_lq#1731,d_current_day#1732,... 4 more fields] parquet
               :              :                 +- Project [i_brand_id#1862, i_class_id#1864, i_category_id#1866]
               :              :                    +- Join Inner, (ws_sold_date_sk#1821 = d_date_sk#1877)
               :              :                       :- Project [ws_sold_date_sk#1821, i_brand_id#1862, i_class_id#1864, i_category_id#1866]
               :              :                       :  +- Join Inner, (ws_item_sk#1824 = i_item_sk#1855)
               :              :                       :     :- Project [ws_sold_date_sk#1821, ws_item_sk#1824]
               :              :                       :     :  +- Filter (isnotnull(ws_item_sk#1824) AND isnotnull(ws_sold_date_sk#1821))
               :              :                       :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#1821,ws_sold_time_sk#1822,ws_ship_date_sk#1823,ws_item_sk#1824,ws_bill_customer_sk#1825,ws_bill_cdemo_sk#1826,ws_bill_hdemo_sk#1827,ws_bill_addr_sk#1828,ws_ship_customer_sk#1829,ws_ship_cdemo_sk#1830,ws_ship_hdemo_sk#1831,ws_ship_addr_sk#1832,ws_web_page_sk#1833,ws_web_site_sk#1834,ws_ship_mode_sk#1835,ws_warehouse_sk#1836,ws_promo_sk#1837,ws_order_number#1838,ws_quantity#1839,ws_wholesale_cost#1840,ws_list_price#1841,ws_sales_price#1842,ws_ext_discount_amt#1843,ws_ext_sales_price#1844,... 10 more fields] parquet
               :              :                       :     +- Project [i_item_sk#1855, i_brand_id#1862, i_class_id#1864, i_category_id#1866]
               :              :                       :        +- Filter isnotnull(i_item_sk#1855)
               :              :                       :           +- Relation spark_catalog.tpcds.item[i_item_sk#1855,i_item_id#1856,i_rec_start_date#1857,i_rec_end_date#1858,i_item_desc#1859,i_current_price#1860,i_wholesale_cost#1861,i_brand_id#1862,i_brand#1863,i_class_id#1864,i_class#1865,i_category_id#1866,i_category#1867,i_manufact_id#1868,i_manufact#1869,i_size#1870,i_formulation#1871,i_color#1872,i_units#1873,i_container#1874,i_manager_id#1875,i_product_name#1876] parquet
               :              :                       +- Project [d_date_sk#1877]
               :              :                          +- Filter ((isnotnull(d_year#1883) AND ((d_year#1883 >= 1999) AND (d_year#1883 <= 2001))) AND isnotnull(d_date_sk#1877))
               :              :                             +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1877,d_date_id#1878,d_date#1879,d_month_seq#1880,d_week_seq#1881,d_quarter_seq#1882,d_year#1883,d_dow#1884,d_moy#1885,d_dom#1886,d_qoy#1887,d_fy_year#1888,d_fy_quarter_seq#1889,d_fy_week_seq#1890,d_day_name#1891,d_quarter_name#1892,d_holiday#1893,d_weekend#1894,d_following_holiday#1895,d_first_dom#1896,d_last_dom#1897,d_same_day_ly#1898,d_same_day_lq#1899,d_current_day#1900,... 4 more fields] parquet
               :              +- Project [d_date_sk#601]
               :                 +- Filter (((isnotnull(d_year#607) AND isnotnull(d_moy#609)) AND ((d_year#607 = 2001) AND (d_moy#609 = 11))) AND isnotnull(d_date_sk#601))
               :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#601,d_date_id#602,d_date#603,d_month_seq#604,d_week_seq#605,d_quarter_seq#606,d_year#607,d_dow#608,d_moy#609,d_dom#610,d_qoy#611,d_fy_year#612,d_fy_quarter_seq#613,d_fy_week_seq#614,d_day_name#615,d_quarter_name#616,d_holiday#617,d_weekend#618,d_following_holiday#619,d_first_dom#620,d_last_dom#621,d_same_day_ly#622,d_same_day_lq#623,d_current_day#624,... 4 more fields] parquet
               +- Project [sales#11, number_sales#12L, web AS channel#2526, i_brand_id#670, i_class_id#672, i_category_id#674]
                  +- Filter (isnotnull(sales#11) AND (sales#11 > scalar-subquery#14 []))
                     :  +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
                     :     +- Union false, false
                     :        :- Project [ss_quantity#307 AS quantity#19, ss_list_price#309 AS list_price#20]
                     :        :  +- Join Inner, (ss_sold_date_sk#297 = d_date_sk#320)
                     :        :     :- Project [ss_sold_date_sk#297, ss_quantity#307, ss_list_price#309]
                     :        :     :  +- Filter isnotnull(ss_sold_date_sk#297)
                     :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#297,ss_sold_time_sk#298,ss_item_sk#299,ss_customer_sk#300,ss_cdemo_sk#301,ss_hdemo_sk#302,ss_addr_sk#303,ss_store_sk#304,ss_promo_sk#305,ss_ticket_number#306,ss_quantity#307,ss_wholesale_cost#308,ss_list_price#309,ss_sales_price#310,ss_ext_discount_amt#311,ss_ext_sales_price#312,ss_ext_wholesale_cost#313,ss_ext_list_price#314,ss_ext_tax#315,ss_coupon_amt#316,ss_net_paid#317,ss_net_paid_inc_tax#318,ss_net_profit#319] parquet
                     :        :     +- Project [d_date_sk#320]
                     :        :        +- Filter ((isnotnull(d_year#326) AND ((d_year#326 >= 1999) AND (d_year#326 <= 2001))) AND isnotnull(d_date_sk#320))
                     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#320,d_date_id#321,d_date#322,d_month_seq#323,d_week_seq#324,d_quarter_seq#325,d_year#326,d_dow#327,d_moy#328,d_dom#329,d_qoy#330,d_fy_year#331,d_fy_quarter_seq#332,d_fy_week_seq#333,d_day_name#334,d_quarter_name#335,d_holiday#336,d_weekend#337,d_following_holiday#338,d_first_dom#339,d_last_dom#340,d_same_day_ly#341,d_same_day_lq#342,d_current_day#343,... 4 more fields] parquet
                     :        :- Project [cs_quantity#366 AS quantity#21, cs_list_price#368 AS list_price#22]
                     :        :  +- Join Inner, (cs_sold_date_sk#348 = d_date_sk#382)
                     :        :     :- Project [cs_sold_date_sk#348, cs_quantity#366, cs_list_price#368]
                     :        :     :  +- Filter isnotnull(cs_sold_date_sk#348)
                     :        :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_sold_time_sk#349,cs_ship_date_sk#350,cs_bill_customer_sk#351,cs_bill_cdemo_sk#352,cs_bill_hdemo_sk#353,cs_bill_addr_sk#354,cs_ship_customer_sk#355,cs_ship_cdemo_sk#356,cs_ship_hdemo_sk#357,cs_ship_addr_sk#358,cs_call_center_sk#359,cs_catalog_page_sk#360,cs_ship_mode_sk#361,cs_warehouse_sk#362,cs_item_sk#363,cs_promo_sk#364,cs_order_number#365,cs_quantity#366,cs_wholesale_cost#367,cs_list_price#368,cs_sales_price#369,cs_ext_discount_amt#370,cs_ext_sales_price#371,... 10 more fields] parquet
                     :        :     +- Project [d_date_sk#382]
                     :        :        +- Filter ((isnotnull(d_year#388) AND ((d_year#388 >= 1999) AND (d_year#388 <= 2001))) AND isnotnull(d_date_sk#382))
                     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#382,d_date_id#383,d_date#384,d_month_seq#385,d_week_seq#386,d_quarter_seq#387,d_year#388,d_dow#389,d_moy#390,d_dom#391,d_qoy#392,d_fy_year#393,d_fy_quarter_seq#394,d_fy_week_seq#395,d_day_name#396,d_quarter_name#397,d_holiday#398,d_weekend#399,d_following_holiday#400,d_first_dom#401,d_last_dom#402,d_same_day_ly#403,d_same_day_lq#404,d_current_day#405,... 4 more fields] parquet
                     :        +- Project [ws_quantity#428 AS quantity#23, ws_list_price#430 AS list_price#24]
                     :           +- Join Inner, (ws_sold_date_sk#410 = d_date_sk#444)
                     :              :- Project [ws_sold_date_sk#410, ws_quantity#428, ws_list_price#430]
                     :              :  +- Filter isnotnull(ws_sold_date_sk#410)
                     :              :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#410,ws_sold_time_sk#411,ws_ship_date_sk#412,ws_item_sk#413,ws_bill_customer_sk#414,ws_bill_cdemo_sk#415,ws_bill_hdemo_sk#416,ws_bill_addr_sk#417,ws_ship_customer_sk#418,ws_ship_cdemo_sk#419,ws_ship_hdemo_sk#420,ws_ship_addr_sk#421,ws_web_page_sk#422,ws_web_site_sk#423,ws_ship_mode_sk#424,ws_warehouse_sk#425,ws_promo_sk#426,ws_order_number#427,ws_quantity#428,ws_wholesale_cost#429,ws_list_price#430,ws_sales_price#431,ws_ext_discount_amt#432,ws_ext_sales_price#433,... 10 more fields] parquet
                     :              +- Project [d_date_sk#444]
                     :                 +- Filter ((isnotnull(d_year#450) AND ((d_year#450 >= 1999) AND (d_year#450 <= 2001))) AND isnotnull(d_date_sk#444))
                     :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#444,d_date_id#445,d_date#446,d_month_seq#447,d_week_seq#448,d_quarter_seq#449,d_year#450,d_dow#451,d_moy#452,d_dom#453,d_qoy#454,d_fy_year#455,d_fy_quarter_seq#456,d_fy_week_seq#457,d_day_name#458,d_quarter_name#459,d_holiday#460,d_weekend#461,d_following_holiday#462,d_first_dom#463,d_last_dom#464,d_same_day_ly#465,d_same_day_lq#466,d_current_day#467,... 4 more fields] parquet
                     +- Aggregate [i_brand_id#670, i_class_id#672, i_category_id#674], [i_brand_id#670, i_class_id#672, i_category_id#674, sum((cast(ws_quantity#647 as double) * ws_list_price#649)) AS sales#11, count(1) AS number_sales#12L]
                        +- Project [ws_quantity#647, ws_list_price#649, i_brand_id#670, i_class_id#672, i_category_id#674]
                           +- Join Inner, (ws_sold_date_sk#629 = d_date_sk#685)
                              :- Project [ws_sold_date_sk#629, ws_quantity#647, ws_list_price#649, i_brand_id#670, i_class_id#672, i_category_id#674]
                              :  +- Join Inner, (ws_item_sk#632 = i_item_sk#663)
                              :     :- Join LeftSemi, (ws_item_sk#632 = ss_item_sk#730)
                              :     :  :- Project [ws_sold_date_sk#629, ws_item_sk#632, ws_quantity#647, ws_list_price#649]
                              :     :  :  +- Filter (isnotnull(ws_item_sk#632) AND isnotnull(ws_sold_date_sk#629))
                              :     :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#629,ws_sold_time_sk#630,ws_ship_date_sk#631,ws_item_sk#632,ws_bill_customer_sk#633,ws_bill_cdemo_sk#634,ws_bill_hdemo_sk#635,ws_bill_addr_sk#636,ws_ship_customer_sk#637,ws_ship_cdemo_sk#638,ws_ship_hdemo_sk#639,ws_ship_addr_sk#640,ws_web_page_sk#641,ws_web_site_sk#642,ws_ship_mode_sk#643,ws_warehouse_sk#644,ws_promo_sk#645,ws_order_number#646,ws_quantity#647,ws_wholesale_cost#648,ws_list_price#649,ws_sales_price#650,ws_ext_discount_amt#651,ws_ext_sales_price#652,... 10 more fields] parquet
                              :     :  +- Project [i_item_sk#2082 AS ss_item_sk#730]
                              :     :     +- Join Inner, (((i_brand_id#2089 = brand_id#15) AND (i_class_id#2091 = class_id#16)) AND (i_category_id#2093 = category_id#17))
                              :     :        :- Project [i_item_sk#2082, i_brand_id#2089, i_class_id#2091, i_category_id#2093]
                              :     :        :  +- Filter ((isnotnull(i_brand_id#2089) AND isnotnull(i_class_id#2091)) AND isnotnull(i_category_id#2093))
                              :     :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#2082,i_item_id#2083,i_rec_start_date#2084,i_rec_end_date#2085,i_item_desc#2086,i_current_price#2087,i_wholesale_cost#2088,i_brand_id#2089,i_brand#2090,i_class_id#2091,i_class#2092,i_category_id#2093,i_category#2094,i_manufact_id#2095,i_manufact#2096,i_size#2097,i_formulation#2098,i_color#2099,i_units#2100,i_container#2101,i_manager_id#2102,i_product_name#2103] parquet
                              :     :        +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#2302) AND (class_id#16 <=> i_class_id#2304)) AND (category_id#17 <=> i_category_id#2306))
                              :     :           :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
                              :     :           :  +- Project [i_brand_id#2134 AS brand_id#15, i_class_id#2136 AS class_id#16, i_category_id#2138 AS category_id#17]
                              :     :           :     +- Join Inner, (ss_sold_date_sk#2104 = d_date_sk#2149)
                              :     :           :        :- Project [ss_sold_date_sk#2104, i_brand_id#2134, i_class_id#2136, i_category_id#2138]
                              :     :           :        :  +- Join Inner, (ss_item_sk#2106 = i_item_sk#2127)
                              :     :           :        :     :- Project [ss_sold_date_sk#2104, ss_item_sk#2106]
                              :     :           :        :     :  +- Filter (isnotnull(ss_item_sk#2106) AND isnotnull(ss_sold_date_sk#2104))
                              :     :           :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#2104,ss_sold_time_sk#2105,ss_item_sk#2106,ss_customer_sk#2107,ss_cdemo_sk#2108,ss_hdemo_sk#2109,ss_addr_sk#2110,ss_store_sk#2111,ss_promo_sk#2112,ss_ticket_number#2113,ss_quantity#2114,ss_wholesale_cost#2115,ss_list_price#2116,ss_sales_price#2117,ss_ext_discount_amt#2118,ss_ext_sales_price#2119,ss_ext_wholesale_cost#2120,ss_ext_list_price#2121,ss_ext_tax#2122,ss_coupon_amt#2123,ss_net_paid#2124,ss_net_paid_inc_tax#2125,ss_net_profit#2126] parquet
                              :     :           :        :     +- Join LeftSemi, (((i_brand_id#2134 <=> i_brand_id#2218) AND (i_class_id#2136 <=> i_class_id#2220)) AND (i_category_id#2138 <=> i_category_id#2222))
                              :     :           :        :        :- Project [i_item_sk#2127, i_brand_id#2134, i_class_id#2136, i_category_id#2138]
                              :     :           :        :        :  +- Filter (isnotnull(i_item_sk#2127) AND ((isnotnull(i_brand_id#2134) AND isnotnull(i_class_id#2136)) AND isnotnull(i_category_id#2138)))
                              :     :           :        :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#2127,i_item_id#2128,i_rec_start_date#2129,i_rec_end_date#2130,i_item_desc#2131,i_current_price#2132,i_wholesale_cost#2133,i_brand_id#2134,i_brand#2135,i_class_id#2136,i_class#2137,i_category_id#2138,i_category#2139,i_manufact_id#2140,i_manufact#2141,i_size#2142,i_formulation#2143,i_color#2144,i_units#2145,i_container#2146,i_manager_id#2147,i_product_name#2148] parquet
                              :     :           :        :        +- Project [i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                              :     :           :        :           +- Join Inner, (cs_sold_date_sk#2177 = d_date_sk#2233)
                              :     :           :        :              :- Project [cs_sold_date_sk#2177, i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                              :     :           :        :              :  +- Join Inner, (cs_item_sk#2192 = i_item_sk#2211)
                              :     :           :        :              :     :- Project [cs_sold_date_sk#2177, cs_item_sk#2192]
                              :     :           :        :              :     :  +- Filter (isnotnull(cs_item_sk#2192) AND isnotnull(cs_sold_date_sk#2177))
                              :     :           :        :              :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#2177,cs_sold_time_sk#2178,cs_ship_date_sk#2179,cs_bill_customer_sk#2180,cs_bill_cdemo_sk#2181,cs_bill_hdemo_sk#2182,cs_bill_addr_sk#2183,cs_ship_customer_sk#2184,cs_ship_cdemo_sk#2185,cs_ship_hdemo_sk#2186,cs_ship_addr_sk#2187,cs_call_center_sk#2188,cs_catalog_page_sk#2189,cs_ship_mode_sk#2190,cs_warehouse_sk#2191,cs_item_sk#2192,cs_promo_sk#2193,cs_order_number#2194,cs_quantity#2195,cs_wholesale_cost#2196,cs_list_price#2197,cs_sales_price#2198,cs_ext_discount_amt#2199,cs_ext_sales_price#2200,... 10 more fields] parquet
                              :     :           :        :              :     +- Project [i_item_sk#2211, i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                              :     :           :        :              :        +- Filter isnotnull(i_item_sk#2211)
                              :     :           :        :              :           +- Relation spark_catalog.tpcds.item[i_item_sk#2211,i_item_id#2212,i_rec_start_date#2213,i_rec_end_date#2214,i_item_desc#2215,i_current_price#2216,i_wholesale_cost#2217,i_brand_id#2218,i_brand#2219,i_class_id#2220,i_class#2221,i_category_id#2222,i_category#2223,i_manufact_id#2224,i_manufact#2225,i_size#2226,i_formulation#2227,i_color#2228,i_units#2229,i_container#2230,i_manager_id#2231,i_product_name#2232] parquet
                              :     :           :        :              +- Project [d_date_sk#2233]
                              :     :           :        :                 +- Filter ((isnotnull(d_year#2239) AND ((d_year#2239 >= 1999) AND (d_year#2239 <= 2001))) AND isnotnull(d_date_sk#2233))
                              :     :           :        :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#2233,d_date_id#2234,d_date#2235,d_month_seq#2236,d_week_seq#2237,d_quarter_seq#2238,d_year#2239,d_dow#2240,d_moy#2241,d_dom#2242,d_qoy#2243,d_fy_year#2244,d_fy_quarter_seq#2245,d_fy_week_seq#2246,d_day_name#2247,d_quarter_name#2248,d_holiday#2249,d_weekend#2250,d_following_holiday#2251,d_first_dom#2252,d_last_dom#2253,d_same_day_ly#2254,d_same_day_lq#2255,d_current_day#2256,... 4 more fields] parquet
                              :     :           :        +- Project [d_date_sk#2149]
                              :     :           :           +- Filter ((isnotnull(d_year#2155) AND ((d_year#2155 >= 1999) AND (d_year#2155 <= 2001))) AND isnotnull(d_date_sk#2149))
                              :     :           :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#2149,d_date_id#2150,d_date#2151,d_month_seq#2152,d_week_seq#2153,d_quarter_seq#2154,d_year#2155,d_dow#2156,d_moy#2157,d_dom#2158,d_qoy#2159,d_fy_year#2160,d_fy_quarter_seq#2161,d_fy_week_seq#2162,d_day_name#2163,d_quarter_name#2164,d_holiday#2165,d_weekend#2166,d_following_holiday#2167,d_first_dom#2168,d_last_dom#2169,d_same_day_ly#2170,d_same_day_lq#2171,d_current_day#2172,... 4 more fields] parquet
                              :     :           +- Project [i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                              :     :              +- Join Inner, (ws_sold_date_sk#2261 = d_date_sk#2317)
                              :     :                 :- Project [ws_sold_date_sk#2261, i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                              :     :                 :  +- Join Inner, (ws_item_sk#2264 = i_item_sk#2295)
                              :     :                 :     :- Project [ws_sold_date_sk#2261, ws_item_sk#2264]
                              :     :                 :     :  +- Filter (isnotnull(ws_item_sk#2264) AND isnotnull(ws_sold_date_sk#2261))
                              :     :                 :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#2261,ws_sold_time_sk#2262,ws_ship_date_sk#2263,ws_item_sk#2264,ws_bill_customer_sk#2265,ws_bill_cdemo_sk#2266,ws_bill_hdemo_sk#2267,ws_bill_addr_sk#2268,ws_ship_customer_sk#2269,ws_ship_cdemo_sk#2270,ws_ship_hdemo_sk#2271,ws_ship_addr_sk#2272,ws_web_page_sk#2273,ws_web_site_sk#2274,ws_ship_mode_sk#2275,ws_warehouse_sk#2276,ws_promo_sk#2277,ws_order_number#2278,ws_quantity#2279,ws_wholesale_cost#2280,ws_list_price#2281,ws_sales_price#2282,ws_ext_discount_amt#2283,ws_ext_sales_price#2284,... 10 more fields] parquet
                              :     :                 :     +- Project [i_item_sk#2295, i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                              :     :                 :        +- Filter isnotnull(i_item_sk#2295)
                              :     :                 :           +- Relation spark_catalog.tpcds.item[i_item_sk#2295,i_item_id#2296,i_rec_start_date#2297,i_rec_end_date#2298,i_item_desc#2299,i_current_price#2300,i_wholesale_cost#2301,i_brand_id#2302,i_brand#2303,i_class_id#2304,i_class#2305,i_category_id#2306,i_category#2307,i_manufact_id#2308,i_manufact#2309,i_size#2310,i_formulation#2311,i_color#2312,i_units#2313,i_container#2314,i_manager_id#2315,i_product_name#2316] parquet
                              :     :                 +- Project [d_date_sk#2317]
                              :     :                    +- Filter ((isnotnull(d_year#2323) AND ((d_year#2323 >= 1999) AND (d_year#2323 <= 2001))) AND isnotnull(d_date_sk#2317))
                              :     :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#2317,d_date_id#2318,d_date#2319,d_month_seq#2320,d_week_seq#2321,d_quarter_seq#2322,d_year#2323,d_dow#2324,d_moy#2325,d_dom#2326,d_qoy#2327,d_fy_year#2328,d_fy_quarter_seq#2329,d_fy_week_seq#2330,d_day_name#2331,d_quarter_name#2332,d_holiday#2333,d_weekend#2334,d_following_holiday#2335,d_first_dom#2336,d_last_dom#2337,d_same_day_ly#2338,d_same_day_lq#2339,d_current_day#2340,... 4 more fields] parquet
                              :     +- Join LeftSemi, (i_item_sk#663 = ss_item_sk#730)
                              :        :- Project [i_item_sk#663, i_brand_id#670, i_class_id#672, i_category_id#674]
                              :        :  +- Filter isnotnull(i_item_sk#663)
                              :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#663,i_item_id#664,i_rec_start_date#665,i_rec_end_date#666,i_item_desc#667,i_current_price#668,i_wholesale_cost#669,i_brand_id#670,i_brand#671,i_class_id#672,i_class#673,i_category_id#674,i_category#675,i_manufact_id#676,i_manufact#677,i_size#678,i_formulation#679,i_color#680,i_units#681,i_container#682,i_manager_id#683,i_product_name#684] parquet
                              :        +- Project [i_item_sk#2082 AS ss_item_sk#730]
                              :           +- Join Inner, (((i_brand_id#2089 = brand_id#15) AND (i_class_id#2091 = class_id#16)) AND (i_category_id#2093 = category_id#17))
                              :              :- Project [i_item_sk#2082, i_brand_id#2089, i_class_id#2091, i_category_id#2093]
                              :              :  +- Filter ((isnotnull(i_brand_id#2089) AND isnotnull(i_class_id#2091)) AND isnotnull(i_category_id#2093))
                              :              :     +- Relation spark_catalog.tpcds.item[i_item_sk#2082,i_item_id#2083,i_rec_start_date#2084,i_rec_end_date#2085,i_item_desc#2086,i_current_price#2087,i_wholesale_cost#2088,i_brand_id#2089,i_brand#2090,i_class_id#2091,i_class#2092,i_category_id#2093,i_category#2094,i_manufact_id#2095,i_manufact#2096,i_size#2097,i_formulation#2098,i_color#2099,i_units#2100,i_container#2101,i_manager_id#2102,i_product_name#2103] parquet
                              :              +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#2302) AND (class_id#16 <=> i_class_id#2304)) AND (category_id#17 <=> i_category_id#2306))
                              :                 :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
                              :                 :  +- Project [i_brand_id#2134 AS brand_id#15, i_class_id#2136 AS class_id#16, i_category_id#2138 AS category_id#17]
                              :                 :     +- Join Inner, (ss_sold_date_sk#2104 = d_date_sk#2149)
                              :                 :        :- Project [ss_sold_date_sk#2104, i_brand_id#2134, i_class_id#2136, i_category_id#2138]
                              :                 :        :  +- Join Inner, (ss_item_sk#2106 = i_item_sk#2127)
                              :                 :        :     :- Project [ss_sold_date_sk#2104, ss_item_sk#2106]
                              :                 :        :     :  +- Filter (isnotnull(ss_item_sk#2106) AND isnotnull(ss_sold_date_sk#2104))
                              :                 :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#2104,ss_sold_time_sk#2105,ss_item_sk#2106,ss_customer_sk#2107,ss_cdemo_sk#2108,ss_hdemo_sk#2109,ss_addr_sk#2110,ss_store_sk#2111,ss_promo_sk#2112,ss_ticket_number#2113,ss_quantity#2114,ss_wholesale_cost#2115,ss_list_price#2116,ss_sales_price#2117,ss_ext_discount_amt#2118,ss_ext_sales_price#2119,ss_ext_wholesale_cost#2120,ss_ext_list_price#2121,ss_ext_tax#2122,ss_coupon_amt#2123,ss_net_paid#2124,ss_net_paid_inc_tax#2125,ss_net_profit#2126] parquet
                              :                 :        :     +- Join LeftSemi, (((i_brand_id#2134 <=> i_brand_id#2218) AND (i_class_id#2136 <=> i_class_id#2220)) AND (i_category_id#2138 <=> i_category_id#2222))
                              :                 :        :        :- Project [i_item_sk#2127, i_brand_id#2134, i_class_id#2136, i_category_id#2138]
                              :                 :        :        :  +- Filter (isnotnull(i_item_sk#2127) AND ((isnotnull(i_brand_id#2134) AND isnotnull(i_class_id#2136)) AND isnotnull(i_category_id#2138)))
                              :                 :        :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#2127,i_item_id#2128,i_rec_start_date#2129,i_rec_end_date#2130,i_item_desc#2131,i_current_price#2132,i_wholesale_cost#2133,i_brand_id#2134,i_brand#2135,i_class_id#2136,i_class#2137,i_category_id#2138,i_category#2139,i_manufact_id#2140,i_manufact#2141,i_size#2142,i_formulation#2143,i_color#2144,i_units#2145,i_container#2146,i_manager_id#2147,i_product_name#2148] parquet
                              :                 :        :        +- Project [i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                              :                 :        :           +- Join Inner, (cs_sold_date_sk#2177 = d_date_sk#2233)
                              :                 :        :              :- Project [cs_sold_date_sk#2177, i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                              :                 :        :              :  +- Join Inner, (cs_item_sk#2192 = i_item_sk#2211)
                              :                 :        :              :     :- Project [cs_sold_date_sk#2177, cs_item_sk#2192]
                              :                 :        :              :     :  +- Filter (isnotnull(cs_item_sk#2192) AND isnotnull(cs_sold_date_sk#2177))
                              :                 :        :              :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#2177,cs_sold_time_sk#2178,cs_ship_date_sk#2179,cs_bill_customer_sk#2180,cs_bill_cdemo_sk#2181,cs_bill_hdemo_sk#2182,cs_bill_addr_sk#2183,cs_ship_customer_sk#2184,cs_ship_cdemo_sk#2185,cs_ship_hdemo_sk#2186,cs_ship_addr_sk#2187,cs_call_center_sk#2188,cs_catalog_page_sk#2189,cs_ship_mode_sk#2190,cs_warehouse_sk#2191,cs_item_sk#2192,cs_promo_sk#2193,cs_order_number#2194,cs_quantity#2195,cs_wholesale_cost#2196,cs_list_price#2197,cs_sales_price#2198,cs_ext_discount_amt#2199,cs_ext_sales_price#2200,... 10 more fields] parquet
                              :                 :        :              :     +- Project [i_item_sk#2211, i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                              :                 :        :              :        +- Filter isnotnull(i_item_sk#2211)
                              :                 :        :              :           +- Relation spark_catalog.tpcds.item[i_item_sk#2211,i_item_id#2212,i_rec_start_date#2213,i_rec_end_date#2214,i_item_desc#2215,i_current_price#2216,i_wholesale_cost#2217,i_brand_id#2218,i_brand#2219,i_class_id#2220,i_class#2221,i_category_id#2222,i_category#2223,i_manufact_id#2224,i_manufact#2225,i_size#2226,i_formulation#2227,i_color#2228,i_units#2229,i_container#2230,i_manager_id#2231,i_product_name#2232] parquet
                              :                 :        :              +- Project [d_date_sk#2233]
                              :                 :        :                 +- Filter ((isnotnull(d_year#2239) AND ((d_year#2239 >= 1999) AND (d_year#2239 <= 2001))) AND isnotnull(d_date_sk#2233))
                              :                 :        :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#2233,d_date_id#2234,d_date#2235,d_month_seq#2236,d_week_seq#2237,d_quarter_seq#2238,d_year#2239,d_dow#2240,d_moy#2241,d_dom#2242,d_qoy#2243,d_fy_year#2244,d_fy_quarter_seq#2245,d_fy_week_seq#2246,d_day_name#2247,d_quarter_name#2248,d_holiday#2249,d_weekend#2250,d_following_holiday#2251,d_first_dom#2252,d_last_dom#2253,d_same_day_ly#2254,d_same_day_lq#2255,d_current_day#2256,... 4 more fields] parquet
                              :                 :        +- Project [d_date_sk#2149]
                              :                 :           +- Filter ((isnotnull(d_year#2155) AND ((d_year#2155 >= 1999) AND (d_year#2155 <= 2001))) AND isnotnull(d_date_sk#2149))
                              :                 :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#2149,d_date_id#2150,d_date#2151,d_month_seq#2152,d_week_seq#2153,d_quarter_seq#2154,d_year#2155,d_dow#2156,d_moy#2157,d_dom#2158,d_qoy#2159,d_fy_year#2160,d_fy_quarter_seq#2161,d_fy_week_seq#2162,d_day_name#2163,d_quarter_name#2164,d_holiday#2165,d_weekend#2166,d_following_holiday#2167,d_first_dom#2168,d_last_dom#2169,d_same_day_ly#2170,d_same_day_lq#2171,d_current_day#2172,... 4 more fields] parquet
                              :                 +- Project [i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                              :                    +- Join Inner, (ws_sold_date_sk#2261 = d_date_sk#2317)
                              :                       :- Project [ws_sold_date_sk#2261, i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                              :                       :  +- Join Inner, (ws_item_sk#2264 = i_item_sk#2295)
                              :                       :     :- Project [ws_sold_date_sk#2261, ws_item_sk#2264]
                              :                       :     :  +- Filter (isnotnull(ws_item_sk#2264) AND isnotnull(ws_sold_date_sk#2261))
                              :                       :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#2261,ws_sold_time_sk#2262,ws_ship_date_sk#2263,ws_item_sk#2264,ws_bill_customer_sk#2265,ws_bill_cdemo_sk#2266,ws_bill_hdemo_sk#2267,ws_bill_addr_sk#2268,ws_ship_customer_sk#2269,ws_ship_cdemo_sk#2270,ws_ship_hdemo_sk#2271,ws_ship_addr_sk#2272,ws_web_page_sk#2273,ws_web_site_sk#2274,ws_ship_mode_sk#2275,ws_warehouse_sk#2276,ws_promo_sk#2277,ws_order_number#2278,ws_quantity#2279,ws_wholesale_cost#2280,ws_list_price#2281,ws_sales_price#2282,ws_ext_discount_amt#2283,ws_ext_sales_price#2284,... 10 more fields] parquet
                              :                       :     +- Project [i_item_sk#2295, i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                              :                       :        +- Filter isnotnull(i_item_sk#2295)
                              :                       :           +- Relation spark_catalog.tpcds.item[i_item_sk#2295,i_item_id#2296,i_rec_start_date#2297,i_rec_end_date#2298,i_item_desc#2299,i_current_price#2300,i_wholesale_cost#2301,i_brand_id#2302,i_brand#2303,i_class_id#2304,i_class#2305,i_category_id#2306,i_category#2307,i_manufact_id#2308,i_manufact#2309,i_size#2310,i_formulation#2311,i_color#2312,i_units#2313,i_container#2314,i_manager_id#2315,i_product_name#2316] parquet
                              :                       +- Project [d_date_sk#2317]
                              :                          +- Filter ((isnotnull(d_year#2323) AND ((d_year#2323 >= 1999) AND (d_year#2323 <= 2001))) AND isnotnull(d_date_sk#2317))
                              :                             +- Relation spark_catalog.tpcds.date_dim[d_date_sk#2317,d_date_id#2318,d_date#2319,d_month_seq#2320,d_week_seq#2321,d_quarter_seq#2322,d_year#2323,d_dow#2324,d_moy#2325,d_dom#2326,d_qoy#2327,d_fy_year#2328,d_fy_quarter_seq#2329,d_fy_week_seq#2330,d_day_name#2331,d_quarter_name#2332,d_holiday#2333,d_weekend#2334,d_following_holiday#2335,d_first_dom#2336,d_last_dom#2337,d_same_day_ly#2338,d_same_day_lq#2339,d_current_day#2340,... 4 more fields] parquet
                              +- Project [d_date_sk#685]
                                 +- Filter (((isnotnull(d_year#691) AND isnotnull(d_moy#693)) AND ((d_year#691 = 2001) AND (d_moy#693 = 11))) AND isnotnull(d_date_sk#685))
                                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#685,d_date_id#686,d_date#687,d_month_seq#688,d_week_seq#689,d_quarter_seq#690,d_year#691,d_dow#692,d_moy#693,d_dom#694,d_qoy#695,d_fy_year#696,d_fy_quarter_seq#697,d_fy_week_seq#698,d_day_name#699,d_quarter_name#700,d_holiday#701,d_weekend#702,d_following_holiday#703,d_first_dom#704,d_last_dom#705,d_same_day_ly#706,d_same_day_lq#707,d_current_day#708,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[channel#758 ASC NULLS FIRST,i_brand_id#759 ASC NULLS FIRST,i_class_id#760 ASC NULLS FIRST,i_category_id#761 ASC NULLS FIRST], output=[channel#758,i_brand_id#759,i_class_id#760,i_category_id#761,sum(sales)#751,sum(number_sales)#752L])
   +- HashAggregate(keys=[channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L], functions=[sum(sales#1), sum(number_sales#2L)], output=[channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, sum(sales)#751, sum(number_sales)#752L])
      +- Exchange hashpartitioning(channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L, 200), ENSURE_REQUIREMENTS, [plan_id=1865]
         +- HashAggregate(keys=[channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L], functions=[partial_sum(sales#1), partial_sum(number_sales#2L)], output=[channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L, sum#2532, sum#2533L])
            +- Expand [[sales#1, number_sales#2L, channel#753, i_brand_id#754, i_class_id#755, i_category_id#756, 0], [sales#1, number_sales#2L, channel#753, i_brand_id#754, i_class_id#755, null, 1], [sales#1, number_sales#2L, channel#753, i_brand_id#754, null, null, 3], [sales#1, number_sales#2L, channel#753, null, null, null, 7], [sales#1, number_sales#2L, null, null, null, null, 15]], [sales#1, number_sales#2L, channel#758, i_brand_id#759, i_class_id#760, i_category_id#761, spark_grouping_id#757L]
               +- Union
                  :- Project [sales#1, number_sales#2L, store AS channel#753, i_brand_id#502 AS i_brand_id#754, i_class_id#504 AS i_class_id#755, i_category_id#506 AS i_category_id#756]
                  :  +- Filter (isnotnull(sales#1) AND (sales#1 > Subquery subquery#4, [id=#1355]))
                  :     :  +- Subquery subquery#4, [id=#1355]
                  :     :     +- AdaptiveSparkPlan isFinalPlan=false
                  :     :        +- HashAggregate(keys=[], functions=[avg((cast(quantity#19 as double) * list_price#20))], output=[average_sales#25])
                  :     :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1195]
                  :     :              +- HashAggregate(keys=[], functions=[partial_avg((cast(quantity#19 as double) * list_price#20))], output=[sum#2548, count#2549L])
                  :     :                 +- Union
                  :     :                    :- Project [ss_quantity#307 AS quantity#19, ss_list_price#309 AS list_price#20]
                  :     :                    :  +- BroadcastHashJoin [ss_sold_date_sk#297], [d_date_sk#320], Inner, BuildRight, false
                  :     :                    :     :- Filter isnotnull(ss_sold_date_sk#297)
                  :     :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#297,ss_quantity#307,ss_list_price#309] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#297)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_quantity:int,ss_list_price:double>
                  :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1183]
                  :     :                    :        +- Project [d_date_sk#320]
                  :     :                    :           +- Filter (((isnotnull(d_year#326) AND (d_year#326 >= 1999)) AND (d_year#326 <= 2001)) AND isnotnull(d_date_sk#320))
                  :     :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#320,d_year#326] Batched: true, DataFilters: [isnotnull(d_year#326), (d_year#326 >= 1999), (d_year#326 <= 2001), isnotnull(d_date_sk#320)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     :                    :- Project [cs_quantity#366 AS quantity#21, cs_list_price#368 AS list_price#22]
                  :     :                    :  +- BroadcastHashJoin [cs_sold_date_sk#348], [d_date_sk#382], Inner, BuildRight, false
                  :     :                    :     :- Filter isnotnull(cs_sold_date_sk#348)
                  :     :                    :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_quantity#366,cs_list_price#368] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#348)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_quantity:int,cs_list_price:double>
                  :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1186]
                  :     :                    :        +- Project [d_date_sk#382]
                  :     :                    :           +- Filter (((isnotnull(d_year#388) AND (d_year#388 >= 1999)) AND (d_year#388 <= 2001)) AND isnotnull(d_date_sk#382))
                  :     :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#382,d_year#388] Batched: true, DataFilters: [isnotnull(d_year#388), (d_year#388 >= 1999), (d_year#388 <= 2001), isnotnull(d_date_sk#382)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     :                    +- Project [ws_quantity#428 AS quantity#23, ws_list_price#430 AS list_price#24]
                  :     :                       +- BroadcastHashJoin [ws_sold_date_sk#410], [d_date_sk#444], Inner, BuildRight, false
                  :     :                          :- Filter isnotnull(ws_sold_date_sk#410)
                  :     :                          :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#410,ws_quantity#428,ws_list_price#430] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#410)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_quantity:int,ws_list_price:double>
                  :     :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1189]
                  :     :                             +- Project [d_date_sk#444]
                  :     :                                +- Filter (((isnotnull(d_year#450) AND (d_year#450 >= 1999)) AND (d_year#450 <= 2001)) AND isnotnull(d_date_sk#444))
                  :     :                                   +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#444,d_year#450] Batched: true, DataFilters: [isnotnull(d_year#450), (d_year#450 >= 1999), (d_year#450 <= 2001), isnotnull(d_date_sk#444)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     +- HashAggregate(keys=[i_brand_id#502, i_class_id#504, i_category_id#506], functions=[sum((cast(ss_quantity#482 as double) * ss_list_price#484)), count(1)], output=[i_brand_id#502, i_class_id#504, i_category_id#506, sales#1, number_sales#2L])
                  :        +- Exchange hashpartitioning(i_brand_id#502, i_class_id#504, i_category_id#506, 200), ENSURE_REQUIREMENTS, [plan_id=1625]
                  :           +- HashAggregate(keys=[i_brand_id#502, i_class_id#504, i_category_id#506], functions=[partial_sum((cast(ss_quantity#482 as double) * ss_list_price#484)), partial_count(1)], output=[i_brand_id#502, i_class_id#504, i_category_id#506, sum#2536, count#2537L])
                  :              +- Project [ss_quantity#482, ss_list_price#484, i_brand_id#502, i_class_id#504, i_category_id#506]
                  :                 +- BroadcastHashJoin [ss_sold_date_sk#472], [d_date_sk#517], Inner, BuildRight, false
                  :                    :- Project [ss_sold_date_sk#472, ss_quantity#482, ss_list_price#484, i_brand_id#502, i_class_id#504, i_category_id#506]
                  :                    :  +- BroadcastHashJoin [ss_item_sk#474], [i_item_sk#495], Inner, BuildRight, false
                  :                    :     :- SortMergeJoin [ss_item_sk#474], [ss_item_sk#18], LeftSemi
                  :                    :     :  :- Sort [ss_item_sk#474 ASC NULLS FIRST], false, 0
                  :                    :     :  :  +- Exchange hashpartitioning(ss_item_sk#474, 200), ENSURE_REQUIREMENTS, [plan_id=1559]
                  :                    :     :  :     +- Filter (isnotnull(ss_item_sk#474) AND isnotnull(ss_sold_date_sk#472))
                  :                    :     :  :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#472,ss_item_sk#474,ss_quantity#482,ss_list_price#484] Batched: true, DataFilters: [isnotnull(ss_item_sk#474), isnotnull(ss_sold_date_sk#472)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_quantity:int,ss_list_price:double>
                  :                    :     :  +- Sort [ss_item_sk#18 ASC NULLS FIRST], false, 0
                  :                    :     :     +- Exchange hashpartitioning(ss_item_sk#18, 200), ENSURE_REQUIREMENTS, [plan_id=1560]
                  :                    :     :        +- Project [i_item_sk#34 AS ss_item_sk#18]
                  :                    :     :           +- BroadcastHashJoin [i_brand_id#41, i_class_id#43, i_category_id#45], [brand_id#15, class_id#16, category_id#17], Inner, BuildLeft, false
                  :                    :     :              :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, int, false], input[2, int, false], input[3, int, false]),false), [plan_id=1554]
                  :                    :     :              :  +- Filter ((isnotnull(i_brand_id#41) AND isnotnull(i_class_id#43)) AND isnotnull(i_category_id#45))
                  :                    :     :              :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#34,i_brand_id#41,i_class_id#43,i_category_id#45] Batched: true, DataFilters: [isnotnull(i_brand_id#41), isnotnull(i_class_id#43), isnotnull(i_category_id#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :              +- SortMergeJoin [coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17)], [coalesce(i_brand_id#254, 0), isnull(i_brand_id#254), coalesce(i_class_id#256, 0), isnull(i_class_id#256), coalesce(i_category_id#258, 0), isnull(i_category_id#258)], LeftSemi
                  :                    :     :                 :- Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, isnull(brand_id#15) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, isnull(class_id#16) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST, isnull(category_id#17) ASC NULLS FIRST], false, 0
                  :                    :     :                 :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17), 200), ENSURE_REQUIREMENTS, [plan_id=1548]
                  :                    :     :                 :     +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :     :                 :        +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200), ENSURE_REQUIREMENTS, [plan_id=1537]
                  :                    :     :                 :           +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :     :                 :              +- Project [i_brand_id#182 AS brand_id#15, i_class_id#184 AS class_id#16, i_category_id#186 AS category_id#17]
                  :                    :     :                 :                 +- BroadcastHashJoin [ss_sold_date_sk#56], [d_date_sk#79], Inner, BuildRight, false
                  :                    :     :                 :                    :- Project [ss_sold_date_sk#56, i_brand_id#182, i_class_id#184, i_category_id#186]
                  :                    :     :                 :                    :  +- BroadcastHashJoin [ss_item_sk#58], [i_item_sk#175], Inner, BuildRight, false
                  :                    :     :                 :                    :     :- Filter (isnotnull(ss_item_sk#58) AND isnotnull(ss_sold_date_sk#56))
                  :                    :     :                 :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#56,ss_item_sk#58] Batched: true, DataFilters: [isnotnull(ss_item_sk#58), isnotnull(ss_sold_date_sk#56)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
                  :                    :     :                 :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1528]
                  :                    :     :                 :                    :        +- SortMergeJoin [coalesce(i_brand_id#182, 0), isnull(i_brand_id#182), coalesce(i_class_id#184, 0), isnull(i_class_id#184), coalesce(i_category_id#186, 0), isnull(i_category_id#186)], [coalesce(i_brand_id#204, 0), isnull(i_brand_id#204), coalesce(i_class_id#206, 0), isnull(i_class_id#206), coalesce(i_category_id#208, 0), isnull(i_category_id#208)], LeftSemi
                  :                    :     :                 :                    :           :- Sort [coalesce(i_brand_id#182, 0) ASC NULLS FIRST, isnull(i_brand_id#182) ASC NULLS FIRST, coalesce(i_class_id#184, 0) ASC NULLS FIRST, isnull(i_class_id#184) ASC NULLS FIRST, coalesce(i_category_id#186, 0) ASC NULLS FIRST, isnull(i_category_id#186) ASC NULLS FIRST], false, 0
                  :                    :     :                 :                    :           :  +- Exchange hashpartitioning(coalesce(i_brand_id#182, 0), isnull(i_brand_id#182), coalesce(i_class_id#184, 0), isnull(i_class_id#184), coalesce(i_category_id#186, 0), isnull(i_category_id#186), 200), ENSURE_REQUIREMENTS, [plan_id=1522]
                  :                    :     :                 :                    :           :     +- Filter (((isnotnull(i_item_sk#175) AND isnotnull(i_brand_id#182)) AND isnotnull(i_class_id#184)) AND isnotnull(i_category_id#186))
                  :                    :     :                 :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#175,i_brand_id#182,i_class_id#184,i_category_id#186] Batched: true, DataFilters: [isnotnull(i_item_sk#175), isnotnull(i_brand_id#182), isnotnull(i_class_id#184), isnotnull(i_cate..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :                 :                    :           +- Sort [coalesce(i_brand_id#204, 0) ASC NULLS FIRST, isnull(i_brand_id#204) ASC NULLS FIRST, coalesce(i_class_id#206, 0) ASC NULLS FIRST, isnull(i_class_id#206) ASC NULLS FIRST, coalesce(i_category_id#208, 0) ASC NULLS FIRST, isnull(i_category_id#208) ASC NULLS FIRST], false, 0
                  :                    :     :                 :                    :              +- Exchange hashpartitioning(coalesce(i_brand_id#204, 0), isnull(i_brand_id#204), coalesce(i_class_id#206, 0), isnull(i_class_id#206), coalesce(i_category_id#208, 0), isnull(i_category_id#208), 200), ENSURE_REQUIREMENTS, [plan_id=1523]
                  :                    :     :                 :                    :                 +- Project [i_brand_id#204, i_class_id#206, i_category_id#208]
                  :                    :     :                 :                    :                    +- BroadcastHashJoin [cs_sold_date_sk#107], [d_date_sk#219], Inner, BuildRight, false
                  :                    :     :                 :                    :                       :- Project [cs_sold_date_sk#107, i_brand_id#204, i_class_id#206, i_category_id#208]
                  :                    :     :                 :                    :                       :  +- BroadcastHashJoin [cs_item_sk#122], [i_item_sk#197], Inner, BuildRight, false
                  :                    :     :                 :                    :                       :     :- Filter (isnotnull(cs_item_sk#122) AND isnotnull(cs_sold_date_sk#107))
                  :                    :     :                 :                    :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#107,cs_item_sk#122] Batched: true, DataFilters: [isnotnull(cs_item_sk#122), isnotnull(cs_sold_date_sk#107)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int>
                  :                    :     :                 :                    :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1513]
                  :                    :     :                 :                    :                       :        +- Filter isnotnull(i_item_sk#197)
                  :                    :     :                 :                    :                       :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#197,i_brand_id#204,i_class_id#206,i_category_id#208] Batched: true, DataFilters: [isnotnull(i_item_sk#197)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :                 :                    :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1517]
                  :                    :     :                 :                    :                          +- Project [d_date_sk#219]
                  :                    :     :                 :                    :                             +- Filter (((isnotnull(d_year#225) AND (d_year#225 >= 1999)) AND (d_year#225 <= 2001)) AND isnotnull(d_date_sk#219))
                  :                    :     :                 :                    :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#219,d_year#225] Batched: true, DataFilters: [isnotnull(d_year#225), (d_year#225 >= 1999), (d_year#225 <= 2001), isnotnull(d_date_sk#219)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :     :                 :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1532]
                  :                    :     :                 :                       +- Project [d_date_sk#79]
                  :                    :     :                 :                          +- Filter (((isnotnull(d_year#85) AND (d_year#85 >= 1999)) AND (d_year#85 <= 2001)) AND isnotnull(d_date_sk#79))
                  :                    :     :                 :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#79,d_year#85] Batched: true, DataFilters: [isnotnull(d_year#85), (d_year#85 >= 1999), (d_year#85 <= 2001), isnotnull(d_date_sk#79)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :     :                 +- Sort [coalesce(i_brand_id#254, 0) ASC NULLS FIRST, isnull(i_brand_id#254) ASC NULLS FIRST, coalesce(i_class_id#256, 0) ASC NULLS FIRST, isnull(i_class_id#256) ASC NULLS FIRST, coalesce(i_category_id#258, 0) ASC NULLS FIRST, isnull(i_category_id#258) ASC NULLS FIRST], false, 0
                  :                    :     :                    +- Exchange hashpartitioning(coalesce(i_brand_id#254, 0), isnull(i_brand_id#254), coalesce(i_class_id#256, 0), isnull(i_class_id#256), coalesce(i_category_id#258, 0), isnull(i_category_id#258), 200), ENSURE_REQUIREMENTS, [plan_id=1549]
                  :                    :     :                       +- Project [i_brand_id#254, i_class_id#256, i_category_id#258]
                  :                    :     :                          +- BroadcastHashJoin [ws_sold_date_sk#141], [d_date_sk#269], Inner, BuildRight, false
                  :                    :     :                             :- Project [ws_sold_date_sk#141, i_brand_id#254, i_class_id#256, i_category_id#258]
                  :                    :     :                             :  +- BroadcastHashJoin [ws_item_sk#144], [i_item_sk#247], Inner, BuildRight, false
                  :                    :     :                             :     :- Filter (isnotnull(ws_item_sk#144) AND isnotnull(ws_sold_date_sk#141))
                  :                    :     :                             :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#141,ws_item_sk#144] Batched: true, DataFilters: [isnotnull(ws_item_sk#144), isnotnull(ws_sold_date_sk#141)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int>
                  :                    :     :                             :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1539]
                  :                    :     :                             :        +- Filter isnotnull(i_item_sk#247)
                  :                    :     :                             :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#247,i_brand_id#254,i_class_id#256,i_category_id#258] Batched: true, DataFilters: [isnotnull(i_item_sk#247)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1543]
                  :                    :     :                                +- Project [d_date_sk#269]
                  :                    :     :                                   +- Filter (((isnotnull(d_year#275) AND (d_year#275 >= 1999)) AND (d_year#275 <= 2001)) AND isnotnull(d_date_sk#269))
                  :                    :     :                                      +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#269,d_year#275] Batched: true, DataFilters: [isnotnull(d_year#275), (d_year#275 >= 1999), (d_year#275 <= 2001), isnotnull(d_date_sk#269)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1616]
                  :                    :        +- SortMergeJoin [i_item_sk#495], [ss_item_sk#18], LeftSemi
                  :                    :           :- Sort [i_item_sk#495 ASC NULLS FIRST], false, 0
                  :                    :           :  +- Exchange hashpartitioning(i_item_sk#495, 200), ENSURE_REQUIREMENTS, [plan_id=1610]
                  :                    :           :     +- Filter isnotnull(i_item_sk#495)
                  :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#495,i_brand_id#502,i_class_id#504,i_category_id#506] Batched: true, DataFilters: [isnotnull(i_item_sk#495)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :           +- Sort [ss_item_sk#18 ASC NULLS FIRST], false, 0
                  :                    :              +- Exchange hashpartitioning(ss_item_sk#18, 200), ENSURE_REQUIREMENTS, [plan_id=1611]
                  :                    :                 +- Project [i_item_sk#34 AS ss_item_sk#18]
                  :                    :                    +- BroadcastHashJoin [i_brand_id#41, i_class_id#43, i_category_id#45], [brand_id#15, class_id#16, category_id#17], Inner, BuildLeft, false
                  :                    :                       :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, int, false], input[2, int, false], input[3, int, false]),false), [plan_id=1605]
                  :                    :                       :  +- Filter ((isnotnull(i_brand_id#41) AND isnotnull(i_class_id#43)) AND isnotnull(i_category_id#45))
                  :                    :                       :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#34,i_brand_id#41,i_class_id#43,i_category_id#45] Batched: true, DataFilters: [isnotnull(i_brand_id#41), isnotnull(i_class_id#43), isnotnull(i_category_id#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                       +- SortMergeJoin [coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17)], [coalesce(i_brand_id#254, 0), isnull(i_brand_id#254), coalesce(i_class_id#256, 0), isnull(i_class_id#256), coalesce(i_category_id#258, 0), isnull(i_category_id#258)], LeftSemi
                  :                    :                          :- Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, isnull(brand_id#15) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, isnull(class_id#16) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST, isnull(category_id#17) ASC NULLS FIRST], false, 0
                  :                    :                          :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17), 200), ENSURE_REQUIREMENTS, [plan_id=1599]
                  :                    :                          :     +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :                          :        +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200), ENSURE_REQUIREMENTS, [plan_id=1588]
                  :                    :                          :           +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :                          :              +- Project [i_brand_id#182 AS brand_id#15, i_class_id#184 AS class_id#16, i_category_id#186 AS category_id#17]
                  :                    :                          :                 +- BroadcastHashJoin [ss_sold_date_sk#56], [d_date_sk#79], Inner, BuildRight, false
                  :                    :                          :                    :- Project [ss_sold_date_sk#56, i_brand_id#182, i_class_id#184, i_category_id#186]
                  :                    :                          :                    :  +- BroadcastHashJoin [ss_item_sk#58], [i_item_sk#175], Inner, BuildRight, false
                  :                    :                          :                    :     :- Filter (isnotnull(ss_item_sk#58) AND isnotnull(ss_sold_date_sk#56))
                  :                    :                          :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#56,ss_item_sk#58] Batched: true, DataFilters: [isnotnull(ss_item_sk#58), isnotnull(ss_sold_date_sk#56)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
                  :                    :                          :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1579]
                  :                    :                          :                    :        +- SortMergeJoin [coalesce(i_brand_id#182, 0), isnull(i_brand_id#182), coalesce(i_class_id#184, 0), isnull(i_class_id#184), coalesce(i_category_id#186, 0), isnull(i_category_id#186)], [coalesce(i_brand_id#204, 0), isnull(i_brand_id#204), coalesce(i_class_id#206, 0), isnull(i_class_id#206), coalesce(i_category_id#208, 0), isnull(i_category_id#208)], LeftSemi
                  :                    :                          :                    :           :- Sort [coalesce(i_brand_id#182, 0) ASC NULLS FIRST, isnull(i_brand_id#182) ASC NULLS FIRST, coalesce(i_class_id#184, 0) ASC NULLS FIRST, isnull(i_class_id#184) ASC NULLS FIRST, coalesce(i_category_id#186, 0) ASC NULLS FIRST, isnull(i_category_id#186) ASC NULLS FIRST], false, 0
                  :                    :                          :                    :           :  +- Exchange hashpartitioning(coalesce(i_brand_id#182, 0), isnull(i_brand_id#182), coalesce(i_class_id#184, 0), isnull(i_class_id#184), coalesce(i_category_id#186, 0), isnull(i_category_id#186), 200), ENSURE_REQUIREMENTS, [plan_id=1573]
                  :                    :                          :                    :           :     +- Filter (((isnotnull(i_item_sk#175) AND isnotnull(i_brand_id#182)) AND isnotnull(i_class_id#184)) AND isnotnull(i_category_id#186))
                  :                    :                          :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#175,i_brand_id#182,i_class_id#184,i_category_id#186] Batched: true, DataFilters: [isnotnull(i_item_sk#175), isnotnull(i_brand_id#182), isnotnull(i_class_id#184), isnotnull(i_cate..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                          :                    :           +- Sort [coalesce(i_brand_id#204, 0) ASC NULLS FIRST, isnull(i_brand_id#204) ASC NULLS FIRST, coalesce(i_class_id#206, 0) ASC NULLS FIRST, isnull(i_class_id#206) ASC NULLS FIRST, coalesce(i_category_id#208, 0) ASC NULLS FIRST, isnull(i_category_id#208) ASC NULLS FIRST], false, 0
                  :                    :                          :                    :              +- Exchange hashpartitioning(coalesce(i_brand_id#204, 0), isnull(i_brand_id#204), coalesce(i_class_id#206, 0), isnull(i_class_id#206), coalesce(i_category_id#208, 0), isnull(i_category_id#208), 200), ENSURE_REQUIREMENTS, [plan_id=1574]
                  :                    :                          :                    :                 +- Project [i_brand_id#204, i_class_id#206, i_category_id#208]
                  :                    :                          :                    :                    +- BroadcastHashJoin [cs_sold_date_sk#107], [d_date_sk#219], Inner, BuildRight, false
                  :                    :                          :                    :                       :- Project [cs_sold_date_sk#107, i_brand_id#204, i_class_id#206, i_category_id#208]
                  :                    :                          :                    :                       :  +- BroadcastHashJoin [cs_item_sk#122], [i_item_sk#197], Inner, BuildRight, false
                  :                    :                          :                    :                       :     :- Filter (isnotnull(cs_item_sk#122) AND isnotnull(cs_sold_date_sk#107))
                  :                    :                          :                    :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#107,cs_item_sk#122] Batched: true, DataFilters: [isnotnull(cs_item_sk#122), isnotnull(cs_sold_date_sk#107)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int>
                  :                    :                          :                    :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1564]
                  :                    :                          :                    :                       :        +- Filter isnotnull(i_item_sk#197)
                  :                    :                          :                    :                       :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#197,i_brand_id#204,i_class_id#206,i_category_id#208] Batched: true, DataFilters: [isnotnull(i_item_sk#197)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                          :                    :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1568]
                  :                    :                          :                    :                          +- Project [d_date_sk#219]
                  :                    :                          :                    :                             +- Filter (((isnotnull(d_year#225) AND (d_year#225 >= 1999)) AND (d_year#225 <= 2001)) AND isnotnull(d_date_sk#219))
                  :                    :                          :                    :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#219,d_year#225] Batched: true, DataFilters: [isnotnull(d_year#225), (d_year#225 >= 1999), (d_year#225 <= 2001), isnotnull(d_date_sk#219)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :                          :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1583]
                  :                    :                          :                       +- Project [d_date_sk#79]
                  :                    :                          :                          +- Filter (((isnotnull(d_year#85) AND (d_year#85 >= 1999)) AND (d_year#85 <= 2001)) AND isnotnull(d_date_sk#79))
                  :                    :                          :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#79,d_year#85] Batched: true, DataFilters: [isnotnull(d_year#85), (d_year#85 >= 1999), (d_year#85 <= 2001), isnotnull(d_date_sk#79)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :                          +- Sort [coalesce(i_brand_id#254, 0) ASC NULLS FIRST, isnull(i_brand_id#254) ASC NULLS FIRST, coalesce(i_class_id#256, 0) ASC NULLS FIRST, isnull(i_class_id#256) ASC NULLS FIRST, coalesce(i_category_id#258, 0) ASC NULLS FIRST, isnull(i_category_id#258) ASC NULLS FIRST], false, 0
                  :                    :                             +- Exchange hashpartitioning(coalesce(i_brand_id#254, 0), isnull(i_brand_id#254), coalesce(i_class_id#256, 0), isnull(i_class_id#256), coalesce(i_category_id#258, 0), isnull(i_category_id#258), 200), ENSURE_REQUIREMENTS, [plan_id=1600]
                  :                    :                                +- Project [i_brand_id#254, i_class_id#256, i_category_id#258]
                  :                    :                                   +- BroadcastHashJoin [ws_sold_date_sk#141], [d_date_sk#269], Inner, BuildRight, false
                  :                    :                                      :- Project [ws_sold_date_sk#141, i_brand_id#254, i_class_id#256, i_category_id#258]
                  :                    :                                      :  +- BroadcastHashJoin [ws_item_sk#144], [i_item_sk#247], Inner, BuildRight, false
                  :                    :                                      :     :- Filter (isnotnull(ws_item_sk#144) AND isnotnull(ws_sold_date_sk#141))
                  :                    :                                      :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#141,ws_item_sk#144] Batched: true, DataFilters: [isnotnull(ws_item_sk#144), isnotnull(ws_sold_date_sk#141)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int>
                  :                    :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1590]
                  :                    :                                      :        +- Filter isnotnull(i_item_sk#247)
                  :                    :                                      :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#247,i_brand_id#254,i_class_id#256,i_category_id#258] Batched: true, DataFilters: [isnotnull(i_item_sk#247)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1594]
                  :                    :                                         +- Project [d_date_sk#269]
                  :                    :                                            +- Filter (((isnotnull(d_year#275) AND (d_year#275 >= 1999)) AND (d_year#275 <= 2001)) AND isnotnull(d_date_sk#269))
                  :                    :                                               +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#269,d_year#275] Batched: true, DataFilters: [isnotnull(d_year#275), (d_year#275 >= 1999), (d_year#275 <= 2001), isnotnull(d_date_sk#269)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1620]
                  :                       +- Project [d_date_sk#517]
                  :                          +- Filter ((((isnotnull(d_year#523) AND isnotnull(d_moy#525)) AND (d_year#523 = 2001)) AND (d_moy#525 = 11)) AND isnotnull(d_date_sk#517))
                  :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#517,d_year#523,d_moy#525] Batched: true, DataFilters: [isnotnull(d_year#523), isnotnull(d_moy#525), (d_year#523 = 2001), (d_moy#525 = 11), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :- Project [sales#6, number_sales#7L, catalog AS channel#2522, i_brand_id#586, i_class_id#588, i_category_id#590]
                  :  +- Filter (isnotnull(sales#6) AND (sales#6 > Subquery subquery#9, [id=#1358]))
                  :     :  +- Subquery subquery#9, [id=#1358]
                  :     :     +- AdaptiveSparkPlan isFinalPlan=false
                  :     :        +- HashAggregate(keys=[], functions=[avg((cast(quantity#19 as double) * list_price#20))], output=[average_sales#25])
                  :     :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1274]
                  :     :              +- HashAggregate(keys=[], functions=[partial_avg((cast(quantity#19 as double) * list_price#20))], output=[sum#2548, count#2549L])
                  :     :                 +- Union
                  :     :                    :- Project [ss_quantity#307 AS quantity#19, ss_list_price#309 AS list_price#20]
                  :     :                    :  +- BroadcastHashJoin [ss_sold_date_sk#297], [d_date_sk#320], Inner, BuildRight, false
                  :     :                    :     :- Filter isnotnull(ss_sold_date_sk#297)
                  :     :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#297,ss_quantity#307,ss_list_price#309] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#297)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_quantity:int,ss_list_price:double>
                  :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1262]
                  :     :                    :        +- Project [d_date_sk#320]
                  :     :                    :           +- Filter (((isnotnull(d_year#326) AND (d_year#326 >= 1999)) AND (d_year#326 <= 2001)) AND isnotnull(d_date_sk#320))
                  :     :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#320,d_year#326] Batched: true, DataFilters: [isnotnull(d_year#326), (d_year#326 >= 1999), (d_year#326 <= 2001), isnotnull(d_date_sk#320)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     :                    :- Project [cs_quantity#366 AS quantity#21, cs_list_price#368 AS list_price#22]
                  :     :                    :  +- BroadcastHashJoin [cs_sold_date_sk#348], [d_date_sk#382], Inner, BuildRight, false
                  :     :                    :     :- Filter isnotnull(cs_sold_date_sk#348)
                  :     :                    :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_quantity#366,cs_list_price#368] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#348)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_quantity:int,cs_list_price:double>
                  :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1265]
                  :     :                    :        +- Project [d_date_sk#382]
                  :     :                    :           +- Filter (((isnotnull(d_year#388) AND (d_year#388 >= 1999)) AND (d_year#388 <= 2001)) AND isnotnull(d_date_sk#382))
                  :     :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#382,d_year#388] Batched: true, DataFilters: [isnotnull(d_year#388), (d_year#388 >= 1999), (d_year#388 <= 2001), isnotnull(d_date_sk#382)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     :                    +- Project [ws_quantity#428 AS quantity#23, ws_list_price#430 AS list_price#24]
                  :     :                       +- BroadcastHashJoin [ws_sold_date_sk#410], [d_date_sk#444], Inner, BuildRight, false
                  :     :                          :- Filter isnotnull(ws_sold_date_sk#410)
                  :     :                          :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#410,ws_quantity#428,ws_list_price#430] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#410)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_quantity:int,ws_list_price:double>
                  :     :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1268]
                  :     :                             +- Project [d_date_sk#444]
                  :     :                                +- Filter (((isnotnull(d_year#450) AND (d_year#450 >= 1999)) AND (d_year#450 <= 2001)) AND isnotnull(d_date_sk#444))
                  :     :                                   +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#444,d_year#450] Batched: true, DataFilters: [isnotnull(d_year#450), (d_year#450 >= 1999), (d_year#450 <= 2001), isnotnull(d_date_sk#444)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     +- HashAggregate(keys=[i_brand_id#586, i_class_id#588, i_category_id#590], functions=[sum((cast(cs_quantity#563 as double) * cs_list_price#565)), count(1)], output=[i_brand_id#586, i_class_id#588, i_category_id#590, sales#6, number_sales#7L])
                  :        +- Exchange hashpartitioning(i_brand_id#586, i_class_id#588, i_category_id#590, 200), ENSURE_REQUIREMENTS, [plan_id=1741]
                  :           +- HashAggregate(keys=[i_brand_id#586, i_class_id#588, i_category_id#590], functions=[partial_sum((cast(cs_quantity#563 as double) * cs_list_price#565)), partial_count(1)], output=[i_brand_id#586, i_class_id#588, i_category_id#590, sum#2540, count#2541L])
                  :              +- Project [cs_quantity#563, cs_list_price#565, i_brand_id#586, i_class_id#588, i_category_id#590]
                  :                 +- BroadcastHashJoin [cs_sold_date_sk#545], [d_date_sk#601], Inner, BuildRight, false
                  :                    :- Project [cs_sold_date_sk#545, cs_quantity#563, cs_list_price#565, i_brand_id#586, i_class_id#588, i_category_id#590]
                  :                    :  +- BroadcastHashJoin [cs_item_sk#560], [i_item_sk#579], Inner, BuildRight, false
                  :                    :     :- SortMergeJoin [cs_item_sk#560], [ss_item_sk#729], LeftSemi
                  :                    :     :  :- Sort [cs_item_sk#560 ASC NULLS FIRST], false, 0
                  :                    :     :  :  +- Exchange hashpartitioning(cs_item_sk#560, 200), ENSURE_REQUIREMENTS, [plan_id=1675]
                  :                    :     :  :     +- Filter (isnotnull(cs_item_sk#560) AND isnotnull(cs_sold_date_sk#545))
                  :                    :     :  :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#545,cs_item_sk#560,cs_quantity#563,cs_list_price#565] Batched: true, DataFilters: [isnotnull(cs_item_sk#560), isnotnull(cs_sold_date_sk#545)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_quantity:int,cs_list_price:double>
                  :                    :     :  +- Sort [ss_item_sk#729 ASC NULLS FIRST], false, 0
                  :                    :     :     +- Exchange hashpartitioning(ss_item_sk#729, 200), ENSURE_REQUIREMENTS, [plan_id=1676]
                  :                    :     :        +- Project [i_item_sk#1642 AS ss_item_sk#729]
                  :                    :     :           +- BroadcastHashJoin [i_brand_id#1649, i_class_id#1651, i_category_id#1653], [brand_id#15, class_id#16, category_id#17], Inner, BuildLeft, false
                  :                    :     :              :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, int, false], input[2, int, false], input[3, int, false]),false), [plan_id=1670]
                  :                    :     :              :  +- Filter ((isnotnull(i_brand_id#1649) AND isnotnull(i_class_id#1651)) AND isnotnull(i_category_id#1653))
                  :                    :     :              :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1642,i_brand_id#1649,i_class_id#1651,i_category_id#1653] Batched: true, DataFilters: [isnotnull(i_brand_id#1649), isnotnull(i_class_id#1651), isnotnull(i_category_id#1653)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :              +- SortMergeJoin [coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17)], [coalesce(i_brand_id#1862, 0), isnull(i_brand_id#1862), coalesce(i_class_id#1864, 0), isnull(i_class_id#1864), coalesce(i_category_id#1866, 0), isnull(i_category_id#1866)], LeftSemi
                  :                    :     :                 :- Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, isnull(brand_id#15) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, isnull(class_id#16) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST, isnull(category_id#17) ASC NULLS FIRST], false, 0
                  :                    :     :                 :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17), 200), ENSURE_REQUIREMENTS, [plan_id=1664]
                  :                    :     :                 :     +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :     :                 :        +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200), ENSURE_REQUIREMENTS, [plan_id=1653]
                  :                    :     :                 :           +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :     :                 :              +- Project [i_brand_id#1694 AS brand_id#15, i_class_id#1696 AS class_id#16, i_category_id#1698 AS category_id#17]
                  :                    :     :                 :                 +- BroadcastHashJoin [ss_sold_date_sk#1664], [d_date_sk#1709], Inner, BuildRight, false
                  :                    :     :                 :                    :- Project [ss_sold_date_sk#1664, i_brand_id#1694, i_class_id#1696, i_category_id#1698]
                  :                    :     :                 :                    :  +- BroadcastHashJoin [ss_item_sk#1666], [i_item_sk#1687], Inner, BuildRight, false
                  :                    :     :                 :                    :     :- Filter (isnotnull(ss_item_sk#1666) AND isnotnull(ss_sold_date_sk#1664))
                  :                    :     :                 :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#1664,ss_item_sk#1666] Batched: true, DataFilters: [isnotnull(ss_item_sk#1666), isnotnull(ss_sold_date_sk#1664)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
                  :                    :     :                 :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1644]
                  :                    :     :                 :                    :        +- SortMergeJoin [coalesce(i_brand_id#1694, 0), isnull(i_brand_id#1694), coalesce(i_class_id#1696, 0), isnull(i_class_id#1696), coalesce(i_category_id#1698, 0), isnull(i_category_id#1698)], [coalesce(i_brand_id#1778, 0), isnull(i_brand_id#1778), coalesce(i_class_id#1780, 0), isnull(i_class_id#1780), coalesce(i_category_id#1782, 0), isnull(i_category_id#1782)], LeftSemi
                  :                    :     :                 :                    :           :- Sort [coalesce(i_brand_id#1694, 0) ASC NULLS FIRST, isnull(i_brand_id#1694) ASC NULLS FIRST, coalesce(i_class_id#1696, 0) ASC NULLS FIRST, isnull(i_class_id#1696) ASC NULLS FIRST, coalesce(i_category_id#1698, 0) ASC NULLS FIRST, isnull(i_category_id#1698) ASC NULLS FIRST], false, 0
                  :                    :     :                 :                    :           :  +- Exchange hashpartitioning(coalesce(i_brand_id#1694, 0), isnull(i_brand_id#1694), coalesce(i_class_id#1696, 0), isnull(i_class_id#1696), coalesce(i_category_id#1698, 0), isnull(i_category_id#1698), 200), ENSURE_REQUIREMENTS, [plan_id=1638]
                  :                    :     :                 :                    :           :     +- Filter (((isnotnull(i_item_sk#1687) AND isnotnull(i_brand_id#1694)) AND isnotnull(i_class_id#1696)) AND isnotnull(i_category_id#1698))
                  :                    :     :                 :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1687,i_brand_id#1694,i_class_id#1696,i_category_id#1698] Batched: true, DataFilters: [isnotnull(i_item_sk#1687), isnotnull(i_brand_id#1694), isnotnull(i_class_id#1696), isnotnull(i_c..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :                 :                    :           +- Sort [coalesce(i_brand_id#1778, 0) ASC NULLS FIRST, isnull(i_brand_id#1778) ASC NULLS FIRST, coalesce(i_class_id#1780, 0) ASC NULLS FIRST, isnull(i_class_id#1780) ASC NULLS FIRST, coalesce(i_category_id#1782, 0) ASC NULLS FIRST, isnull(i_category_id#1782) ASC NULLS FIRST], false, 0
                  :                    :     :                 :                    :              +- Exchange hashpartitioning(coalesce(i_brand_id#1778, 0), isnull(i_brand_id#1778), coalesce(i_class_id#1780, 0), isnull(i_class_id#1780), coalesce(i_category_id#1782, 0), isnull(i_category_id#1782), 200), ENSURE_REQUIREMENTS, [plan_id=1639]
                  :                    :     :                 :                    :                 +- Project [i_brand_id#1778, i_class_id#1780, i_category_id#1782]
                  :                    :     :                 :                    :                    +- BroadcastHashJoin [cs_sold_date_sk#1737], [d_date_sk#1793], Inner, BuildRight, false
                  :                    :     :                 :                    :                       :- Project [cs_sold_date_sk#1737, i_brand_id#1778, i_class_id#1780, i_category_id#1782]
                  :                    :     :                 :                    :                       :  +- BroadcastHashJoin [cs_item_sk#1752], [i_item_sk#1771], Inner, BuildRight, false
                  :                    :     :                 :                    :                       :     :- Filter (isnotnull(cs_item_sk#1752) AND isnotnull(cs_sold_date_sk#1737))
                  :                    :     :                 :                    :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#1737,cs_item_sk#1752] Batched: true, DataFilters: [isnotnull(cs_item_sk#1752), isnotnull(cs_sold_date_sk#1737)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int>
                  :                    :     :                 :                    :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1629]
                  :                    :     :                 :                    :                       :        +- Filter isnotnull(i_item_sk#1771)
                  :                    :     :                 :                    :                       :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1771,i_brand_id#1778,i_class_id#1780,i_category_id#1782] Batched: true, DataFilters: [isnotnull(i_item_sk#1771)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :                 :                    :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1633]
                  :                    :     :                 :                    :                          +- Project [d_date_sk#1793]
                  :                    :     :                 :                    :                             +- Filter (((isnotnull(d_year#1799) AND (d_year#1799 >= 1999)) AND (d_year#1799 <= 2001)) AND isnotnull(d_date_sk#1793))
                  :                    :     :                 :                    :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1793,d_year#1799] Batched: true, DataFilters: [isnotnull(d_year#1799), (d_year#1799 >= 1999), (d_year#1799 <= 2001), isnotnull(d_date_sk#1793)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :     :                 :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1648]
                  :                    :     :                 :                       +- Project [d_date_sk#1709]
                  :                    :     :                 :                          +- Filter (((isnotnull(d_year#1715) AND (d_year#1715 >= 1999)) AND (d_year#1715 <= 2001)) AND isnotnull(d_date_sk#1709))
                  :                    :     :                 :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1709,d_year#1715] Batched: true, DataFilters: [isnotnull(d_year#1715), (d_year#1715 >= 1999), (d_year#1715 <= 2001), isnotnull(d_date_sk#1709)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :     :                 +- Sort [coalesce(i_brand_id#1862, 0) ASC NULLS FIRST, isnull(i_brand_id#1862) ASC NULLS FIRST, coalesce(i_class_id#1864, 0) ASC NULLS FIRST, isnull(i_class_id#1864) ASC NULLS FIRST, coalesce(i_category_id#1866, 0) ASC NULLS FIRST, isnull(i_category_id#1866) ASC NULLS FIRST], false, 0
                  :                    :     :                    +- Exchange hashpartitioning(coalesce(i_brand_id#1862, 0), isnull(i_brand_id#1862), coalesce(i_class_id#1864, 0), isnull(i_class_id#1864), coalesce(i_category_id#1866, 0), isnull(i_category_id#1866), 200), ENSURE_REQUIREMENTS, [plan_id=1665]
                  :                    :     :                       +- Project [i_brand_id#1862, i_class_id#1864, i_category_id#1866]
                  :                    :     :                          +- BroadcastHashJoin [ws_sold_date_sk#1821], [d_date_sk#1877], Inner, BuildRight, false
                  :                    :     :                             :- Project [ws_sold_date_sk#1821, i_brand_id#1862, i_class_id#1864, i_category_id#1866]
                  :                    :     :                             :  +- BroadcastHashJoin [ws_item_sk#1824], [i_item_sk#1855], Inner, BuildRight, false
                  :                    :     :                             :     :- Filter (isnotnull(ws_item_sk#1824) AND isnotnull(ws_sold_date_sk#1821))
                  :                    :     :                             :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#1821,ws_item_sk#1824] Batched: true, DataFilters: [isnotnull(ws_item_sk#1824), isnotnull(ws_sold_date_sk#1821)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int>
                  :                    :     :                             :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1655]
                  :                    :     :                             :        +- Filter isnotnull(i_item_sk#1855)
                  :                    :     :                             :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1855,i_brand_id#1862,i_class_id#1864,i_category_id#1866] Batched: true, DataFilters: [isnotnull(i_item_sk#1855)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :     :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1659]
                  :                    :     :                                +- Project [d_date_sk#1877]
                  :                    :     :                                   +- Filter (((isnotnull(d_year#1883) AND (d_year#1883 >= 1999)) AND (d_year#1883 <= 2001)) AND isnotnull(d_date_sk#1877))
                  :                    :     :                                      +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1877,d_year#1883] Batched: true, DataFilters: [isnotnull(d_year#1883), (d_year#1883 >= 1999), (d_year#1883 <= 2001), isnotnull(d_date_sk#1877)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1732]
                  :                    :        +- SortMergeJoin [i_item_sk#579], [ss_item_sk#729], LeftSemi
                  :                    :           :- Sort [i_item_sk#579 ASC NULLS FIRST], false, 0
                  :                    :           :  +- Exchange hashpartitioning(i_item_sk#579, 200), ENSURE_REQUIREMENTS, [plan_id=1726]
                  :                    :           :     +- Filter isnotnull(i_item_sk#579)
                  :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#579,i_brand_id#586,i_class_id#588,i_category_id#590] Batched: true, DataFilters: [isnotnull(i_item_sk#579)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :           +- Sort [ss_item_sk#729 ASC NULLS FIRST], false, 0
                  :                    :              +- Exchange hashpartitioning(ss_item_sk#729, 200), ENSURE_REQUIREMENTS, [plan_id=1727]
                  :                    :                 +- Project [i_item_sk#1642 AS ss_item_sk#729]
                  :                    :                    +- BroadcastHashJoin [i_brand_id#1649, i_class_id#1651, i_category_id#1653], [brand_id#15, class_id#16, category_id#17], Inner, BuildLeft, false
                  :                    :                       :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, int, false], input[2, int, false], input[3, int, false]),false), [plan_id=1721]
                  :                    :                       :  +- Filter ((isnotnull(i_brand_id#1649) AND isnotnull(i_class_id#1651)) AND isnotnull(i_category_id#1653))
                  :                    :                       :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1642,i_brand_id#1649,i_class_id#1651,i_category_id#1653] Batched: true, DataFilters: [isnotnull(i_brand_id#1649), isnotnull(i_class_id#1651), isnotnull(i_category_id#1653)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                       +- SortMergeJoin [coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17)], [coalesce(i_brand_id#1862, 0), isnull(i_brand_id#1862), coalesce(i_class_id#1864, 0), isnull(i_class_id#1864), coalesce(i_category_id#1866, 0), isnull(i_category_id#1866)], LeftSemi
                  :                    :                          :- Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, isnull(brand_id#15) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, isnull(class_id#16) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST, isnull(category_id#17) ASC NULLS FIRST], false, 0
                  :                    :                          :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17), 200), ENSURE_REQUIREMENTS, [plan_id=1715]
                  :                    :                          :     +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :                          :        +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200), ENSURE_REQUIREMENTS, [plan_id=1704]
                  :                    :                          :           +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                  :                    :                          :              +- Project [i_brand_id#1694 AS brand_id#15, i_class_id#1696 AS class_id#16, i_category_id#1698 AS category_id#17]
                  :                    :                          :                 +- BroadcastHashJoin [ss_sold_date_sk#1664], [d_date_sk#1709], Inner, BuildRight, false
                  :                    :                          :                    :- Project [ss_sold_date_sk#1664, i_brand_id#1694, i_class_id#1696, i_category_id#1698]
                  :                    :                          :                    :  +- BroadcastHashJoin [ss_item_sk#1666], [i_item_sk#1687], Inner, BuildRight, false
                  :                    :                          :                    :     :- Filter (isnotnull(ss_item_sk#1666) AND isnotnull(ss_sold_date_sk#1664))
                  :                    :                          :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#1664,ss_item_sk#1666] Batched: true, DataFilters: [isnotnull(ss_item_sk#1666), isnotnull(ss_sold_date_sk#1664)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
                  :                    :                          :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1695]
                  :                    :                          :                    :        +- SortMergeJoin [coalesce(i_brand_id#1694, 0), isnull(i_brand_id#1694), coalesce(i_class_id#1696, 0), isnull(i_class_id#1696), coalesce(i_category_id#1698, 0), isnull(i_category_id#1698)], [coalesce(i_brand_id#1778, 0), isnull(i_brand_id#1778), coalesce(i_class_id#1780, 0), isnull(i_class_id#1780), coalesce(i_category_id#1782, 0), isnull(i_category_id#1782)], LeftSemi
                  :                    :                          :                    :           :- Sort [coalesce(i_brand_id#1694, 0) ASC NULLS FIRST, isnull(i_brand_id#1694) ASC NULLS FIRST, coalesce(i_class_id#1696, 0) ASC NULLS FIRST, isnull(i_class_id#1696) ASC NULLS FIRST, coalesce(i_category_id#1698, 0) ASC NULLS FIRST, isnull(i_category_id#1698) ASC NULLS FIRST], false, 0
                  :                    :                          :                    :           :  +- Exchange hashpartitioning(coalesce(i_brand_id#1694, 0), isnull(i_brand_id#1694), coalesce(i_class_id#1696, 0), isnull(i_class_id#1696), coalesce(i_category_id#1698, 0), isnull(i_category_id#1698), 200), ENSURE_REQUIREMENTS, [plan_id=1689]
                  :                    :                          :                    :           :     +- Filter (((isnotnull(i_item_sk#1687) AND isnotnull(i_brand_id#1694)) AND isnotnull(i_class_id#1696)) AND isnotnull(i_category_id#1698))
                  :                    :                          :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1687,i_brand_id#1694,i_class_id#1696,i_category_id#1698] Batched: true, DataFilters: [isnotnull(i_item_sk#1687), isnotnull(i_brand_id#1694), isnotnull(i_class_id#1696), isnotnull(i_c..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                          :                    :           +- Sort [coalesce(i_brand_id#1778, 0) ASC NULLS FIRST, isnull(i_brand_id#1778) ASC NULLS FIRST, coalesce(i_class_id#1780, 0) ASC NULLS FIRST, isnull(i_class_id#1780) ASC NULLS FIRST, coalesce(i_category_id#1782, 0) ASC NULLS FIRST, isnull(i_category_id#1782) ASC NULLS FIRST], false, 0
                  :                    :                          :                    :              +- Exchange hashpartitioning(coalesce(i_brand_id#1778, 0), isnull(i_brand_id#1778), coalesce(i_class_id#1780, 0), isnull(i_class_id#1780), coalesce(i_category_id#1782, 0), isnull(i_category_id#1782), 200), ENSURE_REQUIREMENTS, [plan_id=1690]
                  :                    :                          :                    :                 +- Project [i_brand_id#1778, i_class_id#1780, i_category_id#1782]
                  :                    :                          :                    :                    +- BroadcastHashJoin [cs_sold_date_sk#1737], [d_date_sk#1793], Inner, BuildRight, false
                  :                    :                          :                    :                       :- Project [cs_sold_date_sk#1737, i_brand_id#1778, i_class_id#1780, i_category_id#1782]
                  :                    :                          :                    :                       :  +- BroadcastHashJoin [cs_item_sk#1752], [i_item_sk#1771], Inner, BuildRight, false
                  :                    :                          :                    :                       :     :- Filter (isnotnull(cs_item_sk#1752) AND isnotnull(cs_sold_date_sk#1737))
                  :                    :                          :                    :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#1737,cs_item_sk#1752] Batched: true, DataFilters: [isnotnull(cs_item_sk#1752), isnotnull(cs_sold_date_sk#1737)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int>
                  :                    :                          :                    :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1680]
                  :                    :                          :                    :                       :        +- Filter isnotnull(i_item_sk#1771)
                  :                    :                          :                    :                       :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1771,i_brand_id#1778,i_class_id#1780,i_category_id#1782] Batched: true, DataFilters: [isnotnull(i_item_sk#1771)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                          :                    :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1684]
                  :                    :                          :                    :                          +- Project [d_date_sk#1793]
                  :                    :                          :                    :                             +- Filter (((isnotnull(d_year#1799) AND (d_year#1799 >= 1999)) AND (d_year#1799 <= 2001)) AND isnotnull(d_date_sk#1793))
                  :                    :                          :                    :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1793,d_year#1799] Batched: true, DataFilters: [isnotnull(d_year#1799), (d_year#1799 >= 1999), (d_year#1799 <= 2001), isnotnull(d_date_sk#1793)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :                          :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1699]
                  :                    :                          :                       +- Project [d_date_sk#1709]
                  :                    :                          :                          +- Filter (((isnotnull(d_year#1715) AND (d_year#1715 >= 1999)) AND (d_year#1715 <= 2001)) AND isnotnull(d_date_sk#1709))
                  :                    :                          :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1709,d_year#1715] Batched: true, DataFilters: [isnotnull(d_year#1715), (d_year#1715 >= 1999), (d_year#1715 <= 2001), isnotnull(d_date_sk#1709)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    :                          +- Sort [coalesce(i_brand_id#1862, 0) ASC NULLS FIRST, isnull(i_brand_id#1862) ASC NULLS FIRST, coalesce(i_class_id#1864, 0) ASC NULLS FIRST, isnull(i_class_id#1864) ASC NULLS FIRST, coalesce(i_category_id#1866, 0) ASC NULLS FIRST, isnull(i_category_id#1866) ASC NULLS FIRST], false, 0
                  :                    :                             +- Exchange hashpartitioning(coalesce(i_brand_id#1862, 0), isnull(i_brand_id#1862), coalesce(i_class_id#1864, 0), isnull(i_class_id#1864), coalesce(i_category_id#1866, 0), isnull(i_category_id#1866), 200), ENSURE_REQUIREMENTS, [plan_id=1716]
                  :                    :                                +- Project [i_brand_id#1862, i_class_id#1864, i_category_id#1866]
                  :                    :                                   +- BroadcastHashJoin [ws_sold_date_sk#1821], [d_date_sk#1877], Inner, BuildRight, false
                  :                    :                                      :- Project [ws_sold_date_sk#1821, i_brand_id#1862, i_class_id#1864, i_category_id#1866]
                  :                    :                                      :  +- BroadcastHashJoin [ws_item_sk#1824], [i_item_sk#1855], Inner, BuildRight, false
                  :                    :                                      :     :- Filter (isnotnull(ws_item_sk#1824) AND isnotnull(ws_sold_date_sk#1821))
                  :                    :                                      :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#1821,ws_item_sk#1824] Batched: true, DataFilters: [isnotnull(ws_item_sk#1824), isnotnull(ws_sold_date_sk#1821)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int>
                  :                    :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1706]
                  :                    :                                      :        +- Filter isnotnull(i_item_sk#1855)
                  :                    :                                      :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#1855,i_brand_id#1862,i_class_id#1864,i_category_id#1866] Batched: true, DataFilters: [isnotnull(i_item_sk#1855)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                  :                    :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1710]
                  :                    :                                         +- Project [d_date_sk#1877]
                  :                    :                                            +- Filter (((isnotnull(d_year#1883) AND (d_year#1883 >= 1999)) AND (d_year#1883 <= 2001)) AND isnotnull(d_date_sk#1877))
                  :                    :                                               +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1877,d_year#1883] Batched: true, DataFilters: [isnotnull(d_year#1883), (d_year#1883 >= 1999), (d_year#1883 <= 2001), isnotnull(d_date_sk#1877)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1736]
                  :                       +- Project [d_date_sk#601]
                  :                          +- Filter ((((isnotnull(d_year#607) AND isnotnull(d_moy#609)) AND (d_year#607 = 2001)) AND (d_moy#609 = 11)) AND isnotnull(d_date_sk#601))
                  :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#601,d_year#607,d_moy#609] Batched: true, DataFilters: [isnotnull(d_year#607), isnotnull(d_moy#609), (d_year#607 = 2001), (d_moy#609 = 11), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  +- Project [sales#11, number_sales#12L, web AS channel#2526, i_brand_id#670, i_class_id#672, i_category_id#674]
                     +- Filter (isnotnull(sales#11) AND (sales#11 > Subquery subquery#14, [id=#1361]))
                        :  +- Subquery subquery#14, [id=#1361]
                        :     +- AdaptiveSparkPlan isFinalPlan=false
                        :        +- HashAggregate(keys=[], functions=[avg((cast(quantity#19 as double) * list_price#20))], output=[average_sales#25])
                        :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1353]
                        :              +- HashAggregate(keys=[], functions=[partial_avg((cast(quantity#19 as double) * list_price#20))], output=[sum#2548, count#2549L])
                        :                 +- Union
                        :                    :- Project [ss_quantity#307 AS quantity#19, ss_list_price#309 AS list_price#20]
                        :                    :  +- BroadcastHashJoin [ss_sold_date_sk#297], [d_date_sk#320], Inner, BuildRight, false
                        :                    :     :- Filter isnotnull(ss_sold_date_sk#297)
                        :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#297,ss_quantity#307,ss_list_price#309] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#297)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_quantity:int,ss_list_price:double>
                        :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1341]
                        :                    :        +- Project [d_date_sk#320]
                        :                    :           +- Filter (((isnotnull(d_year#326) AND (d_year#326 >= 1999)) AND (d_year#326 <= 2001)) AND isnotnull(d_date_sk#320))
                        :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#320,d_year#326] Batched: true, DataFilters: [isnotnull(d_year#326), (d_year#326 >= 1999), (d_year#326 <= 2001), isnotnull(d_date_sk#320)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                        :                    :- Project [cs_quantity#366 AS quantity#21, cs_list_price#368 AS list_price#22]
                        :                    :  +- BroadcastHashJoin [cs_sold_date_sk#348], [d_date_sk#382], Inner, BuildRight, false
                        :                    :     :- Filter isnotnull(cs_sold_date_sk#348)
                        :                    :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_quantity#366,cs_list_price#368] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#348)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_quantity:int,cs_list_price:double>
                        :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1344]
                        :                    :        +- Project [d_date_sk#382]
                        :                    :           +- Filter (((isnotnull(d_year#388) AND (d_year#388 >= 1999)) AND (d_year#388 <= 2001)) AND isnotnull(d_date_sk#382))
                        :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#382,d_year#388] Batched: true, DataFilters: [isnotnull(d_year#388), (d_year#388 >= 1999), (d_year#388 <= 2001), isnotnull(d_date_sk#382)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                        :                    +- Project [ws_quantity#428 AS quantity#23, ws_list_price#430 AS list_price#24]
                        :                       +- BroadcastHashJoin [ws_sold_date_sk#410], [d_date_sk#444], Inner, BuildRight, false
                        :                          :- Filter isnotnull(ws_sold_date_sk#410)
                        :                          :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#410,ws_quantity#428,ws_list_price#430] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#410)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_quantity:int,ws_list_price:double>
                        :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1347]
                        :                             +- Project [d_date_sk#444]
                        :                                +- Filter (((isnotnull(d_year#450) AND (d_year#450 >= 1999)) AND (d_year#450 <= 2001)) AND isnotnull(d_date_sk#444))
                        :                                   +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#444,d_year#450] Batched: true, DataFilters: [isnotnull(d_year#450), (d_year#450 >= 1999), (d_year#450 <= 2001), isnotnull(d_date_sk#444)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                        +- HashAggregate(keys=[i_brand_id#670, i_class_id#672, i_category_id#674], functions=[sum((cast(ws_quantity#647 as double) * ws_list_price#649)), count(1)], output=[i_brand_id#670, i_class_id#672, i_category_id#674, sales#11, number_sales#12L])
                           +- Exchange hashpartitioning(i_brand_id#670, i_class_id#672, i_category_id#674, 200), ENSURE_REQUIREMENTS, [plan_id=1857]
                              +- HashAggregate(keys=[i_brand_id#670, i_class_id#672, i_category_id#674], functions=[partial_sum((cast(ws_quantity#647 as double) * ws_list_price#649)), partial_count(1)], output=[i_brand_id#670, i_class_id#672, i_category_id#674, sum#2544, count#2545L])
                                 +- Project [ws_quantity#647, ws_list_price#649, i_brand_id#670, i_class_id#672, i_category_id#674]
                                    +- BroadcastHashJoin [ws_sold_date_sk#629], [d_date_sk#685], Inner, BuildRight, false
                                       :- Project [ws_sold_date_sk#629, ws_quantity#647, ws_list_price#649, i_brand_id#670, i_class_id#672, i_category_id#674]
                                       :  +- BroadcastHashJoin [ws_item_sk#632], [i_item_sk#663], Inner, BuildRight, false
                                       :     :- SortMergeJoin [ws_item_sk#632], [ss_item_sk#730], LeftSemi
                                       :     :  :- Sort [ws_item_sk#632 ASC NULLS FIRST], false, 0
                                       :     :  :  +- Exchange hashpartitioning(ws_item_sk#632, 200), ENSURE_REQUIREMENTS, [plan_id=1791]
                                       :     :  :     +- Filter (isnotnull(ws_item_sk#632) AND isnotnull(ws_sold_date_sk#629))
                                       :     :  :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#629,ws_item_sk#632,ws_quantity#647,ws_list_price#649] Batched: true, DataFilters: [isnotnull(ws_item_sk#632), isnotnull(ws_sold_date_sk#629)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_quantity:int,ws_list_price:double>
                                       :     :  +- Sort [ss_item_sk#730 ASC NULLS FIRST], false, 0
                                       :     :     +- Exchange hashpartitioning(ss_item_sk#730, 200), ENSURE_REQUIREMENTS, [plan_id=1792]
                                       :     :        +- Project [i_item_sk#2082 AS ss_item_sk#730]
                                       :     :           +- BroadcastHashJoin [i_brand_id#2089, i_class_id#2091, i_category_id#2093], [brand_id#15, class_id#16, category_id#17], Inner, BuildLeft, false
                                       :     :              :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, int, false], input[2, int, false], input[3, int, false]),false), [plan_id=1786]
                                       :     :              :  +- Filter ((isnotnull(i_brand_id#2089) AND isnotnull(i_class_id#2091)) AND isnotnull(i_category_id#2093))
                                       :     :              :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2082,i_brand_id#2089,i_class_id#2091,i_category_id#2093] Batched: true, DataFilters: [isnotnull(i_brand_id#2089), isnotnull(i_class_id#2091), isnotnull(i_category_id#2093)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :     :              +- SortMergeJoin [coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17)], [coalesce(i_brand_id#2302, 0), isnull(i_brand_id#2302), coalesce(i_class_id#2304, 0), isnull(i_class_id#2304), coalesce(i_category_id#2306, 0), isnull(i_category_id#2306)], LeftSemi
                                       :     :                 :- Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, isnull(brand_id#15) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, isnull(class_id#16) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST, isnull(category_id#17) ASC NULLS FIRST], false, 0
                                       :     :                 :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17), 200), ENSURE_REQUIREMENTS, [plan_id=1780]
                                       :     :                 :     +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                                       :     :                 :        +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200), ENSURE_REQUIREMENTS, [plan_id=1769]
                                       :     :                 :           +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                                       :     :                 :              +- Project [i_brand_id#2134 AS brand_id#15, i_class_id#2136 AS class_id#16, i_category_id#2138 AS category_id#17]
                                       :     :                 :                 +- BroadcastHashJoin [ss_sold_date_sk#2104], [d_date_sk#2149], Inner, BuildRight, false
                                       :     :                 :                    :- Project [ss_sold_date_sk#2104, i_brand_id#2134, i_class_id#2136, i_category_id#2138]
                                       :     :                 :                    :  +- BroadcastHashJoin [ss_item_sk#2106], [i_item_sk#2127], Inner, BuildRight, false
                                       :     :                 :                    :     :- Filter (isnotnull(ss_item_sk#2106) AND isnotnull(ss_sold_date_sk#2104))
                                       :     :                 :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#2104,ss_item_sk#2106] Batched: true, DataFilters: [isnotnull(ss_item_sk#2106), isnotnull(ss_sold_date_sk#2104)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
                                       :     :                 :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1760]
                                       :     :                 :                    :        +- SortMergeJoin [coalesce(i_brand_id#2134, 0), isnull(i_brand_id#2134), coalesce(i_class_id#2136, 0), isnull(i_class_id#2136), coalesce(i_category_id#2138, 0), isnull(i_category_id#2138)], [coalesce(i_brand_id#2218, 0), isnull(i_brand_id#2218), coalesce(i_class_id#2220, 0), isnull(i_class_id#2220), coalesce(i_category_id#2222, 0), isnull(i_category_id#2222)], LeftSemi
                                       :     :                 :                    :           :- Sort [coalesce(i_brand_id#2134, 0) ASC NULLS FIRST, isnull(i_brand_id#2134) ASC NULLS FIRST, coalesce(i_class_id#2136, 0) ASC NULLS FIRST, isnull(i_class_id#2136) ASC NULLS FIRST, coalesce(i_category_id#2138, 0) ASC NULLS FIRST, isnull(i_category_id#2138) ASC NULLS FIRST], false, 0
                                       :     :                 :                    :           :  +- Exchange hashpartitioning(coalesce(i_brand_id#2134, 0), isnull(i_brand_id#2134), coalesce(i_class_id#2136, 0), isnull(i_class_id#2136), coalesce(i_category_id#2138, 0), isnull(i_category_id#2138), 200), ENSURE_REQUIREMENTS, [plan_id=1754]
                                       :     :                 :                    :           :     +- Filter (((isnotnull(i_item_sk#2127) AND isnotnull(i_brand_id#2134)) AND isnotnull(i_class_id#2136)) AND isnotnull(i_category_id#2138))
                                       :     :                 :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2127,i_brand_id#2134,i_class_id#2136,i_category_id#2138] Batched: true, DataFilters: [isnotnull(i_item_sk#2127), isnotnull(i_brand_id#2134), isnotnull(i_class_id#2136), isnotnull(i_c..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :     :                 :                    :           +- Sort [coalesce(i_brand_id#2218, 0) ASC NULLS FIRST, isnull(i_brand_id#2218) ASC NULLS FIRST, coalesce(i_class_id#2220, 0) ASC NULLS FIRST, isnull(i_class_id#2220) ASC NULLS FIRST, coalesce(i_category_id#2222, 0) ASC NULLS FIRST, isnull(i_category_id#2222) ASC NULLS FIRST], false, 0
                                       :     :                 :                    :              +- Exchange hashpartitioning(coalesce(i_brand_id#2218, 0), isnull(i_brand_id#2218), coalesce(i_class_id#2220, 0), isnull(i_class_id#2220), coalesce(i_category_id#2222, 0), isnull(i_category_id#2222), 200), ENSURE_REQUIREMENTS, [plan_id=1755]
                                       :     :                 :                    :                 +- Project [i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                                       :     :                 :                    :                    +- BroadcastHashJoin [cs_sold_date_sk#2177], [d_date_sk#2233], Inner, BuildRight, false
                                       :     :                 :                    :                       :- Project [cs_sold_date_sk#2177, i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                                       :     :                 :                    :                       :  +- BroadcastHashJoin [cs_item_sk#2192], [i_item_sk#2211], Inner, BuildRight, false
                                       :     :                 :                    :                       :     :- Filter (isnotnull(cs_item_sk#2192) AND isnotnull(cs_sold_date_sk#2177))
                                       :     :                 :                    :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#2177,cs_item_sk#2192] Batched: true, DataFilters: [isnotnull(cs_item_sk#2192), isnotnull(cs_sold_date_sk#2177)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int>
                                       :     :                 :                    :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1745]
                                       :     :                 :                    :                       :        +- Filter isnotnull(i_item_sk#2211)
                                       :     :                 :                    :                       :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2211,i_brand_id#2218,i_class_id#2220,i_category_id#2222] Batched: true, DataFilters: [isnotnull(i_item_sk#2211)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :     :                 :                    :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1749]
                                       :     :                 :                    :                          +- Project [d_date_sk#2233]
                                       :     :                 :                    :                             +- Filter (((isnotnull(d_year#2239) AND (d_year#2239 >= 1999)) AND (d_year#2239 <= 2001)) AND isnotnull(d_date_sk#2233))
                                       :     :                 :                    :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#2233,d_year#2239] Batched: true, DataFilters: [isnotnull(d_year#2239), (d_year#2239 >= 1999), (d_year#2239 <= 2001), isnotnull(d_date_sk#2233)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                                       :     :                 :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1764]
                                       :     :                 :                       +- Project [d_date_sk#2149]
                                       :     :                 :                          +- Filter (((isnotnull(d_year#2155) AND (d_year#2155 >= 1999)) AND (d_year#2155 <= 2001)) AND isnotnull(d_date_sk#2149))
                                       :     :                 :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#2149,d_year#2155] Batched: true, DataFilters: [isnotnull(d_year#2155), (d_year#2155 >= 1999), (d_year#2155 <= 2001), isnotnull(d_date_sk#2149)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                                       :     :                 +- Sort [coalesce(i_brand_id#2302, 0) ASC NULLS FIRST, isnull(i_brand_id#2302) ASC NULLS FIRST, coalesce(i_class_id#2304, 0) ASC NULLS FIRST, isnull(i_class_id#2304) ASC NULLS FIRST, coalesce(i_category_id#2306, 0) ASC NULLS FIRST, isnull(i_category_id#2306) ASC NULLS FIRST], false, 0
                                       :     :                    +- Exchange hashpartitioning(coalesce(i_brand_id#2302, 0), isnull(i_brand_id#2302), coalesce(i_class_id#2304, 0), isnull(i_class_id#2304), coalesce(i_category_id#2306, 0), isnull(i_category_id#2306), 200), ENSURE_REQUIREMENTS, [plan_id=1781]
                                       :     :                       +- Project [i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                                       :     :                          +- BroadcastHashJoin [ws_sold_date_sk#2261], [d_date_sk#2317], Inner, BuildRight, false
                                       :     :                             :- Project [ws_sold_date_sk#2261, i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                                       :     :                             :  +- BroadcastHashJoin [ws_item_sk#2264], [i_item_sk#2295], Inner, BuildRight, false
                                       :     :                             :     :- Filter (isnotnull(ws_item_sk#2264) AND isnotnull(ws_sold_date_sk#2261))
                                       :     :                             :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#2261,ws_item_sk#2264] Batched: true, DataFilters: [isnotnull(ws_item_sk#2264), isnotnull(ws_sold_date_sk#2261)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int>
                                       :     :                             :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1771]
                                       :     :                             :        +- Filter isnotnull(i_item_sk#2295)
                                       :     :                             :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2295,i_brand_id#2302,i_class_id#2304,i_category_id#2306] Batched: true, DataFilters: [isnotnull(i_item_sk#2295)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :     :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1775]
                                       :     :                                +- Project [d_date_sk#2317]
                                       :     :                                   +- Filter (((isnotnull(d_year#2323) AND (d_year#2323 >= 1999)) AND (d_year#2323 <= 2001)) AND isnotnull(d_date_sk#2317))
                                       :     :                                      +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#2317,d_year#2323] Batched: true, DataFilters: [isnotnull(d_year#2323), (d_year#2323 >= 1999), (d_year#2323 <= 2001), isnotnull(d_date_sk#2317)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1848]
                                       :        +- SortMergeJoin [i_item_sk#663], [ss_item_sk#730], LeftSemi
                                       :           :- Sort [i_item_sk#663 ASC NULLS FIRST], false, 0
                                       :           :  +- Exchange hashpartitioning(i_item_sk#663, 200), ENSURE_REQUIREMENTS, [plan_id=1842]
                                       :           :     +- Filter isnotnull(i_item_sk#663)
                                       :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#663,i_brand_id#670,i_class_id#672,i_category_id#674] Batched: true, DataFilters: [isnotnull(i_item_sk#663)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :           +- Sort [ss_item_sk#730 ASC NULLS FIRST], false, 0
                                       :              +- Exchange hashpartitioning(ss_item_sk#730, 200), ENSURE_REQUIREMENTS, [plan_id=1843]
                                       :                 +- Project [i_item_sk#2082 AS ss_item_sk#730]
                                       :                    +- BroadcastHashJoin [i_brand_id#2089, i_class_id#2091, i_category_id#2093], [brand_id#15, class_id#16, category_id#17], Inner, BuildLeft, false
                                       :                       :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, int, false], input[2, int, false], input[3, int, false]),false), [plan_id=1837]
                                       :                       :  +- Filter ((isnotnull(i_brand_id#2089) AND isnotnull(i_class_id#2091)) AND isnotnull(i_category_id#2093))
                                       :                       :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2082,i_brand_id#2089,i_class_id#2091,i_category_id#2093] Batched: true, DataFilters: [isnotnull(i_brand_id#2089), isnotnull(i_class_id#2091), isnotnull(i_category_id#2093)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :                       +- SortMergeJoin [coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17)], [coalesce(i_brand_id#2302, 0), isnull(i_brand_id#2302), coalesce(i_class_id#2304, 0), isnull(i_class_id#2304), coalesce(i_category_id#2306, 0), isnull(i_category_id#2306)], LeftSemi
                                       :                          :- Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, isnull(brand_id#15) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, isnull(class_id#16) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST, isnull(category_id#17) ASC NULLS FIRST], false, 0
                                       :                          :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), isnull(brand_id#15), coalesce(class_id#16, 0), isnull(class_id#16), coalesce(category_id#17, 0), isnull(category_id#17), 200), ENSURE_REQUIREMENTS, [plan_id=1831]
                                       :                          :     +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                                       :                          :        +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200), ENSURE_REQUIREMENTS, [plan_id=1820]
                                       :                          :           +- HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
                                       :                          :              +- Project [i_brand_id#2134 AS brand_id#15, i_class_id#2136 AS class_id#16, i_category_id#2138 AS category_id#17]
                                       :                          :                 +- BroadcastHashJoin [ss_sold_date_sk#2104], [d_date_sk#2149], Inner, BuildRight, false
                                       :                          :                    :- Project [ss_sold_date_sk#2104, i_brand_id#2134, i_class_id#2136, i_category_id#2138]
                                       :                          :                    :  +- BroadcastHashJoin [ss_item_sk#2106], [i_item_sk#2127], Inner, BuildRight, false
                                       :                          :                    :     :- Filter (isnotnull(ss_item_sk#2106) AND isnotnull(ss_sold_date_sk#2104))
                                       :                          :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#2104,ss_item_sk#2106] Batched: true, DataFilters: [isnotnull(ss_item_sk#2106), isnotnull(ss_sold_date_sk#2104)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
                                       :                          :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1811]
                                       :                          :                    :        +- SortMergeJoin [coalesce(i_brand_id#2134, 0), isnull(i_brand_id#2134), coalesce(i_class_id#2136, 0), isnull(i_class_id#2136), coalesce(i_category_id#2138, 0), isnull(i_category_id#2138)], [coalesce(i_brand_id#2218, 0), isnull(i_brand_id#2218), coalesce(i_class_id#2220, 0), isnull(i_class_id#2220), coalesce(i_category_id#2222, 0), isnull(i_category_id#2222)], LeftSemi
                                       :                          :                    :           :- Sort [coalesce(i_brand_id#2134, 0) ASC NULLS FIRST, isnull(i_brand_id#2134) ASC NULLS FIRST, coalesce(i_class_id#2136, 0) ASC NULLS FIRST, isnull(i_class_id#2136) ASC NULLS FIRST, coalesce(i_category_id#2138, 0) ASC NULLS FIRST, isnull(i_category_id#2138) ASC NULLS FIRST], false, 0
                                       :                          :                    :           :  +- Exchange hashpartitioning(coalesce(i_brand_id#2134, 0), isnull(i_brand_id#2134), coalesce(i_class_id#2136, 0), isnull(i_class_id#2136), coalesce(i_category_id#2138, 0), isnull(i_category_id#2138), 200), ENSURE_REQUIREMENTS, [plan_id=1805]
                                       :                          :                    :           :     +- Filter (((isnotnull(i_item_sk#2127) AND isnotnull(i_brand_id#2134)) AND isnotnull(i_class_id#2136)) AND isnotnull(i_category_id#2138))
                                       :                          :                    :           :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2127,i_brand_id#2134,i_class_id#2136,i_category_id#2138] Batched: true, DataFilters: [isnotnull(i_item_sk#2127), isnotnull(i_brand_id#2134), isnotnull(i_class_id#2136), isnotnull(i_c..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :                          :                    :           +- Sort [coalesce(i_brand_id#2218, 0) ASC NULLS FIRST, isnull(i_brand_id#2218) ASC NULLS FIRST, coalesce(i_class_id#2220, 0) ASC NULLS FIRST, isnull(i_class_id#2220) ASC NULLS FIRST, coalesce(i_category_id#2222, 0) ASC NULLS FIRST, isnull(i_category_id#2222) ASC NULLS FIRST], false, 0
                                       :                          :                    :              +- Exchange hashpartitioning(coalesce(i_brand_id#2218, 0), isnull(i_brand_id#2218), coalesce(i_class_id#2220, 0), isnull(i_class_id#2220), coalesce(i_category_id#2222, 0), isnull(i_category_id#2222), 200), ENSURE_REQUIREMENTS, [plan_id=1806]
                                       :                          :                    :                 +- Project [i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                                       :                          :                    :                    +- BroadcastHashJoin [cs_sold_date_sk#2177], [d_date_sk#2233], Inner, BuildRight, false
                                       :                          :                    :                       :- Project [cs_sold_date_sk#2177, i_brand_id#2218, i_class_id#2220, i_category_id#2222]
                                       :                          :                    :                       :  +- BroadcastHashJoin [cs_item_sk#2192], [i_item_sk#2211], Inner, BuildRight, false
                                       :                          :                    :                       :     :- Filter (isnotnull(cs_item_sk#2192) AND isnotnull(cs_sold_date_sk#2177))
                                       :                          :                    :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#2177,cs_item_sk#2192] Batched: true, DataFilters: [isnotnull(cs_item_sk#2192), isnotnull(cs_sold_date_sk#2177)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int>
                                       :                          :                    :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1796]
                                       :                          :                    :                       :        +- Filter isnotnull(i_item_sk#2211)
                                       :                          :                    :                       :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2211,i_brand_id#2218,i_class_id#2220,i_category_id#2222] Batched: true, DataFilters: [isnotnull(i_item_sk#2211)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :                          :                    :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1800]
                                       :                          :                    :                          +- Project [d_date_sk#2233]
                                       :                          :                    :                             +- Filter (((isnotnull(d_year#2239) AND (d_year#2239 >= 1999)) AND (d_year#2239 <= 2001)) AND isnotnull(d_date_sk#2233))
                                       :                          :                    :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#2233,d_year#2239] Batched: true, DataFilters: [isnotnull(d_year#2239), (d_year#2239 >= 1999), (d_year#2239 <= 2001), isnotnull(d_date_sk#2233)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                                       :                          :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1815]
                                       :                          :                       +- Project [d_date_sk#2149]
                                       :                          :                          +- Filter (((isnotnull(d_year#2155) AND (d_year#2155 >= 1999)) AND (d_year#2155 <= 2001)) AND isnotnull(d_date_sk#2149))
                                       :                          :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#2149,d_year#2155] Batched: true, DataFilters: [isnotnull(d_year#2155), (d_year#2155 >= 1999), (d_year#2155 <= 2001), isnotnull(d_date_sk#2149)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                                       :                          +- Sort [coalesce(i_brand_id#2302, 0) ASC NULLS FIRST, isnull(i_brand_id#2302) ASC NULLS FIRST, coalesce(i_class_id#2304, 0) ASC NULLS FIRST, isnull(i_class_id#2304) ASC NULLS FIRST, coalesce(i_category_id#2306, 0) ASC NULLS FIRST, isnull(i_category_id#2306) ASC NULLS FIRST], false, 0
                                       :                             +- Exchange hashpartitioning(coalesce(i_brand_id#2302, 0), isnull(i_brand_id#2302), coalesce(i_class_id#2304, 0), isnull(i_class_id#2304), coalesce(i_category_id#2306, 0), isnull(i_category_id#2306), 200), ENSURE_REQUIREMENTS, [plan_id=1832]
                                       :                                +- Project [i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                                       :                                   +- BroadcastHashJoin [ws_sold_date_sk#2261], [d_date_sk#2317], Inner, BuildRight, false
                                       :                                      :- Project [ws_sold_date_sk#2261, i_brand_id#2302, i_class_id#2304, i_category_id#2306]
                                       :                                      :  +- BroadcastHashJoin [ws_item_sk#2264], [i_item_sk#2295], Inner, BuildRight, false
                                       :                                      :     :- Filter (isnotnull(ws_item_sk#2264) AND isnotnull(ws_sold_date_sk#2261))
                                       :                                      :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#2261,ws_item_sk#2264] Batched: true, DataFilters: [isnotnull(ws_item_sk#2264), isnotnull(ws_sold_date_sk#2261)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int>
                                       :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1822]
                                       :                                      :        +- Filter isnotnull(i_item_sk#2295)
                                       :                                      :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#2295,i_brand_id#2302,i_class_id#2304,i_category_id#2306] Batched: true, DataFilters: [isnotnull(i_item_sk#2295)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
                                       :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1826]
                                       :                                         +- Project [d_date_sk#2317]
                                       :                                            +- Filter (((isnotnull(d_year#2323) AND (d_year#2323 >= 1999)) AND (d_year#2323 <= 2001)) AND isnotnull(d_date_sk#2317))
                                       :                                               +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#2317,d_year#2323] Batched: true, DataFilters: [isnotnull(d_year#2323), (d_year#2323 >= 1999), (d_year#2323 <= 2001), isnotnull(d_date_sk#2317)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=1852]
                                          +- Project [d_date_sk#685]
                                             +- Filter ((((isnotnull(d_year#691) AND isnotnull(d_moy#693)) AND (d_year#691 = 2001)) AND (d_moy#693 = 11)) AND isnotnull(d_date_sk#685))
                                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#685,d_year#691,d_moy#693] Batched: true, DataFilters: [isnotnull(d_year#691), isnotnull(d_moy#693), (d_year#691 = 2001), (d_moy#693 = 11), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>

Time taken: 6.07 seconds, Fetched 1 row(s)
store	1001001	1	1	372549.58999999997	137	store	1001001	1	1	1316197.4299999997	340
store	1001002	1	1	799646.86	229	store	1001002	1	1	775776.63	188
store	1002001	2	1	765555.91	202	store	1002001	2	1	1218310.8200000003	337
store	1002002	2	1	1030666.6499999999	250	store	1002002	2	1	605616.7899999999	166
store	1003001	3	1	547136.9600000001	167	store	1003001	3	1	1172847.07	341
store	1003002	3	1	674007.1199999999	188	store	1003002	3	1	607135.0400000002	154
store	1004001	4	1	682747.23	187	store	1004001	4	1	1171699.76	311
store	1004002	4	1	739362.1599999999	210	store	1004002	4	1	649347.19	184
store	2001001	1	2	688211.0800000001	216	store	2001001	1	2	1342495.45	365
store	2001002	1	2	906330.7	237	store	2001002	1	2	541063.5	153
store	2002001	2	2	868177.8700000001	211	store	2002001	2	2	1514792.19	402
store	2002002	2	2	648459.06	179	store	2002002	2	2	588040.22	159
store	2003001	3	2	559505.3500000001	151	store	2003001	3	2	1171821.55	300
store	2003002	3	2	746714.1100000001	219	store	2003002	3	2	695877.1599999999	171
store	2004001	4	2	772046.77	235	store	2004001	4	2	1371151.6999999997	362
store	2004002	4	2	685029.2899999999	192	store	2004002	4	2	646760.33	187
store	3001001	1	3	596684.04	165	store	3001001	1	3	1059529.81	312
store	3001002	1	3	795882.4700000001	202	store	3001002	1	3	494739.47	142
store	3002001	2	3	661760.5800000001	181	store	3002001	2	3	1411948.74	380
store	3002002	2	3	864250.1100000001	244	store	3002002	2	3	630689.3	183
store	3003001	3	3	749054.1599999999	224	store	3003001	3	3	1513634.7099999997	381
store	3003002	3	3	737460.7100000001	194	store	3003002	3	3	713930.8600000001	164
store	3004001	4	3	508491.02	161	store	3004001	4	3	1082215.49	293
store	3004002	4	3	867362.83	220	store	3004002	4	3	630978.5599999999	172
store	4001001	1	4	847845.3999999999	207	store	4001001	1	4	1291518.9700000002	340
store	4001002	1	4	841877.7999999999	232	store	4001002	1	4	520226.86	143
store	4002001	2	4	543692.2	162	store	4002001	2	4	1378081.9	362
store	4002002	2	4	912449.6800000002	256	store	4002002	2	4	713528.3800000001	200
store	4003001	3	4	725565.2200000001	189	store	4003001	3	4	1285387.4899999998	311
store	4003002	3	4	686780.8800000001	197	store	4003002	3	4	623656.88	160
store	4004001	4	4	665419.78	170	store	4004001	4	4	1141222.24	334
store	4004002	4	4	866534.14	204	store	4004002	4	4	644963.98	168
store	5001001	1	5	626379.58	167	store	5001001	1	5	1258495.82	336
store	5001002	1	5	813592.7000000002	198	store	5001002	1	5	671017.15	179
store	5002001	2	5	468954.69	153	store	5002001	2	5	1195162.4200000002	297
store	5002002	2	5	885968.6	211	store	5002002	2	5	691132.99	183
store	5003001	3	5	709102.6399999999	206	store	5003001	3	5	1369869.3099999996	375
store	5003002	3	5	1019553.8700000001	266	store	5003002	3	5	671873.72	181
store	5004001	4	5	801416.97	219	store	5004001	4	5	1405232.1800000002	371
store	5004002	4	5	1061684.6400000001	273	store	5004002	4	5	790288.63	209
store	6001001	1	6	24105.889999999996	9	store	6001001	1	6	43566.03	11
store	6001002	1	6	40621.70999999999	10	store	6001002	1	6	31684.03	10
store	6001003	1	6	23365.0	8	store	6001003	1	6	25117.82	10
store	6001004	1	6	29458.48	6	store	6001004	1	6	29630.399999999998	7
store	6001005	1	6	40555.76	13	store	6001005	1	6	67204.16	14
store	6001006	1	6	49871.81999999999	10	store	6001006	1	6	17872.800000000003	6
store	6001007	1	6	23526.21	8	store	6001007	1	6	34341.06	13
store	6001008	1	6	74782.9	20	store	6001008	1	6	127415.74000000002	33
store	6002001	2	6	55244.229999999996	15	store	6002001	2	6	67018.71	20
store	6002002	2	6	24045.720000000005	11	store	6002002	2	6	73776.49	17
store	6002004	2	6	45590.44	15	store	6002004	2	6	13288.010000000002	5
store	6002005	2	6	53619.89000000001	11	store	6002005	2	6	57765.0	12
store	6002006	2	6	36339.64	12	store	6002006	2	6	28988.989999999998	8
store	6002007	2	6	33520.909999999996	9	store	6002007	2	6	55473.590000000004	15
store	6002008	2	6	24710.01	7	store	6002008	2	6	25085.330000000005	11
store	6003001	3	6	33165.63	9	store	6003001	3	6	84144.62000000001	18
store	6003002	3	6	52293.57	15	store	6003002	3	6	59151.700000000004	15
store	6003003	3	6	33342.979999999996	7	store	6003003	3	6	43720.13	20
store	6003004	3	6	26344.41	9	store	6003004	3	6	30930.85	9
store	6003005	3	6	89998.03	26	store	6003005	3	6	95491.0	29
store	6003006	3	6	31862.34	10	store	6003006	3	6	24121.07	3
store	6003007	3	6	92405.03	24	store	6003007	3	6	129219.45999999999	31
store	6003008	3	6	46145.62	13	store	6003008	3	6	41948.28	19
store	6004001	4	6	12839.97	5	store	6004001	4	6	74616.32	16
store	6004002	4	6	33700.509999999995	11	store	6004002	4	6	45202.399999999994	11
store	6004003	4	6	31387.04	12	store	6004003	4	6	103532.44000000002	21
store	6004004	4	6	42644.68	8	store	6004004	4	6	15287.71	6
store	6004005	4	6	15515.220000000001	3	store	6004005	4	6	60172.67	16
store	6004006	4	6	60466.48	15	store	6004006	4	6	38426.03999999999	10
store	6004007	4	6	48158.78	12	store	6004007	4	6	55265.06	13
store	6004008	4	6	110528.91999999998	26	store	6004008	4	6	17409.99	6
store	6005001	5	6	38589.67	11	store	6005001	5	6	135393.23	42
store	6005002	5	6	47495.549999999996	8	store	6005002	5	6	49267.29	12
store	6005003	5	6	14884.61	3	store	6005003	5	6	74851.15000000001	16
store	6005005	5	6	37483.240000000005	12	store	6005005	5	6	73876.82	18
store	6005006	5	6	61699.19	17	store	6005006	5	6	52082.170000000006	11
store	6005007	5	6	18764.89	5	store	6005007	5	6	48445.42999999999	16
store	6005008	5	6	25927.829999999998	9	store	6005008	5	6	16473.68	7
store	6006001	6	6	36995.31	14	store	6006001	6	6	69739.81	24
store	6006002	6	6	85385.15999999999	22	store	6006002	6	6	87585.47	23
store	6006003	6	6	47063.869999999995	11	store	6006003	6	6	114431.48	29
store	6006004	6	6	20527.5	8	store	6006004	6	6	23213.370000000003	3
store	6006005	6	6	7442.61	4	store	6006005	6	6	93912.14	18
store	6006006	6	6	25335.02	8	store	6006006	6	6	24102.429999999997	5
store	6006007	6	6	12901.85	6	store	6006007	6	6	107194.04000000001	30
store	6006008	6	6	39533.23	14	store	6006008	6	6	48680.37	14
store	6007001	7	6	9032.59	3	store	6007001	7	6	15183.24	6
store	6007002	7	6	57662.38	13	store	6007002	7	6	42989.38999999999	13
store	6007003	7	6	44207.42	17	store	6007003	7	6	141091.04	39
store	6007004	7	6	147787.95	29	store	6007004	7	6	69437.35	16
store	6007005	7	6	33681.869999999995	9	store	6007005	7	6	49926.37	15
store	6007006	7	6	50875.6	16	store	6007006	7	6	75381.63	24
store	6007007	7	6	13622.219999999998	7	store	6007007	7	6	102650.94	31
store	6007008	7	6	34697.31	12	store	6007008	7	6	49841.13	18
store	6008001	8	6	44993.329999999994	10	store	6008001	8	6	57972.98	22
store	6008002	8	6	39437.31	8	store	6008002	8	6	48434.65	14
store	6008003	8	6	59905.770000000004	14	store	6008003	8	6	60930.66	20
store	6008004	8	6	36676.25	9	store	6008004	8	6	31817.07	9
store	6008005	8	6	84238.28	27	store	6008005	8	6	136071.69999999998	27
store	6008007	8	6	14672.769999999999	7	store	6008007	8	6	59474.21	18
Time taken: 26.241 seconds, Fetched 100 row(s)
