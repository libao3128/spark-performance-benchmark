Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580818243
== Parsed Logical Plan ==
CTE [ss, cs, ws]
:  :- 'SubqueryAlias ss
:  :  +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('ss_ext_sales_price) AS total_sales#1]
:  :     +- 'Filter ((('i_manufact_id IN (list#2 []) AND ('ss_item_sk = 'i_item_sk)) AND (('ss_sold_date_sk = 'd_date_sk) AND ('d_year = 1998))) AND ((('d_moy = 5) AND ('ss_addr_sk = 'ca_address_sk)) AND ('ca_gmt_offset = -5)))
:  :        :  +- 'Project ['i_manufact_id]
:  :        :     +- 'Filter 'i_category IN (Electronics)
:  :        :        +- 'UnresolvedRelation [item], [], false
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation [store_sales], [], false
:  :           :  :  +- 'UnresolvedRelation [date_dim], [], false
:  :           :  +- 'UnresolvedRelation [customer_address], [], false
:  :           +- 'UnresolvedRelation [item], [], false
:  :- 'SubqueryAlias cs
:  :  +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('cs_ext_sales_price) AS total_sales#3]
:  :     +- 'Filter ((('i_manufact_id IN (list#4 []) AND ('cs_item_sk = 'i_item_sk)) AND (('cs_sold_date_sk = 'd_date_sk) AND ('d_year = 1998))) AND ((('d_moy = 5) AND ('cs_bill_addr_sk = 'ca_address_sk)) AND ('ca_gmt_offset = -5)))
:  :        :  +- 'Project ['i_manufact_id]
:  :        :     +- 'Filter 'i_category IN (Electronics)
:  :        :        +- 'UnresolvedRelation [item], [], false
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation [catalog_sales], [], false
:  :           :  :  +- 'UnresolvedRelation [date_dim], [], false
:  :           :  +- 'UnresolvedRelation [customer_address], [], false
:  :           +- 'UnresolvedRelation [item], [], false
:  +- 'SubqueryAlias ws
:     +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('ws_ext_sales_price) AS total_sales#5]
:        +- 'Filter ((('i_manufact_id IN (list#6 []) AND ('ws_item_sk = 'i_item_sk)) AND (('ws_sold_date_sk = 'd_date_sk) AND ('d_year = 1998))) AND ((('d_moy = 5) AND ('ws_bill_addr_sk = 'ca_address_sk)) AND ('ca_gmt_offset = -5)))
:           :  +- 'Project ['i_manufact_id]
:           :     +- 'Filter 'i_category IN (Electronics)
:           :        +- 'UnresolvedRelation [item], [], false
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'Join Inner
:              :  :  :- 'UnresolvedRelation [web_sales], [], false
:              :  :  +- 'UnresolvedRelation [date_dim], [], false
:              :  +- 'UnresolvedRelation [customer_address], [], false
:              +- 'UnresolvedRelation [item], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['total_sales ASC NULLS FIRST], true
         +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('total_sales) AS total_sales#0]
            +- 'SubqueryAlias tmp1
               +- 'Union false, false
                  :- 'Union false, false
                  :  :- 'Project [*]
                  :  :  +- 'UnresolvedRelation [ss], [], false
                  :  +- 'Project [*]
                  :     +- 'UnresolvedRelation [cs], [], false
                  +- 'Project [*]
                     +- 'UnresolvedRelation [ws], [], false

== Analyzed Logical Plan ==
i_manufact_id: int, total_sales: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ss
:     +- Aggregate [i_manufact_id#89], [i_manufact_id#89, sum(ss_ext_sales_price#27) AS total_sales#1]
:        +- Filter (((i_manufact_id#89 IN (list#2 []) AND (ss_item_sk#14 = i_item_sk#76)) AND ((ss_sold_date_sk#12 = d_date_sk#35) AND (d_year#41 = 1998))) AND (((d_moy#43 = 5) AND (ss_addr_sk#18 = ca_address_sk#63)) AND (ca_gmt_offset#74 = cast(-5 as double))))
:           :  +- Project [i_manufact_id#317]
:           :     +- Filter i_category#316 IN (Electronics)
:           :        +- SubqueryAlias spark_catalog.tpcds.item
:           :           +- Relation spark_catalog.tpcds.item[i_item_sk#304,i_item_id#305,i_rec_start_date#306,i_rec_end_date#307,i_item_desc#308,i_current_price#309,i_wholesale_cost#310,i_brand_id#311,i_brand#312,i_class_id#313,i_class#314,i_category_id#315,i_category#316,i_manufact_id#317,i_manufact#318,i_size#319,i_formulation#320,i_color#321,i_units#322,i_container#323,i_manager_id#324,i_product_name#325] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#12,ss_sold_time_sk#13,ss_item_sk#14,ss_customer_sk#15,ss_cdemo_sk#16,ss_hdemo_sk#17,ss_addr_sk#18,ss_store_sk#19,ss_promo_sk#20,ss_ticket_number#21,ss_quantity#22,ss_wholesale_cost#23,ss_list_price#24,ss_sales_price#25,ss_ext_discount_amt#26,ss_ext_sales_price#27,ss_ext_wholesale_cost#28,ss_ext_list_price#29,ss_ext_tax#30,ss_coupon_amt#31,ss_net_paid#32,ss_net_paid_inc_tax#33,ss_net_profit#34] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#35,d_date_id#36,d_date#37,d_month_seq#38,d_week_seq#39,d_quarter_seq#40,d_year#41,d_dow#42,d_moy#43,d_dom#44,d_qoy#45,d_fy_year#46,d_fy_quarter_seq#47,d_fy_week_seq#48,d_day_name#49,d_quarter_name#50,d_holiday#51,d_weekend#52,d_following_holiday#53,d_first_dom#54,d_last_dom#55,d_same_day_ly#56,d_same_day_lq#57,d_current_day#58,... 4 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.customer_address
:              :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#63,ca_address_id#64,ca_street_number#65,ca_street_name#66,ca_street_type#67,ca_suite_number#68,ca_city#69,ca_county#70,ca_state#71,ca_zip#72,ca_country#73,ca_gmt_offset#74,ca_location_type#75] parquet
:              +- SubqueryAlias spark_catalog.tpcds.item
:                 +- Relation spark_catalog.tpcds.item[i_item_sk#76,i_item_id#77,i_rec_start_date#78,i_rec_end_date#79,i_item_desc#80,i_current_price#81,i_wholesale_cost#82,i_brand_id#83,i_brand#84,i_class_id#85,i_class#86,i_category_id#87,i_category#88,i_manufact_id#89,i_manufact#90,i_size#91,i_formulation#92,i_color#93,i_units#94,i_container#95,i_manager_id#96,i_product_name#97] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias cs
:     +- Aggregate [i_manufact_id#220], [i_manufact_id#220, sum(cs_ext_sales_price#121) AS total_sales#3]
:        +- Filter (((i_manufact_id#220 IN (list#4 []) AND (cs_item_sk#113 = i_item_sk#207)) AND ((cs_sold_date_sk#98 = d_date_sk#166) AND (d_year#172 = 1998))) AND (((d_moy#174 = 5) AND (cs_bill_addr_sk#104 = ca_address_sk#194)) AND (ca_gmt_offset#205 = cast(-5 as double))))
:           :  +- Project [i_manufact_id#339]
:           :     +- Filter i_category#338 IN (Electronics)
:           :        +- SubqueryAlias spark_catalog.tpcds.item
:           :           +- Relation spark_catalog.tpcds.item[i_item_sk#326,i_item_id#327,i_rec_start_date#328,i_rec_end_date#329,i_item_desc#330,i_current_price#331,i_wholesale_cost#332,i_brand_id#333,i_brand#334,i_class_id#335,i_class#336,i_category_id#337,i_category#338,i_manufact_id#339,i_manufact#340,i_size#341,i_formulation#342,i_color#343,i_units#344,i_container#345,i_manager_id#346,i_product_name#347] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :  :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#98,cs_sold_time_sk#99,cs_ship_date_sk#100,cs_bill_customer_sk#101,cs_bill_cdemo_sk#102,cs_bill_hdemo_sk#103,cs_bill_addr_sk#104,cs_ship_customer_sk#105,cs_ship_cdemo_sk#106,cs_ship_hdemo_sk#107,cs_ship_addr_sk#108,cs_call_center_sk#109,cs_catalog_page_sk#110,cs_ship_mode_sk#111,cs_warehouse_sk#112,cs_item_sk#113,cs_promo_sk#114,cs_order_number#115,cs_quantity#116,cs_wholesale_cost#117,cs_list_price#118,cs_sales_price#119,cs_ext_discount_amt#120,cs_ext_sales_price#121,... 10 more fields] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#166,d_date_id#167,d_date#168,d_month_seq#169,d_week_seq#170,d_quarter_seq#171,d_year#172,d_dow#173,d_moy#174,d_dom#175,d_qoy#176,d_fy_year#177,d_fy_quarter_seq#178,d_fy_week_seq#179,d_day_name#180,d_quarter_name#181,d_holiday#182,d_weekend#183,d_following_holiday#184,d_first_dom#185,d_last_dom#186,d_same_day_ly#187,d_same_day_lq#188,d_current_day#189,... 4 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.customer_address
:              :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#194,ca_address_id#195,ca_street_number#196,ca_street_name#197,ca_street_type#198,ca_suite_number#199,ca_city#200,ca_county#201,ca_state#202,ca_zip#203,ca_country#204,ca_gmt_offset#205,ca_location_type#206] parquet
:              +- SubqueryAlias spark_catalog.tpcds.item
:                 +- Relation spark_catalog.tpcds.item[i_item_sk#207,i_item_id#208,i_rec_start_date#209,i_rec_end_date#210,i_item_desc#211,i_current_price#212,i_wholesale_cost#213,i_brand_id#214,i_brand#215,i_class_id#216,i_class#217,i_category_id#218,i_category#219,i_manufact_id#220,i_manufact#221,i_size#222,i_formulation#223,i_color#224,i_units#225,i_container#226,i_manager_id#227,i_product_name#228] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias ws
:     +- Aggregate [i_manufact_id#283], [i_manufact_id#283, sum(ws_ext_sales_price#155) AS total_sales#5]
:        +- Filter (((i_manufact_id#283 IN (list#6 []) AND (ws_item_sk#135 = i_item_sk#270)) AND ((ws_sold_date_sk#132 = d_date_sk#229) AND (d_year#235 = 1998))) AND (((d_moy#237 = 5) AND (ws_bill_addr_sk#139 = ca_address_sk#257)) AND (ca_gmt_offset#268 = cast(-5 as double))))
:           :  +- Project [i_manufact_id#361]
:           :     +- Filter i_category#360 IN (Electronics)
:           :        +- SubqueryAlias spark_catalog.tpcds.item
:           :           +- Relation spark_catalog.tpcds.item[i_item_sk#348,i_item_id#349,i_rec_start_date#350,i_rec_end_date#351,i_item_desc#352,i_current_price#353,i_wholesale_cost#354,i_brand_id#355,i_brand#356,i_class_id#357,i_class#358,i_category_id#359,i_category#360,i_manufact_id#361,i_manufact#362,i_size#363,i_formulation#364,i_color#365,i_units#366,i_container#367,i_manager_id#368,i_product_name#369] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#132,ws_sold_time_sk#133,ws_ship_date_sk#134,ws_item_sk#135,ws_bill_customer_sk#136,ws_bill_cdemo_sk#137,ws_bill_hdemo_sk#138,ws_bill_addr_sk#139,ws_ship_customer_sk#140,ws_ship_cdemo_sk#141,ws_ship_hdemo_sk#142,ws_ship_addr_sk#143,ws_web_page_sk#144,ws_web_site_sk#145,ws_ship_mode_sk#146,ws_warehouse_sk#147,ws_promo_sk#148,ws_order_number#149,ws_quantity#150,ws_wholesale_cost#151,ws_list_price#152,ws_sales_price#153,ws_ext_discount_amt#154,ws_ext_sales_price#155,... 10 more fields] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#229,d_date_id#230,d_date#231,d_month_seq#232,d_week_seq#233,d_quarter_seq#234,d_year#235,d_dow#236,d_moy#237,d_dom#238,d_qoy#239,d_fy_year#240,d_fy_quarter_seq#241,d_fy_week_seq#242,d_day_name#243,d_quarter_name#244,d_holiday#245,d_weekend#246,d_following_holiday#247,d_first_dom#248,d_last_dom#249,d_same_day_ly#250,d_same_day_lq#251,d_current_day#252,... 4 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.customer_address
:              :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#257,ca_address_id#258,ca_street_number#259,ca_street_name#260,ca_street_type#261,ca_suite_number#262,ca_city#263,ca_county#264,ca_state#265,ca_zip#266,ca_country#267,ca_gmt_offset#268,ca_location_type#269] parquet
:              +- SubqueryAlias spark_catalog.tpcds.item
:                 +- Relation spark_catalog.tpcds.item[i_item_sk#270,i_item_id#271,i_rec_start_date#272,i_rec_end_date#273,i_item_desc#274,i_current_price#275,i_wholesale_cost#276,i_brand_id#277,i_brand#278,i_class_id#279,i_class#280,i_category_id#281,i_category#282,i_manufact_id#283,i_manufact#284,i_size#285,i_formulation#286,i_color#287,i_units#288,i_container#289,i_manager_id#290,i_product_name#291] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [total_sales#0 ASC NULLS FIRST], true
         +- Aggregate [i_manufact_id#89], [i_manufact_id#89, sum(total_sales#1) AS total_sales#0]
            +- SubqueryAlias tmp1
               +- Union false, false
                  :- Union false, false
                  :  :- Project [i_manufact_id#89, total_sales#1]
                  :  :  +- SubqueryAlias ss
                  :  :     +- CTERelationRef 0, true, [i_manufact_id#89, total_sales#1], false
                  :  +- Project [i_manufact_id#220, total_sales#3]
                  :     +- SubqueryAlias cs
                  :        +- CTERelationRef 1, true, [i_manufact_id#220, total_sales#3], false
                  +- Project [i_manufact_id#283, total_sales#5]
                     +- SubqueryAlias ws
                        +- CTERelationRef 2, true, [i_manufact_id#283, total_sales#5], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [total_sales#0 ASC NULLS FIRST], true
      +- Aggregate [i_manufact_id#89], [i_manufact_id#89, sum(total_sales#1) AS total_sales#0]
         +- Union false, false
            :- Aggregate [i_manufact_id#89], [i_manufact_id#89, sum(ss_ext_sales_price#27) AS total_sales#1]
            :  +- Project [ss_ext_sales_price#27, i_manufact_id#89]
            :     +- Join Inner, (ss_item_sk#14 = i_item_sk#76)
            :        :- Project [ss_item_sk#14, ss_ext_sales_price#27]
            :        :  +- Join Inner, (ss_addr_sk#18 = ca_address_sk#63)
            :        :     :- Project [ss_item_sk#14, ss_addr_sk#18, ss_ext_sales_price#27]
            :        :     :  +- Join Inner, (ss_sold_date_sk#12 = d_date_sk#35)
            :        :     :     :- Project [ss_sold_date_sk#12, ss_item_sk#14, ss_addr_sk#18, ss_ext_sales_price#27]
            :        :     :     :  +- Filter (isnotnull(ss_sold_date_sk#12) AND (isnotnull(ss_addr_sk#18) AND isnotnull(ss_item_sk#14)))
            :        :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#12,ss_sold_time_sk#13,ss_item_sk#14,ss_customer_sk#15,ss_cdemo_sk#16,ss_hdemo_sk#17,ss_addr_sk#18,ss_store_sk#19,ss_promo_sk#20,ss_ticket_number#21,ss_quantity#22,ss_wholesale_cost#23,ss_list_price#24,ss_sales_price#25,ss_ext_discount_amt#26,ss_ext_sales_price#27,ss_ext_wholesale_cost#28,ss_ext_list_price#29,ss_ext_tax#30,ss_coupon_amt#31,ss_net_paid#32,ss_net_paid_inc_tax#33,ss_net_profit#34] parquet
            :        :     :     +- Project [d_date_sk#35]
            :        :     :        +- Filter (((isnotnull(d_year#41) AND isnotnull(d_moy#43)) AND ((d_year#41 = 1998) AND (d_moy#43 = 5))) AND isnotnull(d_date_sk#35))
            :        :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#35,d_date_id#36,d_date#37,d_month_seq#38,d_week_seq#39,d_quarter_seq#40,d_year#41,d_dow#42,d_moy#43,d_dom#44,d_qoy#45,d_fy_year#46,d_fy_quarter_seq#47,d_fy_week_seq#48,d_day_name#49,d_quarter_name#50,d_holiday#51,d_weekend#52,d_following_holiday#53,d_first_dom#54,d_last_dom#55,d_same_day_ly#56,d_same_day_lq#57,d_current_day#58,... 4 more fields] parquet
            :        :     +- Project [ca_address_sk#63]
            :        :        +- Filter ((isnotnull(ca_gmt_offset#74) AND (ca_gmt_offset#74 = -5.0)) AND isnotnull(ca_address_sk#63))
            :        :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#63,ca_address_id#64,ca_street_number#65,ca_street_name#66,ca_street_type#67,ca_suite_number#68,ca_city#69,ca_county#70,ca_state#71,ca_zip#72,ca_country#73,ca_gmt_offset#74,ca_location_type#75] parquet
            :        +- Join LeftSemi, (i_manufact_id#89 = i_manufact_id#317)
            :           :- Project [i_item_sk#76, i_manufact_id#89]
            :           :  +- Filter isnotnull(i_item_sk#76)
            :           :     +- Relation spark_catalog.tpcds.item[i_item_sk#76,i_item_id#77,i_rec_start_date#78,i_rec_end_date#79,i_item_desc#80,i_current_price#81,i_wholesale_cost#82,i_brand_id#83,i_brand#84,i_class_id#85,i_class#86,i_category_id#87,i_category#88,i_manufact_id#89,i_manufact#90,i_size#91,i_formulation#92,i_color#93,i_units#94,i_container#95,i_manager_id#96,i_product_name#97] parquet
            :           +- Project [i_manufact_id#317]
            :              +- Filter (isnotnull(i_category#316) AND (i_category#316 = Electronics))
            :                 +- Relation spark_catalog.tpcds.item[i_item_sk#304,i_item_id#305,i_rec_start_date#306,i_rec_end_date#307,i_item_desc#308,i_current_price#309,i_wholesale_cost#310,i_brand_id#311,i_brand#312,i_class_id#313,i_class#314,i_category_id#315,i_category#316,i_manufact_id#317,i_manufact#318,i_size#319,i_formulation#320,i_color#321,i_units#322,i_container#323,i_manager_id#324,i_product_name#325] parquet
            :- Aggregate [i_manufact_id#220], [i_manufact_id#220, sum(cs_ext_sales_price#121) AS total_sales#3]
            :  +- Project [cs_ext_sales_price#121, i_manufact_id#220]
            :     +- Join Inner, (cs_item_sk#113 = i_item_sk#207)
            :        :- Project [cs_item_sk#113, cs_ext_sales_price#121]
            :        :  +- Join Inner, (cs_bill_addr_sk#104 = ca_address_sk#194)
            :        :     :- Project [cs_bill_addr_sk#104, cs_item_sk#113, cs_ext_sales_price#121]
            :        :     :  +- Join Inner, (cs_sold_date_sk#98 = d_date_sk#166)
            :        :     :     :- Project [cs_sold_date_sk#98, cs_bill_addr_sk#104, cs_item_sk#113, cs_ext_sales_price#121]
            :        :     :     :  +- Filter (isnotnull(cs_sold_date_sk#98) AND (isnotnull(cs_bill_addr_sk#104) AND isnotnull(cs_item_sk#113)))
            :        :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#98,cs_sold_time_sk#99,cs_ship_date_sk#100,cs_bill_customer_sk#101,cs_bill_cdemo_sk#102,cs_bill_hdemo_sk#103,cs_bill_addr_sk#104,cs_ship_customer_sk#105,cs_ship_cdemo_sk#106,cs_ship_hdemo_sk#107,cs_ship_addr_sk#108,cs_call_center_sk#109,cs_catalog_page_sk#110,cs_ship_mode_sk#111,cs_warehouse_sk#112,cs_item_sk#113,cs_promo_sk#114,cs_order_number#115,cs_quantity#116,cs_wholesale_cost#117,cs_list_price#118,cs_sales_price#119,cs_ext_discount_amt#120,cs_ext_sales_price#121,... 10 more fields] parquet
            :        :     :     +- Project [d_date_sk#166]
            :        :     :        +- Filter (((isnotnull(d_year#172) AND isnotnull(d_moy#174)) AND ((d_year#172 = 1998) AND (d_moy#174 = 5))) AND isnotnull(d_date_sk#166))
            :        :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#166,d_date_id#167,d_date#168,d_month_seq#169,d_week_seq#170,d_quarter_seq#171,d_year#172,d_dow#173,d_moy#174,d_dom#175,d_qoy#176,d_fy_year#177,d_fy_quarter_seq#178,d_fy_week_seq#179,d_day_name#180,d_quarter_name#181,d_holiday#182,d_weekend#183,d_following_holiday#184,d_first_dom#185,d_last_dom#186,d_same_day_ly#187,d_same_day_lq#188,d_current_day#189,... 4 more fields] parquet
            :        :     +- Project [ca_address_sk#194]
            :        :        +- Filter ((isnotnull(ca_gmt_offset#205) AND (ca_gmt_offset#205 = -5.0)) AND isnotnull(ca_address_sk#194))
            :        :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#194,ca_address_id#195,ca_street_number#196,ca_street_name#197,ca_street_type#198,ca_suite_number#199,ca_city#200,ca_county#201,ca_state#202,ca_zip#203,ca_country#204,ca_gmt_offset#205,ca_location_type#206] parquet
            :        +- Join LeftSemi, (i_manufact_id#220 = i_manufact_id#339)
            :           :- Project [i_item_sk#207, i_manufact_id#220]
            :           :  +- Filter isnotnull(i_item_sk#207)
            :           :     +- Relation spark_catalog.tpcds.item[i_item_sk#207,i_item_id#208,i_rec_start_date#209,i_rec_end_date#210,i_item_desc#211,i_current_price#212,i_wholesale_cost#213,i_brand_id#214,i_brand#215,i_class_id#216,i_class#217,i_category_id#218,i_category#219,i_manufact_id#220,i_manufact#221,i_size#222,i_formulation#223,i_color#224,i_units#225,i_container#226,i_manager_id#227,i_product_name#228] parquet
            :           +- Project [i_manufact_id#339]
            :              +- Filter (isnotnull(i_category#338) AND (i_category#338 = Electronics))
            :                 +- Relation spark_catalog.tpcds.item[i_item_sk#326,i_item_id#327,i_rec_start_date#328,i_rec_end_date#329,i_item_desc#330,i_current_price#331,i_wholesale_cost#332,i_brand_id#333,i_brand#334,i_class_id#335,i_class#336,i_category_id#337,i_category#338,i_manufact_id#339,i_manufact#340,i_size#341,i_formulation#342,i_color#343,i_units#344,i_container#345,i_manager_id#346,i_product_name#347] parquet
            +- Aggregate [i_manufact_id#283], [i_manufact_id#283, sum(ws_ext_sales_price#155) AS total_sales#5]
               +- Project [ws_ext_sales_price#155, i_manufact_id#283]
                  +- Join Inner, (ws_item_sk#135 = i_item_sk#270)
                     :- Project [ws_item_sk#135, ws_ext_sales_price#155]
                     :  +- Join Inner, (ws_bill_addr_sk#139 = ca_address_sk#257)
                     :     :- Project [ws_item_sk#135, ws_bill_addr_sk#139, ws_ext_sales_price#155]
                     :     :  +- Join Inner, (ws_sold_date_sk#132 = d_date_sk#229)
                     :     :     :- Project [ws_sold_date_sk#132, ws_item_sk#135, ws_bill_addr_sk#139, ws_ext_sales_price#155]
                     :     :     :  +- Filter (isnotnull(ws_sold_date_sk#132) AND (isnotnull(ws_bill_addr_sk#139) AND isnotnull(ws_item_sk#135)))
                     :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#132,ws_sold_time_sk#133,ws_ship_date_sk#134,ws_item_sk#135,ws_bill_customer_sk#136,ws_bill_cdemo_sk#137,ws_bill_hdemo_sk#138,ws_bill_addr_sk#139,ws_ship_customer_sk#140,ws_ship_cdemo_sk#141,ws_ship_hdemo_sk#142,ws_ship_addr_sk#143,ws_web_page_sk#144,ws_web_site_sk#145,ws_ship_mode_sk#146,ws_warehouse_sk#147,ws_promo_sk#148,ws_order_number#149,ws_quantity#150,ws_wholesale_cost#151,ws_list_price#152,ws_sales_price#153,ws_ext_discount_amt#154,ws_ext_sales_price#155,... 10 more fields] parquet
                     :     :     +- Project [d_date_sk#229]
                     :     :        +- Filter (((isnotnull(d_year#235) AND isnotnull(d_moy#237)) AND ((d_year#235 = 1998) AND (d_moy#237 = 5))) AND isnotnull(d_date_sk#229))
                     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#229,d_date_id#230,d_date#231,d_month_seq#232,d_week_seq#233,d_quarter_seq#234,d_year#235,d_dow#236,d_moy#237,d_dom#238,d_qoy#239,d_fy_year#240,d_fy_quarter_seq#241,d_fy_week_seq#242,d_day_name#243,d_quarter_name#244,d_holiday#245,d_weekend#246,d_following_holiday#247,d_first_dom#248,d_last_dom#249,d_same_day_ly#250,d_same_day_lq#251,d_current_day#252,... 4 more fields] parquet
                     :     +- Project [ca_address_sk#257]
                     :        +- Filter ((isnotnull(ca_gmt_offset#268) AND (ca_gmt_offset#268 = -5.0)) AND isnotnull(ca_address_sk#257))
                     :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#257,ca_address_id#258,ca_street_number#259,ca_street_name#260,ca_street_type#261,ca_suite_number#262,ca_city#263,ca_county#264,ca_state#265,ca_zip#266,ca_country#267,ca_gmt_offset#268,ca_location_type#269] parquet
                     +- Join LeftSemi, (i_manufact_id#283 = i_manufact_id#361)
                        :- Project [i_item_sk#270, i_manufact_id#283]
                        :  +- Filter isnotnull(i_item_sk#270)
                        :     +- Relation spark_catalog.tpcds.item[i_item_sk#270,i_item_id#271,i_rec_start_date#272,i_rec_end_date#273,i_item_desc#274,i_current_price#275,i_wholesale_cost#276,i_brand_id#277,i_brand#278,i_class_id#279,i_class#280,i_category_id#281,i_category#282,i_manufact_id#283,i_manufact#284,i_size#285,i_formulation#286,i_color#287,i_units#288,i_container#289,i_manager_id#290,i_product_name#291] parquet
                        +- Project [i_manufact_id#361]
                           +- Filter (isnotnull(i_category#360) AND (i_category#360 = Electronics))
                              +- Relation spark_catalog.tpcds.item[i_item_sk#348,i_item_id#349,i_rec_start_date#350,i_rec_end_date#351,i_item_desc#352,i_current_price#353,i_wholesale_cost#354,i_brand_id#355,i_brand#356,i_class_id#357,i_class#358,i_category_id#359,i_category#360,i_manufact_id#361,i_manufact#362,i_size#363,i_formulation#364,i_color#365,i_units#366,i_container#367,i_manager_id#368,i_product_name#369] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[total_sales#0 ASC NULLS FIRST], output=[i_manufact_id#89,total_sales#0])
   +- HashAggregate(keys=[i_manufact_id#89], functions=[sum(total_sales#1)], output=[i_manufact_id#89, total_sales#0])
      +- Exchange hashpartitioning(i_manufact_id#89, 200), ENSURE_REQUIREMENTS, [plan_id=330]
         +- HashAggregate(keys=[i_manufact_id#89], functions=[partial_sum(total_sales#1)], output=[i_manufact_id#89, sum#375])
            +- Union
               :- HashAggregate(keys=[i_manufact_id#89], functions=[sum(ss_ext_sales_price#27)], output=[i_manufact_id#89, total_sales#1])
               :  +- Exchange hashpartitioning(i_manufact_id#89, 200), ENSURE_REQUIREMENTS, [plan_id=291]
               :     +- HashAggregate(keys=[i_manufact_id#89], functions=[partial_sum(ss_ext_sales_price#27)], output=[i_manufact_id#89, sum#377])
               :        +- Project [ss_ext_sales_price#27, i_manufact_id#89]
               :           +- BroadcastHashJoin [ss_item_sk#14], [i_item_sk#76], Inner, BuildRight, false
               :              :- Project [ss_item_sk#14, ss_ext_sales_price#27]
               :              :  +- BroadcastHashJoin [ss_addr_sk#18], [ca_address_sk#63], Inner, BuildRight, false
               :              :     :- Project [ss_item_sk#14, ss_addr_sk#18, ss_ext_sales_price#27]
               :              :     :  +- BroadcastHashJoin [ss_sold_date_sk#12], [d_date_sk#35], Inner, BuildRight, false
               :              :     :     :- Filter ((isnotnull(ss_sold_date_sk#12) AND isnotnull(ss_addr_sk#18)) AND isnotnull(ss_item_sk#14))
               :              :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#12,ss_item_sk#14,ss_addr_sk#18,ss_ext_sales_price#27] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#12), isnotnull(ss_addr_sk#18), isnotnull(ss_item_sk#14)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
               :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=276]
               :              :     :        +- Project [d_date_sk#35]
               :              :     :           +- Filter ((((isnotnull(d_year#41) AND isnotnull(d_moy#43)) AND (d_year#41 = 1998)) AND (d_moy#43 = 5)) AND isnotnull(d_date_sk#35))
               :              :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#35,d_year#41,d_moy#43] Batched: true, DataFilters: [isnotnull(d_year#41), isnotnull(d_moy#43), (d_year#41 = 1998), (d_moy#43 = 5), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,5), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=280]
               :              :        +- Project [ca_address_sk#63]
               :              :           +- Filter ((isnotnull(ca_gmt_offset#74) AND (ca_gmt_offset#74 = -5.0)) AND isnotnull(ca_address_sk#63))
               :              :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#63,ca_gmt_offset#74] Batched: true, DataFilters: [isnotnull(ca_gmt_offset#74), (ca_gmt_offset#74 = -5.0), isnotnull(ca_address_sk#63)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-5.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
               :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=286]
               :                 +- BroadcastHashJoin [i_manufact_id#89], [i_manufact_id#317], LeftSemi, BuildRight, false
               :                    :- Filter isnotnull(i_item_sk#76)
               :                    :  +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#76,i_manufact_id#89] Batched: true, DataFilters: [isnotnull(i_item_sk#76)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_manufact_id:int>
               :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=283]
               :                       +- Project [i_manufact_id#317]
               :                          +- Filter (isnotnull(i_category#316) AND (i_category#316 = Electronics))
               :                             +- FileScan parquet spark_catalog.tpcds.item[i_category#316,i_manufact_id#317] Batched: true, DataFilters: [isnotnull(i_category#316), (i_category#316 = Electronics)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Electronics)], ReadSchema: struct<i_category:string,i_manufact_id:int>
               :- HashAggregate(keys=[i_manufact_id#220], functions=[sum(cs_ext_sales_price#121)], output=[i_manufact_id#220, total_sales#3])
               :  +- Exchange hashpartitioning(i_manufact_id#220, 200), ENSURE_REQUIREMENTS, [plan_id=308]
               :     +- HashAggregate(keys=[i_manufact_id#220], functions=[partial_sum(cs_ext_sales_price#121)], output=[i_manufact_id#220, sum#379])
               :        +- Project [cs_ext_sales_price#121, i_manufact_id#220]
               :           +- BroadcastHashJoin [cs_item_sk#113], [i_item_sk#207], Inner, BuildRight, false
               :              :- Project [cs_item_sk#113, cs_ext_sales_price#121]
               :              :  +- BroadcastHashJoin [cs_bill_addr_sk#104], [ca_address_sk#194], Inner, BuildRight, false
               :              :     :- Project [cs_bill_addr_sk#104, cs_item_sk#113, cs_ext_sales_price#121]
               :              :     :  +- BroadcastHashJoin [cs_sold_date_sk#98], [d_date_sk#166], Inner, BuildRight, false
               :              :     :     :- Filter ((isnotnull(cs_sold_date_sk#98) AND isnotnull(cs_bill_addr_sk#104)) AND isnotnull(cs_item_sk#113))
               :              :     :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#98,cs_bill_addr_sk#104,cs_item_sk#113,cs_ext_sales_price#121] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#98), isnotnull(cs_bill_addr_sk#104), isnotnull(cs_item_sk#113)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_bill_addr_sk), IsNotNull(cs_item_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_addr_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
               :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=293]
               :              :     :        +- Project [d_date_sk#166]
               :              :     :           +- Filter ((((isnotnull(d_year#172) AND isnotnull(d_moy#174)) AND (d_year#172 = 1998)) AND (d_moy#174 = 5)) AND isnotnull(d_date_sk#166))
               :              :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#166,d_year#172,d_moy#174] Batched: true, DataFilters: [isnotnull(d_year#172), isnotnull(d_moy#174), (d_year#172 = 1998), (d_moy#174 = 5), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,5), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=297]
               :              :        +- Project [ca_address_sk#194]
               :              :           +- Filter ((isnotnull(ca_gmt_offset#205) AND (ca_gmt_offset#205 = -5.0)) AND isnotnull(ca_address_sk#194))
               :              :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#194,ca_gmt_offset#205] Batched: true, DataFilters: [isnotnull(ca_gmt_offset#205), (ca_gmt_offset#205 = -5.0), isnotnull(ca_address_sk#194)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-5.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
               :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=303]
               :                 +- BroadcastHashJoin [i_manufact_id#220], [i_manufact_id#339], LeftSemi, BuildRight, false
               :                    :- Filter isnotnull(i_item_sk#207)
               :                    :  +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#207,i_manufact_id#220] Batched: true, DataFilters: [isnotnull(i_item_sk#207)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_manufact_id:int>
               :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=300]
               :                       +- Project [i_manufact_id#339]
               :                          +- Filter (isnotnull(i_category#338) AND (i_category#338 = Electronics))
               :                             +- FileScan parquet spark_catalog.tpcds.item[i_category#338,i_manufact_id#339] Batched: true, DataFilters: [isnotnull(i_category#338), (i_category#338 = Electronics)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Electronics)], ReadSchema: struct<i_category:string,i_manufact_id:int>
               +- HashAggregate(keys=[i_manufact_id#283], functions=[sum(ws_ext_sales_price#155)], output=[i_manufact_id#283, total_sales#5])
                  +- Exchange hashpartitioning(i_manufact_id#283, 200), ENSURE_REQUIREMENTS, [plan_id=325]
                     +- HashAggregate(keys=[i_manufact_id#283], functions=[partial_sum(ws_ext_sales_price#155)], output=[i_manufact_id#283, sum#381])
                        +- Project [ws_ext_sales_price#155, i_manufact_id#283]
                           +- BroadcastHashJoin [ws_item_sk#135], [i_item_sk#270], Inner, BuildRight, false
                              :- Project [ws_item_sk#135, ws_ext_sales_price#155]
                              :  +- BroadcastHashJoin [ws_bill_addr_sk#139], [ca_address_sk#257], Inner, BuildRight, false
                              :     :- Project [ws_item_sk#135, ws_bill_addr_sk#139, ws_ext_sales_price#155]
                              :     :  +- BroadcastHashJoin [ws_sold_date_sk#132], [d_date_sk#229], Inner, BuildRight, false
                              :     :     :- Filter ((isnotnull(ws_sold_date_sk#132) AND isnotnull(ws_bill_addr_sk#139)) AND isnotnull(ws_item_sk#135))
                              :     :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#132,ws_item_sk#135,ws_bill_addr_sk#139,ws_ext_sales_price#155] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#132), isnotnull(ws_bill_addr_sk#139), isnotnull(ws_item_sk#135)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk), IsNotNull(ws_item_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
                              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=310]
                              :     :        +- Project [d_date_sk#229]
                              :     :           +- Filter ((((isnotnull(d_year#235) AND isnotnull(d_moy#237)) AND (d_year#235 = 1998)) AND (d_moy#237 = 5)) AND isnotnull(d_date_sk#229))
                              :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#229,d_year#235,d_moy#237] Batched: true, DataFilters: [isnotnull(d_year#235), isnotnull(d_moy#237), (d_year#235 = 1998), (d_moy#237 = 5), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,5), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=314]
                              :        +- Project [ca_address_sk#257]
                              :           +- Filter ((isnotnull(ca_gmt_offset#268) AND (ca_gmt_offset#268 = -5.0)) AND isnotnull(ca_address_sk#257))
                              :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#257,ca_gmt_offset#268] Batched: true, DataFilters: [isnotnull(ca_gmt_offset#268), (ca_gmt_offset#268 = -5.0), isnotnull(ca_address_sk#257)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-5.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=320]
                                 +- BroadcastHashJoin [i_manufact_id#283], [i_manufact_id#361], LeftSemi, BuildRight, false
                                    :- Filter isnotnull(i_item_sk#270)
                                    :  +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#270,i_manufact_id#283] Batched: true, DataFilters: [isnotnull(i_item_sk#270)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_manufact_id:int>
                                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=317]
                                       +- Project [i_manufact_id#361]
                                          +- Filter (isnotnull(i_category#360) AND (i_category#360 = Electronics))
                                             +- FileScan parquet spark_catalog.tpcds.item[i_category#360,i_manufact_id#361] Batched: true, DataFilters: [isnotnull(i_category#360), (i_category#360 = Electronics)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Electronics)], ReadSchema: struct<i_category:string,i_manufact_id:int>

Time taken: 3.483 seconds, Fetched 1 row(s)
