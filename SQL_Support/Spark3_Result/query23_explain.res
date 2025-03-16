Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580365286
== Parsed Logical Plan ==
CTE [frequent_ss_items, max_store_sales, best_ss_customer]
:  :- 'SubqueryAlias frequent_ss_items
:  :  +- 'UnresolvedHaving ('count(1) > 4)
:  :     +- 'Aggregate ['substr('i_item_desc, 1, 30), 'i_item_sk, 'd_date], ['substr('i_item_desc, 1, 30) AS itemdesc#6, 'i_item_sk AS item_sk#7, 'd_date AS solddate#8, 'count(1) AS cnt#9]
:  :        +- 'Filter ((('ss_sold_date_sk = 'd_date_sk) AND ('ss_item_sk = 'i_item_sk)) AND 'd_year IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
:  :           +- 'Join Inner
:  :              :- 'Join Inner
:  :              :  :- 'UnresolvedRelation [store_sales], [], false
:  :              :  +- 'UnresolvedRelation [date_dim], [], false
:  :              +- 'UnresolvedRelation [item], [], false
:  :- 'SubqueryAlias max_store_sales
:  :  +- 'Project ['max('csales) AS tpcds_cmax#11]
:  :     +- 'SubqueryAlias __auto_generated_subquery_name
:  :        +- 'Aggregate ['c_customer_sk], ['c_customer_sk, 'sum(('ss_quantity * 'ss_sales_price)) AS csales#10]
:  :           +- 'Filter ((('ss_customer_sk = 'c_customer_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND 'd_year IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
:  :              +- 'Join Inner
:  :                 :- 'Join Inner
:  :                 :  :- 'UnresolvedRelation [store_sales], [], false
:  :                 :  +- 'UnresolvedRelation [customer], [], false
:  :                 +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias best_ss_customer
:     +- 'UnresolvedHaving ('sum(('ss_quantity * 'ss_sales_price)) > ((50 / 100.0) * scalar-subquery#13 []))
:        :  +- 'Project [*]
:        :     +- 'UnresolvedRelation [max_store_sales], [], false
:        +- 'Aggregate ['c_customer_sk], ['c_customer_sk, 'sum(('ss_quantity * 'ss_sales_price)) AS ssales#12]
:           +- 'Filter ('ss_customer_sk = 'c_customer_sk)
:              +- 'Join Inner
:                 :- 'UnresolvedRelation [store_sales], [], false
:                 +- 'UnresolvedRelation [customer], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Project [unresolvedalias('sum('sales), None)]
         +- 'SubqueryAlias __auto_generated_subquery_name
            +- 'Union false, false
               :- 'Project [('cs_quantity * 'cs_list_price) AS sales#0]
               :  +- 'Filter (((('d_year = 2000) AND ('d_moy = 2)) AND ('cs_sold_date_sk = 'd_date_sk)) AND ('cs_item_sk IN (list#1 []) AND 'cs_bill_customer_sk IN (list#2 [])))
               :     :  :- 'Project ['item_sk]
               :     :  :  +- 'UnresolvedRelation [frequent_ss_items], [], false
               :     :  +- 'Project ['c_customer_sk]
               :     :     +- 'UnresolvedRelation [best_ss_customer], [], false
               :     +- 'Join Inner
               :        :- 'UnresolvedRelation [catalog_sales], [], false
               :        +- 'UnresolvedRelation [date_dim], [], false
               +- 'Project [('ws_quantity * 'ws_list_price) AS sales#3]
                  +- 'Filter (((('d_year = 2000) AND ('d_moy = 2)) AND ('ws_sold_date_sk = 'd_date_sk)) AND ('ws_item_sk IN (list#4 []) AND 'ws_bill_customer_sk IN (list#5 [])))
                     :  :- 'Project ['item_sk]
                     :  :  +- 'UnresolvedRelation [frequent_ss_items], [], false
                     :  +- 'Project ['c_customer_sk]
                     :     +- 'UnresolvedRelation [best_ss_customer], [], false
                     +- 'Join Inner
                        :- 'UnresolvedRelation [web_sales], [], false
                        +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
sum(sales): double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias frequent_ss_items
:     +- Filter (cnt#9L > cast(4 as bigint))
:        +- Aggregate [substr(i_item_desc#76, 1, 30), i_item_sk#72, d_date#46], [substr(i_item_desc#76, 1, 30) AS itemdesc#6, i_item_sk#72 AS item_sk#7, d_date#46 AS solddate#8, count(1) AS cnt#9L]
:           +- Filter (((ss_sold_date_sk#21 = d_date_sk#44) AND (ss_item_sk#23 = i_item_sk#72)) AND d_year#50 IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
:              +- Join Inner
:                 :- Join Inner
:                 :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:                 :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
:                 :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
:                 +- SubqueryAlias spark_catalog.tpcds.item
:                    +- Relation spark_catalog.tpcds.item[i_item_sk#72,i_item_id#73,i_rec_start_date#74,i_rec_end_date#75,i_item_desc#76,i_current_price#77,i_wholesale_cost#78,i_brand_id#79,i_brand#80,i_class_id#81,i_class#82,i_category_id#83,i_category#84,i_manufact_id#85,i_manufact#86,i_size#87,i_formulation#88,i_color#89,i_units#90,i_container#91,i_manager_id#92,i_product_name#93] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias max_store_sales
:     +- Aggregate [max(csales#10) AS tpcds_cmax#11]
:        +- SubqueryAlias __auto_generated_subquery_name
:           +- Aggregate [c_customer_sk#94], [c_customer_sk#94, sum((cast(ss_quantity#190 as double) * ss_sales_price#193)) AS csales#10]
:              +- Filter (((ss_customer_sk#183 = c_customer_sk#94) AND (ss_sold_date_sk#180 = d_date_sk#203)) AND d_year#209 IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
:                 +- Join Inner
:                    :- Join Inner
:                    :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:                    :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#180,ss_sold_time_sk#181,ss_item_sk#182,ss_customer_sk#183,ss_cdemo_sk#184,ss_hdemo_sk#185,ss_addr_sk#186,ss_store_sk#187,ss_promo_sk#188,ss_ticket_number#189,ss_quantity#190,ss_wholesale_cost#191,ss_list_price#192,ss_sales_price#193,ss_ext_discount_amt#194,ss_ext_sales_price#195,ss_ext_wholesale_cost#196,ss_ext_list_price#197,ss_ext_tax#198,ss_coupon_amt#199,ss_net_paid#200,ss_net_paid_inc_tax#201,ss_net_profit#202] parquet
:                    :  +- SubqueryAlias spark_catalog.tpcds.customer
:                    :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
:                    +- SubqueryAlias spark_catalog.tpcds.date_dim
:                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#203,d_date_id#204,d_date#205,d_month_seq#206,d_week_seq#207,d_quarter_seq#208,d_year#209,d_dow#210,d_moy#211,d_dom#212,d_qoy#213,d_fy_year#214,d_fy_quarter_seq#215,d_fy_week_seq#216,d_day_name#217,d_quarter_name#218,d_holiday#219,d_weekend#220,d_following_holiday#221,d_first_dom#222,d_last_dom#223,d_same_day_ly#224,d_same_day_lq#225,d_current_day#226,... 4 more fields] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias best_ss_customer
:     +- Filter (ssales#12 > (cast((cast(50 as decimal(2,0)) / 100.0) as double) * scalar-subquery#13 []))
:        :  +- Project [tpcds_cmax#11]
:        :     +- SubqueryAlias max_store_sales
:        :        +- CTERelationRef 1, true, [tpcds_cmax#11], false
:        +- Aggregate [c_customer_sk#254], [c_customer_sk#254, sum((cast(ss_quantity#241 as double) * ss_sales_price#244)) AS ssales#12]
:           +- Filter (ss_customer_sk#234 = c_customer_sk#254)
:              +- Join Inner
:                 :- SubqueryAlias spark_catalog.tpcds.store_sales
:                 :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#231,ss_sold_time_sk#232,ss_item_sk#233,ss_customer_sk#234,ss_cdemo_sk#235,ss_hdemo_sk#236,ss_addr_sk#237,ss_store_sk#238,ss_promo_sk#239,ss_ticket_number#240,ss_quantity#241,ss_wholesale_cost#242,ss_list_price#243,ss_sales_price#244,ss_ext_discount_amt#245,ss_ext_sales_price#246,ss_ext_wholesale_cost#247,ss_ext_list_price#248,ss_ext_tax#249,ss_coupon_amt#250,ss_net_paid#251,ss_net_paid_inc_tax#252,ss_net_profit#253] parquet
:                 +- SubqueryAlias spark_catalog.tpcds.customer
:                    +- Relation spark_catalog.tpcds.customer[c_customer_sk#254,c_customer_id#255,c_current_cdemo_sk#256,c_current_hdemo_sk#257,c_current_addr_sk#258,c_first_shipto_date_sk#259,c_first_sales_date_sk#260,c_salutation#261,c_first_name#262,c_last_name#263,c_preferred_cust_flag#264,c_birth_day#265,c_birth_month#266,c_birth_year#267,c_birth_country#268,c_login#269,c_email_address#270,c_last_review_date#271] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Aggregate [sum(sales#0) AS sum(sales)#351]
         +- SubqueryAlias __auto_generated_subquery_name
            +- Union false, false
               :- Project [(cast(cs_quantity#130 as double) * cs_list_price#132) AS sales#0]
               :  +- Filter ((((d_year#278 = 2000) AND (d_moy#280 = 2)) AND (cs_sold_date_sk#112 = d_date_sk#272)) AND (cs_item_sk#127 IN (list#1 []) AND cs_bill_customer_sk#115 IN (list#2 [])))
               :     :  :- Project [item_sk#7]
               :     :  :  +- SubqueryAlias frequent_ss_items
               :     :  :     +- CTERelationRef 0, true, [itemdesc#6, item_sk#7, solddate#8, cnt#9L], false
               :     :  +- Project [c_customer_sk#254]
               :     :     +- SubqueryAlias best_ss_customer
               :     :        +- CTERelationRef 2, true, [c_customer_sk#254, ssales#12], false
               :     +- Join Inner
               :        :- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :        :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#112,cs_sold_time_sk#113,cs_ship_date_sk#114,cs_bill_customer_sk#115,cs_bill_cdemo_sk#116,cs_bill_hdemo_sk#117,cs_bill_addr_sk#118,cs_ship_customer_sk#119,cs_ship_cdemo_sk#120,cs_ship_hdemo_sk#121,cs_ship_addr_sk#122,cs_call_center_sk#123,cs_catalog_page_sk#124,cs_ship_mode_sk#125,cs_warehouse_sk#126,cs_item_sk#127,cs_promo_sk#128,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_list_price#132,cs_sales_price#133,cs_ext_discount_amt#134,cs_ext_sales_price#135,... 10 more fields] parquet
               :        +- SubqueryAlias spark_catalog.tpcds.date_dim
               :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#272,d_date_id#273,d_date#274,d_month_seq#275,d_week_seq#276,d_quarter_seq#277,d_year#278,d_dow#279,d_moy#280,d_dom#281,d_qoy#282,d_fy_year#283,d_fy_quarter_seq#284,d_fy_week_seq#285,d_day_name#286,d_quarter_name#287,d_holiday#288,d_weekend#289,d_following_holiday#290,d_first_dom#291,d_last_dom#292,d_same_day_ly#293,d_same_day_lq#294,d_current_day#295,... 4 more fields] parquet
               +- Project [(cast(ws_quantity#164 as double) * ws_list_price#166) AS sales#3]
                  +- Filter ((((d_year#306 = 2000) AND (d_moy#308 = 2)) AND (ws_sold_date_sk#146 = d_date_sk#300)) AND (ws_item_sk#149 IN (list#4 []) AND ws_bill_customer_sk#150 IN (list#5 [])))
                     :  :- Project [item_sk#339]
                     :  :  +- SubqueryAlias frequent_ss_items
                     :  :     +- CTERelationRef 0, true, [itemdesc#338, item_sk#339, solddate#340, cnt#341L], false
                     :  +- Project [c_customer_sk#344]
                     :     +- SubqueryAlias best_ss_customer
                     :        +- CTERelationRef 2, true, [c_customer_sk#344, ssales#345], false
                     +- Join Inner
                        :- SubqueryAlias spark_catalog.tpcds.web_sales
                        :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#146,ws_sold_time_sk#147,ws_ship_date_sk#148,ws_item_sk#149,ws_bill_customer_sk#150,ws_bill_cdemo_sk#151,ws_bill_hdemo_sk#152,ws_bill_addr_sk#153,ws_ship_customer_sk#154,ws_ship_cdemo_sk#155,ws_ship_hdemo_sk#156,ws_ship_addr_sk#157,ws_web_page_sk#158,ws_web_site_sk#159,ws_ship_mode_sk#160,ws_warehouse_sk#161,ws_promo_sk#162,ws_order_number#163,ws_quantity#164,ws_wholesale_cost#165,ws_list_price#166,ws_sales_price#167,ws_ext_discount_amt#168,ws_ext_sales_price#169,... 10 more fields] parquet
                        +- SubqueryAlias spark_catalog.tpcds.date_dim
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#300,d_date_id#301,d_date#302,d_month_seq#303,d_week_seq#304,d_quarter_seq#305,d_year#306,d_dow#307,d_moy#308,d_dom#309,d_qoy#310,d_fy_year#311,d_fy_quarter_seq#312,d_fy_week_seq#313,d_day_name#314,d_quarter_name#315,d_holiday#316,d_weekend#317,d_following_holiday#318,d_first_dom#319,d_last_dom#320,d_same_day_ly#321,d_same_day_lq#322,d_current_day#323,... 4 more fields] parquet

== Optimized Logical Plan ==
Aggregate [sum(sales#0) AS sum(sales)#351]
+- Union false, false
   :- Project [(cast(cs_quantity#130 as double) * cs_list_price#132) AS sales#0]
   :  +- Join Inner, (cs_sold_date_sk#112 = d_date_sk#272)
   :     :- Project [cs_sold_date_sk#112, cs_quantity#130, cs_list_price#132]
   :     :  +- Join LeftSemi, (cs_bill_customer_sk#115 = c_customer_sk#254)
   :     :     :- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_quantity#130, cs_list_price#132]
   :     :     :  +- Join LeftSemi, (cs_item_sk#127 = item_sk#7)
   :     :     :     :- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_item_sk#127, cs_quantity#130, cs_list_price#132]
   :     :     :     :  +- Filter isnotnull(cs_sold_date_sk#112)
   :     :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#112,cs_sold_time_sk#113,cs_ship_date_sk#114,cs_bill_customer_sk#115,cs_bill_cdemo_sk#116,cs_bill_hdemo_sk#117,cs_bill_addr_sk#118,cs_ship_customer_sk#119,cs_ship_cdemo_sk#120,cs_ship_hdemo_sk#121,cs_ship_addr_sk#122,cs_call_center_sk#123,cs_catalog_page_sk#124,cs_ship_mode_sk#125,cs_warehouse_sk#126,cs_item_sk#127,cs_promo_sk#128,cs_order_number#129,cs_quantity#130,cs_wholesale_cost#131,cs_list_price#132,cs_sales_price#133,cs_ext_discount_amt#134,cs_ext_sales_price#135,... 10 more fields] parquet
   :     :     :     +- Project [item_sk#7]
   :     :     :        +- Filter (cnt#9L > 4)
   :     :     :           +- Aggregate [_groupingexpression#540, i_item_sk#72, d_date#46], [i_item_sk#72 AS item_sk#7, count(1) AS cnt#9L]
   :     :     :              +- Project [d_date#46, i_item_sk#72, substr(i_item_desc#76, 1, 30) AS _groupingexpression#540]
   :     :     :                 +- Join Inner, (ss_item_sk#23 = i_item_sk#72)
   :     :     :                    :- Project [ss_item_sk#23, d_date#46]
   :     :     :                    :  +- Join Inner, (ss_sold_date_sk#21 = d_date_sk#44)
   :     :     :                    :     :- Project [ss_sold_date_sk#21, ss_item_sk#23]
   :     :     :                    :     :  +- Filter (isnotnull(ss_sold_date_sk#21) AND isnotnull(ss_item_sk#23))
   :     :     :                    :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
   :     :     :                    :     +- Project [d_date_sk#44, d_date#46]
   :     :     :                    :        +- Filter (d_year#50 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#44))
   :     :     :                    :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
   :     :     :                    +- Project [i_item_sk#72, i_item_desc#76]
   :     :     :                       +- Filter isnotnull(i_item_sk#72)
   :     :     :                          +- Relation spark_catalog.tpcds.item[i_item_sk#72,i_item_id#73,i_rec_start_date#74,i_rec_end_date#75,i_item_desc#76,i_current_price#77,i_wholesale_cost#78,i_brand_id#79,i_brand#80,i_class_id#81,i_class#82,i_category_id#83,i_category#84,i_manufact_id#85,i_manufact#86,i_size#87,i_formulation#88,i_color#89,i_units#90,i_container#91,i_manager_id#92,i_product_name#93] parquet
   :     :     +- Project [c_customer_sk#254]
   :     :        +- Filter (isnotnull(ssales#12) AND (ssales#12 > (0.5 * scalar-subquery#13 [])))
   :     :           :  +- Aggregate [max(csales#10) AS tpcds_cmax#11]
   :     :           :     +- Aggregate [c_customer_sk#94], [sum((cast(ss_quantity#190 as double) * ss_sales_price#193)) AS csales#10]
   :     :           :        +- Project [ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
   :     :           :           +- Join Inner, (ss_sold_date_sk#180 = d_date_sk#203)
   :     :           :              :- Project [ss_sold_date_sk#180, ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
   :     :           :              :  +- Join Inner, (ss_customer_sk#183 = c_customer_sk#94)
   :     :           :              :     :- Project [ss_sold_date_sk#180, ss_customer_sk#183, ss_quantity#190, ss_sales_price#193]
   :     :           :              :     :  +- Filter (isnotnull(ss_customer_sk#183) AND isnotnull(ss_sold_date_sk#180))
   :     :           :              :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#180,ss_sold_time_sk#181,ss_item_sk#182,ss_customer_sk#183,ss_cdemo_sk#184,ss_hdemo_sk#185,ss_addr_sk#186,ss_store_sk#187,ss_promo_sk#188,ss_ticket_number#189,ss_quantity#190,ss_wholesale_cost#191,ss_list_price#192,ss_sales_price#193,ss_ext_discount_amt#194,ss_ext_sales_price#195,ss_ext_wholesale_cost#196,ss_ext_list_price#197,ss_ext_tax#198,ss_coupon_amt#199,ss_net_paid#200,ss_net_paid_inc_tax#201,ss_net_profit#202] parquet
   :     :           :              :     +- Project [c_customer_sk#94]
   :     :           :              :        +- Filter isnotnull(c_customer_sk#94)
   :     :           :              :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
   :     :           :              +- Project [d_date_sk#203]
   :     :           :                 +- Filter (d_year#209 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#203))
   :     :           :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#203,d_date_id#204,d_date#205,d_month_seq#206,d_week_seq#207,d_quarter_seq#208,d_year#209,d_dow#210,d_moy#211,d_dom#212,d_qoy#213,d_fy_year#214,d_fy_quarter_seq#215,d_fy_week_seq#216,d_day_name#217,d_quarter_name#218,d_holiday#219,d_weekend#220,d_following_holiday#221,d_first_dom#222,d_last_dom#223,d_same_day_ly#224,d_same_day_lq#225,d_current_day#226,... 4 more fields] parquet
   :     :           +- Aggregate [c_customer_sk#254], [c_customer_sk#254, sum((cast(ss_quantity#241 as double) * ss_sales_price#244)) AS ssales#12]
   :     :              +- Project [ss_quantity#241, ss_sales_price#244, c_customer_sk#254]
   :     :                 +- Join Inner, (ss_customer_sk#234 = c_customer_sk#254)
   :     :                    :- Project [ss_customer_sk#234, ss_quantity#241, ss_sales_price#244]
   :     :                    :  +- Filter isnotnull(ss_customer_sk#234)
   :     :                    :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#231,ss_sold_time_sk#232,ss_item_sk#233,ss_customer_sk#234,ss_cdemo_sk#235,ss_hdemo_sk#236,ss_addr_sk#237,ss_store_sk#238,ss_promo_sk#239,ss_ticket_number#240,ss_quantity#241,ss_wholesale_cost#242,ss_list_price#243,ss_sales_price#244,ss_ext_discount_amt#245,ss_ext_sales_price#246,ss_ext_wholesale_cost#247,ss_ext_list_price#248,ss_ext_tax#249,ss_coupon_amt#250,ss_net_paid#251,ss_net_paid_inc_tax#252,ss_net_profit#253] parquet
   :     :                    +- Project [c_customer_sk#254]
   :     :                       +- Filter isnotnull(c_customer_sk#254)
   :     :                          +- Relation spark_catalog.tpcds.customer[c_customer_sk#254,c_customer_id#255,c_current_cdemo_sk#256,c_current_hdemo_sk#257,c_current_addr_sk#258,c_first_shipto_date_sk#259,c_first_sales_date_sk#260,c_salutation#261,c_first_name#262,c_last_name#263,c_preferred_cust_flag#264,c_birth_day#265,c_birth_month#266,c_birth_year#267,c_birth_country#268,c_login#269,c_email_address#270,c_last_review_date#271] parquet
   :     +- Project [d_date_sk#272]
   :        +- Filter (((isnotnull(d_year#278) AND isnotnull(d_moy#280)) AND ((d_year#278 = 2000) AND (d_moy#280 = 2))) AND isnotnull(d_date_sk#272))
   :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#272,d_date_id#273,d_date#274,d_month_seq#275,d_week_seq#276,d_quarter_seq#277,d_year#278,d_dow#279,d_moy#280,d_dom#281,d_qoy#282,d_fy_year#283,d_fy_quarter_seq#284,d_fy_week_seq#285,d_day_name#286,d_quarter_name#287,d_holiday#288,d_weekend#289,d_following_holiday#290,d_first_dom#291,d_last_dom#292,d_same_day_ly#293,d_same_day_lq#294,d_current_day#295,... 4 more fields] parquet
   +- Project [(cast(ws_quantity#164 as double) * ws_list_price#166) AS sales#3]
      +- Join Inner, (ws_sold_date_sk#146 = d_date_sk#300)
         :- Project [ws_sold_date_sk#146, ws_quantity#164, ws_list_price#166]
         :  +- Join LeftSemi, (ws_bill_customer_sk#150 = c_customer_sk#641)
         :     :- Project [ws_sold_date_sk#146, ws_bill_customer_sk#150, ws_quantity#164, ws_list_price#166]
         :     :  +- Join LeftSemi, (ws_item_sk#149 = item_sk#615)
         :     :     :- Project [ws_sold_date_sk#146, ws_item_sk#149, ws_bill_customer_sk#150, ws_quantity#164, ws_list_price#166]
         :     :     :  +- Filter isnotnull(ws_sold_date_sk#146)
         :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#146,ws_sold_time_sk#147,ws_ship_date_sk#148,ws_item_sk#149,ws_bill_customer_sk#150,ws_bill_cdemo_sk#151,ws_bill_hdemo_sk#152,ws_bill_addr_sk#153,ws_ship_customer_sk#154,ws_ship_cdemo_sk#155,ws_ship_hdemo_sk#156,ws_ship_addr_sk#157,ws_web_page_sk#158,ws_web_site_sk#159,ws_ship_mode_sk#160,ws_warehouse_sk#161,ws_promo_sk#162,ws_order_number#163,ws_quantity#164,ws_wholesale_cost#165,ws_list_price#166,ws_sales_price#167,ws_ext_discount_amt#168,ws_ext_sales_price#169,... 10 more fields] parquet
         :     :     +- Project [item_sk#615]
         :     :        +- Filter (cnt#617L > 4)
         :     :           +- Aggregate [_groupingexpression#540, i_item_sk#592, d_date#566], [i_item_sk#592 AS item_sk#615, count(1) AS cnt#617L]
         :     :              +- Project [d_date#566, i_item_sk#592, substr(i_item_desc#596, 1, 30) AS _groupingexpression#540]
         :     :                 +- Join Inner, (ss_item_sk#543 = i_item_sk#592)
         :     :                    :- Project [ss_item_sk#543, d_date#566]
         :     :                    :  +- Join Inner, (ss_sold_date_sk#541 = d_date_sk#564)
         :     :                    :     :- Project [ss_sold_date_sk#541, ss_item_sk#543]
         :     :                    :     :  +- Filter (isnotnull(ss_sold_date_sk#541) AND isnotnull(ss_item_sk#543))
         :     :                    :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#541,ss_sold_time_sk#542,ss_item_sk#543,ss_customer_sk#544,ss_cdemo_sk#545,ss_hdemo_sk#546,ss_addr_sk#547,ss_store_sk#548,ss_promo_sk#549,ss_ticket_number#550,ss_quantity#551,ss_wholesale_cost#552,ss_list_price#553,ss_sales_price#554,ss_ext_discount_amt#555,ss_ext_sales_price#556,ss_ext_wholesale_cost#557,ss_ext_list_price#558,ss_ext_tax#559,ss_coupon_amt#560,ss_net_paid#561,ss_net_paid_inc_tax#562,ss_net_profit#563] parquet
         :     :                    :     +- Project [d_date_sk#564, d_date#566]
         :     :                    :        +- Filter (d_year#570 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#564))
         :     :                    :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#564,d_date_id#565,d_date#566,d_month_seq#567,d_week_seq#568,d_quarter_seq#569,d_year#570,d_dow#571,d_moy#572,d_dom#573,d_qoy#574,d_fy_year#575,d_fy_quarter_seq#576,d_fy_week_seq#577,d_day_name#578,d_quarter_name#579,d_holiday#580,d_weekend#581,d_following_holiday#582,d_first_dom#583,d_last_dom#584,d_same_day_ly#585,d_same_day_lq#586,d_current_day#587,... 4 more fields] parquet
         :     :                    +- Project [i_item_sk#592, i_item_desc#596]
         :     :                       +- Filter isnotnull(i_item_sk#592)
         :     :                          +- Relation spark_catalog.tpcds.item[i_item_sk#592,i_item_id#593,i_rec_start_date#594,i_rec_end_date#595,i_item_desc#596,i_current_price#597,i_wholesale_cost#598,i_brand_id#599,i_brand#600,i_class_id#601,i_class#602,i_category_id#603,i_category#604,i_manufact_id#605,i_manufact#606,i_size#607,i_formulation#608,i_color#609,i_units#610,i_container#611,i_manager_id#612,i_product_name#613] parquet
         :     +- Project [c_customer_sk#641]
         :        +- Filter (isnotnull(ssales#728) AND (ssales#728 > (0.5 * scalar-subquery#13 [])))
         :           :  +- Aggregate [max(csales#10) AS tpcds_cmax#11]
         :           :     +- Aggregate [c_customer_sk#94], [sum((cast(ss_quantity#190 as double) * ss_sales_price#193)) AS csales#10]
         :           :        +- Project [ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
         :           :           +- Join Inner, (ss_sold_date_sk#180 = d_date_sk#203)
         :           :              :- Project [ss_sold_date_sk#180, ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
         :           :              :  +- Join Inner, (ss_customer_sk#183 = c_customer_sk#94)
         :           :              :     :- Project [ss_sold_date_sk#180, ss_customer_sk#183, ss_quantity#190, ss_sales_price#193]
         :           :              :     :  +- Filter (isnotnull(ss_customer_sk#183) AND isnotnull(ss_sold_date_sk#180))
         :           :              :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#180,ss_sold_time_sk#181,ss_item_sk#182,ss_customer_sk#183,ss_cdemo_sk#184,ss_hdemo_sk#185,ss_addr_sk#186,ss_store_sk#187,ss_promo_sk#188,ss_ticket_number#189,ss_quantity#190,ss_wholesale_cost#191,ss_list_price#192,ss_sales_price#193,ss_ext_discount_amt#194,ss_ext_sales_price#195,ss_ext_wholesale_cost#196,ss_ext_list_price#197,ss_ext_tax#198,ss_coupon_amt#199,ss_net_paid#200,ss_net_paid_inc_tax#201,ss_net_profit#202] parquet
         :           :              :     +- Project [c_customer_sk#94]
         :           :              :        +- Filter isnotnull(c_customer_sk#94)
         :           :              :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
         :           :              +- Project [d_date_sk#203]
         :           :                 +- Filter (d_year#209 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#203))
         :           :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#203,d_date_id#204,d_date#205,d_month_seq#206,d_week_seq#207,d_quarter_seq#208,d_year#209,d_dow#210,d_moy#211,d_dom#212,d_qoy#213,d_fy_year#214,d_fy_quarter_seq#215,d_fy_week_seq#216,d_day_name#217,d_quarter_name#218,d_holiday#219,d_weekend#220,d_following_holiday#221,d_first_dom#222,d_last_dom#223,d_same_day_ly#224,d_same_day_lq#225,d_current_day#226,... 4 more fields] parquet
         :           +- Aggregate [c_customer_sk#641], [c_customer_sk#641, sum((cast(ss_quantity#628 as double) * ss_sales_price#631)) AS ssales#728]
         :              +- Project [ss_quantity#628, ss_sales_price#631, c_customer_sk#641]
         :                 +- Join Inner, (ss_customer_sk#621 = c_customer_sk#641)
         :                    :- Project [ss_customer_sk#621, ss_quantity#628, ss_sales_price#631]
         :                    :  +- Filter isnotnull(ss_customer_sk#621)
         :                    :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#618,ss_sold_time_sk#619,ss_item_sk#620,ss_customer_sk#621,ss_cdemo_sk#622,ss_hdemo_sk#623,ss_addr_sk#624,ss_store_sk#625,ss_promo_sk#626,ss_ticket_number#627,ss_quantity#628,ss_wholesale_cost#629,ss_list_price#630,ss_sales_price#631,ss_ext_discount_amt#632,ss_ext_sales_price#633,ss_ext_wholesale_cost#634,ss_ext_list_price#635,ss_ext_tax#636,ss_coupon_amt#637,ss_net_paid#638,ss_net_paid_inc_tax#639,ss_net_profit#640] parquet
         :                    +- Project [c_customer_sk#641]
         :                       +- Filter isnotnull(c_customer_sk#641)
         :                          +- Relation spark_catalog.tpcds.customer[c_customer_sk#641,c_customer_id#642,c_current_cdemo_sk#643,c_current_hdemo_sk#644,c_current_addr_sk#645,c_first_shipto_date_sk#646,c_first_sales_date_sk#647,c_salutation#648,c_first_name#649,c_last_name#650,c_preferred_cust_flag#651,c_birth_day#652,c_birth_month#653,c_birth_year#654,c_birth_country#655,c_login#656,c_email_address#657,c_last_review_date#658] parquet
         +- Project [d_date_sk#300]
            +- Filter (((isnotnull(d_year#306) AND isnotnull(d_moy#308)) AND ((d_year#306 = 2000) AND (d_moy#308 = 2))) AND isnotnull(d_date_sk#300))
               +- Relation spark_catalog.tpcds.date_dim[d_date_sk#300,d_date_id#301,d_date#302,d_month_seq#303,d_week_seq#304,d_quarter_seq#305,d_year#306,d_dow#307,d_moy#308,d_dom#309,d_qoy#310,d_fy_year#311,d_fy_quarter_seq#312,d_fy_week_seq#313,d_day_name#314,d_quarter_name#315,d_holiday#316,d_weekend#317,d_following_holiday#318,d_first_dom#319,d_last_dom#320,d_same_day_ly#321,d_same_day_lq#322,d_current_day#323,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(sales#0)], output=[sum(sales)#351])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=490]
      +- HashAggregate(keys=[], functions=[partial_sum(sales#0)], output=[sum#730])
         +- Union
            :- Project [(cast(cs_quantity#130 as double) * cs_list_price#132) AS sales#0]
            :  +- BroadcastHashJoin [cs_sold_date_sk#112], [d_date_sk#272], Inner, BuildRight, false
            :     :- Project [cs_sold_date_sk#112, cs_quantity#130, cs_list_price#132]
            :     :  +- SortMergeJoin [cs_bill_customer_sk#115], [c_customer_sk#254], LeftSemi
            :     :     :- Sort [cs_bill_customer_sk#115 ASC NULLS FIRST], false, 0
            :     :     :  +- Exchange hashpartitioning(cs_bill_customer_sk#115, 200), ENSURE_REQUIREMENTS, [plan_id=437]
            :     :     :     +- Project [cs_sold_date_sk#112, cs_bill_customer_sk#115, cs_quantity#130, cs_list_price#132]
            :     :     :        +- SortMergeJoin [cs_item_sk#127], [item_sk#7], LeftSemi
            :     :     :           :- Sort [cs_item_sk#127 ASC NULLS FIRST], false, 0
            :     :     :           :  +- Exchange hashpartitioning(cs_item_sk#127, 200), ENSURE_REQUIREMENTS, [plan_id=420]
            :     :     :           :     +- Filter isnotnull(cs_sold_date_sk#112)
            :     :     :           :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#112,cs_bill_customer_sk#115,cs_item_sk#127,cs_quantity#130,cs_list_price#132] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#112)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_quantity:int,cs_list_price:d...
            :     :     :           +- Sort [item_sk#7 ASC NULLS FIRST], false, 0
            :     :     :              +- Exchange hashpartitioning(item_sk#7, 200), ENSURE_REQUIREMENTS, [plan_id=421]
            :     :     :                 +- Project [item_sk#7]
            :     :     :                    +- Filter (cnt#9L > 4)
            :     :     :                       +- HashAggregate(keys=[_groupingexpression#540, i_item_sk#72, d_date#46], functions=[count(1)], output=[item_sk#7, cnt#9L])
            :     :     :                          +- Exchange hashpartitioning(_groupingexpression#540, i_item_sk#72, d_date#46, 200), ENSURE_REQUIREMENTS, [plan_id=414]
            :     :     :                             +- HashAggregate(keys=[_groupingexpression#540, i_item_sk#72, d_date#46], functions=[partial_count(1)], output=[_groupingexpression#540, i_item_sk#72, d_date#46, count#732L])
            :     :     :                                +- Project [d_date#46, i_item_sk#72, substr(i_item_desc#76, 1, 30) AS _groupingexpression#540]
            :     :     :                                   +- BroadcastHashJoin [ss_item_sk#23], [i_item_sk#72], Inner, BuildRight, false
            :     :     :                                      :- Project [ss_item_sk#23, d_date#46]
            :     :     :                                      :  +- BroadcastHashJoin [ss_sold_date_sk#21], [d_date_sk#44], Inner, BuildRight, false
            :     :     :                                      :     :- Filter (isnotnull(ss_sold_date_sk#21) AND isnotnull(ss_item_sk#23))
            :     :     :                                      :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_item_sk#23] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#21), isnotnull(ss_item_sk#23)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
            :     :     :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=405]
            :     :     :                                      :        +- Project [d_date_sk#44, d_date#46]
            :     :     :                                      :           +- Filter (d_year#50 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#44))
            :     :     :                                      :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#44,d_date#46,d_year#50] Batched: true, DataFilters: [d_year#50 IN (2000,2001,2002,2003), isnotnull(d_date_sk#44)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_year, [2000,2001,2002,2003]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string,d_year:int>
            :     :     :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=409]
            :     :     :                                         +- Filter isnotnull(i_item_sk#72)
            :     :     :                                            +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#72,i_item_desc#76] Batched: true, DataFilters: [isnotnull(i_item_sk#72)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string>
            :     :     +- Sort [c_customer_sk#254 ASC NULLS FIRST], false, 0
            :     :        +- Project [c_customer_sk#254]
            :     :           +- Filter (isnotnull(ssales#12) AND (ssales#12 > (0.5 * Subquery subquery#13, [id=#344])))
            :     :              :  +- Subquery subquery#13, [id=#344]
            :     :              :     +- AdaptiveSparkPlan isFinalPlan=false
            :     :              :        +- HashAggregate(keys=[], functions=[max(csales#10)], output=[tpcds_cmax#11])
            :     :              :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=342]
            :     :              :              +- HashAggregate(keys=[], functions=[partial_max(csales#10)], output=[max#738])
            :     :              :                 +- HashAggregate(keys=[c_customer_sk#94], functions=[sum((cast(ss_quantity#190 as double) * ss_sales_price#193))], output=[csales#10])
            :     :              :                    +- Exchange hashpartitioning(c_customer_sk#94, 200), ENSURE_REQUIREMENTS, [plan_id=338]
            :     :              :                       +- HashAggregate(keys=[c_customer_sk#94], functions=[partial_sum((cast(ss_quantity#190 as double) * ss_sales_price#193))], output=[c_customer_sk#94, sum#740])
            :     :              :                          +- Project [ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
            :     :              :                             +- BroadcastHashJoin [ss_sold_date_sk#180], [d_date_sk#203], Inner, BuildRight, false
            :     :              :                                :- Project [ss_sold_date_sk#180, ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
            :     :              :                                :  +- BroadcastHashJoin [ss_customer_sk#183], [c_customer_sk#94], Inner, BuildRight, false
            :     :              :                                :     :- Filter (isnotnull(ss_customer_sk#183) AND isnotnull(ss_sold_date_sk#180))
            :     :              :                                :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#180,ss_customer_sk#183,ss_quantity#190,ss_sales_price#193] Batched: true, DataFilters: [isnotnull(ss_customer_sk#183), isnotnull(ss_sold_date_sk#180)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_quantity:int,ss_sales_price:double>
            :     :              :                                :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=329]
            :     :              :                                :        +- Filter isnotnull(c_customer_sk#94)
            :     :              :                                :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#94] Batched: true, DataFilters: [isnotnull(c_customer_sk#94)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int>
            :     :              :                                +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=333]
            :     :              :                                   +- Project [d_date_sk#203]
            :     :              :                                      +- Filter (d_year#209 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#203))
            :     :              :                                         +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#203,d_year#209] Batched: true, DataFilters: [d_year#209 IN (2000,2001,2002,2003), isnotnull(d_date_sk#203)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_year, [2000,2001,2002,2003]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
            :     :              +- HashAggregate(keys=[c_customer_sk#254], functions=[sum((cast(ss_quantity#241 as double) * ss_sales_price#244))], output=[c_customer_sk#254, ssales#12])
            :     :                 +- Exchange hashpartitioning(c_customer_sk#254, 200), ENSURE_REQUIREMENTS, [plan_id=431]
            :     :                    +- HashAggregate(keys=[c_customer_sk#254], functions=[partial_sum((cast(ss_quantity#241 as double) * ss_sales_price#244))], output=[c_customer_sk#254, sum#734])
            :     :                       +- Project [ss_quantity#241, ss_sales_price#244, c_customer_sk#254]
            :     :                          +- BroadcastHashJoin [ss_customer_sk#234], [c_customer_sk#254], Inner, BuildRight, false
            :     :                             :- Filter isnotnull(ss_customer_sk#234)
            :     :                             :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_customer_sk#234,ss_quantity#241,ss_sales_price#244] Batched: true, DataFilters: [isnotnull(ss_customer_sk#234)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_customer_sk:int,ss_quantity:int,ss_sales_price:double>
            :     :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=426]
            :     :                                +- Filter isnotnull(c_customer_sk#254)
            :     :                                   +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#254] Batched: true, DataFilters: [isnotnull(c_customer_sk#254)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int>
            :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=443]
            :        +- Project [d_date_sk#272]
            :           +- Filter ((((isnotnull(d_year#278) AND isnotnull(d_moy#280)) AND (d_year#278 = 2000)) AND (d_moy#280 = 2)) AND isnotnull(d_date_sk#272))
            :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#272,d_year#278,d_moy#280] Batched: true, DataFilters: [isnotnull(d_year#278), isnotnull(d_moy#280), (d_year#278 = 2000), (d_moy#280 = 2), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2000), EqualTo(d_moy,2), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
            +- Project [(cast(ws_quantity#164 as double) * ws_list_price#166) AS sales#3]
               +- BroadcastHashJoin [ws_sold_date_sk#146], [d_date_sk#300], Inner, BuildRight, false
                  :- Project [ws_sold_date_sk#146, ws_quantity#164, ws_list_price#166]
                  :  +- SortMergeJoin [ws_bill_customer_sk#150], [c_customer_sk#641], LeftSemi
                  :     :- Sort [ws_bill_customer_sk#150 ASC NULLS FIRST], false, 0
                  :     :  +- Exchange hashpartitioning(ws_bill_customer_sk#150, 200), ENSURE_REQUIREMENTS, [plan_id=478]
                  :     :     +- Project [ws_sold_date_sk#146, ws_bill_customer_sk#150, ws_quantity#164, ws_list_price#166]
                  :     :        +- SortMergeJoin [ws_item_sk#149], [item_sk#615], LeftSemi
                  :     :           :- Sort [ws_item_sk#149 ASC NULLS FIRST], false, 0
                  :     :           :  +- Exchange hashpartitioning(ws_item_sk#149, 200), ENSURE_REQUIREMENTS, [plan_id=461]
                  :     :           :     +- Filter isnotnull(ws_sold_date_sk#146)
                  :     :           :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#146,ws_item_sk#149,ws_bill_customer_sk#150,ws_quantity#164,ws_list_price#166] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#146)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int,ws_quantity:int,ws_list_price:d...
                  :     :           +- Sort [item_sk#615 ASC NULLS FIRST], false, 0
                  :     :              +- Exchange hashpartitioning(item_sk#615, 200), ENSURE_REQUIREMENTS, [plan_id=462]
                  :     :                 +- Project [item_sk#615]
                  :     :                    +- Filter (cnt#617L > 4)
                  :     :                       +- HashAggregate(keys=[_groupingexpression#540, i_item_sk#592, d_date#566], functions=[count(1)], output=[item_sk#615, cnt#617L])
                  :     :                          +- Exchange hashpartitioning(_groupingexpression#540, i_item_sk#592, d_date#566, 200), ENSURE_REQUIREMENTS, [plan_id=455]
                  :     :                             +- HashAggregate(keys=[_groupingexpression#540, i_item_sk#592, d_date#566], functions=[partial_count(1)], output=[_groupingexpression#540, i_item_sk#592, d_date#566, count#732L])
                  :     :                                +- Project [d_date#566, i_item_sk#592, substr(i_item_desc#596, 1, 30) AS _groupingexpression#540]
                  :     :                                   +- BroadcastHashJoin [ss_item_sk#543], [i_item_sk#592], Inner, BuildRight, false
                  :     :                                      :- Project [ss_item_sk#543, d_date#566]
                  :     :                                      :  +- BroadcastHashJoin [ss_sold_date_sk#541], [d_date_sk#564], Inner, BuildRight, false
                  :     :                                      :     :- Filter (isnotnull(ss_sold_date_sk#541) AND isnotnull(ss_item_sk#543))
                  :     :                                      :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#541,ss_item_sk#543] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#541), isnotnull(ss_item_sk#543)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
                  :     :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=446]
                  :     :                                      :        +- Project [d_date_sk#564, d_date#566]
                  :     :                                      :           +- Filter (d_year#570 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#564))
                  :     :                                      :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#564,d_date#566,d_year#570] Batched: true, DataFilters: [d_year#570 IN (2000,2001,2002,2003), isnotnull(d_date_sk#564)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_year, [2000,2001,2002,2003]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string,d_year:int>
                  :     :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=450]
                  :     :                                         +- Filter isnotnull(i_item_sk#592)
                  :     :                                            +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#592,i_item_desc#596] Batched: true, DataFilters: [isnotnull(i_item_sk#592)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string>
                  :     +- Sort [c_customer_sk#641 ASC NULLS FIRST], false, 0
                  :        +- Project [c_customer_sk#641]
                  :           +- Filter (isnotnull(ssales#728) AND (ssales#728 > (0.5 * Subquery subquery#13, [id=#351])))
                  :              :  +- Subquery subquery#13, [id=#351]
                  :              :     +- AdaptiveSparkPlan isFinalPlan=false
                  :              :        +- HashAggregate(keys=[], functions=[max(csales#10)], output=[tpcds_cmax#11])
                  :              :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=342]
                  :              :              +- HashAggregate(keys=[], functions=[partial_max(csales#10)], output=[max#738])
                  :              :                 +- HashAggregate(keys=[c_customer_sk#94], functions=[sum((cast(ss_quantity#190 as double) * ss_sales_price#193))], output=[csales#10])
                  :              :                    +- Exchange hashpartitioning(c_customer_sk#94, 200), ENSURE_REQUIREMENTS, [plan_id=338]
                  :              :                       +- HashAggregate(keys=[c_customer_sk#94], functions=[partial_sum((cast(ss_quantity#190 as double) * ss_sales_price#193))], output=[c_customer_sk#94, sum#740])
                  :              :                          +- Project [ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
                  :              :                             +- BroadcastHashJoin [ss_sold_date_sk#180], [d_date_sk#203], Inner, BuildRight, false
                  :              :                                :- Project [ss_sold_date_sk#180, ss_quantity#190, ss_sales_price#193, c_customer_sk#94]
                  :              :                                :  +- BroadcastHashJoin [ss_customer_sk#183], [c_customer_sk#94], Inner, BuildRight, false
                  :              :                                :     :- Filter (isnotnull(ss_customer_sk#183) AND isnotnull(ss_sold_date_sk#180))
                  :              :                                :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#180,ss_customer_sk#183,ss_quantity#190,ss_sales_price#193] Batched: true, DataFilters: [isnotnull(ss_customer_sk#183), isnotnull(ss_sold_date_sk#180)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_quantity:int,ss_sales_price:double>
                  :              :                                :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=329]
                  :              :                                :        +- Filter isnotnull(c_customer_sk#94)
                  :              :                                :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#94] Batched: true, DataFilters: [isnotnull(c_customer_sk#94)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int>
                  :              :                                +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=333]
                  :              :                                   +- Project [d_date_sk#203]
                  :              :                                      +- Filter (d_year#209 IN (2000,2001,2002,2003) AND isnotnull(d_date_sk#203))
                  :              :                                         +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#203,d_year#209] Batched: true, DataFilters: [d_year#209 IN (2000,2001,2002,2003), isnotnull(d_date_sk#203)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_year, [2000,2001,2002,2003]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                  :              +- HashAggregate(keys=[c_customer_sk#641], functions=[sum((cast(ss_quantity#628 as double) * ss_sales_price#631))], output=[c_customer_sk#641, ssales#728])
                  :                 +- Exchange hashpartitioning(c_customer_sk#641, 200), ENSURE_REQUIREMENTS, [plan_id=472]
                  :                    +- HashAggregate(keys=[c_customer_sk#641], functions=[partial_sum((cast(ss_quantity#628 as double) * ss_sales_price#631))], output=[c_customer_sk#641, sum#736])
                  :                       +- Project [ss_quantity#628, ss_sales_price#631, c_customer_sk#641]
                  :                          +- BroadcastHashJoin [ss_customer_sk#621], [c_customer_sk#641], Inner, BuildRight, false
                  :                             :- Filter isnotnull(ss_customer_sk#621)
                  :                             :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_customer_sk#621,ss_quantity#628,ss_sales_price#631] Batched: true, DataFilters: [isnotnull(ss_customer_sk#621)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_customer_sk:int,ss_quantity:int,ss_sales_price:double>
                  :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=467]
                  :                                +- Filter isnotnull(c_customer_sk#641)
                  :                                   +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#641] Batched: true, DataFilters: [isnotnull(c_customer_sk#641)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=484]
                     +- Project [d_date_sk#300]
                        +- Filter ((((isnotnull(d_year#306) AND isnotnull(d_moy#308)) AND (d_year#306 = 2000)) AND (d_moy#308 = 2)) AND isnotnull(d_date_sk#300))
                           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#300,d_year#306,d_moy#308] Batched: true, DataFilters: [isnotnull(d_year#306), isnotnull(d_moy#308), (d_year#306 = 2000), (d_moy#308 = 2), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2000), EqualTo(d_moy,2), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>

Time taken: 4.118 seconds, Fetched 1 row(s)
NULL	Robert	598.86
Brown	Monika	6031.52
Collins	Gordon	727.5699999999999
Green	Jesse	9672.960000000001
Time taken: 24.129 seconds, Fetched 4 row(s)
