Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581634265
== Parsed Logical Plan ==
CTE [my_customers, my_revenue, segments]
:  :- 'SubqueryAlias my_customers
:  :  +- 'Distinct
:  :     +- 'Project ['c_customer_sk, 'c_current_addr_sk]
:  :        +- 'Filter (((('sold_date_sk = 'd_date_sk) AND ('item_sk = 'i_item_sk)) AND (('i_category = Women) AND ('i_class = maternity))) AND ((('c_customer_sk = 'cs_or_ws_sales.customer_sk) AND ('d_moy = 12)) AND ('d_year = 1998)))
:  :           +- 'Join Inner
:  :              :- 'Join Inner
:  :              :  :- 'Join Inner
:  :              :  :  :- 'SubqueryAlias cs_or_ws_sales
:  :              :  :  :  +- 'Union false, false
:  :              :  :  :     :- 'Project ['cs_sold_date_sk AS sold_date_sk#2, 'cs_bill_customer_sk AS customer_sk#3, 'cs_item_sk AS item_sk#4]
:  :              :  :  :     :  +- 'UnresolvedRelation [catalog_sales], [], false
:  :              :  :  :     +- 'Project ['ws_sold_date_sk AS sold_date_sk#5, 'ws_bill_customer_sk AS customer_sk#6, 'ws_item_sk AS item_sk#7]
:  :              :  :  :        +- 'UnresolvedRelation [web_sales], [], false
:  :              :  :  +- 'UnresolvedRelation [item], [], false
:  :              :  +- 'UnresolvedRelation [date_dim], [], false
:  :              +- 'UnresolvedRelation [customer], [], false
:  :- 'SubqueryAlias my_revenue
:  :  +- 'Aggregate ['c_customer_sk], ['c_customer_sk, 'sum('ss_ext_sales_price) AS revenue#8]
:  :     +- 'Filter (((('c_current_addr_sk = 'ca_address_sk) AND ('ca_county = 's_county)) AND ('ca_state = 's_state)) AND ((('ss_sold_date_sk = 'd_date_sk) AND ('c_customer_sk = 'ss_customer_sk)) AND (('d_month_seq >= scalar-subquery#9 []) AND ('d_month_seq <= scalar-subquery#10 []))))
:  :        :  :- 'Distinct
:  :        :  :  +- 'Project [unresolvedalias(('d_month_seq + 1), None)]
:  :        :  :     +- 'Filter (('d_year = 1998) AND ('d_moy = 12))
:  :        :  :        +- 'UnresolvedRelation [date_dim], [], false
:  :        :  +- 'Distinct
:  :        :     +- 'Project [unresolvedalias(('d_month_seq + 3), None)]
:  :        :        +- 'Filter (('d_year = 1998) AND ('d_moy = 12))
:  :        :           +- 'UnresolvedRelation [date_dim], [], false
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'Join Inner
:  :           :  :  :  :- 'UnresolvedRelation [my_customers], [], false
:  :           :  :  :  +- 'UnresolvedRelation [store_sales], [], false
:  :           :  :  +- 'UnresolvedRelation [customer_address], [], false
:  :           :  +- 'UnresolvedRelation [store], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias segments
:     +- 'Project [cast(('revenue / 50) as int) AS segment#11]
:        +- 'UnresolvedRelation [my_revenue], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['segment ASC NULLS FIRST, 'num_customers ASC NULLS FIRST], true
         +- 'Aggregate ['segment], ['segment, 'count(1) AS num_customers#0, ('segment * 50) AS segment_base#1]
            +- 'UnresolvedRelation [segments], [], false

== Analyzed Logical Plan ==
segment: int, num_customers: bigint, segment_base: int
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias my_customers
:     +- Distinct
:        +- Project [c_customer_sk#136, c_current_addr_sk#140]
:           +- Filter ((((sold_date_sk#2 = d_date_sk#108) AND (item_sk#4 = i_item_sk#86)) AND ((i_category#98 = Women) AND (i_class#96 = maternity))) AND (((c_customer_sk#136 = customer_sk#3) AND (d_moy#116 = 12)) AND (d_year#114 = 1998)))
:              +- Join Inner
:                 :- Join Inner
:                 :  :- Join Inner
:                 :  :  :- SubqueryAlias cs_or_ws_sales
:                 :  :  :  +- Union false, false
:                 :  :  :     :- Project [cs_sold_date_sk#18 AS sold_date_sk#2, cs_bill_customer_sk#21 AS customer_sk#3, cs_item_sk#33 AS item_sk#4]
:                 :  :  :     :  +- SubqueryAlias spark_catalog.tpcds.catalog_sales
:                 :  :  :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#18,cs_sold_time_sk#19,cs_ship_date_sk#20,cs_bill_customer_sk#21,cs_bill_cdemo_sk#22,cs_bill_hdemo_sk#23,cs_bill_addr_sk#24,cs_ship_customer_sk#25,cs_ship_cdemo_sk#26,cs_ship_hdemo_sk#27,cs_ship_addr_sk#28,cs_call_center_sk#29,cs_catalog_page_sk#30,cs_ship_mode_sk#31,cs_warehouse_sk#32,cs_item_sk#33,cs_promo_sk#34,cs_order_number#35,cs_quantity#36,cs_wholesale_cost#37,cs_list_price#38,cs_sales_price#39,cs_ext_discount_amt#40,cs_ext_sales_price#41,... 10 more fields] parquet
:                 :  :  :     +- Project [ws_sold_date_sk#52 AS sold_date_sk#5, ws_bill_customer_sk#56 AS customer_sk#6, ws_item_sk#55 AS item_sk#7]
:                 :  :  :        +- SubqueryAlias spark_catalog.tpcds.web_sales
:                 :  :  :           +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#52,ws_sold_time_sk#53,ws_ship_date_sk#54,ws_item_sk#55,ws_bill_customer_sk#56,ws_bill_cdemo_sk#57,ws_bill_hdemo_sk#58,ws_bill_addr_sk#59,ws_ship_customer_sk#60,ws_ship_cdemo_sk#61,ws_ship_hdemo_sk#62,ws_ship_addr_sk#63,ws_web_page_sk#64,ws_web_site_sk#65,ws_ship_mode_sk#66,ws_warehouse_sk#67,ws_promo_sk#68,ws_order_number#69,ws_quantity#70,ws_wholesale_cost#71,ws_list_price#72,ws_sales_price#73,ws_ext_discount_amt#74,ws_ext_sales_price#75,... 10 more fields] parquet
:                 :  :  +- SubqueryAlias spark_catalog.tpcds.item
:                 :  :     +- Relation spark_catalog.tpcds.item[i_item_sk#86,i_item_id#87,i_rec_start_date#88,i_rec_end_date#89,i_item_desc#90,i_current_price#91,i_wholesale_cost#92,i_brand_id#93,i_brand#94,i_class_id#95,i_class#96,i_category_id#97,i_category#98,i_manufact_id#99,i_manufact#100,i_size#101,i_formulation#102,i_color#103,i_units#104,i_container#105,i_manager_id#106,i_product_name#107] parquet
:                 :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#108,d_date_id#109,d_date#110,d_month_seq#111,d_week_seq#112,d_quarter_seq#113,d_year#114,d_dow#115,d_moy#116,d_dom#117,d_qoy#118,d_fy_year#119,d_fy_quarter_seq#120,d_fy_week_seq#121,d_day_name#122,d_quarter_name#123,d_holiday#124,d_weekend#125,d_following_holiday#126,d_first_dom#127,d_last_dom#128,d_same_day_ly#129,d_same_day_lq#130,d_current_day#131,... 4 more fields] parquet
:                 +- SubqueryAlias spark_catalog.tpcds.customer
:                    +- Relation spark_catalog.tpcds.customer[c_customer_sk#136,c_customer_id#137,c_current_cdemo_sk#138,c_current_hdemo_sk#139,c_current_addr_sk#140,c_first_shipto_date_sk#141,c_first_sales_date_sk#142,c_salutation#143,c_first_name#144,c_last_name#145,c_preferred_cust_flag#146,c_birth_day#147,c_birth_month#148,c_birth_year#149,c_birth_country#150,c_login#151,c_email_address#152,c_last_review_date#153] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias my_revenue
:     +- Aggregate [c_customer_sk#136], [c_customer_sk#136, sum(ss_ext_sales_price#169) AS revenue#8]
:        +- Filter ((((c_current_addr_sk#140 = ca_address_sk#177) AND (ca_county#184 = s_county#213)) AND (ca_state#185 = s_state#214)) AND (((ss_sold_date_sk#154 = d_date_sk#219) AND (c_customer_sk#136 = ss_customer_sk#157)) AND ((d_month_seq#222 >= scalar-subquery#9 []) AND (d_month_seq#222 <= scalar-subquery#10 []))))
:           :  :- Distinct
:           :  :  +- Project [(d_month_seq#261 + 1) AS (d_month_seq + 1)#252]
:           :  :     +- Filter ((d_year#264 = 1998) AND (d_moy#266 = 12))
:           :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#258,d_date_id#259,d_date#260,d_month_seq#261,d_week_seq#262,d_quarter_seq#263,d_year#264,d_dow#265,d_moy#266,d_dom#267,d_qoy#268,d_fy_year#269,d_fy_quarter_seq#270,d_fy_week_seq#271,d_day_name#272,d_quarter_name#273,d_holiday#274,d_weekend#275,d_following_holiday#276,d_first_dom#277,d_last_dom#278,d_same_day_ly#279,d_same_day_lq#280,d_current_day#281,... 4 more fields] parquet
:           :  +- Distinct
:           :     +- Project [(d_month_seq#289 + 3) AS (d_month_seq + 3)#253]
:           :        +- Filter ((d_year#292 = 1998) AND (d_moy#294 = 12))
:           :           +- SubqueryAlias spark_catalog.tpcds.date_dim
:           :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#286,d_date_id#287,d_date#288,d_month_seq#289,d_week_seq#290,d_quarter_seq#291,d_year#292,d_dow#293,d_moy#294,d_dom#295,d_qoy#296,d_fy_year#297,d_fy_quarter_seq#298,d_fy_week_seq#299,d_day_name#300,d_quarter_name#301,d_holiday#302,d_weekend#303,d_following_holiday#304,d_first_dom#305,d_last_dom#306,d_same_day_ly#307,d_same_day_lq#308,d_current_day#309,... 4 more fields] parquet
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- Join Inner
:              :  :  :  :- SubqueryAlias my_customers
:              :  :  :  :  +- CTERelationRef 0, true, [c_customer_sk#136, c_current_addr_sk#140], false
:              :  :  :  +- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#154,ss_sold_time_sk#155,ss_item_sk#156,ss_customer_sk#157,ss_cdemo_sk#158,ss_hdemo_sk#159,ss_addr_sk#160,ss_store_sk#161,ss_promo_sk#162,ss_ticket_number#163,ss_quantity#164,ss_wholesale_cost#165,ss_list_price#166,ss_sales_price#167,ss_ext_discount_amt#168,ss_ext_sales_price#169,ss_ext_wholesale_cost#170,ss_ext_list_price#171,ss_ext_tax#172,ss_coupon_amt#173,ss_net_paid#174,ss_net_paid_inc_tax#175,ss_net_profit#176] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
:              :  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#177,ca_address_id#178,ca_street_number#179,ca_street_name#180,ca_street_type#181,ca_suite_number#182,ca_city#183,ca_county#184,ca_state#185,ca_zip#186,ca_country#187,ca_gmt_offset#188,ca_location_type#189] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.store
:              :     +- Relation spark_catalog.tpcds.store[s_store_sk#190,s_store_id#191,s_rec_start_date#192,s_rec_end_date#193,s_closed_date_sk#194,s_store_name#195,s_number_employees#196,s_floor_space#197,s_hours#198,s_manager#199,s_market_id#200,s_geography_class#201,s_market_desc#202,s_market_manager#203,s_division_id#204,s_division_name#205,s_company_id#206,s_company_name#207,s_street_number#208,s_street_name#209,s_street_type#210,s_suite_number#211,s_city#212,s_county#213,... 5 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#219,d_date_id#220,d_date#221,d_month_seq#222,d_week_seq#223,d_quarter_seq#224,d_year#225,d_dow#226,d_moy#227,d_dom#228,d_qoy#229,d_fy_year#230,d_fy_quarter_seq#231,d_fy_week_seq#232,d_day_name#233,d_quarter_name#234,d_holiday#235,d_weekend#236,d_following_holiday#237,d_first_dom#238,d_last_dom#239,d_same_day_ly#240,d_same_day_lq#241,d_current_day#242,... 4 more fields] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias segments
:     +- Project [cast((revenue#8 / cast(50 as double)) as int) AS segment#11]
:        +- SubqueryAlias my_revenue
:           +- CTERelationRef 1, true, [c_customer_sk#136, revenue#8], false
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [segment#11 ASC NULLS FIRST, num_customers#0L ASC NULLS FIRST], true
         +- Aggregate [segment#11], [segment#11, count(1) AS num_customers#0L, (segment#11 * 50) AS segment_base#1]
            +- SubqueryAlias segments
               +- CTERelationRef 2, true, [segment#11], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [segment#11 ASC NULLS FIRST, num_customers#0L ASC NULLS FIRST], true
      +- Aggregate [segment#11], [segment#11, count(1) AS num_customers#0L, (segment#11 * 50) AS segment_base#1]
         +- Aggregate [c_customer_sk#136], [cast((sum(ss_ext_sales_price#169) / 50.0) as int) AS segment#11]
            +- Project [c_customer_sk#136, ss_ext_sales_price#169]
               +- Join Inner, (ss_sold_date_sk#154 = d_date_sk#219)
                  :- Project [c_customer_sk#136, ss_sold_date_sk#154, ss_ext_sales_price#169]
                  :  +- Join Inner, ((ca_county#184 = s_county#213) AND (ca_state#185 = s_state#214))
                  :     :- Project [c_customer_sk#136, ss_sold_date_sk#154, ss_ext_sales_price#169, ca_county#184, ca_state#185]
                  :     :  +- Join Inner, (c_current_addr_sk#140 = ca_address_sk#177)
                  :     :     :- Project [c_customer_sk#136, c_current_addr_sk#140, ss_sold_date_sk#154, ss_ext_sales_price#169]
                  :     :     :  +- Join Inner, (c_customer_sk#136 = ss_customer_sk#157)
                  :     :     :     :- Aggregate [c_customer_sk#136, c_current_addr_sk#140], [c_customer_sk#136, c_current_addr_sk#140]
                  :     :     :     :  +- Project [c_customer_sk#136, c_current_addr_sk#140]
                  :     :     :     :     +- Join Inner, (c_customer_sk#136 = customer_sk#3)
                  :     :     :     :        :- Project [customer_sk#3]
                  :     :     :     :        :  +- Join Inner, (sold_date_sk#2 = d_date_sk#108)
                  :     :     :     :        :     :- Project [sold_date_sk#2, customer_sk#3]
                  :     :     :     :        :     :  +- Join Inner, (item_sk#4 = i_item_sk#86)
                  :     :     :     :        :     :     :- Union false, false
                  :     :     :     :        :     :     :  :- Project [cs_sold_date_sk#18 AS sold_date_sk#2, cs_bill_customer_sk#21 AS customer_sk#3, cs_item_sk#33 AS item_sk#4]
                  :     :     :     :        :     :     :  :  +- Filter (isnotnull(cs_item_sk#33) AND (isnotnull(cs_sold_date_sk#18) AND isnotnull(cs_bill_customer_sk#21)))
                  :     :     :     :        :     :     :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#18,cs_sold_time_sk#19,cs_ship_date_sk#20,cs_bill_customer_sk#21,cs_bill_cdemo_sk#22,cs_bill_hdemo_sk#23,cs_bill_addr_sk#24,cs_ship_customer_sk#25,cs_ship_cdemo_sk#26,cs_ship_hdemo_sk#27,cs_ship_addr_sk#28,cs_call_center_sk#29,cs_catalog_page_sk#30,cs_ship_mode_sk#31,cs_warehouse_sk#32,cs_item_sk#33,cs_promo_sk#34,cs_order_number#35,cs_quantity#36,cs_wholesale_cost#37,cs_list_price#38,cs_sales_price#39,cs_ext_discount_amt#40,cs_ext_sales_price#41,... 10 more fields] parquet
                  :     :     :     :        :     :     :  +- Project [ws_sold_date_sk#52 AS sold_date_sk#5, ws_bill_customer_sk#56 AS customer_sk#6, ws_item_sk#55 AS item_sk#7]
                  :     :     :     :        :     :     :     +- Filter (isnotnull(ws_item_sk#55) AND (isnotnull(ws_sold_date_sk#52) AND isnotnull(ws_bill_customer_sk#56)))
                  :     :     :     :        :     :     :        +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#52,ws_sold_time_sk#53,ws_ship_date_sk#54,ws_item_sk#55,ws_bill_customer_sk#56,ws_bill_cdemo_sk#57,ws_bill_hdemo_sk#58,ws_bill_addr_sk#59,ws_ship_customer_sk#60,ws_ship_cdemo_sk#61,ws_ship_hdemo_sk#62,ws_ship_addr_sk#63,ws_web_page_sk#64,ws_web_site_sk#65,ws_ship_mode_sk#66,ws_warehouse_sk#67,ws_promo_sk#68,ws_order_number#69,ws_quantity#70,ws_wholesale_cost#71,ws_list_price#72,ws_sales_price#73,ws_ext_discount_amt#74,ws_ext_sales_price#75,... 10 more fields] parquet
                  :     :     :     :        :     :     +- Project [i_item_sk#86]
                  :     :     :     :        :     :        +- Filter (((isnotnull(i_category#98) AND isnotnull(i_class#96)) AND ((i_category#98 = Women) AND (i_class#96 = maternity))) AND isnotnull(i_item_sk#86))
                  :     :     :     :        :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#86,i_item_id#87,i_rec_start_date#88,i_rec_end_date#89,i_item_desc#90,i_current_price#91,i_wholesale_cost#92,i_brand_id#93,i_brand#94,i_class_id#95,i_class#96,i_category_id#97,i_category#98,i_manufact_id#99,i_manufact#100,i_size#101,i_formulation#102,i_color#103,i_units#104,i_container#105,i_manager_id#106,i_product_name#107] parquet
                  :     :     :     :        :     +- Project [d_date_sk#108]
                  :     :     :     :        :        +- Filter (((isnotnull(d_moy#116) AND isnotnull(d_year#114)) AND ((d_moy#116 = 12) AND (d_year#114 = 1998))) AND isnotnull(d_date_sk#108))
                  :     :     :     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#108,d_date_id#109,d_date#110,d_month_seq#111,d_week_seq#112,d_quarter_seq#113,d_year#114,d_dow#115,d_moy#116,d_dom#117,d_qoy#118,d_fy_year#119,d_fy_quarter_seq#120,d_fy_week_seq#121,d_day_name#122,d_quarter_name#123,d_holiday#124,d_weekend#125,d_following_holiday#126,d_first_dom#127,d_last_dom#128,d_same_day_ly#129,d_same_day_lq#130,d_current_day#131,... 4 more fields] parquet
                  :     :     :     :        +- Project [c_customer_sk#136, c_current_addr_sk#140]
                  :     :     :     :           +- Filter (isnotnull(c_customer_sk#136) AND isnotnull(c_current_addr_sk#140))
                  :     :     :     :              +- Relation spark_catalog.tpcds.customer[c_customer_sk#136,c_customer_id#137,c_current_cdemo_sk#138,c_current_hdemo_sk#139,c_current_addr_sk#140,c_first_shipto_date_sk#141,c_first_sales_date_sk#142,c_salutation#143,c_first_name#144,c_last_name#145,c_preferred_cust_flag#146,c_birth_day#147,c_birth_month#148,c_birth_year#149,c_birth_country#150,c_login#151,c_email_address#152,c_last_review_date#153] parquet
                  :     :     :     +- Project [ss_sold_date_sk#154, ss_customer_sk#157, ss_ext_sales_price#169]
                  :     :     :        +- Filter (isnotnull(ss_customer_sk#157) AND isnotnull(ss_sold_date_sk#154))
                  :     :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#154,ss_sold_time_sk#155,ss_item_sk#156,ss_customer_sk#157,ss_cdemo_sk#158,ss_hdemo_sk#159,ss_addr_sk#160,ss_store_sk#161,ss_promo_sk#162,ss_ticket_number#163,ss_quantity#164,ss_wholesale_cost#165,ss_list_price#166,ss_sales_price#167,ss_ext_discount_amt#168,ss_ext_sales_price#169,ss_ext_wholesale_cost#170,ss_ext_list_price#171,ss_ext_tax#172,ss_coupon_amt#173,ss_net_paid#174,ss_net_paid_inc_tax#175,ss_net_profit#176] parquet
                  :     :     +- Project [ca_address_sk#177, ca_county#184, ca_state#185]
                  :     :        +- Filter (isnotnull(ca_address_sk#177) AND (isnotnull(ca_county#184) AND isnotnull(ca_state#185)))
                  :     :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#177,ca_address_id#178,ca_street_number#179,ca_street_name#180,ca_street_type#181,ca_suite_number#182,ca_city#183,ca_county#184,ca_state#185,ca_zip#186,ca_country#187,ca_gmt_offset#188,ca_location_type#189] parquet
                  :     +- Project [s_county#213, s_state#214]
                  :        +- Filter (isnotnull(s_county#213) AND isnotnull(s_state#214))
                  :           +- Relation spark_catalog.tpcds.store[s_store_sk#190,s_store_id#191,s_rec_start_date#192,s_rec_end_date#193,s_closed_date_sk#194,s_store_name#195,s_number_employees#196,s_floor_space#197,s_hours#198,s_manager#199,s_market_id#200,s_geography_class#201,s_market_desc#202,s_market_manager#203,s_division_id#204,s_division_name#205,s_company_id#206,s_company_name#207,s_street_number#208,s_street_name#209,s_street_type#210,s_suite_number#211,s_city#212,s_county#213,... 5 more fields] parquet
                  +- Project [d_date_sk#219]
                     +- Filter ((isnotnull(d_month_seq#222) AND ((d_month_seq#222 >= scalar-subquery#9 []) AND (d_month_seq#222 <= scalar-subquery#10 []))) AND isnotnull(d_date_sk#219))
                        :  :- Aggregate [(d_month_seq + 1)#252], [(d_month_seq + 1)#252]
                        :  :  +- Project [(d_month_seq#261 + 1) AS (d_month_seq + 1)#252]
                        :  :     +- Filter ((isnotnull(d_year#264) AND isnotnull(d_moy#266)) AND ((d_year#264 = 1998) AND (d_moy#266 = 12)))
                        :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#258,d_date_id#259,d_date#260,d_month_seq#261,d_week_seq#262,d_quarter_seq#263,d_year#264,d_dow#265,d_moy#266,d_dom#267,d_qoy#268,d_fy_year#269,d_fy_quarter_seq#270,d_fy_week_seq#271,d_day_name#272,d_quarter_name#273,d_holiday#274,d_weekend#275,d_following_holiday#276,d_first_dom#277,d_last_dom#278,d_same_day_ly#279,d_same_day_lq#280,d_current_day#281,... 4 more fields] parquet
                        :  +- Aggregate [(d_month_seq + 3)#253], [(d_month_seq + 3)#253]
                        :     +- Project [(d_month_seq#289 + 3) AS (d_month_seq + 3)#253]
                        :        +- Filter ((isnotnull(d_year#292) AND isnotnull(d_moy#294)) AND ((d_year#292 = 1998) AND (d_moy#294 = 12)))
                        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#286,d_date_id#287,d_date#288,d_month_seq#289,d_week_seq#290,d_quarter_seq#291,d_year#292,d_dow#293,d_moy#294,d_dom#295,d_qoy#296,d_fy_year#297,d_fy_quarter_seq#298,d_fy_week_seq#299,d_day_name#300,d_quarter_name#301,d_holiday#302,d_weekend#303,d_following_holiday#304,d_first_dom#305,d_last_dom#306,d_same_day_ly#307,d_same_day_lq#308,d_current_day#309,... 4 more fields] parquet
                        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#219,d_date_id#220,d_date#221,d_month_seq#222,d_week_seq#223,d_quarter_seq#224,d_year#225,d_dow#226,d_moy#227,d_dom#228,d_qoy#229,d_fy_year#230,d_fy_quarter_seq#231,d_fy_week_seq#232,d_day_name#233,d_quarter_name#234,d_holiday#235,d_weekend#236,d_following_holiday#237,d_first_dom#238,d_last_dom#239,d_same_day_ly#240,d_same_day_lq#241,d_current_day#242,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[segment#11 ASC NULLS FIRST,num_customers#0L ASC NULLS FIRST], output=[segment#11,num_customers#0L,segment_base#1])
   +- HashAggregate(keys=[segment#11], functions=[count(1)], output=[segment#11, num_customers#0L, segment_base#1])
      +- Exchange hashpartitioning(segment#11, 200), ENSURE_REQUIREMENTS, [plan_id=253]
         +- HashAggregate(keys=[segment#11], functions=[partial_count(1)], output=[segment#11, count#316L])
            +- HashAggregate(keys=[c_customer_sk#136], functions=[sum(ss_ext_sales_price#169)], output=[segment#11])
               +- HashAggregate(keys=[c_customer_sk#136], functions=[partial_sum(ss_ext_sales_price#169)], output=[c_customer_sk#136, sum#318])
                  +- Project [c_customer_sk#136, ss_ext_sales_price#169]
                     +- BroadcastHashJoin [ss_sold_date_sk#154], [d_date_sk#219], Inner, BuildRight, false
                        :- Project [c_customer_sk#136, ss_sold_date_sk#154, ss_ext_sales_price#169]
                        :  +- BroadcastHashJoin [ca_county#184, ca_state#185], [s_county#213, s_state#214], Inner, BuildRight, false
                        :     :- Project [c_customer_sk#136, ss_sold_date_sk#154, ss_ext_sales_price#169, ca_county#184, ca_state#185]
                        :     :  +- BroadcastHashJoin [c_current_addr_sk#140], [ca_address_sk#177], Inner, BuildRight, false
                        :     :     :- Project [c_customer_sk#136, c_current_addr_sk#140, ss_sold_date_sk#154, ss_ext_sales_price#169]
                        :     :     :  +- SortMergeJoin [c_customer_sk#136], [ss_customer_sk#157], Inner
                        :     :     :     :- Sort [c_customer_sk#136 ASC NULLS FIRST], false, 0
                        :     :     :     :  +- Exchange hashpartitioning(c_customer_sk#136, 200), ENSURE_REQUIREMENTS, [plan_id=231]
                        :     :     :     :     +- HashAggregate(keys=[c_customer_sk#136, c_current_addr_sk#140], functions=[], output=[c_customer_sk#136, c_current_addr_sk#140])
                        :     :     :     :        +- Exchange hashpartitioning(c_customer_sk#136, c_current_addr_sk#140, 200), ENSURE_REQUIREMENTS, [plan_id=227]
                        :     :     :     :           +- HashAggregate(keys=[c_customer_sk#136, c_current_addr_sk#140], functions=[], output=[c_customer_sk#136, c_current_addr_sk#140])
                        :     :     :     :              +- Project [c_customer_sk#136, c_current_addr_sk#140]
                        :     :     :     :                 +- BroadcastHashJoin [customer_sk#3], [c_customer_sk#136], Inner, BuildRight, false
                        :     :     :     :                    :- Project [customer_sk#3]
                        :     :     :     :                    :  +- BroadcastHashJoin [sold_date_sk#2], [d_date_sk#108], Inner, BuildRight, false
                        :     :     :     :                    :     :- Project [sold_date_sk#2, customer_sk#3]
                        :     :     :     :                    :     :  +- BroadcastHashJoin [item_sk#4], [i_item_sk#86], Inner, BuildRight, false
                        :     :     :     :                    :     :     :- Union
                        :     :     :     :                    :     :     :  :- Project [cs_sold_date_sk#18 AS sold_date_sk#2, cs_bill_customer_sk#21 AS customer_sk#3, cs_item_sk#33 AS item_sk#4]
                        :     :     :     :                    :     :     :  :  +- Filter ((isnotnull(cs_item_sk#33) AND isnotnull(cs_sold_date_sk#18)) AND isnotnull(cs_bill_customer_sk#21))
                        :     :     :     :                    :     :     :  :     +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#18,cs_bill_customer_sk#21,cs_item_sk#33] Batched: true, DataFilters: [isnotnull(cs_item_sk#33), isnotnull(cs_sold_date_sk#18), isnotnull(cs_bill_customer_sk#21)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_bill_customer_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int>
                        :     :     :     :                    :     :     :  +- Project [ws_sold_date_sk#52 AS sold_date_sk#5, ws_bill_customer_sk#56 AS customer_sk#6, ws_item_sk#55 AS item_sk#7]
                        :     :     :     :                    :     :     :     +- Filter ((isnotnull(ws_item_sk#55) AND isnotnull(ws_sold_date_sk#52)) AND isnotnull(ws_bill_customer_sk#56))
                        :     :     :     :                    :     :     :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#52,ws_item_sk#55,ws_bill_customer_sk#56] Batched: true, DataFilters: [isnotnull(ws_item_sk#55), isnotnull(ws_sold_date_sk#52), isnotnull(ws_bill_customer_sk#56)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int>
                        :     :     :     :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=214]
                        :     :     :     :                    :     :        +- Project [i_item_sk#86]
                        :     :     :     :                    :     :           +- Filter ((((isnotnull(i_category#98) AND isnotnull(i_class#96)) AND (i_category#98 = Women)) AND (i_class#96 = maternity)) AND isnotnull(i_item_sk#86))
                        :     :     :     :                    :     :              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#86,i_class#96,i_category#98] Batched: true, DataFilters: [isnotnull(i_category#98), isnotnull(i_class#96), (i_category#98 = Women), (i_class#96 = maternit..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), IsNotNull(i_class), EqualTo(i_category,Women), EqualTo(i_class,maternity)..., ReadSchema: struct<i_item_sk:int,i_class:string,i_category:string>
                        :     :     :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=218]
                        :     :     :     :                    :        +- Project [d_date_sk#108]
                        :     :     :     :                    :           +- Filter ((((isnotnull(d_moy#116) AND isnotnull(d_year#114)) AND (d_moy#116 = 12)) AND (d_year#114 = 1998)) AND isnotnull(d_date_sk#108))
                        :     :     :     :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#108,d_year#114,d_moy#116] Batched: true, DataFilters: [isnotnull(d_moy#116), isnotnull(d_year#114), (d_moy#116 = 12), (d_year#114 = 1998), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,12), EqualTo(d_year,1998), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                        :     :     :     :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=222]
                        :     :     :     :                       +- Filter (isnotnull(c_customer_sk#136) AND isnotnull(c_current_addr_sk#140))
                        :     :     :     :                          +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#136,c_current_addr_sk#140] Batched: true, DataFilters: [isnotnull(c_customer_sk#136), isnotnull(c_current_addr_sk#140)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                        :     :     :     +- Sort [ss_customer_sk#157 ASC NULLS FIRST], false, 0
                        :     :     :        +- Exchange hashpartitioning(ss_customer_sk#157, 200), ENSURE_REQUIREMENTS, [plan_id=232]
                        :     :     :           +- Filter (isnotnull(ss_customer_sk#157) AND isnotnull(ss_sold_date_sk#154))
                        :     :     :              +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#154,ss_customer_sk#157,ss_ext_sales_price#169] Batched: true, DataFilters: [isnotnull(ss_customer_sk#157), isnotnull(ss_sold_date_sk#154)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_ext_sales_price:double>
                        :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=238]
                        :     :        +- Filter ((isnotnull(ca_address_sk#177) AND isnotnull(ca_county#184)) AND isnotnull(ca_state#185))
                        :     :           +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#177,ca_county#184,ca_state#185] Batched: true, DataFilters: [isnotnull(ca_address_sk#177), isnotnull(ca_county#184), isnotnull(ca_state#185)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county), IsNotNull(ca_state)], ReadSchema: struct<ca_address_sk:int,ca_county:string,ca_state:string>
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false], input[1, string, false]),false), [plan_id=242]
                        :        +- Filter (isnotnull(s_county#213) AND isnotnull(s_state#214))
                        :           +- FileScan parquet spark_catalog.tpcds.store[s_county#213,s_state#214] Batched: true, DataFilters: [isnotnull(s_county#213), isnotnull(s_state#214)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_county), IsNotNull(s_state)], ReadSchema: struct<s_county:string,s_state:string>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=246]
                           +- Project [d_date_sk#219]
                              +- Filter (((isnotnull(d_month_seq#222) AND (d_month_seq#222 >= Subquery subquery#9, [id=#185])) AND (d_month_seq#222 <= Subquery subquery#10, [id=#186])) AND isnotnull(d_date_sk#219))
                                 :  :- Subquery subquery#9, [id=#185]
                                 :  :  +- AdaptiveSparkPlan isFinalPlan=false
                                 :  :     +- HashAggregate(keys=[(d_month_seq + 1)#252], functions=[], output=[(d_month_seq + 1)#252])
                                 :  :        +- Exchange hashpartitioning((d_month_seq + 1)#252, 200), ENSURE_REQUIREMENTS, [plan_id=171]
                                 :  :           +- HashAggregate(keys=[(d_month_seq + 1)#252], functions=[], output=[(d_month_seq + 1)#252])
                                 :  :              +- Project [(d_month_seq#261 + 1) AS (d_month_seq + 1)#252]
                                 :  :                 +- Filter (((isnotnull(d_year#264) AND isnotnull(d_moy#266)) AND (d_year#264 = 1998)) AND (d_moy#266 = 12))
                                 :  :                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_month_seq#261,d_year#264,d_moy#266] Batched: true, DataFilters: [isnotnull(d_year#264), isnotnull(d_moy#266), (d_year#264 = 1998), (d_moy#266 = 12)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,12)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                                 :  +- Subquery subquery#10, [id=#186]
                                 :     +- AdaptiveSparkPlan isFinalPlan=false
                                 :        +- HashAggregate(keys=[(d_month_seq + 3)#253], functions=[], output=[(d_month_seq + 3)#253])
                                 :           +- Exchange hashpartitioning((d_month_seq + 3)#253, 200), ENSURE_REQUIREMENTS, [plan_id=183]
                                 :              +- HashAggregate(keys=[(d_month_seq + 3)#253], functions=[], output=[(d_month_seq + 3)#253])
                                 :                 +- Project [(d_month_seq#289 + 3) AS (d_month_seq + 3)#253]
                                 :                    +- Filter (((isnotnull(d_year#292) AND isnotnull(d_moy#294)) AND (d_year#292 = 1998)) AND (d_moy#294 = 12))
                                 :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_month_seq#289,d_year#292,d_moy#294] Batched: true, DataFilters: [isnotnull(d_year#292), isnotnull(d_moy#294), (d_year#292 = 1998), (d_moy#294 = 12)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,12)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                                 +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#219,d_month_seq#222] Batched: true, DataFilters: [isnotnull(d_month_seq#222), isnotnull(d_date_sk#219)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_month_seq:int>

Time taken: 3.414 seconds, Fetched 1 row(s)
