Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581898907
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['promotions ASC NULLS FIRST, 'total ASC NULLS FIRST], true
      +- 'Project ['promotions, 'total, unresolvedalias(((cast('promotions as decimal(15,4)) / cast('total as decimal(15,4))) * 100), None)]
         +- 'Join Inner
            :- 'SubqueryAlias promotional_sales
            :  +- 'Project ['sum('ss_ext_sales_price) AS promotions#0]
            :     +- 'Filter ((((('ss_sold_date_sk = 'd_date_sk) AND ('ss_store_sk = 's_store_sk)) AND ('ss_promo_sk = 'p_promo_sk)) AND ((('ss_customer_sk = 'c_customer_sk) AND ('ca_address_sk = 'c_current_addr_sk)) AND ('ss_item_sk = 'i_item_sk))) AND (((('ca_gmt_offset = -5) AND ('i_category = Jewelry)) AND ((('p_channel_dmail = Y) OR ('p_channel_email = Y)) OR ('p_channel_tv = Y))) AND ((('s_gmt_offset = -5) AND ('d_year = 1998)) AND ('d_moy = 11))))
            :        +- 'Join Inner
            :           :- 'Join Inner
            :           :  :- 'Join Inner
            :           :  :  :- 'Join Inner
            :           :  :  :  :- 'Join Inner
            :           :  :  :  :  :- 'Join Inner
            :           :  :  :  :  :  :- 'UnresolvedRelation [store_sales], [], false
            :           :  :  :  :  :  +- 'UnresolvedRelation [store], [], false
            :           :  :  :  :  +- 'UnresolvedRelation [promotion], [], false
            :           :  :  :  +- 'UnresolvedRelation [date_dim], [], false
            :           :  :  +- 'UnresolvedRelation [customer], [], false
            :           :  +- 'UnresolvedRelation [customer_address], [], false
            :           +- 'UnresolvedRelation [item], [], false
            +- 'SubqueryAlias all_sales
               +- 'Project ['sum('ss_ext_sales_price) AS total#1]
                  +- 'Filter ((((('ss_sold_date_sk = 'd_date_sk) AND ('ss_store_sk = 's_store_sk)) AND ('ss_customer_sk = 'c_customer_sk)) AND (('ca_address_sk = 'c_current_addr_sk) AND ('ss_item_sk = 'i_item_sk))) AND (((('ca_gmt_offset = -5) AND ('i_category = Jewelry)) AND ('s_gmt_offset = -5)) AND (('d_year = 1998) AND ('d_moy = 11))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'Join Inner
                        :  :  :  :- 'Join Inner
                        :  :  :  :  :- 'UnresolvedRelation [store_sales], [], false
                        :  :  :  :  +- 'UnresolvedRelation [store], [], false
                        :  :  :  +- 'UnresolvedRelation [date_dim], [], false
                        :  :  +- 'UnresolvedRelation [customer], [], false
                        :  +- 'UnresolvedRelation [customer_address], [], false
                        +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
promotions: double, total: double, ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100): decimal(38,19)
GlobalLimit 100
+- LocalLimit 100
   +- Sort [promotions#0 ASC NULLS FIRST, total#1 ASC NULLS FIRST], true
      +- Project [promotions#0, total#1, ((cast(promotions#0 as decimal(15,4)) / cast(total#1 as decimal(15,4))) * cast(100 as decimal(3,0))) AS ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100)#307]
         +- Join Inner
            :- SubqueryAlias promotional_sales
            :  +- Aggregate [sum(ss_ext_sales_price#22) AS promotions#0]
            :     +- Filter (((((ss_sold_date_sk#7 = d_date_sk#78) AND (ss_store_sk#14 = s_store_sk#30)) AND (ss_promo_sk#15 = p_promo_sk#59)) AND (((ss_customer_sk#10 = c_customer_sk#106) AND (ca_address_sk#124 = c_current_addr_sk#110)) AND (ss_item_sk#9 = i_item_sk#137))) AND ((((ca_gmt_offset#135 = cast(-5 as double)) AND (i_category#149 = Jewelry)) AND (((p_channel_dmail#67 = Y) OR (p_channel_email#68 = Y)) OR (p_channel_tv#70 = Y))) AND (((s_gmt_offset#57 = cast(-5 as double)) AND (d_year#84 = 1998)) AND (d_moy#86 = 11))))
            :        +- Join Inner
            :           :- Join Inner
            :           :  :- Join Inner
            :           :  :  :- Join Inner
            :           :  :  :  :- Join Inner
            :           :  :  :  :  :- Join Inner
            :           :  :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
            :           :  :  :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
            :           :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store
            :           :  :  :  :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#30,s_store_id#31,s_rec_start_date#32,s_rec_end_date#33,s_closed_date_sk#34,s_store_name#35,s_number_employees#36,s_floor_space#37,s_hours#38,s_manager#39,s_market_id#40,s_geography_class#41,s_market_desc#42,s_market_manager#43,s_division_id#44,s_division_name#45,s_company_id#46,s_company_name#47,s_street_number#48,s_street_name#49,s_street_type#50,s_suite_number#51,s_city#52,s_county#53,... 5 more fields] parquet
            :           :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.promotion
            :           :  :  :  :     +- Relation spark_catalog.tpcds.promotion[p_promo_sk#59,p_promo_id#60,p_start_date_sk#61,p_end_date_sk#62,p_item_sk#63,p_cost#64,p_response_target#65,p_promo_name#66,p_channel_dmail#67,p_channel_email#68,p_channel_catalog#69,p_channel_tv#70,p_channel_radio#71,p_channel_press#72,p_channel_event#73,p_channel_demo#74,p_channel_details#75,p_purpose#76,p_discount_active#77] parquet
            :           :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
            :           :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#78,d_date_id#79,d_date#80,d_month_seq#81,d_week_seq#82,d_quarter_seq#83,d_year#84,d_dow#85,d_moy#86,d_dom#87,d_qoy#88,d_fy_year#89,d_fy_quarter_seq#90,d_fy_week_seq#91,d_day_name#92,d_quarter_name#93,d_holiday#94,d_weekend#95,d_following_holiday#96,d_first_dom#97,d_last_dom#98,d_same_day_ly#99,d_same_day_lq#100,d_current_day#101,... 4 more fields] parquet
            :           :  :  +- SubqueryAlias spark_catalog.tpcds.customer
            :           :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#106,c_customer_id#107,c_current_cdemo_sk#108,c_current_hdemo_sk#109,c_current_addr_sk#110,c_first_shipto_date_sk#111,c_first_sales_date_sk#112,c_salutation#113,c_first_name#114,c_last_name#115,c_preferred_cust_flag#116,c_birth_day#117,c_birth_month#118,c_birth_year#119,c_birth_country#120,c_login#121,c_email_address#122,c_last_review_date#123] parquet
            :           :  +- SubqueryAlias spark_catalog.tpcds.customer_address
            :           :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#124,ca_address_id#125,ca_street_number#126,ca_street_name#127,ca_street_type#128,ca_suite_number#129,ca_city#130,ca_county#131,ca_state#132,ca_zip#133,ca_country#134,ca_gmt_offset#135,ca_location_type#136] parquet
            :           +- SubqueryAlias spark_catalog.tpcds.item
            :              +- Relation spark_catalog.tpcds.item[i_item_sk#137,i_item_id#138,i_rec_start_date#139,i_rec_end_date#140,i_item_desc#141,i_current_price#142,i_wholesale_cost#143,i_brand_id#144,i_brand#145,i_class_id#146,i_class#147,i_category_id#148,i_category#149,i_manufact_id#150,i_manufact#151,i_size#152,i_formulation#153,i_color#154,i_units#155,i_container#156,i_manager_id#157,i_product_name#158] parquet
            +- SubqueryAlias all_sales
               +- Aggregate [sum(ss_ext_sales_price#174) AS total#1]
                  +- Filter (((((ss_sold_date_sk#159 = d_date_sk#211) AND (ss_store_sk#166 = s_store_sk#182)) AND (ss_customer_sk#162 = c_customer_sk#239)) AND ((ca_address_sk#257 = c_current_addr_sk#243) AND (ss_item_sk#161 = i_item_sk#270))) AND ((((ca_gmt_offset#268 = cast(-5 as double)) AND (i_category#282 = Jewelry)) AND (s_gmt_offset#209 = cast(-5 as double))) AND ((d_year#217 = 1998) AND (d_moy#219 = 11))))
                     +- Join Inner
                        :- Join Inner
                        :  :- Join Inner
                        :  :  :- Join Inner
                        :  :  :  :- Join Inner
                        :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                        :  :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#159,ss_sold_time_sk#160,ss_item_sk#161,ss_customer_sk#162,ss_cdemo_sk#163,ss_hdemo_sk#164,ss_addr_sk#165,ss_store_sk#166,ss_promo_sk#167,ss_ticket_number#168,ss_quantity#169,ss_wholesale_cost#170,ss_list_price#171,ss_sales_price#172,ss_ext_discount_amt#173,ss_ext_sales_price#174,ss_ext_wholesale_cost#175,ss_ext_list_price#176,ss_ext_tax#177,ss_coupon_amt#178,ss_net_paid#179,ss_net_paid_inc_tax#180,ss_net_profit#181] parquet
                        :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store
                        :  :  :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#182,s_store_id#183,s_rec_start_date#184,s_rec_end_date#185,s_closed_date_sk#186,s_store_name#187,s_number_employees#188,s_floor_space#189,s_hours#190,s_manager#191,s_market_id#192,s_geography_class#193,s_market_desc#194,s_market_manager#195,s_division_id#196,s_division_name#197,s_company_id#198,s_company_name#199,s_street_number#200,s_street_name#201,s_street_type#202,s_suite_number#203,s_city#204,s_county#205,... 5 more fields] parquet
                        :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
                        :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#211,d_date_id#212,d_date#213,d_month_seq#214,d_week_seq#215,d_quarter_seq#216,d_year#217,d_dow#218,d_moy#219,d_dom#220,d_qoy#221,d_fy_year#222,d_fy_quarter_seq#223,d_fy_week_seq#224,d_day_name#225,d_quarter_name#226,d_holiday#227,d_weekend#228,d_following_holiday#229,d_first_dom#230,d_last_dom#231,d_same_day_ly#232,d_same_day_lq#233,d_current_day#234,... 4 more fields] parquet
                        :  :  +- SubqueryAlias spark_catalog.tpcds.customer
                        :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#239,c_customer_id#240,c_current_cdemo_sk#241,c_current_hdemo_sk#242,c_current_addr_sk#243,c_first_shipto_date_sk#244,c_first_sales_date_sk#245,c_salutation#246,c_first_name#247,c_last_name#248,c_preferred_cust_flag#249,c_birth_day#250,c_birth_month#251,c_birth_year#252,c_birth_country#253,c_login#254,c_email_address#255,c_last_review_date#256] parquet
                        :  +- SubqueryAlias spark_catalog.tpcds.customer_address
                        :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#257,ca_address_id#258,ca_street_number#259,ca_street_name#260,ca_street_type#261,ca_suite_number#262,ca_city#263,ca_county#264,ca_state#265,ca_zip#266,ca_country#267,ca_gmt_offset#268,ca_location_type#269] parquet
                        +- SubqueryAlias spark_catalog.tpcds.item
                           +- Relation spark_catalog.tpcds.item[i_item_sk#270,i_item_id#271,i_rec_start_date#272,i_rec_end_date#273,i_item_desc#274,i_current_price#275,i_wholesale_cost#276,i_brand_id#277,i_brand#278,i_class_id#279,i_class#280,i_category_id#281,i_category#282,i_manufact_id#283,i_manufact#284,i_size#285,i_formulation#286,i_color#287,i_units#288,i_container#289,i_manager_id#290,i_product_name#291] parquet

== Optimized Logical Plan ==
Project [promotions#0, total#1, ((cast(promotions#0 as decimal(15,4)) / cast(total#1 as decimal(15,4))) * 100) AS ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100)#307]
+- Join Inner
   :- Aggregate [sum(ss_ext_sales_price#22) AS promotions#0]
   :  +- Project [ss_ext_sales_price#22]
   :     +- Join Inner, (ss_item_sk#9 = i_item_sk#137)
   :        :- Project [ss_item_sk#9, ss_ext_sales_price#22]
   :        :  +- Join Inner, (ca_address_sk#124 = c_current_addr_sk#110)
   :        :     :- Project [ss_item_sk#9, ss_ext_sales_price#22, c_current_addr_sk#110]
   :        :     :  +- Join Inner, (ss_customer_sk#10 = c_customer_sk#106)
   :        :     :     :- Project [ss_item_sk#9, ss_customer_sk#10, ss_ext_sales_price#22]
   :        :     :     :  +- Join Inner, (ss_sold_date_sk#7 = d_date_sk#78)
   :        :     :     :     :- Project [ss_sold_date_sk#7, ss_item_sk#9, ss_customer_sk#10, ss_ext_sales_price#22]
   :        :     :     :     :  +- Join Inner, (ss_promo_sk#15 = p_promo_sk#59)
   :        :     :     :     :     :- Project [ss_sold_date_sk#7, ss_item_sk#9, ss_customer_sk#10, ss_promo_sk#15, ss_ext_sales_price#22]
   :        :     :     :     :     :  +- Join Inner, (ss_store_sk#14 = s_store_sk#30)
   :        :     :     :     :     :     :- Project [ss_sold_date_sk#7, ss_item_sk#9, ss_customer_sk#10, ss_store_sk#14, ss_promo_sk#15, ss_ext_sales_price#22]
   :        :     :     :     :     :     :  +- Filter ((isnotnull(ss_store_sk#14) AND isnotnull(ss_promo_sk#15)) AND ((isnotnull(ss_sold_date_sk#7) AND isnotnull(ss_customer_sk#10)) AND isnotnull(ss_item_sk#9)))
   :        :     :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
   :        :     :     :     :     :     +- Project [s_store_sk#30]
   :        :     :     :     :     :        +- Filter ((isnotnull(s_gmt_offset#57) AND (s_gmt_offset#57 = -5.0)) AND isnotnull(s_store_sk#30))
   :        :     :     :     :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#30,s_store_id#31,s_rec_start_date#32,s_rec_end_date#33,s_closed_date_sk#34,s_store_name#35,s_number_employees#36,s_floor_space#37,s_hours#38,s_manager#39,s_market_id#40,s_geography_class#41,s_market_desc#42,s_market_manager#43,s_division_id#44,s_division_name#45,s_company_id#46,s_company_name#47,s_street_number#48,s_street_name#49,s_street_type#50,s_suite_number#51,s_city#52,s_county#53,... 5 more fields] parquet
   :        :     :     :     :     +- Project [p_promo_sk#59]
   :        :     :     :     :        +- Filter ((((p_channel_dmail#67 = Y) OR (p_channel_email#68 = Y)) OR (p_channel_tv#70 = Y)) AND isnotnull(p_promo_sk#59))
   :        :     :     :     :           +- Relation spark_catalog.tpcds.promotion[p_promo_sk#59,p_promo_id#60,p_start_date_sk#61,p_end_date_sk#62,p_item_sk#63,p_cost#64,p_response_target#65,p_promo_name#66,p_channel_dmail#67,p_channel_email#68,p_channel_catalog#69,p_channel_tv#70,p_channel_radio#71,p_channel_press#72,p_channel_event#73,p_channel_demo#74,p_channel_details#75,p_purpose#76,p_discount_active#77] parquet
   :        :     :     :     +- Project [d_date_sk#78]
   :        :     :     :        +- Filter (((isnotnull(d_year#84) AND isnotnull(d_moy#86)) AND ((d_year#84 = 1998) AND (d_moy#86 = 11))) AND isnotnull(d_date_sk#78))
   :        :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#78,d_date_id#79,d_date#80,d_month_seq#81,d_week_seq#82,d_quarter_seq#83,d_year#84,d_dow#85,d_moy#86,d_dom#87,d_qoy#88,d_fy_year#89,d_fy_quarter_seq#90,d_fy_week_seq#91,d_day_name#92,d_quarter_name#93,d_holiday#94,d_weekend#95,d_following_holiday#96,d_first_dom#97,d_last_dom#98,d_same_day_ly#99,d_same_day_lq#100,d_current_day#101,... 4 more fields] parquet
   :        :     :     +- Project [c_customer_sk#106, c_current_addr_sk#110]
   :        :     :        +- Filter (isnotnull(c_customer_sk#106) AND isnotnull(c_current_addr_sk#110))
   :        :     :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#106,c_customer_id#107,c_current_cdemo_sk#108,c_current_hdemo_sk#109,c_current_addr_sk#110,c_first_shipto_date_sk#111,c_first_sales_date_sk#112,c_salutation#113,c_first_name#114,c_last_name#115,c_preferred_cust_flag#116,c_birth_day#117,c_birth_month#118,c_birth_year#119,c_birth_country#120,c_login#121,c_email_address#122,c_last_review_date#123] parquet
   :        :     +- Project [ca_address_sk#124]
   :        :        +- Filter ((isnotnull(ca_gmt_offset#135) AND (ca_gmt_offset#135 = -5.0)) AND isnotnull(ca_address_sk#124))
   :        :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#124,ca_address_id#125,ca_street_number#126,ca_street_name#127,ca_street_type#128,ca_suite_number#129,ca_city#130,ca_county#131,ca_state#132,ca_zip#133,ca_country#134,ca_gmt_offset#135,ca_location_type#136] parquet
   :        +- Project [i_item_sk#137]
   :           +- Filter ((isnotnull(i_category#149) AND (i_category#149 = Jewelry)) AND isnotnull(i_item_sk#137))
   :              +- Relation spark_catalog.tpcds.item[i_item_sk#137,i_item_id#138,i_rec_start_date#139,i_rec_end_date#140,i_item_desc#141,i_current_price#142,i_wholesale_cost#143,i_brand_id#144,i_brand#145,i_class_id#146,i_class#147,i_category_id#148,i_category#149,i_manufact_id#150,i_manufact#151,i_size#152,i_formulation#153,i_color#154,i_units#155,i_container#156,i_manager_id#157,i_product_name#158] parquet
   +- Aggregate [sum(ss_ext_sales_price#174) AS total#1]
      +- Project [ss_ext_sales_price#174]
         +- Join Inner, (ss_item_sk#161 = i_item_sk#270)
            :- Project [ss_item_sk#161, ss_ext_sales_price#174]
            :  +- Join Inner, (ca_address_sk#257 = c_current_addr_sk#243)
            :     :- Project [ss_item_sk#161, ss_ext_sales_price#174, c_current_addr_sk#243]
            :     :  +- Join Inner, (ss_customer_sk#162 = c_customer_sk#239)
            :     :     :- Project [ss_item_sk#161, ss_customer_sk#162, ss_ext_sales_price#174]
            :     :     :  +- Join Inner, (ss_sold_date_sk#159 = d_date_sk#211)
            :     :     :     :- Project [ss_sold_date_sk#159, ss_item_sk#161, ss_customer_sk#162, ss_ext_sales_price#174]
            :     :     :     :  +- Join Inner, (ss_store_sk#166 = s_store_sk#182)
            :     :     :     :     :- Project [ss_sold_date_sk#159, ss_item_sk#161, ss_customer_sk#162, ss_store_sk#166, ss_ext_sales_price#174]
            :     :     :     :     :  +- Filter (isnotnull(ss_store_sk#166) AND ((isnotnull(ss_sold_date_sk#159) AND isnotnull(ss_customer_sk#162)) AND isnotnull(ss_item_sk#161)))
            :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#159,ss_sold_time_sk#160,ss_item_sk#161,ss_customer_sk#162,ss_cdemo_sk#163,ss_hdemo_sk#164,ss_addr_sk#165,ss_store_sk#166,ss_promo_sk#167,ss_ticket_number#168,ss_quantity#169,ss_wholesale_cost#170,ss_list_price#171,ss_sales_price#172,ss_ext_discount_amt#173,ss_ext_sales_price#174,ss_ext_wholesale_cost#175,ss_ext_list_price#176,ss_ext_tax#177,ss_coupon_amt#178,ss_net_paid#179,ss_net_paid_inc_tax#180,ss_net_profit#181] parquet
            :     :     :     :     +- Project [s_store_sk#182]
            :     :     :     :        +- Filter ((isnotnull(s_gmt_offset#209) AND (s_gmt_offset#209 = -5.0)) AND isnotnull(s_store_sk#182))
            :     :     :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#182,s_store_id#183,s_rec_start_date#184,s_rec_end_date#185,s_closed_date_sk#186,s_store_name#187,s_number_employees#188,s_floor_space#189,s_hours#190,s_manager#191,s_market_id#192,s_geography_class#193,s_market_desc#194,s_market_manager#195,s_division_id#196,s_division_name#197,s_company_id#198,s_company_name#199,s_street_number#200,s_street_name#201,s_street_type#202,s_suite_number#203,s_city#204,s_county#205,... 5 more fields] parquet
            :     :     :     +- Project [d_date_sk#211]
            :     :     :        +- Filter (((isnotnull(d_year#217) AND isnotnull(d_moy#219)) AND ((d_year#217 = 1998) AND (d_moy#219 = 11))) AND isnotnull(d_date_sk#211))
            :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#211,d_date_id#212,d_date#213,d_month_seq#214,d_week_seq#215,d_quarter_seq#216,d_year#217,d_dow#218,d_moy#219,d_dom#220,d_qoy#221,d_fy_year#222,d_fy_quarter_seq#223,d_fy_week_seq#224,d_day_name#225,d_quarter_name#226,d_holiday#227,d_weekend#228,d_following_holiday#229,d_first_dom#230,d_last_dom#231,d_same_day_ly#232,d_same_day_lq#233,d_current_day#234,... 4 more fields] parquet
            :     :     +- Project [c_customer_sk#239, c_current_addr_sk#243]
            :     :        +- Filter (isnotnull(c_customer_sk#239) AND isnotnull(c_current_addr_sk#243))
            :     :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#239,c_customer_id#240,c_current_cdemo_sk#241,c_current_hdemo_sk#242,c_current_addr_sk#243,c_first_shipto_date_sk#244,c_first_sales_date_sk#245,c_salutation#246,c_first_name#247,c_last_name#248,c_preferred_cust_flag#249,c_birth_day#250,c_birth_month#251,c_birth_year#252,c_birth_country#253,c_login#254,c_email_address#255,c_last_review_date#256] parquet
            :     +- Project [ca_address_sk#257]
            :        +- Filter ((isnotnull(ca_gmt_offset#268) AND (ca_gmt_offset#268 = -5.0)) AND isnotnull(ca_address_sk#257))
            :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#257,ca_address_id#258,ca_street_number#259,ca_street_name#260,ca_street_type#261,ca_suite_number#262,ca_city#263,ca_county#264,ca_state#265,ca_zip#266,ca_country#267,ca_gmt_offset#268,ca_location_type#269] parquet
            +- Project [i_item_sk#270]
               +- Filter ((isnotnull(i_category#282) AND (i_category#282 = Jewelry)) AND isnotnull(i_item_sk#270))
                  +- Relation spark_catalog.tpcds.item[i_item_sk#270,i_item_id#271,i_rec_start_date#272,i_rec_end_date#273,i_item_desc#274,i_current_price#275,i_wholesale_cost#276,i_brand_id#277,i_brand#278,i_class_id#279,i_class#280,i_category_id#281,i_category#282,i_manufact_id#283,i_manufact#284,i_size#285,i_formulation#286,i_color#287,i_units#288,i_container#289,i_manager_id#290,i_product_name#291] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [promotions#0, total#1, ((cast(promotions#0 as decimal(15,4)) / cast(total#1 as decimal(15,4))) * 100) AS ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100)#307]
   +- BroadcastNestedLoopJoin BuildRight, Inner
      :- HashAggregate(keys=[], functions=[sum(ss_ext_sales_price#22)], output=[promotions#0])
      :  +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=273]
      :     +- HashAggregate(keys=[], functions=[partial_sum(ss_ext_sales_price#22)], output=[sum#309])
      :        +- Project [ss_ext_sales_price#22]
      :           +- BroadcastHashJoin [ss_item_sk#9], [i_item_sk#137], Inner, BuildRight, false
      :              :- Project [ss_item_sk#9, ss_ext_sales_price#22]
      :              :  +- BroadcastHashJoin [c_current_addr_sk#110], [ca_address_sk#124], Inner, BuildRight, false
      :              :     :- Project [ss_item_sk#9, ss_ext_sales_price#22, c_current_addr_sk#110]
      :              :     :  +- BroadcastHashJoin [ss_customer_sk#10], [c_customer_sk#106], Inner, BuildRight, false
      :              :     :     :- Project [ss_item_sk#9, ss_customer_sk#10, ss_ext_sales_price#22]
      :              :     :     :  +- BroadcastHashJoin [ss_sold_date_sk#7], [d_date_sk#78], Inner, BuildRight, false
      :              :     :     :     :- Project [ss_sold_date_sk#7, ss_item_sk#9, ss_customer_sk#10, ss_ext_sales_price#22]
      :              :     :     :     :  +- BroadcastHashJoin [ss_promo_sk#15], [p_promo_sk#59], Inner, BuildRight, false
      :              :     :     :     :     :- Project [ss_sold_date_sk#7, ss_item_sk#9, ss_customer_sk#10, ss_promo_sk#15, ss_ext_sales_price#22]
      :              :     :     :     :     :  +- BroadcastHashJoin [ss_store_sk#14], [s_store_sk#30], Inner, BuildRight, false
      :              :     :     :     :     :     :- Filter ((((isnotnull(ss_store_sk#14) AND isnotnull(ss_promo_sk#15)) AND isnotnull(ss_sold_date_sk#7)) AND isnotnull(ss_customer_sk#10)) AND isnotnull(ss_item_sk#9))
      :              :     :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_item_sk#9,ss_customer_sk#10,ss_store_sk#14,ss_promo_sk#15,ss_ext_sales_price#22] Batched: true, DataFilters: [isnotnull(ss_store_sk#14), isnotnull(ss_promo_sk#15), isnotnull(ss_sold_date_sk#7), isnotnull(ss..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_promo_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_promo_sk:int,ss_e...
      :              :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=248]
      :              :     :     :     :     :        +- Project [s_store_sk#30]
      :              :     :     :     :     :           +- Filter ((isnotnull(s_gmt_offset#57) AND (s_gmt_offset#57 = -5.0)) AND isnotnull(s_store_sk#30))
      :              :     :     :     :     :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#30,s_gmt_offset#57] Batched: true, DataFilters: [isnotnull(s_gmt_offset#57), (s_gmt_offset#57 = -5.0), isnotnull(s_store_sk#30)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_gmt_offset), EqualTo(s_gmt_offset,-5.0), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_gmt_offset:double>
      :              :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=252]
      :              :     :     :     :        +- Project [p_promo_sk#59]
      :              :     :     :     :           +- Filter ((((p_channel_dmail#67 = Y) OR (p_channel_email#68 = Y)) OR (p_channel_tv#70 = Y)) AND isnotnull(p_promo_sk#59))
      :              :     :     :     :              +- FileScan parquet spark_catalog.tpcds.promotion[p_promo_sk#59,p_channel_dmail#67,p_channel_email#68,p_channel_tv#70] Batched: true, DataFilters: [(((p_channel_dmail#67 = Y) OR (p_channel_email#68 = Y)) OR (p_channel_tv#70 = Y)), isnotnull(p_p..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(p_channel_dmail,Y),EqualTo(p_channel_email,Y)),EqualTo(p_channel_tv,Y)), IsNotNull..., ReadSchema: struct<p_promo_sk:int,p_channel_dmail:string,p_channel_email:string,p_channel_tv:string>
      :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=256]
      :              :     :     :        +- Project [d_date_sk#78]
      :              :     :     :           +- Filter ((((isnotnull(d_year#84) AND isnotnull(d_moy#86)) AND (d_year#84 = 1998)) AND (d_moy#86 = 11)) AND isnotnull(d_date_sk#78))
      :              :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#78,d_year#84,d_moy#86] Batched: true, DataFilters: [isnotnull(d_year#84), isnotnull(d_moy#86), (d_year#84 = 1998), (d_moy#86 = 11), isnotnull(d_date..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
      :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=260]
      :              :     :        +- Filter (isnotnull(c_customer_sk#106) AND isnotnull(c_current_addr_sk#110))
      :              :     :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#106,c_current_addr_sk#110] Batched: true, DataFilters: [isnotnull(c_customer_sk#106), isnotnull(c_current_addr_sk#110)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
      :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=264]
      :              :        +- Project [ca_address_sk#124]
      :              :           +- Filter ((isnotnull(ca_gmt_offset#135) AND (ca_gmt_offset#135 = -5.0)) AND isnotnull(ca_address_sk#124))
      :              :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#124,ca_gmt_offset#135] Batched: true, DataFilters: [isnotnull(ca_gmt_offset#135), (ca_gmt_offset#135 = -5.0), isnotnull(ca_address_sk#124)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-5.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
      :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=268]
      :                 +- Project [i_item_sk#137]
      :                    +- Filter ((isnotnull(i_category#149) AND (i_category#149 = Jewelry)) AND isnotnull(i_item_sk#137))
      :                       +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#137,i_category#149] Batched: true, DataFilters: [isnotnull(i_category#149), (i_category#149 = Jewelry), isnotnull(i_item_sk#137)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Jewelry), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category:string>
      +- BroadcastExchange IdentityBroadcastMode, [plan_id=299]
         +- HashAggregate(keys=[], functions=[sum(ss_ext_sales_price#174)], output=[total#1])
            +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=296]
               +- HashAggregate(keys=[], functions=[partial_sum(ss_ext_sales_price#174)], output=[sum#311])
                  +- Project [ss_ext_sales_price#174]
                     +- BroadcastHashJoin [ss_item_sk#161], [i_item_sk#270], Inner, BuildRight, false
                        :- Project [ss_item_sk#161, ss_ext_sales_price#174]
                        :  +- BroadcastHashJoin [c_current_addr_sk#243], [ca_address_sk#257], Inner, BuildRight, false
                        :     :- Project [ss_item_sk#161, ss_ext_sales_price#174, c_current_addr_sk#243]
                        :     :  +- BroadcastHashJoin [ss_customer_sk#162], [c_customer_sk#239], Inner, BuildRight, false
                        :     :     :- Project [ss_item_sk#161, ss_customer_sk#162, ss_ext_sales_price#174]
                        :     :     :  +- BroadcastHashJoin [ss_sold_date_sk#159], [d_date_sk#211], Inner, BuildRight, false
                        :     :     :     :- Project [ss_sold_date_sk#159, ss_item_sk#161, ss_customer_sk#162, ss_ext_sales_price#174]
                        :     :     :     :  +- BroadcastHashJoin [ss_store_sk#166], [s_store_sk#182], Inner, BuildRight, false
                        :     :     :     :     :- Filter (((isnotnull(ss_store_sk#166) AND isnotnull(ss_sold_date_sk#159)) AND isnotnull(ss_customer_sk#162)) AND isnotnull(ss_item_sk#161))
                        :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#159,ss_item_sk#161,ss_customer_sk#162,ss_store_sk#166,ss_ext_sales_price#174] Batched: true, DataFilters: [isnotnull(ss_store_sk#166), isnotnull(ss_sold_date_sk#159), isnotnull(ss_customer_sk#162), isnot..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_customer_sk), IsNotNull(ss_item..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ext_sales_price:d...
                        :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=275]
                        :     :     :     :        +- Project [s_store_sk#182]
                        :     :     :     :           +- Filter ((isnotnull(s_gmt_offset#209) AND (s_gmt_offset#209 = -5.0)) AND isnotnull(s_store_sk#182))
                        :     :     :     :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#182,s_gmt_offset#209] Batched: true, DataFilters: [isnotnull(s_gmt_offset#209), (s_gmt_offset#209 = -5.0), isnotnull(s_store_sk#182)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_gmt_offset), EqualTo(s_gmt_offset,-5.0), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_gmt_offset:double>
                        :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=279]
                        :     :     :        +- Project [d_date_sk#211]
                        :     :     :           +- Filter ((((isnotnull(d_year#217) AND isnotnull(d_moy#219)) AND (d_year#217 = 1998)) AND (d_moy#219 = 11)) AND isnotnull(d_date_sk#211))
                        :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#211,d_year#217,d_moy#219] Batched: true, DataFilters: [isnotnull(d_year#217), isnotnull(d_moy#219), (d_year#217 = 1998), (d_moy#219 = 11), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                        :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=283]
                        :     :        +- Filter (isnotnull(c_customer_sk#239) AND isnotnull(c_current_addr_sk#243))
                        :     :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#239,c_current_addr_sk#243] Batched: true, DataFilters: [isnotnull(c_customer_sk#239), isnotnull(c_current_addr_sk#243)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=287]
                        :        +- Project [ca_address_sk#257]
                        :           +- Filter ((isnotnull(ca_gmt_offset#268) AND (ca_gmt_offset#268 = -5.0)) AND isnotnull(ca_address_sk#257))
                        :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#257,ca_gmt_offset#268] Batched: true, DataFilters: [isnotnull(ca_gmt_offset#268), (ca_gmt_offset#268 = -5.0), isnotnull(ca_address_sk#257)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-5.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=291]
                           +- Project [i_item_sk#270]
                              +- Filter ((isnotnull(i_category#282) AND (i_category#282 = Jewelry)) AND isnotnull(i_item_sk#270))
                                 +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#270,i_category#282] Batched: true, DataFilters: [isnotnull(i_category#282), (i_category#282 = Jewelry), isnotnull(i_item_sk#270)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Jewelry), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category:string>

Time taken: 3.502 seconds, Fetched 1 row(s)
