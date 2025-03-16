Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579823599
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['cd_gender ASC NULLS FIRST, 'cd_marital_status ASC NULLS FIRST, 'cd_education_status ASC NULLS FIRST, 'cd_purchase_estimate ASC NULLS FIRST, 'cd_credit_rating ASC NULLS FIRST, 'cd_dep_count ASC NULLS FIRST, 'cd_dep_employed_count ASC NULLS FIRST, 'cd_dep_college_count ASC NULLS FIRST], true
      +- 'Aggregate ['cd_gender, 'cd_marital_status, 'cd_education_status, 'cd_purchase_estimate, 'cd_credit_rating, 'cd_dep_count, 'cd_dep_employed_count, 'cd_dep_college_count], ['cd_gender, 'cd_marital_status, 'cd_education_status, 'count(1) AS cnt1#0, 'cd_purchase_estimate, 'count(1) AS cnt2#1, 'cd_credit_rating, 'count(1) AS cnt3#2, 'cd_dep_count, 'count(1) AS cnt4#3, 'cd_dep_employed_count, 'count(1) AS cnt5#4, 'cd_dep_college_count, 'count(1) AS cnt6#5]
         +- 'Filter (((('c.c_current_addr_sk = 'ca.ca_address_sk) AND 'ca_county IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County)) AND ('cd_demo_sk = 'c.c_current_cdemo_sk)) AND (exists#6 [] AND (exists#7 [] OR exists#8 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ss_customer_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND (('d_year = 2002) AND (('d_moy >= 1) AND ('d_moy <= (1 + 3)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation [store_sales], [], false
            :  :        +- 'UnresolvedRelation [date_dim], [], false
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ws_bill_customer_sk) AND ('ws_sold_date_sk = 'd_date_sk)) AND (('d_year = 2002) AND (('d_moy >= 1) AND ('d_moy <= (1 + 3)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation [web_sales], [], false
            :  :        +- 'UnresolvedRelation [date_dim], [], false
            :  +- 'Project [*]
            :     +- 'Filter ((('c.c_customer_sk = 'cs_ship_customer_sk) AND ('cs_sold_date_sk = 'd_date_sk)) AND (('d_year = 2002) AND (('d_moy >= 1) AND ('d_moy <= (1 + 3)))))
            :        +- 'Join Inner
            :           :- 'UnresolvedRelation [catalog_sales], [], false
            :           +- 'UnresolvedRelation [date_dim], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias c
               :  :  +- 'UnresolvedRelation [customer], [], false
               :  +- 'SubqueryAlias ca
               :     +- 'UnresolvedRelation [customer_address], [], false
               +- 'UnresolvedRelation [customer_demographics], [], false

== Analyzed Logical Plan ==
cd_gender: string, cd_marital_status: string, cd_education_status: string, cnt1: bigint, cd_purchase_estimate: int, cnt2: bigint, cd_credit_rating: string, cnt3: bigint, cd_dep_count: int, cnt4: bigint, cd_dep_employed_count: int, cnt5: bigint, cd_dep_college_count: int, cnt6: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#52 ASC NULLS FIRST, cd_marital_status#53 ASC NULLS FIRST, cd_education_status#54 ASC NULLS FIRST, cd_purchase_estimate#55 ASC NULLS FIRST, cd_credit_rating#56 ASC NULLS FIRST, cd_dep_count#57 ASC NULLS FIRST, cd_dep_employed_count#58 ASC NULLS FIRST, cd_dep_college_count#59 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59], [cd_gender#52, cd_marital_status#53, cd_education_status#54, count(1) AS cnt1#0L, cd_purchase_estimate#55, count(1) AS cnt2#1L, cd_credit_rating#56, count(1) AS cnt3#2L, cd_dep_count#57, count(1) AS cnt4#3L, cd_dep_employed_count#58, count(1) AS cnt5#4L, cd_dep_college_count#59, count(1) AS cnt6#5L]
         +- Filter ((((c_current_addr_sk#24 = ca_address_sk#38) AND ca_county#45 IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County)) AND (cd_demo_sk#51 = c_current_cdemo_sk#22)) AND (exists#6 [c_customer_sk#20] AND (exists#7 [c_customer_sk#20] OR exists#8 [c_customer_sk#20])))
            :  :- Project [ss_sold_date_sk#60, ss_sold_time_sk#61, ss_item_sk#62, ss_customer_sk#63, ss_cdemo_sk#64, ss_hdemo_sk#65, ss_addr_sk#66, ss_store_sk#67, ss_promo_sk#68, ss_ticket_number#69, ss_quantity#70, ss_wholesale_cost#71, ss_list_price#72, ss_sales_price#73, ss_ext_discount_amt#74, ss_ext_sales_price#75, ss_ext_wholesale_cost#76, ss_ext_list_price#77, ss_ext_tax#78, ss_coupon_amt#79, ss_net_paid#80, ss_net_paid_inc_tax#81, ss_net_profit#82, d_date_sk#83, ... 27 more fields]
            :  :  +- Filter (((outer(c_customer_sk#20) = ss_customer_sk#63) AND (ss_sold_date_sk#60 = d_date_sk#83)) AND ((d_year#89 = 2002) AND ((d_moy#91 >= 1) AND (d_moy#91 <= (1 + 3)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias spark_catalog.tpcds.store_sales
            :  :        :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#60,ss_sold_time_sk#61,ss_item_sk#62,ss_customer_sk#63,ss_cdemo_sk#64,ss_hdemo_sk#65,ss_addr_sk#66,ss_store_sk#67,ss_promo_sk#68,ss_ticket_number#69,ss_quantity#70,ss_wholesale_cost#71,ss_list_price#72,ss_sales_price#73,ss_ext_discount_amt#74,ss_ext_sales_price#75,ss_ext_wholesale_cost#76,ss_ext_list_price#77,ss_ext_tax#78,ss_coupon_amt#79,ss_net_paid#80,ss_net_paid_inc_tax#81,ss_net_profit#82] parquet
            :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
            :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#83,d_date_id#84,d_date#85,d_month_seq#86,d_week_seq#87,d_quarter_seq#88,d_year#89,d_dow#90,d_moy#91,d_dom#92,d_qoy#93,d_fy_year#94,d_fy_quarter_seq#95,d_fy_week_seq#96,d_day_name#97,d_quarter_name#98,d_holiday#99,d_weekend#100,d_following_holiday#101,d_first_dom#102,d_last_dom#103,d_same_day_ly#104,d_same_day_lq#105,d_current_day#106,... 4 more fields] parquet
            :  :- Project [ws_sold_date_sk#113, ws_sold_time_sk#114, ws_ship_date_sk#115, ws_item_sk#116, ws_bill_customer_sk#117, ws_bill_cdemo_sk#118, ws_bill_hdemo_sk#119, ws_bill_addr_sk#120, ws_ship_customer_sk#121, ws_ship_cdemo_sk#122, ws_ship_hdemo_sk#123, ws_ship_addr_sk#124, ws_web_page_sk#125, ws_web_site_sk#126, ws_ship_mode_sk#127, ws_warehouse_sk#128, ws_promo_sk#129, ws_order_number#130, ws_quantity#131, ws_wholesale_cost#132, ws_list_price#133, ws_sales_price#134, ws_ext_discount_amt#135, ws_ext_sales_price#136, ... 38 more fields]
            :  :  +- Filter (((outer(c_customer_sk#20) = ws_bill_customer_sk#117) AND (ws_sold_date_sk#113 = d_date_sk#186)) AND ((d_year#192 = 2002) AND ((d_moy#194 >= 1) AND (d_moy#194 <= (1 + 3)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias spark_catalog.tpcds.web_sales
            :  :        :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#113,ws_sold_time_sk#114,ws_ship_date_sk#115,ws_item_sk#116,ws_bill_customer_sk#117,ws_bill_cdemo_sk#118,ws_bill_hdemo_sk#119,ws_bill_addr_sk#120,ws_ship_customer_sk#121,ws_ship_cdemo_sk#122,ws_ship_hdemo_sk#123,ws_ship_addr_sk#124,ws_web_page_sk#125,ws_web_site_sk#126,ws_ship_mode_sk#127,ws_warehouse_sk#128,ws_promo_sk#129,ws_order_number#130,ws_quantity#131,ws_wholesale_cost#132,ws_list_price#133,ws_sales_price#134,ws_ext_discount_amt#135,ws_ext_sales_price#136,... 10 more fields] parquet
            :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
            :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#186,d_date_id#187,d_date#188,d_month_seq#189,d_week_seq#190,d_quarter_seq#191,d_year#192,d_dow#193,d_moy#194,d_dom#195,d_qoy#196,d_fy_year#197,d_fy_quarter_seq#198,d_fy_week_seq#199,d_day_name#200,d_quarter_name#201,d_holiday#202,d_weekend#203,d_following_holiday#204,d_first_dom#205,d_last_dom#206,d_same_day_ly#207,d_same_day_lq#208,d_current_day#209,... 4 more fields] parquet
            :  +- Project [cs_sold_date_sk#148, cs_sold_time_sk#149, cs_ship_date_sk#150, cs_bill_customer_sk#151, cs_bill_cdemo_sk#152, cs_bill_hdemo_sk#153, cs_bill_addr_sk#154, cs_ship_customer_sk#155, cs_ship_cdemo_sk#156, cs_ship_hdemo_sk#157, cs_ship_addr_sk#158, cs_call_center_sk#159, cs_catalog_page_sk#160, cs_ship_mode_sk#161, cs_warehouse_sk#162, cs_item_sk#163, cs_promo_sk#164, cs_order_number#165, cs_quantity#166, cs_wholesale_cost#167, cs_list_price#168, cs_sales_price#169, cs_ext_discount_amt#170, cs_ext_sales_price#171, ... 38 more fields]
            :     +- Filter (((outer(c_customer_sk#20) = cs_ship_customer_sk#155) AND (cs_sold_date_sk#148 = d_date_sk#214)) AND ((d_year#220 = 2002) AND ((d_moy#222 >= 1) AND (d_moy#222 <= (1 + 3)))))
            :        +- Join Inner
            :           :- SubqueryAlias spark_catalog.tpcds.catalog_sales
            :           :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#148,cs_sold_time_sk#149,cs_ship_date_sk#150,cs_bill_customer_sk#151,cs_bill_cdemo_sk#152,cs_bill_hdemo_sk#153,cs_bill_addr_sk#154,cs_ship_customer_sk#155,cs_ship_cdemo_sk#156,cs_ship_hdemo_sk#157,cs_ship_addr_sk#158,cs_call_center_sk#159,cs_catalog_page_sk#160,cs_ship_mode_sk#161,cs_warehouse_sk#162,cs_item_sk#163,cs_promo_sk#164,cs_order_number#165,cs_quantity#166,cs_wholesale_cost#167,cs_list_price#168,cs_sales_price#169,cs_ext_discount_amt#170,cs_ext_sales_price#171,... 10 more fields] parquet
            :           +- SubqueryAlias spark_catalog.tpcds.date_dim
            :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#214,d_date_id#215,d_date#216,d_month_seq#217,d_week_seq#218,d_quarter_seq#219,d_year#220,d_dow#221,d_moy#222,d_dom#223,d_qoy#224,d_fy_year#225,d_fy_quarter_seq#226,d_fy_week_seq#227,d_day_name#228,d_quarter_name#229,d_holiday#230,d_weekend#231,d_following_holiday#232,d_first_dom#233,d_last_dom#234,d_same_day_ly#235,d_same_day_lq#236,d_current_day#237,... 4 more fields] parquet
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias c
               :  :  +- SubqueryAlias spark_catalog.tpcds.customer
               :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#20,c_customer_id#21,c_current_cdemo_sk#22,c_current_hdemo_sk#23,c_current_addr_sk#24,c_first_shipto_date_sk#25,c_first_sales_date_sk#26,c_salutation#27,c_first_name#28,c_last_name#29,c_preferred_cust_flag#30,c_birth_day#31,c_birth_month#32,c_birth_year#33,c_birth_country#34,c_login#35,c_email_address#36,c_last_review_date#37] parquet
               :  +- SubqueryAlias ca
               :     +- SubqueryAlias spark_catalog.tpcds.customer_address
               :        +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#38,ca_address_id#39,ca_street_number#40,ca_street_name#41,ca_street_type#42,ca_suite_number#43,ca_city#44,ca_county#45,ca_state#46,ca_zip#47,ca_country#48,ca_gmt_offset#49,ca_location_type#50] parquet
               +- SubqueryAlias spark_catalog.tpcds.customer_demographics
                  +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#51,cd_gender#52,cd_marital_status#53,cd_education_status#54,cd_purchase_estimate#55,cd_credit_rating#56,cd_dep_count#57,cd_dep_employed_count#58,cd_dep_college_count#59] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#52 ASC NULLS FIRST, cd_marital_status#53 ASC NULLS FIRST, cd_education_status#54 ASC NULLS FIRST, cd_purchase_estimate#55 ASC NULLS FIRST, cd_credit_rating#56 ASC NULLS FIRST, cd_dep_count#57 ASC NULLS FIRST, cd_dep_employed_count#58 ASC NULLS FIRST, cd_dep_college_count#59 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59], [cd_gender#52, cd_marital_status#53, cd_education_status#54, count(1) AS cnt1#0L, cd_purchase_estimate#55, count(1) AS cnt2#1L, cd_credit_rating#56, count(1) AS cnt3#2L, cd_dep_count#57, count(1) AS cnt4#3L, cd_dep_employed_count#58, count(1) AS cnt5#4L, cd_dep_college_count#59, count(1) AS cnt6#5L]
         +- Project [cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59]
            +- Join Inner, (cd_demo_sk#51 = c_current_cdemo_sk#22)
               :- Project [c_current_cdemo_sk#22]
               :  +- Join Inner, (c_current_addr_sk#24 = ca_address_sk#38)
               :     :- Project [c_current_cdemo_sk#22, c_current_addr_sk#24]
               :     :  +- Filter (exists#242 OR exists#243)
               :     :     +- Join ExistenceJoin(exists#243), (c_customer_sk#20 = cs_ship_customer_sk#155)
               :     :        :- Join ExistenceJoin(exists#242), (c_customer_sk#20 = ws_bill_customer_sk#117)
               :     :        :  :- Join LeftSemi, (c_customer_sk#20 = ss_customer_sk#63)
               :     :        :  :  :- Project [c_customer_sk#20, c_current_cdemo_sk#22, c_current_addr_sk#24]
               :     :        :  :  :  +- Filter (isnotnull(c_current_addr_sk#24) AND isnotnull(c_current_cdemo_sk#22))
               :     :        :  :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#20,c_customer_id#21,c_current_cdemo_sk#22,c_current_hdemo_sk#23,c_current_addr_sk#24,c_first_shipto_date_sk#25,c_first_sales_date_sk#26,c_salutation#27,c_first_name#28,c_last_name#29,c_preferred_cust_flag#30,c_birth_day#31,c_birth_month#32,c_birth_year#33,c_birth_country#34,c_login#35,c_email_address#36,c_last_review_date#37] parquet
               :     :        :  :  +- Project [ss_customer_sk#63]
               :     :        :  :     +- Join Inner, (ss_sold_date_sk#60 = d_date_sk#83)
               :     :        :  :        :- Project [ss_sold_date_sk#60, ss_customer_sk#63]
               :     :        :  :        :  +- Filter isnotnull(ss_sold_date_sk#60)
               :     :        :  :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#60,ss_sold_time_sk#61,ss_item_sk#62,ss_customer_sk#63,ss_cdemo_sk#64,ss_hdemo_sk#65,ss_addr_sk#66,ss_store_sk#67,ss_promo_sk#68,ss_ticket_number#69,ss_quantity#70,ss_wholesale_cost#71,ss_list_price#72,ss_sales_price#73,ss_ext_discount_amt#74,ss_ext_sales_price#75,ss_ext_wholesale_cost#76,ss_ext_list_price#77,ss_ext_tax#78,ss_coupon_amt#79,ss_net_paid#80,ss_net_paid_inc_tax#81,ss_net_profit#82] parquet
               :     :        :  :        +- Project [d_date_sk#83]
               :     :        :  :           +- Filter (((isnotnull(d_year#89) AND isnotnull(d_moy#91)) AND (((d_year#89 = 2002) AND (d_moy#91 >= 1)) AND (d_moy#91 <= 4))) AND isnotnull(d_date_sk#83))
               :     :        :  :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#83,d_date_id#84,d_date#85,d_month_seq#86,d_week_seq#87,d_quarter_seq#88,d_year#89,d_dow#90,d_moy#91,d_dom#92,d_qoy#93,d_fy_year#94,d_fy_quarter_seq#95,d_fy_week_seq#96,d_day_name#97,d_quarter_name#98,d_holiday#99,d_weekend#100,d_following_holiday#101,d_first_dom#102,d_last_dom#103,d_same_day_ly#104,d_same_day_lq#105,d_current_day#106,... 4 more fields] parquet
               :     :        :  +- Project [ws_bill_customer_sk#117]
               :     :        :     +- Join Inner, (ws_sold_date_sk#113 = d_date_sk#186)
               :     :        :        :- Project [ws_sold_date_sk#113, ws_bill_customer_sk#117]
               :     :        :        :  +- Filter isnotnull(ws_sold_date_sk#113)
               :     :        :        :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#113,ws_sold_time_sk#114,ws_ship_date_sk#115,ws_item_sk#116,ws_bill_customer_sk#117,ws_bill_cdemo_sk#118,ws_bill_hdemo_sk#119,ws_bill_addr_sk#120,ws_ship_customer_sk#121,ws_ship_cdemo_sk#122,ws_ship_hdemo_sk#123,ws_ship_addr_sk#124,ws_web_page_sk#125,ws_web_site_sk#126,ws_ship_mode_sk#127,ws_warehouse_sk#128,ws_promo_sk#129,ws_order_number#130,ws_quantity#131,ws_wholesale_cost#132,ws_list_price#133,ws_sales_price#134,ws_ext_discount_amt#135,ws_ext_sales_price#136,... 10 more fields] parquet
               :     :        :        +- Project [d_date_sk#186]
               :     :        :           +- Filter (((isnotnull(d_year#192) AND isnotnull(d_moy#194)) AND (((d_year#192 = 2002) AND (d_moy#194 >= 1)) AND (d_moy#194 <= 4))) AND isnotnull(d_date_sk#186))
               :     :        :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#186,d_date_id#187,d_date#188,d_month_seq#189,d_week_seq#190,d_quarter_seq#191,d_year#192,d_dow#193,d_moy#194,d_dom#195,d_qoy#196,d_fy_year#197,d_fy_quarter_seq#198,d_fy_week_seq#199,d_day_name#200,d_quarter_name#201,d_holiday#202,d_weekend#203,d_following_holiday#204,d_first_dom#205,d_last_dom#206,d_same_day_ly#207,d_same_day_lq#208,d_current_day#209,... 4 more fields] parquet
               :     :        +- Project [cs_ship_customer_sk#155]
               :     :           +- Join Inner, (cs_sold_date_sk#148 = d_date_sk#214)
               :     :              :- Project [cs_sold_date_sk#148, cs_ship_customer_sk#155]
               :     :              :  +- Filter isnotnull(cs_sold_date_sk#148)
               :     :              :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#148,cs_sold_time_sk#149,cs_ship_date_sk#150,cs_bill_customer_sk#151,cs_bill_cdemo_sk#152,cs_bill_hdemo_sk#153,cs_bill_addr_sk#154,cs_ship_customer_sk#155,cs_ship_cdemo_sk#156,cs_ship_hdemo_sk#157,cs_ship_addr_sk#158,cs_call_center_sk#159,cs_catalog_page_sk#160,cs_ship_mode_sk#161,cs_warehouse_sk#162,cs_item_sk#163,cs_promo_sk#164,cs_order_number#165,cs_quantity#166,cs_wholesale_cost#167,cs_list_price#168,cs_sales_price#169,cs_ext_discount_amt#170,cs_ext_sales_price#171,... 10 more fields] parquet
               :     :              +- Project [d_date_sk#214]
               :     :                 +- Filter (((isnotnull(d_year#220) AND isnotnull(d_moy#222)) AND (((d_year#220 = 2002) AND (d_moy#222 >= 1)) AND (d_moy#222 <= 4))) AND isnotnull(d_date_sk#214))
               :     :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#214,d_date_id#215,d_date#216,d_month_seq#217,d_week_seq#218,d_quarter_seq#219,d_year#220,d_dow#221,d_moy#222,d_dom#223,d_qoy#224,d_fy_year#225,d_fy_quarter_seq#226,d_fy_week_seq#227,d_day_name#228,d_quarter_name#229,d_holiday#230,d_weekend#231,d_following_holiday#232,d_first_dom#233,d_last_dom#234,d_same_day_ly#235,d_same_day_lq#236,d_current_day#237,... 4 more fields] parquet
               :     +- Project [ca_address_sk#38]
               :        +- Filter (ca_county#45 IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County) AND isnotnull(ca_address_sk#38))
               :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#38,ca_address_id#39,ca_street_number#40,ca_street_name#41,ca_street_type#42,ca_suite_number#43,ca_city#44,ca_county#45,ca_state#46,ca_zip#47,ca_country#48,ca_gmt_offset#49,ca_location_type#50] parquet
               +- Filter isnotnull(cd_demo_sk#51)
                  +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#51,cd_gender#52,cd_marital_status#53,cd_education_status#54,cd_purchase_estimate#55,cd_credit_rating#56,cd_dep_count#57,cd_dep_employed_count#58,cd_dep_college_count#59] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[cd_gender#52 ASC NULLS FIRST,cd_marital_status#53 ASC NULLS FIRST,cd_education_status#54 ASC NULLS FIRST,cd_purchase_estimate#55 ASC NULLS FIRST,cd_credit_rating#56 ASC NULLS FIRST,cd_dep_count#57 ASC NULLS FIRST,cd_dep_employed_count#58 ASC NULLS FIRST,cd_dep_college_count#59 ASC NULLS FIRST], output=[cd_gender#52,cd_marital_status#53,cd_education_status#54,cnt1#0L,cd_purchase_estimate#55,cnt2#1L,cd_credit_rating#56,cnt3#2L,cd_dep_count#57,cnt4#3L,cd_dep_employed_count#58,cnt5#4L,cd_dep_college_count#59,cnt6#5L])
   +- HashAggregate(keys=[cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59], functions=[count(1)], output=[cd_gender#52, cd_marital_status#53, cd_education_status#54, cnt1#0L, cd_purchase_estimate#55, cnt2#1L, cd_credit_rating#56, cnt3#2L, cd_dep_count#57, cnt4#3L, cd_dep_employed_count#58, cnt5#4L, cd_dep_college_count#59, cnt6#5L])
      +- Exchange hashpartitioning(cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59, 200), ENSURE_REQUIREMENTS, [plan_id=204]
         +- HashAggregate(keys=[cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59], functions=[partial_count(1)], output=[cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59, count#245L])
            +- Project [cd_gender#52, cd_marital_status#53, cd_education_status#54, cd_purchase_estimate#55, cd_credit_rating#56, cd_dep_count#57, cd_dep_employed_count#58, cd_dep_college_count#59]
               +- BroadcastHashJoin [c_current_cdemo_sk#22], [cd_demo_sk#51], Inner, BuildRight, false
                  :- Project [c_current_cdemo_sk#22]
                  :  +- BroadcastHashJoin [c_current_addr_sk#24], [ca_address_sk#38], Inner, BuildRight, false
                  :     :- Project [c_current_cdemo_sk#22, c_current_addr_sk#24]
                  :     :  +- Filter (exists#242 OR exists#243)
                  :     :     +- SortMergeJoin [c_customer_sk#20], [cs_ship_customer_sk#155], ExistenceJoin(exists#243)
                  :     :        :- SortMergeJoin [c_customer_sk#20], [ws_bill_customer_sk#117], ExistenceJoin(exists#242)
                  :     :        :  :- SortMergeJoin [c_customer_sk#20], [ss_customer_sk#63], LeftSemi
                  :     :        :  :  :- Sort [c_customer_sk#20 ASC NULLS FIRST], false, 0
                  :     :        :  :  :  +- Exchange hashpartitioning(c_customer_sk#20, 200), ENSURE_REQUIREMENTS, [plan_id=171]
                  :     :        :  :  :     +- Filter (isnotnull(c_current_addr_sk#24) AND isnotnull(c_current_cdemo_sk#22))
                  :     :        :  :  :        +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#20,c_current_cdemo_sk#22,c_current_addr_sk#24] Batched: true, DataFilters: [isnotnull(c_current_addr_sk#24), isnotnull(c_current_cdemo_sk#22)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk)], ReadSchema: struct<c_customer_sk:int,c_current_cdemo_sk:int,c_current_addr_sk:int>
                  :     :        :  :  +- Sort [ss_customer_sk#63 ASC NULLS FIRST], false, 0
                  :     :        :  :     +- Exchange hashpartitioning(ss_customer_sk#63, 200), ENSURE_REQUIREMENTS, [plan_id=172]
                  :     :        :  :        +- Project [ss_customer_sk#63]
                  :     :        :  :           +- BroadcastHashJoin [ss_sold_date_sk#60], [d_date_sk#83], Inner, BuildRight, false
                  :     :        :  :              :- Filter isnotnull(ss_sold_date_sk#60)
                  :     :        :  :              :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#60,ss_customer_sk#63] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#60)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int>
                  :     :        :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=166]
                  :     :        :  :                 +- Project [d_date_sk#83]
                  :     :        :  :                    +- Filter (((((isnotnull(d_year#89) AND isnotnull(d_moy#91)) AND (d_year#89 = 2002)) AND (d_moy#91 >= 1)) AND (d_moy#91 <= 4)) AND isnotnull(d_date_sk#83))
                  :     :        :  :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#83,d_year#89,d_moy#91] Batched: true, DataFilters: [isnotnull(d_year#89), isnotnull(d_moy#91), (d_year#89 = 2002), (d_moy#91 >= 1), (d_moy#91 <= 4),..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2002), GreaterThanOrEqual(d_moy,1), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :        :  +- Sort [ws_bill_customer_sk#117 ASC NULLS FIRST], false, 0
                  :     :        :     +- Exchange hashpartitioning(ws_bill_customer_sk#117, 200), ENSURE_REQUIREMENTS, [plan_id=181]
                  :     :        :        +- Project [ws_bill_customer_sk#117]
                  :     :        :           +- BroadcastHashJoin [ws_sold_date_sk#113], [d_date_sk#186], Inner, BuildRight, false
                  :     :        :              :- Filter isnotnull(ws_sold_date_sk#113)
                  :     :        :              :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#113,ws_bill_customer_sk#117] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#113)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int>
                  :     :        :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=176]
                  :     :        :                 +- Project [d_date_sk#186]
                  :     :        :                    +- Filter (((((isnotnull(d_year#192) AND isnotnull(d_moy#194)) AND (d_year#192 = 2002)) AND (d_moy#194 >= 1)) AND (d_moy#194 <= 4)) AND isnotnull(d_date_sk#186))
                  :     :        :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#186,d_year#192,d_moy#194] Batched: true, DataFilters: [isnotnull(d_year#192), isnotnull(d_moy#194), (d_year#192 = 2002), (d_moy#194 >= 1), (d_moy#194 <..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2002), GreaterThanOrEqual(d_moy,1), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :        +- Sort [cs_ship_customer_sk#155 ASC NULLS FIRST], false, 0
                  :     :           +- Exchange hashpartitioning(cs_ship_customer_sk#155, 200), ENSURE_REQUIREMENTS, [plan_id=189]
                  :     :              +- Project [cs_ship_customer_sk#155]
                  :     :                 +- BroadcastHashJoin [cs_sold_date_sk#148], [d_date_sk#214], Inner, BuildRight, false
                  :     :                    :- Filter isnotnull(cs_sold_date_sk#148)
                  :     :                    :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#148,cs_ship_customer_sk#155] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#148)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ship_customer_sk:int>
                  :     :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=184]
                  :     :                       +- Project [d_date_sk#214]
                  :     :                          +- Filter (((((isnotnull(d_year#220) AND isnotnull(d_moy#222)) AND (d_year#220 = 2002)) AND (d_moy#222 >= 1)) AND (d_moy#222 <= 4)) AND isnotnull(d_date_sk#214))
                  :     :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#214,d_year#220,d_moy#222] Batched: true, DataFilters: [isnotnull(d_year#220), isnotnull(d_moy#222), (d_year#220 = 2002), (d_moy#222 >= 1), (d_moy#222 <..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2002), GreaterThanOrEqual(d_moy,1), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=195]
                  :        +- Project [ca_address_sk#38]
                  :           +- Filter (ca_county#45 IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County) AND isnotnull(ca_address_sk#38))
                  :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#38,ca_county#45] Batched: true, DataFilters: [ca_county#45 IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County), isn..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(ca_county, [Dona Ana County,Jefferson County,La Porte County,Rush County,Toole County]), IsNo..., ReadSchema: struct<ca_address_sk:int,ca_county:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=199]
                     +- Filter isnotnull(cd_demo_sk#51)
                        +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#51,cd_gender#52,cd_marital_status#53,cd_education_status#54,cd_purchase_estimate#55,cd_credit_rating#56,cd_dep_count#57,cd_dep_employed_count#58,cd_dep_college_count#59] Batched: true, DataFilters: [isnotnull(cd_demo_sk#51)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string,cd_pur...

Time taken: 3.221 seconds, Fetched 1 row(s)
