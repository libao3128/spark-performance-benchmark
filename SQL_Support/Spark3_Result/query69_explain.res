Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582208294
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['cd_gender ASC NULLS FIRST, 'cd_marital_status ASC NULLS FIRST, 'cd_education_status ASC NULLS FIRST, 'cd_purchase_estimate ASC NULLS FIRST, 'cd_credit_rating ASC NULLS FIRST], true
      +- 'Aggregate ['cd_gender, 'cd_marital_status, 'cd_education_status, 'cd_purchase_estimate, 'cd_credit_rating], ['cd_gender, 'cd_marital_status, 'cd_education_status, 'count(1) AS cnt1#0, 'cd_purchase_estimate, 'count(1) AS cnt2#1, 'cd_credit_rating, 'count(1) AS cnt3#2]
         +- 'Filter (((('c.c_current_addr_sk = 'ca.ca_address_sk) AND 'ca_state IN (KY,GA,NM)) AND ('cd_demo_sk = 'c.c_current_cdemo_sk)) AND (exists#3 [] AND (NOT exists#4 [] AND NOT exists#5 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ss_customer_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND (('d_year = 2001) AND (('d_moy >= 4) AND ('d_moy <= (4 + 2)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation [store_sales], [], false
            :  :        +- 'UnresolvedRelation [date_dim], [], false
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ws_bill_customer_sk) AND ('ws_sold_date_sk = 'd_date_sk)) AND (('d_year = 2001) AND (('d_moy >= 4) AND ('d_moy <= (4 + 2)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation [web_sales], [], false
            :  :        +- 'UnresolvedRelation [date_dim], [], false
            :  +- 'Project [*]
            :     +- 'Filter ((('c.c_customer_sk = 'cs_ship_customer_sk) AND ('cs_sold_date_sk = 'd_date_sk)) AND (('d_year = 2001) AND (('d_moy >= 4) AND ('d_moy <= (4 + 2)))))
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
cd_gender: string, cd_marital_status: string, cd_education_status: string, cnt1: bigint, cd_purchase_estimate: int, cnt2: bigint, cd_credit_rating: string, cnt3: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#46 ASC NULLS FIRST, cd_marital_status#47 ASC NULLS FIRST, cd_education_status#48 ASC NULLS FIRST, cd_purchase_estimate#49 ASC NULLS FIRST, cd_credit_rating#50 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50], [cd_gender#46, cd_marital_status#47, cd_education_status#48, count(1) AS cnt1#0L, cd_purchase_estimate#49, count(1) AS cnt2#1L, cd_credit_rating#50, count(1) AS cnt3#2L]
         +- Filter ((((c_current_addr_sk#18 = ca_address_sk#32) AND ca_state#40 IN (KY,GA,NM)) AND (cd_demo_sk#45 = c_current_cdemo_sk#16)) AND (exists#3 [c_customer_sk#14] AND (NOT exists#4 [c_customer_sk#14] AND NOT exists#5 [c_customer_sk#14])))
            :  :- Project [ss_sold_date_sk#54, ss_sold_time_sk#55, ss_item_sk#56, ss_customer_sk#57, ss_cdemo_sk#58, ss_hdemo_sk#59, ss_addr_sk#60, ss_store_sk#61, ss_promo_sk#62, ss_ticket_number#63, ss_quantity#64, ss_wholesale_cost#65, ss_list_price#66, ss_sales_price#67, ss_ext_discount_amt#68, ss_ext_sales_price#69, ss_ext_wholesale_cost#70, ss_ext_list_price#71, ss_ext_tax#72, ss_coupon_amt#73, ss_net_paid#74, ss_net_paid_inc_tax#75, ss_net_profit#76, d_date_sk#77, ... 27 more fields]
            :  :  +- Filter (((outer(c_customer_sk#14) = ss_customer_sk#57) AND (ss_sold_date_sk#54 = d_date_sk#77)) AND ((d_year#83 = 2001) AND ((d_moy#85 >= 4) AND (d_moy#85 <= (4 + 2)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias spark_catalog.tpcds.store_sales
            :  :        :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#54,ss_sold_time_sk#55,ss_item_sk#56,ss_customer_sk#57,ss_cdemo_sk#58,ss_hdemo_sk#59,ss_addr_sk#60,ss_store_sk#61,ss_promo_sk#62,ss_ticket_number#63,ss_quantity#64,ss_wholesale_cost#65,ss_list_price#66,ss_sales_price#67,ss_ext_discount_amt#68,ss_ext_sales_price#69,ss_ext_wholesale_cost#70,ss_ext_list_price#71,ss_ext_tax#72,ss_coupon_amt#73,ss_net_paid#74,ss_net_paid_inc_tax#75,ss_net_profit#76] parquet
            :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
            :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#77,d_date_id#78,d_date#79,d_month_seq#80,d_week_seq#81,d_quarter_seq#82,d_year#83,d_dow#84,d_moy#85,d_dom#86,d_qoy#87,d_fy_year#88,d_fy_quarter_seq#89,d_fy_week_seq#90,d_day_name#91,d_quarter_name#92,d_holiday#93,d_weekend#94,d_following_holiday#95,d_first_dom#96,d_last_dom#97,d_same_day_ly#98,d_same_day_lq#99,d_current_day#100,... 4 more fields] parquet
            :  :- Project [ws_sold_date_sk#107, ws_sold_time_sk#108, ws_ship_date_sk#109, ws_item_sk#110, ws_bill_customer_sk#111, ws_bill_cdemo_sk#112, ws_bill_hdemo_sk#113, ws_bill_addr_sk#114, ws_ship_customer_sk#115, ws_ship_cdemo_sk#116, ws_ship_hdemo_sk#117, ws_ship_addr_sk#118, ws_web_page_sk#119, ws_web_site_sk#120, ws_ship_mode_sk#121, ws_warehouse_sk#122, ws_promo_sk#123, ws_order_number#124, ws_quantity#125, ws_wholesale_cost#126, ws_list_price#127, ws_sales_price#128, ws_ext_discount_amt#129, ws_ext_sales_price#130, ... 38 more fields]
            :  :  +- Filter (((outer(c_customer_sk#14) = ws_bill_customer_sk#111) AND (ws_sold_date_sk#107 = d_date_sk#180)) AND ((d_year#186 = 2001) AND ((d_moy#188 >= 4) AND (d_moy#188 <= (4 + 2)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias spark_catalog.tpcds.web_sales
            :  :        :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#107,ws_sold_time_sk#108,ws_ship_date_sk#109,ws_item_sk#110,ws_bill_customer_sk#111,ws_bill_cdemo_sk#112,ws_bill_hdemo_sk#113,ws_bill_addr_sk#114,ws_ship_customer_sk#115,ws_ship_cdemo_sk#116,ws_ship_hdemo_sk#117,ws_ship_addr_sk#118,ws_web_page_sk#119,ws_web_site_sk#120,ws_ship_mode_sk#121,ws_warehouse_sk#122,ws_promo_sk#123,ws_order_number#124,ws_quantity#125,ws_wholesale_cost#126,ws_list_price#127,ws_sales_price#128,ws_ext_discount_amt#129,ws_ext_sales_price#130,... 10 more fields] parquet
            :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
            :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#180,d_date_id#181,d_date#182,d_month_seq#183,d_week_seq#184,d_quarter_seq#185,d_year#186,d_dow#187,d_moy#188,d_dom#189,d_qoy#190,d_fy_year#191,d_fy_quarter_seq#192,d_fy_week_seq#193,d_day_name#194,d_quarter_name#195,d_holiday#196,d_weekend#197,d_following_holiday#198,d_first_dom#199,d_last_dom#200,d_same_day_ly#201,d_same_day_lq#202,d_current_day#203,... 4 more fields] parquet
            :  +- Project [cs_sold_date_sk#142, cs_sold_time_sk#143, cs_ship_date_sk#144, cs_bill_customer_sk#145, cs_bill_cdemo_sk#146, cs_bill_hdemo_sk#147, cs_bill_addr_sk#148, cs_ship_customer_sk#149, cs_ship_cdemo_sk#150, cs_ship_hdemo_sk#151, cs_ship_addr_sk#152, cs_call_center_sk#153, cs_catalog_page_sk#154, cs_ship_mode_sk#155, cs_warehouse_sk#156, cs_item_sk#157, cs_promo_sk#158, cs_order_number#159, cs_quantity#160, cs_wholesale_cost#161, cs_list_price#162, cs_sales_price#163, cs_ext_discount_amt#164, cs_ext_sales_price#165, ... 38 more fields]
            :     +- Filter (((outer(c_customer_sk#14) = cs_ship_customer_sk#149) AND (cs_sold_date_sk#142 = d_date_sk#208)) AND ((d_year#214 = 2001) AND ((d_moy#216 >= 4) AND (d_moy#216 <= (4 + 2)))))
            :        +- Join Inner
            :           :- SubqueryAlias spark_catalog.tpcds.catalog_sales
            :           :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#142,cs_sold_time_sk#143,cs_ship_date_sk#144,cs_bill_customer_sk#145,cs_bill_cdemo_sk#146,cs_bill_hdemo_sk#147,cs_bill_addr_sk#148,cs_ship_customer_sk#149,cs_ship_cdemo_sk#150,cs_ship_hdemo_sk#151,cs_ship_addr_sk#152,cs_call_center_sk#153,cs_catalog_page_sk#154,cs_ship_mode_sk#155,cs_warehouse_sk#156,cs_item_sk#157,cs_promo_sk#158,cs_order_number#159,cs_quantity#160,cs_wholesale_cost#161,cs_list_price#162,cs_sales_price#163,cs_ext_discount_amt#164,cs_ext_sales_price#165,... 10 more fields] parquet
            :           +- SubqueryAlias spark_catalog.tpcds.date_dim
            :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#208,d_date_id#209,d_date#210,d_month_seq#211,d_week_seq#212,d_quarter_seq#213,d_year#214,d_dow#215,d_moy#216,d_dom#217,d_qoy#218,d_fy_year#219,d_fy_quarter_seq#220,d_fy_week_seq#221,d_day_name#222,d_quarter_name#223,d_holiday#224,d_weekend#225,d_following_holiday#226,d_first_dom#227,d_last_dom#228,d_same_day_ly#229,d_same_day_lq#230,d_current_day#231,... 4 more fields] parquet
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias c
               :  :  +- SubqueryAlias spark_catalog.tpcds.customer
               :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :  +- SubqueryAlias ca
               :     +- SubqueryAlias spark_catalog.tpcds.customer_address
               :        +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#32,ca_address_id#33,ca_street_number#34,ca_street_name#35,ca_street_type#36,ca_suite_number#37,ca_city#38,ca_county#39,ca_state#40,ca_zip#41,ca_country#42,ca_gmt_offset#43,ca_location_type#44] parquet
               +- SubqueryAlias spark_catalog.tpcds.customer_demographics
                  +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#45,cd_gender#46,cd_marital_status#47,cd_education_status#48,cd_purchase_estimate#49,cd_credit_rating#50,cd_dep_count#51,cd_dep_employed_count#52,cd_dep_college_count#53] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#46 ASC NULLS FIRST, cd_marital_status#47 ASC NULLS FIRST, cd_education_status#48 ASC NULLS FIRST, cd_purchase_estimate#49 ASC NULLS FIRST, cd_credit_rating#50 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50], [cd_gender#46, cd_marital_status#47, cd_education_status#48, count(1) AS cnt1#0L, cd_purchase_estimate#49, count(1) AS cnt2#1L, cd_credit_rating#50, count(1) AS cnt3#2L]
         +- Project [cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50]
            +- Join Inner, (cd_demo_sk#45 = c_current_cdemo_sk#16)
               :- Project [c_current_cdemo_sk#16]
               :  +- Join Inner, (c_current_addr_sk#18 = ca_address_sk#32)
               :     :- Project [c_current_cdemo_sk#16, c_current_addr_sk#18]
               :     :  +- Join LeftAnti, (c_customer_sk#14 = cs_ship_customer_sk#149)
               :     :     :- Join LeftAnti, (c_customer_sk#14 = ws_bill_customer_sk#111)
               :     :     :  :- Join LeftSemi, (c_customer_sk#14 = ss_customer_sk#57)
               :     :     :  :  :- Project [c_customer_sk#14, c_current_cdemo_sk#16, c_current_addr_sk#18]
               :     :     :  :  :  +- Filter (isnotnull(c_current_addr_sk#18) AND isnotnull(c_current_cdemo_sk#16))
               :     :     :  :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :     :     :  :  +- Project [ss_customer_sk#57]
               :     :     :  :     +- Join Inner, (ss_sold_date_sk#54 = d_date_sk#77)
               :     :     :  :        :- Project [ss_sold_date_sk#54, ss_customer_sk#57]
               :     :     :  :        :  +- Filter isnotnull(ss_sold_date_sk#54)
               :     :     :  :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#54,ss_sold_time_sk#55,ss_item_sk#56,ss_customer_sk#57,ss_cdemo_sk#58,ss_hdemo_sk#59,ss_addr_sk#60,ss_store_sk#61,ss_promo_sk#62,ss_ticket_number#63,ss_quantity#64,ss_wholesale_cost#65,ss_list_price#66,ss_sales_price#67,ss_ext_discount_amt#68,ss_ext_sales_price#69,ss_ext_wholesale_cost#70,ss_ext_list_price#71,ss_ext_tax#72,ss_coupon_amt#73,ss_net_paid#74,ss_net_paid_inc_tax#75,ss_net_profit#76] parquet
               :     :     :  :        +- Project [d_date_sk#77]
               :     :     :  :           +- Filter (((isnotnull(d_year#83) AND isnotnull(d_moy#85)) AND (((d_year#83 = 2001) AND (d_moy#85 >= 4)) AND (d_moy#85 <= 6))) AND isnotnull(d_date_sk#77))
               :     :     :  :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#77,d_date_id#78,d_date#79,d_month_seq#80,d_week_seq#81,d_quarter_seq#82,d_year#83,d_dow#84,d_moy#85,d_dom#86,d_qoy#87,d_fy_year#88,d_fy_quarter_seq#89,d_fy_week_seq#90,d_day_name#91,d_quarter_name#92,d_holiday#93,d_weekend#94,d_following_holiday#95,d_first_dom#96,d_last_dom#97,d_same_day_ly#98,d_same_day_lq#99,d_current_day#100,... 4 more fields] parquet
               :     :     :  +- Project [ws_bill_customer_sk#111]
               :     :     :     +- Join Inner, (ws_sold_date_sk#107 = d_date_sk#180)
               :     :     :        :- Project [ws_sold_date_sk#107, ws_bill_customer_sk#111]
               :     :     :        :  +- Filter isnotnull(ws_sold_date_sk#107)
               :     :     :        :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#107,ws_sold_time_sk#108,ws_ship_date_sk#109,ws_item_sk#110,ws_bill_customer_sk#111,ws_bill_cdemo_sk#112,ws_bill_hdemo_sk#113,ws_bill_addr_sk#114,ws_ship_customer_sk#115,ws_ship_cdemo_sk#116,ws_ship_hdemo_sk#117,ws_ship_addr_sk#118,ws_web_page_sk#119,ws_web_site_sk#120,ws_ship_mode_sk#121,ws_warehouse_sk#122,ws_promo_sk#123,ws_order_number#124,ws_quantity#125,ws_wholesale_cost#126,ws_list_price#127,ws_sales_price#128,ws_ext_discount_amt#129,ws_ext_sales_price#130,... 10 more fields] parquet
               :     :     :        +- Project [d_date_sk#180]
               :     :     :           +- Filter (((isnotnull(d_year#186) AND isnotnull(d_moy#188)) AND (((d_year#186 = 2001) AND (d_moy#188 >= 4)) AND (d_moy#188 <= 6))) AND isnotnull(d_date_sk#180))
               :     :     :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#180,d_date_id#181,d_date#182,d_month_seq#183,d_week_seq#184,d_quarter_seq#185,d_year#186,d_dow#187,d_moy#188,d_dom#189,d_qoy#190,d_fy_year#191,d_fy_quarter_seq#192,d_fy_week_seq#193,d_day_name#194,d_quarter_name#195,d_holiday#196,d_weekend#197,d_following_holiday#198,d_first_dom#199,d_last_dom#200,d_same_day_ly#201,d_same_day_lq#202,d_current_day#203,... 4 more fields] parquet
               :     :     +- Project [cs_ship_customer_sk#149]
               :     :        +- Join Inner, (cs_sold_date_sk#142 = d_date_sk#208)
               :     :           :- Project [cs_sold_date_sk#142, cs_ship_customer_sk#149]
               :     :           :  +- Filter isnotnull(cs_sold_date_sk#142)
               :     :           :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#142,cs_sold_time_sk#143,cs_ship_date_sk#144,cs_bill_customer_sk#145,cs_bill_cdemo_sk#146,cs_bill_hdemo_sk#147,cs_bill_addr_sk#148,cs_ship_customer_sk#149,cs_ship_cdemo_sk#150,cs_ship_hdemo_sk#151,cs_ship_addr_sk#152,cs_call_center_sk#153,cs_catalog_page_sk#154,cs_ship_mode_sk#155,cs_warehouse_sk#156,cs_item_sk#157,cs_promo_sk#158,cs_order_number#159,cs_quantity#160,cs_wholesale_cost#161,cs_list_price#162,cs_sales_price#163,cs_ext_discount_amt#164,cs_ext_sales_price#165,... 10 more fields] parquet
               :     :           +- Project [d_date_sk#208]
               :     :              +- Filter (((isnotnull(d_year#214) AND isnotnull(d_moy#216)) AND (((d_year#214 = 2001) AND (d_moy#216 >= 4)) AND (d_moy#216 <= 6))) AND isnotnull(d_date_sk#208))
               :     :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#208,d_date_id#209,d_date#210,d_month_seq#211,d_week_seq#212,d_quarter_seq#213,d_year#214,d_dow#215,d_moy#216,d_dom#217,d_qoy#218,d_fy_year#219,d_fy_quarter_seq#220,d_fy_week_seq#221,d_day_name#222,d_quarter_name#223,d_holiday#224,d_weekend#225,d_following_holiday#226,d_first_dom#227,d_last_dom#228,d_same_day_ly#229,d_same_day_lq#230,d_current_day#231,... 4 more fields] parquet
               :     +- Project [ca_address_sk#32]
               :        +- Filter (ca_state#40 IN (KY,GA,NM) AND isnotnull(ca_address_sk#32))
               :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#32,ca_address_id#33,ca_street_number#34,ca_street_name#35,ca_street_type#36,ca_suite_number#37,ca_city#38,ca_county#39,ca_state#40,ca_zip#41,ca_country#42,ca_gmt_offset#43,ca_location_type#44] parquet
               +- Project [cd_demo_sk#45, cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50]
                  +- Filter isnotnull(cd_demo_sk#45)
                     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#45,cd_gender#46,cd_marital_status#47,cd_education_status#48,cd_purchase_estimate#49,cd_credit_rating#50,cd_dep_count#51,cd_dep_employed_count#52,cd_dep_college_count#53] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[cd_gender#46 ASC NULLS FIRST,cd_marital_status#47 ASC NULLS FIRST,cd_education_status#48 ASC NULLS FIRST,cd_purchase_estimate#49 ASC NULLS FIRST,cd_credit_rating#50 ASC NULLS FIRST], output=[cd_gender#46,cd_marital_status#47,cd_education_status#48,cnt1#0L,cd_purchase_estimate#49,cnt2#1L,cd_credit_rating#50,cnt3#2L])
   +- HashAggregate(keys=[cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50], functions=[count(1)], output=[cd_gender#46, cd_marital_status#47, cd_education_status#48, cnt1#0L, cd_purchase_estimate#49, cnt2#1L, cd_credit_rating#50, cnt3#2L])
      +- Exchange hashpartitioning(cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50, 200), ENSURE_REQUIREMENTS, [plan_id=198]
         +- HashAggregate(keys=[cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50], functions=[partial_count(1)], output=[cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50, count#237L])
            +- Project [cd_gender#46, cd_marital_status#47, cd_education_status#48, cd_purchase_estimate#49, cd_credit_rating#50]
               +- BroadcastHashJoin [c_current_cdemo_sk#16], [cd_demo_sk#45], Inner, BuildRight, false
                  :- Project [c_current_cdemo_sk#16]
                  :  +- BroadcastHashJoin [c_current_addr_sk#18], [ca_address_sk#32], Inner, BuildRight, false
                  :     :- Project [c_current_cdemo_sk#16, c_current_addr_sk#18]
                  :     :  +- SortMergeJoin [c_customer_sk#14], [cs_ship_customer_sk#149], LeftAnti
                  :     :     :- SortMergeJoin [c_customer_sk#14], [ws_bill_customer_sk#111], LeftAnti
                  :     :     :  :- SortMergeJoin [c_customer_sk#14], [ss_customer_sk#57], LeftSemi
                  :     :     :  :  :- Sort [c_customer_sk#14 ASC NULLS FIRST], false, 0
                  :     :     :  :  :  +- Exchange hashpartitioning(c_customer_sk#14, 200), ENSURE_REQUIREMENTS, [plan_id=166]
                  :     :     :  :  :     +- Filter (isnotnull(c_current_addr_sk#18) AND isnotnull(c_current_cdemo_sk#16))
                  :     :     :  :  :        +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#14,c_current_cdemo_sk#16,c_current_addr_sk#18] Batched: true, DataFilters: [isnotnull(c_current_addr_sk#18), isnotnull(c_current_cdemo_sk#16)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk)], ReadSchema: struct<c_customer_sk:int,c_current_cdemo_sk:int,c_current_addr_sk:int>
                  :     :     :  :  +- Sort [ss_customer_sk#57 ASC NULLS FIRST], false, 0
                  :     :     :  :     +- Exchange hashpartitioning(ss_customer_sk#57, 200), ENSURE_REQUIREMENTS, [plan_id=167]
                  :     :     :  :        +- Project [ss_customer_sk#57]
                  :     :     :  :           +- BroadcastHashJoin [ss_sold_date_sk#54], [d_date_sk#77], Inner, BuildRight, false
                  :     :     :  :              :- Filter isnotnull(ss_sold_date_sk#54)
                  :     :     :  :              :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#54,ss_customer_sk#57] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#54)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int>
                  :     :     :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=161]
                  :     :     :  :                 +- Project [d_date_sk#77]
                  :     :     :  :                    +- Filter (((((isnotnull(d_year#83) AND isnotnull(d_moy#85)) AND (d_year#83 = 2001)) AND (d_moy#85 >= 4)) AND (d_moy#85 <= 6)) AND isnotnull(d_date_sk#77))
                  :     :     :  :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#77,d_year#83,d_moy#85] Batched: true, DataFilters: [isnotnull(d_year#83), isnotnull(d_moy#85), (d_year#83 = 2001), (d_moy#85 >= 4), (d_moy#85 <= 6),..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), GreaterThanOrEqual(d_moy,4), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :     :  +- Sort [ws_bill_customer_sk#111 ASC NULLS FIRST], false, 0
                  :     :     :     +- Exchange hashpartitioning(ws_bill_customer_sk#111, 200), ENSURE_REQUIREMENTS, [plan_id=176]
                  :     :     :        +- Project [ws_bill_customer_sk#111]
                  :     :     :           +- BroadcastHashJoin [ws_sold_date_sk#107], [d_date_sk#180], Inner, BuildRight, false
                  :     :     :              :- Filter isnotnull(ws_sold_date_sk#107)
                  :     :     :              :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#107,ws_bill_customer_sk#111] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#107)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int>
                  :     :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=171]
                  :     :     :                 +- Project [d_date_sk#180]
                  :     :     :                    +- Filter (((((isnotnull(d_year#186) AND isnotnull(d_moy#188)) AND (d_year#186 = 2001)) AND (d_moy#188 >= 4)) AND (d_moy#188 <= 6)) AND isnotnull(d_date_sk#180))
                  :     :     :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#180,d_year#186,d_moy#188] Batched: true, DataFilters: [isnotnull(d_year#186), isnotnull(d_moy#188), (d_year#186 = 2001), (d_moy#188 >= 4), (d_moy#188 <..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), GreaterThanOrEqual(d_moy,4), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :     +- Sort [cs_ship_customer_sk#149 ASC NULLS FIRST], false, 0
                  :     :        +- Exchange hashpartitioning(cs_ship_customer_sk#149, 200), ENSURE_REQUIREMENTS, [plan_id=184]
                  :     :           +- Project [cs_ship_customer_sk#149]
                  :     :              +- BroadcastHashJoin [cs_sold_date_sk#142], [d_date_sk#208], Inner, BuildRight, false
                  :     :                 :- Filter isnotnull(cs_sold_date_sk#142)
                  :     :                 :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#142,cs_ship_customer_sk#149] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#142)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ship_customer_sk:int>
                  :     :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=179]
                  :     :                    +- Project [d_date_sk#208]
                  :     :                       +- Filter (((((isnotnull(d_year#214) AND isnotnull(d_moy#216)) AND (d_year#214 = 2001)) AND (d_moy#216 >= 4)) AND (d_moy#216 <= 6)) AND isnotnull(d_date_sk#208))
                  :     :                          +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#208,d_year#214,d_moy#216] Batched: true, DataFilters: [isnotnull(d_year#214), isnotnull(d_moy#216), (d_year#214 = 2001), (d_moy#216 >= 4), (d_moy#216 <..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), GreaterThanOrEqual(d_moy,4), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=189]
                  :        +- Project [ca_address_sk#32]
                  :           +- Filter (ca_state#40 IN (KY,GA,NM) AND isnotnull(ca_address_sk#32))
                  :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#32,ca_state#40] Batched: true, DataFilters: [ca_state#40 IN (KY,GA,NM), isnotnull(ca_address_sk#32)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(ca_state, [GA,KY,NM]), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=193]
                     +- Filter isnotnull(cd_demo_sk#45)
                        +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#45,cd_gender#46,cd_marital_status#47,cd_education_status#48,cd_purchase_estimate#49,cd_credit_rating#50] Batched: true, DataFilters: [isnotnull(cd_demo_sk#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string,cd_pur...

Time taken: 3.056 seconds, Fetched 1 row(s)
