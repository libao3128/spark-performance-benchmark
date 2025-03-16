Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582863807
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('r_reason_desc, 1, 20) ASC NULLS FIRST, 'avg('ws_quantity) ASC NULLS FIRST, 'avg('wr_refunded_cash) ASC NULLS FIRST, 'avg('wr_fee) ASC NULLS FIRST], true
      +- 'Aggregate ['r_reason_desc], [unresolvedalias('substr('r_reason_desc, 1, 20), None), unresolvedalias('avg('ws_quantity), None), unresolvedalias('avg('wr_refunded_cash), None), unresolvedalias('avg('wr_fee), None)]
         +- 'Filter ((((('ws_web_page_sk = 'wp_web_page_sk) AND ('ws_item_sk = 'wr_item_sk)) AND ('ws_order_number = 'wr_order_number)) AND ((('ws_sold_date_sk = 'd_date_sk) AND ('d_year = 2000)) AND ('cd1.cd_demo_sk = 'wr_refunded_cdemo_sk))) AND (((('cd2.cd_demo_sk = 'wr_returning_cdemo_sk) AND ('ca_address_sk = 'wr_refunded_addr_sk)) AND ('r_reason_sk = 'wr_reason_sk)) AND ((((((('cd1.cd_marital_status = M) AND ('cd1.cd_marital_status = 'cd2.cd_marital_status)) AND ('cd1.cd_education_status = Advanced Degree)) AND (('cd1.cd_education_status = 'cd2.cd_education_status) AND (('ws_sales_price >= 100.00) AND ('ws_sales_price <= 150.00)))) OR (((('cd1.cd_marital_status = S) AND ('cd1.cd_marital_status = 'cd2.cd_marital_status)) AND ('cd1.cd_education_status = College)) AND (('cd1.cd_education_status = 'cd2.cd_education_status) AND (('ws_sales_price >= 50.00) AND ('ws_sales_price <= 100.00))))) OR (((('cd1.cd_marital_status = W) AND ('cd1.cd_marital_status = 'cd2.cd_marital_status)) AND ('cd1.cd_education_status = 2 yr Degree)) AND (('cd1.cd_education_status = 'cd2.cd_education_status) AND (('ws_sales_price >= 150.00) AND ('ws_sales_price <= 200.00))))) AND ((((('ca_country = United States) AND 'ca_state IN (IN,OH,NJ)) AND (('ws_net_profit >= 100) AND ('ws_net_profit <= 200))) OR ((('ca_country = United States) AND 'ca_state IN (WI,CT,KY)) AND (('ws_net_profit >= 150) AND ('ws_net_profit <= 300)))) OR ((('ca_country = United States) AND 'ca_state IN (LA,IA,AR)) AND (('ws_net_profit >= 50) AND ('ws_net_profit <= 250)))))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :  :- 'UnresolvedRelation [web_sales], [], false
               :  :  :  :  :  :  +- 'UnresolvedRelation [web_returns], [], false
               :  :  :  :  :  +- 'UnresolvedRelation [web_page], [], false
               :  :  :  :  +- 'SubqueryAlias cd1
               :  :  :  :     +- 'UnresolvedRelation [customer_demographics], [], false
               :  :  :  +- 'SubqueryAlias cd2
               :  :  :     +- 'UnresolvedRelation [customer_demographics], [], false
               :  :  +- 'UnresolvedRelation [customer_address], [], false
               :  +- 'UnresolvedRelation [date_dim], [], false
               +- 'UnresolvedRelation [reason], [], false

== Analyzed Logical Plan ==
substr(r_reason_desc, 1, 20): string, avg(ws_quantity): double, avg(wr_refunded_cash): double, avg(wr_fee): double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substr(r_reason_desc, 1, 20)#150 ASC NULLS FIRST, avg(ws_quantity)#151 ASC NULLS FIRST, avg(wr_refunded_cash)#152 ASC NULLS FIRST, avg(wr_fee)#153 ASC NULLS FIRST], true
      +- Aggregate [r_reason_desc#129], [substr(r_reason_desc#129, 1, 20) AS substr(r_reason_desc, 1, 20)#150, avg(ws_quantity#23) AS avg(ws_quantity)#151, avg(wr_refunded_cash#59) AS avg(wr_refunded_cash)#152, avg(wr_fee#57) AS avg(wr_fee)#153]
         +- Filter (((((ws_web_page_sk#17 = wp_web_page_sk#63) AND (ws_item_sk#8 = wr_item_sk#41)) AND (ws_order_number#22 = wr_order_number#52)) AND (((ws_sold_date_sk#5 = d_date_sk#99) AND (d_year#105 = 2000)) AND (cd_demo_sk#77 = wr_refunded_cdemo_sk#43))) AND ((((cd_demo_sk#130 = wr_returning_cdemo_sk#47) AND (ca_address_sk#86 = wr_refunded_addr_sk#45)) AND (r_reason_sk#127 = wr_reason_sk#51)) AND (((((((cd_marital_status#79 = M) AND (cd_marital_status#79 = cd_marital_status#132)) AND (cd_education_status#80 = Advanced Degree)) AND ((cd_education_status#80 = cd_education_status#133) AND ((ws_sales_price#26 >= cast(100.00 as double)) AND (ws_sales_price#26 <= cast(150.00 as double))))) OR ((((cd_marital_status#79 = S) AND (cd_marital_status#79 = cd_marital_status#132)) AND (cd_education_status#80 = College)) AND ((cd_education_status#80 = cd_education_status#133) AND ((ws_sales_price#26 >= cast(50.00 as double)) AND (ws_sales_price#26 <= cast(100.00 as double)))))) OR ((((cd_marital_status#79 = W) AND (cd_marital_status#79 = cd_marital_status#132)) AND (cd_education_status#80 = 2 yr Degree)) AND ((cd_education_status#80 = cd_education_status#133) AND ((ws_sales_price#26 >= cast(150.00 as double)) AND (ws_sales_price#26 <= cast(200.00 as double)))))) AND (((((ca_country#96 = United States) AND ca_state#94 IN (IN,OH,NJ)) AND ((ws_net_profit#38 >= cast(100 as double)) AND (ws_net_profit#38 <= cast(200 as double)))) OR (((ca_country#96 = United States) AND ca_state#94 IN (WI,CT,KY)) AND ((ws_net_profit#38 >= cast(150 as double)) AND (ws_net_profit#38 <= cast(300 as double))))) OR (((ca_country#96 = United States) AND ca_state#94 IN (LA,IA,AR)) AND ((ws_net_profit#38 >= cast(50 as double)) AND (ws_net_profit#38 <= cast(250 as double))))))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- Join Inner
               :  :  :  :  :- Join Inner
               :  :  :  :  :  :- Join Inner
               :  :  :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
               :  :  :  :  :  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#5,ws_sold_time_sk#6,ws_ship_date_sk#7,ws_item_sk#8,ws_bill_customer_sk#9,ws_bill_cdemo_sk#10,ws_bill_hdemo_sk#11,ws_bill_addr_sk#12,ws_ship_customer_sk#13,ws_ship_cdemo_sk#14,ws_ship_hdemo_sk#15,ws_ship_addr_sk#16,ws_web_page_sk#17,ws_web_site_sk#18,ws_ship_mode_sk#19,ws_warehouse_sk#20,ws_promo_sk#21,ws_order_number#22,ws_quantity#23,ws_wholesale_cost#24,ws_list_price#25,ws_sales_price#26,ws_ext_discount_amt#27,ws_ext_sales_price#28,... 10 more fields] parquet
               :  :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.web_returns
               :  :  :  :  :  :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#39,wr_returned_time_sk#40,wr_item_sk#41,wr_refunded_customer_sk#42,wr_refunded_cdemo_sk#43,wr_refunded_hdemo_sk#44,wr_refunded_addr_sk#45,wr_returning_customer_sk#46,wr_returning_cdemo_sk#47,wr_returning_hdemo_sk#48,wr_returning_addr_sk#49,wr_web_page_sk#50,wr_reason_sk#51,wr_order_number#52,wr_return_quantity#53,wr_return_amt#54,wr_return_tax#55,wr_return_amt_inc_tax#56,wr_fee#57,wr_return_ship_cost#58,wr_refunded_cash#59,wr_reversed_charge#60,wr_account_credit#61,wr_net_loss#62] parquet
               :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.web_page
               :  :  :  :  :     +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#63,wp_web_page_id#64,wp_rec_start_date#65,wp_rec_end_date#66,wp_creation_date_sk#67,wp_access_date_sk#68,wp_autogen_flag#69,wp_customer_sk#70,wp_url#71,wp_type#72,wp_char_count#73,wp_link_count#74,wp_image_count#75,wp_max_ad_count#76] parquet
               :  :  :  :  +- SubqueryAlias cd1
               :  :  :  :     +- SubqueryAlias spark_catalog.tpcds.customer_demographics
               :  :  :  :        +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#77,cd_gender#78,cd_marital_status#79,cd_education_status#80,cd_purchase_estimate#81,cd_credit_rating#82,cd_dep_count#83,cd_dep_employed_count#84,cd_dep_college_count#85] parquet
               :  :  :  +- SubqueryAlias cd2
               :  :  :     +- SubqueryAlias spark_catalog.tpcds.customer_demographics
               :  :  :        +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#130,cd_gender#131,cd_marital_status#132,cd_education_status#133,cd_purchase_estimate#134,cd_credit_rating#135,cd_dep_count#136,cd_dep_employed_count#137,cd_dep_college_count#138] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
               :  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#86,ca_address_id#87,ca_street_number#88,ca_street_name#89,ca_street_type#90,ca_suite_number#91,ca_city#92,ca_county#93,ca_state#94,ca_zip#95,ca_country#96,ca_gmt_offset#97,ca_location_type#98] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#99,d_date_id#100,d_date#101,d_month_seq#102,d_week_seq#103,d_quarter_seq#104,d_year#105,d_dow#106,d_moy#107,d_dom#108,d_qoy#109,d_fy_year#110,d_fy_quarter_seq#111,d_fy_week_seq#112,d_day_name#113,d_quarter_name#114,d_holiday#115,d_weekend#116,d_following_holiday#117,d_first_dom#118,d_last_dom#119,d_same_day_ly#120,d_same_day_lq#121,d_current_day#122,... 4 more fields] parquet
               +- SubqueryAlias spark_catalog.tpcds.reason
                  +- Relation spark_catalog.tpcds.reason[r_reason_sk#127,r_reason_id#128,r_reason_desc#129] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substr(r_reason_desc, 1, 20)#150 ASC NULLS FIRST, avg(ws_quantity)#151 ASC NULLS FIRST, avg(wr_refunded_cash)#152 ASC NULLS FIRST, avg(wr_fee)#153 ASC NULLS FIRST], true
      +- Aggregate [r_reason_desc#129], [substr(r_reason_desc#129, 1, 20) AS substr(r_reason_desc, 1, 20)#150, avg(ws_quantity#23) AS avg(ws_quantity)#151, avg(wr_refunded_cash#59) AS avg(wr_refunded_cash)#152, avg(wr_fee#57) AS avg(wr_fee)#153]
         +- Project [ws_quantity#23, wr_fee#57, wr_refunded_cash#59, r_reason_desc#129]
            +- Join Inner, (r_reason_sk#127 = wr_reason_sk#51)
               :- Project [ws_quantity#23, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
               :  +- Join Inner, (ws_sold_date_sk#5 = d_date_sk#99)
               :     :- Project [ws_sold_date_sk#5, ws_quantity#23, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
               :     :  +- Join Inner, ((ca_address_sk#86 = wr_refunded_addr_sk#45) AND ((((ca_state#94 IN (IN,OH,NJ) AND (ws_net_profit#38 >= 100.0)) AND (ws_net_profit#38 <= 200.0)) OR ((ca_state#94 IN (WI,CT,KY) AND (ws_net_profit#38 >= 150.0)) AND (ws_net_profit#38 <= 300.0))) OR ((ca_state#94 IN (LA,IA,AR) AND (ws_net_profit#38 >= 50.0)) AND (ws_net_profit#38 <= 250.0))))
               :     :     :- Project [ws_sold_date_sk#5, ws_quantity#23, ws_net_profit#38, wr_refunded_addr_sk#45, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
               :     :     :  +- Join Inner, (((cd_demo_sk#130 = wr_returning_cdemo_sk#47) AND (cd_marital_status#79 = cd_marital_status#132)) AND (cd_education_status#80 = cd_education_status#133))
               :     :     :     :- Project [ws_sold_date_sk#5, ws_quantity#23, ws_net_profit#38, wr_refunded_addr_sk#45, wr_returning_cdemo_sk#47, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59, cd_marital_status#79, cd_education_status#80]
               :     :     :     :  +- Join Inner, (((((((cd_marital_status#79 = M) AND (cd_education_status#80 = Advanced Degree)) AND (ws_sales_price#26 >= 100.0)) AND (ws_sales_price#26 <= 150.0)) OR ((((cd_marital_status#79 = S) AND (cd_education_status#80 = College)) AND (ws_sales_price#26 >= 50.0)) AND (ws_sales_price#26 <= 100.0))) OR ((((cd_marital_status#79 = W) AND (cd_education_status#80 = 2 yr Degree)) AND (ws_sales_price#26 >= 150.0)) AND (ws_sales_price#26 <= 200.0))) AND (cd_demo_sk#77 = wr_refunded_cdemo_sk#43))
               :     :     :     :     :- Project [ws_sold_date_sk#5, ws_quantity#23, ws_sales_price#26, ws_net_profit#38, wr_refunded_cdemo_sk#43, wr_refunded_addr_sk#45, wr_returning_cdemo_sk#47, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
               :     :     :     :     :  +- Join Inner, (ws_web_page_sk#17 = wp_web_page_sk#63)
               :     :     :     :     :     :- Project [ws_sold_date_sk#5, ws_web_page_sk#17, ws_quantity#23, ws_sales_price#26, ws_net_profit#38, wr_refunded_cdemo_sk#43, wr_refunded_addr_sk#45, wr_returning_cdemo_sk#47, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
               :     :     :     :     :     :  +- Join Inner, ((ws_item_sk#8 = wr_item_sk#41) AND (ws_order_number#22 = wr_order_number#52))
               :     :     :     :     :     :     :- Project [ws_sold_date_sk#5, ws_item_sk#8, ws_web_page_sk#17, ws_order_number#22, ws_quantity#23, ws_sales_price#26, ws_net_profit#38]
               :     :     :     :     :     :     :  +- Filter ((((isnotnull(ws_item_sk#8) AND isnotnull(ws_order_number#22)) AND isnotnull(ws_web_page_sk#17)) AND isnotnull(ws_sold_date_sk#5)) AND (((((ws_sales_price#26 >= 100.0) AND (ws_sales_price#26 <= 150.0)) OR ((ws_sales_price#26 >= 50.0) AND (ws_sales_price#26 <= 100.0))) OR ((ws_sales_price#26 >= 150.0) AND (ws_sales_price#26 <= 200.0))) AND ((((ws_net_profit#38 >= 100.0) AND (ws_net_profit#38 <= 200.0)) OR ((ws_net_profit#38 >= 150.0) AND (ws_net_profit#38 <= 300.0))) OR ((ws_net_profit#38 >= 50.0) AND (ws_net_profit#38 <= 250.0)))))
               :     :     :     :     :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#5,ws_sold_time_sk#6,ws_ship_date_sk#7,ws_item_sk#8,ws_bill_customer_sk#9,ws_bill_cdemo_sk#10,ws_bill_hdemo_sk#11,ws_bill_addr_sk#12,ws_ship_customer_sk#13,ws_ship_cdemo_sk#14,ws_ship_hdemo_sk#15,ws_ship_addr_sk#16,ws_web_page_sk#17,ws_web_site_sk#18,ws_ship_mode_sk#19,ws_warehouse_sk#20,ws_promo_sk#21,ws_order_number#22,ws_quantity#23,ws_wholesale_cost#24,ws_list_price#25,ws_sales_price#26,ws_ext_discount_amt#27,ws_ext_sales_price#28,... 10 more fields] parquet
               :     :     :     :     :     :     +- Project [wr_item_sk#41, wr_refunded_cdemo_sk#43, wr_refunded_addr_sk#45, wr_returning_cdemo_sk#47, wr_reason_sk#51, wr_order_number#52, wr_fee#57, wr_refunded_cash#59]
               :     :     :     :     :     :        +- Filter (((isnotnull(wr_item_sk#41) AND isnotnull(wr_order_number#52)) AND isnotnull(wr_refunded_cdemo_sk#43)) AND ((isnotnull(wr_returning_cdemo_sk#47) AND isnotnull(wr_refunded_addr_sk#45)) AND isnotnull(wr_reason_sk#51)))
               :     :     :     :     :     :           +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#39,wr_returned_time_sk#40,wr_item_sk#41,wr_refunded_customer_sk#42,wr_refunded_cdemo_sk#43,wr_refunded_hdemo_sk#44,wr_refunded_addr_sk#45,wr_returning_customer_sk#46,wr_returning_cdemo_sk#47,wr_returning_hdemo_sk#48,wr_returning_addr_sk#49,wr_web_page_sk#50,wr_reason_sk#51,wr_order_number#52,wr_return_quantity#53,wr_return_amt#54,wr_return_tax#55,wr_return_amt_inc_tax#56,wr_fee#57,wr_return_ship_cost#58,wr_refunded_cash#59,wr_reversed_charge#60,wr_account_credit#61,wr_net_loss#62] parquet
               :     :     :     :     :     +- Project [wp_web_page_sk#63]
               :     :     :     :     :        +- Filter isnotnull(wp_web_page_sk#63)
               :     :     :     :     :           +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#63,wp_web_page_id#64,wp_rec_start_date#65,wp_rec_end_date#66,wp_creation_date_sk#67,wp_access_date_sk#68,wp_autogen_flag#69,wp_customer_sk#70,wp_url#71,wp_type#72,wp_char_count#73,wp_link_count#74,wp_image_count#75,wp_max_ad_count#76] parquet
               :     :     :     :     +- Project [cd_demo_sk#77, cd_marital_status#79, cd_education_status#80]
               :     :     :     :        +- Filter ((isnotnull(cd_demo_sk#77) AND (isnotnull(cd_marital_status#79) AND isnotnull(cd_education_status#80))) AND ((((cd_marital_status#79 = M) AND (cd_education_status#80 = Advanced Degree)) OR ((cd_marital_status#79 = S) AND (cd_education_status#80 = College))) OR ((cd_marital_status#79 = W) AND (cd_education_status#80 = 2 yr Degree))))
               :     :     :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#77,cd_gender#78,cd_marital_status#79,cd_education_status#80,cd_purchase_estimate#81,cd_credit_rating#82,cd_dep_count#83,cd_dep_employed_count#84,cd_dep_college_count#85] parquet
               :     :     :     +- Project [cd_demo_sk#130, cd_marital_status#132, cd_education_status#133]
               :     :     :        +- Filter ((isnotnull(cd_demo_sk#130) AND isnotnull(cd_marital_status#132)) AND isnotnull(cd_education_status#133))
               :     :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#130,cd_gender#131,cd_marital_status#132,cd_education_status#133,cd_purchase_estimate#134,cd_credit_rating#135,cd_dep_count#136,cd_dep_employed_count#137,cd_dep_college_count#138] parquet
               :     :     +- Project [ca_address_sk#86, ca_state#94]
               :     :        +- Filter (((isnotnull(ca_country#96) AND (ca_country#96 = United States)) AND isnotnull(ca_address_sk#86)) AND ((ca_state#94 IN (IN,OH,NJ) OR ca_state#94 IN (WI,CT,KY)) OR ca_state#94 IN (LA,IA,AR)))
               :     :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#86,ca_address_id#87,ca_street_number#88,ca_street_name#89,ca_street_type#90,ca_suite_number#91,ca_city#92,ca_county#93,ca_state#94,ca_zip#95,ca_country#96,ca_gmt_offset#97,ca_location_type#98] parquet
               :     +- Project [d_date_sk#99]
               :        +- Filter ((isnotnull(d_year#105) AND (d_year#105 = 2000)) AND isnotnull(d_date_sk#99))
               :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#99,d_date_id#100,d_date#101,d_month_seq#102,d_week_seq#103,d_quarter_seq#104,d_year#105,d_dow#106,d_moy#107,d_dom#108,d_qoy#109,d_fy_year#110,d_fy_quarter_seq#111,d_fy_week_seq#112,d_day_name#113,d_quarter_name#114,d_holiday#115,d_weekend#116,d_following_holiday#117,d_first_dom#118,d_last_dom#119,d_same_day_ly#120,d_same_day_lq#121,d_current_day#122,... 4 more fields] parquet
               +- Project [r_reason_sk#127, r_reason_desc#129]
                  +- Filter isnotnull(r_reason_sk#127)
                     +- Relation spark_catalog.tpcds.reason[r_reason_sk#127,r_reason_id#128,r_reason_desc#129] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[substr(r_reason_desc, 1, 20)#150 ASC NULLS FIRST,avg(ws_quantity)#151 ASC NULLS FIRST,avg(wr_refunded_cash)#152 ASC NULLS FIRST,avg(wr_fee)#153 ASC NULLS FIRST], output=[substr(r_reason_desc, 1, 20)#150,avg(ws_quantity)#151,avg(wr_refunded_cash)#152,avg(wr_fee)#153])
   +- HashAggregate(keys=[r_reason_desc#129], functions=[avg(ws_quantity#23), avg(wr_refunded_cash#59), avg(wr_fee#57)], output=[substr(r_reason_desc, 1, 20)#150, avg(ws_quantity)#151, avg(wr_refunded_cash)#152, avg(wr_fee)#153])
      +- Exchange hashpartitioning(r_reason_desc#129, 200), ENSURE_REQUIREMENTS, [plan_id=182]
         +- HashAggregate(keys=[r_reason_desc#129], functions=[partial_avg(ws_quantity#23), partial_avg(wr_refunded_cash#59), partial_avg(wr_fee#57)], output=[r_reason_desc#129, sum#163, count#164L, sum#165, count#166L, sum#167, count#168L])
            +- Project [ws_quantity#23, wr_fee#57, wr_refunded_cash#59, r_reason_desc#129]
               +- BroadcastHashJoin [wr_reason_sk#51], [r_reason_sk#127], Inner, BuildRight, false
                  :- Project [ws_quantity#23, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
                  :  +- BroadcastHashJoin [ws_sold_date_sk#5], [d_date_sk#99], Inner, BuildRight, false
                  :     :- Project [ws_sold_date_sk#5, ws_quantity#23, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
                  :     :  +- BroadcastHashJoin [wr_refunded_addr_sk#45], [ca_address_sk#86], Inner, BuildRight, ((((ca_state#94 IN (IN,OH,NJ) AND (ws_net_profit#38 >= 100.0)) AND (ws_net_profit#38 <= 200.0)) OR ((ca_state#94 IN (WI,CT,KY) AND (ws_net_profit#38 >= 150.0)) AND (ws_net_profit#38 <= 300.0))) OR ((ca_state#94 IN (LA,IA,AR) AND (ws_net_profit#38 >= 50.0)) AND (ws_net_profit#38 <= 250.0))), false
                  :     :     :- Project [ws_sold_date_sk#5, ws_quantity#23, ws_net_profit#38, wr_refunded_addr_sk#45, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
                  :     :     :  +- BroadcastHashJoin [wr_returning_cdemo_sk#47, cd_marital_status#79, cd_education_status#80], [cd_demo_sk#130, cd_marital_status#132, cd_education_status#133], Inner, BuildRight, false
                  :     :     :     :- Project [ws_sold_date_sk#5, ws_quantity#23, ws_net_profit#38, wr_refunded_addr_sk#45, wr_returning_cdemo_sk#47, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59, cd_marital_status#79, cd_education_status#80]
                  :     :     :     :  +- BroadcastHashJoin [wr_refunded_cdemo_sk#43], [cd_demo_sk#77], Inner, BuildRight, ((((((cd_marital_status#79 = M) AND (cd_education_status#80 = Advanced Degree)) AND (ws_sales_price#26 >= 100.0)) AND (ws_sales_price#26 <= 150.0)) OR ((((cd_marital_status#79 = S) AND (cd_education_status#80 = College)) AND (ws_sales_price#26 >= 50.0)) AND (ws_sales_price#26 <= 100.0))) OR ((((cd_marital_status#79 = W) AND (cd_education_status#80 = 2 yr Degree)) AND (ws_sales_price#26 >= 150.0)) AND (ws_sales_price#26 <= 200.0))), false
                  :     :     :     :     :- Project [ws_sold_date_sk#5, ws_quantity#23, ws_sales_price#26, ws_net_profit#38, wr_refunded_cdemo_sk#43, wr_refunded_addr_sk#45, wr_returning_cdemo_sk#47, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
                  :     :     :     :     :  +- BroadcastHashJoin [ws_web_page_sk#17], [wp_web_page_sk#63], Inner, BuildRight, false
                  :     :     :     :     :     :- Project [ws_sold_date_sk#5, ws_web_page_sk#17, ws_quantity#23, ws_sales_price#26, ws_net_profit#38, wr_refunded_cdemo_sk#43, wr_refunded_addr_sk#45, wr_returning_cdemo_sk#47, wr_reason_sk#51, wr_fee#57, wr_refunded_cash#59]
                  :     :     :     :     :     :  +- BroadcastHashJoin [ws_item_sk#8, ws_order_number#22], [wr_item_sk#41, wr_order_number#52], Inner, BuildRight, false
                  :     :     :     :     :     :     :- Filter (((((isnotnull(ws_item_sk#8) AND isnotnull(ws_order_number#22)) AND isnotnull(ws_web_page_sk#17)) AND isnotnull(ws_sold_date_sk#5)) AND ((((ws_sales_price#26 >= 100.0) AND (ws_sales_price#26 <= 150.0)) OR ((ws_sales_price#26 >= 50.0) AND (ws_sales_price#26 <= 100.0))) OR ((ws_sales_price#26 >= 150.0) AND (ws_sales_price#26 <= 200.0)))) AND ((((ws_net_profit#38 >= 100.0) AND (ws_net_profit#38 <= 200.0)) OR ((ws_net_profit#38 >= 150.0) AND (ws_net_profit#38 <= 300.0))) OR ((ws_net_profit#38 >= 50.0) AND (ws_net_profit#38 <= 250.0))))
                  :     :     :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#5,ws_item_sk#8,ws_web_page_sk#17,ws_order_number#22,ws_quantity#23,ws_sales_price#26,ws_net_profit#38] Batched: true, DataFilters: [isnotnull(ws_item_sk#8), isnotnull(ws_order_number#22), isnotnull(ws_web_page_sk#17), isnotnull(..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_order_number), IsNotNull(ws_web_page_sk), IsNotNull(ws_sold_..., ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_web_page_sk:int,ws_order_number:int,ws_quantity:int,...
                  :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, false] as bigint), 32) | (cast(input[5, int, false] as bigint) & 4294967295))),false), [plan_id=153]
                  :     :     :     :     :     :        +- Filter (((((isnotnull(wr_item_sk#41) AND isnotnull(wr_order_number#52)) AND isnotnull(wr_refunded_cdemo_sk#43)) AND isnotnull(wr_returning_cdemo_sk#47)) AND isnotnull(wr_refunded_addr_sk#45)) AND isnotnull(wr_reason_sk#51))
                  :     :     :     :     :     :           +- FileScan parquet spark_catalog.tpcds.web_returns[wr_item_sk#41,wr_refunded_cdemo_sk#43,wr_refunded_addr_sk#45,wr_returning_cdemo_sk#47,wr_reason_sk#51,wr_order_number#52,wr_fee#57,wr_refunded_cash#59] Batched: true, DataFilters: [isnotnull(wr_item_sk#41), isnotnull(wr_order_number#52), isnotnull(wr_refunded_cdemo_sk#43), isn..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_item_sk), IsNotNull(wr_order_number), IsNotNull(wr_refunded_cdemo_sk), IsNotNull(wr..., ReadSchema: struct<wr_item_sk:int,wr_refunded_cdemo_sk:int,wr_refunded_addr_sk:int,wr_returning_cdemo_sk:int,...
                  :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=157]
                  :     :     :     :     :        +- Filter isnotnull(wp_web_page_sk#63)
                  :     :     :     :     :           +- FileScan parquet spark_catalog.tpcds.web_page[wp_web_page_sk#63] Batched: true, DataFilters: [isnotnull(wp_web_page_sk#63)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_web_page_sk)], ReadSchema: struct<wp_web_page_sk:int>
                  :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=161]
                  :     :     :     :        +- Filter (((isnotnull(cd_demo_sk#77) AND isnotnull(cd_marital_status#79)) AND isnotnull(cd_education_status#80)) AND ((((cd_marital_status#79 = M) AND (cd_education_status#80 = Advanced Degree)) OR ((cd_marital_status#79 = S) AND (cd_education_status#80 = College))) OR ((cd_marital_status#79 = W) AND (cd_education_status#80 = 2 yr Degree))))
                  :     :     :     :           +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#77,cd_marital_status#79,cd_education_status#80] Batched: true, DataFilters: [isnotnull(cd_demo_sk#77), isnotnull(cd_marital_status#79), isnotnull(cd_education_status#80), ((..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk), IsNotNull(cd_marital_status), IsNotNull(cd_education_status), Or(Or(And(E..., ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, false], input[1, string, false], input[2, string, false]),false), [plan_id=165]
                  :     :     :        +- Filter ((isnotnull(cd_demo_sk#130) AND isnotnull(cd_marital_status#132)) AND isnotnull(cd_education_status#133))
                  :     :     :           +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#130,cd_marital_status#132,cd_education_status#133] Batched: true, DataFilters: [isnotnull(cd_demo_sk#130), isnotnull(cd_marital_status#132), isnotnull(cd_education_status#133)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk), IsNotNull(cd_marital_status), IsNotNull(cd_education_status)], ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=169]
                  :     :        +- Project [ca_address_sk#86, ca_state#94]
                  :     :           +- Filter (((isnotnull(ca_country#96) AND (ca_country#96 = United States)) AND isnotnull(ca_address_sk#86)) AND ((ca_state#94 IN (IN,OH,NJ) OR ca_state#94 IN (WI,CT,KY)) OR ca_state#94 IN (LA,IA,AR)))
                  :     :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#86,ca_state#94,ca_country#96] Batched: true, DataFilters: [isnotnull(ca_country#96), (ca_country#96 = United States), isnotnull(ca_address_sk#86), ((ca_sta..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_country), EqualTo(ca_country,United States), IsNotNull(ca_address_sk), Or(Or(In(ca_..., ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_country:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=173]
                  :        +- Project [d_date_sk#99]
                  :           +- Filter ((isnotnull(d_year#105) AND (d_year#105 = 2000)) AND isnotnull(d_date_sk#99))
                  :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#99,d_year#105] Batched: true, DataFilters: [isnotnull(d_year#105), (d_year#105 = 2000), isnotnull(d_date_sk#99)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=177]
                     +- Filter isnotnull(r_reason_sk#127)
                        +- FileScan parquet spark_catalog.tpcds.reason[r_reason_sk#127,r_reason_desc#129] Batched: true, DataFilters: [isnotnull(r_reason_sk#127)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(r_reason_sk)], ReadSchema: struct<r_reason_sk:int,r_reason_desc:string>

Time taken: 3.63 seconds, Fetched 1 row(s)
