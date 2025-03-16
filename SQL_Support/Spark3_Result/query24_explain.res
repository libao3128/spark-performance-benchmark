Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580450063
== Parsed Logical Plan ==
CTE [ssales]
:  +- 'SubqueryAlias ssales
:     +- 'Aggregate ['c_last_name, 'c_first_name, 's_store_name, 'ca_state, 's_state, 'i_color, 'i_current_price, 'i_manager_id, 'i_units, 'i_size], ['c_last_name, 'c_first_name, 's_store_name, 'ca_state, 's_state, 'i_color, 'i_current_price, 'i_manager_id, 'i_units, 'i_size, 'sum('ss_net_paid) AS netpaid#2]
:        +- 'Filter ((((('ss_ticket_number = 'sr_ticket_number) AND ('ss_item_sk = 'sr_item_sk)) AND ('ss_customer_sk = 'c_customer_sk)) AND (('ss_item_sk = 'i_item_sk) AND ('ss_store_sk = 's_store_sk))) AND ((('c_current_addr_sk = 'ca_address_sk) AND NOT ('c_birth_country = 'upper('ca_country))) AND (('s_zip = 'ca_zip) AND ('s_market_id = 8))))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'Join Inner
:              :  :  :- 'Join Inner
:              :  :  :  :- 'Join Inner
:              :  :  :  :  :- 'UnresolvedRelation [store_sales], [], false
:              :  :  :  :  +- 'UnresolvedRelation [store_returns], [], false
:              :  :  :  +- 'UnresolvedRelation [store], [], false
:              :  :  +- 'UnresolvedRelation [item], [], false
:              :  +- 'UnresolvedRelation [customer], [], false
:              +- 'UnresolvedRelation [customer_address], [], false
+- 'Sort ['c_last_name ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 's_store_name ASC NULLS FIRST], true
   +- 'UnresolvedHaving ('sum('netpaid) > scalar-subquery#1 [])
      :  +- 'Project [unresolvedalias((0.05 * 'avg('netpaid)), None)]
      :     +- 'UnresolvedRelation [ssales], [], false
      +- 'Aggregate ['c_last_name, 'c_first_name, 's_store_name], ['c_last_name, 'c_first_name, 's_store_name, 'sum('netpaid) AS paid#0]
         +- 'Filter ('i_color = pale)
            +- 'UnresolvedRelation [ssales], [], false

== Analyzed Logical Plan ==
c_last_name: string, c_first_name: string, s_store_name: string, paid: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ssales
:     +- Aggregate [c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, i_current_price#85, i_manager_id#100, i_units#98, i_size#95], [c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, i_current_price#85, i_manager_id#100, i_units#98, i_size#95, sum(ss_net_paid#28) AS netpaid#2]
:        +- Filter (((((ss_ticket_number#17 = sr_ticket_number#40) AND (ss_item_sk#10 = sr_item_sk#33)) AND (ss_customer_sk#11 = c_customer_sk#102)) AND ((ss_item_sk#10 = i_item_sk#80) AND (ss_store_sk#15 = s_store_sk#51))) AND (((c_current_addr_sk#106 = ca_address_sk#120) AND NOT (c_birth_country#116 = upper(ca_country#130))) AND ((s_zip#76 = ca_zip#129) AND (s_market_id#61 = 8))))
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- Join Inner
:              :  :  :  :- Join Inner
:              :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
:              :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store_returns
:              :  :  :  :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#31,sr_return_time_sk#32,sr_item_sk#33,sr_customer_sk#34,sr_cdemo_sk#35,sr_hdemo_sk#36,sr_addr_sk#37,sr_store_sk#38,sr_reason_sk#39,sr_ticket_number#40,sr_return_quantity#41,sr_return_amt#42,sr_return_tax#43,sr_return_amt_inc_tax#44,sr_fee#45,sr_return_ship_cost#46,sr_refunded_cash#47,sr_reversed_charge#48,sr_store_credit#49,sr_net_loss#50] parquet
:              :  :  :  +- SubqueryAlias spark_catalog.tpcds.store
:              :  :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#51,s_store_id#52,s_rec_start_date#53,s_rec_end_date#54,s_closed_date_sk#55,s_store_name#56,s_number_employees#57,s_floor_space#58,s_hours#59,s_manager#60,s_market_id#61,s_geography_class#62,s_market_desc#63,s_market_manager#64,s_division_id#65,s_division_name#66,s_company_id#67,s_company_name#68,s_street_number#69,s_street_name#70,s_street_type#71,s_suite_number#72,s_city#73,s_county#74,... 5 more fields] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.item
:              :  :     +- Relation spark_catalog.tpcds.item[i_item_sk#80,i_item_id#81,i_rec_start_date#82,i_rec_end_date#83,i_item_desc#84,i_current_price#85,i_wholesale_cost#86,i_brand_id#87,i_brand#88,i_class_id#89,i_class#90,i_category_id#91,i_category#92,i_manufact_id#93,i_manufact#94,i_size#95,i_formulation#96,i_color#97,i_units#98,i_container#99,i_manager_id#100,i_product_name#101] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.customer
:              :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#102,c_customer_id#103,c_current_cdemo_sk#104,c_current_hdemo_sk#105,c_current_addr_sk#106,c_first_shipto_date_sk#107,c_first_sales_date_sk#108,c_salutation#109,c_first_name#110,c_last_name#111,c_preferred_cust_flag#112,c_birth_day#113,c_birth_month#114,c_birth_year#115,c_birth_country#116,c_login#117,c_email_address#118,c_last_review_date#119] parquet
:              +- SubqueryAlias spark_catalog.tpcds.customer_address
:                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#120,ca_address_id#121,ca_street_number#122,ca_street_name#123,ca_street_type#124,ca_suite_number#125,ca_city#126,ca_county#127,ca_state#128,ca_zip#129,ca_country#130,ca_gmt_offset#131,ca_location_type#132] parquet
+- Sort [c_last_name#111 ASC NULLS FIRST, c_first_name#110 ASC NULLS FIRST, s_store_name#56 ASC NULLS FIRST], true
   +- Filter (paid#0 > scalar-subquery#1 [])
      :  +- Aggregate [(cast(0.05 as double) * avg(netpaid#150)) AS (0.05 * avg(netpaid))#153]
      :     +- SubqueryAlias ssales
      :        +- CTERelationRef 0, true, [c_last_name#140, c_first_name#141, s_store_name#142, ca_state#143, s_state#144, i_color#145, i_current_price#146, i_manager_id#147, i_units#148, i_size#149, netpaid#150], false
      +- Aggregate [c_last_name#111, c_first_name#110, s_store_name#56], [c_last_name#111, c_first_name#110, s_store_name#56, sum(netpaid#2) AS paid#0]
         +- Filter (i_color#97 = pale)
            +- SubqueryAlias ssales
               +- CTERelationRef 0, true, [c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, i_current_price#85, i_manager_id#100, i_units#98, i_size#95, netpaid#2], false

== Optimized Logical Plan ==
Sort [c_last_name#111 ASC NULLS FIRST, c_first_name#110 ASC NULLS FIRST, s_store_name#56 ASC NULLS FIRST], true
+- Filter (isnotnull(paid#0) AND (paid#0 > scalar-subquery#1 []))
   :  +- Aggregate [(0.05 * avg(netpaid#150)) AS (0.05 * avg(netpaid))#153]
   :     +- Aggregate [c_last_name#384, c_first_name#383, s_store_name#329, ca_state#401, s_state#348, i_color#370, i_current_price#358, i_manager_id#373, i_units#371, i_size#368], [sum(ss_net_paid#301) AS netpaid#150]
   :        +- Project [ss_net_paid#301, s_store_name#329, s_state#348, i_current_price#358, i_size#368, i_color#370, i_units#371, i_manager_id#373, c_first_name#383, c_last_name#384, ca_state#401]
   :           +- Join Inner, (((c_current_addr_sk#379 = ca_address_sk#393) AND NOT (c_birth_country#389 = upper(ca_country#403))) AND (s_zip#349 = ca_zip#402))
   :              :- Project [ss_net_paid#301, s_store_name#329, s_state#348, s_zip#349, i_current_price#358, i_size#368, i_color#370, i_units#371, i_manager_id#373, c_current_addr_sk#379, c_first_name#383, c_last_name#384, c_birth_country#389]
   :              :  +- Join Inner, (ss_customer_sk#284 = c_customer_sk#375)
   :              :     :- Project [ss_customer_sk#284, ss_net_paid#301, s_store_name#329, s_state#348, s_zip#349, i_current_price#358, i_size#368, i_color#370, i_units#371, i_manager_id#373]
   :              :     :  +- Join Inner, (ss_item_sk#283 = i_item_sk#353)
   :              :     :     :- Project [ss_item_sk#283, ss_customer_sk#284, ss_net_paid#301, s_store_name#329, s_state#348, s_zip#349]
   :              :     :     :  +- Join Inner, (ss_store_sk#288 = s_store_sk#324)
   :              :     :     :     :- Project [ss_item_sk#283, ss_customer_sk#284, ss_store_sk#288, ss_net_paid#301]
   :              :     :     :     :  +- Join Inner, ((ss_ticket_number#290 = sr_ticket_number#313) AND (ss_item_sk#283 = sr_item_sk#306))
   :              :     :     :     :     :- Project [ss_item_sk#283, ss_customer_sk#284, ss_store_sk#288, ss_ticket_number#290, ss_net_paid#301]
   :              :     :     :     :     :  +- Filter (((isnotnull(ss_ticket_number#290) AND isnotnull(ss_item_sk#283)) AND isnotnull(ss_store_sk#288)) AND isnotnull(ss_customer_sk#284))
   :              :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#281,ss_sold_time_sk#282,ss_item_sk#283,ss_customer_sk#284,ss_cdemo_sk#285,ss_hdemo_sk#286,ss_addr_sk#287,ss_store_sk#288,ss_promo_sk#289,ss_ticket_number#290,ss_quantity#291,ss_wholesale_cost#292,ss_list_price#293,ss_sales_price#294,ss_ext_discount_amt#295,ss_ext_sales_price#296,ss_ext_wholesale_cost#297,ss_ext_list_price#298,ss_ext_tax#299,ss_coupon_amt#300,ss_net_paid#301,ss_net_paid_inc_tax#302,ss_net_profit#303] parquet
   :              :     :     :     :     +- Project [sr_item_sk#306, sr_ticket_number#313]
   :              :     :     :     :        +- Filter (isnotnull(sr_ticket_number#313) AND isnotnull(sr_item_sk#306))
   :              :     :     :     :           +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#304,sr_return_time_sk#305,sr_item_sk#306,sr_customer_sk#307,sr_cdemo_sk#308,sr_hdemo_sk#309,sr_addr_sk#310,sr_store_sk#311,sr_reason_sk#312,sr_ticket_number#313,sr_return_quantity#314,sr_return_amt#315,sr_return_tax#316,sr_return_amt_inc_tax#317,sr_fee#318,sr_return_ship_cost#319,sr_refunded_cash#320,sr_reversed_charge#321,sr_store_credit#322,sr_net_loss#323] parquet
   :              :     :     :     +- Project [s_store_sk#324, s_store_name#329, s_state#348, s_zip#349]
   :              :     :     :        +- Filter (((isnotnull(s_market_id#334) AND (s_market_id#334 = 8)) AND isnotnull(s_store_sk#324)) AND isnotnull(s_zip#349))
   :              :     :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#324,s_store_id#325,s_rec_start_date#326,s_rec_end_date#327,s_closed_date_sk#328,s_store_name#329,s_number_employees#330,s_floor_space#331,s_hours#332,s_manager#333,s_market_id#334,s_geography_class#335,s_market_desc#336,s_market_manager#337,s_division_id#338,s_division_name#339,s_company_id#340,s_company_name#341,s_street_number#342,s_street_name#343,s_street_type#344,s_suite_number#345,s_city#346,s_county#347,... 5 more fields] parquet
   :              :     :     +- Project [i_item_sk#353, i_current_price#358, i_size#368, i_color#370, i_units#371, i_manager_id#373]
   :              :     :        +- Filter isnotnull(i_item_sk#353)
   :              :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#353,i_item_id#354,i_rec_start_date#355,i_rec_end_date#356,i_item_desc#357,i_current_price#358,i_wholesale_cost#359,i_brand_id#360,i_brand#361,i_class_id#362,i_class#363,i_category_id#364,i_category#365,i_manufact_id#366,i_manufact#367,i_size#368,i_formulation#369,i_color#370,i_units#371,i_container#372,i_manager_id#373,i_product_name#374] parquet
   :              :     +- Project [c_customer_sk#375, c_current_addr_sk#379, c_first_name#383, c_last_name#384, c_birth_country#389]
   :              :        +- Filter (isnotnull(c_customer_sk#375) AND (isnotnull(c_current_addr_sk#379) AND isnotnull(c_birth_country#389)))
   :              :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#375,c_customer_id#376,c_current_cdemo_sk#377,c_current_hdemo_sk#378,c_current_addr_sk#379,c_first_shipto_date_sk#380,c_first_sales_date_sk#381,c_salutation#382,c_first_name#383,c_last_name#384,c_preferred_cust_flag#385,c_birth_day#386,c_birth_month#387,c_birth_year#388,c_birth_country#389,c_login#390,c_email_address#391,c_last_review_date#392] parquet
   :              +- Project [ca_address_sk#393, ca_state#401, ca_zip#402, ca_country#403]
   :                 +- Filter ((isnotnull(ca_address_sk#393) AND isnotnull(ca_country#403)) AND isnotnull(ca_zip#402))
   :                    +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#393,ca_address_id#394,ca_street_number#395,ca_street_name#396,ca_street_type#397,ca_suite_number#398,ca_city#399,ca_county#400,ca_state#401,ca_zip#402,ca_country#403,ca_gmt_offset#404,ca_location_type#405] parquet
   +- Aggregate [c_last_name#111, c_first_name#110, s_store_name#56], [c_last_name#111, c_first_name#110, s_store_name#56, sum(netpaid#2) AS paid#0]
      +- Aggregate [c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, i_current_price#85, i_manager_id#100, i_units#98, i_size#95], [c_last_name#111, c_first_name#110, s_store_name#56, sum(ss_net_paid#28) AS netpaid#2]
         +- Project [ss_net_paid#28, s_store_name#56, s_state#75, i_current_price#85, i_size#95, i_color#97, i_units#98, i_manager_id#100, c_first_name#110, c_last_name#111, ca_state#128]
            +- Join Inner, (((c_current_addr_sk#106 = ca_address_sk#120) AND NOT (c_birth_country#116 = upper(ca_country#130))) AND (s_zip#76 = ca_zip#129))
               :- Project [ss_net_paid#28, s_store_name#56, s_state#75, s_zip#76, i_current_price#85, i_size#95, i_color#97, i_units#98, i_manager_id#100, c_current_addr_sk#106, c_first_name#110, c_last_name#111, c_birth_country#116]
               :  +- Join Inner, (ss_customer_sk#11 = c_customer_sk#102)
               :     :- Project [ss_customer_sk#11, ss_net_paid#28, s_store_name#56, s_state#75, s_zip#76, i_current_price#85, i_size#95, i_color#97, i_units#98, i_manager_id#100]
               :     :  +- Join Inner, (ss_item_sk#10 = i_item_sk#80)
               :     :     :- Project [ss_item_sk#10, ss_customer_sk#11, ss_net_paid#28, s_store_name#56, s_state#75, s_zip#76]
               :     :     :  +- Join Inner, (ss_store_sk#15 = s_store_sk#51)
               :     :     :     :- Project [ss_item_sk#10, ss_customer_sk#11, ss_store_sk#15, ss_net_paid#28]
               :     :     :     :  +- Join Inner, ((ss_ticket_number#17 = sr_ticket_number#40) AND (ss_item_sk#10 = sr_item_sk#33))
               :     :     :     :     :- Project [ss_item_sk#10, ss_customer_sk#11, ss_store_sk#15, ss_ticket_number#17, ss_net_paid#28]
               :     :     :     :     :  +- Filter (((isnotnull(ss_ticket_number#17) AND isnotnull(ss_item_sk#10)) AND isnotnull(ss_store_sk#15)) AND isnotnull(ss_customer_sk#11))
               :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
               :     :     :     :     +- Project [sr_item_sk#33, sr_ticket_number#40]
               :     :     :     :        +- Filter (isnotnull(sr_ticket_number#40) AND isnotnull(sr_item_sk#33))
               :     :     :     :           +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#31,sr_return_time_sk#32,sr_item_sk#33,sr_customer_sk#34,sr_cdemo_sk#35,sr_hdemo_sk#36,sr_addr_sk#37,sr_store_sk#38,sr_reason_sk#39,sr_ticket_number#40,sr_return_quantity#41,sr_return_amt#42,sr_return_tax#43,sr_return_amt_inc_tax#44,sr_fee#45,sr_return_ship_cost#46,sr_refunded_cash#47,sr_reversed_charge#48,sr_store_credit#49,sr_net_loss#50] parquet
               :     :     :     +- Project [s_store_sk#51, s_store_name#56, s_state#75, s_zip#76]
               :     :     :        +- Filter (((isnotnull(s_market_id#61) AND (s_market_id#61 = 8)) AND isnotnull(s_store_sk#51)) AND isnotnull(s_zip#76))
               :     :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#51,s_store_id#52,s_rec_start_date#53,s_rec_end_date#54,s_closed_date_sk#55,s_store_name#56,s_number_employees#57,s_floor_space#58,s_hours#59,s_manager#60,s_market_id#61,s_geography_class#62,s_market_desc#63,s_market_manager#64,s_division_id#65,s_division_name#66,s_company_id#67,s_company_name#68,s_street_number#69,s_street_name#70,s_street_type#71,s_suite_number#72,s_city#73,s_county#74,... 5 more fields] parquet
               :     :     +- Project [i_item_sk#80, i_current_price#85, i_size#95, i_color#97, i_units#98, i_manager_id#100]
               :     :        +- Filter ((isnotnull(i_color#97) AND (i_color#97 = pale)) AND isnotnull(i_item_sk#80))
               :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#80,i_item_id#81,i_rec_start_date#82,i_rec_end_date#83,i_item_desc#84,i_current_price#85,i_wholesale_cost#86,i_brand_id#87,i_brand#88,i_class_id#89,i_class#90,i_category_id#91,i_category#92,i_manufact_id#93,i_manufact#94,i_size#95,i_formulation#96,i_color#97,i_units#98,i_container#99,i_manager_id#100,i_product_name#101] parquet
               :     +- Project [c_customer_sk#102, c_current_addr_sk#106, c_first_name#110, c_last_name#111, c_birth_country#116]
               :        +- Filter (isnotnull(c_customer_sk#102) AND (isnotnull(c_current_addr_sk#106) AND isnotnull(c_birth_country#116)))
               :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#102,c_customer_id#103,c_current_cdemo_sk#104,c_current_hdemo_sk#105,c_current_addr_sk#106,c_first_shipto_date_sk#107,c_first_sales_date_sk#108,c_salutation#109,c_first_name#110,c_last_name#111,c_preferred_cust_flag#112,c_birth_day#113,c_birth_month#114,c_birth_year#115,c_birth_country#116,c_login#117,c_email_address#118,c_last_review_date#119] parquet
               +- Project [ca_address_sk#120, ca_state#128, ca_zip#129, ca_country#130]
                  +- Filter ((isnotnull(ca_address_sk#120) AND isnotnull(ca_country#130)) AND isnotnull(ca_zip#129))
                     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#120,ca_address_id#121,ca_street_number#122,ca_street_name#123,ca_street_type#124,ca_suite_number#125,ca_city#126,ca_county#127,ca_state#128,ca_zip#129,ca_country#130,ca_gmt_offset#131,ca_location_type#132] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [c_last_name#111 ASC NULLS FIRST, c_first_name#110 ASC NULLS FIRST, s_store_name#56 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(c_last_name#111 ASC NULLS FIRST, c_first_name#110 ASC NULLS FIRST, s_store_name#56 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=275]
      +- Filter (isnotnull(paid#0) AND (paid#0 > Subquery subquery#1, [id=#226]))
         :  +- Subquery subquery#1, [id=#226]
         :     +- AdaptiveSparkPlan isFinalPlan=false
         :        +- HashAggregate(keys=[], functions=[avg(netpaid#150)], output=[(0.05 * avg(netpaid))#153])
         :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=224]
         :              +- HashAggregate(keys=[], functions=[partial_avg(netpaid#150)], output=[sum#413, count#414L])
         :                 +- HashAggregate(keys=[c_last_name#384, c_first_name#383, s_store_name#329, ca_state#401, s_state#348, i_color#370, i_current_price#358, i_manager_id#373, i_units#371, i_size#368], functions=[sum(ss_net_paid#301)], output=[netpaid#150])
         :                    +- Exchange hashpartitioning(c_last_name#384, c_first_name#383, s_store_name#329, ca_state#401, s_state#348, i_color#370, i_current_price#358, i_manager_id#373, i_units#371, i_size#368, 200), ENSURE_REQUIREMENTS, [plan_id=220]
         :                       +- HashAggregate(keys=[c_last_name#384, c_first_name#383, s_store_name#329, ca_state#401, s_state#348, i_color#370, knownfloatingpointnormalized(normalizenanandzero(i_current_price#358)) AS i_current_price#358, i_manager_id#373, i_units#371, i_size#368], functions=[partial_sum(ss_net_paid#301)], output=[c_last_name#384, c_first_name#383, s_store_name#329, ca_state#401, s_state#348, i_color#370, i_current_price#358, i_manager_id#373, i_units#371, i_size#368, sum#416])
         :                          +- Project [ss_net_paid#301, s_store_name#329, s_state#348, i_current_price#358, i_size#368, i_color#370, i_units#371, i_manager_id#373, c_first_name#383, c_last_name#384, ca_state#401]
         :                             +- BroadcastHashJoin [c_current_addr_sk#379, s_zip#349], [ca_address_sk#393, ca_zip#402], Inner, BuildRight, NOT (c_birth_country#389 = upper(ca_country#403)), false
         :                                :- Project [ss_net_paid#301, s_store_name#329, s_state#348, s_zip#349, i_current_price#358, i_size#368, i_color#370, i_units#371, i_manager_id#373, c_current_addr_sk#379, c_first_name#383, c_last_name#384, c_birth_country#389]
         :                                :  +- BroadcastHashJoin [ss_customer_sk#284], [c_customer_sk#375], Inner, BuildRight, false
         :                                :     :- Project [ss_customer_sk#284, ss_net_paid#301, s_store_name#329, s_state#348, s_zip#349, i_current_price#358, i_size#368, i_color#370, i_units#371, i_manager_id#373]
         :                                :     :  +- BroadcastHashJoin [ss_item_sk#283], [i_item_sk#353], Inner, BuildRight, false
         :                                :     :     :- Project [ss_item_sk#283, ss_customer_sk#284, ss_net_paid#301, s_store_name#329, s_state#348, s_zip#349]
         :                                :     :     :  +- BroadcastHashJoin [ss_store_sk#288], [s_store_sk#324], Inner, BuildRight, false
         :                                :     :     :     :- Project [ss_item_sk#283, ss_customer_sk#284, ss_store_sk#288, ss_net_paid#301]
         :                                :     :     :     :  +- BroadcastHashJoin [ss_ticket_number#290, ss_item_sk#283], [sr_ticket_number#313, sr_item_sk#306], Inner, BuildRight, false
         :                                :     :     :     :     :- Filter (((isnotnull(ss_ticket_number#290) AND isnotnull(ss_item_sk#283)) AND isnotnull(ss_store_sk#288)) AND isnotnull(ss_customer_sk#284))
         :                                :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_item_sk#283,ss_customer_sk#284,ss_store_sk#288,ss_ticket_number#290,ss_net_paid#301] Batched: true, DataFilters: [isnotnull(ss_ticket_number#290), isnotnull(ss_item_sk#283), isnotnull(ss_store_sk#288), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_ticket_number), IsNotNull(ss_item_sk), IsNotNull(ss_store_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int,ss_net_paid:double>
         :                                :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=199]
         :                                :     :     :     :        +- Filter (isnotnull(sr_ticket_number#313) AND isnotnull(sr_item_sk#306))
         :                                :     :     :     :           +- FileScan parquet spark_catalog.tpcds.store_returns[sr_item_sk#306,sr_ticket_number#313] Batched: true, DataFilters: [isnotnull(sr_ticket_number#313), isnotnull(sr_item_sk#306)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_ticket_number), IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int>
         :                                :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=203]
         :                                :     :     :        +- Project [s_store_sk#324, s_store_name#329, s_state#348, s_zip#349]
         :                                :     :     :           +- Filter (((isnotnull(s_market_id#334) AND (s_market_id#334 = 8)) AND isnotnull(s_store_sk#324)) AND isnotnull(s_zip#349))
         :                                :     :     :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#324,s_store_name#329,s_market_id#334,s_state#348,s_zip#349] Batched: true, DataFilters: [isnotnull(s_market_id#334), (s_market_id#334 = 8), isnotnull(s_store_sk#324), isnotnull(s_zip#349)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_market_id), EqualTo(s_market_id,8), IsNotNull(s_store_sk), IsNotNull(s_zip)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_market_id:int,s_state:string,s_zip:string>
         :                                :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=207]
         :                                :     :        +- Filter isnotnull(i_item_sk#353)
         :                                :     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#353,i_current_price#358,i_size#368,i_color#370,i_units#371,i_manager_id#373] Batched: true, DataFilters: [isnotnull(i_item_sk#353)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double,i_size:string,i_color:string,i_units:string,i_manager...
         :                                :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=211]
         :                                :        +- Filter ((isnotnull(c_customer_sk#375) AND isnotnull(c_current_addr_sk#379)) AND isnotnull(c_birth_country#389))
         :                                :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#375,c_current_addr_sk#379,c_first_name#383,c_last_name#384,c_birth_country#389] Batched: true, DataFilters: [isnotnull(c_customer_sk#375), isnotnull(c_current_addr_sk#379), isnotnull(c_birth_country#389)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk), IsNotNull(c_birth_country)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string,c_birth_cou...
         :                                +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, false], input[2, string, false]),false), [plan_id=215]
         :                                   +- Filter ((isnotnull(ca_address_sk#393) AND isnotnull(ca_country#403)) AND isnotnull(ca_zip#402))
         :                                      +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#393,ca_state#401,ca_zip#402,ca_country#403] Batched: true, DataFilters: [isnotnull(ca_address_sk#393), isnotnull(ca_country#403), isnotnull(ca_zip#402)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_country), IsNotNull(ca_zip)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_zip:string,ca_country:string>
         +- HashAggregate(keys=[c_last_name#111, c_first_name#110, s_store_name#56], functions=[sum(netpaid#2)], output=[c_last_name#111, c_first_name#110, s_store_name#56, paid#0])
            +- Exchange hashpartitioning(c_last_name#111, c_first_name#110, s_store_name#56, 200), ENSURE_REQUIREMENTS, [plan_id=271]
               +- HashAggregate(keys=[c_last_name#111, c_first_name#110, s_store_name#56], functions=[partial_sum(netpaid#2)], output=[c_last_name#111, c_first_name#110, s_store_name#56, sum#408])
                  +- HashAggregate(keys=[c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, i_current_price#85, i_manager_id#100, i_units#98, i_size#95], functions=[sum(ss_net_paid#28)], output=[c_last_name#111, c_first_name#110, s_store_name#56, netpaid#2])
                     +- Exchange hashpartitioning(c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, i_current_price#85, i_manager_id#100, i_units#98, i_size#95, 200), ENSURE_REQUIREMENTS, [plan_id=267]
                        +- HashAggregate(keys=[c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, knownfloatingpointnormalized(normalizenanandzero(i_current_price#85)) AS i_current_price#85, i_manager_id#100, i_units#98, i_size#95], functions=[partial_sum(ss_net_paid#28)], output=[c_last_name#111, c_first_name#110, s_store_name#56, ca_state#128, s_state#75, i_color#97, i_current_price#85, i_manager_id#100, i_units#98, i_size#95, sum#410])
                           +- Project [ss_net_paid#28, s_store_name#56, s_state#75, i_current_price#85, i_size#95, i_color#97, i_units#98, i_manager_id#100, c_first_name#110, c_last_name#111, ca_state#128]
                              +- BroadcastHashJoin [c_current_addr_sk#106, s_zip#76], [ca_address_sk#120, ca_zip#129], Inner, BuildRight, NOT (c_birth_country#116 = upper(ca_country#130)), false
                                 :- Project [ss_net_paid#28, s_store_name#56, s_state#75, s_zip#76, i_current_price#85, i_size#95, i_color#97, i_units#98, i_manager_id#100, c_current_addr_sk#106, c_first_name#110, c_last_name#111, c_birth_country#116]
                                 :  +- BroadcastHashJoin [ss_customer_sk#11], [c_customer_sk#102], Inner, BuildRight, false
                                 :     :- Project [ss_customer_sk#11, ss_net_paid#28, s_store_name#56, s_state#75, s_zip#76, i_current_price#85, i_size#95, i_color#97, i_units#98, i_manager_id#100]
                                 :     :  +- BroadcastHashJoin [ss_item_sk#10], [i_item_sk#80], Inner, BuildRight, false
                                 :     :     :- Project [ss_item_sk#10, ss_customer_sk#11, ss_net_paid#28, s_store_name#56, s_state#75, s_zip#76]
                                 :     :     :  +- BroadcastHashJoin [ss_store_sk#15], [s_store_sk#51], Inner, BuildRight, false
                                 :     :     :     :- Project [ss_item_sk#10, ss_customer_sk#11, ss_store_sk#15, ss_net_paid#28]
                                 :     :     :     :  +- BroadcastHashJoin [ss_ticket_number#17, ss_item_sk#10], [sr_ticket_number#40, sr_item_sk#33], Inner, BuildRight, false
                                 :     :     :     :     :- Filter (((isnotnull(ss_ticket_number#17) AND isnotnull(ss_item_sk#10)) AND isnotnull(ss_store_sk#15)) AND isnotnull(ss_customer_sk#11))
                                 :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_item_sk#10,ss_customer_sk#11,ss_store_sk#15,ss_ticket_number#17,ss_net_paid#28] Batched: true, DataFilters: [isnotnull(ss_ticket_number#17), isnotnull(ss_item_sk#10), isnotnull(ss_store_sk#15), isnotnull(s..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_ticket_number), IsNotNull(ss_item_sk), IsNotNull(ss_store_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int,ss_net_paid:double>
                                 :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=246]
                                 :     :     :     :        +- Filter (isnotnull(sr_ticket_number#40) AND isnotnull(sr_item_sk#33))
                                 :     :     :     :           +- FileScan parquet spark_catalog.tpcds.store_returns[sr_item_sk#33,sr_ticket_number#40] Batched: true, DataFilters: [isnotnull(sr_ticket_number#40), isnotnull(sr_item_sk#33)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_ticket_number), IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int>
                                 :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=250]
                                 :     :     :        +- Project [s_store_sk#51, s_store_name#56, s_state#75, s_zip#76]
                                 :     :     :           +- Filter (((isnotnull(s_market_id#61) AND (s_market_id#61 = 8)) AND isnotnull(s_store_sk#51)) AND isnotnull(s_zip#76))
                                 :     :     :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#51,s_store_name#56,s_market_id#61,s_state#75,s_zip#76] Batched: true, DataFilters: [isnotnull(s_market_id#61), (s_market_id#61 = 8), isnotnull(s_store_sk#51), isnotnull(s_zip#76)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_market_id), EqualTo(s_market_id,8), IsNotNull(s_store_sk), IsNotNull(s_zip)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_market_id:int,s_state:string,s_zip:string>
                                 :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=254]
                                 :     :        +- Filter ((isnotnull(i_color#97) AND (i_color#97 = pale)) AND isnotnull(i_item_sk#80))
                                 :     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#80,i_current_price#85,i_size#95,i_color#97,i_units#98,i_manager_id#100] Batched: true, DataFilters: [isnotnull(i_color#97), (i_color#97 = pale), isnotnull(i_item_sk#80)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_color), EqualTo(i_color,pale), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double,i_size:string,i_color:string,i_units:string,i_manager...
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=258]
                                 :        +- Filter ((isnotnull(c_customer_sk#102) AND isnotnull(c_current_addr_sk#106)) AND isnotnull(c_birth_country#116))
                                 :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#102,c_current_addr_sk#106,c_first_name#110,c_last_name#111,c_birth_country#116] Batched: true, DataFilters: [isnotnull(c_customer_sk#102), isnotnull(c_current_addr_sk#106), isnotnull(c_birth_country#116)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk), IsNotNull(c_birth_country)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string,c_birth_cou...
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, false], input[2, string, false]),false), [plan_id=262]
                                    +- Filter ((isnotnull(ca_address_sk#120) AND isnotnull(ca_country#130)) AND isnotnull(ca_zip#129))
                                       +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#120,ca_state#128,ca_zip#129,ca_country#130] Batched: true, DataFilters: [isnotnull(ca_address_sk#120), isnotnull(ca_country#130), isnotnull(ca_zip#129)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_country), IsNotNull(ca_zip)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_zip:string,ca_country:string>

Time taken: 3.32 seconds, Fetched 1 row(s)
Time taken: 11.545 seconds
