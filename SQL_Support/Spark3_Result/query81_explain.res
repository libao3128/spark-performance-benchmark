Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4041
Spark master: local[*], Application Id: local-1741582691312
== Parsed Logical Plan ==
CTE [customer_total_return]
:  +- 'SubqueryAlias customer_total_return
:     +- 'Aggregate ['cr_returning_customer_sk, 'ca_state], ['cr_returning_customer_sk AS ctr_customer_sk#1, 'ca_state AS ctr_state#2, 'sum('cr_return_amt_inc_tax) AS ctr_total_return#3]
:        +- 'Filter ((('cr_returned_date_sk = 'd_date_sk) AND ('d_year = 2000)) AND ('cr_returning_addr_sk = 'ca_address_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation [catalog_returns], [], false
:              :  +- 'UnresolvedRelation [date_dim], [], false
:              +- 'UnresolvedRelation [customer_address], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['c_customer_id ASC NULLS FIRST, 'c_salutation ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'c_last_name ASC NULLS FIRST, 'ca_street_number ASC NULLS FIRST, 'ca_street_name ASC NULLS FIRST, 'ca_street_type ASC NULLS FIRST, 'ca_suite_number ASC NULLS FIRST, 'ca_city ASC NULLS FIRST, 'ca_county ASC NULLS FIRST, 'ca_state ASC NULLS FIRST, 'ca_zip ASC NULLS FIRST, 'ca_country ASC NULLS FIRST, 'ca_gmt_offset ASC NULLS FIRST, 'ca_location_type ASC NULLS FIRST, 'ctr_total_return ASC NULLS FIRST], true
         +- 'Project ['c_customer_id, 'c_salutation, 'c_first_name, 'c_last_name, 'ca_street_number, 'ca_street_name, 'ca_street_type, 'ca_suite_number, 'ca_city, 'ca_county, 'ca_state, 'ca_zip, 'ca_country, 'ca_gmt_offset, 'ca_location_type, 'ctr_total_return]
            +- 'Filter ((('ctr1.ctr_total_return > scalar-subquery#0 []) AND ('ca_address_sk = 'c_current_addr_sk)) AND (('ca_state = GA) AND ('ctr1.ctr_customer_sk = 'c_customer_sk)))
               :  +- 'Project [unresolvedalias(('avg('ctr_total_return) * 1.2), None)]
               :     +- 'Filter ('ctr1.ctr_state = 'ctr2.ctr_state)
               :        +- 'SubqueryAlias ctr2
               :           +- 'UnresolvedRelation [customer_total_return], [], false
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'SubqueryAlias ctr1
                  :  :  +- 'UnresolvedRelation [customer_total_return], [], false
                  :  +- 'UnresolvedRelation [customer_address], [], false
                  +- 'UnresolvedRelation [customer], [], false

== Analyzed Logical Plan ==
c_customer_id: string, c_salutation: string, c_first_name: string, c_last_name: string, ca_street_number: string, ca_street_name: string, ca_street_type: string, ca_suite_number: string, ca_city: string, ca_county: string, ca_state: string, ca_zip: string, ca_country: string, ca_gmt_offset: double, ca_location_type: string, ctr_total_return: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias customer_total_return
:     +- Aggregate [cr_returning_customer_sk#16, ca_state#72], [cr_returning_customer_sk#16 AS ctr_customer_sk#1, ca_state#72 AS ctr_state#2, sum(cr_return_amt_inc_tax#29) AS ctr_total_return#3]
:        +- Filter (((cr_returned_date_sk#9 = d_date_sk#36) AND (d_year#42 = 2000)) AND (cr_returning_addr_sk#19 = ca_address_sk#64))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.catalog_returns
:              :  :  +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#9,cr_returned_time_sk#10,cr_item_sk#11,cr_refunded_customer_sk#12,cr_refunded_cdemo_sk#13,cr_refunded_hdemo_sk#14,cr_refunded_addr_sk#15,cr_returning_customer_sk#16,cr_returning_cdemo_sk#17,cr_returning_hdemo_sk#18,cr_returning_addr_sk#19,cr_call_center_sk#20,cr_catalog_page_sk#21,cr_ship_mode_sk#22,cr_warehouse_sk#23,cr_reason_sk#24,cr_order_number#25,cr_return_quantity#26,cr_return_amount#27,cr_return_tax#28,cr_return_amt_inc_tax#29,cr_fee#30,cr_return_ship_cost#31,cr_refunded_cash#32,... 3 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#36,d_date_id#37,d_date#38,d_month_seq#39,d_week_seq#40,d_quarter_seq#41,d_year#42,d_dow#43,d_moy#44,d_dom#45,d_qoy#46,d_fy_year#47,d_fy_quarter_seq#48,d_fy_week_seq#49,d_day_name#50,d_quarter_name#51,d_holiday#52,d_weekend#53,d_following_holiday#54,d_first_dom#55,d_last_dom#56,d_same_day_ly#57,d_same_day_lq#58,d_current_day#59,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.customer_address
:                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#64,ca_address_id#65,ca_street_number#66,ca_street_name#67,ca_street_type#68,ca_suite_number#69,ca_city#70,ca_county#71,ca_state#72,ca_zip#73,ca_country#74,ca_gmt_offset#75,ca_location_type#76] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [c_customer_id#78 ASC NULLS FIRST, c_salutation#84 ASC NULLS FIRST, c_first_name#85 ASC NULLS FIRST, c_last_name#86 ASC NULLS FIRST, ca_street_number#97 ASC NULLS FIRST, ca_street_name#98 ASC NULLS FIRST, ca_street_type#99 ASC NULLS FIRST, ca_suite_number#100 ASC NULLS FIRST, ca_city#101 ASC NULLS FIRST, ca_county#102 ASC NULLS FIRST, ca_state#103 ASC NULLS FIRST, ca_zip#104 ASC NULLS FIRST, ca_country#105 ASC NULLS FIRST, ca_gmt_offset#106 ASC NULLS FIRST, ca_location_type#107 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
         +- Project [c_customer_id#78, c_salutation#84, c_first_name#85, c_last_name#86, ca_street_number#97, ca_street_name#98, ca_street_type#99, ca_suite_number#100, ca_city#101, ca_county#102, ca_state#103, ca_zip#104, ca_country#105, ca_gmt_offset#106, ca_location_type#107, ctr_total_return#3]
            +- Filter (((ctr_total_return#3 > scalar-subquery#0 [ctr_state#2]) AND (ca_address_sk#95 = c_current_addr_sk#81)) AND ((ca_state#103 = GA) AND (ctr_customer_sk#1 = c_customer_sk#77)))
               :  +- Aggregate [(avg(ctr_total_return#114) * cast(1.2 as double)) AS (avg(ctr_total_return) * 1.2)#116]
               :     +- Filter (outer(ctr_state#2) = ctr_state#113)
               :        +- SubqueryAlias ctr2
               :           +- SubqueryAlias customer_total_return
               :              +- CTERelationRef 0, true, [ctr_customer_sk#112, ctr_state#113, ctr_total_return#114], false
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias ctr1
                  :  :  +- SubqueryAlias customer_total_return
                  :  :     +- CTERelationRef 0, true, [ctr_customer_sk#1, ctr_state#2, ctr_total_return#3], false
                  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
                  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#95,ca_address_id#96,ca_street_number#97,ca_street_name#98,ca_street_type#99,ca_suite_number#100,ca_city#101,ca_county#102,ca_state#103,ca_zip#104,ca_country#105,ca_gmt_offset#106,ca_location_type#107] parquet
                  +- SubqueryAlias spark_catalog.tpcds.customer
                     +- Relation spark_catalog.tpcds.customer[c_customer_sk#77,c_customer_id#78,c_current_cdemo_sk#79,c_current_hdemo_sk#80,c_current_addr_sk#81,c_first_shipto_date_sk#82,c_first_sales_date_sk#83,c_salutation#84,c_first_name#85,c_last_name#86,c_preferred_cust_flag#87,c_birth_day#88,c_birth_month#89,c_birth_year#90,c_birth_country#91,c_login#92,c_email_address#93,c_last_review_date#94] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#78 ASC NULLS FIRST, c_salutation#84 ASC NULLS FIRST, c_first_name#85 ASC NULLS FIRST, c_last_name#86 ASC NULLS FIRST, ca_street_number#97 ASC NULLS FIRST, ca_street_name#98 ASC NULLS FIRST, ca_street_type#99 ASC NULLS FIRST, ca_suite_number#100 ASC NULLS FIRST, ca_city#101 ASC NULLS FIRST, ca_county#102 ASC NULLS FIRST, ca_state#103 ASC NULLS FIRST, ca_zip#104 ASC NULLS FIRST, ca_country#105 ASC NULLS FIRST, ca_gmt_offset#106 ASC NULLS FIRST, ca_location_type#107 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
      +- Project [c_customer_id#78, c_salutation#84, c_first_name#85, c_last_name#86, ca_street_number#97, ca_street_name#98, ca_street_type#99, ca_suite_number#100, ca_city#101, ca_county#102, ca_state#103, ca_zip#104, ca_country#105, ca_gmt_offset#106, ca_location_type#107, ctr_total_return#3]
         +- Join Inner, (ca_address_sk#95 = c_current_addr_sk#81)
            :- Project [ctr_total_return#3, c_customer_id#78, c_current_addr_sk#81, c_salutation#84, c_first_name#85, c_last_name#86]
            :  +- Join Inner, (ctr_customer_sk#1 = c_customer_sk#77)
            :     :- Project [ctr_customer_sk#1, ctr_total_return#3]
            :     :  +- Join Inner, ((ctr_total_return#3 > (avg(ctr_total_return) * 1.2)#116) AND (ctr_state#2 = ctr_state#113))
            :     :     :- Filter isnotnull(ctr_total_return#3)
            :     :     :  +- Aggregate [cr_returning_customer_sk#16, ca_state#72], [cr_returning_customer_sk#16 AS ctr_customer_sk#1, ca_state#72 AS ctr_state#2, sum(cr_return_amt_inc_tax#29) AS ctr_total_return#3]
            :     :     :     +- Project [cr_returning_customer_sk#16, cr_return_amt_inc_tax#29, ca_state#72]
            :     :     :        +- Join Inner, (cr_returning_addr_sk#19 = ca_address_sk#64)
            :     :     :           :- Project [cr_returning_customer_sk#16, cr_returning_addr_sk#19, cr_return_amt_inc_tax#29]
            :     :     :           :  +- Join Inner, (cr_returned_date_sk#9 = d_date_sk#36)
            :     :     :           :     :- Project [cr_returned_date_sk#9, cr_returning_customer_sk#16, cr_returning_addr_sk#19, cr_return_amt_inc_tax#29]
            :     :     :           :     :  +- Filter ((isnotnull(cr_returned_date_sk#9) AND isnotnull(cr_returning_addr_sk#19)) AND isnotnull(cr_returning_customer_sk#16))
            :     :     :           :     :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#9,cr_returned_time_sk#10,cr_item_sk#11,cr_refunded_customer_sk#12,cr_refunded_cdemo_sk#13,cr_refunded_hdemo_sk#14,cr_refunded_addr_sk#15,cr_returning_customer_sk#16,cr_returning_cdemo_sk#17,cr_returning_hdemo_sk#18,cr_returning_addr_sk#19,cr_call_center_sk#20,cr_catalog_page_sk#21,cr_ship_mode_sk#22,cr_warehouse_sk#23,cr_reason_sk#24,cr_order_number#25,cr_return_quantity#26,cr_return_amount#27,cr_return_tax#28,cr_return_amt_inc_tax#29,cr_fee#30,cr_return_ship_cost#31,cr_refunded_cash#32,... 3 more fields] parquet
            :     :     :           :     +- Project [d_date_sk#36]
            :     :     :           :        +- Filter ((isnotnull(d_year#42) AND (d_year#42 = 2000)) AND isnotnull(d_date_sk#36))
            :     :     :           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#36,d_date_id#37,d_date#38,d_month_seq#39,d_week_seq#40,d_quarter_seq#41,d_year#42,d_dow#43,d_moy#44,d_dom#45,d_qoy#46,d_fy_year#47,d_fy_quarter_seq#48,d_fy_week_seq#49,d_day_name#50,d_quarter_name#51,d_holiday#52,d_weekend#53,d_following_holiday#54,d_first_dom#55,d_last_dom#56,d_same_day_ly#57,d_same_day_lq#58,d_current_day#59,... 4 more fields] parquet
            :     :     :           +- Project [ca_address_sk#64, ca_state#72]
            :     :     :              +- Filter (isnotnull(ca_address_sk#64) AND isnotnull(ca_state#72))
            :     :     :                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#64,ca_address_id#65,ca_street_number#66,ca_street_name#67,ca_street_type#68,ca_suite_number#69,ca_city#70,ca_county#71,ca_state#72,ca_zip#73,ca_country#74,ca_gmt_offset#75,ca_location_type#76] parquet
            :     :     +- Filter isnotnull((avg(ctr_total_return) * 1.2)#116)
            :     :        +- Aggregate [ctr_state#113], [(avg(ctr_total_return#114) * 1.2) AS (avg(ctr_total_return) * 1.2)#116, ctr_state#113]
            :     :           +- Aggregate [cr_returning_customer_sk#197, ca_state#253], [ca_state#253 AS ctr_state#113, sum(cr_return_amt_inc_tax#210) AS ctr_total_return#114]
            :     :              +- Project [cr_returning_customer_sk#197, cr_return_amt_inc_tax#210, ca_state#253]
            :     :                 +- Join Inner, (cr_returning_addr_sk#200 = ca_address_sk#245)
            :     :                    :- Project [cr_returning_customer_sk#197, cr_returning_addr_sk#200, cr_return_amt_inc_tax#210]
            :     :                    :  +- Join Inner, (cr_returned_date_sk#190 = d_date_sk#217)
            :     :                    :     :- Project [cr_returned_date_sk#190, cr_returning_customer_sk#197, cr_returning_addr_sk#200, cr_return_amt_inc_tax#210]
            :     :                    :     :  +- Filter (isnotnull(cr_returned_date_sk#190) AND isnotnull(cr_returning_addr_sk#200))
            :     :                    :     :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#190,cr_returned_time_sk#191,cr_item_sk#192,cr_refunded_customer_sk#193,cr_refunded_cdemo_sk#194,cr_refunded_hdemo_sk#195,cr_refunded_addr_sk#196,cr_returning_customer_sk#197,cr_returning_cdemo_sk#198,cr_returning_hdemo_sk#199,cr_returning_addr_sk#200,cr_call_center_sk#201,cr_catalog_page_sk#202,cr_ship_mode_sk#203,cr_warehouse_sk#204,cr_reason_sk#205,cr_order_number#206,cr_return_quantity#207,cr_return_amount#208,cr_return_tax#209,cr_return_amt_inc_tax#210,cr_fee#211,cr_return_ship_cost#212,cr_refunded_cash#213,... 3 more fields] parquet
            :     :                    :     +- Project [d_date_sk#217]
            :     :                    :        +- Filter ((isnotnull(d_year#223) AND (d_year#223 = 2000)) AND isnotnull(d_date_sk#217))
            :     :                    :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#217,d_date_id#218,d_date#219,d_month_seq#220,d_week_seq#221,d_quarter_seq#222,d_year#223,d_dow#224,d_moy#225,d_dom#226,d_qoy#227,d_fy_year#228,d_fy_quarter_seq#229,d_fy_week_seq#230,d_day_name#231,d_quarter_name#232,d_holiday#233,d_weekend#234,d_following_holiday#235,d_first_dom#236,d_last_dom#237,d_same_day_ly#238,d_same_day_lq#239,d_current_day#240,... 4 more fields] parquet
            :     :                    +- Project [ca_address_sk#245, ca_state#253]
            :     :                       +- Filter (isnotnull(ca_address_sk#245) AND isnotnull(ca_state#253))
            :     :                          +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#245,ca_address_id#246,ca_street_number#247,ca_street_name#248,ca_street_type#249,ca_suite_number#250,ca_city#251,ca_county#252,ca_state#253,ca_zip#254,ca_country#255,ca_gmt_offset#256,ca_location_type#257] parquet
            :     +- Project [c_customer_sk#77, c_customer_id#78, c_current_addr_sk#81, c_salutation#84, c_first_name#85, c_last_name#86]
            :        +- Filter (isnotnull(c_customer_sk#77) AND isnotnull(c_current_addr_sk#81))
            :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#77,c_customer_id#78,c_current_cdemo_sk#79,c_current_hdemo_sk#80,c_current_addr_sk#81,c_first_shipto_date_sk#82,c_first_sales_date_sk#83,c_salutation#84,c_first_name#85,c_last_name#86,c_preferred_cust_flag#87,c_birth_day#88,c_birth_month#89,c_birth_year#90,c_birth_country#91,c_login#92,c_email_address#93,c_last_review_date#94] parquet
            +- Project [ca_address_sk#95, ca_street_number#97, ca_street_name#98, ca_street_type#99, ca_suite_number#100, ca_city#101, ca_county#102, ca_state#103, ca_zip#104, ca_country#105, ca_gmt_offset#106, ca_location_type#107]
               +- Filter ((isnotnull(ca_state#103) AND (ca_state#103 = GA)) AND isnotnull(ca_address_sk#95))
                  +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#95,ca_address_id#96,ca_street_number#97,ca_street_name#98,ca_street_type#99,ca_suite_number#100,ca_city#101,ca_county#102,ca_state#103,ca_zip#104,ca_country#105,ca_gmt_offset#106,ca_location_type#107] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#78 ASC NULLS FIRST,c_salutation#84 ASC NULLS FIRST,c_first_name#85 ASC NULLS FIRST,c_last_name#86 ASC NULLS FIRST,ca_street_number#97 ASC NULLS FIRST,ca_street_name#98 ASC NULLS FIRST,ca_street_type#99 ASC NULLS FIRST,ca_suite_number#100 ASC NULLS FIRST,ca_city#101 ASC NULLS FIRST,ca_county#102 ASC NULLS FIRST,ca_state#103 ASC NULLS FIRST,ca_zip#104 ASC NULLS FIRST,ca_country#105 ASC NULLS FIRST,ca_gmt_offset#106 ASC NULLS FIRST,ca_location_type#107 ASC NULLS FIRST,ctr_total_return#3 ASC NULLS FIRST], output=[c_customer_id#78,c_salutation#84,c_first_name#85,c_last_name#86,ca_street_number#97,ca_street_name#98,ca_street_type#99,ca_suite_number#100,ca_city#101,ca_county#102,ca_state#103,ca_zip#104,ca_country#105,ca_gmt_offset#106,ca_location_type#107,ctr_total_return#3])
   +- Project [c_customer_id#78, c_salutation#84, c_first_name#85, c_last_name#86, ca_street_number#97, ca_street_name#98, ca_street_type#99, ca_suite_number#100, ca_city#101, ca_county#102, ca_state#103, ca_zip#104, ca_country#105, ca_gmt_offset#106, ca_location_type#107, ctr_total_return#3]
      +- BroadcastHashJoin [c_current_addr_sk#81], [ca_address_sk#95], Inner, BuildRight, false
         :- Project [ctr_total_return#3, c_customer_id#78, c_current_addr_sk#81, c_salutation#84, c_first_name#85, c_last_name#86]
         :  +- BroadcastHashJoin [ctr_customer_sk#1], [c_customer_sk#77], Inner, BuildRight, false
         :     :- Project [ctr_customer_sk#1, ctr_total_return#3]
         :     :  +- SortMergeJoin [ctr_state#2], [ctr_state#113], Inner, (ctr_total_return#3 > (avg(ctr_total_return) * 1.2)#116)
         :     :     :- Sort [ctr_state#2 ASC NULLS FIRST], false, 0
         :     :     :  +- Exchange hashpartitioning(ctr_state#2, 200), ENSURE_REQUIREMENTS, [plan_id=211]
         :     :     :     +- Filter isnotnull(ctr_total_return#3)
         :     :     :        +- HashAggregate(keys=[cr_returning_customer_sk#16, ca_state#72], functions=[sum(cr_return_amt_inc_tax#29)], output=[ctr_customer_sk#1, ctr_state#2, ctr_total_return#3])
         :     :     :           +- Exchange hashpartitioning(cr_returning_customer_sk#16, ca_state#72, 200), ENSURE_REQUIREMENTS, [plan_id=190]
         :     :     :              +- HashAggregate(keys=[cr_returning_customer_sk#16, ca_state#72], functions=[partial_sum(cr_return_amt_inc_tax#29)], output=[cr_returning_customer_sk#16, ca_state#72, sum#262])
         :     :     :                 +- Project [cr_returning_customer_sk#16, cr_return_amt_inc_tax#29, ca_state#72]
         :     :     :                    +- BroadcastHashJoin [cr_returning_addr_sk#19], [ca_address_sk#64], Inner, BuildRight, false
         :     :     :                       :- Project [cr_returning_customer_sk#16, cr_returning_addr_sk#19, cr_return_amt_inc_tax#29]
         :     :     :                       :  +- BroadcastHashJoin [cr_returned_date_sk#9], [d_date_sk#36], Inner, BuildRight, false
         :     :     :                       :     :- Filter ((isnotnull(cr_returned_date_sk#9) AND isnotnull(cr_returning_addr_sk#19)) AND isnotnull(cr_returning_customer_sk#16))
         :     :     :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#9,cr_returning_customer_sk#16,cr_returning_addr_sk#19,cr_return_amt_inc_tax#29] Batched: true, DataFilters: [isnotnull(cr_returned_date_sk#9), isnotnull(cr_returning_addr_sk#19), isnotnull(cr_returning_cus..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk), IsNotNull(cr_returning_addr_sk), IsNotNull(cr_returning_customer..., ReadSchema: struct<cr_returned_date_sk:int,cr_returning_customer_sk:int,cr_returning_addr_sk:int,cr_return_am...
         :     :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=181]
         :     :     :                       :        +- Project [d_date_sk#36]
         :     :     :                       :           +- Filter ((isnotnull(d_year#42) AND (d_year#42 = 2000)) AND isnotnull(d_date_sk#36))
         :     :     :                       :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#36,d_year#42] Batched: true, DataFilters: [isnotnull(d_year#42), (d_year#42 = 2000), isnotnull(d_date_sk#36)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=185]
         :     :     :                          +- Filter (isnotnull(ca_address_sk#64) AND isnotnull(ca_state#72))
         :     :     :                             +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#64,ca_state#72] Batched: true, DataFilters: [isnotnull(ca_address_sk#64), isnotnull(ca_state#72)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_state)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
         :     :     +- Sort [ctr_state#113 ASC NULLS FIRST], false, 0
         :     :        +- Filter isnotnull((avg(ctr_total_return) * 1.2)#116)
         :     :           +- HashAggregate(keys=[ctr_state#113], functions=[avg(ctr_total_return#114)], output=[(avg(ctr_total_return) * 1.2)#116, ctr_state#113])
         :     :              +- Exchange hashpartitioning(ctr_state#113, 200), ENSURE_REQUIREMENTS, [plan_id=206]
         :     :                 +- HashAggregate(keys=[ctr_state#113], functions=[partial_avg(ctr_total_return#114)], output=[ctr_state#113, sum#265, count#266L])
         :     :                    +- HashAggregate(keys=[cr_returning_customer_sk#197, ca_state#253], functions=[sum(cr_return_amt_inc_tax#210)], output=[ctr_state#113, ctr_total_return#114])
         :     :                       +- Exchange hashpartitioning(cr_returning_customer_sk#197, ca_state#253, 200), ENSURE_REQUIREMENTS, [plan_id=202]
         :     :                          +- HashAggregate(keys=[cr_returning_customer_sk#197, ca_state#253], functions=[partial_sum(cr_return_amt_inc_tax#210)], output=[cr_returning_customer_sk#197, ca_state#253, sum#268])
         :     :                             +- Project [cr_returning_customer_sk#197, cr_return_amt_inc_tax#210, ca_state#253]
         :     :                                +- BroadcastHashJoin [cr_returning_addr_sk#200], [ca_address_sk#245], Inner, BuildRight, false
         :     :                                   :- Project [cr_returning_customer_sk#197, cr_returning_addr_sk#200, cr_return_amt_inc_tax#210]
         :     :                                   :  +- BroadcastHashJoin [cr_returned_date_sk#190], [d_date_sk#217], Inner, BuildRight, false
         :     :                                   :     :- Filter (isnotnull(cr_returned_date_sk#190) AND isnotnull(cr_returning_addr_sk#200))
         :     :                                   :     :  +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#190,cr_returning_customer_sk#197,cr_returning_addr_sk#200,cr_return_amt_inc_tax#210] Batched: true, DataFilters: [isnotnull(cr_returned_date_sk#190), isnotnull(cr_returning_addr_sk#200)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk), IsNotNull(cr_returning_addr_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_returning_customer_sk:int,cr_returning_addr_sk:int,cr_return_am...
         :     :                                   :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=193]
         :     :                                   :        +- Project [d_date_sk#217]
         :     :                                   :           +- Filter ((isnotnull(d_year#223) AND (d_year#223 = 2000)) AND isnotnull(d_date_sk#217))
         :     :                                   :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#217,d_year#223] Batched: true, DataFilters: [isnotnull(d_year#223), (d_year#223 = 2000), isnotnull(d_date_sk#217)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     :                                   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=197]
         :     :                                      +- Filter (isnotnull(ca_address_sk#245) AND isnotnull(ca_state#253))
         :     :                                         +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#245,ca_state#253] Batched: true, DataFilters: [isnotnull(ca_address_sk#245), isnotnull(ca_state#253)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_state)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=217]
         :        +- Filter (isnotnull(c_customer_sk#77) AND isnotnull(c_current_addr_sk#81))
         :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#77,c_customer_id#78,c_current_addr_sk#81,c_salutation#84,c_first_name#85,c_last_name#86] Batched: true, DataFilters: [isnotnull(c_customer_sk#77), isnotnull(c_current_addr_sk#81)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_current_addr_sk:int,c_salutation:string,c_first_n...
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=221]
            +- Filter ((isnotnull(ca_state#103) AND (ca_state#103 = GA)) AND isnotnull(ca_address_sk#95))
               +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#95,ca_street_number#97,ca_street_name#98,ca_street_type#99,ca_suite_number#100,ca_city#101,ca_county#102,ca_state#103,ca_zip#104,ca_country#105,ca_gmt_offset#106,ca_location_type#107] Batched: true, DataFilters: [isnotnull(ca_state#103), (ca_state#103 = GA), isnotnull(ca_address_sk#95)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,GA), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_street_number:string,ca_street_name:string,ca_street_type:string,ca_s...

Time taken: 3.467 seconds, Fetched 1 row(s)
