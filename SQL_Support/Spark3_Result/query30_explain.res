Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580702294
== Parsed Logical Plan ==
CTE [customer_total_return]
:  +- 'SubqueryAlias customer_total_return
:     +- 'Aggregate ['wr_returning_customer_sk, 'ca_state], ['wr_returning_customer_sk AS ctr_customer_sk#1, 'ca_state AS ctr_state#2, 'sum('wr_return_amt) AS ctr_total_return#3]
:        +- 'Filter ((('wr_returned_date_sk = 'd_date_sk) AND ('d_year = 2002)) AND ('wr_returning_addr_sk = 'ca_address_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation [web_returns], [], false
:              :  +- 'UnresolvedRelation [date_dim], [], false
:              +- 'UnresolvedRelation [customer_address], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['c_customer_id ASC NULLS FIRST, 'c_salutation ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'c_last_name ASC NULLS FIRST, 'c_preferred_cust_flag ASC NULLS FIRST, 'c_birth_day ASC NULLS FIRST, 'c_birth_month ASC NULLS FIRST, 'c_birth_year ASC NULLS FIRST, 'c_birth_country ASC NULLS FIRST, 'c_login ASC NULLS FIRST, 'c_email_address ASC NULLS FIRST, 'c_last_review_date ASC NULLS FIRST, 'ctr_total_return ASC NULLS FIRST], true
         +- 'Project ['c_customer_id, 'c_salutation, 'c_first_name, 'c_last_name, 'c_preferred_cust_flag, 'c_birth_day, 'c_birth_month, 'c_birth_year, 'c_birth_country, 'c_login, 'c_email_address, 'c_last_review_date, 'ctr_total_return]
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
c_customer_id: string, c_salutation: string, c_first_name: string, c_last_name: string, c_preferred_cust_flag: string, c_birth_day: int, c_birth_month: int, c_birth_year: int, c_birth_country: string, c_login: string, c_email_address: string, c_last_review_date: string, ctr_total_return: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias customer_total_return
:     +- Aggregate [wr_returning_customer_sk#16, ca_state#69], [wr_returning_customer_sk#16 AS ctr_customer_sk#1, ca_state#69 AS ctr_state#2, sum(wr_return_amt#24) AS ctr_total_return#3]
:        +- Filter (((wr_returned_date_sk#9 = d_date_sk#33) AND (d_year#39 = 2002)) AND (wr_returning_addr_sk#19 = ca_address_sk#61))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.web_returns
:              :  :  +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#9,wr_returned_time_sk#10,wr_item_sk#11,wr_refunded_customer_sk#12,wr_refunded_cdemo_sk#13,wr_refunded_hdemo_sk#14,wr_refunded_addr_sk#15,wr_returning_customer_sk#16,wr_returning_cdemo_sk#17,wr_returning_hdemo_sk#18,wr_returning_addr_sk#19,wr_web_page_sk#20,wr_reason_sk#21,wr_order_number#22,wr_return_quantity#23,wr_return_amt#24,wr_return_tax#25,wr_return_amt_inc_tax#26,wr_fee#27,wr_return_ship_cost#28,wr_refunded_cash#29,wr_reversed_charge#30,wr_account_credit#31,wr_net_loss#32] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.customer_address
:                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [c_customer_id#75 ASC NULLS FIRST, c_salutation#81 ASC NULLS FIRST, c_first_name#82 ASC NULLS FIRST, c_last_name#83 ASC NULLS FIRST, c_preferred_cust_flag#84 ASC NULLS FIRST, c_birth_day#85 ASC NULLS FIRST, c_birth_month#86 ASC NULLS FIRST, c_birth_year#87 ASC NULLS FIRST, c_birth_country#88 ASC NULLS FIRST, c_login#89 ASC NULLS FIRST, c_email_address#90 ASC NULLS FIRST, c_last_review_date#91 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
         +- Project [c_customer_id#75, c_salutation#81, c_first_name#82, c_last_name#83, c_preferred_cust_flag#84, c_birth_day#85, c_birth_month#86, c_birth_year#87, c_birth_country#88, c_login#89, c_email_address#90, c_last_review_date#91, ctr_total_return#3]
            +- Filter (((ctr_total_return#3 > scalar-subquery#0 [ctr_state#2]) AND (ca_address_sk#92 = c_current_addr_sk#78)) AND ((ca_state#100 = GA) AND (ctr_customer_sk#1 = c_customer_sk#74)))
               :  +- Aggregate [(avg(ctr_total_return#111) * cast(1.2 as double)) AS (avg(ctr_total_return) * 1.2)#113]
               :     +- Filter (outer(ctr_state#2) = ctr_state#110)
               :        +- SubqueryAlias ctr2
               :           +- SubqueryAlias customer_total_return
               :              +- CTERelationRef 0, true, [ctr_customer_sk#109, ctr_state#110, ctr_total_return#111], false
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias ctr1
                  :  :  +- SubqueryAlias customer_total_return
                  :  :     +- CTERelationRef 0, true, [ctr_customer_sk#1, ctr_state#2, ctr_total_return#3], false
                  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
                  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#92,ca_address_id#93,ca_street_number#94,ca_street_name#95,ca_street_type#96,ca_suite_number#97,ca_city#98,ca_county#99,ca_state#100,ca_zip#101,ca_country#102,ca_gmt_offset#103,ca_location_type#104] parquet
                  +- SubqueryAlias spark_catalog.tpcds.customer
                     +- Relation spark_catalog.tpcds.customer[c_customer_sk#74,c_customer_id#75,c_current_cdemo_sk#76,c_current_hdemo_sk#77,c_current_addr_sk#78,c_first_shipto_date_sk#79,c_first_sales_date_sk#80,c_salutation#81,c_first_name#82,c_last_name#83,c_preferred_cust_flag#84,c_birth_day#85,c_birth_month#86,c_birth_year#87,c_birth_country#88,c_login#89,c_email_address#90,c_last_review_date#91] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#75 ASC NULLS FIRST, c_salutation#81 ASC NULLS FIRST, c_first_name#82 ASC NULLS FIRST, c_last_name#83 ASC NULLS FIRST, c_preferred_cust_flag#84 ASC NULLS FIRST, c_birth_day#85 ASC NULLS FIRST, c_birth_month#86 ASC NULLS FIRST, c_birth_year#87 ASC NULLS FIRST, c_birth_country#88 ASC NULLS FIRST, c_login#89 ASC NULLS FIRST, c_email_address#90 ASC NULLS FIRST, c_last_review_date#91 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
      +- Project [c_customer_id#75, c_salutation#81, c_first_name#82, c_last_name#83, c_preferred_cust_flag#84, c_birth_day#85, c_birth_month#86, c_birth_year#87, c_birth_country#88, c_login#89, c_email_address#90, c_last_review_date#91, ctr_total_return#3]
         +- Join Inner, (ca_address_sk#92 = c_current_addr_sk#78)
            :- Project [ctr_total_return#3, c_customer_id#75, c_current_addr_sk#78, c_salutation#81, c_first_name#82, c_last_name#83, c_preferred_cust_flag#84, c_birth_day#85, c_birth_month#86, c_birth_year#87, c_birth_country#88, c_login#89, c_email_address#90, c_last_review_date#91]
            :  +- Join Inner, (ctr_customer_sk#1 = c_customer_sk#74)
            :     :- Project [ctr_customer_sk#1, ctr_total_return#3]
            :     :  +- Join Inner, ((ctr_total_return#3 > (avg(ctr_total_return) * 1.2)#113) AND (ctr_state#2 = ctr_state#110))
            :     :     :- Filter isnotnull(ctr_total_return#3)
            :     :     :  +- Aggregate [wr_returning_customer_sk#16, ca_state#69], [wr_returning_customer_sk#16 AS ctr_customer_sk#1, ca_state#69 AS ctr_state#2, sum(wr_return_amt#24) AS ctr_total_return#3]
            :     :     :     +- Project [wr_returning_customer_sk#16, wr_return_amt#24, ca_state#69]
            :     :     :        +- Join Inner, (wr_returning_addr_sk#19 = ca_address_sk#61)
            :     :     :           :- Project [wr_returning_customer_sk#16, wr_returning_addr_sk#19, wr_return_amt#24]
            :     :     :           :  +- Join Inner, (wr_returned_date_sk#9 = d_date_sk#33)
            :     :     :           :     :- Project [wr_returned_date_sk#9, wr_returning_customer_sk#16, wr_returning_addr_sk#19, wr_return_amt#24]
            :     :     :           :     :  +- Filter ((isnotnull(wr_returned_date_sk#9) AND isnotnull(wr_returning_addr_sk#19)) AND isnotnull(wr_returning_customer_sk#16))
            :     :     :           :     :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#9,wr_returned_time_sk#10,wr_item_sk#11,wr_refunded_customer_sk#12,wr_refunded_cdemo_sk#13,wr_refunded_hdemo_sk#14,wr_refunded_addr_sk#15,wr_returning_customer_sk#16,wr_returning_cdemo_sk#17,wr_returning_hdemo_sk#18,wr_returning_addr_sk#19,wr_web_page_sk#20,wr_reason_sk#21,wr_order_number#22,wr_return_quantity#23,wr_return_amt#24,wr_return_tax#25,wr_return_amt_inc_tax#26,wr_fee#27,wr_return_ship_cost#28,wr_refunded_cash#29,wr_reversed_charge#30,wr_account_credit#31,wr_net_loss#32] parquet
            :     :     :           :     +- Project [d_date_sk#33]
            :     :     :           :        +- Filter ((isnotnull(d_year#39) AND (d_year#39 = 2002)) AND isnotnull(d_date_sk#33))
            :     :     :           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
            :     :     :           +- Project [ca_address_sk#61, ca_state#69]
            :     :     :              +- Filter (isnotnull(ca_address_sk#61) AND isnotnull(ca_state#69))
            :     :     :                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet
            :     :     +- Filter isnotnull((avg(ctr_total_return) * 1.2)#113)
            :     :        +- Aggregate [ctr_state#110], [(avg(ctr_total_return#111) * 1.2) AS (avg(ctr_total_return) * 1.2)#113, ctr_state#110]
            :     :           +- Aggregate [wr_returning_customer_sk#191, ca_state#244], [ca_state#244 AS ctr_state#110, sum(wr_return_amt#199) AS ctr_total_return#111]
            :     :              +- Project [wr_returning_customer_sk#191, wr_return_amt#199, ca_state#244]
            :     :                 +- Join Inner, (wr_returning_addr_sk#194 = ca_address_sk#236)
            :     :                    :- Project [wr_returning_customer_sk#191, wr_returning_addr_sk#194, wr_return_amt#199]
            :     :                    :  +- Join Inner, (wr_returned_date_sk#184 = d_date_sk#208)
            :     :                    :     :- Project [wr_returned_date_sk#184, wr_returning_customer_sk#191, wr_returning_addr_sk#194, wr_return_amt#199]
            :     :                    :     :  +- Filter (isnotnull(wr_returned_date_sk#184) AND isnotnull(wr_returning_addr_sk#194))
            :     :                    :     :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#184,wr_returned_time_sk#185,wr_item_sk#186,wr_refunded_customer_sk#187,wr_refunded_cdemo_sk#188,wr_refunded_hdemo_sk#189,wr_refunded_addr_sk#190,wr_returning_customer_sk#191,wr_returning_cdemo_sk#192,wr_returning_hdemo_sk#193,wr_returning_addr_sk#194,wr_web_page_sk#195,wr_reason_sk#196,wr_order_number#197,wr_return_quantity#198,wr_return_amt#199,wr_return_tax#200,wr_return_amt_inc_tax#201,wr_fee#202,wr_return_ship_cost#203,wr_refunded_cash#204,wr_reversed_charge#205,wr_account_credit#206,wr_net_loss#207] parquet
            :     :                    :     +- Project [d_date_sk#208]
            :     :                    :        +- Filter ((isnotnull(d_year#214) AND (d_year#214 = 2002)) AND isnotnull(d_date_sk#208))
            :     :                    :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#208,d_date_id#209,d_date#210,d_month_seq#211,d_week_seq#212,d_quarter_seq#213,d_year#214,d_dow#215,d_moy#216,d_dom#217,d_qoy#218,d_fy_year#219,d_fy_quarter_seq#220,d_fy_week_seq#221,d_day_name#222,d_quarter_name#223,d_holiday#224,d_weekend#225,d_following_holiday#226,d_first_dom#227,d_last_dom#228,d_same_day_ly#229,d_same_day_lq#230,d_current_day#231,... 4 more fields] parquet
            :     :                    +- Project [ca_address_sk#236, ca_state#244]
            :     :                       +- Filter (isnotnull(ca_address_sk#236) AND isnotnull(ca_state#244))
            :     :                          +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#236,ca_address_id#237,ca_street_number#238,ca_street_name#239,ca_street_type#240,ca_suite_number#241,ca_city#242,ca_county#243,ca_state#244,ca_zip#245,ca_country#246,ca_gmt_offset#247,ca_location_type#248] parquet
            :     +- Project [c_customer_sk#74, c_customer_id#75, c_current_addr_sk#78, c_salutation#81, c_first_name#82, c_last_name#83, c_preferred_cust_flag#84, c_birth_day#85, c_birth_month#86, c_birth_year#87, c_birth_country#88, c_login#89, c_email_address#90, c_last_review_date#91]
            :        +- Filter (isnotnull(c_customer_sk#74) AND isnotnull(c_current_addr_sk#78))
            :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#74,c_customer_id#75,c_current_cdemo_sk#76,c_current_hdemo_sk#77,c_current_addr_sk#78,c_first_shipto_date_sk#79,c_first_sales_date_sk#80,c_salutation#81,c_first_name#82,c_last_name#83,c_preferred_cust_flag#84,c_birth_day#85,c_birth_month#86,c_birth_year#87,c_birth_country#88,c_login#89,c_email_address#90,c_last_review_date#91] parquet
            +- Project [ca_address_sk#92]
               +- Filter ((isnotnull(ca_state#100) AND (ca_state#100 = GA)) AND isnotnull(ca_address_sk#92))
                  +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#92,ca_address_id#93,ca_street_number#94,ca_street_name#95,ca_street_type#96,ca_suite_number#97,ca_city#98,ca_county#99,ca_state#100,ca_zip#101,ca_country#102,ca_gmt_offset#103,ca_location_type#104] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#75 ASC NULLS FIRST,c_salutation#81 ASC NULLS FIRST,c_first_name#82 ASC NULLS FIRST,c_last_name#83 ASC NULLS FIRST,c_preferred_cust_flag#84 ASC NULLS FIRST,c_birth_day#85 ASC NULLS FIRST,c_birth_month#86 ASC NULLS FIRST,c_birth_year#87 ASC NULLS FIRST,c_birth_country#88 ASC NULLS FIRST,c_login#89 ASC NULLS FIRST,c_email_address#90 ASC NULLS FIRST,c_last_review_date#91 ASC NULLS FIRST,ctr_total_return#3 ASC NULLS FIRST], output=[c_customer_id#75,c_salutation#81,c_first_name#82,c_last_name#83,c_preferred_cust_flag#84,c_birth_day#85,c_birth_month#86,c_birth_year#87,c_birth_country#88,c_login#89,c_email_address#90,c_last_review_date#91,ctr_total_return#3])
   +- Project [c_customer_id#75, c_salutation#81, c_first_name#82, c_last_name#83, c_preferred_cust_flag#84, c_birth_day#85, c_birth_month#86, c_birth_year#87, c_birth_country#88, c_login#89, c_email_address#90, c_last_review_date#91, ctr_total_return#3]
      +- BroadcastHashJoin [c_current_addr_sk#78], [ca_address_sk#92], Inner, BuildRight, false
         :- Project [ctr_total_return#3, c_customer_id#75, c_current_addr_sk#78, c_salutation#81, c_first_name#82, c_last_name#83, c_preferred_cust_flag#84, c_birth_day#85, c_birth_month#86, c_birth_year#87, c_birth_country#88, c_login#89, c_email_address#90, c_last_review_date#91]
         :  +- BroadcastHashJoin [ctr_customer_sk#1], [c_customer_sk#74], Inner, BuildRight, false
         :     :- Project [ctr_customer_sk#1, ctr_total_return#3]
         :     :  +- SortMergeJoin [ctr_state#2], [ctr_state#110], Inner, (ctr_total_return#3 > (avg(ctr_total_return) * 1.2)#113)
         :     :     :- Sort [ctr_state#2 ASC NULLS FIRST], false, 0
         :     :     :  +- Exchange hashpartitioning(ctr_state#2, 200), ENSURE_REQUIREMENTS, [plan_id=211]
         :     :     :     +- Filter isnotnull(ctr_total_return#3)
         :     :     :        +- HashAggregate(keys=[wr_returning_customer_sk#16, ca_state#69], functions=[sum(wr_return_amt#24)], output=[ctr_customer_sk#1, ctr_state#2, ctr_total_return#3])
         :     :     :           +- Exchange hashpartitioning(wr_returning_customer_sk#16, ca_state#69, 200), ENSURE_REQUIREMENTS, [plan_id=190]
         :     :     :              +- HashAggregate(keys=[wr_returning_customer_sk#16, ca_state#69], functions=[partial_sum(wr_return_amt#24)], output=[wr_returning_customer_sk#16, ca_state#69, sum#253])
         :     :     :                 +- Project [wr_returning_customer_sk#16, wr_return_amt#24, ca_state#69]
         :     :     :                    +- BroadcastHashJoin [wr_returning_addr_sk#19], [ca_address_sk#61], Inner, BuildRight, false
         :     :     :                       :- Project [wr_returning_customer_sk#16, wr_returning_addr_sk#19, wr_return_amt#24]
         :     :     :                       :  +- BroadcastHashJoin [wr_returned_date_sk#9], [d_date_sk#33], Inner, BuildRight, false
         :     :     :                       :     :- Filter ((isnotnull(wr_returned_date_sk#9) AND isnotnull(wr_returning_addr_sk#19)) AND isnotnull(wr_returning_customer_sk#16))
         :     :     :                       :     :  +- FileScan parquet spark_catalog.tpcds.web_returns[wr_returned_date_sk#9,wr_returning_customer_sk#16,wr_returning_addr_sk#19,wr_return_amt#24] Batched: true, DataFilters: [isnotnull(wr_returned_date_sk#9), isnotnull(wr_returning_addr_sk#19), isnotnull(wr_returning_cus..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk), IsNotNull(wr_returning_addr_sk), IsNotNull(wr_returning_customer..., ReadSchema: struct<wr_returned_date_sk:int,wr_returning_customer_sk:int,wr_returning_addr_sk:int,wr_return_am...
         :     :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=181]
         :     :     :                       :        +- Project [d_date_sk#33]
         :     :     :                       :           +- Filter ((isnotnull(d_year#39) AND (d_year#39 = 2002)) AND isnotnull(d_date_sk#33))
         :     :     :                       :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#33,d_year#39] Batched: true, DataFilters: [isnotnull(d_year#39), (d_year#39 = 2002), isnotnull(d_date_sk#33)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=185]
         :     :     :                          +- Filter (isnotnull(ca_address_sk#61) AND isnotnull(ca_state#69))
         :     :     :                             +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#61,ca_state#69] Batched: true, DataFilters: [isnotnull(ca_address_sk#61), isnotnull(ca_state#69)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_state)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
         :     :     +- Sort [ctr_state#110 ASC NULLS FIRST], false, 0
         :     :        +- Filter isnotnull((avg(ctr_total_return) * 1.2)#113)
         :     :           +- HashAggregate(keys=[ctr_state#110], functions=[avg(ctr_total_return#111)], output=[(avg(ctr_total_return) * 1.2)#113, ctr_state#110])
         :     :              +- Exchange hashpartitioning(ctr_state#110, 200), ENSURE_REQUIREMENTS, [plan_id=206]
         :     :                 +- HashAggregate(keys=[ctr_state#110], functions=[partial_avg(ctr_total_return#111)], output=[ctr_state#110, sum#256, count#257L])
         :     :                    +- HashAggregate(keys=[wr_returning_customer_sk#191, ca_state#244], functions=[sum(wr_return_amt#199)], output=[ctr_state#110, ctr_total_return#111])
         :     :                       +- Exchange hashpartitioning(wr_returning_customer_sk#191, ca_state#244, 200), ENSURE_REQUIREMENTS, [plan_id=202]
         :     :                          +- HashAggregate(keys=[wr_returning_customer_sk#191, ca_state#244], functions=[partial_sum(wr_return_amt#199)], output=[wr_returning_customer_sk#191, ca_state#244, sum#259])
         :     :                             +- Project [wr_returning_customer_sk#191, wr_return_amt#199, ca_state#244]
         :     :                                +- BroadcastHashJoin [wr_returning_addr_sk#194], [ca_address_sk#236], Inner, BuildRight, false
         :     :                                   :- Project [wr_returning_customer_sk#191, wr_returning_addr_sk#194, wr_return_amt#199]
         :     :                                   :  +- BroadcastHashJoin [wr_returned_date_sk#184], [d_date_sk#208], Inner, BuildRight, false
         :     :                                   :     :- Filter (isnotnull(wr_returned_date_sk#184) AND isnotnull(wr_returning_addr_sk#194))
         :     :                                   :     :  +- FileScan parquet spark_catalog.tpcds.web_returns[wr_returned_date_sk#184,wr_returning_customer_sk#191,wr_returning_addr_sk#194,wr_return_amt#199] Batched: true, DataFilters: [isnotnull(wr_returned_date_sk#184), isnotnull(wr_returning_addr_sk#194)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk), IsNotNull(wr_returning_addr_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_returning_customer_sk:int,wr_returning_addr_sk:int,wr_return_am...
         :     :                                   :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=193]
         :     :                                   :        +- Project [d_date_sk#208]
         :     :                                   :           +- Filter ((isnotnull(d_year#214) AND (d_year#214 = 2002)) AND isnotnull(d_date_sk#208))
         :     :                                   :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#208,d_year#214] Batched: true, DataFilters: [isnotnull(d_year#214), (d_year#214 = 2002), isnotnull(d_date_sk#208)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     :                                   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=197]
         :     :                                      +- Filter (isnotnull(ca_address_sk#236) AND isnotnull(ca_state#244))
         :     :                                         +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#236,ca_state#244] Batched: true, DataFilters: [isnotnull(ca_address_sk#236), isnotnull(ca_state#244)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_state)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=217]
         :        +- Filter (isnotnull(c_customer_sk#74) AND isnotnull(c_current_addr_sk#78))
         :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#74,c_customer_id#75,c_current_addr_sk#78,c_salutation#81,c_first_name#82,c_last_name#83,c_preferred_cust_flag#84,c_birth_day#85,c_birth_month#86,c_birth_year#87,c_birth_country#88,c_login#89,c_email_address#90,c_last_review_date#91] Batched: true, DataFilters: [isnotnull(c_customer_sk#74), isnotnull(c_current_addr_sk#78)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_current_addr_sk:int,c_salutation:string,c_first_n...
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=221]
            +- Project [ca_address_sk#92]
               +- Filter ((isnotnull(ca_state#100) AND (ca_state#100 = GA)) AND isnotnull(ca_address_sk#92))
                  +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#92,ca_state#100] Batched: true, DataFilters: [isnotnull(ca_state#100), (ca_state#100 = GA), isnotnull(ca_address_sk#92)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,GA), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>

Time taken: 3.054 seconds, Fetched 1 row(s)
