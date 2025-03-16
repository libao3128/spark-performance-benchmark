Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579469250
== Parsed Logical Plan ==
CTE [customer_total_return]
:  +- 'SubqueryAlias customer_total_return
:     +- 'Aggregate ['sr_customer_sk, 'sr_store_sk], ['sr_customer_sk AS ctr_customer_sk#1, 'sr_store_sk AS ctr_store_sk#2, 'sum('SR_RETURN_AMT) AS ctr_total_return#3]
:        +- 'Filter (('sr_returned_date_sk = 'd_date_sk) AND ('d_year = 2000))
:           +- 'Join Inner
:              :- 'UnresolvedRelation [store_returns], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['c_customer_id ASC NULLS FIRST], true
         +- 'Project ['c_customer_id]
            +- 'Filter ((('ctr1.ctr_total_return > scalar-subquery#0 []) AND ('s_store_sk = 'ctr1.ctr_store_sk)) AND (('s_state = TN) AND ('ctr1.ctr_customer_sk = 'c_customer_sk)))
               :  +- 'Project [unresolvedalias(('avg('ctr_total_return) * 1.2), None)]
               :     +- 'Filter ('ctr1.ctr_store_sk = 'ctr2.ctr_store_sk)
               :        +- 'SubqueryAlias ctr2
               :           +- 'UnresolvedRelation [customer_total_return], [], false
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'SubqueryAlias ctr1
                  :  :  +- 'UnresolvedRelation [customer_total_return], [], false
                  :  +- 'UnresolvedRelation [store], [], false
                  +- 'UnresolvedRelation [customer], [], false

== Analyzed Logical Plan ==
c_customer_id: string
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias customer_total_return
:     +- Aggregate [sr_customer_sk#12, sr_store_sk#16], [sr_customer_sk#12 AS ctr_customer_sk#1, sr_store_sk#16 AS ctr_store_sk#2, sum(SR_RETURN_AMT#20) AS ctr_total_return#3]
:        +- Filter ((sr_returned_date_sk#9 = d_date_sk#29) AND (d_year#35 = 2000))
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.store_returns
:              :  +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#9,sr_return_time_sk#10,sr_item_sk#11,sr_customer_sk#12,sr_cdemo_sk#13,sr_hdemo_sk#14,sr_addr_sk#15,sr_store_sk#16,sr_reason_sk#17,sr_ticket_number#18,sr_return_quantity#19,sr_return_amt#20,sr_return_tax#21,sr_return_amt_inc_tax#22,sr_fee#23,sr_return_ship_cost#24,sr_refunded_cash#25,sr_reversed_charge#26,sr_store_credit#27,sr_net_loss#28] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#29,d_date_id#30,d_date#31,d_month_seq#32,d_week_seq#33,d_quarter_seq#34,d_year#35,d_dow#36,d_moy#37,d_dom#38,d_qoy#39,d_fy_year#40,d_fy_quarter_seq#41,d_fy_week_seq#42,d_day_name#43,d_quarter_name#44,d_holiday#45,d_weekend#46,d_following_holiday#47,d_first_dom#48,d_last_dom#49,d_same_day_ly#50,d_same_day_lq#51,d_current_day#52,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [c_customer_id#87 ASC NULLS FIRST], true
         +- Project [c_customer_id#87]
            +- Filter (((ctr_total_return#3 > scalar-subquery#0 [ctr_store_sk#2]) AND (s_store_sk#57 = ctr_store_sk#2)) AND ((s_state#81 = TN) AND (ctr_customer_sk#1 = c_customer_sk#86)))
               :  +- Aggregate [(avg(ctr_total_return#109) * cast(1.2 as double)) AS (avg(ctr_total_return) * 1.2)#111]
               :     +- Filter (outer(ctr_store_sk#2) = ctr_store_sk#108)
               :        +- SubqueryAlias ctr2
               :           +- SubqueryAlias customer_total_return
               :              +- CTERelationRef 0, true, [ctr_customer_sk#107, ctr_store_sk#108, ctr_total_return#109], false
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias ctr1
                  :  :  +- SubqueryAlias customer_total_return
                  :  :     +- CTERelationRef 0, true, [ctr_customer_sk#1, ctr_store_sk#2, ctr_total_return#3], false
                  :  +- SubqueryAlias spark_catalog.tpcds.store
                  :     +- Relation spark_catalog.tpcds.store[s_store_sk#57,s_store_id#58,s_rec_start_date#59,s_rec_end_date#60,s_closed_date_sk#61,s_store_name#62,s_number_employees#63,s_floor_space#64,s_hours#65,s_manager#66,s_market_id#67,s_geography_class#68,s_market_desc#69,s_market_manager#70,s_division_id#71,s_division_name#72,s_company_id#73,s_company_name#74,s_street_number#75,s_street_name#76,s_street_type#77,s_suite_number#78,s_city#79,s_county#80,... 5 more fields] parquet
                  +- SubqueryAlias spark_catalog.tpcds.customer
                     +- Relation spark_catalog.tpcds.customer[c_customer_sk#86,c_customer_id#87,c_current_cdemo_sk#88,c_current_hdemo_sk#89,c_current_addr_sk#90,c_first_shipto_date_sk#91,c_first_sales_date_sk#92,c_salutation#93,c_first_name#94,c_last_name#95,c_preferred_cust_flag#96,c_birth_day#97,c_birth_month#98,c_birth_year#99,c_birth_country#100,c_login#101,c_email_address#102,c_last_review_date#103] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#87 ASC NULLS FIRST], true
      +- Project [c_customer_id#87]
         +- Join Inner, (ctr_customer_sk#1 = c_customer_sk#86)
            :- Project [ctr_customer_sk#1]
            :  +- Join Inner, (s_store_sk#57 = ctr_store_sk#2)
            :     :- Project [ctr_customer_sk#1, ctr_store_sk#2]
            :     :  +- Join Inner, ((ctr_total_return#3 > (avg(ctr_total_return) * 1.2)#111) AND (ctr_store_sk#2 = ctr_store_sk#108))
            :     :     :- Filter isnotnull(ctr_total_return#3)
            :     :     :  +- Aggregate [sr_customer_sk#12, sr_store_sk#16], [sr_customer_sk#12 AS ctr_customer_sk#1, sr_store_sk#16 AS ctr_store_sk#2, sum(SR_RETURN_AMT#20) AS ctr_total_return#3]
            :     :     :     +- Project [sr_customer_sk#12, sr_store_sk#16, sr_return_amt#20]
            :     :     :        +- Join Inner, (sr_returned_date_sk#9 = d_date_sk#29)
            :     :     :           :- Project [sr_returned_date_sk#9, sr_customer_sk#12, sr_store_sk#16, sr_return_amt#20]
            :     :     :           :  +- Filter (isnotnull(sr_returned_date_sk#9) AND (isnotnull(sr_store_sk#16) AND isnotnull(sr_customer_sk#12)))
            :     :     :           :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#9,sr_return_time_sk#10,sr_item_sk#11,sr_customer_sk#12,sr_cdemo_sk#13,sr_hdemo_sk#14,sr_addr_sk#15,sr_store_sk#16,sr_reason_sk#17,sr_ticket_number#18,sr_return_quantity#19,sr_return_amt#20,sr_return_tax#21,sr_return_amt_inc_tax#22,sr_fee#23,sr_return_ship_cost#24,sr_refunded_cash#25,sr_reversed_charge#26,sr_store_credit#27,sr_net_loss#28] parquet
            :     :     :           +- Project [d_date_sk#29]
            :     :     :              +- Filter ((isnotnull(d_year#35) AND (d_year#35 = 2000)) AND isnotnull(d_date_sk#29))
            :     :     :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#29,d_date_id#30,d_date#31,d_month_seq#32,d_week_seq#33,d_quarter_seq#34,d_year#35,d_dow#36,d_moy#37,d_dom#38,d_qoy#39,d_fy_year#40,d_fy_quarter_seq#41,d_fy_week_seq#42,d_day_name#43,d_quarter_name#44,d_holiday#45,d_weekend#46,d_following_holiday#47,d_first_dom#48,d_last_dom#49,d_same_day_ly#50,d_same_day_lq#51,d_current_day#52,... 4 more fields] parquet
            :     :     +- Filter isnotnull((avg(ctr_total_return) * 1.2)#111)
            :     :        +- Aggregate [ctr_store_sk#108], [(avg(ctr_total_return#109) * 1.2) AS (avg(ctr_total_return) * 1.2)#111, ctr_store_sk#108]
            :     :           +- Aggregate [sr_customer_sk#168, sr_store_sk#172], [sr_store_sk#172 AS ctr_store_sk#108, sum(SR_RETURN_AMT#176) AS ctr_total_return#109]
            :     :              +- Project [sr_customer_sk#168, sr_store_sk#172, sr_return_amt#176]
            :     :                 +- Join Inner, (sr_returned_date_sk#165 = d_date_sk#185)
            :     :                    :- Project [sr_returned_date_sk#165, sr_customer_sk#168, sr_store_sk#172, sr_return_amt#176]
            :     :                    :  +- Filter (isnotnull(sr_returned_date_sk#165) AND isnotnull(sr_store_sk#172))
            :     :                    :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#165,sr_return_time_sk#166,sr_item_sk#167,sr_customer_sk#168,sr_cdemo_sk#169,sr_hdemo_sk#170,sr_addr_sk#171,sr_store_sk#172,sr_reason_sk#173,sr_ticket_number#174,sr_return_quantity#175,sr_return_amt#176,sr_return_tax#177,sr_return_amt_inc_tax#178,sr_fee#179,sr_return_ship_cost#180,sr_refunded_cash#181,sr_reversed_charge#182,sr_store_credit#183,sr_net_loss#184] parquet
            :     :                    +- Project [d_date_sk#185]
            :     :                       +- Filter ((isnotnull(d_year#191) AND (d_year#191 = 2000)) AND isnotnull(d_date_sk#185))
            :     :                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#185,d_date_id#186,d_date#187,d_month_seq#188,d_week_seq#189,d_quarter_seq#190,d_year#191,d_dow#192,d_moy#193,d_dom#194,d_qoy#195,d_fy_year#196,d_fy_quarter_seq#197,d_fy_week_seq#198,d_day_name#199,d_quarter_name#200,d_holiday#201,d_weekend#202,d_following_holiday#203,d_first_dom#204,d_last_dom#205,d_same_day_ly#206,d_same_day_lq#207,d_current_day#208,... 4 more fields] parquet
            :     +- Project [s_store_sk#57]
            :        +- Filter ((isnotnull(s_state#81) AND (s_state#81 = TN)) AND isnotnull(s_store_sk#57))
            :           +- Relation spark_catalog.tpcds.store[s_store_sk#57,s_store_id#58,s_rec_start_date#59,s_rec_end_date#60,s_closed_date_sk#61,s_store_name#62,s_number_employees#63,s_floor_space#64,s_hours#65,s_manager#66,s_market_id#67,s_geography_class#68,s_market_desc#69,s_market_manager#70,s_division_id#71,s_division_name#72,s_company_id#73,s_company_name#74,s_street_number#75,s_street_name#76,s_street_type#77,s_suite_number#78,s_city#79,s_county#80,... 5 more fields] parquet
            +- Project [c_customer_sk#86, c_customer_id#87]
               +- Filter isnotnull(c_customer_sk#86)
                  +- Relation spark_catalog.tpcds.customer[c_customer_sk#86,c_customer_id#87,c_current_cdemo_sk#88,c_current_hdemo_sk#89,c_current_addr_sk#90,c_first_shipto_date_sk#91,c_first_sales_date_sk#92,c_salutation#93,c_first_name#94,c_last_name#95,c_preferred_cust_flag#96,c_birth_day#97,c_birth_month#98,c_birth_year#99,c_birth_country#100,c_login#101,c_email_address#102,c_last_review_date#103] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#87 ASC NULLS FIRST], output=[c_customer_id#87])
   +- Project [c_customer_id#87]
      +- BroadcastHashJoin [ctr_customer_sk#1], [c_customer_sk#86], Inner, BuildRight, false
         :- Project [ctr_customer_sk#1]
         :  +- BroadcastHashJoin [ctr_store_sk#2], [s_store_sk#57], Inner, BuildRight, false
         :     :- Project [ctr_customer_sk#1, ctr_store_sk#2]
         :     :  +- SortMergeJoin [ctr_store_sk#2], [ctr_store_sk#108], Inner, (ctr_total_return#3 > (avg(ctr_total_return) * 1.2)#111)
         :     :     :- Sort [ctr_store_sk#2 ASC NULLS FIRST], false, 0
         :     :     :  +- Exchange hashpartitioning(ctr_store_sk#2, 200), ENSURE_REQUIREMENTS, [plan_id=167]
         :     :     :     +- Filter isnotnull(ctr_total_return#3)
         :     :     :        +- HashAggregate(keys=[sr_customer_sk#12, sr_store_sk#16], functions=[sum(SR_RETURN_AMT#20)], output=[ctr_customer_sk#1, ctr_store_sk#2, ctr_total_return#3])
         :     :     :           +- Exchange hashpartitioning(sr_customer_sk#12, sr_store_sk#16, 200), ENSURE_REQUIREMENTS, [plan_id=150]
         :     :     :              +- HashAggregate(keys=[sr_customer_sk#12, sr_store_sk#16], functions=[partial_sum(SR_RETURN_AMT#20)], output=[sr_customer_sk#12, sr_store_sk#16, sum#217])
         :     :     :                 +- Project [sr_customer_sk#12, sr_store_sk#16, sr_return_amt#20]
         :     :     :                    +- BroadcastHashJoin [sr_returned_date_sk#9], [d_date_sk#29], Inner, BuildRight, false
         :     :     :                       :- Filter ((isnotnull(sr_returned_date_sk#9) AND isnotnull(sr_store_sk#16)) AND isnotnull(sr_customer_sk#12))
         :     :     :                       :  +- FileScan parquet spark_catalog.tpcds.store_returns[sr_returned_date_sk#9,sr_customer_sk#12,sr_store_sk#16,sr_return_amt#20] Batched: true, DataFilters: [isnotnull(sr_returned_date_sk#9), isnotnull(sr_store_sk#16), isnotnull(sr_customer_sk#12)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk), IsNotNull(sr_customer_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_customer_sk:int,sr_store_sk:int,sr_return_amt:double>
         :     :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=145]
         :     :     :                          +- Project [d_date_sk#29]
         :     :     :                             +- Filter ((isnotnull(d_year#35) AND (d_year#35 = 2000)) AND isnotnull(d_date_sk#29))
         :     :     :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#29,d_year#35] Batched: true, DataFilters: [isnotnull(d_year#35), (d_year#35 = 2000), isnotnull(d_date_sk#29)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     :     +- Sort [ctr_store_sk#108 ASC NULLS FIRST], false, 0
         :     :        +- Filter isnotnull((avg(ctr_total_return) * 1.2)#111)
         :     :           +- HashAggregate(keys=[ctr_store_sk#108], functions=[avg(ctr_total_return#109)], output=[(avg(ctr_total_return) * 1.2)#111, ctr_store_sk#108])
         :     :              +- Exchange hashpartitioning(ctr_store_sk#108, 200), ENSURE_REQUIREMENTS, [plan_id=162]
         :     :                 +- HashAggregate(keys=[ctr_store_sk#108], functions=[partial_avg(ctr_total_return#109)], output=[ctr_store_sk#108, sum#220, count#221L])
         :     :                    +- HashAggregate(keys=[sr_customer_sk#168, sr_store_sk#172], functions=[sum(SR_RETURN_AMT#176)], output=[ctr_store_sk#108, ctr_total_return#109])
         :     :                       +- Exchange hashpartitioning(sr_customer_sk#168, sr_store_sk#172, 200), ENSURE_REQUIREMENTS, [plan_id=158]
         :     :                          +- HashAggregate(keys=[sr_customer_sk#168, sr_store_sk#172], functions=[partial_sum(SR_RETURN_AMT#176)], output=[sr_customer_sk#168, sr_store_sk#172, sum#223])
         :     :                             +- Project [sr_customer_sk#168, sr_store_sk#172, sr_return_amt#176]
         :     :                                +- BroadcastHashJoin [sr_returned_date_sk#165], [d_date_sk#185], Inner, BuildRight, false
         :     :                                   :- Filter (isnotnull(sr_returned_date_sk#165) AND isnotnull(sr_store_sk#172))
         :     :                                   :  +- FileScan parquet spark_catalog.tpcds.store_returns[sr_returned_date_sk#165,sr_customer_sk#168,sr_store_sk#172,sr_return_amt#176] Batched: true, DataFilters: [isnotnull(sr_returned_date_sk#165), isnotnull(sr_store_sk#172)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_customer_sk:int,sr_store_sk:int,sr_return_amt:double>
         :     :                                   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=153]
         :     :                                      +- Project [d_date_sk#185]
         :     :                                         +- Filter ((isnotnull(d_year#191) AND (d_year#191 = 2000)) AND isnotnull(d_date_sk#185))
         :     :                                            +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#185,d_year#191] Batched: true, DataFilters: [isnotnull(d_year#191), (d_year#191 = 2000), isnotnull(d_date_sk#185)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=173]
         :        +- Project [s_store_sk#57]
         :           +- Filter ((isnotnull(s_state#81) AND (s_state#81 = TN)) AND isnotnull(s_store_sk#57))
         :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#57,s_state#81] Batched: true, DataFilters: [isnotnull(s_state#81), (s_state#81 = TN), isnotnull(s_store_sk#57)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_state), EqualTo(s_state,TN), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=177]
            +- Filter isnotnull(c_customer_sk#86)
               +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#86,c_customer_id#87] Batched: true, DataFilters: [isnotnull(c_customer_sk#86)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string>

Time taken: 2.819 seconds, Fetched 1 row(s)
