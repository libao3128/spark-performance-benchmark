== Parsed Logical Plan ==
CTE [customer_total_return]
:  +- 'SubqueryAlias `customer_total_return`
:     +- 'Aggregate ['sr_customer_sk, 'sr_store_sk], ['sr_customer_sk AS ctr_customer_sk#1, 'sr_store_sk AS ctr_store_sk#2, 'sum('SR_RETURN_AMT) AS ctr_total_return#3]
:        +- 'Filter (('sr_returned_date_sk = 'd_date_sk) && ('d_year = 2000))
:           +- 'Join Inner
:              :- 'UnresolvedRelation `store_returns`
:              +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['c_customer_id ASC NULLS FIRST], true
         +- 'Project ['c_customer_id]
            +- 'Filter ((('ctr1.ctr_total_return > scalar-subquery#0 []) && ('s_store_sk = 'ctr1.ctr_store_sk)) && (('s_state = TN) && ('ctr1.ctr_customer_sk = 'c_customer_sk)))
               :  +- 'Project [unresolvedalias(('avg('ctr_total_return) * 1.2), None)]
               :     +- 'Filter ('ctr1.ctr_store_sk = 'ctr2.ctr_store_sk)
               :        +- 'SubqueryAlias `ctr2`
               :           +- 'UnresolvedRelation `customer_total_return`
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'SubqueryAlias `ctr1`
                  :  :  +- 'UnresolvedRelation `customer_total_return`
                  :  +- 'UnresolvedRelation `store`
                  +- 'UnresolvedRelation `customer`

== Analyzed Logical Plan ==
c_customer_id: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#85 ASC NULLS FIRST], true
      +- Project [c_customer_id#85]
         +- Filter (((ctr_total_return#3 > scalar-subquery#0 [ctr_store_sk#2]) && (s_store_sk#55 = ctr_store_sk#2)) && ((s_state#79 = TN) && (ctr_customer_sk#1 = c_customer_sk#84)))
            :  +- Aggregate [(avg(ctr_total_return#3) * cast(1.2 as double)) AS (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#103]
            :     +- Filter (outer(ctr_store_sk#2) = ctr_store_sk#2)
            :        +- SubqueryAlias `ctr2`
            :           +- SubqueryAlias `customer_total_return`
            :              +- Aggregate [sr_customer_sk#9, sr_store_sk#13], [sr_customer_sk#9 AS ctr_customer_sk#1, sr_store_sk#13 AS ctr_store_sk#2, sum(SR_RETURN_AMT#17) AS ctr_total_return#3]
            :                 +- Filter ((sr_returned_date_sk#6 = d_date_sk#26) && (d_year#32 = 2000))
            :                    +- Join Inner
            :                       :- SubqueryAlias `tpcds`.`store_returns`
            :                       :  +- Relation[sr_returned_date_sk#6,sr_return_time_sk#7,sr_item_sk#8,sr_customer_sk#9,sr_cdemo_sk#10,sr_hdemo_sk#11,sr_addr_sk#12,sr_store_sk#13,sr_reason_sk#14,sr_ticket_number#15,sr_return_quantity#16,sr_return_amt#17,sr_return_tax#18,sr_return_amt_inc_tax#19,sr_fee#20,sr_return_ship_cost#21,sr_refunded_cash#22,sr_reversed_charge#23,sr_store_credit#24,sr_net_loss#25] parquet
            :                       +- SubqueryAlias `tpcds`.`date_dim`
            :                          +- Relation[d_date_sk#26,d_date_id#27,d_date#28,d_month_seq#29,d_week_seq#30,d_quarter_seq#31,d_year#32,d_dow#33,d_moy#34,d_dom#35,d_qoy#36,d_fy_year#37,d_fy_quarter_seq#38,d_fy_week_seq#39,d_day_name#40,d_quarter_name#41,d_holiday#42,d_weekend#43,d_following_holiday#44,d_first_dom#45,d_last_dom#46,d_same_day_ly#47,d_same_day_lq#48,d_current_day#49,... 4 more fields] parquet
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `ctr1`
               :  :  +- SubqueryAlias `customer_total_return`
               :  :     +- Aggregate [sr_customer_sk#9, sr_store_sk#13], [sr_customer_sk#9 AS ctr_customer_sk#1, sr_store_sk#13 AS ctr_store_sk#2, sum(SR_RETURN_AMT#17) AS ctr_total_return#3]
               :  :        +- Filter ((sr_returned_date_sk#6 = d_date_sk#26) && (d_year#32 = 2000))
               :  :           +- Join Inner
               :  :              :- SubqueryAlias `tpcds`.`store_returns`
               :  :              :  +- Relation[sr_returned_date_sk#6,sr_return_time_sk#7,sr_item_sk#8,sr_customer_sk#9,sr_cdemo_sk#10,sr_hdemo_sk#11,sr_addr_sk#12,sr_store_sk#13,sr_reason_sk#14,sr_ticket_number#15,sr_return_quantity#16,sr_return_amt#17,sr_return_tax#18,sr_return_amt_inc_tax#19,sr_fee#20,sr_return_ship_cost#21,sr_refunded_cash#22,sr_reversed_charge#23,sr_store_credit#24,sr_net_loss#25] parquet
               :  :              +- SubqueryAlias `tpcds`.`date_dim`
               :  :                 +- Relation[d_date_sk#26,d_date_id#27,d_date#28,d_month_seq#29,d_week_seq#30,d_quarter_seq#31,d_year#32,d_dow#33,d_moy#34,d_dom#35,d_qoy#36,d_fy_year#37,d_fy_quarter_seq#38,d_fy_week_seq#39,d_day_name#40,d_quarter_name#41,d_holiday#42,d_weekend#43,d_following_holiday#44,d_first_dom#45,d_last_dom#46,d_same_day_ly#47,d_same_day_lq#48,d_current_day#49,... 4 more fields] parquet
               :  +- SubqueryAlias `tpcds`.`store`
               :     +- Relation[s_store_sk#55,s_store_id#56,s_rec_start_date#57,s_rec_end_date#58,s_closed_date_sk#59,s_store_name#60,s_number_employees#61,s_floor_space#62,s_hours#63,s_manager#64,s_market_id#65,s_geography_class#66,s_market_desc#67,s_market_manager#68,s_division_id#69,s_division_name#70,s_company_id#71,s_company_name#72,s_street_number#73,s_street_name#74,s_street_type#75,s_suite_number#76,s_city#77,s_county#78,... 5 more fields] parquet
               +- SubqueryAlias `tpcds`.`customer`
                  +- Relation[c_customer_sk#84,c_customer_id#85,c_current_cdemo_sk#86,c_current_hdemo_sk#87,c_current_addr_sk#88,c_first_shipto_date_sk#89,c_first_sales_date_sk#90,c_salutation#91,c_first_name#92,c_last_name#93,c_preferred_cust_flag#94,c_birth_day#95,c_birth_month#96,c_birth_year#97,c_birth_country#98,c_login#99,c_email_address#100,c_last_review_date#101] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#85 ASC NULLS FIRST], true
      +- Project [c_customer_id#85]
         +- Join Inner, (ctr_customer_sk#1 = c_customer_sk#84)
            :- Project [ctr_customer_sk#1]
            :  +- Join Inner, (s_store_sk#55 = ctr_store_sk#2)
            :     :- Project [ctr_customer_sk#1, ctr_store_sk#2]
            :     :  +- Join Inner, ((ctr_total_return#3 > (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#103) && (ctr_store_sk#2 = ctr_store_sk#2#104))
            :     :     :- Filter isnotnull(ctr_total_return#3)
            :     :     :  +- Aggregate [sr_customer_sk#9, sr_store_sk#13], [sr_customer_sk#9 AS ctr_customer_sk#1, sr_store_sk#13 AS ctr_store_sk#2, sum(SR_RETURN_AMT#17) AS ctr_total_return#3]
            :     :     :     +- Project [sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
            :     :     :        +- Join Inner, (sr_returned_date_sk#6 = d_date_sk#26)
            :     :     :           :- Project [sr_returned_date_sk#6, sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
            :     :     :           :  +- Filter ((isnotnull(sr_returned_date_sk#6) && isnotnull(sr_store_sk#13)) && isnotnull(sr_customer_sk#9))
            :     :     :           :     +- Relation[sr_returned_date_sk#6,sr_return_time_sk#7,sr_item_sk#8,sr_customer_sk#9,sr_cdemo_sk#10,sr_hdemo_sk#11,sr_addr_sk#12,sr_store_sk#13,sr_reason_sk#14,sr_ticket_number#15,sr_return_quantity#16,sr_return_amt#17,sr_return_tax#18,sr_return_amt_inc_tax#19,sr_fee#20,sr_return_ship_cost#21,sr_refunded_cash#22,sr_reversed_charge#23,sr_store_credit#24,sr_net_loss#25] parquet
            :     :     :           +- Project [d_date_sk#26]
            :     :     :              +- Filter ((isnotnull(d_year#32) && (d_year#32 = 2000)) && isnotnull(d_date_sk#26))
            :     :     :                 +- Relation[d_date_sk#26,d_date_id#27,d_date#28,d_month_seq#29,d_week_seq#30,d_quarter_seq#31,d_year#32,d_dow#33,d_moy#34,d_dom#35,d_qoy#36,d_fy_year#37,d_fy_quarter_seq#38,d_fy_week_seq#39,d_day_name#40,d_quarter_name#41,d_holiday#42,d_weekend#43,d_following_holiday#44,d_first_dom#45,d_last_dom#46,d_same_day_ly#47,d_same_day_lq#48,d_current_day#49,... 4 more fields] parquet
            :     :     +- Filter isnotnull((avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#103)
            :     :        +- Aggregate [ctr_store_sk#2], [(avg(ctr_total_return#3) * 1.2) AS (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#103, ctr_store_sk#2 AS ctr_store_sk#2#104]
            :     :           +- Aggregate [sr_customer_sk#9, sr_store_sk#13], [sr_store_sk#13 AS ctr_store_sk#2, sum(SR_RETURN_AMT#17) AS ctr_total_return#3]
            :     :              +- Project [sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
            :     :                 +- Join Inner, (sr_returned_date_sk#6 = d_date_sk#26)
            :     :                    :- Project [sr_returned_date_sk#6, sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
            :     :                    :  +- Filter (isnotnull(sr_returned_date_sk#6) && isnotnull(sr_store_sk#13))
            :     :                    :     +- Relation[sr_returned_date_sk#6,sr_return_time_sk#7,sr_item_sk#8,sr_customer_sk#9,sr_cdemo_sk#10,sr_hdemo_sk#11,sr_addr_sk#12,sr_store_sk#13,sr_reason_sk#14,sr_ticket_number#15,sr_return_quantity#16,sr_return_amt#17,sr_return_tax#18,sr_return_amt_inc_tax#19,sr_fee#20,sr_return_ship_cost#21,sr_refunded_cash#22,sr_reversed_charge#23,sr_store_credit#24,sr_net_loss#25] parquet
            :     :                    +- Project [d_date_sk#26]
            :     :                       +- Filter ((isnotnull(d_year#32) && (d_year#32 = 2000)) && isnotnull(d_date_sk#26))
            :     :                          +- Relation[d_date_sk#26,d_date_id#27,d_date#28,d_month_seq#29,d_week_seq#30,d_quarter_seq#31,d_year#32,d_dow#33,d_moy#34,d_dom#35,d_qoy#36,d_fy_year#37,d_fy_quarter_seq#38,d_fy_week_seq#39,d_day_name#40,d_quarter_name#41,d_holiday#42,d_weekend#43,d_following_holiday#44,d_first_dom#45,d_last_dom#46,d_same_day_ly#47,d_same_day_lq#48,d_current_day#49,... 4 more fields] parquet
            :     +- Project [s_store_sk#55]
            :        +- Filter ((isnotnull(s_state#79) && (s_state#79 = TN)) && isnotnull(s_store_sk#55))
            :           +- Relation[s_store_sk#55,s_store_id#56,s_rec_start_date#57,s_rec_end_date#58,s_closed_date_sk#59,s_store_name#60,s_number_employees#61,s_floor_space#62,s_hours#63,s_manager#64,s_market_id#65,s_geography_class#66,s_market_desc#67,s_market_manager#68,s_division_id#69,s_division_name#70,s_company_id#71,s_company_name#72,s_street_number#73,s_street_name#74,s_street_type#75,s_suite_number#76,s_city#77,s_county#78,... 5 more fields] parquet
            +- Project [c_customer_sk#84, c_customer_id#85]
               +- Filter isnotnull(c_customer_sk#84)
                  +- Relation[c_customer_sk#84,c_customer_id#85,c_current_cdemo_sk#86,c_current_hdemo_sk#87,c_current_addr_sk#88,c_first_shipto_date_sk#89,c_first_sales_date_sk#90,c_salutation#91,c_first_name#92,c_last_name#93,c_preferred_cust_flag#94,c_birth_day#95,c_birth_month#96,c_birth_year#97,c_birth_country#98,c_login#99,c_email_address#100,c_last_review_date#101] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#85 ASC NULLS FIRST], output=[c_customer_id#85])
+- *(12) Project [c_customer_id#85]
   +- *(12) BroadcastHashJoin [ctr_customer_sk#1], [c_customer_sk#84], Inner, BuildRight
      :- *(12) Project [ctr_customer_sk#1]
      :  +- *(12) BroadcastHashJoin [ctr_store_sk#2], [s_store_sk#55], Inner, BuildRight
      :     :- *(12) Project [ctr_customer_sk#1, ctr_store_sk#2]
      :     :  +- *(12) SortMergeJoin [ctr_store_sk#2], [ctr_store_sk#2#104], Inner, (ctr_total_return#3 > (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#103)
      :     :     :- *(4) Sort [ctr_store_sk#2 ASC NULLS FIRST], false, 0
      :     :     :  +- Exchange hashpartitioning(ctr_store_sk#2, 200)
      :     :     :     +- *(3) Filter isnotnull(ctr_total_return#3)
      :     :     :        +- *(3) HashAggregate(keys=[sr_customer_sk#9, sr_store_sk#13], functions=[sum(SR_RETURN_AMT#17)], output=[ctr_customer_sk#1, ctr_store_sk#2, ctr_total_return#3])
      :     :     :           +- Exchange hashpartitioning(sr_customer_sk#9, sr_store_sk#13, 200)
      :     :     :              +- *(2) HashAggregate(keys=[sr_customer_sk#9, sr_store_sk#13], functions=[partial_sum(SR_RETURN_AMT#17)], output=[sr_customer_sk#9, sr_store_sk#13, sum#106])
      :     :     :                 +- *(2) Project [sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
      :     :     :                    +- *(2) BroadcastHashJoin [sr_returned_date_sk#6], [d_date_sk#26], Inner, BuildRight
      :     :     :                       :- *(2) Project [sr_returned_date_sk#6, sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
      :     :     :                       :  +- *(2) Filter ((isnotnull(sr_returned_date_sk#6) && isnotnull(sr_store_sk#13)) && isnotnull(sr_customer_sk#9))
      :     :     :                       :     +- *(2) FileScan parquet tpcds.store_returns[sr_returned_date_sk#6,sr_customer_sk#9,sr_store_sk#13,sr_return_amt#17] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk), IsNotNull(sr_customer_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_customer_sk:int,sr_store_sk:int,sr_return_amt:double>
      :     :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :     :                          +- *(1) Project [d_date_sk#26]
      :     :     :                             +- *(1) Filter ((isnotnull(d_year#32) && (d_year#32 = 2000)) && isnotnull(d_date_sk#26))
      :     :     :                                +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#26,d_year#32] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :     :     +- *(9) Sort [ctr_store_sk#2#104 ASC NULLS FIRST], false, 0
      :     :        +- Exchange hashpartitioning(ctr_store_sk#2#104, 200)
      :     :           +- *(8) Filter isnotnull((avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#103)
      :     :              +- *(8) HashAggregate(keys=[ctr_store_sk#2], functions=[avg(ctr_total_return#3)], output=[(avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#103, ctr_store_sk#2#104])
      :     :                 +- Exchange hashpartitioning(ctr_store_sk#2, 200)
      :     :                    +- *(7) HashAggregate(keys=[ctr_store_sk#2], functions=[partial_avg(ctr_total_return#3)], output=[ctr_store_sk#2, sum#109, count#110L])
      :     :                       +- *(7) HashAggregate(keys=[sr_customer_sk#9, sr_store_sk#13], functions=[sum(SR_RETURN_AMT#17)], output=[ctr_store_sk#2, ctr_total_return#3])
      :     :                          +- Exchange hashpartitioning(sr_customer_sk#9, sr_store_sk#13, 200)
      :     :                             +- *(6) HashAggregate(keys=[sr_customer_sk#9, sr_store_sk#13], functions=[partial_sum(SR_RETURN_AMT#17)], output=[sr_customer_sk#9, sr_store_sk#13, sum#106])
      :     :                                +- *(6) Project [sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
      :     :                                   +- *(6) BroadcastHashJoin [sr_returned_date_sk#6], [d_date_sk#26], Inner, BuildRight
      :     :                                      :- *(6) Project [sr_returned_date_sk#6, sr_customer_sk#9, sr_store_sk#13, sr_return_amt#17]
      :     :                                      :  +- *(6) Filter (isnotnull(sr_returned_date_sk#6) && isnotnull(sr_store_sk#13))
      :     :                                      :     +- *(6) FileScan parquet tpcds.store_returns[sr_returned_date_sk#6,sr_customer_sk#9,sr_store_sk#13,sr_return_amt#17] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_customer_sk:int,sr_store_sk:int,sr_return_amt:double>
      :     :                                      +- ReusedExchange [d_date_sk#26], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :        +- *(10) Project [s_store_sk#55]
      :           +- *(10) Filter ((isnotnull(s_state#79) && (s_state#79 = TN)) && isnotnull(s_store_sk#55))
      :              +- *(10) FileScan parquet tpcds.store[s_store_sk#55,s_state#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_state), EqualTo(s_state,TN), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>
      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         +- *(11) Project [c_customer_sk#84, c_customer_id#85]
            +- *(11) Filter isnotnull(c_customer_sk#84)
               +- *(11) FileScan parquet tpcds.customer[c_customer_sk#84,c_customer_id#85] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string>
Time taken: 4.44 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 1 in stream 0 using template query1.tpl
----------------------------------------------------^^^

