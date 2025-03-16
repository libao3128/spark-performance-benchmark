== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['count(1) ASC NULLS FIRST], true
      +- 'Project [unresolvedalias('count(1), None)]
         +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 20))) && ((('time_dim.t_minute >= 30) && ('household_demographics.hd_dep_count = 7)) && ('store.s_store_name = ese)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation `store_sales`
               :  :  +- 'UnresolvedRelation `household_demographics`
               :  +- 'UnresolvedRelation `time_dim`
               +- 'UnresolvedRelation `store`

== Analyzed Logical Plan ==
count(1): bigint
GlobalLimit 100
+- LocalLimit 100
   +- Project [count(1)#71L]
      +- Sort [count(1)#71L ASC NULLS FIRST], true
         +- Aggregate [count(1) AS count(1)#71L]
            +- Filter ((((ss_sold_time_sk#5 = t_time_sk#32) && (ss_hdemo_sk#9 = hd_demo_sk#27)) && ((ss_store_sk#11 = s_store_sk#42) && (t_hour#35 = 20))) && (((t_minute#36 >= 30) && (hd_dep_count#30 = 7)) && (s_store_name#47 = ese)))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :  :  +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
                  :  :  +- SubqueryAlias `tpcds`.`household_demographics`
                  :  :     +- Relation[hd_demo_sk#27,hd_income_band_sk#28,hd_buy_potential#29,hd_dep_count#30,hd_vehicle_count#31] parquet
                  :  +- SubqueryAlias `tpcds`.`time_dim`
                  :     +- Relation[t_time_sk#32,t_time_id#33,t_time#34,t_hour#35,t_minute#36,t_second#37,t_am_pm#38,t_shift#39,t_sub_shift#40,t_meal_time#41] parquet
                  +- SubqueryAlias `tpcds`.`store`
                     +- Relation[s_store_sk#42,s_store_id#43,s_rec_start_date#44,s_rec_end_date#45,s_closed_date_sk#46,s_store_name#47,s_number_employees#48,s_floor_space#49,s_hours#50,s_manager#51,s_market_id#52,s_geography_class#53,s_market_desc#54,s_market_manager#55,s_division_id#56,s_division_name#57,s_company_id#58,s_company_name#59,s_street_number#60,s_street_name#61,s_street_type#62,s_suite_number#63,s_city#64,s_county#65,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [count(1)#71L ASC NULLS FIRST], true
      +- Aggregate [count(1) AS count(1)#71L]
         +- Project
            +- Join Inner, (ss_store_sk#11 = s_store_sk#42)
               :- Project [ss_store_sk#11]
               :  +- Join Inner, (ss_sold_time_sk#5 = t_time_sk#32)
               :     :- Project [ss_sold_time_sk#5, ss_store_sk#11]
               :     :  +- Join Inner, (ss_hdemo_sk#9 = hd_demo_sk#27)
               :     :     :- Project [ss_sold_time_sk#5, ss_hdemo_sk#9, ss_store_sk#11]
               :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#9) && isnotnull(ss_sold_time_sk#5)) && isnotnull(ss_store_sk#11))
               :     :     :     +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
               :     :     +- Project [hd_demo_sk#27]
               :     :        +- Filter ((isnotnull(hd_dep_count#30) && (hd_dep_count#30 = 7)) && isnotnull(hd_demo_sk#27))
               :     :           +- Relation[hd_demo_sk#27,hd_income_band_sk#28,hd_buy_potential#29,hd_dep_count#30,hd_vehicle_count#31] parquet
               :     +- Project [t_time_sk#32]
               :        +- Filter ((((isnotnull(t_hour#35) && isnotnull(t_minute#36)) && (t_hour#35 = 20)) && (t_minute#36 >= 30)) && isnotnull(t_time_sk#32))
               :           +- Relation[t_time_sk#32,t_time_id#33,t_time#34,t_hour#35,t_minute#36,t_second#37,t_am_pm#38,t_shift#39,t_sub_shift#40,t_meal_time#41] parquet
               +- Project [s_store_sk#42]
                  +- Filter ((isnotnull(s_store_name#47) && (s_store_name#47 = ese)) && isnotnull(s_store_sk#42))
                     +- Relation[s_store_sk#42,s_store_id#43,s_rec_start_date#44,s_rec_end_date#45,s_closed_date_sk#46,s_store_name#47,s_number_employees#48,s_floor_space#49,s_hours#50,s_manager#51,s_market_id#52,s_geography_class#53,s_market_desc#54,s_market_manager#55,s_division_id#56,s_division_name#57,s_company_id#58,s_company_name#59,s_street_number#60,s_street_name#61,s_street_type#62,s_suite_number#63,s_city#64,s_county#65,... 5 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[count(1)#71L ASC NULLS FIRST], output=[count(1)#71L])
+- *(5) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#71L])
   +- Exchange SinglePartition
      +- *(4) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#74L])
         +- *(4) Project
            +- *(4) BroadcastHashJoin [ss_store_sk#11], [s_store_sk#42], Inner, BuildRight
               :- *(4) Project [ss_store_sk#11]
               :  +- *(4) BroadcastHashJoin [ss_sold_time_sk#5], [t_time_sk#32], Inner, BuildRight
               :     :- *(4) Project [ss_sold_time_sk#5, ss_store_sk#11]
               :     :  +- *(4) BroadcastHashJoin [ss_hdemo_sk#9], [hd_demo_sk#27], Inner, BuildRight
               :     :     :- *(4) Project [ss_sold_time_sk#5, ss_hdemo_sk#9, ss_store_sk#11]
               :     :     :  +- *(4) Filter ((isnotnull(ss_hdemo_sk#9) && isnotnull(ss_sold_time_sk#5)) && isnotnull(ss_store_sk#11))
               :     :     :     +- *(4) FileScan parquet tpcds.store_sales[ss_sold_time_sk#5,ss_hdemo_sk#9,ss_store_sk#11] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(1) Project [hd_demo_sk#27]
               :     :           +- *(1) Filter ((isnotnull(hd_dep_count#30) && (hd_dep_count#30 = 7)) && isnotnull(hd_demo_sk#27))
               :     :              +- *(1) FileScan parquet tpcds.household_demographics[hd_demo_sk#27,hd_dep_count#30] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_dep_count), EqualTo(hd_dep_count,7), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(2) Project [t_time_sk#32]
               :           +- *(2) Filter ((((isnotnull(t_hour#35) && isnotnull(t_minute#36)) && (t_hour#35 = 20)) && (t_minute#36 >= 30)) && isnotnull(t_time_sk#32))
               :              +- *(2) FileScan parquet tpcds.time_dim[t_time_sk#32,t_hour#35,t_minute#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,20), GreaterThanOrEqual(t_minute,30), IsN..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(3) Project [s_store_sk#42]
                     +- *(3) Filter ((isnotnull(s_store_name#47) && (s_store_name#47 = ese)) && isnotnull(s_store_sk#42))
                        +- *(3) FileScan parquet tpcds.store[s_store_sk#42,s_store_name#47] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
Time taken: 3.478 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 96 in stream 0 using template query96.tpl
------------------------------------------------------^^^

