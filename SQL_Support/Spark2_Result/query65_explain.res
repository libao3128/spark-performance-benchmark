== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_store_name ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST], true
      +- 'Project ['s_store_name, 'i_item_desc, 'sc.revenue, 'i_current_price, 'i_wholesale_cost, 'i_brand]
         +- 'Filter ((('sb.ss_store_sk = 'sc.ss_store_sk) && ('sc.revenue <= (0.1 * 'sb.ave))) && (('s_store_sk = 'sc.ss_store_sk) && ('i_item_sk = 'sc.ss_item_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation `store`
               :  :  +- 'UnresolvedRelation `item`
               :  +- 'SubqueryAlias `sb`
               :     +- 'Aggregate ['ss_store_sk], ['ss_store_sk, 'avg('revenue) AS ave#1]
               :        +- 'SubqueryAlias `sa`
               :           +- 'Aggregate ['ss_store_sk, 'ss_item_sk], ['ss_store_sk, 'ss_item_sk, 'sum('ss_sales_price) AS revenue#0]
               :              +- 'Filter (('ss_sold_date_sk = 'd_date_sk) && (('d_month_seq >= 1176) && ('d_month_seq <= (1176 + 11))))
               :                 +- 'Join Inner
               :                    :- 'UnresolvedRelation `store_sales`
               :                    +- 'UnresolvedRelation `date_dim`
               +- 'SubqueryAlias `sc`
                  +- 'Aggregate ['ss_store_sk, 'ss_item_sk], ['ss_store_sk, 'ss_item_sk, 'sum('ss_sales_price) AS revenue#2]
                     +- 'Filter (('ss_sold_date_sk = 'd_date_sk) && (('d_month_seq >= 1176) && ('d_month_seq <= (1176 + 11))))
                        +- 'Join Inner
                           :- 'UnresolvedRelation `store_sales`
                           +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
s_store_name: string, i_item_desc: string, revenue: double, i_current_price: double, i_wholesale_cost: double, i_brand: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#10 ASC NULLS FIRST, i_item_desc#38 ASC NULLS FIRST], true
      +- Project [s_store_name#10, i_item_desc#38, revenue#2, i_current_price#39, i_wholesale_cost#40, i_brand#42]
         +- Filter (((ss_store_sk#63 = ss_store_sk#117) && (revenue#2 <= (cast(0.1 as double) * ave#1))) && ((s_store_sk#5 = ss_store_sk#117) && (i_item_sk#34 = ss_item_sk#112)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias `tpcds`.`store`
               :  :  :  +- Relation[s_store_sk#5,s_store_id#6,s_rec_start_date#7,s_rec_end_date#8,s_closed_date_sk#9,s_store_name#10,s_number_employees#11,s_floor_space#12,s_hours#13,s_manager#14,s_market_id#15,s_geography_class#16,s_market_desc#17,s_market_manager#18,s_division_id#19,s_division_name#20,s_company_id#21,s_company_name#22,s_street_number#23,s_street_name#24,s_street_type#25,s_suite_number#26,s_city#27,s_county#28,... 5 more fields] parquet
               :  :  +- SubqueryAlias `tpcds`.`item`
               :  :     +- Relation[i_item_sk#34,i_item_id#35,i_rec_start_date#36,i_rec_end_date#37,i_item_desc#38,i_current_price#39,i_wholesale_cost#40,i_brand_id#41,i_brand#42,i_class_id#43,i_class#44,i_category_id#45,i_category#46,i_manufact_id#47,i_manufact#48,i_size#49,i_formulation#50,i_color#51,i_units#52,i_container#53,i_manager_id#54,i_product_name#55] parquet
               :  +- SubqueryAlias `sb`
               :     +- Aggregate [ss_store_sk#63], [ss_store_sk#63, avg(revenue#0) AS ave#1]
               :        +- SubqueryAlias `sa`
               :           +- Aggregate [ss_store_sk#63, ss_item_sk#58], [ss_store_sk#63, ss_item_sk#58, sum(ss_sales_price#69) AS revenue#0]
               :              +- Filter ((ss_sold_date_sk#56 = d_date_sk#79) && ((d_month_seq#82 >= 1176) && (d_month_seq#82 <= (1176 + 11))))
               :                 +- Join Inner
               :                    :- SubqueryAlias `tpcds`.`store_sales`
               :                    :  +- Relation[ss_sold_date_sk#56,ss_sold_time_sk#57,ss_item_sk#58,ss_customer_sk#59,ss_cdemo_sk#60,ss_hdemo_sk#61,ss_addr_sk#62,ss_store_sk#63,ss_promo_sk#64,ss_ticket_number#65,ss_quantity#66,ss_wholesale_cost#67,ss_list_price#68,ss_sales_price#69,ss_ext_discount_amt#70,ss_ext_sales_price#71,ss_ext_wholesale_cost#72,ss_ext_list_price#73,ss_ext_tax#74,ss_coupon_amt#75,ss_net_paid#76,ss_net_paid_inc_tax#77,ss_net_profit#78] parquet
               :                    +- SubqueryAlias `tpcds`.`date_dim`
               :                       +- Relation[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet
               +- SubqueryAlias `sc`
                  +- Aggregate [ss_store_sk#117, ss_item_sk#112], [ss_store_sk#117, ss_item_sk#112, sum(ss_sales_price#123) AS revenue#2]
                     +- Filter ((ss_sold_date_sk#110 = d_date_sk#79) && ((d_month_seq#82 >= 1176) && (d_month_seq#82 <= (1176 + 11))))
                        +- Join Inner
                           :- SubqueryAlias `tpcds`.`store_sales`
                           :  +- Relation[ss_sold_date_sk#110,ss_sold_time_sk#111,ss_item_sk#112,ss_customer_sk#113,ss_cdemo_sk#114,ss_hdemo_sk#115,ss_addr_sk#116,ss_store_sk#117,ss_promo_sk#118,ss_ticket_number#119,ss_quantity#120,ss_wholesale_cost#121,ss_list_price#122,ss_sales_price#123,ss_ext_discount_amt#124,ss_ext_sales_price#125,ss_ext_wholesale_cost#126,ss_ext_list_price#127,ss_ext_tax#128,ss_coupon_amt#129,ss_net_paid#130,ss_net_paid_inc_tax#131,ss_net_profit#132] parquet
                           +- SubqueryAlias `tpcds`.`date_dim`
                              +- Relation[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#10 ASC NULLS FIRST, i_item_desc#38 ASC NULLS FIRST], true
      +- Project [s_store_name#10, i_item_desc#38, revenue#2, i_current_price#39, i_wholesale_cost#40, i_brand#42]
         +- Join Inner, ((ss_store_sk#63 = ss_store_sk#117) && (revenue#2 <= (0.1 * ave#1)))
            :- Project [s_store_name#10, ss_store_sk#117, revenue#2, i_item_desc#38, i_current_price#39, i_wholesale_cost#40, i_brand#42]
            :  +- Join Inner, (i_item_sk#34 = ss_item_sk#112)
            :     :- Project [s_store_name#10, ss_store_sk#117, ss_item_sk#112, revenue#2]
            :     :  +- Join Inner, (s_store_sk#5 = ss_store_sk#117)
            :     :     :- Project [s_store_sk#5, s_store_name#10]
            :     :     :  +- Filter isnotnull(s_store_sk#5)
            :     :     :     +- Relation[s_store_sk#5,s_store_id#6,s_rec_start_date#7,s_rec_end_date#8,s_closed_date_sk#9,s_store_name#10,s_number_employees#11,s_floor_space#12,s_hours#13,s_manager#14,s_market_id#15,s_geography_class#16,s_market_desc#17,s_market_manager#18,s_division_id#19,s_division_name#20,s_company_id#21,s_company_name#22,s_street_number#23,s_street_name#24,s_street_type#25,s_suite_number#26,s_city#27,s_county#28,... 5 more fields] parquet
            :     :     +- Filter isnotnull(revenue#2)
            :     :        +- Aggregate [ss_store_sk#117, ss_item_sk#112], [ss_store_sk#117, ss_item_sk#112, sum(ss_sales_price#123) AS revenue#2]
            :     :           +- Project [ss_item_sk#112, ss_store_sk#117, ss_sales_price#123]
            :     :              +- Join Inner, (ss_sold_date_sk#110 = d_date_sk#79)
            :     :                 :- Project [ss_sold_date_sk#110, ss_item_sk#112, ss_store_sk#117, ss_sales_price#123]
            :     :                 :  +- Filter ((isnotnull(ss_sold_date_sk#110) && isnotnull(ss_store_sk#117)) && isnotnull(ss_item_sk#112))
            :     :                 :     +- Relation[ss_sold_date_sk#110,ss_sold_time_sk#111,ss_item_sk#112,ss_customer_sk#113,ss_cdemo_sk#114,ss_hdemo_sk#115,ss_addr_sk#116,ss_store_sk#117,ss_promo_sk#118,ss_ticket_number#119,ss_quantity#120,ss_wholesale_cost#121,ss_list_price#122,ss_sales_price#123,ss_ext_discount_amt#124,ss_ext_sales_price#125,ss_ext_wholesale_cost#126,ss_ext_list_price#127,ss_ext_tax#128,ss_coupon_amt#129,ss_net_paid#130,ss_net_paid_inc_tax#131,ss_net_profit#132] parquet
            :     :                 +- Project [d_date_sk#79]
            :     :                    +- Filter (((isnotnull(d_month_seq#82) && (d_month_seq#82 >= 1176)) && (d_month_seq#82 <= 1187)) && isnotnull(d_date_sk#79))
            :     :                       +- Relation[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet
            :     +- Project [i_item_sk#34, i_item_desc#38, i_current_price#39, i_wholesale_cost#40, i_brand#42]
            :        +- Filter isnotnull(i_item_sk#34)
            :           +- Relation[i_item_sk#34,i_item_id#35,i_rec_start_date#36,i_rec_end_date#37,i_item_desc#38,i_current_price#39,i_wholesale_cost#40,i_brand_id#41,i_brand#42,i_class_id#43,i_class#44,i_category_id#45,i_category#46,i_manufact_id#47,i_manufact#48,i_size#49,i_formulation#50,i_color#51,i_units#52,i_container#53,i_manager_id#54,i_product_name#55] parquet
            +- Filter isnotnull(ave#1)
               +- Aggregate [ss_store_sk#63], [ss_store_sk#63, avg(revenue#0) AS ave#1]
                  +- Aggregate [ss_store_sk#63, ss_item_sk#58], [ss_store_sk#63, sum(ss_sales_price#69) AS revenue#0]
                     +- Project [ss_item_sk#58, ss_store_sk#63, ss_sales_price#69]
                        +- Join Inner, (ss_sold_date_sk#56 = d_date_sk#79)
                           :- Project [ss_sold_date_sk#56, ss_item_sk#58, ss_store_sk#63, ss_sales_price#69]
                           :  +- Filter (isnotnull(ss_sold_date_sk#56) && isnotnull(ss_store_sk#63))
                           :     +- Relation[ss_sold_date_sk#56,ss_sold_time_sk#57,ss_item_sk#58,ss_customer_sk#59,ss_cdemo_sk#60,ss_hdemo_sk#61,ss_addr_sk#62,ss_store_sk#63,ss_promo_sk#64,ss_ticket_number#65,ss_quantity#66,ss_wholesale_cost#67,ss_list_price#68,ss_sales_price#69,ss_ext_discount_amt#70,ss_ext_sales_price#71,ss_ext_wholesale_cost#72,ss_ext_list_price#73,ss_ext_tax#74,ss_coupon_amt#75,ss_net_paid#76,ss_net_paid_inc_tax#77,ss_net_profit#78] parquet
                           +- Project [d_date_sk#79]
                              +- Filter (((isnotnull(d_month_seq#82) && (d_month_seq#82 >= 1176)) && (d_month_seq#82 <= 1187)) && isnotnull(d_date_sk#79))
                                 +- Relation[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_store_name#10 ASC NULLS FIRST,i_item_desc#38 ASC NULLS FIRST], output=[s_store_name#10,i_item_desc#38,revenue#2,i_current_price#39,i_wholesale_cost#40,i_brand#42])
+- *(11) Project [s_store_name#10, i_item_desc#38, revenue#2, i_current_price#39, i_wholesale_cost#40, i_brand#42]
   +- *(11) SortMergeJoin [ss_store_sk#117], [ss_store_sk#63], Inner, (revenue#2 <= (0.1 * ave#1))
      :- *(6) Sort [ss_store_sk#117 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(ss_store_sk#117, 200)
      :     +- *(5) Project [s_store_name#10, ss_store_sk#117, revenue#2, i_item_desc#38, i_current_price#39, i_wholesale_cost#40, i_brand#42]
      :        +- *(5) BroadcastHashJoin [ss_item_sk#112], [i_item_sk#34], Inner, BuildRight
      :           :- *(5) Project [s_store_name#10, ss_store_sk#117, ss_item_sk#112, revenue#2]
      :           :  +- *(5) BroadcastHashJoin [s_store_sk#5], [ss_store_sk#117], Inner, BuildLeft
      :           :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :           :     :  +- *(1) Project [s_store_sk#5, s_store_name#10]
      :           :     :     +- *(1) Filter isnotnull(s_store_sk#5)
      :           :     :        +- *(1) FileScan parquet tpcds.store[s_store_sk#5,s_store_name#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
      :           :     +- *(5) Filter isnotnull(revenue#2)
      :           :        +- *(5) HashAggregate(keys=[ss_store_sk#117, ss_item_sk#112], functions=[sum(ss_sales_price#123)], output=[ss_store_sk#117, ss_item_sk#112, revenue#2])
      :           :           +- Exchange hashpartitioning(ss_store_sk#117, ss_item_sk#112, 200)
      :           :              +- *(3) HashAggregate(keys=[ss_store_sk#117, ss_item_sk#112], functions=[partial_sum(ss_sales_price#123)], output=[ss_store_sk#117, ss_item_sk#112, sum#134])
      :           :                 +- *(3) Project [ss_item_sk#112, ss_store_sk#117, ss_sales_price#123]
      :           :                    +- *(3) BroadcastHashJoin [ss_sold_date_sk#110], [d_date_sk#79], Inner, BuildRight
      :           :                       :- *(3) Project [ss_sold_date_sk#110, ss_item_sk#112, ss_store_sk#117, ss_sales_price#123]
      :           :                       :  +- *(3) Filter ((isnotnull(ss_sold_date_sk#110) && isnotnull(ss_store_sk#117)) && isnotnull(ss_item_sk#112))
      :           :                       :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#110,ss_item_sk#112,ss_store_sk#117,ss_sales_price#123] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
      :           :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :           :                          +- *(2) Project [d_date_sk#79]
      :           :                             +- *(2) Filter (((isnotnull(d_month_seq#82) && (d_month_seq#82 >= 1176)) && (d_month_seq#82 <= 1187)) && isnotnull(d_date_sk#79))
      :           :                                +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#79,d_month_seq#82] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1176), LessThanOrEqual(d_month_seq,1187),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
      :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              +- *(4) Project [i_item_sk#34, i_item_desc#38, i_current_price#39, i_wholesale_cost#40, i_brand#42]
      :                 +- *(4) Filter isnotnull(i_item_sk#34)
      :                    +- *(4) FileScan parquet tpcds.item[i_item_sk#34,i_item_desc#38,i_current_price#39,i_wholesale_cost#40,i_brand#42] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string,i_current_price:double,i_wholesale_cost:double,i_brand:st...
      +- *(10) Sort [ss_store_sk#63 ASC NULLS FIRST], false, 0
         +- *(10) Filter isnotnull(ave#1)
            +- *(10) HashAggregate(keys=[ss_store_sk#63], functions=[avg(revenue#0)], output=[ss_store_sk#63, ave#1])
               +- Exchange hashpartitioning(ss_store_sk#63, 200)
                  +- *(9) HashAggregate(keys=[ss_store_sk#63], functions=[partial_avg(revenue#0)], output=[ss_store_sk#63, sum#137, count#138L])
                     +- *(9) HashAggregate(keys=[ss_store_sk#63, ss_item_sk#58], functions=[sum(ss_sales_price#69)], output=[ss_store_sk#63, revenue#0])
                        +- Exchange hashpartitioning(ss_store_sk#63, ss_item_sk#58, 200)
                           +- *(8) HashAggregate(keys=[ss_store_sk#63, ss_item_sk#58], functions=[partial_sum(ss_sales_price#69)], output=[ss_store_sk#63, ss_item_sk#58, sum#140])
                              +- *(8) Project [ss_item_sk#58, ss_store_sk#63, ss_sales_price#69]
                                 +- *(8) BroadcastHashJoin [ss_sold_date_sk#56], [d_date_sk#79], Inner, BuildRight
                                    :- *(8) Project [ss_sold_date_sk#56, ss_item_sk#58, ss_store_sk#63, ss_sales_price#69]
                                    :  +- *(8) Filter (isnotnull(ss_sold_date_sk#56) && isnotnull(ss_store_sk#63))
                                    :     +- *(8) FileScan parquet tpcds.store_sales[ss_sold_date_sk#56,ss_item_sk#58,ss_store_sk#63,ss_sales_price#69] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
                                    +- ReusedExchange [d_date_sk#79], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.213 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 65 in stream 0 using template query65.tpl
------------------------------------------------------^^^

