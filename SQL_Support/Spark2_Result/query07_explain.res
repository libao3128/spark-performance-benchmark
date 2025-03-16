== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id], ['i_item_id, 'avg('ss_quantity) AS agg1#0, 'avg('ss_list_price) AS agg2#1, 'avg('ss_coupon_amt) AS agg3#2, 'avg('ss_sales_price) AS agg4#3]
         +- 'Filter ((((('ss_sold_date_sk = 'd_date_sk) && ('ss_item_sk = 'i_item_sk)) && ('ss_cdemo_sk = 'cd_demo_sk)) && (('ss_promo_sk = 'p_promo_sk) && ('cd_gender = M))) && ((('cd_marital_status = S) && ('cd_education_status = College)) && ((('p_channel_email = N) || ('p_channel_event = N)) && ('d_year = 2000))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation `store_sales`
               :  :  :  +- 'UnresolvedRelation `customer_demographics`
               :  :  +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `item`
               +- 'UnresolvedRelation `promotion`

== Analyzed Logical Plan ==
i_item_id: string, agg1: double, agg2: double, agg3: double, agg4: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [i_item_id#67, agg1#0, agg2#1, agg3#2, agg4#3]
      +- Sort [i_item_id#67 ASC NULLS FIRST], true
         +- Aggregate [i_item_id#67], [i_item_id#67, avg(cast(ss_quantity#16 as bigint)) AS agg1#0, avg(ss_list_price#18) AS agg2#1, avg(ss_coupon_amt#25) AS agg3#2, avg(ss_sales_price#19) AS agg4#3]
            +- Filter (((((ss_sold_date_sk#6 = d_date_sk#38) && (ss_item_sk#8 = i_item_sk#66)) && (ss_cdemo_sk#10 = cd_demo_sk#29)) && ((ss_promo_sk#14 = p_promo_sk#88) && (cd_gender#30 = M))) && (((cd_marital_status#31 = S) && (cd_education_status#32 = College)) && (((p_channel_email#97 = N) || (p_channel_event#102 = N)) && (d_year#44 = 2000))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :  :  :  +- Relation[ss_sold_date_sk#6,ss_sold_time_sk#7,ss_item_sk#8,ss_customer_sk#9,ss_cdemo_sk#10,ss_hdemo_sk#11,ss_addr_sk#12,ss_store_sk#13,ss_promo_sk#14,ss_ticket_number#15,ss_quantity#16,ss_wholesale_cost#17,ss_list_price#18,ss_sales_price#19,ss_ext_discount_amt#20,ss_ext_sales_price#21,ss_ext_wholesale_cost#22,ss_ext_list_price#23,ss_ext_tax#24,ss_coupon_amt#25,ss_net_paid#26,ss_net_paid_inc_tax#27,ss_net_profit#28] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`customer_demographics`
                  :  :  :     +- Relation[cd_demo_sk#29,cd_gender#30,cd_marital_status#31,cd_education_status#32,cd_purchase_estimate#33,cd_credit_rating#34,cd_dep_count#35,cd_dep_employed_count#36,cd_dep_college_count#37] parquet
                  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :     +- Relation[d_date_sk#38,d_date_id#39,d_date#40,d_month_seq#41,d_week_seq#42,d_quarter_seq#43,d_year#44,d_dow#45,d_moy#46,d_dom#47,d_qoy#48,d_fy_year#49,d_fy_quarter_seq#50,d_fy_week_seq#51,d_day_name#52,d_quarter_name#53,d_holiday#54,d_weekend#55,d_following_holiday#56,d_first_dom#57,d_last_dom#58,d_same_day_ly#59,d_same_day_lq#60,d_current_day#61,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`item`
                  :     +- Relation[i_item_sk#66,i_item_id#67,i_rec_start_date#68,i_rec_end_date#69,i_item_desc#70,i_current_price#71,i_wholesale_cost#72,i_brand_id#73,i_brand#74,i_class_id#75,i_class#76,i_category_id#77,i_category#78,i_manufact_id#79,i_manufact#80,i_size#81,i_formulation#82,i_color#83,i_units#84,i_container#85,i_manager_id#86,i_product_name#87] parquet
                  +- SubqueryAlias `tpcds`.`promotion`
                     +- Relation[p_promo_sk#88,p_promo_id#89,p_start_date_sk#90,p_end_date_sk#91,p_item_sk#92,p_cost#93,p_response_target#94,p_promo_name#95,p_channel_dmail#96,p_channel_email#97,p_channel_catalog#98,p_channel_tv#99,p_channel_radio#100,p_channel_press#101,p_channel_event#102,p_channel_demo#103,p_channel_details#104,p_purpose#105,p_discount_active#106] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#67 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#67], [i_item_id#67, avg(cast(ss_quantity#16 as bigint)) AS agg1#0, avg(ss_list_price#18) AS agg2#1, avg(ss_coupon_amt#25) AS agg3#2, avg(ss_sales_price#19) AS agg4#3]
         +- Project [ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25, i_item_id#67]
            +- Join Inner, (ss_promo_sk#14 = p_promo_sk#88)
               :- Project [ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25, i_item_id#67]
               :  +- Join Inner, (ss_item_sk#8 = i_item_sk#66)
               :     :- Project [ss_item_sk#8, ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25]
               :     :  +- Join Inner, (ss_sold_date_sk#6 = d_date_sk#38)
               :     :     :- Project [ss_sold_date_sk#6, ss_item_sk#8, ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25]
               :     :     :  +- Join Inner, (ss_cdemo_sk#10 = cd_demo_sk#29)
               :     :     :     :- Project [ss_sold_date_sk#6, ss_item_sk#8, ss_cdemo_sk#10, ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25]
               :     :     :     :  +- Filter (((isnotnull(ss_cdemo_sk#10) && isnotnull(ss_sold_date_sk#6)) && isnotnull(ss_item_sk#8)) && isnotnull(ss_promo_sk#14))
               :     :     :     :     +- Relation[ss_sold_date_sk#6,ss_sold_time_sk#7,ss_item_sk#8,ss_customer_sk#9,ss_cdemo_sk#10,ss_hdemo_sk#11,ss_addr_sk#12,ss_store_sk#13,ss_promo_sk#14,ss_ticket_number#15,ss_quantity#16,ss_wholesale_cost#17,ss_list_price#18,ss_sales_price#19,ss_ext_discount_amt#20,ss_ext_sales_price#21,ss_ext_wholesale_cost#22,ss_ext_list_price#23,ss_ext_tax#24,ss_coupon_amt#25,ss_net_paid#26,ss_net_paid_inc_tax#27,ss_net_profit#28] parquet
               :     :     :     +- Project [cd_demo_sk#29]
               :     :     :        +- Filter ((((((isnotnull(cd_marital_status#31) && isnotnull(cd_gender#30)) && isnotnull(cd_education_status#32)) && (cd_gender#30 = M)) && (cd_marital_status#31 = S)) && (cd_education_status#32 = College)) && isnotnull(cd_demo_sk#29))
               :     :     :           +- Relation[cd_demo_sk#29,cd_gender#30,cd_marital_status#31,cd_education_status#32,cd_purchase_estimate#33,cd_credit_rating#34,cd_dep_count#35,cd_dep_employed_count#36,cd_dep_college_count#37] parquet
               :     :     +- Project [d_date_sk#38]
               :     :        +- Filter ((isnotnull(d_year#44) && (d_year#44 = 2000)) && isnotnull(d_date_sk#38))
               :     :           +- Relation[d_date_sk#38,d_date_id#39,d_date#40,d_month_seq#41,d_week_seq#42,d_quarter_seq#43,d_year#44,d_dow#45,d_moy#46,d_dom#47,d_qoy#48,d_fy_year#49,d_fy_quarter_seq#50,d_fy_week_seq#51,d_day_name#52,d_quarter_name#53,d_holiday#54,d_weekend#55,d_following_holiday#56,d_first_dom#57,d_last_dom#58,d_same_day_ly#59,d_same_day_lq#60,d_current_day#61,... 4 more fields] parquet
               :     +- Project [i_item_sk#66, i_item_id#67]
               :        +- Filter isnotnull(i_item_sk#66)
               :           +- Relation[i_item_sk#66,i_item_id#67,i_rec_start_date#68,i_rec_end_date#69,i_item_desc#70,i_current_price#71,i_wholesale_cost#72,i_brand_id#73,i_brand#74,i_class_id#75,i_class#76,i_category_id#77,i_category#78,i_manufact_id#79,i_manufact#80,i_size#81,i_formulation#82,i_color#83,i_units#84,i_container#85,i_manager_id#86,i_product_name#87] parquet
               +- Project [p_promo_sk#88]
                  +- Filter (((p_channel_email#97 = N) || (p_channel_event#102 = N)) && isnotnull(p_promo_sk#88))
                     +- Relation[p_promo_sk#88,p_promo_id#89,p_start_date_sk#90,p_end_date_sk#91,p_item_sk#92,p_cost#93,p_response_target#94,p_promo_name#95,p_channel_dmail#96,p_channel_email#97,p_channel_catalog#98,p_channel_tv#99,p_channel_radio#100,p_channel_press#101,p_channel_event#102,p_channel_demo#103,p_channel_details#104,p_purpose#105,p_discount_active#106] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_item_id#67 ASC NULLS FIRST], output=[i_item_id#67,agg1#0,agg2#1,agg3#2,agg4#3])
+- *(6) HashAggregate(keys=[i_item_id#67], functions=[avg(cast(ss_quantity#16 as bigint)), avg(ss_list_price#18), avg(ss_coupon_amt#25), avg(ss_sales_price#19)], output=[i_item_id#67, agg1#0, agg2#1, agg3#2, agg4#3])
   +- Exchange hashpartitioning(i_item_id#67, 200)
      +- *(5) HashAggregate(keys=[i_item_id#67], functions=[partial_avg(cast(ss_quantity#16 as bigint)), partial_avg(ss_list_price#18), partial_avg(ss_coupon_amt#25), partial_avg(ss_sales_price#19)], output=[i_item_id#67, sum#120, count#121L, sum#122, count#123L, sum#124, count#125L, sum#126, count#127L])
         +- *(5) Project [ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25, i_item_id#67]
            +- *(5) BroadcastHashJoin [ss_promo_sk#14], [p_promo_sk#88], Inner, BuildRight
               :- *(5) Project [ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25, i_item_id#67]
               :  +- *(5) BroadcastHashJoin [ss_item_sk#8], [i_item_sk#66], Inner, BuildRight
               :     :- *(5) Project [ss_item_sk#8, ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25]
               :     :  +- *(5) BroadcastHashJoin [ss_sold_date_sk#6], [d_date_sk#38], Inner, BuildRight
               :     :     :- *(5) Project [ss_sold_date_sk#6, ss_item_sk#8, ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25]
               :     :     :  +- *(5) BroadcastHashJoin [ss_cdemo_sk#10], [cd_demo_sk#29], Inner, BuildRight
               :     :     :     :- *(5) Project [ss_sold_date_sk#6, ss_item_sk#8, ss_cdemo_sk#10, ss_promo_sk#14, ss_quantity#16, ss_list_price#18, ss_sales_price#19, ss_coupon_amt#25]
               :     :     :     :  +- *(5) Filter (((isnotnull(ss_cdemo_sk#10) && isnotnull(ss_sold_date_sk#6)) && isnotnull(ss_item_sk#8)) && isnotnull(ss_promo_sk#14))
               :     :     :     :     +- *(5) FileScan parquet tpcds.store_sales[ss_sold_date_sk#6,ss_item_sk#8,ss_cdemo_sk#10,ss_promo_sk#14,ss_quantity#16,ss_list_price#18,ss_sales_price#19,ss_coupon_amt#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_cdemo_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk), IsNotNull(ss_promo_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_cdemo_sk:int,ss_promo_sk:int,ss_quantity:int,ss_list...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(1) Project [cd_demo_sk#29]
               :     :     :           +- *(1) Filter ((((((isnotnull(cd_marital_status#31) && isnotnull(cd_gender#30)) && isnotnull(cd_education_status#32)) && (cd_gender#30 = M)) && (cd_marital_status#31 = S)) && (cd_education_status#32 = College)) && isnotnull(cd_demo_sk#29))
               :     :     :              +- *(1) FileScan parquet tpcds.customer_demographics[cd_demo_sk#29,cd_gender#30,cd_marital_status#31,cd_education_status#32] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_marital_status), IsNotNull(cd_gender), IsNotNull(cd_education_status), EqualTo(cd_g..., ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(2) Project [d_date_sk#38]
               :     :           +- *(2) Filter ((isnotnull(d_year#44) && (d_year#44 = 2000)) && isnotnull(d_date_sk#38))
               :     :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#38,d_year#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(3) Project [i_item_sk#66, i_item_id#67]
               :           +- *(3) Filter isnotnull(i_item_sk#66)
               :              +- *(3) FileScan parquet tpcds.item[i_item_sk#66,i_item_id#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(4) Project [p_promo_sk#88]
                     +- *(4) Filter (((p_channel_email#97 = N) || (p_channel_event#102 = N)) && isnotnull(p_promo_sk#88))
                        +- *(4) FileScan parquet tpcds.promotion[p_promo_sk#88,p_channel_email#97,p_channel_event#102] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/p..., PartitionFilters: [], PushedFilters: [Or(EqualTo(p_channel_email,N),EqualTo(p_channel_event,N)), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_email:string,p_channel_event:string>
Time taken: 3.826 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 7 in stream 0 using template query7.tpl
----------------------------------------------------^^^

