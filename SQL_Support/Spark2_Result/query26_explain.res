== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id], ['i_item_id, 'avg('cs_quantity) AS agg1#0, 'avg('cs_list_price) AS agg2#1, 'avg('cs_coupon_amt) AS agg3#2, 'avg('cs_sales_price) AS agg4#3]
         +- 'Filter ((((('cs_sold_date_sk = 'd_date_sk) && ('cs_item_sk = 'i_item_sk)) && ('cs_bill_cdemo_sk = 'cd_demo_sk)) && (('cs_promo_sk = 'p_promo_sk) && ('cd_gender = M))) && ((('cd_marital_status = S) && ('cd_education_status = College)) && ((('p_channel_email = N) || ('p_channel_event = N)) && ('d_year = 2000))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation `catalog_sales`
               :  :  :  +- 'UnresolvedRelation `customer_demographics`
               :  :  +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `item`
               +- 'UnresolvedRelation `promotion`

== Analyzed Logical Plan ==
i_item_id: string, agg1: double, agg2: double, agg3: double, agg4: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [i_item_id#78, agg1#0, agg2#1, agg3#2, agg4#3]
      +- Sort [i_item_id#78 ASC NULLS FIRST], true
         +- Aggregate [i_item_id#78], [i_item_id#78, avg(cast(cs_quantity#24 as bigint)) AS agg1#0, avg(cs_list_price#26) AS agg2#1, avg(cs_coupon_amt#33) AS agg3#2, avg(cs_sales_price#27) AS agg4#3]
            +- Filter (((((cs_sold_date_sk#6 = d_date_sk#49) && (cs_item_sk#21 = i_item_sk#77)) && (cs_bill_cdemo_sk#10 = cd_demo_sk#40)) && ((cs_promo_sk#22 = p_promo_sk#99) && (cd_gender#41 = M))) && (((cd_marital_status#42 = S) && (cd_education_status#43 = College)) && (((p_channel_email#108 = N) || (p_channel_event#113 = N)) && (d_year#55 = 2000))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
                  :  :  :  :  +- Relation[cs_sold_date_sk#6,cs_sold_time_sk#7,cs_ship_date_sk#8,cs_bill_customer_sk#9,cs_bill_cdemo_sk#10,cs_bill_hdemo_sk#11,cs_bill_addr_sk#12,cs_ship_customer_sk#13,cs_ship_cdemo_sk#14,cs_ship_hdemo_sk#15,cs_ship_addr_sk#16,cs_call_center_sk#17,cs_catalog_page_sk#18,cs_ship_mode_sk#19,cs_warehouse_sk#20,cs_item_sk#21,cs_promo_sk#22,cs_order_number#23,cs_quantity#24,cs_wholesale_cost#25,cs_list_price#26,cs_sales_price#27,cs_ext_discount_amt#28,cs_ext_sales_price#29,... 10 more fields] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`customer_demographics`
                  :  :  :     +- Relation[cd_demo_sk#40,cd_gender#41,cd_marital_status#42,cd_education_status#43,cd_purchase_estimate#44,cd_credit_rating#45,cd_dep_count#46,cd_dep_employed_count#47,cd_dep_college_count#48] parquet
                  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :     +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`item`
                  :     +- Relation[i_item_sk#77,i_item_id#78,i_rec_start_date#79,i_rec_end_date#80,i_item_desc#81,i_current_price#82,i_wholesale_cost#83,i_brand_id#84,i_brand#85,i_class_id#86,i_class#87,i_category_id#88,i_category#89,i_manufact_id#90,i_manufact#91,i_size#92,i_formulation#93,i_color#94,i_units#95,i_container#96,i_manager_id#97,i_product_name#98] parquet
                  +- SubqueryAlias `tpcds`.`promotion`
                     +- Relation[p_promo_sk#99,p_promo_id#100,p_start_date_sk#101,p_end_date_sk#102,p_item_sk#103,p_cost#104,p_response_target#105,p_promo_name#106,p_channel_dmail#107,p_channel_email#108,p_channel_catalog#109,p_channel_tv#110,p_channel_radio#111,p_channel_press#112,p_channel_event#113,p_channel_demo#114,p_channel_details#115,p_purpose#116,p_discount_active#117] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#78 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#78], [i_item_id#78, avg(cast(cs_quantity#24 as bigint)) AS agg1#0, avg(cs_list_price#26) AS agg2#1, avg(cs_coupon_amt#33) AS agg3#2, avg(cs_sales_price#27) AS agg4#3]
         +- Project [cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33, i_item_id#78]
            +- Join Inner, (cs_promo_sk#22 = p_promo_sk#99)
               :- Project [cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33, i_item_id#78]
               :  +- Join Inner, (cs_item_sk#21 = i_item_sk#77)
               :     :- Project [cs_item_sk#21, cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33]
               :     :  +- Join Inner, (cs_sold_date_sk#6 = d_date_sk#49)
               :     :     :- Project [cs_sold_date_sk#6, cs_item_sk#21, cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33]
               :     :     :  +- Join Inner, (cs_bill_cdemo_sk#10 = cd_demo_sk#40)
               :     :     :     :- Project [cs_sold_date_sk#6, cs_bill_cdemo_sk#10, cs_item_sk#21, cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33]
               :     :     :     :  +- Filter (((isnotnull(cs_bill_cdemo_sk#10) && isnotnull(cs_sold_date_sk#6)) && isnotnull(cs_item_sk#21)) && isnotnull(cs_promo_sk#22))
               :     :     :     :     +- Relation[cs_sold_date_sk#6,cs_sold_time_sk#7,cs_ship_date_sk#8,cs_bill_customer_sk#9,cs_bill_cdemo_sk#10,cs_bill_hdemo_sk#11,cs_bill_addr_sk#12,cs_ship_customer_sk#13,cs_ship_cdemo_sk#14,cs_ship_hdemo_sk#15,cs_ship_addr_sk#16,cs_call_center_sk#17,cs_catalog_page_sk#18,cs_ship_mode_sk#19,cs_warehouse_sk#20,cs_item_sk#21,cs_promo_sk#22,cs_order_number#23,cs_quantity#24,cs_wholesale_cost#25,cs_list_price#26,cs_sales_price#27,cs_ext_discount_amt#28,cs_ext_sales_price#29,... 10 more fields] parquet
               :     :     :     +- Project [cd_demo_sk#40]
               :     :     :        +- Filter ((((((isnotnull(cd_marital_status#42) && isnotnull(cd_gender#41)) && isnotnull(cd_education_status#43)) && (cd_gender#41 = M)) && (cd_marital_status#42 = S)) && (cd_education_status#43 = College)) && isnotnull(cd_demo_sk#40))
               :     :     :           +- Relation[cd_demo_sk#40,cd_gender#41,cd_marital_status#42,cd_education_status#43,cd_purchase_estimate#44,cd_credit_rating#45,cd_dep_count#46,cd_dep_employed_count#47,cd_dep_college_count#48] parquet
               :     :     +- Project [d_date_sk#49]
               :     :        +- Filter ((isnotnull(d_year#55) && (d_year#55 = 2000)) && isnotnull(d_date_sk#49))
               :     :           +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
               :     +- Project [i_item_sk#77, i_item_id#78]
               :        +- Filter isnotnull(i_item_sk#77)
               :           +- Relation[i_item_sk#77,i_item_id#78,i_rec_start_date#79,i_rec_end_date#80,i_item_desc#81,i_current_price#82,i_wholesale_cost#83,i_brand_id#84,i_brand#85,i_class_id#86,i_class#87,i_category_id#88,i_category#89,i_manufact_id#90,i_manufact#91,i_size#92,i_formulation#93,i_color#94,i_units#95,i_container#96,i_manager_id#97,i_product_name#98] parquet
               +- Project [p_promo_sk#99]
                  +- Filter (((p_channel_email#108 = N) || (p_channel_event#113 = N)) && isnotnull(p_promo_sk#99))
                     +- Relation[p_promo_sk#99,p_promo_id#100,p_start_date_sk#101,p_end_date_sk#102,p_item_sk#103,p_cost#104,p_response_target#105,p_promo_name#106,p_channel_dmail#107,p_channel_email#108,p_channel_catalog#109,p_channel_tv#110,p_channel_radio#111,p_channel_press#112,p_channel_event#113,p_channel_demo#114,p_channel_details#115,p_purpose#116,p_discount_active#117] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_item_id#78 ASC NULLS FIRST], output=[i_item_id#78,agg1#0,agg2#1,agg3#2,agg4#3])
+- *(6) HashAggregate(keys=[i_item_id#78], functions=[avg(cast(cs_quantity#24 as bigint)), avg(cs_list_price#26), avg(cs_coupon_amt#33), avg(cs_sales_price#27)], output=[i_item_id#78, agg1#0, agg2#1, agg3#2, agg4#3])
   +- Exchange hashpartitioning(i_item_id#78, 200)
      +- *(5) HashAggregate(keys=[i_item_id#78], functions=[partial_avg(cast(cs_quantity#24 as bigint)), partial_avg(cs_list_price#26), partial_avg(cs_coupon_amt#33), partial_avg(cs_sales_price#27)], output=[i_item_id#78, sum#131, count#132L, sum#133, count#134L, sum#135, count#136L, sum#137, count#138L])
         +- *(5) Project [cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33, i_item_id#78]
            +- *(5) BroadcastHashJoin [cs_promo_sk#22], [p_promo_sk#99], Inner, BuildRight
               :- *(5) Project [cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33, i_item_id#78]
               :  +- *(5) BroadcastHashJoin [cs_item_sk#21], [i_item_sk#77], Inner, BuildRight
               :     :- *(5) Project [cs_item_sk#21, cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33]
               :     :  +- *(5) BroadcastHashJoin [cs_sold_date_sk#6], [d_date_sk#49], Inner, BuildRight
               :     :     :- *(5) Project [cs_sold_date_sk#6, cs_item_sk#21, cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33]
               :     :     :  +- *(5) BroadcastHashJoin [cs_bill_cdemo_sk#10], [cd_demo_sk#40], Inner, BuildRight
               :     :     :     :- *(5) Project [cs_sold_date_sk#6, cs_bill_cdemo_sk#10, cs_item_sk#21, cs_promo_sk#22, cs_quantity#24, cs_list_price#26, cs_sales_price#27, cs_coupon_amt#33]
               :     :     :     :  +- *(5) Filter (((isnotnull(cs_bill_cdemo_sk#10) && isnotnull(cs_sold_date_sk#6)) && isnotnull(cs_item_sk#21)) && isnotnull(cs_promo_sk#22))
               :     :     :     :     +- *(5) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#6,cs_bill_cdemo_sk#10,cs_item_sk#21,cs_promo_sk#22,cs_quantity#24,cs_list_price#26,cs_sales_price#27,cs_coupon_amt#33] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_bill_cdemo_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_item_sk), IsNotNull(cs_pro..., ReadSchema: struct<cs_sold_date_sk:int,cs_bill_cdemo_sk:int,cs_item_sk:int,cs_promo_sk:int,cs_quantity:int,cs...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(1) Project [cd_demo_sk#40]
               :     :     :           +- *(1) Filter ((((((isnotnull(cd_marital_status#42) && isnotnull(cd_gender#41)) && isnotnull(cd_education_status#43)) && (cd_gender#41 = M)) && (cd_marital_status#42 = S)) && (cd_education_status#43 = College)) && isnotnull(cd_demo_sk#40))
               :     :     :              +- *(1) FileScan parquet tpcds.customer_demographics[cd_demo_sk#40,cd_gender#41,cd_marital_status#42,cd_education_status#43] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_marital_status), IsNotNull(cd_gender), IsNotNull(cd_education_status), EqualTo(cd_g..., ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(2) Project [d_date_sk#49]
               :     :           +- *(2) Filter ((isnotnull(d_year#55) && (d_year#55 = 2000)) && isnotnull(d_date_sk#49))
               :     :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#49,d_year#55] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(3) Project [i_item_sk#77, i_item_id#78]
               :           +- *(3) Filter isnotnull(i_item_sk#77)
               :              +- *(3) FileScan parquet tpcds.item[i_item_sk#77,i_item_id#78] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(4) Project [p_promo_sk#99]
                     +- *(4) Filter (((p_channel_email#108 = N) || (p_channel_event#113 = N)) && isnotnull(p_promo_sk#99))
                        +- *(4) FileScan parquet tpcds.promotion[p_promo_sk#99,p_channel_email#108,p_channel_event#113] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/p..., PartitionFilters: [], PushedFilters: [Or(EqualTo(p_channel_email,N),EqualTo(p_channel_event,N)), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_email:string,p_channel_event:string>
Time taken: 4.047 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 26 in stream 0 using template query26.tpl
------------------------------------------------------^^^

