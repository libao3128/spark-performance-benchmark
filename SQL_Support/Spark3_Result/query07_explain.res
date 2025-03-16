Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579711572
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id], ['i_item_id, 'avg('ss_quantity) AS agg1#0, 'avg('ss_list_price) AS agg2#1, 'avg('ss_coupon_amt) AS agg3#2, 'avg('ss_sales_price) AS agg4#3]
         +- 'Filter ((((('ss_sold_date_sk = 'd_date_sk) AND ('ss_item_sk = 'i_item_sk)) AND ('ss_cdemo_sk = 'cd_demo_sk)) AND (('ss_promo_sk = 'p_promo_sk) AND ('cd_gender = M))) AND ((('cd_marital_status = S) AND ('cd_education_status = College)) AND ((('p_channel_email = N) OR ('p_channel_event = N)) AND ('d_year = 2000))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :  :  +- 'UnresolvedRelation [customer_demographics], [], false
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [item], [], false
               +- 'UnresolvedRelation [promotion], [], false

== Analyzed Logical Plan ==
i_item_id: string, agg1: double, agg2: double, agg3: double, agg4: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#70 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#70], [i_item_id#70, avg(ss_quantity#19) AS agg1#0, avg(ss_list_price#21) AS agg2#1, avg(ss_coupon_amt#28) AS agg3#2, avg(ss_sales_price#22) AS agg4#3]
         +- Filter (((((ss_sold_date_sk#9 = d_date_sk#41) AND (ss_item_sk#11 = i_item_sk#69)) AND (ss_cdemo_sk#13 = cd_demo_sk#32)) AND ((ss_promo_sk#17 = p_promo_sk#91) AND (cd_gender#33 = M))) AND (((cd_marital_status#34 = S) AND (cd_education_status#35 = College)) AND (((p_channel_email#100 = N) OR (p_channel_event#105 = N)) AND (d_year#47 = 2000))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
               :  :  :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#32,cd_gender#33,cd_marital_status#34,cd_education_status#35,cd_purchase_estimate#36,cd_credit_rating#37,cd_dep_count#38,cd_dep_employed_count#39,cd_dep_college_count#40] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.item
               :     +- Relation spark_catalog.tpcds.item[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet
               +- SubqueryAlias spark_catalog.tpcds.promotion
                  +- Relation spark_catalog.tpcds.promotion[p_promo_sk#91,p_promo_id#92,p_start_date_sk#93,p_end_date_sk#94,p_item_sk#95,p_cost#96,p_response_target#97,p_promo_name#98,p_channel_dmail#99,p_channel_email#100,p_channel_catalog#101,p_channel_tv#102,p_channel_radio#103,p_channel_press#104,p_channel_event#105,p_channel_demo#106,p_channel_details#107,p_purpose#108,p_discount_active#109] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#70 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#70], [i_item_id#70, avg(ss_quantity#19) AS agg1#0, avg(ss_list_price#21) AS agg2#1, avg(ss_coupon_amt#28) AS agg3#2, avg(ss_sales_price#22) AS agg4#3]
         +- Project [ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28, i_item_id#70]
            +- Join Inner, (ss_promo_sk#17 = p_promo_sk#91)
               :- Project [ss_promo_sk#17, ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28, i_item_id#70]
               :  +- Join Inner, (ss_item_sk#11 = i_item_sk#69)
               :     :- Project [ss_item_sk#11, ss_promo_sk#17, ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28]
               :     :  +- Join Inner, (ss_sold_date_sk#9 = d_date_sk#41)
               :     :     :- Project [ss_sold_date_sk#9, ss_item_sk#11, ss_promo_sk#17, ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28]
               :     :     :  +- Join Inner, (ss_cdemo_sk#13 = cd_demo_sk#32)
               :     :     :     :- Project [ss_sold_date_sk#9, ss_item_sk#11, ss_cdemo_sk#13, ss_promo_sk#17, ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28]
               :     :     :     :  +- Filter ((isnotnull(ss_cdemo_sk#13) AND isnotnull(ss_sold_date_sk#9)) AND (isnotnull(ss_item_sk#11) AND isnotnull(ss_promo_sk#17)))
               :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
               :     :     :     +- Project [cd_demo_sk#32]
               :     :     :        +- Filter ((((isnotnull(cd_gender#33) AND isnotnull(cd_marital_status#34)) AND isnotnull(cd_education_status#35)) AND (((cd_gender#33 = M) AND (cd_marital_status#34 = S)) AND (cd_education_status#35 = College))) AND isnotnull(cd_demo_sk#32))
               :     :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#32,cd_gender#33,cd_marital_status#34,cd_education_status#35,cd_purchase_estimate#36,cd_credit_rating#37,cd_dep_count#38,cd_dep_employed_count#39,cd_dep_college_count#40] parquet
               :     :     +- Project [d_date_sk#41]
               :     :        +- Filter ((isnotnull(d_year#47) AND (d_year#47 = 2000)) AND isnotnull(d_date_sk#41))
               :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
               :     +- Project [i_item_sk#69, i_item_id#70]
               :        +- Filter isnotnull(i_item_sk#69)
               :           +- Relation spark_catalog.tpcds.item[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet
               +- Project [p_promo_sk#91]
                  +- Filter (((p_channel_email#100 = N) OR (p_channel_event#105 = N)) AND isnotnull(p_promo_sk#91))
                     +- Relation spark_catalog.tpcds.promotion[p_promo_sk#91,p_promo_id#92,p_start_date_sk#93,p_end_date_sk#94,p_item_sk#95,p_cost#96,p_response_target#97,p_promo_name#98,p_channel_dmail#99,p_channel_email#100,p_channel_catalog#101,p_channel_tv#102,p_channel_radio#103,p_channel_press#104,p_channel_event#105,p_channel_demo#106,p_channel_details#107,p_purpose#108,p_discount_active#109] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_item_id#70 ASC NULLS FIRST], output=[i_item_id#70,agg1#0,agg2#1,agg3#2,agg4#3])
   +- HashAggregate(keys=[i_item_id#70], functions=[avg(ss_quantity#19), avg(ss_list_price#21), avg(ss_coupon_amt#28), avg(ss_sales_price#22)], output=[i_item_id#70, agg1#0, agg2#1, agg3#2, agg4#3])
      +- Exchange hashpartitioning(i_item_id#70, 200), ENSURE_REQUIREMENTS, [plan_id=116]
         +- HashAggregate(keys=[i_item_id#70], functions=[partial_avg(ss_quantity#19), partial_avg(ss_list_price#21), partial_avg(ss_coupon_amt#28), partial_avg(ss_sales_price#22)], output=[i_item_id#70, sum#127, count#128L, sum#129, count#130L, sum#131, count#132L, sum#133, count#134L])
            +- Project [ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28, i_item_id#70]
               +- BroadcastHashJoin [ss_promo_sk#17], [p_promo_sk#91], Inner, BuildRight, false
                  :- Project [ss_promo_sk#17, ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28, i_item_id#70]
                  :  +- BroadcastHashJoin [ss_item_sk#11], [i_item_sk#69], Inner, BuildRight, false
                  :     :- Project [ss_item_sk#11, ss_promo_sk#17, ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28]
                  :     :  +- BroadcastHashJoin [ss_sold_date_sk#9], [d_date_sk#41], Inner, BuildRight, false
                  :     :     :- Project [ss_sold_date_sk#9, ss_item_sk#11, ss_promo_sk#17, ss_quantity#19, ss_list_price#21, ss_sales_price#22, ss_coupon_amt#28]
                  :     :     :  +- BroadcastHashJoin [ss_cdemo_sk#13], [cd_demo_sk#32], Inner, BuildRight, false
                  :     :     :     :- Filter (((isnotnull(ss_cdemo_sk#13) AND isnotnull(ss_sold_date_sk#9)) AND isnotnull(ss_item_sk#11)) AND isnotnull(ss_promo_sk#17))
                  :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#9,ss_item_sk#11,ss_cdemo_sk#13,ss_promo_sk#17,ss_quantity#19,ss_list_price#21,ss_sales_price#22,ss_coupon_amt#28] Batched: true, DataFilters: [isnotnull(ss_cdemo_sk#13), isnotnull(ss_sold_date_sk#9), isnotnull(ss_item_sk#11), isnotnull(ss_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_cdemo_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk), IsNotNull(ss_promo_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_cdemo_sk:int,ss_promo_sk:int,ss_quantity:int,ss_list...
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=99]
                  :     :     :        +- Project [cd_demo_sk#32]
                  :     :     :           +- Filter ((((((isnotnull(cd_gender#33) AND isnotnull(cd_marital_status#34)) AND isnotnull(cd_education_status#35)) AND (cd_gender#33 = M)) AND (cd_marital_status#34 = S)) AND (cd_education_status#35 = College)) AND isnotnull(cd_demo_sk#32))
                  :     :     :              +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#32,cd_gender#33,cd_marital_status#34,cd_education_status#35] Batched: true, DataFilters: [isnotnull(cd_gender#33), isnotnull(cd_marital_status#34), isnotnull(cd_education_status#35), (cd..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_gender), IsNotNull(cd_marital_status), IsNotNull(cd_education_status), EqualTo(cd_g..., ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=103]
                  :     :        +- Project [d_date_sk#41]
                  :     :           +- Filter ((isnotnull(d_year#47) AND (d_year#47 = 2000)) AND isnotnull(d_date_sk#41))
                  :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#41,d_year#47] Batched: true, DataFilters: [isnotnull(d_year#47), (d_year#47 = 2000), isnotnull(d_date_sk#41)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=107]
                  :        +- Filter isnotnull(i_item_sk#69)
                  :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#69,i_item_id#70] Batched: true, DataFilters: [isnotnull(i_item_sk#69)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=111]
                     +- Project [p_promo_sk#91]
                        +- Filter (((p_channel_email#100 = N) OR (p_channel_event#105 = N)) AND isnotnull(p_promo_sk#91))
                           +- FileScan parquet spark_catalog.tpcds.promotion[p_promo_sk#91,p_channel_email#100,p_channel_event#105] Batched: true, DataFilters: [((p_channel_email#100 = N) OR (p_channel_event#105 = N)), isnotnull(p_promo_sk#91)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(EqualTo(p_channel_email,N),EqualTo(p_channel_event,N)), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_email:string,p_channel_event:string>

Time taken: 2.567 seconds, Fetched 1 row(s)
