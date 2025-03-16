Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580544047
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id], ['i_item_id, 'avg('cs_quantity) AS agg1#0, 'avg('cs_list_price) AS agg2#1, 'avg('cs_coupon_amt) AS agg3#2, 'avg('cs_sales_price) AS agg4#3]
         +- 'Filter ((((('cs_sold_date_sk = 'd_date_sk) AND ('cs_item_sk = 'i_item_sk)) AND ('cs_bill_cdemo_sk = 'cd_demo_sk)) AND (('cs_promo_sk = 'p_promo_sk) AND ('cd_gender = M))) AND ((('cd_marital_status = S) AND ('cd_education_status = College)) AND ((('p_channel_email = N) OR ('p_channel_event = N)) AND ('d_year = 2000))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation [catalog_sales], [], false
               :  :  :  +- 'UnresolvedRelation [customer_demographics], [], false
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [item], [], false
               +- 'UnresolvedRelation [promotion], [], false

== Analyzed Logical Plan ==
i_item_id: string, agg1: double, agg2: double, agg3: double, agg4: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#81 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#81], [i_item_id#81, avg(cs_quantity#27) AS agg1#0, avg(cs_list_price#29) AS agg2#1, avg(cs_coupon_amt#36) AS agg3#2, avg(cs_sales_price#30) AS agg4#3]
         +- Filter (((((cs_sold_date_sk#9 = d_date_sk#52) AND (cs_item_sk#24 = i_item_sk#80)) AND (cs_bill_cdemo_sk#13 = cd_demo_sk#43)) AND ((cs_promo_sk#25 = p_promo_sk#102) AND (cd_gender#44 = M))) AND (((cd_marital_status#45 = S) AND (cd_education_status#46 = College)) AND (((p_channel_email#111 = N) OR (p_channel_event#116 = N)) AND (d_year#58 = 2000))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#9,cs_sold_time_sk#10,cs_ship_date_sk#11,cs_bill_customer_sk#12,cs_bill_cdemo_sk#13,cs_bill_hdemo_sk#14,cs_bill_addr_sk#15,cs_ship_customer_sk#16,cs_ship_cdemo_sk#17,cs_ship_hdemo_sk#18,cs_ship_addr_sk#19,cs_call_center_sk#20,cs_catalog_page_sk#21,cs_ship_mode_sk#22,cs_warehouse_sk#23,cs_item_sk#24,cs_promo_sk#25,cs_order_number#26,cs_quantity#27,cs_wholesale_cost#28,cs_list_price#29,cs_sales_price#30,cs_ext_discount_amt#31,cs_ext_sales_price#32,... 10 more fields] parquet
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
               :  :  :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#43,cd_gender#44,cd_marital_status#45,cd_education_status#46,cd_purchase_estimate#47,cd_credit_rating#48,cd_dep_count#49,cd_dep_employed_count#50,cd_dep_college_count#51] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.item
               :     +- Relation spark_catalog.tpcds.item[i_item_sk#80,i_item_id#81,i_rec_start_date#82,i_rec_end_date#83,i_item_desc#84,i_current_price#85,i_wholesale_cost#86,i_brand_id#87,i_brand#88,i_class_id#89,i_class#90,i_category_id#91,i_category#92,i_manufact_id#93,i_manufact#94,i_size#95,i_formulation#96,i_color#97,i_units#98,i_container#99,i_manager_id#100,i_product_name#101] parquet
               +- SubqueryAlias spark_catalog.tpcds.promotion
                  +- Relation spark_catalog.tpcds.promotion[p_promo_sk#102,p_promo_id#103,p_start_date_sk#104,p_end_date_sk#105,p_item_sk#106,p_cost#107,p_response_target#108,p_promo_name#109,p_channel_dmail#110,p_channel_email#111,p_channel_catalog#112,p_channel_tv#113,p_channel_radio#114,p_channel_press#115,p_channel_event#116,p_channel_demo#117,p_channel_details#118,p_purpose#119,p_discount_active#120] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#81 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#81], [i_item_id#81, avg(cs_quantity#27) AS agg1#0, avg(cs_list_price#29) AS agg2#1, avg(cs_coupon_amt#36) AS agg3#2, avg(cs_sales_price#30) AS agg4#3]
         +- Project [cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36, i_item_id#81]
            +- Join Inner, (cs_promo_sk#25 = p_promo_sk#102)
               :- Project [cs_promo_sk#25, cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36, i_item_id#81]
               :  +- Join Inner, (cs_item_sk#24 = i_item_sk#80)
               :     :- Project [cs_item_sk#24, cs_promo_sk#25, cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36]
               :     :  +- Join Inner, (cs_sold_date_sk#9 = d_date_sk#52)
               :     :     :- Project [cs_sold_date_sk#9, cs_item_sk#24, cs_promo_sk#25, cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36]
               :     :     :  +- Join Inner, (cs_bill_cdemo_sk#13 = cd_demo_sk#43)
               :     :     :     :- Project [cs_sold_date_sk#9, cs_bill_cdemo_sk#13, cs_item_sk#24, cs_promo_sk#25, cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36]
               :     :     :     :  +- Filter ((isnotnull(cs_bill_cdemo_sk#13) AND isnotnull(cs_sold_date_sk#9)) AND (isnotnull(cs_item_sk#24) AND isnotnull(cs_promo_sk#25)))
               :     :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#9,cs_sold_time_sk#10,cs_ship_date_sk#11,cs_bill_customer_sk#12,cs_bill_cdemo_sk#13,cs_bill_hdemo_sk#14,cs_bill_addr_sk#15,cs_ship_customer_sk#16,cs_ship_cdemo_sk#17,cs_ship_hdemo_sk#18,cs_ship_addr_sk#19,cs_call_center_sk#20,cs_catalog_page_sk#21,cs_ship_mode_sk#22,cs_warehouse_sk#23,cs_item_sk#24,cs_promo_sk#25,cs_order_number#26,cs_quantity#27,cs_wholesale_cost#28,cs_list_price#29,cs_sales_price#30,cs_ext_discount_amt#31,cs_ext_sales_price#32,... 10 more fields] parquet
               :     :     :     +- Project [cd_demo_sk#43]
               :     :     :        +- Filter ((((isnotnull(cd_gender#44) AND isnotnull(cd_marital_status#45)) AND isnotnull(cd_education_status#46)) AND (((cd_gender#44 = M) AND (cd_marital_status#45 = S)) AND (cd_education_status#46 = College))) AND isnotnull(cd_demo_sk#43))
               :     :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#43,cd_gender#44,cd_marital_status#45,cd_education_status#46,cd_purchase_estimate#47,cd_credit_rating#48,cd_dep_count#49,cd_dep_employed_count#50,cd_dep_college_count#51] parquet
               :     :     +- Project [d_date_sk#52]
               :     :        +- Filter ((isnotnull(d_year#58) AND (d_year#58 = 2000)) AND isnotnull(d_date_sk#52))
               :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
               :     +- Project [i_item_sk#80, i_item_id#81]
               :        +- Filter isnotnull(i_item_sk#80)
               :           +- Relation spark_catalog.tpcds.item[i_item_sk#80,i_item_id#81,i_rec_start_date#82,i_rec_end_date#83,i_item_desc#84,i_current_price#85,i_wholesale_cost#86,i_brand_id#87,i_brand#88,i_class_id#89,i_class#90,i_category_id#91,i_category#92,i_manufact_id#93,i_manufact#94,i_size#95,i_formulation#96,i_color#97,i_units#98,i_container#99,i_manager_id#100,i_product_name#101] parquet
               +- Project [p_promo_sk#102]
                  +- Filter (((p_channel_email#111 = N) OR (p_channel_event#116 = N)) AND isnotnull(p_promo_sk#102))
                     +- Relation spark_catalog.tpcds.promotion[p_promo_sk#102,p_promo_id#103,p_start_date_sk#104,p_end_date_sk#105,p_item_sk#106,p_cost#107,p_response_target#108,p_promo_name#109,p_channel_dmail#110,p_channel_email#111,p_channel_catalog#112,p_channel_tv#113,p_channel_radio#114,p_channel_press#115,p_channel_event#116,p_channel_demo#117,p_channel_details#118,p_purpose#119,p_discount_active#120] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_item_id#81 ASC NULLS FIRST], output=[i_item_id#81,agg1#0,agg2#1,agg3#2,agg4#3])
   +- HashAggregate(keys=[i_item_id#81], functions=[avg(cs_quantity#27), avg(cs_list_price#29), avg(cs_coupon_amt#36), avg(cs_sales_price#30)], output=[i_item_id#81, agg1#0, agg2#1, agg3#2, agg4#3])
      +- Exchange hashpartitioning(i_item_id#81, 200), ENSURE_REQUIREMENTS, [plan_id=116]
         +- HashAggregate(keys=[i_item_id#81], functions=[partial_avg(cs_quantity#27), partial_avg(cs_list_price#29), partial_avg(cs_coupon_amt#36), partial_avg(cs_sales_price#30)], output=[i_item_id#81, sum#138, count#139L, sum#140, count#141L, sum#142, count#143L, sum#144, count#145L])
            +- Project [cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36, i_item_id#81]
               +- BroadcastHashJoin [cs_promo_sk#25], [p_promo_sk#102], Inner, BuildRight, false
                  :- Project [cs_promo_sk#25, cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36, i_item_id#81]
                  :  +- BroadcastHashJoin [cs_item_sk#24], [i_item_sk#80], Inner, BuildRight, false
                  :     :- Project [cs_item_sk#24, cs_promo_sk#25, cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36]
                  :     :  +- BroadcastHashJoin [cs_sold_date_sk#9], [d_date_sk#52], Inner, BuildRight, false
                  :     :     :- Project [cs_sold_date_sk#9, cs_item_sk#24, cs_promo_sk#25, cs_quantity#27, cs_list_price#29, cs_sales_price#30, cs_coupon_amt#36]
                  :     :     :  +- BroadcastHashJoin [cs_bill_cdemo_sk#13], [cd_demo_sk#43], Inner, BuildRight, false
                  :     :     :     :- Filter (((isnotnull(cs_bill_cdemo_sk#13) AND isnotnull(cs_sold_date_sk#9)) AND isnotnull(cs_item_sk#24)) AND isnotnull(cs_promo_sk#25))
                  :     :     :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#9,cs_bill_cdemo_sk#13,cs_item_sk#24,cs_promo_sk#25,cs_quantity#27,cs_list_price#29,cs_sales_price#30,cs_coupon_amt#36] Batched: true, DataFilters: [isnotnull(cs_bill_cdemo_sk#13), isnotnull(cs_sold_date_sk#9), isnotnull(cs_item_sk#24), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_bill_cdemo_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_item_sk), IsNotNull(cs_pro..., ReadSchema: struct<cs_sold_date_sk:int,cs_bill_cdemo_sk:int,cs_item_sk:int,cs_promo_sk:int,cs_quantity:int,cs...
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=99]
                  :     :     :        +- Project [cd_demo_sk#43]
                  :     :     :           +- Filter ((((((isnotnull(cd_gender#44) AND isnotnull(cd_marital_status#45)) AND isnotnull(cd_education_status#46)) AND (cd_gender#44 = M)) AND (cd_marital_status#45 = S)) AND (cd_education_status#46 = College)) AND isnotnull(cd_demo_sk#43))
                  :     :     :              +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#43,cd_gender#44,cd_marital_status#45,cd_education_status#46] Batched: true, DataFilters: [isnotnull(cd_gender#44), isnotnull(cd_marital_status#45), isnotnull(cd_education_status#46), (cd..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_gender), IsNotNull(cd_marital_status), IsNotNull(cd_education_status), EqualTo(cd_g..., ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=103]
                  :     :        +- Project [d_date_sk#52]
                  :     :           +- Filter ((isnotnull(d_year#58) AND (d_year#58 = 2000)) AND isnotnull(d_date_sk#52))
                  :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#52,d_year#58] Batched: true, DataFilters: [isnotnull(d_year#58), (d_year#58 = 2000), isnotnull(d_date_sk#52)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=107]
                  :        +- Filter isnotnull(i_item_sk#80)
                  :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#80,i_item_id#81] Batched: true, DataFilters: [isnotnull(i_item_sk#80)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=111]
                     +- Project [p_promo_sk#102]
                        +- Filter (((p_channel_email#111 = N) OR (p_channel_event#116 = N)) AND isnotnull(p_promo_sk#102))
                           +- FileScan parquet spark_catalog.tpcds.promotion[p_promo_sk#102,p_channel_email#111,p_channel_event#116] Batched: true, DataFilters: [((p_channel_email#111 = N) OR (p_channel_event#116 = N)), isnotnull(p_promo_sk#102)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(EqualTo(p_channel_email,N),EqualTo(p_channel_event,N)), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_email:string,p_channel_event:string>

Time taken: 2.674 seconds, Fetched 1 row(s)
