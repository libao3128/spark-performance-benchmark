Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579673430
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['cnt ASC NULLS FIRST], true
      +- 'UnresolvedHaving ('count(1) >= 10)
         +- 'Aggregate ['a.ca_state], ['a.ca_state AS state#0, 'count(1) AS cnt#1]
            +- 'Filter (((('a.ca_address_sk = 'c.c_current_addr_sk) AND ('c.c_customer_sk = 's.ss_customer_sk)) AND ('s.ss_sold_date_sk = 'd.d_date_sk)) AND ((('s.ss_item_sk = 'i.i_item_sk) AND ('d.d_month_seq = scalar-subquery#2 [])) AND ('i.i_current_price > (1.2 * scalar-subquery#3 []))))
               :  :- 'Distinct
               :  :  +- 'Project ['d_month_seq]
               :  :     +- 'Filter (('d_year = 2001) AND ('d_moy = 1))
               :  :        +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'Project [unresolvedalias('avg('j.i_current_price), None)]
               :     +- 'Filter ('j.i_category = 'i.i_category)
               :        +- 'SubqueryAlias j
               :           +- 'UnresolvedRelation [item], [], false
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'Join Inner
                  :  :  :- 'Join Inner
                  :  :  :  :- 'SubqueryAlias a
                  :  :  :  :  +- 'UnresolvedRelation [customer_address], [], false
                  :  :  :  +- 'SubqueryAlias c
                  :  :  :     +- 'UnresolvedRelation [customer], [], false
                  :  :  +- 'SubqueryAlias s
                  :  :     +- 'UnresolvedRelation [store_sales], [], false
                  :  +- 'SubqueryAlias d
                  :     +- 'UnresolvedRelation [date_dim], [], false
                  +- 'SubqueryAlias i
                     +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
state: string, cnt: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cnt#1L ASC NULLS FIRST], true
      +- Filter (cnt#1L >= cast(10 as bigint))
         +- Aggregate [ca_state#19], [ca_state#19 AS state#0, count(1) AS cnt#1L]
            +- Filter ((((ca_address_sk#11 = c_current_addr_sk#28) AND (c_customer_sk#24 = ss_customer_sk#45)) AND (ss_sold_date_sk#42 = d_date_sk#65)) AND (((ss_item_sk#44 = i_item_sk#93) AND (d_month_seq#68 = scalar-subquery#2 [])) AND (i_current_price#98 > (cast(1.2 as double) * scalar-subquery#3 [i_category#105]))))
               :  :- Distinct
               :  :  +- Project [d_month_seq#125]
               :  :     +- Filter ((d_year#128 = 2001) AND (d_moy#130 = 1))
               :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#122,d_date_id#123,d_date#124,d_month_seq#125,d_week_seq#126,d_quarter_seq#127,d_year#128,d_dow#129,d_moy#130,d_dom#131,d_qoy#132,d_fy_year#133,d_fy_quarter_seq#134,d_fy_week_seq#135,d_day_name#136,d_quarter_name#137,d_holiday#138,d_weekend#139,d_following_holiday#140,d_first_dom#141,d_last_dom#142,d_same_day_ly#143,d_same_day_lq#144,d_current_day#145,... 4 more fields] parquet
               :  +- Aggregate [avg(i_current_price#155) AS avg(i_current_price)#118]
               :     +- Filter (i_category#162 = outer(i_category#105))
               :        +- SubqueryAlias j
               :           +- SubqueryAlias spark_catalog.tpcds.item
               :              +- Relation spark_catalog.tpcds.item[i_item_sk#150,i_item_id#151,i_rec_start_date#152,i_rec_end_date#153,i_item_desc#154,i_current_price#155,i_wholesale_cost#156,i_brand_id#157,i_brand#158,i_class_id#159,i_class#160,i_category_id#161,i_category#162,i_manufact_id#163,i_manufact#164,i_size#165,i_formulation#166,i_color#167,i_units#168,i_container#169,i_manager_id#170,i_product_name#171] parquet
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- SubqueryAlias a
                  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
                  :  :  :  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#11,ca_address_id#12,ca_street_number#13,ca_street_name#14,ca_street_type#15,ca_suite_number#16,ca_city#17,ca_county#18,ca_state#19,ca_zip#20,ca_country#21,ca_gmt_offset#22,ca_location_type#23] parquet
                  :  :  :  +- SubqueryAlias c
                  :  :  :     +- SubqueryAlias spark_catalog.tpcds.customer
                  :  :  :        +- Relation spark_catalog.tpcds.customer[c_customer_sk#24,c_customer_id#25,c_current_cdemo_sk#26,c_current_hdemo_sk#27,c_current_addr_sk#28,c_first_shipto_date_sk#29,c_first_sales_date_sk#30,c_salutation#31,c_first_name#32,c_last_name#33,c_preferred_cust_flag#34,c_birth_day#35,c_birth_month#36,c_birth_year#37,c_birth_country#38,c_login#39,c_email_address#40,c_last_review_date#41] parquet
                  :  :  +- SubqueryAlias s
                  :  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
                  :  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#42,ss_sold_time_sk#43,ss_item_sk#44,ss_customer_sk#45,ss_cdemo_sk#46,ss_hdemo_sk#47,ss_addr_sk#48,ss_store_sk#49,ss_promo_sk#50,ss_ticket_number#51,ss_quantity#52,ss_wholesale_cost#53,ss_list_price#54,ss_sales_price#55,ss_ext_discount_amt#56,ss_ext_sales_price#57,ss_ext_wholesale_cost#58,ss_ext_list_price#59,ss_ext_tax#60,ss_coupon_amt#61,ss_net_paid#62,ss_net_paid_inc_tax#63,ss_net_profit#64] parquet
                  :  +- SubqueryAlias d
                  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
                  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
                  +- SubqueryAlias i
                     +- SubqueryAlias spark_catalog.tpcds.item
                        +- Relation spark_catalog.tpcds.item[i_item_sk#93,i_item_id#94,i_rec_start_date#95,i_rec_end_date#96,i_item_desc#97,i_current_price#98,i_wholesale_cost#99,i_brand_id#100,i_brand#101,i_class_id#102,i_class#103,i_category_id#104,i_category#105,i_manufact_id#106,i_manufact#107,i_size#108,i_formulation#109,i_color#110,i_units#111,i_container#112,i_manager_id#113,i_product_name#114] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cnt#1L ASC NULLS FIRST], true
      +- Filter (cnt#1L >= 10)
         +- Aggregate [ca_state#19], [ca_state#19 AS state#0, count(1) AS cnt#1L]
            +- Project [ca_state#19]
               +- Join Inner, (ss_item_sk#44 = i_item_sk#93)
                  :- Project [ca_state#19, ss_item_sk#44]
                  :  +- Join Inner, (ss_sold_date_sk#42 = d_date_sk#65)
                  :     :- Project [ca_state#19, ss_sold_date_sk#42, ss_item_sk#44]
                  :     :  +- Join Inner, (c_customer_sk#24 = ss_customer_sk#45)
                  :     :     :- Project [ca_state#19, c_customer_sk#24]
                  :     :     :  +- Join Inner, (ca_address_sk#11 = c_current_addr_sk#28)
                  :     :     :     :- Project [ca_address_sk#11, ca_state#19]
                  :     :     :     :  +- Filter isnotnull(ca_address_sk#11)
                  :     :     :     :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#11,ca_address_id#12,ca_street_number#13,ca_street_name#14,ca_street_type#15,ca_suite_number#16,ca_city#17,ca_county#18,ca_state#19,ca_zip#20,ca_country#21,ca_gmt_offset#22,ca_location_type#23] parquet
                  :     :     :     +- Project [c_customer_sk#24, c_current_addr_sk#28]
                  :     :     :        +- Filter (isnotnull(c_current_addr_sk#28) AND isnotnull(c_customer_sk#24))
                  :     :     :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#24,c_customer_id#25,c_current_cdemo_sk#26,c_current_hdemo_sk#27,c_current_addr_sk#28,c_first_shipto_date_sk#29,c_first_sales_date_sk#30,c_salutation#31,c_first_name#32,c_last_name#33,c_preferred_cust_flag#34,c_birth_day#35,c_birth_month#36,c_birth_year#37,c_birth_country#38,c_login#39,c_email_address#40,c_last_review_date#41] parquet
                  :     :     +- Project [ss_sold_date_sk#42, ss_item_sk#44, ss_customer_sk#45]
                  :     :        +- Filter (isnotnull(ss_customer_sk#45) AND (isnotnull(ss_sold_date_sk#42) AND isnotnull(ss_item_sk#44)))
                  :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#42,ss_sold_time_sk#43,ss_item_sk#44,ss_customer_sk#45,ss_cdemo_sk#46,ss_hdemo_sk#47,ss_addr_sk#48,ss_store_sk#49,ss_promo_sk#50,ss_ticket_number#51,ss_quantity#52,ss_wholesale_cost#53,ss_list_price#54,ss_sales_price#55,ss_ext_discount_amt#56,ss_ext_sales_price#57,ss_ext_wholesale_cost#58,ss_ext_list_price#59,ss_ext_tax#60,ss_coupon_amt#61,ss_net_paid#62,ss_net_paid_inc_tax#63,ss_net_profit#64] parquet
                  :     +- Project [d_date_sk#65]
                  :        +- Filter ((isnotnull(d_month_seq#68) AND (d_month_seq#68 = scalar-subquery#2 [])) AND isnotnull(d_date_sk#65))
                  :           :  +- Aggregate [d_month_seq#125], [d_month_seq#125]
                  :           :     +- Project [d_month_seq#125]
                  :           :        +- Filter ((isnotnull(d_year#128) AND isnotnull(d_moy#130)) AND ((d_year#128 = 2001) AND (d_moy#130 = 1)))
                  :           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#122,d_date_id#123,d_date#124,d_month_seq#125,d_week_seq#126,d_quarter_seq#127,d_year#128,d_dow#129,d_moy#130,d_dom#131,d_qoy#132,d_fy_year#133,d_fy_quarter_seq#134,d_fy_week_seq#135,d_day_name#136,d_quarter_name#137,d_holiday#138,d_weekend#139,d_following_holiday#140,d_first_dom#141,d_last_dom#142,d_same_day_ly#143,d_same_day_lq#144,d_current_day#145,... 4 more fields] parquet
                  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
                  +- Project [i_item_sk#93]
                     +- Join Inner, ((i_current_price#98 > (1.2 * avg(i_current_price)#118)) AND (i_category#162 = i_category#105))
                        :- Project [i_item_sk#93, i_current_price#98, i_category#105]
                        :  +- Filter ((isnotnull(i_current_price#98) AND isnotnull(i_category#105)) AND isnotnull(i_item_sk#93))
                        :     +- Relation spark_catalog.tpcds.item[i_item_sk#93,i_item_id#94,i_rec_start_date#95,i_rec_end_date#96,i_item_desc#97,i_current_price#98,i_wholesale_cost#99,i_brand_id#100,i_brand#101,i_class_id#102,i_class#103,i_category_id#104,i_category#105,i_manufact_id#106,i_manufact#107,i_size#108,i_formulation#109,i_color#110,i_units#111,i_container#112,i_manager_id#113,i_product_name#114] parquet
                        +- Filter isnotnull(avg(i_current_price)#118)
                           +- Aggregate [i_category#162], [avg(i_current_price#155) AS avg(i_current_price)#118, i_category#162]
                              +- Project [i_current_price#155, i_category#162]
                                 +- Filter isnotnull(i_category#162)
                                    +- Relation spark_catalog.tpcds.item[i_item_sk#150,i_item_id#151,i_rec_start_date#152,i_rec_end_date#153,i_item_desc#154,i_current_price#155,i_wholesale_cost#156,i_brand_id#157,i_brand#158,i_class_id#159,i_class#160,i_category_id#161,i_category#162,i_manufact_id#163,i_manufact#164,i_size#165,i_formulation#166,i_color#167,i_units#168,i_container#169,i_manager_id#170,i_product_name#171] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[cnt#1L ASC NULLS FIRST], output=[state#0,cnt#1L])
   +- Filter (cnt#1L >= 10)
      +- HashAggregate(keys=[ca_state#19], functions=[count(1)], output=[state#0, cnt#1L])
         +- Exchange hashpartitioning(ca_state#19, 200), ENSURE_REQUIREMENTS, [plan_id=187]
            +- HashAggregate(keys=[ca_state#19], functions=[partial_count(1)], output=[ca_state#19, count#173L])
               +- Project [ca_state#19]
                  +- BroadcastHashJoin [ss_item_sk#44], [i_item_sk#93], Inner, BuildRight, false
                     :- Project [ca_state#19, ss_item_sk#44]
                     :  +- BroadcastHashJoin [ss_sold_date_sk#42], [d_date_sk#65], Inner, BuildRight, false
                     :     :- Project [ca_state#19, ss_sold_date_sk#42, ss_item_sk#44]
                     :     :  +- SortMergeJoin [c_customer_sk#24], [ss_customer_sk#45], Inner
                     :     :     :- Sort [c_customer_sk#24 ASC NULLS FIRST], false, 0
                     :     :     :  +- Exchange hashpartitioning(c_customer_sk#24, 200), ENSURE_REQUIREMENTS, [plan_id=164]
                     :     :     :     +- Project [ca_state#19, c_customer_sk#24]
                     :     :     :        +- BroadcastHashJoin [ca_address_sk#11], [c_current_addr_sk#28], Inner, BuildLeft, false
                     :     :     :           :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=159]
                     :     :     :           :  +- Filter isnotnull(ca_address_sk#11)
                     :     :     :           :     +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#11,ca_state#19] Batched: true, DataFilters: [isnotnull(ca_address_sk#11)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                     :     :     :           +- Filter (isnotnull(c_current_addr_sk#28) AND isnotnull(c_customer_sk#24))
                     :     :     :              +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#24,c_current_addr_sk#28] Batched: true, DataFilters: [isnotnull(c_current_addr_sk#28), isnotnull(c_customer_sk#24)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                     :     :     +- Sort [ss_customer_sk#45 ASC NULLS FIRST], false, 0
                     :     :        +- Exchange hashpartitioning(ss_customer_sk#45, 200), ENSURE_REQUIREMENTS, [plan_id=165]
                     :     :           +- Filter ((isnotnull(ss_customer_sk#45) AND isnotnull(ss_sold_date_sk#42)) AND isnotnull(ss_item_sk#44))
                     :     :              +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#42,ss_item_sk#44,ss_customer_sk#45] Batched: true, DataFilters: [isnotnull(ss_customer_sk#45), isnotnull(ss_sold_date_sk#42), isnotnull(ss_item_sk#44)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=171]
                     :        +- Project [d_date_sk#65]
                     :           +- Filter ((isnotnull(d_month_seq#68) AND (d_month_seq#68 = Subquery subquery#2, [id=#130])) AND isnotnull(d_date_sk#65))
                     :              :  +- Subquery subquery#2, [id=#130]
                     :              :     +- AdaptiveSparkPlan isFinalPlan=false
                     :              :        +- HashAggregate(keys=[d_month_seq#125], functions=[], output=[d_month_seq#125])
                     :              :           +- Exchange hashpartitioning(d_month_seq#125, 200), ENSURE_REQUIREMENTS, [plan_id=128]
                     :              :              +- HashAggregate(keys=[d_month_seq#125], functions=[], output=[d_month_seq#125])
                     :              :                 +- Project [d_month_seq#125]
                     :              :                    +- Filter (((isnotnull(d_year#128) AND isnotnull(d_moy#130)) AND (d_year#128 = 2001)) AND (d_moy#130 = 1))
                     :              :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_month_seq#125,d_year#128,d_moy#130] Batched: true, DataFilters: [isnotnull(d_year#128), isnotnull(d_moy#130), (d_year#128 = 2001), (d_moy#130 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,1)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#65,d_month_seq#68] Batched: true, DataFilters: [isnotnull(d_month_seq#68), isnotnull(d_date_sk#65)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=182]
                        +- Project [i_item_sk#93]
                           +- BroadcastHashJoin [i_category#105], [i_category#162], Inner, BuildRight, (i_current_price#98 > (1.2 * avg(i_current_price)#118)), false
                              :- Filter ((isnotnull(i_current_price#98) AND isnotnull(i_category#105)) AND isnotnull(i_item_sk#93))
                              :  +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#93,i_current_price#98,i_category#105] Batched: true, DataFilters: [isnotnull(i_current_price#98), isnotnull(i_category#105), isnotnull(i_item_sk#93)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), IsNotNull(i_category), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double,i_category:string>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(input[1, string, true]),false), [plan_id=178]
                                 +- Filter isnotnull(avg(i_current_price)#118)
                                    +- HashAggregate(keys=[i_category#162], functions=[avg(i_current_price#155)], output=[avg(i_current_price)#118, i_category#162])
                                       +- Exchange hashpartitioning(i_category#162, 200), ENSURE_REQUIREMENTS, [plan_id=174]
                                          +- HashAggregate(keys=[i_category#162], functions=[partial_avg(i_current_price#155)], output=[i_category#162, sum#176, count#177L])
                                             +- Filter isnotnull(i_category#162)
                                                +- FileScan parquet spark_catalog.tpcds.item[i_current_price#155,i_category#162] Batched: true, DataFilters: [isnotnull(i_category#162)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category)], ReadSchema: struct<i_current_price:double,i_category:string>

Time taken: 2.781 seconds, Fetched 1 row(s)
