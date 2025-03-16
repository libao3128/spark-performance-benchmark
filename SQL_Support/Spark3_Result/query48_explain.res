Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581410982
== Parsed Logical Plan ==
'Project [unresolvedalias('sum('ss_quantity), None)]
+- 'Filter (((('s_store_sk = 'ss_store_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND ('d_year = 2000)) AND (((((('cd_demo_sk = 'ss_cdemo_sk) AND ('cd_marital_status = M)) AND (('cd_education_status = 4 yr Degree) AND (('ss_sales_price >= 100.00) AND ('ss_sales_price <= 150.00)))) OR ((('cd_demo_sk = 'ss_cdemo_sk) AND ('cd_marital_status = D)) AND (('cd_education_status = 2 yr Degree) AND (('ss_sales_price >= 50.00) AND ('ss_sales_price <= 100.00))))) OR ((('cd_demo_sk = 'ss_cdemo_sk) AND ('cd_marital_status = S)) AND (('cd_education_status = College) AND (('ss_sales_price >= 150.00) AND ('ss_sales_price <= 200.00))))) AND ((((('ss_addr_sk = 'ca_address_sk) AND ('ca_country = United States)) AND ('ca_state IN (CO,OH,TX) AND (('ss_net_profit >= 0) AND ('ss_net_profit <= 2000)))) OR ((('ss_addr_sk = 'ca_address_sk) AND ('ca_country = United States)) AND ('ca_state IN (OR,MN,KY) AND (('ss_net_profit >= 150) AND ('ss_net_profit <= 3000))))) OR ((('ss_addr_sk = 'ca_address_sk) AND ('ca_country = United States)) AND ('ca_state IN (VA,CA,MS) AND (('ss_net_profit >= 50) AND ('ss_net_profit <= 25000)))))))
   +- 'Join Inner
      :- 'Join Inner
      :  :- 'Join Inner
      :  :  :- 'Join Inner
      :  :  :  :- 'UnresolvedRelation [store_sales], [], false
      :  :  :  +- 'UnresolvedRelation [store], [], false
      :  :  +- 'UnresolvedRelation [customer_demographics], [], false
      :  +- 'UnresolvedRelation [customer_address], [], false
      +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
sum(ss_quantity): bigint
Aggregate [sum(ss_quantity#15) AS sum(ss_quantity)#113L]
+- Filter ((((s_store_sk#28 = ss_store_sk#12) AND (ss_sold_date_sk#5 = d_date_sk#79)) AND (d_year#85 = 2000)) AND ((((((cd_demo_sk#57 = ss_cdemo_sk#9) AND (cd_marital_status#59 = M)) AND ((cd_education_status#60 = 4 yr Degree) AND ((ss_sales_price#18 >= cast(100.00 as double)) AND (ss_sales_price#18 <= cast(150.00 as double))))) OR (((cd_demo_sk#57 = ss_cdemo_sk#9) AND (cd_marital_status#59 = D)) AND ((cd_education_status#60 = 2 yr Degree) AND ((ss_sales_price#18 >= cast(50.00 as double)) AND (ss_sales_price#18 <= cast(100.00 as double)))))) OR (((cd_demo_sk#57 = ss_cdemo_sk#9) AND (cd_marital_status#59 = S)) AND ((cd_education_status#60 = College) AND ((ss_sales_price#18 >= cast(150.00 as double)) AND (ss_sales_price#18 <= cast(200.00 as double)))))) AND (((((ss_addr_sk#11 = ca_address_sk#66) AND (ca_country#76 = United States)) AND (ca_state#74 IN (CO,OH,TX) AND ((ss_net_profit#27 >= cast(0 as double)) AND (ss_net_profit#27 <= cast(2000 as double))))) OR (((ss_addr_sk#11 = ca_address_sk#66) AND (ca_country#76 = United States)) AND (ca_state#74 IN (OR,MN,KY) AND ((ss_net_profit#27 >= cast(150 as double)) AND (ss_net_profit#27 <= cast(3000 as double)))))) OR (((ss_addr_sk#11 = ca_address_sk#66) AND (ca_country#76 = United States)) AND (ca_state#74 IN (VA,CA,MS) AND ((ss_net_profit#27 >= cast(50 as double)) AND (ss_net_profit#27 <= cast(25000 as double))))))))
   +- Join Inner
      :- Join Inner
      :  :- Join Inner
      :  :  :- Join Inner
      :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
      :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
      :  :  :  +- SubqueryAlias spark_catalog.tpcds.store
      :  :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#28,s_store_id#29,s_rec_start_date#30,s_rec_end_date#31,s_closed_date_sk#32,s_store_name#33,s_number_employees#34,s_floor_space#35,s_hours#36,s_manager#37,s_market_id#38,s_geography_class#39,s_market_desc#40,s_market_manager#41,s_division_id#42,s_division_name#43,s_company_id#44,s_company_name#45,s_street_number#46,s_street_name#47,s_street_type#48,s_suite_number#49,s_city#50,s_county#51,... 5 more fields] parquet
      :  :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
      :  :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#57,cd_gender#58,cd_marital_status#59,cd_education_status#60,cd_purchase_estimate#61,cd_credit_rating#62,cd_dep_count#63,cd_dep_employed_count#64,cd_dep_college_count#65] parquet
      :  +- SubqueryAlias spark_catalog.tpcds.customer_address
      :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#66,ca_address_id#67,ca_street_number#68,ca_street_name#69,ca_street_type#70,ca_suite_number#71,ca_city#72,ca_county#73,ca_state#74,ca_zip#75,ca_country#76,ca_gmt_offset#77,ca_location_type#78] parquet
      +- SubqueryAlias spark_catalog.tpcds.date_dim
         +- Relation spark_catalog.tpcds.date_dim[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet

== Optimized Logical Plan ==
Aggregate [sum(ss_quantity#15) AS sum(ss_quantity)#113L]
+- Project [ss_quantity#15]
   +- Join Inner, (ss_sold_date_sk#5 = d_date_sk#79)
      :- Project [ss_sold_date_sk#5, ss_quantity#15]
      :  +- Join Inner, ((ss_addr_sk#11 = ca_address_sk#66) AND ((((ca_state#74 IN (CO,OH,TX) AND (ss_net_profit#27 >= 0.0)) AND (ss_net_profit#27 <= 2000.0)) OR ((ca_state#74 IN (OR,MN,KY) AND (ss_net_profit#27 >= 150.0)) AND (ss_net_profit#27 <= 3000.0))) OR ((ca_state#74 IN (VA,CA,MS) AND (ss_net_profit#27 >= 50.0)) AND (ss_net_profit#27 <= 25000.0))))
      :     :- Project [ss_sold_date_sk#5, ss_addr_sk#11, ss_quantity#15, ss_net_profit#27]
      :     :  +- Join Inner, ((cd_demo_sk#57 = ss_cdemo_sk#9) AND ((((((cd_marital_status#59 = M) AND (cd_education_status#60 = 4 yr Degree)) AND (ss_sales_price#18 >= 100.0)) AND (ss_sales_price#18 <= 150.0)) OR ((((cd_marital_status#59 = D) AND (cd_education_status#60 = 2 yr Degree)) AND (ss_sales_price#18 >= 50.0)) AND (ss_sales_price#18 <= 100.0))) OR ((((cd_marital_status#59 = S) AND (cd_education_status#60 = College)) AND (ss_sales_price#18 >= 150.0)) AND (ss_sales_price#18 <= 200.0))))
      :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_addr_sk#11, ss_quantity#15, ss_sales_price#18, ss_net_profit#27]
      :     :     :  +- Join Inner, (s_store_sk#28 = ss_store_sk#12)
      :     :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_addr_sk#11, ss_store_sk#12, ss_quantity#15, ss_sales_price#18, ss_net_profit#27]
      :     :     :     :  +- Filter (((isnotnull(ss_store_sk#12) AND isnotnull(ss_cdemo_sk#9)) AND (isnotnull(ss_addr_sk#11) AND isnotnull(ss_sold_date_sk#5))) AND (((((ss_sales_price#18 >= 100.0) AND (ss_sales_price#18 <= 150.0)) OR ((ss_sales_price#18 >= 50.0) AND (ss_sales_price#18 <= 100.0))) OR ((ss_sales_price#18 >= 150.0) AND (ss_sales_price#18 <= 200.0))) AND ((((ss_net_profit#27 >= 0.0) AND (ss_net_profit#27 <= 2000.0)) OR ((ss_net_profit#27 >= 150.0) AND (ss_net_profit#27 <= 3000.0))) OR ((ss_net_profit#27 >= 50.0) AND (ss_net_profit#27 <= 25000.0)))))
      :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
      :     :     :     +- Project [s_store_sk#28]
      :     :     :        +- Filter isnotnull(s_store_sk#28)
      :     :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#28,s_store_id#29,s_rec_start_date#30,s_rec_end_date#31,s_closed_date_sk#32,s_store_name#33,s_number_employees#34,s_floor_space#35,s_hours#36,s_manager#37,s_market_id#38,s_geography_class#39,s_market_desc#40,s_market_manager#41,s_division_id#42,s_division_name#43,s_company_id#44,s_company_name#45,s_street_number#46,s_street_name#47,s_street_type#48,s_suite_number#49,s_city#50,s_county#51,... 5 more fields] parquet
      :     :     +- Project [cd_demo_sk#57, cd_marital_status#59, cd_education_status#60]
      :     :        +- Filter (isnotnull(cd_demo_sk#57) AND ((((cd_marital_status#59 = M) AND (cd_education_status#60 = 4 yr Degree)) OR ((cd_marital_status#59 = D) AND (cd_education_status#60 = 2 yr Degree))) OR ((cd_marital_status#59 = S) AND (cd_education_status#60 = College))))
      :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#57,cd_gender#58,cd_marital_status#59,cd_education_status#60,cd_purchase_estimate#61,cd_credit_rating#62,cd_dep_count#63,cd_dep_employed_count#64,cd_dep_college_count#65] parquet
      :     +- Project [ca_address_sk#66, ca_state#74]
      :        +- Filter (((isnotnull(ca_country#76) AND (ca_country#76 = United States)) AND isnotnull(ca_address_sk#66)) AND ((ca_state#74 IN (CO,OH,TX) OR ca_state#74 IN (OR,MN,KY)) OR ca_state#74 IN (VA,CA,MS)))
      :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#66,ca_address_id#67,ca_street_number#68,ca_street_name#69,ca_street_type#70,ca_suite_number#71,ca_city#72,ca_county#73,ca_state#74,ca_zip#75,ca_country#76,ca_gmt_offset#77,ca_location_type#78] parquet
      +- Project [d_date_sk#79]
         +- Filter ((isnotnull(d_year#85) AND (d_year#85 = 2000)) AND isnotnull(d_date_sk#79))
            +- Relation spark_catalog.tpcds.date_dim[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(ss_quantity#15)], output=[sum(ss_quantity)#113L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=112]
      +- HashAggregate(keys=[], functions=[partial_sum(ss_quantity#15)], output=[sum#115L])
         +- Project [ss_quantity#15]
            +- BroadcastHashJoin [ss_sold_date_sk#5], [d_date_sk#79], Inner, BuildRight, false
               :- Project [ss_sold_date_sk#5, ss_quantity#15]
               :  +- BroadcastHashJoin [ss_addr_sk#11], [ca_address_sk#66], Inner, BuildRight, ((((ca_state#74 IN (CO,OH,TX) AND (ss_net_profit#27 >= 0.0)) AND (ss_net_profit#27 <= 2000.0)) OR ((ca_state#74 IN (OR,MN,KY) AND (ss_net_profit#27 >= 150.0)) AND (ss_net_profit#27 <= 3000.0))) OR ((ca_state#74 IN (VA,CA,MS) AND (ss_net_profit#27 >= 50.0)) AND (ss_net_profit#27 <= 25000.0))), false
               :     :- Project [ss_sold_date_sk#5, ss_addr_sk#11, ss_quantity#15, ss_net_profit#27]
               :     :  +- BroadcastHashJoin [ss_cdemo_sk#9], [cd_demo_sk#57], Inner, BuildRight, ((((((cd_marital_status#59 = M) AND (cd_education_status#60 = 4 yr Degree)) AND (ss_sales_price#18 >= 100.0)) AND (ss_sales_price#18 <= 150.0)) OR ((((cd_marital_status#59 = D) AND (cd_education_status#60 = 2 yr Degree)) AND (ss_sales_price#18 >= 50.0)) AND (ss_sales_price#18 <= 100.0))) OR ((((cd_marital_status#59 = S) AND (cd_education_status#60 = College)) AND (ss_sales_price#18 >= 150.0)) AND (ss_sales_price#18 <= 200.0))), false
               :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_addr_sk#11, ss_quantity#15, ss_sales_price#18, ss_net_profit#27]
               :     :     :  +- BroadcastHashJoin [ss_store_sk#12], [s_store_sk#28], Inner, BuildRight, false
               :     :     :     :- Filter (((((isnotnull(ss_store_sk#12) AND isnotnull(ss_cdemo_sk#9)) AND isnotnull(ss_addr_sk#11)) AND isnotnull(ss_sold_date_sk#5)) AND ((((ss_sales_price#18 >= 100.0) AND (ss_sales_price#18 <= 150.0)) OR ((ss_sales_price#18 >= 50.0) AND (ss_sales_price#18 <= 100.0))) OR ((ss_sales_price#18 >= 150.0) AND (ss_sales_price#18 <= 200.0)))) AND ((((ss_net_profit#27 >= 0.0) AND (ss_net_profit#27 <= 2000.0)) OR ((ss_net_profit#27 >= 150.0) AND (ss_net_profit#27 <= 3000.0))) OR ((ss_net_profit#27 >= 50.0) AND (ss_net_profit#27 <= 25000.0))))
               :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#5,ss_cdemo_sk#9,ss_addr_sk#11,ss_store_sk#12,ss_quantity#15,ss_sales_price#18,ss_net_profit#27] Batched: true, DataFilters: [isnotnull(ss_store_sk#12), isnotnull(ss_cdemo_sk#9), isnotnull(ss_addr_sk#11), isnotnull(ss_sold..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_cdemo_sk), IsNotNull(ss_addr_sk), IsNotNull(ss_sold_date_sk..., ReadSchema: struct<ss_sold_date_sk:int,ss_cdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_quantity:int,ss_sale...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=95]
               :     :     :        +- Filter isnotnull(s_store_sk#28)
               :     :     :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#28] Batched: true, DataFilters: [isnotnull(s_store_sk#28)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=99]
               :     :        +- Filter (isnotnull(cd_demo_sk#57) AND ((((cd_marital_status#59 = M) AND (cd_education_status#60 = 4 yr Degree)) OR ((cd_marital_status#59 = D) AND (cd_education_status#60 = 2 yr Degree))) OR ((cd_marital_status#59 = S) AND (cd_education_status#60 = College))))
               :     :           +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#57,cd_marital_status#59,cd_education_status#60] Batched: true, DataFilters: [isnotnull(cd_demo_sk#57), ((((cd_marital_status#59 = M) AND (cd_education_status#60 = 4 yr Degre..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk), Or(Or(And(EqualTo(cd_marital_status,M),EqualTo(cd_education_status,4 yr D..., ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=103]
               :        +- Project [ca_address_sk#66, ca_state#74]
               :           +- Filter (((isnotnull(ca_country#76) AND (ca_country#76 = United States)) AND isnotnull(ca_address_sk#66)) AND ((ca_state#74 IN (CO,OH,TX) OR ca_state#74 IN (OR,MN,KY)) OR ca_state#74 IN (VA,CA,MS)))
               :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#66,ca_state#74,ca_country#76] Batched: true, DataFilters: [isnotnull(ca_country#76), (ca_country#76 = United States), isnotnull(ca_address_sk#66), ((ca_sta..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_country), EqualTo(ca_country,United States), IsNotNull(ca_address_sk), Or(Or(In(ca_..., ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_country:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=107]
                  +- Project [d_date_sk#79]
                     +- Filter ((isnotnull(d_year#85) AND (d_year#85 = 2000)) AND isnotnull(d_date_sk#79))
                        +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#79,d_year#85] Batched: true, DataFilters: [isnotnull(d_year#85), (d_year#85 = 2000), isnotnull(d_date_sk#79)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>

Time taken: 2.628 seconds, Fetched 1 row(s)
