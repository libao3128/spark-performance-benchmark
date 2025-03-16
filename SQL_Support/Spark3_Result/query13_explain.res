Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579943445
== Parsed Logical Plan ==
'Project [unresolvedalias('avg('ss_quantity), None), unresolvedalias('avg('ss_ext_sales_price), None), unresolvedalias('avg('ss_ext_wholesale_cost), None), unresolvedalias('sum('ss_ext_wholesale_cost), None)]
+- 'Filter (((('s_store_sk = 'ss_store_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND ('d_year = 2001)) AND ((((((('ss_hdemo_sk = 'hd_demo_sk) AND ('cd_demo_sk = 'ss_cdemo_sk)) AND ('cd_marital_status = M)) AND ((('cd_education_status = Advanced Degree) AND (('ss_sales_price >= 100.00) AND ('ss_sales_price <= 150.00))) AND ('hd_dep_count = 3))) OR (((('ss_hdemo_sk = 'hd_demo_sk) AND ('cd_demo_sk = 'ss_cdemo_sk)) AND ('cd_marital_status = S)) AND ((('cd_education_status = College) AND (('ss_sales_price >= 50.00) AND ('ss_sales_price <= 100.00))) AND ('hd_dep_count = 1)))) OR (((('ss_hdemo_sk = 'hd_demo_sk) AND ('cd_demo_sk = 'ss_cdemo_sk)) AND ('cd_marital_status = W)) AND ((('cd_education_status = 2 yr Degree) AND (('ss_sales_price >= 150.00) AND ('ss_sales_price <= 200.00))) AND ('hd_dep_count = 1)))) AND ((((('ss_addr_sk = 'ca_address_sk) AND ('ca_country = United States)) AND ('ca_state IN (TX,OH,TX) AND (('ss_net_profit >= 100) AND ('ss_net_profit <= 200)))) OR ((('ss_addr_sk = 'ca_address_sk) AND ('ca_country = United States)) AND ('ca_state IN (OR,NM,KY) AND (('ss_net_profit >= 150) AND ('ss_net_profit <= 300))))) OR ((('ss_addr_sk = 'ca_address_sk) AND ('ca_country = United States)) AND ('ca_state IN (VA,TX,MS) AND (('ss_net_profit >= 50) AND ('ss_net_profit <= 250)))))))
   +- 'Join Inner
      :- 'Join Inner
      :  :- 'Join Inner
      :  :  :- 'Join Inner
      :  :  :  :- 'Join Inner
      :  :  :  :  :- 'UnresolvedRelation [store_sales], [], false
      :  :  :  :  +- 'UnresolvedRelation [store], [], false
      :  :  :  +- 'UnresolvedRelation [customer_demographics], [], false
      :  :  +- 'UnresolvedRelation [household_demographics], [], false
      :  +- 'UnresolvedRelation [customer_address], [], false
      +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
avg(ss_quantity): double, avg(ss_ext_sales_price): double, avg(ss_ext_wholesale_cost): double, sum(ss_ext_wholesale_cost): double
Aggregate [avg(ss_quantity#15) AS avg(ss_quantity)#122, avg(ss_ext_sales_price#20) AS avg(ss_ext_sales_price)#123, avg(ss_ext_wholesale_cost#21) AS avg(ss_ext_wholesale_cost)#124, sum(ss_ext_wholesale_cost#21) AS sum(ss_ext_wholesale_cost)#125]
+- Filter ((((s_store_sk#28 = ss_store_sk#12) AND (ss_sold_date_sk#5 = d_date_sk#84)) AND (d_year#90 = 2001)) AND (((((((ss_hdemo_sk#10 = hd_demo_sk#66) AND (cd_demo_sk#57 = ss_cdemo_sk#9)) AND (cd_marital_status#59 = M)) AND (((cd_education_status#60 = Advanced Degree) AND ((ss_sales_price#18 >= cast(100.00 as double)) AND (ss_sales_price#18 <= cast(150.00 as double)))) AND (hd_dep_count#69 = 3))) OR ((((ss_hdemo_sk#10 = hd_demo_sk#66) AND (cd_demo_sk#57 = ss_cdemo_sk#9)) AND (cd_marital_status#59 = S)) AND (((cd_education_status#60 = College) AND ((ss_sales_price#18 >= cast(50.00 as double)) AND (ss_sales_price#18 <= cast(100.00 as double)))) AND (hd_dep_count#69 = 1)))) OR ((((ss_hdemo_sk#10 = hd_demo_sk#66) AND (cd_demo_sk#57 = ss_cdemo_sk#9)) AND (cd_marital_status#59 = W)) AND (((cd_education_status#60 = 2 yr Degree) AND ((ss_sales_price#18 >= cast(150.00 as double)) AND (ss_sales_price#18 <= cast(200.00 as double)))) AND (hd_dep_count#69 = 1)))) AND (((((ss_addr_sk#11 = ca_address_sk#71) AND (ca_country#81 = United States)) AND (ca_state#79 IN (TX,OH,TX) AND ((ss_net_profit#27 >= cast(100 as double)) AND (ss_net_profit#27 <= cast(200 as double))))) OR (((ss_addr_sk#11 = ca_address_sk#71) AND (ca_country#81 = United States)) AND (ca_state#79 IN (OR,NM,KY) AND ((ss_net_profit#27 >= cast(150 as double)) AND (ss_net_profit#27 <= cast(300 as double)))))) OR (((ss_addr_sk#11 = ca_address_sk#71) AND (ca_country#81 = United States)) AND (ca_state#79 IN (VA,TX,MS) AND ((ss_net_profit#27 >= cast(50 as double)) AND (ss_net_profit#27 <= cast(250 as double))))))))
   +- Join Inner
      :- Join Inner
      :  :- Join Inner
      :  :  :- Join Inner
      :  :  :  :- Join Inner
      :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
      :  :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
      :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store
      :  :  :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#28,s_store_id#29,s_rec_start_date#30,s_rec_end_date#31,s_closed_date_sk#32,s_store_name#33,s_number_employees#34,s_floor_space#35,s_hours#36,s_manager#37,s_market_id#38,s_geography_class#39,s_market_desc#40,s_market_manager#41,s_division_id#42,s_division_name#43,s_company_id#44,s_company_name#45,s_street_number#46,s_street_name#47,s_street_type#48,s_suite_number#49,s_city#50,s_county#51,... 5 more fields] parquet
      :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
      :  :  :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#57,cd_gender#58,cd_marital_status#59,cd_education_status#60,cd_purchase_estimate#61,cd_credit_rating#62,cd_dep_count#63,cd_dep_employed_count#64,cd_dep_college_count#65] parquet
      :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
      :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#66,hd_income_band_sk#67,hd_buy_potential#68,hd_dep_count#69,hd_vehicle_count#70] parquet
      :  +- SubqueryAlias spark_catalog.tpcds.customer_address
      :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#71,ca_address_id#72,ca_street_number#73,ca_street_name#74,ca_street_type#75,ca_suite_number#76,ca_city#77,ca_county#78,ca_state#79,ca_zip#80,ca_country#81,ca_gmt_offset#82,ca_location_type#83] parquet
      +- SubqueryAlias spark_catalog.tpcds.date_dim
         +- Relation spark_catalog.tpcds.date_dim[d_date_sk#84,d_date_id#85,d_date#86,d_month_seq#87,d_week_seq#88,d_quarter_seq#89,d_year#90,d_dow#91,d_moy#92,d_dom#93,d_qoy#94,d_fy_year#95,d_fy_quarter_seq#96,d_fy_week_seq#97,d_day_name#98,d_quarter_name#99,d_holiday#100,d_weekend#101,d_following_holiday#102,d_first_dom#103,d_last_dom#104,d_same_day_ly#105,d_same_day_lq#106,d_current_day#107,... 4 more fields] parquet

== Optimized Logical Plan ==
Aggregate [avg(ss_quantity#15) AS avg(ss_quantity)#122, avg(ss_ext_sales_price#20) AS avg(ss_ext_sales_price)#123, avg(ss_ext_wholesale_cost#21) AS avg(ss_ext_wholesale_cost)#124, sum(ss_ext_wholesale_cost#21) AS sum(ss_ext_wholesale_cost)#125]
+- Project [ss_quantity#15, ss_ext_sales_price#20, ss_ext_wholesale_cost#21]
   +- Join Inner, ((ss_hdemo_sk#10 = hd_demo_sk#66) AND (((((((cd_marital_status#59 = M) AND (cd_education_status#60 = Advanced Degree)) AND (ss_sales_price#18 >= 100.0)) AND (ss_sales_price#18 <= 150.0)) AND (hd_dep_count#69 = 3)) OR (((((cd_marital_status#59 = S) AND (cd_education_status#60 = College)) AND (ss_sales_price#18 >= 50.0)) AND (ss_sales_price#18 <= 100.0)) AND (hd_dep_count#69 = 1))) OR (((((cd_marital_status#59 = W) AND (cd_education_status#60 = 2 yr Degree)) AND (ss_sales_price#18 >= 150.0)) AND (ss_sales_price#18 <= 200.0)) AND (hd_dep_count#69 = 1))))
      :- Project [ss_hdemo_sk#10, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21, cd_marital_status#59, cd_education_status#60]
      :  +- Join Inner, (((((((cd_marital_status#59 = M) AND (cd_education_status#60 = Advanced Degree)) AND (ss_sales_price#18 >= 100.0)) AND (ss_sales_price#18 <= 150.0)) OR ((((cd_marital_status#59 = S) AND (cd_education_status#60 = College)) AND (ss_sales_price#18 >= 50.0)) AND (ss_sales_price#18 <= 100.0))) OR ((((cd_marital_status#59 = W) AND (cd_education_status#60 = 2 yr Degree)) AND (ss_sales_price#18 >= 150.0)) AND (ss_sales_price#18 <= 200.0))) AND (cd_demo_sk#57 = ss_cdemo_sk#9))
      :     :- Project [ss_cdemo_sk#9, ss_hdemo_sk#10, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21]
      :     :  +- Join Inner, (ss_sold_date_sk#5 = d_date_sk#84)
      :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_hdemo_sk#10, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21]
      :     :     :  +- Join Inner, ((ss_addr_sk#11 = ca_address_sk#71) AND ((((ca_state#79 IN (TX,OH) AND (ss_net_profit#27 >= 100.0)) AND (ss_net_profit#27 <= 200.0)) OR ((ca_state#79 IN (OR,NM,KY) AND (ss_net_profit#27 >= 150.0)) AND (ss_net_profit#27 <= 300.0))) OR ((ca_state#79 IN (VA,TX,MS) AND (ss_net_profit#27 >= 50.0)) AND (ss_net_profit#27 <= 250.0))))
      :     :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_hdemo_sk#10, ss_addr_sk#11, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21, ss_net_profit#27]
      :     :     :     :  +- Join Inner, (s_store_sk#28 = ss_store_sk#12)
      :     :     :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_hdemo_sk#10, ss_addr_sk#11, ss_store_sk#12, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21, ss_net_profit#27]
      :     :     :     :     :  +- Filter (((isnotnull(ss_store_sk#12) AND (((isnotnull(ss_addr_sk#11) AND isnotnull(ss_sold_date_sk#5)) AND isnotnull(ss_cdemo_sk#9)) AND isnotnull(ss_hdemo_sk#10))) AND ((((ss_net_profit#27 >= 100.0) AND (ss_net_profit#27 <= 200.0)) OR ((ss_net_profit#27 >= 150.0) AND (ss_net_profit#27 <= 300.0))) OR ((ss_net_profit#27 >= 50.0) AND (ss_net_profit#27 <= 250.0)))) AND ((((ss_sales_price#18 >= 100.0) AND (ss_sales_price#18 <= 150.0)) OR ((ss_sales_price#18 >= 50.0) AND (ss_sales_price#18 <= 100.0))) OR ((ss_sales_price#18 >= 150.0) AND (ss_sales_price#18 <= 200.0))))
      :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
      :     :     :     :     +- Project [s_store_sk#28]
      :     :     :     :        +- Filter isnotnull(s_store_sk#28)
      :     :     :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#28,s_store_id#29,s_rec_start_date#30,s_rec_end_date#31,s_closed_date_sk#32,s_store_name#33,s_number_employees#34,s_floor_space#35,s_hours#36,s_manager#37,s_market_id#38,s_geography_class#39,s_market_desc#40,s_market_manager#41,s_division_id#42,s_division_name#43,s_company_id#44,s_company_name#45,s_street_number#46,s_street_name#47,s_street_type#48,s_suite_number#49,s_city#50,s_county#51,... 5 more fields] parquet
      :     :     :     +- Project [ca_address_sk#71, ca_state#79]
      :     :     :        +- Filter (((isnotnull(ca_country#81) AND (ca_country#81 = United States)) AND isnotnull(ca_address_sk#71)) AND ((ca_state#79 IN (TX,OH) OR ca_state#79 IN (OR,NM,KY)) OR ca_state#79 IN (VA,TX,MS)))
      :     :     :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#71,ca_address_id#72,ca_street_number#73,ca_street_name#74,ca_street_type#75,ca_suite_number#76,ca_city#77,ca_county#78,ca_state#79,ca_zip#80,ca_country#81,ca_gmt_offset#82,ca_location_type#83] parquet
      :     :     +- Project [d_date_sk#84]
      :     :        +- Filter ((isnotnull(d_year#90) AND (d_year#90 = 2001)) AND isnotnull(d_date_sk#84))
      :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#84,d_date_id#85,d_date#86,d_month_seq#87,d_week_seq#88,d_quarter_seq#89,d_year#90,d_dow#91,d_moy#92,d_dom#93,d_qoy#94,d_fy_year#95,d_fy_quarter_seq#96,d_fy_week_seq#97,d_day_name#98,d_quarter_name#99,d_holiday#100,d_weekend#101,d_following_holiday#102,d_first_dom#103,d_last_dom#104,d_same_day_ly#105,d_same_day_lq#106,d_current_day#107,... 4 more fields] parquet
      :     +- Project [cd_demo_sk#57, cd_marital_status#59, cd_education_status#60]
      :        +- Filter (isnotnull(cd_demo_sk#57) AND ((((cd_marital_status#59 = M) AND (cd_education_status#60 = Advanced Degree)) OR ((cd_marital_status#59 = S) AND (cd_education_status#60 = College))) OR ((cd_marital_status#59 = W) AND (cd_education_status#60 = 2 yr Degree))))
      :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#57,cd_gender#58,cd_marital_status#59,cd_education_status#60,cd_purchase_estimate#61,cd_credit_rating#62,cd_dep_count#63,cd_dep_employed_count#64,cd_dep_college_count#65] parquet
      +- Project [hd_demo_sk#66, hd_dep_count#69]
         +- Filter (isnotnull(hd_demo_sk#66) AND ((hd_dep_count#69 = 3) OR (hd_dep_count#69 = 1)))
            +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#66,hd_income_band_sk#67,hd_buy_potential#68,hd_dep_count#69,hd_vehicle_count#70] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[avg(ss_quantity#15), avg(ss_ext_sales_price#20), avg(ss_ext_wholesale_cost#21), sum(ss_ext_wholesale_cost#21)], output=[avg(ss_quantity)#122, avg(ss_ext_sales_price)#123, avg(ss_ext_wholesale_cost)#124, sum(ss_ext_wholesale_cost)#125])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=134]
      +- HashAggregate(keys=[], functions=[partial_avg(ss_quantity#15), partial_avg(ss_ext_sales_price#20), partial_avg(ss_ext_wholesale_cost#21), partial_sum(ss_ext_wholesale_cost#21)], output=[sum#133, count#134L, sum#135, count#136L, sum#137, count#138L, sum#139])
         +- Project [ss_quantity#15, ss_ext_sales_price#20, ss_ext_wholesale_cost#21]
            +- BroadcastHashJoin [ss_hdemo_sk#10], [hd_demo_sk#66], Inner, BuildRight, (((((((cd_marital_status#59 = M) AND (cd_education_status#60 = Advanced Degree)) AND (ss_sales_price#18 >= 100.0)) AND (ss_sales_price#18 <= 150.0)) AND (hd_dep_count#69 = 3)) OR (((((cd_marital_status#59 = S) AND (cd_education_status#60 = College)) AND (ss_sales_price#18 >= 50.0)) AND (ss_sales_price#18 <= 100.0)) AND (hd_dep_count#69 = 1))) OR (((((cd_marital_status#59 = W) AND (cd_education_status#60 = 2 yr Degree)) AND (ss_sales_price#18 >= 150.0)) AND (ss_sales_price#18 <= 200.0)) AND (hd_dep_count#69 = 1))), false
               :- Project [ss_hdemo_sk#10, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21, cd_marital_status#59, cd_education_status#60]
               :  +- BroadcastHashJoin [ss_cdemo_sk#9], [cd_demo_sk#57], Inner, BuildRight, ((((((cd_marital_status#59 = M) AND (cd_education_status#60 = Advanced Degree)) AND (ss_sales_price#18 >= 100.0)) AND (ss_sales_price#18 <= 150.0)) OR ((((cd_marital_status#59 = S) AND (cd_education_status#60 = College)) AND (ss_sales_price#18 >= 50.0)) AND (ss_sales_price#18 <= 100.0))) OR ((((cd_marital_status#59 = W) AND (cd_education_status#60 = 2 yr Degree)) AND (ss_sales_price#18 >= 150.0)) AND (ss_sales_price#18 <= 200.0))), false
               :     :- Project [ss_cdemo_sk#9, ss_hdemo_sk#10, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21]
               :     :  +- BroadcastHashJoin [ss_sold_date_sk#5], [d_date_sk#84], Inner, BuildRight, false
               :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_hdemo_sk#10, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21]
               :     :     :  +- BroadcastHashJoin [ss_addr_sk#11], [ca_address_sk#71], Inner, BuildRight, ((((ca_state#79 IN (TX,OH) AND (ss_net_profit#27 >= 100.0)) AND (ss_net_profit#27 <= 200.0)) OR ((ca_state#79 IN (OR,NM,KY) AND (ss_net_profit#27 >= 150.0)) AND (ss_net_profit#27 <= 300.0))) OR ((ca_state#79 IN (VA,TX,MS) AND (ss_net_profit#27 >= 50.0)) AND (ss_net_profit#27 <= 250.0))), false
               :     :     :     :- Project [ss_sold_date_sk#5, ss_cdemo_sk#9, ss_hdemo_sk#10, ss_addr_sk#11, ss_quantity#15, ss_sales_price#18, ss_ext_sales_price#20, ss_ext_wholesale_cost#21, ss_net_profit#27]
               :     :     :     :  +- BroadcastHashJoin [ss_store_sk#12], [s_store_sk#28], Inner, BuildRight, false
               :     :     :     :     :- Filter ((((((isnotnull(ss_store_sk#12) AND isnotnull(ss_addr_sk#11)) AND isnotnull(ss_sold_date_sk#5)) AND isnotnull(ss_cdemo_sk#9)) AND isnotnull(ss_hdemo_sk#10)) AND ((((ss_net_profit#27 >= 100.0) AND (ss_net_profit#27 <= 200.0)) OR ((ss_net_profit#27 >= 150.0) AND (ss_net_profit#27 <= 300.0))) OR ((ss_net_profit#27 >= 50.0) AND (ss_net_profit#27 <= 250.0)))) AND ((((ss_sales_price#18 >= 100.0) AND (ss_sales_price#18 <= 150.0)) OR ((ss_sales_price#18 >= 50.0) AND (ss_sales_price#18 <= 100.0))) OR ((ss_sales_price#18 >= 150.0) AND (ss_sales_price#18 <= 200.0))))
               :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#5,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_quantity#15,ss_sales_price#18,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_net_profit#27] Batched: true, DataFilters: [isnotnull(ss_store_sk#12), isnotnull(ss_addr_sk#11), isnotnull(ss_sold_date_sk#5), isnotnull(ss_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_addr_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_cdemo_sk..., ReadSchema: struct<ss_sold_date_sk:int,ss_cdemo_sk:int,ss_hdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_quan...
               :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=113]
               :     :     :     :        +- Filter isnotnull(s_store_sk#28)
               :     :     :     :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#28] Batched: true, DataFilters: [isnotnull(s_store_sk#28)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=117]
               :     :     :        +- Project [ca_address_sk#71, ca_state#79]
               :     :     :           +- Filter (((isnotnull(ca_country#81) AND (ca_country#81 = United States)) AND isnotnull(ca_address_sk#71)) AND ((ca_state#79 IN (TX,OH) OR ca_state#79 IN (OR,NM,KY)) OR ca_state#79 IN (VA,TX,MS)))
               :     :     :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#71,ca_state#79,ca_country#81] Batched: true, DataFilters: [isnotnull(ca_country#81), (ca_country#81 = United States), isnotnull(ca_address_sk#71), ((ca_sta..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_country), EqualTo(ca_country,United States), IsNotNull(ca_address_sk), Or(Or(In(ca_..., ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_country:string>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=121]
               :     :        +- Project [d_date_sk#84]
               :     :           +- Filter ((isnotnull(d_year#90) AND (d_year#90 = 2001)) AND isnotnull(d_date_sk#84))
               :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#84,d_year#90] Batched: true, DataFilters: [isnotnull(d_year#90), (d_year#90 = 2001), isnotnull(d_date_sk#84)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=125]
               :        +- Filter (isnotnull(cd_demo_sk#57) AND ((((cd_marital_status#59 = M) AND (cd_education_status#60 = Advanced Degree)) OR ((cd_marital_status#59 = S) AND (cd_education_status#60 = College))) OR ((cd_marital_status#59 = W) AND (cd_education_status#60 = 2 yr Degree))))
               :           +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#57,cd_marital_status#59,cd_education_status#60] Batched: true, DataFilters: [isnotnull(cd_demo_sk#57), ((((cd_marital_status#59 = M) AND (cd_education_status#60 = Advanced D..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk), Or(Or(And(EqualTo(cd_marital_status,M),EqualTo(cd_education_status,Advanc..., ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=129]
                  +- Filter (isnotnull(hd_demo_sk#66) AND ((hd_dep_count#69 = 3) OR (hd_dep_count#69 = 1)))
                     +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#66,hd_dep_count#69] Batched: true, DataFilters: [isnotnull(hd_demo_sk#66), ((hd_dep_count#69 = 3) OR (hd_dep_count#69 = 1))], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_demo_sk), Or(EqualTo(hd_dep_count,3),EqualTo(hd_dep_count,1))], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int>

Time taken: 2.901 seconds, Fetched 1 row(s)
