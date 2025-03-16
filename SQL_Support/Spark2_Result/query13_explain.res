== Parsed Logical Plan ==
'Project [unresolvedalias('avg('ss_quantity), None), unresolvedalias('avg('ss_ext_sales_price), None), unresolvedalias('avg('ss_ext_wholesale_cost), None), unresolvedalias('sum('ss_ext_wholesale_cost), None)]
+- 'Filter (((('s_store_sk = 'ss_store_sk) && ('ss_sold_date_sk = 'd_date_sk)) && ('d_year = 2001)) && ((((((('ss_hdemo_sk = 'hd_demo_sk) && ('cd_demo_sk = 'ss_cdemo_sk)) && ('cd_marital_status = M)) && ((('cd_education_status = Advanced Degree) && (('ss_sales_price >= 100.00) && ('ss_sales_price <= 150.00))) && ('hd_dep_count = 3))) || (((('ss_hdemo_sk = 'hd_demo_sk) && ('cd_demo_sk = 'ss_cdemo_sk)) && ('cd_marital_status = S)) && ((('cd_education_status = College) && (('ss_sales_price >= 50.00) && ('ss_sales_price <= 100.00))) && ('hd_dep_count = 1)))) || (((('ss_hdemo_sk = 'hd_demo_sk) && ('cd_demo_sk = 'ss_cdemo_sk)) && ('cd_marital_status = W)) && ((('cd_education_status = 2 yr Degree) && (('ss_sales_price >= 150.00) && ('ss_sales_price <= 200.00))) && ('hd_dep_count = 1)))) && ((((('ss_addr_sk = 'ca_address_sk) && ('ca_country = United States)) && ('ca_state IN (TX,OH,TX) && (('ss_net_profit >= 100) && ('ss_net_profit <= 200)))) || ((('ss_addr_sk = 'ca_address_sk) && ('ca_country = United States)) && ('ca_state IN (OR,NM,KY) && (('ss_net_profit >= 150) && ('ss_net_profit <= 300))))) || ((('ss_addr_sk = 'ca_address_sk) && ('ca_country = United States)) && ('ca_state IN (VA,TX,MS) && (('ss_net_profit >= 50) && ('ss_net_profit <= 250)))))))
   +- 'Join Inner
      :- 'Join Inner
      :  :- 'Join Inner
      :  :  :- 'Join Inner
      :  :  :  :- 'Join Inner
      :  :  :  :  :- 'UnresolvedRelation `store_sales`
      :  :  :  :  +- 'UnresolvedRelation `store`
      :  :  :  +- 'UnresolvedRelation `customer_demographics`
      :  :  +- 'UnresolvedRelation `household_demographics`
      :  +- 'UnresolvedRelation `customer_address`
      +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
avg(ss_quantity): double, avg(ss_ext_sales_price): double, avg(ss_ext_wholesale_cost): double, sum(ss_ext_wholesale_cost): double
Aggregate [avg(cast(ss_quantity#12 as bigint)) AS avg(ss_quantity)#113, avg(ss_ext_sales_price#17) AS avg(ss_ext_sales_price)#114, avg(ss_ext_wholesale_cost#18) AS avg(ss_ext_wholesale_cost)#115, sum(ss_ext_wholesale_cost#18) AS sum(ss_ext_wholesale_cost)#116]
+- Filter ((((s_store_sk#25 = ss_store_sk#9) && (ss_sold_date_sk#2 = d_date_sk#81)) && (d_year#87 = 2001)) && (((((((ss_hdemo_sk#7 = hd_demo_sk#63) && (cd_demo_sk#54 = ss_cdemo_sk#6)) && (cd_marital_status#56 = M)) && (((cd_education_status#57 = Advanced Degree) && ((ss_sales_price#15 >= cast(100.00 as double)) && (ss_sales_price#15 <= cast(150.00 as double)))) && (hd_dep_count#66 = 3))) || ((((ss_hdemo_sk#7 = hd_demo_sk#63) && (cd_demo_sk#54 = ss_cdemo_sk#6)) && (cd_marital_status#56 = S)) && (((cd_education_status#57 = College) && ((ss_sales_price#15 >= cast(50.00 as double)) && (ss_sales_price#15 <= cast(100.00 as double)))) && (hd_dep_count#66 = 1)))) || ((((ss_hdemo_sk#7 = hd_demo_sk#63) && (cd_demo_sk#54 = ss_cdemo_sk#6)) && (cd_marital_status#56 = W)) && (((cd_education_status#57 = 2 yr Degree) && ((ss_sales_price#15 >= cast(150.00 as double)) && (ss_sales_price#15 <= cast(200.00 as double)))) && (hd_dep_count#66 = 1)))) && (((((ss_addr_sk#8 = ca_address_sk#68) && (ca_country#78 = United States)) && (ca_state#76 IN (TX,OH,TX) && ((ss_net_profit#24 >= cast(100 as double)) && (ss_net_profit#24 <= cast(200 as double))))) || (((ss_addr_sk#8 = ca_address_sk#68) && (ca_country#78 = United States)) && (ca_state#76 IN (OR,NM,KY) && ((ss_net_profit#24 >= cast(150 as double)) && (ss_net_profit#24 <= cast(300 as double)))))) || (((ss_addr_sk#8 = ca_address_sk#68) && (ca_country#78 = United States)) && (ca_state#76 IN (VA,TX,MS) && ((ss_net_profit#24 >= cast(50 as double)) && (ss_net_profit#24 <= cast(250 as double))))))))
   +- Join Inner
      :- Join Inner
      :  :- Join Inner
      :  :  :- Join Inner
      :  :  :  :- Join Inner
      :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
      :  :  :  :  :  +- Relation[ss_sold_date_sk#2,ss_sold_time_sk#3,ss_item_sk#4,ss_customer_sk#5,ss_cdemo_sk#6,ss_hdemo_sk#7,ss_addr_sk#8,ss_store_sk#9,ss_promo_sk#10,ss_ticket_number#11,ss_quantity#12,ss_wholesale_cost#13,ss_list_price#14,ss_sales_price#15,ss_ext_discount_amt#16,ss_ext_sales_price#17,ss_ext_wholesale_cost#18,ss_ext_list_price#19,ss_ext_tax#20,ss_coupon_amt#21,ss_net_paid#22,ss_net_paid_inc_tax#23,ss_net_profit#24] parquet
      :  :  :  :  +- SubqueryAlias `tpcds`.`store`
      :  :  :  :     +- Relation[s_store_sk#25,s_store_id#26,s_rec_start_date#27,s_rec_end_date#28,s_closed_date_sk#29,s_store_name#30,s_number_employees#31,s_floor_space#32,s_hours#33,s_manager#34,s_market_id#35,s_geography_class#36,s_market_desc#37,s_market_manager#38,s_division_id#39,s_division_name#40,s_company_id#41,s_company_name#42,s_street_number#43,s_street_name#44,s_street_type#45,s_suite_number#46,s_city#47,s_county#48,... 5 more fields] parquet
      :  :  :  +- SubqueryAlias `tpcds`.`customer_demographics`
      :  :  :     +- Relation[cd_demo_sk#54,cd_gender#55,cd_marital_status#56,cd_education_status#57,cd_purchase_estimate#58,cd_credit_rating#59,cd_dep_count#60,cd_dep_employed_count#61,cd_dep_college_count#62] parquet
      :  :  +- SubqueryAlias `tpcds`.`household_demographics`
      :  :     +- Relation[hd_demo_sk#63,hd_income_band_sk#64,hd_buy_potential#65,hd_dep_count#66,hd_vehicle_count#67] parquet
      :  +- SubqueryAlias `tpcds`.`customer_address`
      :     +- Relation[ca_address_sk#68,ca_address_id#69,ca_street_number#70,ca_street_name#71,ca_street_type#72,ca_suite_number#73,ca_city#74,ca_county#75,ca_state#76,ca_zip#77,ca_country#78,ca_gmt_offset#79,ca_location_type#80] parquet
      +- SubqueryAlias `tpcds`.`date_dim`
         +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet

== Optimized Logical Plan ==
Aggregate [avg(cast(ss_quantity#12 as bigint)) AS avg(ss_quantity)#113, avg(ss_ext_sales_price#17) AS avg(ss_ext_sales_price)#114, avg(ss_ext_wholesale_cost#18) AS avg(ss_ext_wholesale_cost)#115, sum(ss_ext_wholesale_cost#18) AS sum(ss_ext_wholesale_cost)#116]
+- Project [ss_quantity#12, ss_ext_sales_price#17, ss_ext_wholesale_cost#18]
   +- Join Inner, ((ss_hdemo_sk#7 = hd_demo_sk#63) && (((((((cd_marital_status#56 = M) && (cd_education_status#57 = Advanced Degree)) && (ss_sales_price#15 >= 100.0)) && (ss_sales_price#15 <= 150.0)) && (hd_dep_count#66 = 3)) || (((((cd_marital_status#56 = S) && (cd_education_status#57 = College)) && (ss_sales_price#15 >= 50.0)) && (ss_sales_price#15 <= 100.0)) && (hd_dep_count#66 = 1))) || (((((cd_marital_status#56 = W) && (cd_education_status#57 = 2 yr Degree)) && (ss_sales_price#15 >= 150.0)) && (ss_sales_price#15 <= 200.0)) && (hd_dep_count#66 = 1))))
      :- Project [ss_hdemo_sk#7, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18, cd_marital_status#56, cd_education_status#57]
      :  +- Join Inner, (cd_demo_sk#54 = ss_cdemo_sk#6)
      :     :- Project [ss_cdemo_sk#6, ss_hdemo_sk#7, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18]
      :     :  +- Join Inner, (ss_sold_date_sk#2 = d_date_sk#81)
      :     :     :- Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_hdemo_sk#7, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18]
      :     :     :  +- Join Inner, ((ss_addr_sk#8 = ca_address_sk#68) && ((((ca_state#76 IN (TX,OH) && (ss_net_profit#24 >= 100.0)) && (ss_net_profit#24 <= 200.0)) || ((ca_state#76 IN (OR,NM,KY) && (ss_net_profit#24 >= 150.0)) && (ss_net_profit#24 <= 300.0))) || ((ca_state#76 IN (VA,TX,MS) && (ss_net_profit#24 >= 50.0)) && (ss_net_profit#24 <= 250.0))))
      :     :     :     :- Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_hdemo_sk#7, ss_addr_sk#8, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18, ss_net_profit#24]
      :     :     :     :  +- Join Inner, (s_store_sk#25 = ss_store_sk#9)
      :     :     :     :     :- Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_hdemo_sk#7, ss_addr_sk#8, ss_store_sk#9, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18, ss_net_profit#24]
      :     :     :     :     :  +- Filter ((((isnotnull(ss_store_sk#9) && isnotnull(ss_addr_sk#8)) && isnotnull(ss_sold_date_sk#2)) && isnotnull(ss_cdemo_sk#6)) && isnotnull(ss_hdemo_sk#7))
      :     :     :     :     :     +- Relation[ss_sold_date_sk#2,ss_sold_time_sk#3,ss_item_sk#4,ss_customer_sk#5,ss_cdemo_sk#6,ss_hdemo_sk#7,ss_addr_sk#8,ss_store_sk#9,ss_promo_sk#10,ss_ticket_number#11,ss_quantity#12,ss_wholesale_cost#13,ss_list_price#14,ss_sales_price#15,ss_ext_discount_amt#16,ss_ext_sales_price#17,ss_ext_wholesale_cost#18,ss_ext_list_price#19,ss_ext_tax#20,ss_coupon_amt#21,ss_net_paid#22,ss_net_paid_inc_tax#23,ss_net_profit#24] parquet
      :     :     :     :     +- Project [s_store_sk#25]
      :     :     :     :        +- Filter isnotnull(s_store_sk#25)
      :     :     :     :           +- Relation[s_store_sk#25,s_store_id#26,s_rec_start_date#27,s_rec_end_date#28,s_closed_date_sk#29,s_store_name#30,s_number_employees#31,s_floor_space#32,s_hours#33,s_manager#34,s_market_id#35,s_geography_class#36,s_market_desc#37,s_market_manager#38,s_division_id#39,s_division_name#40,s_company_id#41,s_company_name#42,s_street_number#43,s_street_name#44,s_street_type#45,s_suite_number#46,s_city#47,s_county#48,... 5 more fields] parquet
      :     :     :     +- Project [ca_address_sk#68, ca_state#76]
      :     :     :        +- Filter ((isnotnull(ca_country#78) && (ca_country#78 = United States)) && isnotnull(ca_address_sk#68))
      :     :     :           +- Relation[ca_address_sk#68,ca_address_id#69,ca_street_number#70,ca_street_name#71,ca_street_type#72,ca_suite_number#73,ca_city#74,ca_county#75,ca_state#76,ca_zip#77,ca_country#78,ca_gmt_offset#79,ca_location_type#80] parquet
      :     :     +- Project [d_date_sk#81]
      :     :        +- Filter ((isnotnull(d_year#87) && (d_year#87 = 2001)) && isnotnull(d_date_sk#81))
      :     :           +- Relation[d_date_sk#81,d_date_id#82,d_date#83,d_month_seq#84,d_week_seq#85,d_quarter_seq#86,d_year#87,d_dow#88,d_moy#89,d_dom#90,d_qoy#91,d_fy_year#92,d_fy_quarter_seq#93,d_fy_week_seq#94,d_day_name#95,d_quarter_name#96,d_holiday#97,d_weekend#98,d_following_holiday#99,d_first_dom#100,d_last_dom#101,d_same_day_ly#102,d_same_day_lq#103,d_current_day#104,... 4 more fields] parquet
      :     +- Project [cd_demo_sk#54, cd_marital_status#56, cd_education_status#57]
      :        +- Filter isnotnull(cd_demo_sk#54)
      :           +- Relation[cd_demo_sk#54,cd_gender#55,cd_marital_status#56,cd_education_status#57,cd_purchase_estimate#58,cd_credit_rating#59,cd_dep_count#60,cd_dep_employed_count#61,cd_dep_college_count#62] parquet
      +- Project [hd_demo_sk#63, hd_dep_count#66]
         +- Filter isnotnull(hd_demo_sk#63)
            +- Relation[hd_demo_sk#63,hd_income_band_sk#64,hd_buy_potential#65,hd_dep_count#66,hd_vehicle_count#67] parquet

== Physical Plan ==
*(7) HashAggregate(keys=[], functions=[avg(cast(ss_quantity#12 as bigint)), avg(ss_ext_sales_price#17), avg(ss_ext_wholesale_cost#18), sum(ss_ext_wholesale_cost#18)], output=[avg(ss_quantity)#113, avg(ss_ext_sales_price)#114, avg(ss_ext_wholesale_cost)#115, sum(ss_ext_wholesale_cost)#116])
+- Exchange SinglePartition
   +- *(6) HashAggregate(keys=[], functions=[partial_avg(cast(ss_quantity#12 as bigint)), partial_avg(ss_ext_sales_price#17), partial_avg(ss_ext_wholesale_cost#18), partial_sum(ss_ext_wholesale_cost#18)], output=[sum#124, count#125L, sum#126, count#127L, sum#128, count#129L, sum#130])
      +- *(6) Project [ss_quantity#12, ss_ext_sales_price#17, ss_ext_wholesale_cost#18]
         +- *(6) BroadcastHashJoin [ss_hdemo_sk#7], [hd_demo_sk#63], Inner, BuildRight, (((((((cd_marital_status#56 = M) && (cd_education_status#57 = Advanced Degree)) && (ss_sales_price#15 >= 100.0)) && (ss_sales_price#15 <= 150.0)) && (hd_dep_count#66 = 3)) || (((((cd_marital_status#56 = S) && (cd_education_status#57 = College)) && (ss_sales_price#15 >= 50.0)) && (ss_sales_price#15 <= 100.0)) && (hd_dep_count#66 = 1))) || (((((cd_marital_status#56 = W) && (cd_education_status#57 = 2 yr Degree)) && (ss_sales_price#15 >= 150.0)) && (ss_sales_price#15 <= 200.0)) && (hd_dep_count#66 = 1)))
            :- *(6) Project [ss_hdemo_sk#7, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18, cd_marital_status#56, cd_education_status#57]
            :  +- *(6) BroadcastHashJoin [ss_cdemo_sk#6], [cd_demo_sk#54], Inner, BuildRight
            :     :- *(6) Project [ss_cdemo_sk#6, ss_hdemo_sk#7, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18]
            :     :  +- *(6) BroadcastHashJoin [ss_sold_date_sk#2], [d_date_sk#81], Inner, BuildRight
            :     :     :- *(6) Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_hdemo_sk#7, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18]
            :     :     :  +- *(6) BroadcastHashJoin [ss_addr_sk#8], [ca_address_sk#68], Inner, BuildRight, ((((ca_state#76 IN (TX,OH) && (ss_net_profit#24 >= 100.0)) && (ss_net_profit#24 <= 200.0)) || ((ca_state#76 IN (OR,NM,KY) && (ss_net_profit#24 >= 150.0)) && (ss_net_profit#24 <= 300.0))) || ((ca_state#76 IN (VA,TX,MS) && (ss_net_profit#24 >= 50.0)) && (ss_net_profit#24 <= 250.0)))
            :     :     :     :- *(6) Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_hdemo_sk#7, ss_addr_sk#8, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18, ss_net_profit#24]
            :     :     :     :  +- *(6) BroadcastHashJoin [ss_store_sk#9], [s_store_sk#25], Inner, BuildRight
            :     :     :     :     :- *(6) Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_hdemo_sk#7, ss_addr_sk#8, ss_store_sk#9, ss_quantity#12, ss_sales_price#15, ss_ext_sales_price#17, ss_ext_wholesale_cost#18, ss_net_profit#24]
            :     :     :     :     :  +- *(6) Filter ((((isnotnull(ss_store_sk#9) && isnotnull(ss_addr_sk#8)) && isnotnull(ss_sold_date_sk#2)) && isnotnull(ss_cdemo_sk#6)) && isnotnull(ss_hdemo_sk#7))
            :     :     :     :     :     +- *(6) FileScan parquet tpcds.store_sales[ss_sold_date_sk#2,ss_cdemo_sk#6,ss_hdemo_sk#7,ss_addr_sk#8,ss_store_sk#9,ss_quantity#12,ss_sales_price#15,ss_ext_sales_price#17,ss_ext_wholesale_cost#18,ss_net_profit#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_addr_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_cdemo_sk..., ReadSchema: struct<ss_sold_date_sk:int,ss_cdemo_sk:int,ss_hdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_quan...
            :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :     :     :        +- *(1) Project [s_store_sk#25]
            :     :     :     :           +- *(1) Filter isnotnull(s_store_sk#25)
            :     :     :     :              +- *(1) FileScan parquet tpcds.store[s_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
            :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :     :        +- *(2) Project [ca_address_sk#68, ca_state#76]
            :     :     :           +- *(2) Filter ((isnotnull(ca_country#78) && (ca_country#78 = United States)) && isnotnull(ca_address_sk#68))
            :     :     :              +- *(2) FileScan parquet tpcds.customer_address[ca_address_sk#68,ca_state#76,ca_country#78] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_country), EqualTo(ca_country,United States), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_country:string>
            :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :        +- *(3) Project [d_date_sk#81]
            :     :           +- *(3) Filter ((isnotnull(d_year#87) && (d_year#87 = 2001)) && isnotnull(d_date_sk#81))
            :     :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#81,d_year#87] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
            :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :        +- *(4) Project [cd_demo_sk#54, cd_marital_status#56, cd_education_status#57]
            :           +- *(4) Filter isnotnull(cd_demo_sk#54)
            :              +- *(4) FileScan parquet tpcds.customer_demographics[cd_demo_sk#54,cd_marital_status#56,cd_education_status#57] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               +- *(5) Project [hd_demo_sk#63, hd_dep_count#66]
                  +- *(5) Filter isnotnull(hd_demo_sk#63)
                     +- *(5) FileScan parquet tpcds.household_demographics[hd_demo_sk#63,hd_dep_count#66] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int>
Time taken: 4.39 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 13 in stream 0 using template query13.tpl
------------------------------------------------------^^^

