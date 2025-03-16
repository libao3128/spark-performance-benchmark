== Parsed Logical Plan ==
'Project [unresolvedalias('sum('ss_quantity), None)]
+- 'Filter (((('s_store_sk = 'ss_store_sk) && ('ss_sold_date_sk = 'd_date_sk)) && ('d_year = 2000)) && (((((('cd_demo_sk = 'ss_cdemo_sk) && ('cd_marital_status = M)) && (('cd_education_status = 4 yr Degree) && (('ss_sales_price >= 100.00) && ('ss_sales_price <= 150.00)))) || ((('cd_demo_sk = 'ss_cdemo_sk) && ('cd_marital_status = D)) && (('cd_education_status = 2 yr Degree) && (('ss_sales_price >= 50.00) && ('ss_sales_price <= 100.00))))) || ((('cd_demo_sk = 'ss_cdemo_sk) && ('cd_marital_status = S)) && (('cd_education_status = College) && (('ss_sales_price >= 150.00) && ('ss_sales_price <= 200.00))))) && ((((('ss_addr_sk = 'ca_address_sk) && ('ca_country = United States)) && ('ca_state IN (CO,OH,TX) && (('ss_net_profit >= 0) && ('ss_net_profit <= 2000)))) || ((('ss_addr_sk = 'ca_address_sk) && ('ca_country = United States)) && ('ca_state IN (OR,MN,KY) && (('ss_net_profit >= 150) && ('ss_net_profit <= 3000))))) || ((('ss_addr_sk = 'ca_address_sk) && ('ca_country = United States)) && ('ca_state IN (VA,CA,MS) && (('ss_net_profit >= 50) && ('ss_net_profit <= 25000)))))))
   +- 'Join Inner
      :- 'Join Inner
      :  :- 'Join Inner
      :  :  :- 'Join Inner
      :  :  :  :- 'UnresolvedRelation `store_sales`
      :  :  :  +- 'UnresolvedRelation `store`
      :  :  +- 'UnresolvedRelation `customer_demographics`
      :  +- 'UnresolvedRelation `customer_address`
      +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
sum(ss_quantity): bigint
Aggregate [sum(cast(ss_quantity#12 as bigint)) AS sum(ss_quantity)#105L]
+- Filter ((((s_store_sk#25 = ss_store_sk#9) && (ss_sold_date_sk#2 = d_date_sk#76)) && (d_year#82 = 2000)) && ((((((cd_demo_sk#54 = ss_cdemo_sk#6) && (cd_marital_status#56 = M)) && ((cd_education_status#57 = 4 yr Degree) && ((ss_sales_price#15 >= cast(100.00 as double)) && (ss_sales_price#15 <= cast(150.00 as double))))) || (((cd_demo_sk#54 = ss_cdemo_sk#6) && (cd_marital_status#56 = D)) && ((cd_education_status#57 = 2 yr Degree) && ((ss_sales_price#15 >= cast(50.00 as double)) && (ss_sales_price#15 <= cast(100.00 as double)))))) || (((cd_demo_sk#54 = ss_cdemo_sk#6) && (cd_marital_status#56 = S)) && ((cd_education_status#57 = College) && ((ss_sales_price#15 >= cast(150.00 as double)) && (ss_sales_price#15 <= cast(200.00 as double)))))) && (((((ss_addr_sk#8 = ca_address_sk#63) && (ca_country#73 = United States)) && (ca_state#71 IN (CO,OH,TX) && ((ss_net_profit#24 >= cast(0 as double)) && (ss_net_profit#24 <= cast(2000 as double))))) || (((ss_addr_sk#8 = ca_address_sk#63) && (ca_country#73 = United States)) && (ca_state#71 IN (OR,MN,KY) && ((ss_net_profit#24 >= cast(150 as double)) && (ss_net_profit#24 <= cast(3000 as double)))))) || (((ss_addr_sk#8 = ca_address_sk#63) && (ca_country#73 = United States)) && (ca_state#71 IN (VA,CA,MS) && ((ss_net_profit#24 >= cast(50 as double)) && (ss_net_profit#24 <= cast(25000 as double))))))))
   +- Join Inner
      :- Join Inner
      :  :- Join Inner
      :  :  :- Join Inner
      :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
      :  :  :  :  +- Relation[ss_sold_date_sk#2,ss_sold_time_sk#3,ss_item_sk#4,ss_customer_sk#5,ss_cdemo_sk#6,ss_hdemo_sk#7,ss_addr_sk#8,ss_store_sk#9,ss_promo_sk#10,ss_ticket_number#11,ss_quantity#12,ss_wholesale_cost#13,ss_list_price#14,ss_sales_price#15,ss_ext_discount_amt#16,ss_ext_sales_price#17,ss_ext_wholesale_cost#18,ss_ext_list_price#19,ss_ext_tax#20,ss_coupon_amt#21,ss_net_paid#22,ss_net_paid_inc_tax#23,ss_net_profit#24] parquet
      :  :  :  +- SubqueryAlias `tpcds`.`store`
      :  :  :     +- Relation[s_store_sk#25,s_store_id#26,s_rec_start_date#27,s_rec_end_date#28,s_closed_date_sk#29,s_store_name#30,s_number_employees#31,s_floor_space#32,s_hours#33,s_manager#34,s_market_id#35,s_geography_class#36,s_market_desc#37,s_market_manager#38,s_division_id#39,s_division_name#40,s_company_id#41,s_company_name#42,s_street_number#43,s_street_name#44,s_street_type#45,s_suite_number#46,s_city#47,s_county#48,... 5 more fields] parquet
      :  :  +- SubqueryAlias `tpcds`.`customer_demographics`
      :  :     +- Relation[cd_demo_sk#54,cd_gender#55,cd_marital_status#56,cd_education_status#57,cd_purchase_estimate#58,cd_credit_rating#59,cd_dep_count#60,cd_dep_employed_count#61,cd_dep_college_count#62] parquet
      :  +- SubqueryAlias `tpcds`.`customer_address`
      :     +- Relation[ca_address_sk#63,ca_address_id#64,ca_street_number#65,ca_street_name#66,ca_street_type#67,ca_suite_number#68,ca_city#69,ca_county#70,ca_state#71,ca_zip#72,ca_country#73,ca_gmt_offset#74,ca_location_type#75] parquet
      +- SubqueryAlias `tpcds`.`date_dim`
         +- Relation[d_date_sk#76,d_date_id#77,d_date#78,d_month_seq#79,d_week_seq#80,d_quarter_seq#81,d_year#82,d_dow#83,d_moy#84,d_dom#85,d_qoy#86,d_fy_year#87,d_fy_quarter_seq#88,d_fy_week_seq#89,d_day_name#90,d_quarter_name#91,d_holiday#92,d_weekend#93,d_following_holiday#94,d_first_dom#95,d_last_dom#96,d_same_day_ly#97,d_same_day_lq#98,d_current_day#99,... 4 more fields] parquet

== Optimized Logical Plan ==
Aggregate [sum(cast(ss_quantity#12 as bigint)) AS sum(ss_quantity)#105L]
+- Project [ss_quantity#12]
   +- Join Inner, (ss_sold_date_sk#2 = d_date_sk#76)
      :- Project [ss_sold_date_sk#2, ss_quantity#12]
      :  +- Join Inner, ((ss_addr_sk#8 = ca_address_sk#63) && ((((ca_state#71 IN (CO,OH,TX) && (ss_net_profit#24 >= 0.0)) && (ss_net_profit#24 <= 2000.0)) || ((ca_state#71 IN (OR,MN,KY) && (ss_net_profit#24 >= 150.0)) && (ss_net_profit#24 <= 3000.0))) || ((ca_state#71 IN (VA,CA,MS) && (ss_net_profit#24 >= 50.0)) && (ss_net_profit#24 <= 25000.0))))
      :     :- Project [ss_sold_date_sk#2, ss_addr_sk#8, ss_quantity#12, ss_net_profit#24]
      :     :  +- Join Inner, ((cd_demo_sk#54 = ss_cdemo_sk#6) && ((((((cd_marital_status#56 = M) && (cd_education_status#57 = 4 yr Degree)) && (ss_sales_price#15 >= 100.0)) && (ss_sales_price#15 <= 150.0)) || ((((cd_marital_status#56 = D) && (cd_education_status#57 = 2 yr Degree)) && (ss_sales_price#15 >= 50.0)) && (ss_sales_price#15 <= 100.0))) || ((((cd_marital_status#56 = S) && (cd_education_status#57 = College)) && (ss_sales_price#15 >= 150.0)) && (ss_sales_price#15 <= 200.0))))
      :     :     :- Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_addr_sk#8, ss_quantity#12, ss_sales_price#15, ss_net_profit#24]
      :     :     :  +- Join Inner, (s_store_sk#25 = ss_store_sk#9)
      :     :     :     :- Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_addr_sk#8, ss_store_sk#9, ss_quantity#12, ss_sales_price#15, ss_net_profit#24]
      :     :     :     :  +- Filter (((isnotnull(ss_store_sk#9) && isnotnull(ss_cdemo_sk#6)) && isnotnull(ss_addr_sk#8)) && isnotnull(ss_sold_date_sk#2))
      :     :     :     :     +- Relation[ss_sold_date_sk#2,ss_sold_time_sk#3,ss_item_sk#4,ss_customer_sk#5,ss_cdemo_sk#6,ss_hdemo_sk#7,ss_addr_sk#8,ss_store_sk#9,ss_promo_sk#10,ss_ticket_number#11,ss_quantity#12,ss_wholesale_cost#13,ss_list_price#14,ss_sales_price#15,ss_ext_discount_amt#16,ss_ext_sales_price#17,ss_ext_wholesale_cost#18,ss_ext_list_price#19,ss_ext_tax#20,ss_coupon_amt#21,ss_net_paid#22,ss_net_paid_inc_tax#23,ss_net_profit#24] parquet
      :     :     :     +- Project [s_store_sk#25]
      :     :     :        +- Filter isnotnull(s_store_sk#25)
      :     :     :           +- Relation[s_store_sk#25,s_store_id#26,s_rec_start_date#27,s_rec_end_date#28,s_closed_date_sk#29,s_store_name#30,s_number_employees#31,s_floor_space#32,s_hours#33,s_manager#34,s_market_id#35,s_geography_class#36,s_market_desc#37,s_market_manager#38,s_division_id#39,s_division_name#40,s_company_id#41,s_company_name#42,s_street_number#43,s_street_name#44,s_street_type#45,s_suite_number#46,s_city#47,s_county#48,... 5 more fields] parquet
      :     :     +- Project [cd_demo_sk#54, cd_marital_status#56, cd_education_status#57]
      :     :        +- Filter isnotnull(cd_demo_sk#54)
      :     :           +- Relation[cd_demo_sk#54,cd_gender#55,cd_marital_status#56,cd_education_status#57,cd_purchase_estimate#58,cd_credit_rating#59,cd_dep_count#60,cd_dep_employed_count#61,cd_dep_college_count#62] parquet
      :     +- Project [ca_address_sk#63, ca_state#71]
      :        +- Filter ((isnotnull(ca_country#73) && (ca_country#73 = United States)) && isnotnull(ca_address_sk#63))
      :           +- Relation[ca_address_sk#63,ca_address_id#64,ca_street_number#65,ca_street_name#66,ca_street_type#67,ca_suite_number#68,ca_city#69,ca_county#70,ca_state#71,ca_zip#72,ca_country#73,ca_gmt_offset#74,ca_location_type#75] parquet
      +- Project [d_date_sk#76]
         +- Filter ((isnotnull(d_year#82) && (d_year#82 = 2000)) && isnotnull(d_date_sk#76))
            +- Relation[d_date_sk#76,d_date_id#77,d_date#78,d_month_seq#79,d_week_seq#80,d_quarter_seq#81,d_year#82,d_dow#83,d_moy#84,d_dom#85,d_qoy#86,d_fy_year#87,d_fy_quarter_seq#88,d_fy_week_seq#89,d_day_name#90,d_quarter_name#91,d_holiday#92,d_weekend#93,d_following_holiday#94,d_first_dom#95,d_last_dom#96,d_same_day_ly#97,d_same_day_lq#98,d_current_day#99,... 4 more fields] parquet

== Physical Plan ==
*(6) HashAggregate(keys=[], functions=[sum(cast(ss_quantity#12 as bigint))], output=[sum(ss_quantity)#105L])
+- Exchange SinglePartition
   +- *(5) HashAggregate(keys=[], functions=[partial_sum(cast(ss_quantity#12 as bigint))], output=[sum#107L])
      +- *(5) Project [ss_quantity#12]
         +- *(5) BroadcastHashJoin [ss_sold_date_sk#2], [d_date_sk#76], Inner, BuildRight
            :- *(5) Project [ss_sold_date_sk#2, ss_quantity#12]
            :  +- *(5) BroadcastHashJoin [ss_addr_sk#8], [ca_address_sk#63], Inner, BuildRight, ((((ca_state#71 IN (CO,OH,TX) && (ss_net_profit#24 >= 0.0)) && (ss_net_profit#24 <= 2000.0)) || ((ca_state#71 IN (OR,MN,KY) && (ss_net_profit#24 >= 150.0)) && (ss_net_profit#24 <= 3000.0))) || ((ca_state#71 IN (VA,CA,MS) && (ss_net_profit#24 >= 50.0)) && (ss_net_profit#24 <= 25000.0)))
            :     :- *(5) Project [ss_sold_date_sk#2, ss_addr_sk#8, ss_quantity#12, ss_net_profit#24]
            :     :  +- *(5) BroadcastHashJoin [ss_cdemo_sk#6], [cd_demo_sk#54], Inner, BuildRight, ((((((cd_marital_status#56 = M) && (cd_education_status#57 = 4 yr Degree)) && (ss_sales_price#15 >= 100.0)) && (ss_sales_price#15 <= 150.0)) || ((((cd_marital_status#56 = D) && (cd_education_status#57 = 2 yr Degree)) && (ss_sales_price#15 >= 50.0)) && (ss_sales_price#15 <= 100.0))) || ((((cd_marital_status#56 = S) && (cd_education_status#57 = College)) && (ss_sales_price#15 >= 150.0)) && (ss_sales_price#15 <= 200.0)))
            :     :     :- *(5) Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_addr_sk#8, ss_quantity#12, ss_sales_price#15, ss_net_profit#24]
            :     :     :  +- *(5) BroadcastHashJoin [ss_store_sk#9], [s_store_sk#25], Inner, BuildRight
            :     :     :     :- *(5) Project [ss_sold_date_sk#2, ss_cdemo_sk#6, ss_addr_sk#8, ss_store_sk#9, ss_quantity#12, ss_sales_price#15, ss_net_profit#24]
            :     :     :     :  +- *(5) Filter (((isnotnull(ss_store_sk#9) && isnotnull(ss_cdemo_sk#6)) && isnotnull(ss_addr_sk#8)) && isnotnull(ss_sold_date_sk#2))
            :     :     :     :     +- *(5) FileScan parquet tpcds.store_sales[ss_sold_date_sk#2,ss_cdemo_sk#6,ss_addr_sk#8,ss_store_sk#9,ss_quantity#12,ss_sales_price#15,ss_net_profit#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_cdemo_sk), IsNotNull(ss_addr_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_cdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_quantity:int,ss_sale...
            :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :     :        +- *(1) Project [s_store_sk#25]
            :     :     :           +- *(1) Filter isnotnull(s_store_sk#25)
            :     :     :              +- *(1) FileScan parquet tpcds.store[s_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
            :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :        +- *(2) Project [cd_demo_sk#54, cd_marital_status#56, cd_education_status#57]
            :     :           +- *(2) Filter isnotnull(cd_demo_sk#54)
            :     :              +- *(2) FileScan parquet tpcds.customer_demographics[cd_demo_sk#54,cd_marital_status#56,cd_education_status#57] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
            :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :        +- *(3) Project [ca_address_sk#63, ca_state#71]
            :           +- *(3) Filter ((isnotnull(ca_country#73) && (ca_country#73 = United States)) && isnotnull(ca_address_sk#63))
            :              +- *(3) FileScan parquet tpcds.customer_address[ca_address_sk#63,ca_state#71,ca_country#73] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_country), EqualTo(ca_country,United States), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_country:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               +- *(4) Project [d_date_sk#76]
                  +- *(4) Filter ((isnotnull(d_year#82) && (d_year#82 = 2000)) && isnotnull(d_date_sk#76))
                     +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#76,d_year#82] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
Time taken: 4.131 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 48 in stream 0 using template query48.tpl
------------------------------------------------------^^^

