== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['c_customer_id ASC NULLS FIRST], true
      +- 'Project ['c_customer_id AS customer_id#0, 'concat('c_last_name, , , 'coalesce('c_first_name, )) AS customername#1]
         +- 'Filter (((('ca_city = Edgewood) && ('c_current_addr_sk = 'ca_address_sk)) && (('ib_lower_bound >= 38128) && ('ib_upper_bound <= (38128 + 50000)))) && ((('ib_income_band_sk = 'hd_income_band_sk) && ('cd_demo_sk = 'c_current_cdemo_sk)) && (('hd_demo_sk = 'c_current_hdemo_sk) && ('sr_cdemo_sk = 'cd_demo_sk))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'UnresolvedRelation `customer`
               :  :  :  :  +- 'UnresolvedRelation `customer_address`
               :  :  :  +- 'UnresolvedRelation `customer_demographics`
               :  :  +- 'UnresolvedRelation `household_demographics`
               :  +- 'UnresolvedRelation `income_band`
               +- 'UnresolvedRelation `store_returns`

== Analyzed Logical Plan ==
customer_id: string, customername: string
GlobalLimit 100
+- LocalLimit 100
   +- Project [customer_id#0, customername#1]
      +- Sort [c_customer_id#5 ASC NULLS FIRST], true
         +- Project [c_customer_id#5 AS customer_id#0, concat(c_last_name#13, , , coalesce(c_first_name#12, )) AS customername#1, c_customer_id#5]
            +- Filter ((((ca_city#28 = Edgewood) && (c_current_addr_sk#8 = ca_address_sk#22)) && ((ib_lower_bound#50 >= 38128) && (ib_upper_bound#51 <= (38128 + 50000)))) && (((ib_income_band_sk#49 = hd_income_band_sk#45) && (cd_demo_sk#35 = c_current_cdemo_sk#6)) && ((hd_demo_sk#44 = c_current_hdemo_sk#7) && (sr_cdemo_sk#56 = cd_demo_sk#35))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- SubqueryAlias `tpcds`.`customer`
                  :  :  :  :  :  +- Relation[c_customer_sk#4,c_customer_id#5,c_current_cdemo_sk#6,c_current_hdemo_sk#7,c_current_addr_sk#8,c_first_shipto_date_sk#9,c_first_sales_date_sk#10,c_salutation#11,c_first_name#12,c_last_name#13,c_preferred_cust_flag#14,c_birth_day#15,c_birth_month#16,c_birth_year#17,c_birth_country#18,c_login#19,c_email_address#20,c_last_review_date#21] parquet
                  :  :  :  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :  :  :  :     +- Relation[ca_address_sk#22,ca_address_id#23,ca_street_number#24,ca_street_name#25,ca_street_type#26,ca_suite_number#27,ca_city#28,ca_county#29,ca_state#30,ca_zip#31,ca_country#32,ca_gmt_offset#33,ca_location_type#34] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`customer_demographics`
                  :  :  :     +- Relation[cd_demo_sk#35,cd_gender#36,cd_marital_status#37,cd_education_status#38,cd_purchase_estimate#39,cd_credit_rating#40,cd_dep_count#41,cd_dep_employed_count#42,cd_dep_college_count#43] parquet
                  :  :  +- SubqueryAlias `tpcds`.`household_demographics`
                  :  :     +- Relation[hd_demo_sk#44,hd_income_band_sk#45,hd_buy_potential#46,hd_dep_count#47,hd_vehicle_count#48] parquet
                  :  +- SubqueryAlias `tpcds`.`income_band`
                  :     +- Relation[ib_income_band_sk#49,ib_lower_bound#50,ib_upper_bound#51] parquet
                  +- SubqueryAlias `tpcds`.`store_returns`
                     +- Relation[sr_returned_date_sk#52,sr_return_time_sk#53,sr_item_sk#54,sr_customer_sk#55,sr_cdemo_sk#56,sr_hdemo_sk#57,sr_addr_sk#58,sr_store_sk#59,sr_reason_sk#60,sr_ticket_number#61,sr_return_quantity#62,sr_return_amt#63,sr_return_tax#64,sr_return_amt_inc_tax#65,sr_fee#66,sr_return_ship_cost#67,sr_refunded_cash#68,sr_reversed_charge#69,sr_store_credit#70,sr_net_loss#71] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Project [customer_id#0, customername#1]
      +- Sort [c_customer_id#5 ASC NULLS FIRST], true
         +- Project [c_customer_id#5 AS customer_id#0, concat(c_last_name#13, , , coalesce(c_first_name#12, )) AS customername#1, c_customer_id#5]
            +- Join Inner, (sr_cdemo_sk#56 = cd_demo_sk#35)
               :- Project [c_customer_id#5, c_first_name#12, c_last_name#13, cd_demo_sk#35]
               :  +- Join Inner, (ib_income_band_sk#49 = hd_income_band_sk#45)
               :     :- Project [c_customer_id#5, c_first_name#12, c_last_name#13, cd_demo_sk#35, hd_income_band_sk#45]
               :     :  +- Join Inner, (hd_demo_sk#44 = c_current_hdemo_sk#7)
               :     :     :- Project [c_customer_id#5, c_current_hdemo_sk#7, c_first_name#12, c_last_name#13, cd_demo_sk#35]
               :     :     :  +- Join Inner, (cd_demo_sk#35 = c_current_cdemo_sk#6)
               :     :     :     :- Project [c_customer_id#5, c_current_cdemo_sk#6, c_current_hdemo_sk#7, c_first_name#12, c_last_name#13]
               :     :     :     :  +- Join Inner, (c_current_addr_sk#8 = ca_address_sk#22)
               :     :     :     :     :- Project [c_customer_id#5, c_current_cdemo_sk#6, c_current_hdemo_sk#7, c_current_addr_sk#8, c_first_name#12, c_last_name#13]
               :     :     :     :     :  +- Filter ((isnotnull(c_current_addr_sk#8) && isnotnull(c_current_cdemo_sk#6)) && isnotnull(c_current_hdemo_sk#7))
               :     :     :     :     :     +- Relation[c_customer_sk#4,c_customer_id#5,c_current_cdemo_sk#6,c_current_hdemo_sk#7,c_current_addr_sk#8,c_first_shipto_date_sk#9,c_first_sales_date_sk#10,c_salutation#11,c_first_name#12,c_last_name#13,c_preferred_cust_flag#14,c_birth_day#15,c_birth_month#16,c_birth_year#17,c_birth_country#18,c_login#19,c_email_address#20,c_last_review_date#21] parquet
               :     :     :     :     +- Project [ca_address_sk#22]
               :     :     :     :        +- Filter ((isnotnull(ca_city#28) && (ca_city#28 = Edgewood)) && isnotnull(ca_address_sk#22))
               :     :     :     :           +- Relation[ca_address_sk#22,ca_address_id#23,ca_street_number#24,ca_street_name#25,ca_street_type#26,ca_suite_number#27,ca_city#28,ca_county#29,ca_state#30,ca_zip#31,ca_country#32,ca_gmt_offset#33,ca_location_type#34] parquet
               :     :     :     +- Project [cd_demo_sk#35]
               :     :     :        +- Filter isnotnull(cd_demo_sk#35)
               :     :     :           +- Relation[cd_demo_sk#35,cd_gender#36,cd_marital_status#37,cd_education_status#38,cd_purchase_estimate#39,cd_credit_rating#40,cd_dep_count#41,cd_dep_employed_count#42,cd_dep_college_count#43] parquet
               :     :     +- Project [hd_demo_sk#44, hd_income_band_sk#45]
               :     :        +- Filter (isnotnull(hd_demo_sk#44) && isnotnull(hd_income_band_sk#45))
               :     :           +- Relation[hd_demo_sk#44,hd_income_band_sk#45,hd_buy_potential#46,hd_dep_count#47,hd_vehicle_count#48] parquet
               :     +- Project [ib_income_band_sk#49]
               :        +- Filter ((((isnotnull(ib_lower_bound#50) && isnotnull(ib_upper_bound#51)) && (ib_lower_bound#50 >= 38128)) && (ib_upper_bound#51 <= 88128)) && isnotnull(ib_income_band_sk#49))
               :           +- Relation[ib_income_band_sk#49,ib_lower_bound#50,ib_upper_bound#51] parquet
               +- Project [sr_cdemo_sk#56]
                  +- Filter isnotnull(sr_cdemo_sk#56)
                     +- Relation[sr_returned_date_sk#52,sr_return_time_sk#53,sr_item_sk#54,sr_customer_sk#55,sr_cdemo_sk#56,sr_hdemo_sk#57,sr_addr_sk#58,sr_store_sk#59,sr_reason_sk#60,sr_ticket_number#61,sr_return_quantity#62,sr_return_amt#63,sr_return_tax#64,sr_return_amt_inc_tax#65,sr_fee#66,sr_return_ship_cost#67,sr_refunded_cash#68,sr_reversed_charge#69,sr_store_credit#70,sr_net_loss#71] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#5 ASC NULLS FIRST], output=[customer_id#0,customername#1])
+- *(6) Project [c_customer_id#5 AS customer_id#0, concat(c_last_name#13, , , coalesce(c_first_name#12, )) AS customername#1, c_customer_id#5]
   +- *(6) BroadcastHashJoin [cd_demo_sk#35], [sr_cdemo_sk#56], Inner, BuildRight
      :- *(6) Project [c_customer_id#5, c_first_name#12, c_last_name#13, cd_demo_sk#35]
      :  +- *(6) BroadcastHashJoin [hd_income_band_sk#45], [ib_income_band_sk#49], Inner, BuildRight
      :     :- *(6) Project [c_customer_id#5, c_first_name#12, c_last_name#13, cd_demo_sk#35, hd_income_band_sk#45]
      :     :  +- *(6) BroadcastHashJoin [c_current_hdemo_sk#7], [hd_demo_sk#44], Inner, BuildRight
      :     :     :- *(6) Project [c_customer_id#5, c_current_hdemo_sk#7, c_first_name#12, c_last_name#13, cd_demo_sk#35]
      :     :     :  +- *(6) BroadcastHashJoin [c_current_cdemo_sk#6], [cd_demo_sk#35], Inner, BuildRight
      :     :     :     :- *(6) Project [c_customer_id#5, c_current_cdemo_sk#6, c_current_hdemo_sk#7, c_first_name#12, c_last_name#13]
      :     :     :     :  +- *(6) BroadcastHashJoin [c_current_addr_sk#8], [ca_address_sk#22], Inner, BuildRight
      :     :     :     :     :- *(6) Project [c_customer_id#5, c_current_cdemo_sk#6, c_current_hdemo_sk#7, c_current_addr_sk#8, c_first_name#12, c_last_name#13]
      :     :     :     :     :  +- *(6) Filter ((isnotnull(c_current_addr_sk#8) && isnotnull(c_current_cdemo_sk#6)) && isnotnull(c_current_hdemo_sk#7))
      :     :     :     :     :     +- *(6) FileScan parquet tpcds.customer[c_customer_id#5,c_current_cdemo_sk#6,c_current_hdemo_sk#7,c_current_addr_sk#8,c_first_name#12,c_last_name#13] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk), IsNotNull(c_current_hdemo_sk)], ReadSchema: struct<c_customer_id:string,c_current_cdemo_sk:int,c_current_hdemo_sk:int,c_current_addr_sk:int,c...
      :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :     :     :        +- *(1) Project [ca_address_sk#22]
      :     :     :     :           +- *(1) Filter ((isnotnull(ca_city#28) && (ca_city#28 = Edgewood)) && isnotnull(ca_address_sk#22))
      :     :     :     :              +- *(1) FileScan parquet tpcds.customer_address[ca_address_sk#22,ca_city#28] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_city), EqualTo(ca_city,Edgewood), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_city:string>
      :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :     :        +- *(2) Project [cd_demo_sk#35]
      :     :     :           +- *(2) Filter isnotnull(cd_demo_sk#35)
      :     :     :              +- *(2) FileScan parquet tpcds.customer_demographics[cd_demo_sk#35] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int>
      :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :        +- *(3) Project [hd_demo_sk#44, hd_income_band_sk#45]
      :     :           +- *(3) Filter (isnotnull(hd_demo_sk#44) && isnotnull(hd_income_band_sk#45))
      :     :              +- *(3) FileScan parquet tpcds.household_demographics[hd_demo_sk#44,hd_income_band_sk#45] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_demo_sk), IsNotNull(hd_income_band_sk)], ReadSchema: struct<hd_demo_sk:int,hd_income_band_sk:int>
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :        +- *(4) Project [ib_income_band_sk#49]
      :           +- *(4) Filter ((((isnotnull(ib_lower_bound#50) && isnotnull(ib_upper_bound#51)) && (ib_lower_bound#50 >= 38128)) && (ib_upper_bound#51 <= 88128)) && isnotnull(ib_income_band_sk#49))
      :              +- *(4) FileScan parquet tpcds.income_band[ib_income_band_sk#49,ib_lower_bound#50,ib_upper_bound#51] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(ib_lower_bound), IsNotNull(ib_upper_bound), GreaterThanOrEqual(ib_lower_bound,38128), ..., ReadSchema: struct<ib_income_band_sk:int,ib_lower_bound:int,ib_upper_bound:int>
      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         +- *(5) Project [sr_cdemo_sk#56]
            +- *(5) Filter isnotnull(sr_cdemo_sk#56)
               +- *(5) FileScan parquet tpcds.store_returns[sr_cdemo_sk#56] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_cdemo_sk)], ReadSchema: struct<sr_cdemo_sk:int>
Time taken: 3.735 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 84 in stream 0 using template query84.tpl
------------------------------------------------------^^^

