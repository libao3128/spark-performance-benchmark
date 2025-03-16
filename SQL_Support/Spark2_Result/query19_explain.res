== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['ext_price DESC NULLS LAST, 'i_brand ASC NULLS FIRST, 'i_brand_id ASC NULLS FIRST, 'i_manufact_id ASC NULLS FIRST, 'i_manufact ASC NULLS FIRST], true
      +- 'Aggregate ['i_brand, 'i_brand_id, 'i_manufact_id, 'i_manufact], ['i_brand_id AS brand_id#0, 'i_brand AS brand#1, 'i_manufact_id, 'i_manufact, 'sum('ss_ext_sales_price) AS ext_price#2]
         +- 'Filter ((((('d_date_sk = 'ss_sold_date_sk) && ('ss_item_sk = 'i_item_sk)) && ('i_manager_id = 8)) && (('d_moy = 11) && ('d_year = 1998))) && ((('ss_customer_sk = 'c_customer_sk) && ('c_current_addr_sk = 'ca_address_sk)) && (NOT ('substr('ca_zip, 1, 5) = 'substr('s_zip, 1, 5)) && ('ss_store_sk = 's_store_sk))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'UnresolvedRelation `date_dim`
               :  :  :  :  +- 'UnresolvedRelation `store_sales`
               :  :  :  +- 'UnresolvedRelation `item`
               :  :  +- 'UnresolvedRelation `customer`
               :  +- 'UnresolvedRelation `customer_address`
               +- 'UnresolvedRelation `store`

== Analyzed Logical Plan ==
brand_id: int, brand: string, i_manufact_id: int, i_manufact: string, ext_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [brand_id#0, brand#1, i_manufact_id#69, i_manufact#70, ext_price#2]
      +- Sort [ext_price#2 DESC NULLS LAST, brand#1 ASC NULLS FIRST, brand_id#0 ASC NULLS FIRST, i_manufact_id#69 ASC NULLS FIRST, i_manufact#70 ASC NULLS FIRST], true
         +- Aggregate [i_brand#64, i_brand_id#63, i_manufact_id#69, i_manufact#70], [i_brand_id#63 AS brand_id#0, i_brand#64 AS brand#1, i_manufact_id#69, i_manufact#70, sum(ss_ext_sales_price#48) AS ext_price#2]
            +- Filter (((((d_date_sk#5 = ss_sold_date_sk#33) && (ss_item_sk#35 = i_item_sk#56)) && (i_manager_id#76 = 8)) && ((d_moy#13 = 11) && (d_year#11 = 1998))) && (((ss_customer_sk#36 = c_customer_sk#78) && (c_current_addr_sk#82 = ca_address_sk#96)) && (NOT (substring(ca_zip#105, 1, 5) = substring(s_zip#134, 1, 5)) && (ss_store_sk#40 = s_store_sk#109))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- SubqueryAlias `tpcds`.`date_dim`
                  :  :  :  :  :  +- Relation[d_date_sk#5,d_date_id#6,d_date#7,d_month_seq#8,d_week_seq#9,d_quarter_seq#10,d_year#11,d_dow#12,d_moy#13,d_dom#14,d_qoy#15,d_fy_year#16,d_fy_quarter_seq#17,d_fy_week_seq#18,d_day_name#19,d_quarter_name#20,d_holiday#21,d_weekend#22,d_following_holiday#23,d_first_dom#24,d_last_dom#25,d_same_day_ly#26,d_same_day_lq#27,d_current_day#28,... 4 more fields] parquet
                  :  :  :  :  +- SubqueryAlias `tpcds`.`store_sales`
                  :  :  :  :     +- Relation[ss_sold_date_sk#33,ss_sold_time_sk#34,ss_item_sk#35,ss_customer_sk#36,ss_cdemo_sk#37,ss_hdemo_sk#38,ss_addr_sk#39,ss_store_sk#40,ss_promo_sk#41,ss_ticket_number#42,ss_quantity#43,ss_wholesale_cost#44,ss_list_price#45,ss_sales_price#46,ss_ext_discount_amt#47,ss_ext_sales_price#48,ss_ext_wholesale_cost#49,ss_ext_list_price#50,ss_ext_tax#51,ss_coupon_amt#52,ss_net_paid#53,ss_net_paid_inc_tax#54,ss_net_profit#55] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`item`
                  :  :  :     +- Relation[i_item_sk#56,i_item_id#57,i_rec_start_date#58,i_rec_end_date#59,i_item_desc#60,i_current_price#61,i_wholesale_cost#62,i_brand_id#63,i_brand#64,i_class_id#65,i_class#66,i_category_id#67,i_category#68,i_manufact_id#69,i_manufact#70,i_size#71,i_formulation#72,i_color#73,i_units#74,i_container#75,i_manager_id#76,i_product_name#77] parquet
                  :  :  +- SubqueryAlias `tpcds`.`customer`
                  :  :     +- Relation[c_customer_sk#78,c_customer_id#79,c_current_cdemo_sk#80,c_current_hdemo_sk#81,c_current_addr_sk#82,c_first_shipto_date_sk#83,c_first_sales_date_sk#84,c_salutation#85,c_first_name#86,c_last_name#87,c_preferred_cust_flag#88,c_birth_day#89,c_birth_month#90,c_birth_year#91,c_birth_country#92,c_login#93,c_email_address#94,c_last_review_date#95] parquet
                  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :     +- Relation[ca_address_sk#96,ca_address_id#97,ca_street_number#98,ca_street_name#99,ca_street_type#100,ca_suite_number#101,ca_city#102,ca_county#103,ca_state#104,ca_zip#105,ca_country#106,ca_gmt_offset#107,ca_location_type#108] parquet
                  +- SubqueryAlias `tpcds`.`store`
                     +- Relation[s_store_sk#109,s_store_id#110,s_rec_start_date#111,s_rec_end_date#112,s_closed_date_sk#113,s_store_name#114,s_number_employees#115,s_floor_space#116,s_hours#117,s_manager#118,s_market_id#119,s_geography_class#120,s_market_desc#121,s_market_manager#122,s_division_id#123,s_division_name#124,s_company_id#125,s_company_name#126,s_street_number#127,s_street_name#128,s_street_type#129,s_suite_number#130,s_city#131,s_county#132,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ext_price#2 DESC NULLS LAST, brand#1 ASC NULLS FIRST, brand_id#0 ASC NULLS FIRST, i_manufact_id#69 ASC NULLS FIRST, i_manufact#70 ASC NULLS FIRST], true
      +- Aggregate [i_brand#64, i_brand_id#63, i_manufact_id#69, i_manufact#70], [i_brand_id#63 AS brand_id#0, i_brand#64 AS brand#1, i_manufact_id#69, i_manufact#70, sum(ss_ext_sales_price#48) AS ext_price#2]
         +- Project [ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70]
            +- Join Inner, (NOT (substring(ca_zip#105, 1, 5) = substring(s_zip#134, 1, 5)) && (ss_store_sk#40 = s_store_sk#109))
               :- Project [ss_store_sk#40, ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70, ca_zip#105]
               :  +- Join Inner, (c_current_addr_sk#82 = ca_address_sk#96)
               :     :- Project [ss_store_sk#40, ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70, c_current_addr_sk#82]
               :     :  +- Join Inner, (ss_customer_sk#36 = c_customer_sk#78)
               :     :     :- Project [ss_customer_sk#36, ss_store_sk#40, ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70]
               :     :     :  +- Join Inner, (ss_item_sk#35 = i_item_sk#56)
               :     :     :     :- Project [ss_item_sk#35, ss_customer_sk#36, ss_store_sk#40, ss_ext_sales_price#48]
               :     :     :     :  +- Join Inner, (d_date_sk#5 = ss_sold_date_sk#33)
               :     :     :     :     :- Project [d_date_sk#5]
               :     :     :     :     :  +- Filter ((((isnotnull(d_moy#13) && isnotnull(d_year#11)) && (d_moy#13 = 11)) && (d_year#11 = 1998)) && isnotnull(d_date_sk#5))
               :     :     :     :     :     +- Relation[d_date_sk#5,d_date_id#6,d_date#7,d_month_seq#8,d_week_seq#9,d_quarter_seq#10,d_year#11,d_dow#12,d_moy#13,d_dom#14,d_qoy#15,d_fy_year#16,d_fy_quarter_seq#17,d_fy_week_seq#18,d_day_name#19,d_quarter_name#20,d_holiday#21,d_weekend#22,d_following_holiday#23,d_first_dom#24,d_last_dom#25,d_same_day_ly#26,d_same_day_lq#27,d_current_day#28,... 4 more fields] parquet
               :     :     :     :     +- Project [ss_sold_date_sk#33, ss_item_sk#35, ss_customer_sk#36, ss_store_sk#40, ss_ext_sales_price#48]
               :     :     :     :        +- Filter (((isnotnull(ss_sold_date_sk#33) && isnotnull(ss_item_sk#35)) && isnotnull(ss_customer_sk#36)) && isnotnull(ss_store_sk#40))
               :     :     :     :           +- Relation[ss_sold_date_sk#33,ss_sold_time_sk#34,ss_item_sk#35,ss_customer_sk#36,ss_cdemo_sk#37,ss_hdemo_sk#38,ss_addr_sk#39,ss_store_sk#40,ss_promo_sk#41,ss_ticket_number#42,ss_quantity#43,ss_wholesale_cost#44,ss_list_price#45,ss_sales_price#46,ss_ext_discount_amt#47,ss_ext_sales_price#48,ss_ext_wholesale_cost#49,ss_ext_list_price#50,ss_ext_tax#51,ss_coupon_amt#52,ss_net_paid#53,ss_net_paid_inc_tax#54,ss_net_profit#55] parquet
               :     :     :     +- Project [i_item_sk#56, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70]
               :     :     :        +- Filter ((isnotnull(i_manager_id#76) && (i_manager_id#76 = 8)) && isnotnull(i_item_sk#56))
               :     :     :           +- Relation[i_item_sk#56,i_item_id#57,i_rec_start_date#58,i_rec_end_date#59,i_item_desc#60,i_current_price#61,i_wholesale_cost#62,i_brand_id#63,i_brand#64,i_class_id#65,i_class#66,i_category_id#67,i_category#68,i_manufact_id#69,i_manufact#70,i_size#71,i_formulation#72,i_color#73,i_units#74,i_container#75,i_manager_id#76,i_product_name#77] parquet
               :     :     +- Project [c_customer_sk#78, c_current_addr_sk#82]
               :     :        +- Filter (isnotnull(c_customer_sk#78) && isnotnull(c_current_addr_sk#82))
               :     :           +- Relation[c_customer_sk#78,c_customer_id#79,c_current_cdemo_sk#80,c_current_hdemo_sk#81,c_current_addr_sk#82,c_first_shipto_date_sk#83,c_first_sales_date_sk#84,c_salutation#85,c_first_name#86,c_last_name#87,c_preferred_cust_flag#88,c_birth_day#89,c_birth_month#90,c_birth_year#91,c_birth_country#92,c_login#93,c_email_address#94,c_last_review_date#95] parquet
               :     +- Project [ca_address_sk#96, ca_zip#105]
               :        +- Filter (isnotnull(ca_address_sk#96) && isnotnull(ca_zip#105))
               :           +- Relation[ca_address_sk#96,ca_address_id#97,ca_street_number#98,ca_street_name#99,ca_street_type#100,ca_suite_number#101,ca_city#102,ca_county#103,ca_state#104,ca_zip#105,ca_country#106,ca_gmt_offset#107,ca_location_type#108] parquet
               +- Project [s_store_sk#109, s_zip#134]
                  +- Filter (isnotnull(s_zip#134) && isnotnull(s_store_sk#109))
                     +- Relation[s_store_sk#109,s_store_id#110,s_rec_start_date#111,s_rec_end_date#112,s_closed_date_sk#113,s_store_name#114,s_number_employees#115,s_floor_space#116,s_hours#117,s_manager#118,s_market_id#119,s_geography_class#120,s_market_desc#121,s_market_manager#122,s_division_id#123,s_division_name#124,s_company_id#125,s_company_name#126,s_street_number#127,s_street_name#128,s_street_type#129,s_suite_number#130,s_city#131,s_county#132,... 5 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[ext_price#2 DESC NULLS LAST,brand#1 ASC NULLS FIRST,brand_id#0 ASC NULLS FIRST,i_manufact_id#69 ASC NULLS FIRST,i_manufact#70 ASC NULLS FIRST], output=[brand_id#0,brand#1,i_manufact_id#69,i_manufact#70,ext_price#2])
+- *(7) HashAggregate(keys=[i_brand#64, i_brand_id#63, i_manufact_id#69, i_manufact#70], functions=[sum(ss_ext_sales_price#48)], output=[brand_id#0, brand#1, i_manufact_id#69, i_manufact#70, ext_price#2])
   +- Exchange hashpartitioning(i_brand#64, i_brand_id#63, i_manufact_id#69, i_manufact#70, 200)
      +- *(6) HashAggregate(keys=[i_brand#64, i_brand_id#63, i_manufact_id#69, i_manufact#70], functions=[partial_sum(ss_ext_sales_price#48)], output=[i_brand#64, i_brand_id#63, i_manufact_id#69, i_manufact#70, sum#147])
         +- *(6) Project [ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70]
            +- *(6) BroadcastHashJoin [ss_store_sk#40], [s_store_sk#109], Inner, BuildRight, NOT (substring(ca_zip#105, 1, 5) = substring(s_zip#134, 1, 5))
               :- *(6) Project [ss_store_sk#40, ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70, ca_zip#105]
               :  +- *(6) BroadcastHashJoin [c_current_addr_sk#82], [ca_address_sk#96], Inner, BuildRight
               :     :- *(6) Project [ss_store_sk#40, ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70, c_current_addr_sk#82]
               :     :  +- *(6) BroadcastHashJoin [ss_customer_sk#36], [c_customer_sk#78], Inner, BuildRight
               :     :     :- *(6) Project [ss_customer_sk#36, ss_store_sk#40, ss_ext_sales_price#48, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70]
               :     :     :  +- *(6) BroadcastHashJoin [ss_item_sk#35], [i_item_sk#56], Inner, BuildRight
               :     :     :     :- *(6) Project [ss_item_sk#35, ss_customer_sk#36, ss_store_sk#40, ss_ext_sales_price#48]
               :     :     :     :  +- *(6) BroadcastHashJoin [d_date_sk#5], [ss_sold_date_sk#33], Inner, BuildLeft
               :     :     :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :     :  +- *(1) Project [d_date_sk#5]
               :     :     :     :     :     +- *(1) Filter ((((isnotnull(d_moy#13) && isnotnull(d_year#11)) && (d_moy#13 = 11)) && (d_year#11 = 1998)) && isnotnull(d_date_sk#5))
               :     :     :     :     :        +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#5,d_year#11,d_moy#13] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,1998), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :     :     :     :     +- *(6) Project [ss_sold_date_sk#33, ss_item_sk#35, ss_customer_sk#36, ss_store_sk#40, ss_ext_sales_price#48]
               :     :     :     :        +- *(6) Filter (((isnotnull(ss_sold_date_sk#33) && isnotnull(ss_item_sk#35)) && isnotnull(ss_customer_sk#36)) && isnotnull(ss_store_sk#40))
               :     :     :     :           +- *(6) FileScan parquet tpcds.store_sales[ss_sold_date_sk#33,ss_item_sk#35,ss_customer_sk#36,ss_store_sk#40,ss_ext_sales_price#48] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk), IsNotNull(ss_customer_sk), IsNotNull(ss_store..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ext_sales_price:d...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(2) Project [i_item_sk#56, i_brand_id#63, i_brand#64, i_manufact_id#69, i_manufact#70]
               :     :     :           +- *(2) Filter ((isnotnull(i_manager_id#76) && (i_manager_id#76 = 8)) && isnotnull(i_item_sk#56))
               :     :     :              +- *(2) FileScan parquet tpcds.item[i_item_sk#56,i_brand_id#63,i_brand#64,i_manufact_id#69,i_manufact#70,i_manager_id#76] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manager_id), EqualTo(i_manager_id,8), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_brand:string,i_manufact_id:int,i_manufact:string,i_manager_...
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(3) Project [c_customer_sk#78, c_current_addr_sk#82]
               :     :           +- *(3) Filter (isnotnull(c_customer_sk#78) && isnotnull(c_current_addr_sk#82))
               :     :              +- *(3) FileScan parquet tpcds.customer[c_customer_sk#78,c_current_addr_sk#82] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(4) Project [ca_address_sk#96, ca_zip#105]
               :           +- *(4) Filter (isnotnull(ca_address_sk#96) && isnotnull(ca_zip#105))
               :              +- *(4) FileScan parquet tpcds.customer_address[ca_address_sk#96,ca_zip#105] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_zip)], ReadSchema: struct<ca_address_sk:int,ca_zip:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(5) Project [s_store_sk#109, s_zip#134]
                     +- *(5) Filter (isnotnull(s_zip#134) && isnotnull(s_store_sk#109))
                        +- *(5) FileScan parquet tpcds.store[s_store_sk#109,s_zip#134] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_zip), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_zip:string>
Time taken: 4.091 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 19 in stream 0 using template query19.tpl
------------------------------------------------------^^^

