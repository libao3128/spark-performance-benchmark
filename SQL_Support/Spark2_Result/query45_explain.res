== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['ca_zip ASC NULLS FIRST, 'ca_city ASC NULLS FIRST], true
      +- 'Aggregate ['ca_zip, 'ca_city], ['ca_zip, 'ca_city, unresolvedalias('sum('ws_sales_price), None)]
         +- 'Filter (((('ws_bill_customer_sk = 'c_customer_sk) && ('c_current_addr_sk = 'ca_address_sk)) && (('ws_item_sk = 'i_item_sk) && ('substr('ca_zip, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || 'i_item_id IN (list#0 [])))) && ((('ws_sold_date_sk = 'd_date_sk) && ('d_qoy = 2)) && ('d_year = 2001)))
            :  +- 'Project ['i_item_id]
            :     +- 'Filter 'i_item_sk IN (2,3,5,7,11,13,17,19,23,29)
            :        +- 'UnresolvedRelation `item`
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation `web_sales`
               :  :  :  +- 'UnresolvedRelation `customer`
               :  :  +- 'UnresolvedRelation `customer_address`
               :  +- 'UnresolvedRelation `date_dim`
               +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
ca_zip: string, ca_city: string, sum(ws_sales_price): double
GlobalLimit 100
+- LocalLimit 100
   +- Project [ca_zip#64, ca_city#61, sum(ws_sales_price)#119]
      +- Sort [ca_zip#64 ASC NULLS FIRST, ca_city#61 ASC NULLS FIRST], true
         +- Aggregate [ca_zip#64, ca_city#61], [ca_zip#64, ca_city#61, sum(ws_sales_price#24) AS sum(ws_sales_price)#119]
            +- Filter ((((ws_bill_customer_sk#7 = c_customer_sk#37) && (c_current_addr_sk#41 = ca_address_sk#55)) && ((ws_item_sk#6 = i_item_sk#96) && (substring(ca_zip#64, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || i_item_id#97 IN (list#0 [])))) && (((ws_sold_date_sk#3 = d_date_sk#68) && (d_qoy#78 = 2)) && (d_year#74 = 2001)))
               :  +- Project [i_item_id#97]
               :     +- Filter i_item_sk#96 IN (2,3,5,7,11,13,17,19,23,29)
               :        +- SubqueryAlias `tpcds`.`item`
               :           +- Relation[i_item_sk#96,i_item_id#97,i_rec_start_date#98,i_rec_end_date#99,i_item_desc#100,i_current_price#101,i_wholesale_cost#102,i_brand_id#103,i_brand#104,i_class_id#105,i_class#106,i_category_id#107,i_category#108,i_manufact_id#109,i_manufact#110,i_size#111,i_formulation#112,i_color#113,i_units#114,i_container#115,i_manager_id#116,i_product_name#117] parquet
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- SubqueryAlias `tpcds`.`web_sales`
                  :  :  :  :  +- Relation[ws_sold_date_sk#3,ws_sold_time_sk#4,ws_ship_date_sk#5,ws_item_sk#6,ws_bill_customer_sk#7,ws_bill_cdemo_sk#8,ws_bill_hdemo_sk#9,ws_bill_addr_sk#10,ws_ship_customer_sk#11,ws_ship_cdemo_sk#12,ws_ship_hdemo_sk#13,ws_ship_addr_sk#14,ws_web_page_sk#15,ws_web_site_sk#16,ws_ship_mode_sk#17,ws_warehouse_sk#18,ws_promo_sk#19,ws_order_number#20,ws_quantity#21,ws_wholesale_cost#22,ws_list_price#23,ws_sales_price#24,ws_ext_discount_amt#25,ws_ext_sales_price#26,... 10 more fields] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`customer`
                  :  :  :     +- Relation[c_customer_sk#37,c_customer_id#38,c_current_cdemo_sk#39,c_current_hdemo_sk#40,c_current_addr_sk#41,c_first_shipto_date_sk#42,c_first_sales_date_sk#43,c_salutation#44,c_first_name#45,c_last_name#46,c_preferred_cust_flag#47,c_birth_day#48,c_birth_month#49,c_birth_year#50,c_birth_country#51,c_login#52,c_email_address#53,c_last_review_date#54] parquet
                  :  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :  :     +- Relation[ca_address_sk#55,ca_address_id#56,ca_street_number#57,ca_street_name#58,ca_street_type#59,ca_suite_number#60,ca_city#61,ca_county#62,ca_state#63,ca_zip#64,ca_country#65,ca_gmt_offset#66,ca_location_type#67] parquet
                  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :     +- Relation[d_date_sk#68,d_date_id#69,d_date#70,d_month_seq#71,d_week_seq#72,d_quarter_seq#73,d_year#74,d_dow#75,d_moy#76,d_dom#77,d_qoy#78,d_fy_year#79,d_fy_quarter_seq#80,d_fy_week_seq#81,d_day_name#82,d_quarter_name#83,d_holiday#84,d_weekend#85,d_following_holiday#86,d_first_dom#87,d_last_dom#88,d_same_day_ly#89,d_same_day_lq#90,d_current_day#91,... 4 more fields] parquet
                  +- SubqueryAlias `tpcds`.`item`
                     +- Relation[i_item_sk#96,i_item_id#97,i_rec_start_date#98,i_rec_end_date#99,i_item_desc#100,i_current_price#101,i_wholesale_cost#102,i_brand_id#103,i_brand#104,i_class_id#105,i_class#106,i_category_id#107,i_category#108,i_manufact_id#109,i_manufact#110,i_size#111,i_formulation#112,i_color#113,i_units#114,i_container#115,i_manager_id#116,i_product_name#117] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ca_zip#64 ASC NULLS FIRST, ca_city#61 ASC NULLS FIRST], true
      +- Aggregate [ca_zip#64, ca_city#61], [ca_zip#64, ca_city#61, sum(ws_sales_price#24) AS sum(ws_sales_price)#119]
         +- Project [ws_sales_price#24, ca_city#61, ca_zip#64]
            +- Filter (substring(ca_zip#64, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || exists#123)
               +- Join ExistenceJoin(exists#123), (i_item_id#97 = i_item_id#97#122)
                  :- Project [ws_sales_price#24, ca_city#61, ca_zip#64, i_item_id#97]
                  :  +- Join Inner, (ws_item_sk#6 = i_item_sk#96)
                  :     :- Project [ws_item_sk#6, ws_sales_price#24, ca_city#61, ca_zip#64]
                  :     :  +- Join Inner, (ws_sold_date_sk#3 = d_date_sk#68)
                  :     :     :- Project [ws_sold_date_sk#3, ws_item_sk#6, ws_sales_price#24, ca_city#61, ca_zip#64]
                  :     :     :  +- Join Inner, (c_current_addr_sk#41 = ca_address_sk#55)
                  :     :     :     :- Project [ws_sold_date_sk#3, ws_item_sk#6, ws_sales_price#24, c_current_addr_sk#41]
                  :     :     :     :  +- Join Inner, (ws_bill_customer_sk#7 = c_customer_sk#37)
                  :     :     :     :     :- Project [ws_sold_date_sk#3, ws_item_sk#6, ws_bill_customer_sk#7, ws_sales_price#24]
                  :     :     :     :     :  +- Filter ((isnotnull(ws_bill_customer_sk#7) && isnotnull(ws_sold_date_sk#3)) && isnotnull(ws_item_sk#6))
                  :     :     :     :     :     +- Relation[ws_sold_date_sk#3,ws_sold_time_sk#4,ws_ship_date_sk#5,ws_item_sk#6,ws_bill_customer_sk#7,ws_bill_cdemo_sk#8,ws_bill_hdemo_sk#9,ws_bill_addr_sk#10,ws_ship_customer_sk#11,ws_ship_cdemo_sk#12,ws_ship_hdemo_sk#13,ws_ship_addr_sk#14,ws_web_page_sk#15,ws_web_site_sk#16,ws_ship_mode_sk#17,ws_warehouse_sk#18,ws_promo_sk#19,ws_order_number#20,ws_quantity#21,ws_wholesale_cost#22,ws_list_price#23,ws_sales_price#24,ws_ext_discount_amt#25,ws_ext_sales_price#26,... 10 more fields] parquet
                  :     :     :     :     +- Project [c_customer_sk#37, c_current_addr_sk#41]
                  :     :     :     :        +- Filter (isnotnull(c_customer_sk#37) && isnotnull(c_current_addr_sk#41))
                  :     :     :     :           +- Relation[c_customer_sk#37,c_customer_id#38,c_current_cdemo_sk#39,c_current_hdemo_sk#40,c_current_addr_sk#41,c_first_shipto_date_sk#42,c_first_sales_date_sk#43,c_salutation#44,c_first_name#45,c_last_name#46,c_preferred_cust_flag#47,c_birth_day#48,c_birth_month#49,c_birth_year#50,c_birth_country#51,c_login#52,c_email_address#53,c_last_review_date#54] parquet
                  :     :     :     +- Project [ca_address_sk#55, ca_city#61, ca_zip#64]
                  :     :     :        +- Filter isnotnull(ca_address_sk#55)
                  :     :     :           +- Relation[ca_address_sk#55,ca_address_id#56,ca_street_number#57,ca_street_name#58,ca_street_type#59,ca_suite_number#60,ca_city#61,ca_county#62,ca_state#63,ca_zip#64,ca_country#65,ca_gmt_offset#66,ca_location_type#67] parquet
                  :     :     +- Project [d_date_sk#68]
                  :     :        +- Filter ((((isnotnull(d_qoy#78) && isnotnull(d_year#74)) && (d_qoy#78 = 2)) && (d_year#74 = 2001)) && isnotnull(d_date_sk#68))
                  :     :           +- Relation[d_date_sk#68,d_date_id#69,d_date#70,d_month_seq#71,d_week_seq#72,d_quarter_seq#73,d_year#74,d_dow#75,d_moy#76,d_dom#77,d_qoy#78,d_fy_year#79,d_fy_quarter_seq#80,d_fy_week_seq#81,d_day_name#82,d_quarter_name#83,d_holiday#84,d_weekend#85,d_following_holiday#86,d_first_dom#87,d_last_dom#88,d_same_day_ly#89,d_same_day_lq#90,d_current_day#91,... 4 more fields] parquet
                  :     +- Project [i_item_sk#96, i_item_id#97]
                  :        +- Filter isnotnull(i_item_sk#96)
                  :           +- Relation[i_item_sk#96,i_item_id#97,i_rec_start_date#98,i_rec_end_date#99,i_item_desc#100,i_current_price#101,i_wholesale_cost#102,i_brand_id#103,i_brand#104,i_class_id#105,i_class#106,i_category_id#107,i_category#108,i_manufact_id#109,i_manufact#110,i_size#111,i_formulation#112,i_color#113,i_units#114,i_container#115,i_manager_id#116,i_product_name#117] parquet
                  +- Project [i_item_id#97 AS i_item_id#97#122]
                     +- Filter i_item_sk#96 IN (2,3,5,7,11,13,17,19,23,29)
                        +- Relation[i_item_sk#96,i_item_id#97,i_rec_start_date#98,i_rec_end_date#99,i_item_desc#100,i_current_price#101,i_wholesale_cost#102,i_brand_id#103,i_brand#104,i_class_id#105,i_class#106,i_category_id#107,i_category#108,i_manufact_id#109,i_manufact#110,i_size#111,i_formulation#112,i_color#113,i_units#114,i_container#115,i_manager_id#116,i_product_name#117] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[ca_zip#64 ASC NULLS FIRST,ca_city#61 ASC NULLS FIRST], output=[ca_zip#64,ca_city#61,sum(ws_sales_price)#119])
+- *(7) HashAggregate(keys=[ca_zip#64, ca_city#61], functions=[sum(ws_sales_price#24)], output=[ca_zip#64, ca_city#61, sum(ws_sales_price)#119])
   +- Exchange hashpartitioning(ca_zip#64, ca_city#61, 200)
      +- *(6) HashAggregate(keys=[ca_zip#64, ca_city#61], functions=[partial_sum(ws_sales_price#24)], output=[ca_zip#64, ca_city#61, sum#125])
         +- *(6) Project [ws_sales_price#24, ca_city#61, ca_zip#64]
            +- *(6) Filter (substring(ca_zip#64, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || exists#123)
               +- *(6) BroadcastHashJoin [i_item_id#97], [i_item_id#97#122], ExistenceJoin(exists#123), BuildRight
                  :- *(6) Project [ws_sales_price#24, ca_city#61, ca_zip#64, i_item_id#97]
                  :  +- *(6) BroadcastHashJoin [ws_item_sk#6], [i_item_sk#96], Inner, BuildRight
                  :     :- *(6) Project [ws_item_sk#6, ws_sales_price#24, ca_city#61, ca_zip#64]
                  :     :  +- *(6) BroadcastHashJoin [ws_sold_date_sk#3], [d_date_sk#68], Inner, BuildRight
                  :     :     :- *(6) Project [ws_sold_date_sk#3, ws_item_sk#6, ws_sales_price#24, ca_city#61, ca_zip#64]
                  :     :     :  +- *(6) BroadcastHashJoin [c_current_addr_sk#41], [ca_address_sk#55], Inner, BuildRight
                  :     :     :     :- *(6) Project [ws_sold_date_sk#3, ws_item_sk#6, ws_sales_price#24, c_current_addr_sk#41]
                  :     :     :     :  +- *(6) BroadcastHashJoin [ws_bill_customer_sk#7], [c_customer_sk#37], Inner, BuildRight
                  :     :     :     :     :- *(6) Project [ws_sold_date_sk#3, ws_item_sk#6, ws_bill_customer_sk#7, ws_sales_price#24]
                  :     :     :     :     :  +- *(6) Filter ((isnotnull(ws_bill_customer_sk#7) && isnotnull(ws_sold_date_sk#3)) && isnotnull(ws_item_sk#6))
                  :     :     :     :     :     +- *(6) FileScan parquet tpcds.web_sales[ws_sold_date_sk#3,ws_item_sk#6,ws_bill_customer_sk#7,ws_sales_price#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int,ws_sales_price:double>
                  :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :     :     :        +- *(1) Project [c_customer_sk#37, c_current_addr_sk#41]
                  :     :     :     :           +- *(1) Filter (isnotnull(c_customer_sk#37) && isnotnull(c_current_addr_sk#41))
                  :     :     :     :              +- *(1) FileScan parquet tpcds.customer[c_customer_sk#37,c_current_addr_sk#41] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :     :        +- *(2) Project [ca_address_sk#55, ca_city#61, ca_zip#64]
                  :     :     :           +- *(2) Filter isnotnull(ca_address_sk#55)
                  :     :     :              +- *(2) FileScan parquet tpcds.customer_address[ca_address_sk#55,ca_city#61,ca_zip#64] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_city:string,ca_zip:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :        +- *(3) Project [d_date_sk#68]
                  :     :           +- *(3) Filter ((((isnotnull(d_qoy#78) && isnotnull(d_year#74)) && (d_qoy#78 = 2)) && (d_year#74 = 2001)) && isnotnull(d_date_sk#68))
                  :     :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#68,d_year#74,d_qoy#78] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        +- *(4) Project [i_item_sk#96, i_item_id#97]
                  :           +- *(4) Filter isnotnull(i_item_sk#96)
                  :              +- *(4) FileScan parquet tpcds.item[i_item_sk#96,i_item_id#97] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
                     +- *(5) Project [i_item_id#97 AS i_item_id#97#122]
                        +- *(5) Filter i_item_sk#96 IN (2,3,5,7,11,13,17,19,23,29)
                           +- *(5) FileScan parquet tpcds.item[i_item_sk#96,i_item_id#97] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [In(i_item_sk, [2,3,5,7,11,13,17,19,23,29])], ReadSchema: struct<i_item_sk:int,i_item_id:string>
Time taken: 4.181 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 45 in stream 0 using template query45.tpl
------------------------------------------------------^^^

