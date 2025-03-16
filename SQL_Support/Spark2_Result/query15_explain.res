== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['ca_zip ASC NULLS FIRST], true
      +- 'Aggregate ['ca_zip], ['ca_zip, unresolvedalias('sum('cs_sales_price), None)]
         +- 'Filter (((('cs_bill_customer_sk = 'c_customer_sk) && ('c_current_addr_sk = 'ca_address_sk)) && (('substr('ca_zip, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || 'ca_state IN (CA,WA,GA)) || ('cs_sales_price > 500))) && ((('cs_sold_date_sk = 'd_date_sk) && ('d_qoy = 2)) && ('d_year = 2001)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation `catalog_sales`
               :  :  +- 'UnresolvedRelation `customer`
               :  +- 'UnresolvedRelation `customer_address`
               +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
ca_zip: string, sum(cs_sales_price): double
GlobalLimit 100
+- LocalLimit 100
   +- Project [ca_zip#63, sum(cs_sales_price)#96]
      +- Sort [ca_zip#63 ASC NULLS FIRST], true
         +- Aggregate [ca_zip#63], [ca_zip#63, sum(cs_sales_price#23) AS sum(cs_sales_price)#96]
            +- Filter ((((cs_bill_customer_sk#5 = c_customer_sk#36) && (c_current_addr_sk#40 = ca_address_sk#54)) && ((substring(ca_zip#63, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || ca_state#62 IN (CA,WA,GA)) || (cs_sales_price#23 > cast(500 as double)))) && (((cs_sold_date_sk#2 = d_date_sk#67) && (d_qoy#77 = 2)) && (d_year#73 = 2001)))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
                  :  :  :  +- Relation[cs_sold_date_sk#2,cs_sold_time_sk#3,cs_ship_date_sk#4,cs_bill_customer_sk#5,cs_bill_cdemo_sk#6,cs_bill_hdemo_sk#7,cs_bill_addr_sk#8,cs_ship_customer_sk#9,cs_ship_cdemo_sk#10,cs_ship_hdemo_sk#11,cs_ship_addr_sk#12,cs_call_center_sk#13,cs_catalog_page_sk#14,cs_ship_mode_sk#15,cs_warehouse_sk#16,cs_item_sk#17,cs_promo_sk#18,cs_order_number#19,cs_quantity#20,cs_wholesale_cost#21,cs_list_price#22,cs_sales_price#23,cs_ext_discount_amt#24,cs_ext_sales_price#25,... 10 more fields] parquet
                  :  :  +- SubqueryAlias `tpcds`.`customer`
                  :  :     +- Relation[c_customer_sk#36,c_customer_id#37,c_current_cdemo_sk#38,c_current_hdemo_sk#39,c_current_addr_sk#40,c_first_shipto_date_sk#41,c_first_sales_date_sk#42,c_salutation#43,c_first_name#44,c_last_name#45,c_preferred_cust_flag#46,c_birth_day#47,c_birth_month#48,c_birth_year#49,c_birth_country#50,c_login#51,c_email_address#52,c_last_review_date#53] parquet
                  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :     +- Relation[ca_address_sk#54,ca_address_id#55,ca_street_number#56,ca_street_name#57,ca_street_type#58,ca_suite_number#59,ca_city#60,ca_county#61,ca_state#62,ca_zip#63,ca_country#64,ca_gmt_offset#65,ca_location_type#66] parquet
                  +- SubqueryAlias `tpcds`.`date_dim`
                     +- Relation[d_date_sk#67,d_date_id#68,d_date#69,d_month_seq#70,d_week_seq#71,d_quarter_seq#72,d_year#73,d_dow#74,d_moy#75,d_dom#76,d_qoy#77,d_fy_year#78,d_fy_quarter_seq#79,d_fy_week_seq#80,d_day_name#81,d_quarter_name#82,d_holiday#83,d_weekend#84,d_following_holiday#85,d_first_dom#86,d_last_dom#87,d_same_day_ly#88,d_same_day_lq#89,d_current_day#90,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ca_zip#63 ASC NULLS FIRST], true
      +- Aggregate [ca_zip#63], [ca_zip#63, sum(cs_sales_price#23) AS sum(cs_sales_price)#96]
         +- Project [cs_sales_price#23, ca_zip#63]
            +- Join Inner, (cs_sold_date_sk#2 = d_date_sk#67)
               :- Project [cs_sold_date_sk#2, cs_sales_price#23, ca_zip#63]
               :  +- Join Inner, ((c_current_addr_sk#40 = ca_address_sk#54) && ((substring(ca_zip#63, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || ca_state#62 IN (CA,WA,GA)) || (cs_sales_price#23 > 500.0)))
               :     :- Project [cs_sold_date_sk#2, cs_sales_price#23, c_current_addr_sk#40]
               :     :  +- Join Inner, (cs_bill_customer_sk#5 = c_customer_sk#36)
               :     :     :- Project [cs_sold_date_sk#2, cs_bill_customer_sk#5, cs_sales_price#23]
               :     :     :  +- Filter (isnotnull(cs_bill_customer_sk#5) && isnotnull(cs_sold_date_sk#2))
               :     :     :     +- Relation[cs_sold_date_sk#2,cs_sold_time_sk#3,cs_ship_date_sk#4,cs_bill_customer_sk#5,cs_bill_cdemo_sk#6,cs_bill_hdemo_sk#7,cs_bill_addr_sk#8,cs_ship_customer_sk#9,cs_ship_cdemo_sk#10,cs_ship_hdemo_sk#11,cs_ship_addr_sk#12,cs_call_center_sk#13,cs_catalog_page_sk#14,cs_ship_mode_sk#15,cs_warehouse_sk#16,cs_item_sk#17,cs_promo_sk#18,cs_order_number#19,cs_quantity#20,cs_wholesale_cost#21,cs_list_price#22,cs_sales_price#23,cs_ext_discount_amt#24,cs_ext_sales_price#25,... 10 more fields] parquet
               :     :     +- Project [c_customer_sk#36, c_current_addr_sk#40]
               :     :        +- Filter (isnotnull(c_customer_sk#36) && isnotnull(c_current_addr_sk#40))
               :     :           +- Relation[c_customer_sk#36,c_customer_id#37,c_current_cdemo_sk#38,c_current_hdemo_sk#39,c_current_addr_sk#40,c_first_shipto_date_sk#41,c_first_sales_date_sk#42,c_salutation#43,c_first_name#44,c_last_name#45,c_preferred_cust_flag#46,c_birth_day#47,c_birth_month#48,c_birth_year#49,c_birth_country#50,c_login#51,c_email_address#52,c_last_review_date#53] parquet
               :     +- Project [ca_address_sk#54, ca_state#62, ca_zip#63]
               :        +- Filter isnotnull(ca_address_sk#54)
               :           +- Relation[ca_address_sk#54,ca_address_id#55,ca_street_number#56,ca_street_name#57,ca_street_type#58,ca_suite_number#59,ca_city#60,ca_county#61,ca_state#62,ca_zip#63,ca_country#64,ca_gmt_offset#65,ca_location_type#66] parquet
               +- Project [d_date_sk#67]
                  +- Filter ((((isnotnull(d_qoy#77) && isnotnull(d_year#73)) && (d_qoy#77 = 2)) && (d_year#73 = 2001)) && isnotnull(d_date_sk#67))
                     +- Relation[d_date_sk#67,d_date_id#68,d_date#69,d_month_seq#70,d_week_seq#71,d_quarter_seq#72,d_year#73,d_dow#74,d_moy#75,d_dom#76,d_qoy#77,d_fy_year#78,d_fy_quarter_seq#79,d_fy_week_seq#80,d_day_name#81,d_quarter_name#82,d_holiday#83,d_weekend#84,d_following_holiday#85,d_first_dom#86,d_last_dom#87,d_same_day_ly#88,d_same_day_lq#89,d_current_day#90,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[ca_zip#63 ASC NULLS FIRST], output=[ca_zip#63,sum(cs_sales_price)#96])
+- *(5) HashAggregate(keys=[ca_zip#63], functions=[sum(cs_sales_price#23)], output=[ca_zip#63, sum(cs_sales_price)#96])
   +- Exchange hashpartitioning(ca_zip#63, 200)
      +- *(4) HashAggregate(keys=[ca_zip#63], functions=[partial_sum(cs_sales_price#23)], output=[ca_zip#63, sum#99])
         +- *(4) Project [cs_sales_price#23, ca_zip#63]
            +- *(4) BroadcastHashJoin [cs_sold_date_sk#2], [d_date_sk#67], Inner, BuildRight
               :- *(4) Project [cs_sold_date_sk#2, cs_sales_price#23, ca_zip#63]
               :  +- *(4) BroadcastHashJoin [c_current_addr_sk#40], [ca_address_sk#54], Inner, BuildRight, ((substring(ca_zip#63, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) || ca_state#62 IN (CA,WA,GA)) || (cs_sales_price#23 > 500.0))
               :     :- *(4) Project [cs_sold_date_sk#2, cs_sales_price#23, c_current_addr_sk#40]
               :     :  +- *(4) BroadcastHashJoin [cs_bill_customer_sk#5], [c_customer_sk#36], Inner, BuildRight
               :     :     :- *(4) Project [cs_sold_date_sk#2, cs_bill_customer_sk#5, cs_sales_price#23]
               :     :     :  +- *(4) Filter (isnotnull(cs_bill_customer_sk#5) && isnotnull(cs_sold_date_sk#2))
               :     :     :     +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#2,cs_bill_customer_sk#5,cs_sales_price#23] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_bill_customer_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_sales_price:double>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(1) Project [c_customer_sk#36, c_current_addr_sk#40]
               :     :           +- *(1) Filter (isnotnull(c_customer_sk#36) && isnotnull(c_current_addr_sk#40))
               :     :              +- *(1) FileScan parquet tpcds.customer[c_customer_sk#36,c_current_addr_sk#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(2) Project [ca_address_sk#54, ca_state#62, ca_zip#63]
               :           +- *(2) Filter isnotnull(ca_address_sk#54)
               :              +- *(2) FileScan parquet tpcds.customer_address[ca_address_sk#54,ca_state#62,ca_zip#63] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_zip:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(3) Project [d_date_sk#67]
                     +- *(3) Filter ((((isnotnull(d_qoy#77) && isnotnull(d_year#73)) && (d_qoy#77 = 2)) && (d_year#73 = 2001)) && isnotnull(d_date_sk#67))
                        +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#67,d_year#73,d_qoy#77] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
Time taken: 3.917 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 15 in stream 0 using template query15.tpl
------------------------------------------------------^^^

