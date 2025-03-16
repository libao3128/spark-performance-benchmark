Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580227118
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['ext_price DESC NULLS LAST, 'i_brand ASC NULLS FIRST, 'i_brand_id ASC NULLS FIRST, 'i_manufact_id ASC NULLS FIRST, 'i_manufact ASC NULLS FIRST], true
      +- 'Aggregate ['i_brand, 'i_brand_id, 'i_manufact_id, 'i_manufact], ['i_brand_id AS brand_id#0, 'i_brand AS brand#1, 'i_manufact_id, 'i_manufact, 'sum('ss_ext_sales_price) AS ext_price#2]
         +- 'Filter ((((('d_date_sk = 'ss_sold_date_sk) AND ('ss_item_sk = 'i_item_sk)) AND ('i_manager_id = 8)) AND (('d_moy = 11) AND ('d_year = 1998))) AND ((('ss_customer_sk = 'c_customer_sk) AND ('c_current_addr_sk = 'ca_address_sk)) AND (NOT ('substr('ca_zip, 1, 5) = 'substr('s_zip, 1, 5)) AND ('ss_store_sk = 's_store_sk))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'UnresolvedRelation [date_dim], [], false
               :  :  :  :  +- 'UnresolvedRelation [store_sales], [], false
               :  :  :  +- 'UnresolvedRelation [item], [], false
               :  :  +- 'UnresolvedRelation [customer], [], false
               :  +- 'UnresolvedRelation [customer_address], [], false
               +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
brand_id: int, brand: string, i_manufact_id: int, i_manufact: string, ext_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ext_price#2 DESC NULLS LAST, brand#1 ASC NULLS FIRST, brand_id#0 ASC NULLS FIRST, i_manufact_id#72 ASC NULLS FIRST, i_manufact#73 ASC NULLS FIRST], true
      +- Aggregate [i_brand#67, i_brand_id#66, i_manufact_id#72, i_manufact#73], [i_brand_id#66 AS brand_id#0, i_brand#67 AS brand#1, i_manufact_id#72, i_manufact#73, sum(ss_ext_sales_price#51) AS ext_price#2]
         +- Filter (((((d_date_sk#8 = ss_sold_date_sk#36) AND (ss_item_sk#38 = i_item_sk#59)) AND (i_manager_id#79 = 8)) AND ((d_moy#16 = 11) AND (d_year#14 = 1998))) AND (((ss_customer_sk#39 = c_customer_sk#81) AND (c_current_addr_sk#85 = ca_address_sk#99)) AND (NOT (substr(ca_zip#108, 1, 5) = substr(s_zip#137, 1, 5)) AND (ss_store_sk#43 = s_store_sk#112))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- Join Inner
               :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  :  :  :  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#8,d_date_id#9,d_date#10,d_month_seq#11,d_week_seq#12,d_quarter_seq#13,d_year#14,d_dow#15,d_moy#16,d_dom#17,d_qoy#18,d_fy_year#19,d_fy_quarter_seq#20,d_fy_week_seq#21,d_day_name#22,d_quarter_name#23,d_holiday#24,d_weekend#25,d_following_holiday#26,d_first_dom#27,d_last_dom#28,d_same_day_ly#29,d_same_day_lq#30,d_current_day#31,... 4 more fields] parquet
               :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :  :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#36,ss_sold_time_sk#37,ss_item_sk#38,ss_customer_sk#39,ss_cdemo_sk#40,ss_hdemo_sk#41,ss_addr_sk#42,ss_store_sk#43,ss_promo_sk#44,ss_ticket_number#45,ss_quantity#46,ss_wholesale_cost#47,ss_list_price#48,ss_sales_price#49,ss_ext_discount_amt#50,ss_ext_sales_price#51,ss_ext_wholesale_cost#52,ss_ext_list_price#53,ss_ext_tax#54,ss_coupon_amt#55,ss_net_paid#56,ss_net_paid_inc_tax#57,ss_net_profit#58] parquet
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.item
               :  :  :     +- Relation spark_catalog.tpcds.item[i_item_sk#59,i_item_id#60,i_rec_start_date#61,i_rec_end_date#62,i_item_desc#63,i_current_price#64,i_wholesale_cost#65,i_brand_id#66,i_brand#67,i_class_id#68,i_class#69,i_category_id#70,i_category#71,i_manufact_id#72,i_manufact#73,i_size#74,i_formulation#75,i_color#76,i_units#77,i_container#78,i_manager_id#79,i_product_name#80] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.customer
               :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#81,c_customer_id#82,c_current_cdemo_sk#83,c_current_hdemo_sk#84,c_current_addr_sk#85,c_first_shipto_date_sk#86,c_first_sales_date_sk#87,c_salutation#88,c_first_name#89,c_last_name#90,c_preferred_cust_flag#91,c_birth_day#92,c_birth_month#93,c_birth_year#94,c_birth_country#95,c_login#96,c_email_address#97,c_last_review_date#98] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.customer_address
               :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#99,ca_address_id#100,ca_street_number#101,ca_street_name#102,ca_street_type#103,ca_suite_number#104,ca_city#105,ca_county#106,ca_state#107,ca_zip#108,ca_country#109,ca_gmt_offset#110,ca_location_type#111] parquet
               +- SubqueryAlias spark_catalog.tpcds.store
                  +- Relation spark_catalog.tpcds.store[s_store_sk#112,s_store_id#113,s_rec_start_date#114,s_rec_end_date#115,s_closed_date_sk#116,s_store_name#117,s_number_employees#118,s_floor_space#119,s_hours#120,s_manager#121,s_market_id#122,s_geography_class#123,s_market_desc#124,s_market_manager#125,s_division_id#126,s_division_name#127,s_company_id#128,s_company_name#129,s_street_number#130,s_street_name#131,s_street_type#132,s_suite_number#133,s_city#134,s_county#135,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ext_price#2 DESC NULLS LAST, brand#1 ASC NULLS FIRST, brand_id#0 ASC NULLS FIRST, i_manufact_id#72 ASC NULLS FIRST, i_manufact#73 ASC NULLS FIRST], true
      +- Aggregate [i_brand#67, i_brand_id#66, i_manufact_id#72, i_manufact#73], [i_brand_id#66 AS brand_id#0, i_brand#67 AS brand#1, i_manufact_id#72, i_manufact#73, sum(ss_ext_sales_price#51) AS ext_price#2]
         +- Project [ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73]
            +- Join Inner, (NOT (substr(ca_zip#108, 1, 5) = substr(s_zip#137, 1, 5)) AND (ss_store_sk#43 = s_store_sk#112))
               :- Project [ss_store_sk#43, ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73, ca_zip#108]
               :  +- Join Inner, (c_current_addr_sk#85 = ca_address_sk#99)
               :     :- Project [ss_store_sk#43, ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73, c_current_addr_sk#85]
               :     :  +- Join Inner, (ss_customer_sk#39 = c_customer_sk#81)
               :     :     :- Project [ss_customer_sk#39, ss_store_sk#43, ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73]
               :     :     :  +- Join Inner, (ss_item_sk#38 = i_item_sk#59)
               :     :     :     :- Project [ss_item_sk#38, ss_customer_sk#39, ss_store_sk#43, ss_ext_sales_price#51]
               :     :     :     :  +- Join Inner, (d_date_sk#8 = ss_sold_date_sk#36)
               :     :     :     :     :- Project [d_date_sk#8]
               :     :     :     :     :  +- Filter (((isnotnull(d_moy#16) AND isnotnull(d_year#14)) AND ((d_moy#16 = 11) AND (d_year#14 = 1998))) AND isnotnull(d_date_sk#8))
               :     :     :     :     :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#8,d_date_id#9,d_date#10,d_month_seq#11,d_week_seq#12,d_quarter_seq#13,d_year#14,d_dow#15,d_moy#16,d_dom#17,d_qoy#18,d_fy_year#19,d_fy_quarter_seq#20,d_fy_week_seq#21,d_day_name#22,d_quarter_name#23,d_holiday#24,d_weekend#25,d_following_holiday#26,d_first_dom#27,d_last_dom#28,d_same_day_ly#29,d_same_day_lq#30,d_current_day#31,... 4 more fields] parquet
               :     :     :     :     +- Project [ss_sold_date_sk#36, ss_item_sk#38, ss_customer_sk#39, ss_store_sk#43, ss_ext_sales_price#51]
               :     :     :     :        +- Filter (isnotnull(ss_sold_date_sk#36) AND ((isnotnull(ss_item_sk#38) AND isnotnull(ss_customer_sk#39)) AND isnotnull(ss_store_sk#43)))
               :     :     :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#36,ss_sold_time_sk#37,ss_item_sk#38,ss_customer_sk#39,ss_cdemo_sk#40,ss_hdemo_sk#41,ss_addr_sk#42,ss_store_sk#43,ss_promo_sk#44,ss_ticket_number#45,ss_quantity#46,ss_wholesale_cost#47,ss_list_price#48,ss_sales_price#49,ss_ext_discount_amt#50,ss_ext_sales_price#51,ss_ext_wholesale_cost#52,ss_ext_list_price#53,ss_ext_tax#54,ss_coupon_amt#55,ss_net_paid#56,ss_net_paid_inc_tax#57,ss_net_profit#58] parquet
               :     :     :     +- Project [i_item_sk#59, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73]
               :     :     :        +- Filter ((isnotnull(i_manager_id#79) AND (i_manager_id#79 = 8)) AND isnotnull(i_item_sk#59))
               :     :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#59,i_item_id#60,i_rec_start_date#61,i_rec_end_date#62,i_item_desc#63,i_current_price#64,i_wholesale_cost#65,i_brand_id#66,i_brand#67,i_class_id#68,i_class#69,i_category_id#70,i_category#71,i_manufact_id#72,i_manufact#73,i_size#74,i_formulation#75,i_color#76,i_units#77,i_container#78,i_manager_id#79,i_product_name#80] parquet
               :     :     +- Project [c_customer_sk#81, c_current_addr_sk#85]
               :     :        +- Filter (isnotnull(c_customer_sk#81) AND isnotnull(c_current_addr_sk#85))
               :     :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#81,c_customer_id#82,c_current_cdemo_sk#83,c_current_hdemo_sk#84,c_current_addr_sk#85,c_first_shipto_date_sk#86,c_first_sales_date_sk#87,c_salutation#88,c_first_name#89,c_last_name#90,c_preferred_cust_flag#91,c_birth_day#92,c_birth_month#93,c_birth_year#94,c_birth_country#95,c_login#96,c_email_address#97,c_last_review_date#98] parquet
               :     +- Project [ca_address_sk#99, ca_zip#108]
               :        +- Filter (isnotnull(ca_address_sk#99) AND isnotnull(ca_zip#108))
               :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#99,ca_address_id#100,ca_street_number#101,ca_street_name#102,ca_street_type#103,ca_suite_number#104,ca_city#105,ca_county#106,ca_state#107,ca_zip#108,ca_country#109,ca_gmt_offset#110,ca_location_type#111] parquet
               +- Project [s_store_sk#112, s_zip#137]
                  +- Filter (isnotnull(s_zip#137) AND isnotnull(s_store_sk#112))
                     +- Relation spark_catalog.tpcds.store[s_store_sk#112,s_store_id#113,s_rec_start_date#114,s_rec_end_date#115,s_closed_date_sk#116,s_store_name#117,s_number_employees#118,s_floor_space#119,s_hours#120,s_manager#121,s_market_id#122,s_geography_class#123,s_market_desc#124,s_market_manager#125,s_division_id#126,s_division_name#127,s_company_id#128,s_company_name#129,s_street_number#130,s_street_name#131,s_street_type#132,s_suite_number#133,s_city#134,s_county#135,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[ext_price#2 DESC NULLS LAST,brand#1 ASC NULLS FIRST,brand_id#0 ASC NULLS FIRST,i_manufact_id#72 ASC NULLS FIRST,i_manufact#73 ASC NULLS FIRST], output=[brand_id#0,brand#1,i_manufact_id#72,i_manufact#73,ext_price#2])
   +- HashAggregate(keys=[i_brand#67, i_brand_id#66, i_manufact_id#72, i_manufact#73], functions=[sum(ss_ext_sales_price#51)], output=[brand_id#0, brand#1, i_manufact_id#72, i_manufact#73, ext_price#2])
      +- Exchange hashpartitioning(i_brand#67, i_brand_id#66, i_manufact_id#72, i_manufact#73, 200), ENSURE_REQUIREMENTS, [plan_id=138]
         +- HashAggregate(keys=[i_brand#67, i_brand_id#66, i_manufact_id#72, i_manufact#73], functions=[partial_sum(ss_ext_sales_price#51)], output=[i_brand#67, i_brand_id#66, i_manufact_id#72, i_manufact#73, sum#149])
            +- Project [ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73]
               +- BroadcastHashJoin [ss_store_sk#43], [s_store_sk#112], Inner, BuildRight, NOT (substr(ca_zip#108, 1, 5) = substr(s_zip#137, 1, 5)), false
                  :- Project [ss_store_sk#43, ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73, ca_zip#108]
                  :  +- BroadcastHashJoin [c_current_addr_sk#85], [ca_address_sk#99], Inner, BuildRight, false
                  :     :- Project [ss_store_sk#43, ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73, c_current_addr_sk#85]
                  :     :  +- BroadcastHashJoin [ss_customer_sk#39], [c_customer_sk#81], Inner, BuildRight, false
                  :     :     :- Project [ss_customer_sk#39, ss_store_sk#43, ss_ext_sales_price#51, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73]
                  :     :     :  +- BroadcastHashJoin [ss_item_sk#38], [i_item_sk#59], Inner, BuildRight, false
                  :     :     :     :- Project [ss_item_sk#38, ss_customer_sk#39, ss_store_sk#43, ss_ext_sales_price#51]
                  :     :     :     :  +- BroadcastHashJoin [d_date_sk#8], [ss_sold_date_sk#36], Inner, BuildLeft, false
                  :     :     :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=117]
                  :     :     :     :     :  +- Project [d_date_sk#8]
                  :     :     :     :     :     +- Filter ((((isnotnull(d_moy#16) AND isnotnull(d_year#14)) AND (d_moy#16 = 11)) AND (d_year#14 = 1998)) AND isnotnull(d_date_sk#8))
                  :     :     :     :     :        +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#8,d_year#14,d_moy#16] Batched: true, DataFilters: [isnotnull(d_moy#16), isnotnull(d_year#14), (d_moy#16 = 11), (d_year#14 = 1998), isnotnull(d_date..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,1998), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :     :     :     +- Filter (((isnotnull(ss_sold_date_sk#36) AND isnotnull(ss_item_sk#38)) AND isnotnull(ss_customer_sk#39)) AND isnotnull(ss_store_sk#43))
                  :     :     :     :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#36,ss_item_sk#38,ss_customer_sk#39,ss_store_sk#43,ss_ext_sales_price#51] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#36), isnotnull(ss_item_sk#38), isnotnull(ss_customer_sk#39), isnotnull..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk), IsNotNull(ss_customer_sk), IsNotNull(ss_store..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ext_sales_price:d...
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=121]
                  :     :     :        +- Project [i_item_sk#59, i_brand_id#66, i_brand#67, i_manufact_id#72, i_manufact#73]
                  :     :     :           +- Filter ((isnotnull(i_manager_id#79) AND (i_manager_id#79 = 8)) AND isnotnull(i_item_sk#59))
                  :     :     :              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#59,i_brand_id#66,i_brand#67,i_manufact_id#72,i_manufact#73,i_manager_id#79] Batched: true, DataFilters: [isnotnull(i_manager_id#79), (i_manager_id#79 = 8), isnotnull(i_item_sk#59)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manager_id), EqualTo(i_manager_id,8), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_brand:string,i_manufact_id:int,i_manufact:string,i_manager_...
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=125]
                  :     :        +- Filter (isnotnull(c_customer_sk#81) AND isnotnull(c_current_addr_sk#85))
                  :     :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#81,c_current_addr_sk#85] Batched: true, DataFilters: [isnotnull(c_customer_sk#81), isnotnull(c_current_addr_sk#85)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=129]
                  :        +- Filter (isnotnull(ca_address_sk#99) AND isnotnull(ca_zip#108))
                  :           +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#99,ca_zip#108] Batched: true, DataFilters: [isnotnull(ca_address_sk#99), isnotnull(ca_zip#108)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_zip)], ReadSchema: struct<ca_address_sk:int,ca_zip:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=133]
                     +- Filter (isnotnull(s_zip#137) AND isnotnull(s_store_sk#112))
                        +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#112,s_zip#137] Batched: true, DataFilters: [isnotnull(s_zip#137), isnotnull(s_store_sk#112)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_zip), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_zip:string>

Time taken: 2.668 seconds, Fetched 1 row(s)
