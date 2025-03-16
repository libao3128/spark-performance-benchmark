Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581294967
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['ca_zip ASC NULLS FIRST, 'ca_city ASC NULLS FIRST], true
      +- 'Aggregate ['ca_zip, 'ca_city], ['ca_zip, 'ca_city, unresolvedalias('sum('ws_sales_price), None)]
         +- 'Filter (((('ws_bill_customer_sk = 'c_customer_sk) AND ('c_current_addr_sk = 'ca_address_sk)) AND (('ws_item_sk = 'i_item_sk) AND ('substr('ca_zip, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR 'i_item_id IN (list#0 [])))) AND ((('ws_sold_date_sk = 'd_date_sk) AND ('d_qoy = 2)) AND ('d_year = 2001)))
            :  +- 'Project ['i_item_id]
            :     +- 'Filter 'i_item_sk IN (2,3,5,7,11,13,17,19,23,29)
            :        +- 'UnresolvedRelation [item], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation [web_sales], [], false
               :  :  :  +- 'UnresolvedRelation [customer], [], false
               :  :  +- 'UnresolvedRelation [customer_address], [], false
               :  +- 'UnresolvedRelation [date_dim], [], false
               +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
ca_zip: string, ca_city: string, sum(ws_sales_price): double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ca_zip#67 ASC NULLS FIRST, ca_city#64 ASC NULLS FIRST], true
      +- Aggregate [ca_zip#67, ca_city#64], [ca_zip#67, ca_city#64, sum(ws_sales_price#27) AS sum(ws_sales_price)#149]
         +- Filter ((((ws_bill_customer_sk#10 = c_customer_sk#40) AND (c_current_addr_sk#44 = ca_address_sk#58)) AND ((ws_item_sk#9 = i_item_sk#99) AND (substr(ca_zip#67, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR i_item_id#100 IN (list#0 [])))) AND (((ws_sold_date_sk#6 = d_date_sk#71) AND (d_qoy#81 = 2)) AND (d_year#77 = 2001)))
            :  +- Project [i_item_id#127]
            :     +- Filter i_item_sk#126 IN (2,3,5,7,11,13,17,19,23,29)
            :        +- SubqueryAlias spark_catalog.tpcds.item
            :           +- Relation spark_catalog.tpcds.item[i_item_sk#126,i_item_id#127,i_rec_start_date#128,i_rec_end_date#129,i_item_desc#130,i_current_price#131,i_wholesale_cost#132,i_brand_id#133,i_brand#134,i_class_id#135,i_class#136,i_category_id#137,i_category#138,i_manufact_id#139,i_manufact#140,i_size#141,i_formulation#142,i_color#143,i_units#144,i_container#145,i_manager_id#146,i_product_name#147] parquet
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
               :  :  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#6,ws_sold_time_sk#7,ws_ship_date_sk#8,ws_item_sk#9,ws_bill_customer_sk#10,ws_bill_cdemo_sk#11,ws_bill_hdemo_sk#12,ws_bill_addr_sk#13,ws_ship_customer_sk#14,ws_ship_cdemo_sk#15,ws_ship_hdemo_sk#16,ws_ship_addr_sk#17,ws_web_page_sk#18,ws_web_site_sk#19,ws_ship_mode_sk#20,ws_warehouse_sk#21,ws_promo_sk#22,ws_order_number#23,ws_quantity#24,ws_wholesale_cost#25,ws_list_price#26,ws_sales_price#27,ws_ext_discount_amt#28,ws_ext_sales_price#29,... 10 more fields] parquet
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer
               :  :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#40,c_customer_id#41,c_current_cdemo_sk#42,c_current_hdemo_sk#43,c_current_addr_sk#44,c_first_shipto_date_sk#45,c_first_sales_date_sk#46,c_salutation#47,c_first_name#48,c_last_name#49,c_preferred_cust_flag#50,c_birth_day#51,c_birth_month#52,c_birth_year#53,c_birth_country#54,c_login#55,c_email_address#56,c_last_review_date#57] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
               :  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#71,d_date_id#72,d_date#73,d_month_seq#74,d_week_seq#75,d_quarter_seq#76,d_year#77,d_dow#78,d_moy#79,d_dom#80,d_qoy#81,d_fy_year#82,d_fy_quarter_seq#83,d_fy_week_seq#84,d_day_name#85,d_quarter_name#86,d_holiday#87,d_weekend#88,d_following_holiday#89,d_first_dom#90,d_last_dom#91,d_same_day_ly#92,d_same_day_lq#93,d_current_day#94,... 4 more fields] parquet
               +- SubqueryAlias spark_catalog.tpcds.item
                  +- Relation spark_catalog.tpcds.item[i_item_sk#99,i_item_id#100,i_rec_start_date#101,i_rec_end_date#102,i_item_desc#103,i_current_price#104,i_wholesale_cost#105,i_brand_id#106,i_brand#107,i_class_id#108,i_class#109,i_category_id#110,i_category#111,i_manufact_id#112,i_manufact#113,i_size#114,i_formulation#115,i_color#116,i_units#117,i_container#118,i_manager_id#119,i_product_name#120] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ca_zip#67 ASC NULLS FIRST, ca_city#64 ASC NULLS FIRST], true
      +- Aggregate [ca_zip#67, ca_city#64], [ca_zip#67, ca_city#64, sum(ws_sales_price#27) AS sum(ws_sales_price)#149]
         +- Project [ws_sales_price#27, ca_city#64, ca_zip#67]
            +- Filter (substr(ca_zip#67, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR exists#150)
               +- Join ExistenceJoin(exists#150), (i_item_id#100 = i_item_id#127)
                  :- Project [ws_sales_price#27, ca_city#64, ca_zip#67, i_item_id#100]
                  :  +- Join Inner, (ws_item_sk#9 = i_item_sk#99)
                  :     :- Project [ws_item_sk#9, ws_sales_price#27, ca_city#64, ca_zip#67]
                  :     :  +- Join Inner, (ws_sold_date_sk#6 = d_date_sk#71)
                  :     :     :- Project [ws_sold_date_sk#6, ws_item_sk#9, ws_sales_price#27, ca_city#64, ca_zip#67]
                  :     :     :  +- Join Inner, (c_current_addr_sk#44 = ca_address_sk#58)
                  :     :     :     :- Project [ws_sold_date_sk#6, ws_item_sk#9, ws_sales_price#27, c_current_addr_sk#44]
                  :     :     :     :  +- Join Inner, (ws_bill_customer_sk#10 = c_customer_sk#40)
                  :     :     :     :     :- Project [ws_sold_date_sk#6, ws_item_sk#9, ws_bill_customer_sk#10, ws_sales_price#27]
                  :     :     :     :     :  +- Filter (isnotnull(ws_bill_customer_sk#10) AND (isnotnull(ws_sold_date_sk#6) AND isnotnull(ws_item_sk#9)))
                  :     :     :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#6,ws_sold_time_sk#7,ws_ship_date_sk#8,ws_item_sk#9,ws_bill_customer_sk#10,ws_bill_cdemo_sk#11,ws_bill_hdemo_sk#12,ws_bill_addr_sk#13,ws_ship_customer_sk#14,ws_ship_cdemo_sk#15,ws_ship_hdemo_sk#16,ws_ship_addr_sk#17,ws_web_page_sk#18,ws_web_site_sk#19,ws_ship_mode_sk#20,ws_warehouse_sk#21,ws_promo_sk#22,ws_order_number#23,ws_quantity#24,ws_wholesale_cost#25,ws_list_price#26,ws_sales_price#27,ws_ext_discount_amt#28,ws_ext_sales_price#29,... 10 more fields] parquet
                  :     :     :     :     +- Project [c_customer_sk#40, c_current_addr_sk#44]
                  :     :     :     :        +- Filter (isnotnull(c_customer_sk#40) AND isnotnull(c_current_addr_sk#44))
                  :     :     :     :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#40,c_customer_id#41,c_current_cdemo_sk#42,c_current_hdemo_sk#43,c_current_addr_sk#44,c_first_shipto_date_sk#45,c_first_sales_date_sk#46,c_salutation#47,c_first_name#48,c_last_name#49,c_preferred_cust_flag#50,c_birth_day#51,c_birth_month#52,c_birth_year#53,c_birth_country#54,c_login#55,c_email_address#56,c_last_review_date#57] parquet
                  :     :     :     +- Project [ca_address_sk#58, ca_city#64, ca_zip#67]
                  :     :     :        +- Filter isnotnull(ca_address_sk#58)
                  :     :     :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet
                  :     :     +- Project [d_date_sk#71]
                  :     :        +- Filter (((isnotnull(d_qoy#81) AND isnotnull(d_year#77)) AND ((d_qoy#81 = 2) AND (d_year#77 = 2001))) AND isnotnull(d_date_sk#71))
                  :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#71,d_date_id#72,d_date#73,d_month_seq#74,d_week_seq#75,d_quarter_seq#76,d_year#77,d_dow#78,d_moy#79,d_dom#80,d_qoy#81,d_fy_year#82,d_fy_quarter_seq#83,d_fy_week_seq#84,d_day_name#85,d_quarter_name#86,d_holiday#87,d_weekend#88,d_following_holiday#89,d_first_dom#90,d_last_dom#91,d_same_day_ly#92,d_same_day_lq#93,d_current_day#94,... 4 more fields] parquet
                  :     +- Project [i_item_sk#99, i_item_id#100]
                  :        +- Filter isnotnull(i_item_sk#99)
                  :           +- Relation spark_catalog.tpcds.item[i_item_sk#99,i_item_id#100,i_rec_start_date#101,i_rec_end_date#102,i_item_desc#103,i_current_price#104,i_wholesale_cost#105,i_brand_id#106,i_brand#107,i_class_id#108,i_class#109,i_category_id#110,i_category#111,i_manufact_id#112,i_manufact#113,i_size#114,i_formulation#115,i_color#116,i_units#117,i_container#118,i_manager_id#119,i_product_name#120] parquet
                  +- Project [i_item_id#127]
                     +- Filter i_item_sk#126 IN (2,3,5,7,11,13,17,19,23,29)
                        +- Relation spark_catalog.tpcds.item[i_item_sk#126,i_item_id#127,i_rec_start_date#128,i_rec_end_date#129,i_item_desc#130,i_current_price#131,i_wholesale_cost#132,i_brand_id#133,i_brand#134,i_class_id#135,i_class#136,i_category_id#137,i_category#138,i_manufact_id#139,i_manufact#140,i_size#141,i_formulation#142,i_color#143,i_units#144,i_container#145,i_manager_id#146,i_product_name#147] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[ca_zip#67 ASC NULLS FIRST,ca_city#64 ASC NULLS FIRST], output=[ca_zip#67,ca_city#64,sum(ws_sales_price)#149])
   +- HashAggregate(keys=[ca_zip#67, ca_city#64], functions=[sum(ws_sales_price#27)], output=[ca_zip#67, ca_city#64, sum(ws_sales_price)#149])
      +- Exchange hashpartitioning(ca_zip#67, ca_city#64, 200), ENSURE_REQUIREMENTS, [plan_id=144]
         +- HashAggregate(keys=[ca_zip#67, ca_city#64], functions=[partial_sum(ws_sales_price#27)], output=[ca_zip#67, ca_city#64, sum#152])
            +- Project [ws_sales_price#27, ca_city#64, ca_zip#67]
               +- Filter (substr(ca_zip#67, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR exists#150)
                  +- BroadcastHashJoin [i_item_id#100], [i_item_id#127], ExistenceJoin(exists#150), BuildRight, false
                     :- Project [ws_sales_price#27, ca_city#64, ca_zip#67, i_item_id#100]
                     :  +- BroadcastHashJoin [ws_item_sk#9], [i_item_sk#99], Inner, BuildRight, false
                     :     :- Project [ws_item_sk#9, ws_sales_price#27, ca_city#64, ca_zip#67]
                     :     :  +- BroadcastHashJoin [ws_sold_date_sk#6], [d_date_sk#71], Inner, BuildRight, false
                     :     :     :- Project [ws_sold_date_sk#6, ws_item_sk#9, ws_sales_price#27, ca_city#64, ca_zip#67]
                     :     :     :  +- BroadcastHashJoin [c_current_addr_sk#44], [ca_address_sk#58], Inner, BuildRight, false
                     :     :     :     :- Project [ws_sold_date_sk#6, ws_item_sk#9, ws_sales_price#27, c_current_addr_sk#44]
                     :     :     :     :  +- BroadcastHashJoin [ws_bill_customer_sk#10], [c_customer_sk#40], Inner, BuildRight, false
                     :     :     :     :     :- Filter ((isnotnull(ws_bill_customer_sk#10) AND isnotnull(ws_sold_date_sk#6)) AND isnotnull(ws_item_sk#9))
                     :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#6,ws_item_sk#9,ws_bill_customer_sk#10,ws_sales_price#27] Batched: true, DataFilters: [isnotnull(ws_bill_customer_sk#10), isnotnull(ws_sold_date_sk#6), isnotnull(ws_item_sk#9)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int,ws_sales_price:double>
                     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=122]
                     :     :     :     :        +- Filter (isnotnull(c_customer_sk#40) AND isnotnull(c_current_addr_sk#44))
                     :     :     :     :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#40,c_current_addr_sk#44] Batched: true, DataFilters: [isnotnull(c_customer_sk#40), isnotnull(c_current_addr_sk#44)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=126]
                     :     :     :        +- Filter isnotnull(ca_address_sk#58)
                     :     :     :           +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#58,ca_city#64,ca_zip#67] Batched: true, DataFilters: [isnotnull(ca_address_sk#58)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_city:string,ca_zip:string>
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=130]
                     :     :        +- Project [d_date_sk#71]
                     :     :           +- Filter ((((isnotnull(d_qoy#81) AND isnotnull(d_year#77)) AND (d_qoy#81 = 2)) AND (d_year#77 = 2001)) AND isnotnull(d_date_sk#71))
                     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#71,d_year#77,d_qoy#81] Batched: true, DataFilters: [isnotnull(d_qoy#81), isnotnull(d_year#77), (d_qoy#81 = 2), (d_year#77 = 2001), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=134]
                     :        +- Filter isnotnull(i_item_sk#99)
                     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#99,i_item_id#100] Batched: true, DataFilters: [isnotnull(i_item_sk#99)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=138]
                        +- Project [i_item_id#127]
                           +- Filter i_item_sk#126 IN (2,3,5,7,11,13,17,19,23,29)
                              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#126,i_item_id#127] Batched: true, DataFilters: [i_item_sk#126 IN (2,3,5,7,11,13,17,19,23,29)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(i_item_sk, [11,13,17,19,2,23,29,3,5,7])], ReadSchema: struct<i_item_sk:int,i_item_id:string>

Time taken: 2.783 seconds, Fetched 1 row(s)
