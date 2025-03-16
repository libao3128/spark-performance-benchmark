Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580075338
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['ca_zip ASC NULLS FIRST], true
      +- 'Aggregate ['ca_zip], ['ca_zip, unresolvedalias('sum('cs_sales_price), None)]
         +- 'Filter (((('cs_bill_customer_sk = 'c_customer_sk) AND ('c_current_addr_sk = 'ca_address_sk)) AND (('substr('ca_zip, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR 'ca_state IN (CA,WA,GA)) OR ('cs_sales_price > 500))) AND ((('cs_sold_date_sk = 'd_date_sk) AND ('d_qoy = 2)) AND ('d_year = 2001)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [catalog_sales], [], false
               :  :  +- 'UnresolvedRelation [customer], [], false
               :  +- 'UnresolvedRelation [customer_address], [], false
               +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
ca_zip: string, sum(cs_sales_price): double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ca_zip#66 ASC NULLS FIRST], true
      +- Aggregate [ca_zip#66], [ca_zip#66, sum(cs_sales_price#26) AS sum(cs_sales_price)#103]
         +- Filter ((((cs_bill_customer_sk#8 = c_customer_sk#39) AND (c_current_addr_sk#43 = ca_address_sk#57)) AND ((substr(ca_zip#66, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR ca_state#65 IN (CA,WA,GA)) OR (cs_sales_price#26 > cast(500 as double)))) AND (((cs_sold_date_sk#5 = d_date_sk#70) AND (d_qoy#80 = 2)) AND (d_year#76 = 2001)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#5,cs_sold_time_sk#6,cs_ship_date_sk#7,cs_bill_customer_sk#8,cs_bill_cdemo_sk#9,cs_bill_hdemo_sk#10,cs_bill_addr_sk#11,cs_ship_customer_sk#12,cs_ship_cdemo_sk#13,cs_ship_hdemo_sk#14,cs_ship_addr_sk#15,cs_call_center_sk#16,cs_catalog_page_sk#17,cs_ship_mode_sk#18,cs_warehouse_sk#19,cs_item_sk#20,cs_promo_sk#21,cs_order_number#22,cs_quantity#23,cs_wholesale_cost#24,cs_list_price#25,cs_sales_price#26,cs_ext_discount_amt#27,cs_ext_sales_price#28,... 10 more fields] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.customer
               :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#39,c_customer_id#40,c_current_cdemo_sk#41,c_current_hdemo_sk#42,c_current_addr_sk#43,c_first_shipto_date_sk#44,c_first_sales_date_sk#45,c_salutation#46,c_first_name#47,c_last_name#48,c_preferred_cust_flag#49,c_birth_day#50,c_birth_month#51,c_birth_year#52,c_birth_country#53,c_login#54,c_email_address#55,c_last_review_date#56] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.customer_address
               :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#57,ca_address_id#58,ca_street_number#59,ca_street_name#60,ca_street_type#61,ca_suite_number#62,ca_city#63,ca_county#64,ca_state#65,ca_zip#66,ca_country#67,ca_gmt_offset#68,ca_location_type#69] parquet
               +- SubqueryAlias spark_catalog.tpcds.date_dim
                  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#70,d_date_id#71,d_date#72,d_month_seq#73,d_week_seq#74,d_quarter_seq#75,d_year#76,d_dow#77,d_moy#78,d_dom#79,d_qoy#80,d_fy_year#81,d_fy_quarter_seq#82,d_fy_week_seq#83,d_day_name#84,d_quarter_name#85,d_holiday#86,d_weekend#87,d_following_holiday#88,d_first_dom#89,d_last_dom#90,d_same_day_ly#91,d_same_day_lq#92,d_current_day#93,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [ca_zip#66 ASC NULLS FIRST], true
      +- Aggregate [ca_zip#66], [ca_zip#66, sum(cs_sales_price#26) AS sum(cs_sales_price)#103]
         +- Project [cs_sales_price#26, ca_zip#66]
            +- Join Inner, (cs_sold_date_sk#5 = d_date_sk#70)
               :- Project [cs_sold_date_sk#5, cs_sales_price#26, ca_zip#66]
               :  +- Join Inner, ((c_current_addr_sk#43 = ca_address_sk#57) AND ((substr(ca_zip#66, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR ca_state#65 IN (CA,WA,GA)) OR (cs_sales_price#26 > 500.0)))
               :     :- Project [cs_sold_date_sk#5, cs_sales_price#26, c_current_addr_sk#43]
               :     :  +- Join Inner, (cs_bill_customer_sk#8 = c_customer_sk#39)
               :     :     :- Project [cs_sold_date_sk#5, cs_bill_customer_sk#8, cs_sales_price#26]
               :     :     :  +- Filter (isnotnull(cs_bill_customer_sk#8) AND isnotnull(cs_sold_date_sk#5))
               :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#5,cs_sold_time_sk#6,cs_ship_date_sk#7,cs_bill_customer_sk#8,cs_bill_cdemo_sk#9,cs_bill_hdemo_sk#10,cs_bill_addr_sk#11,cs_ship_customer_sk#12,cs_ship_cdemo_sk#13,cs_ship_hdemo_sk#14,cs_ship_addr_sk#15,cs_call_center_sk#16,cs_catalog_page_sk#17,cs_ship_mode_sk#18,cs_warehouse_sk#19,cs_item_sk#20,cs_promo_sk#21,cs_order_number#22,cs_quantity#23,cs_wholesale_cost#24,cs_list_price#25,cs_sales_price#26,cs_ext_discount_amt#27,cs_ext_sales_price#28,... 10 more fields] parquet
               :     :     +- Project [c_customer_sk#39, c_current_addr_sk#43]
               :     :        +- Filter (isnotnull(c_customer_sk#39) AND isnotnull(c_current_addr_sk#43))
               :     :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#39,c_customer_id#40,c_current_cdemo_sk#41,c_current_hdemo_sk#42,c_current_addr_sk#43,c_first_shipto_date_sk#44,c_first_sales_date_sk#45,c_salutation#46,c_first_name#47,c_last_name#48,c_preferred_cust_flag#49,c_birth_day#50,c_birth_month#51,c_birth_year#52,c_birth_country#53,c_login#54,c_email_address#55,c_last_review_date#56] parquet
               :     +- Project [ca_address_sk#57, ca_state#65, ca_zip#66]
               :        +- Filter isnotnull(ca_address_sk#57)
               :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#57,ca_address_id#58,ca_street_number#59,ca_street_name#60,ca_street_type#61,ca_suite_number#62,ca_city#63,ca_county#64,ca_state#65,ca_zip#66,ca_country#67,ca_gmt_offset#68,ca_location_type#69] parquet
               +- Project [d_date_sk#70]
                  +- Filter (((isnotnull(d_qoy#80) AND isnotnull(d_year#76)) AND ((d_qoy#80 = 2) AND (d_year#76 = 2001))) AND isnotnull(d_date_sk#70))
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#70,d_date_id#71,d_date#72,d_month_seq#73,d_week_seq#74,d_quarter_seq#75,d_year#76,d_dow#77,d_moy#78,d_dom#79,d_qoy#80,d_fy_year#81,d_fy_quarter_seq#82,d_fy_week_seq#83,d_day_name#84,d_quarter_name#85,d_holiday#86,d_weekend#87,d_following_holiday#88,d_first_dom#89,d_last_dom#90,d_same_day_ly#91,d_same_day_lq#92,d_current_day#93,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[ca_zip#66 ASC NULLS FIRST], output=[ca_zip#66,sum(cs_sales_price)#103])
   +- HashAggregate(keys=[ca_zip#66], functions=[sum(cs_sales_price#26)], output=[ca_zip#66, sum(cs_sales_price)#103])
      +- Exchange hashpartitioning(ca_zip#66, 200), ENSURE_REQUIREMENTS, [plan_id=94]
         +- HashAggregate(keys=[ca_zip#66], functions=[partial_sum(cs_sales_price#26)], output=[ca_zip#66, sum#105])
            +- Project [cs_sales_price#26, ca_zip#66]
               +- BroadcastHashJoin [cs_sold_date_sk#5], [d_date_sk#70], Inner, BuildRight, false
                  :- Project [cs_sold_date_sk#5, cs_sales_price#26, ca_zip#66]
                  :  +- BroadcastHashJoin [c_current_addr_sk#43], [ca_address_sk#57], Inner, BuildRight, ((substr(ca_zip#66, 1, 5) IN (85669,86197,88274,83405,86475,85392,85460,80348,81792) OR ca_state#65 IN (CA,WA,GA)) OR (cs_sales_price#26 > 500.0)), false
                  :     :- Project [cs_sold_date_sk#5, cs_sales_price#26, c_current_addr_sk#43]
                  :     :  +- BroadcastHashJoin [cs_bill_customer_sk#8], [c_customer_sk#39], Inner, BuildRight, false
                  :     :     :- Filter (isnotnull(cs_bill_customer_sk#8) AND isnotnull(cs_sold_date_sk#5))
                  :     :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#5,cs_bill_customer_sk#8,cs_sales_price#26] Batched: true, DataFilters: [isnotnull(cs_bill_customer_sk#8), isnotnull(cs_sold_date_sk#5)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_bill_customer_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_sales_price:double>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=81]
                  :     :        +- Filter (isnotnull(c_customer_sk#39) AND isnotnull(c_current_addr_sk#43))
                  :     :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#39,c_current_addr_sk#43] Batched: true, DataFilters: [isnotnull(c_customer_sk#39), isnotnull(c_current_addr_sk#43)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=85]
                  :        +- Filter isnotnull(ca_address_sk#57)
                  :           +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#57,ca_state#65,ca_zip#66] Batched: true, DataFilters: [isnotnull(ca_address_sk#57)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_zip:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=89]
                     +- Project [d_date_sk#70]
                        +- Filter ((((isnotnull(d_qoy#80) AND isnotnull(d_year#76)) AND (d_qoy#80 = 2)) AND (d_year#76 = 2001)) AND isnotnull(d_date_sk#70))
                           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#70,d_year#76,d_qoy#80] Batched: true, DataFilters: [isnotnull(d_qoy#80), isnotnull(d_year#76), (d_qoy#80 = 2), (d_year#76 = 2001), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>

Time taken: 2.408 seconds, Fetched 1 row(s)
