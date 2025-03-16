Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582816224
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['c_customer_id ASC NULLS FIRST], true
      +- 'Project ['c_customer_id AS customer_id#0, 'concat('c_last_name, , , 'coalesce('c_first_name, )) AS customername#1]
         +- 'Filter (((('ca_city = Edgewood) AND ('c_current_addr_sk = 'ca_address_sk)) AND (('ib_lower_bound >= 38128) AND ('ib_upper_bound <= (38128 + 50000)))) AND ((('ib_income_band_sk = 'hd_income_band_sk) AND ('cd_demo_sk = 'c_current_cdemo_sk)) AND (('hd_demo_sk = 'c_current_hdemo_sk) AND ('sr_cdemo_sk = 'cd_demo_sk))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'UnresolvedRelation [customer], [], false
               :  :  :  :  +- 'UnresolvedRelation [customer_address], [], false
               :  :  :  +- 'UnresolvedRelation [customer_demographics], [], false
               :  :  +- 'UnresolvedRelation [household_demographics], [], false
               :  +- 'UnresolvedRelation [income_band], [], false
               +- 'UnresolvedRelation [store_returns], [], false

== Analyzed Logical Plan ==
customer_id: string, customername: string
GlobalLimit 100
+- LocalLimit 100
   +- Project [customer_id#0, customername#1]
      +- Sort [c_customer_id#8 ASC NULLS FIRST], true
         +- Project [c_customer_id#8 AS customer_id#0, concat(c_last_name#16, , , coalesce(c_first_name#15, )) AS customername#1, c_customer_id#8]
            +- Filter ((((ca_city#31 = Edgewood) AND (c_current_addr_sk#11 = ca_address_sk#25)) AND ((ib_lower_bound#53 >= 38128) AND (ib_upper_bound#54 <= (38128 + 50000)))) AND (((ib_income_band_sk#52 = hd_income_band_sk#48) AND (cd_demo_sk#38 = c_current_cdemo_sk#9)) AND ((hd_demo_sk#47 = c_current_hdemo_sk#10) AND (sr_cdemo_sk#59 = cd_demo_sk#38))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.customer
                  :  :  :  :  :  +- Relation spark_catalog.tpcds.customer[c_customer_sk#7,c_customer_id#8,c_current_cdemo_sk#9,c_current_hdemo_sk#10,c_current_addr_sk#11,c_first_shipto_date_sk#12,c_first_sales_date_sk#13,c_salutation#14,c_first_name#15,c_last_name#16,c_preferred_cust_flag#17,c_birth_day#18,c_birth_month#19,c_birth_year#20,c_birth_country#21,c_login#22,c_email_address#23,c_last_review_date#24] parquet
                  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
                  :  :  :  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#25,ca_address_id#26,ca_street_number#27,ca_street_name#28,ca_street_type#29,ca_suite_number#30,ca_city#31,ca_county#32,ca_state#33,ca_zip#34,ca_country#35,ca_gmt_offset#36,ca_location_type#37] parquet
                  :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
                  :  :  :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#38,cd_gender#39,cd_marital_status#40,cd_education_status#41,cd_purchase_estimate#42,cd_credit_rating#43,cd_dep_count#44,cd_dep_employed_count#45,cd_dep_college_count#46] parquet
                  :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
                  :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#47,hd_income_band_sk#48,hd_buy_potential#49,hd_dep_count#50,hd_vehicle_count#51] parquet
                  :  +- SubqueryAlias spark_catalog.tpcds.income_band
                  :     +- Relation spark_catalog.tpcds.income_band[ib_income_band_sk#52,ib_lower_bound#53,ib_upper_bound#54] parquet
                  +- SubqueryAlias spark_catalog.tpcds.store_returns
                     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#55,sr_return_time_sk#56,sr_item_sk#57,sr_customer_sk#58,sr_cdemo_sk#59,sr_hdemo_sk#60,sr_addr_sk#61,sr_store_sk#62,sr_reason_sk#63,sr_ticket_number#64,sr_return_quantity#65,sr_return_amt#66,sr_return_tax#67,sr_return_amt_inc_tax#68,sr_fee#69,sr_return_ship_cost#70,sr_refunded_cash#71,sr_reversed_charge#72,sr_store_credit#73,sr_net_loss#74] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Project [customer_id#0, customername#1]
      +- Sort [c_customer_id#8 ASC NULLS FIRST], true
         +- Project [c_customer_id#8 AS customer_id#0, concat(c_last_name#16, , , coalesce(c_first_name#15, )) AS customername#1, c_customer_id#8]
            +- Join Inner, (sr_cdemo_sk#59 = cd_demo_sk#38)
               :- Project [c_customer_id#8, c_first_name#15, c_last_name#16, cd_demo_sk#38]
               :  +- Join Inner, (ib_income_band_sk#52 = hd_income_band_sk#48)
               :     :- Project [c_customer_id#8, c_first_name#15, c_last_name#16, cd_demo_sk#38, hd_income_band_sk#48]
               :     :  +- Join Inner, (hd_demo_sk#47 = c_current_hdemo_sk#10)
               :     :     :- Project [c_customer_id#8, c_current_hdemo_sk#10, c_first_name#15, c_last_name#16, cd_demo_sk#38]
               :     :     :  +- Join Inner, (cd_demo_sk#38 = c_current_cdemo_sk#9)
               :     :     :     :- Project [c_customer_id#8, c_current_cdemo_sk#9, c_current_hdemo_sk#10, c_first_name#15, c_last_name#16]
               :     :     :     :  +- Join Inner, (c_current_addr_sk#11 = ca_address_sk#25)
               :     :     :     :     :- Project [c_customer_id#8, c_current_cdemo_sk#9, c_current_hdemo_sk#10, c_current_addr_sk#11, c_first_name#15, c_last_name#16]
               :     :     :     :     :  +- Filter (isnotnull(c_current_addr_sk#11) AND (isnotnull(c_current_cdemo_sk#9) AND isnotnull(c_current_hdemo_sk#10)))
               :     :     :     :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#7,c_customer_id#8,c_current_cdemo_sk#9,c_current_hdemo_sk#10,c_current_addr_sk#11,c_first_shipto_date_sk#12,c_first_sales_date_sk#13,c_salutation#14,c_first_name#15,c_last_name#16,c_preferred_cust_flag#17,c_birth_day#18,c_birth_month#19,c_birth_year#20,c_birth_country#21,c_login#22,c_email_address#23,c_last_review_date#24] parquet
               :     :     :     :     +- Project [ca_address_sk#25]
               :     :     :     :        +- Filter ((isnotnull(ca_city#31) AND (ca_city#31 = Edgewood)) AND isnotnull(ca_address_sk#25))
               :     :     :     :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#25,ca_address_id#26,ca_street_number#27,ca_street_name#28,ca_street_type#29,ca_suite_number#30,ca_city#31,ca_county#32,ca_state#33,ca_zip#34,ca_country#35,ca_gmt_offset#36,ca_location_type#37] parquet
               :     :     :     +- Project [cd_demo_sk#38]
               :     :     :        +- Filter isnotnull(cd_demo_sk#38)
               :     :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#38,cd_gender#39,cd_marital_status#40,cd_education_status#41,cd_purchase_estimate#42,cd_credit_rating#43,cd_dep_count#44,cd_dep_employed_count#45,cd_dep_college_count#46] parquet
               :     :     +- Project [hd_demo_sk#47, hd_income_band_sk#48]
               :     :        +- Filter (isnotnull(hd_demo_sk#47) AND isnotnull(hd_income_band_sk#48))
               :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#47,hd_income_band_sk#48,hd_buy_potential#49,hd_dep_count#50,hd_vehicle_count#51] parquet
               :     +- Project [ib_income_band_sk#52]
               :        +- Filter (((isnotnull(ib_lower_bound#53) AND isnotnull(ib_upper_bound#54)) AND ((ib_lower_bound#53 >= 38128) AND (ib_upper_bound#54 <= 88128))) AND isnotnull(ib_income_band_sk#52))
               :           +- Relation spark_catalog.tpcds.income_band[ib_income_band_sk#52,ib_lower_bound#53,ib_upper_bound#54] parquet
               +- Project [sr_cdemo_sk#59]
                  +- Filter isnotnull(sr_cdemo_sk#59)
                     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#55,sr_return_time_sk#56,sr_item_sk#57,sr_customer_sk#58,sr_cdemo_sk#59,sr_hdemo_sk#60,sr_addr_sk#61,sr_store_sk#62,sr_reason_sk#63,sr_ticket_number#64,sr_return_quantity#65,sr_return_amt#66,sr_return_tax#67,sr_return_amt_inc_tax#68,sr_fee#69,sr_return_ship_cost#70,sr_refunded_cash#71,sr_reversed_charge#72,sr_store_credit#73,sr_net_loss#74] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#8 ASC NULLS FIRST], output=[customer_id#0,customername#1])
   +- Project [c_customer_id#8 AS customer_id#0, concat(c_last_name#16, , , coalesce(c_first_name#15, )) AS customername#1, c_customer_id#8]
      +- BroadcastHashJoin [cd_demo_sk#38], [sr_cdemo_sk#59], Inner, BuildRight, false
         :- Project [c_customer_id#8, c_first_name#15, c_last_name#16, cd_demo_sk#38]
         :  +- BroadcastHashJoin [hd_income_band_sk#48], [ib_income_band_sk#52], Inner, BuildRight, false
         :     :- Project [c_customer_id#8, c_first_name#15, c_last_name#16, cd_demo_sk#38, hd_income_band_sk#48]
         :     :  +- BroadcastHashJoin [c_current_hdemo_sk#10], [hd_demo_sk#47], Inner, BuildRight, false
         :     :     :- Project [c_customer_id#8, c_current_hdemo_sk#10, c_first_name#15, c_last_name#16, cd_demo_sk#38]
         :     :     :  +- BroadcastHashJoin [c_current_cdemo_sk#9], [cd_demo_sk#38], Inner, BuildRight, false
         :     :     :     :- Project [c_customer_id#8, c_current_cdemo_sk#9, c_current_hdemo_sk#10, c_first_name#15, c_last_name#16]
         :     :     :     :  +- BroadcastHashJoin [c_current_addr_sk#11], [ca_address_sk#25], Inner, BuildRight, false
         :     :     :     :     :- Filter ((isnotnull(c_current_addr_sk#11) AND isnotnull(c_current_cdemo_sk#9)) AND isnotnull(c_current_hdemo_sk#10))
         :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.customer[c_customer_id#8,c_current_cdemo_sk#9,c_current_hdemo_sk#10,c_current_addr_sk#11,c_first_name#15,c_last_name#16] Batched: true, DataFilters: [isnotnull(c_current_addr_sk#11), isnotnull(c_current_cdemo_sk#9), isnotnull(c_current_hdemo_sk#10)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk), IsNotNull(c_current_hdemo_sk)], ReadSchema: struct<c_customer_id:string,c_current_cdemo_sk:int,c_current_hdemo_sk:int,c_current_addr_sk:int,c...
         :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=108]
         :     :     :     :        +- Project [ca_address_sk#25]
         :     :     :     :           +- Filter ((isnotnull(ca_city#31) AND (ca_city#31 = Edgewood)) AND isnotnull(ca_address_sk#25))
         :     :     :     :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#25,ca_city#31] Batched: true, DataFilters: [isnotnull(ca_city#31), (ca_city#31 = Edgewood), isnotnull(ca_address_sk#25)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_city), EqualTo(ca_city,Edgewood), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_city:string>
         :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=112]
         :     :     :        +- Filter isnotnull(cd_demo_sk#38)
         :     :     :           +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#38] Batched: true, DataFilters: [isnotnull(cd_demo_sk#38)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int>
         :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=116]
         :     :        +- Filter (isnotnull(hd_demo_sk#47) AND isnotnull(hd_income_band_sk#48))
         :     :           +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#47,hd_income_band_sk#48] Batched: true, DataFilters: [isnotnull(hd_demo_sk#47), isnotnull(hd_income_band_sk#48)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_demo_sk), IsNotNull(hd_income_band_sk)], ReadSchema: struct<hd_demo_sk:int,hd_income_band_sk:int>
         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=120]
         :        +- Project [ib_income_band_sk#52]
         :           +- Filter ((((isnotnull(ib_lower_bound#53) AND isnotnull(ib_upper_bound#54)) AND (ib_lower_bound#53 >= 38128)) AND (ib_upper_bound#54 <= 88128)) AND isnotnull(ib_income_band_sk#52))
         :              +- FileScan parquet spark_catalog.tpcds.income_band[ib_income_band_sk#52,ib_lower_bound#53,ib_upper_bound#54] Batched: true, DataFilters: [isnotnull(ib_lower_bound#53), isnotnull(ib_upper_bound#54), (ib_lower_bound#53 >= 38128), (ib_up..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ib_lower_bound), IsNotNull(ib_upper_bound), GreaterThanOrEqual(ib_lower_bound,38128), ..., ReadSchema: struct<ib_income_band_sk:int,ib_lower_bound:int,ib_upper_bound:int>
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=124]
            +- Filter isnotnull(sr_cdemo_sk#59)
               +- FileScan parquet spark_catalog.tpcds.store_returns[sr_cdemo_sk#59] Batched: true, DataFilters: [isnotnull(sr_cdemo_sk#59)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_cdemo_sk)], ReadSchema: struct<sr_cdemo_sk:int>

Time taken: 2.998 seconds, Fetched 1 row(s)
