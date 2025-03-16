Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581330594
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['c_last_name ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'ca_city ASC NULLS FIRST, 'bought_city ASC NULLS FIRST, 'ss_ticket_number ASC NULLS FIRST], true
      +- 'Project ['c_last_name, 'c_first_name, 'ca_city, 'bought_city, 'ss_ticket_number, 'amt, 'profit]
         +- 'Filter ((('ss_customer_sk = 'c_customer_sk) AND ('customer.c_current_addr_sk = 'current_addr.ca_address_sk)) AND NOT ('current_addr.ca_city = 'bought_city))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias dn
               :  :  +- 'Aggregate ['ss_ticket_number, 'ss_customer_sk, 'ss_addr_sk, 'ca_city], ['ss_ticket_number, 'ss_customer_sk, 'ca_city AS bought_city#0, 'sum('ss_coupon_amt) AS amt#1, 'sum('ss_net_profit) AS profit#2]
               :  :     +- 'Filter (((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) AND ('store_sales.ss_store_sk = 'store.s_store_sk)) AND (('store_sales.ss_hdemo_sk = 'household_demographics.hd_demo_sk) AND ('store_sales.ss_addr_sk = 'customer_address.ca_address_sk))) AND (((('household_demographics.hd_dep_count = 4) OR ('household_demographics.hd_vehicle_count = 3)) AND 'date_dim.d_dow IN (6,0)) AND ('date_dim.d_year IN (1999,(1999 + 1),(1999 + 2)) AND 'store.s_city IN (Fairview,Midway,Fairview,Fairview,Fairview))))
               :  :        +- 'Join Inner
               :  :           :- 'Join Inner
               :  :           :  :- 'Join Inner
               :  :           :  :  :- 'Join Inner
               :  :           :  :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :           :  :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  :           :  :  +- 'UnresolvedRelation [store], [], false
               :  :           :  +- 'UnresolvedRelation [household_demographics], [], false
               :  :           +- 'UnresolvedRelation [customer_address], [], false
               :  +- 'UnresolvedRelation [customer], [], false
               +- 'SubqueryAlias current_addr
                  +- 'UnresolvedRelation [customer_address], [], false

== Analyzed Logical Plan ==
c_last_name: string, c_first_name: string, ca_city: string, bought_city: string, ss_ticket_number: int, amt: double, profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#115 ASC NULLS FIRST, c_first_name#114 ASC NULLS FIRST, ca_city#130 ASC NULLS FIRST, bought_city#0 ASC NULLS FIRST, ss_ticket_number#17 ASC NULLS FIRST], true
      +- Project [c_last_name#115, c_first_name#114, ca_city#130, bought_city#0, ss_ticket_number#17, amt#1, profit#2]
         +- Filter (((ss_customer_sk#11 = c_customer_sk#106) AND (c_current_addr_sk#110 = ca_address_sk#124)) AND NOT (ca_city#130 = bought_city#0))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias dn
               :  :  +- Aggregate [ss_ticket_number#17, ss_customer_sk#11, ss_addr_sk#14, ca_city#99], [ss_ticket_number#17, ss_customer_sk#11, ca_city#99 AS bought_city#0, sum(ss_coupon_amt#27) AS amt#1, sum(ss_net_profit#30) AS profit#2]
               :  :     +- Filter ((((ss_sold_date_sk#8 = d_date_sk#31) AND (ss_store_sk#15 = s_store_sk#59)) AND ((ss_hdemo_sk#13 = hd_demo_sk#88) AND (ss_addr_sk#14 = ca_address_sk#93))) AND ((((hd_dep_count#91 = 4) OR (hd_vehicle_count#92 = 3)) AND d_dow#38 IN (6,0)) AND (d_year#37 IN (1999,(1999 + 1),(1999 + 2)) AND s_city#81 IN (Fairview,Midway,Fairview,Fairview,Fairview))))
               :  :        +- Join Inner
               :  :           :- Join Inner
               :  :           :  :- Join Inner
               :  :           :  :  :- Join Inner
               :  :           :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :           :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
               :  :           :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :           :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#31,d_date_id#32,d_date#33,d_month_seq#34,d_week_seq#35,d_quarter_seq#36,d_year#37,d_dow#38,d_moy#39,d_dom#40,d_qoy#41,d_fy_year#42,d_fy_quarter_seq#43,d_fy_week_seq#44,d_day_name#45,d_quarter_name#46,d_holiday#47,d_weekend#48,d_following_holiday#49,d_first_dom#50,d_last_dom#51,d_same_day_ly#52,d_same_day_lq#53,d_current_day#54,... 4 more fields] parquet
               :  :           :  :  +- SubqueryAlias spark_catalog.tpcds.store
               :  :           :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#59,s_store_id#60,s_rec_start_date#61,s_rec_end_date#62,s_closed_date_sk#63,s_store_name#64,s_number_employees#65,s_floor_space#66,s_hours#67,s_manager#68,s_market_id#69,s_geography_class#70,s_market_desc#71,s_market_manager#72,s_division_id#73,s_division_name#74,s_company_id#75,s_company_name#76,s_street_number#77,s_street_name#78,s_street_type#79,s_suite_number#80,s_city#81,s_county#82,... 5 more fields] parquet
               :  :           :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
               :  :           :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#88,hd_income_band_sk#89,hd_buy_potential#90,hd_dep_count#91,hd_vehicle_count#92] parquet
               :  :           +- SubqueryAlias spark_catalog.tpcds.customer_address
               :  :              +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#93,ca_address_id#94,ca_street_number#95,ca_street_name#96,ca_street_type#97,ca_suite_number#98,ca_city#99,ca_county#100,ca_state#101,ca_zip#102,ca_country#103,ca_gmt_offset#104,ca_location_type#105] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.customer
               :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#106,c_customer_id#107,c_current_cdemo_sk#108,c_current_hdemo_sk#109,c_current_addr_sk#110,c_first_shipto_date_sk#111,c_first_sales_date_sk#112,c_salutation#113,c_first_name#114,c_last_name#115,c_preferred_cust_flag#116,c_birth_day#117,c_birth_month#118,c_birth_year#119,c_birth_country#120,c_login#121,c_email_address#122,c_last_review_date#123] parquet
               +- SubqueryAlias current_addr
                  +- SubqueryAlias spark_catalog.tpcds.customer_address
                     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#124,ca_address_id#125,ca_street_number#126,ca_street_name#127,ca_street_type#128,ca_suite_number#129,ca_city#130,ca_county#131,ca_state#132,ca_zip#133,ca_country#134,ca_gmt_offset#135,ca_location_type#136] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#115 ASC NULLS FIRST, c_first_name#114 ASC NULLS FIRST, ca_city#130 ASC NULLS FIRST, bought_city#0 ASC NULLS FIRST, ss_ticket_number#17 ASC NULLS FIRST], true
      +- Project [c_last_name#115, c_first_name#114, ca_city#130, bought_city#0, ss_ticket_number#17, amt#1, profit#2]
         +- Join Inner, ((c_current_addr_sk#110 = ca_address_sk#124) AND NOT (ca_city#130 = bought_city#0))
            :- Project [ss_ticket_number#17, bought_city#0, amt#1, profit#2, c_current_addr_sk#110, c_first_name#114, c_last_name#115]
            :  +- Join Inner, (ss_customer_sk#11 = c_customer_sk#106)
            :     :- Aggregate [ss_ticket_number#17, ss_customer_sk#11, ss_addr_sk#14, ca_city#99], [ss_ticket_number#17, ss_customer_sk#11, ca_city#99 AS bought_city#0, sum(ss_coupon_amt#27) AS amt#1, sum(ss_net_profit#30) AS profit#2]
            :     :  +- Project [ss_customer_sk#11, ss_addr_sk#14, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30, ca_city#99]
            :     :     +- Join Inner, (ss_addr_sk#14 = ca_address_sk#93)
            :     :        :- Project [ss_customer_sk#11, ss_addr_sk#14, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30]
            :     :        :  +- Join Inner, (ss_hdemo_sk#13 = hd_demo_sk#88)
            :     :        :     :- Project [ss_customer_sk#11, ss_hdemo_sk#13, ss_addr_sk#14, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30]
            :     :        :     :  +- Join Inner, (ss_store_sk#15 = s_store_sk#59)
            :     :        :     :     :- Project [ss_customer_sk#11, ss_hdemo_sk#13, ss_addr_sk#14, ss_store_sk#15, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30]
            :     :        :     :     :  +- Join Inner, (ss_sold_date_sk#8 = d_date_sk#31)
            :     :        :     :     :     :- Project [ss_sold_date_sk#8, ss_customer_sk#11, ss_hdemo_sk#13, ss_addr_sk#14, ss_store_sk#15, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30]
            :     :        :     :     :     :  +- Filter ((isnotnull(ss_sold_date_sk#8) AND isnotnull(ss_store_sk#15)) AND ((isnotnull(ss_hdemo_sk#13) AND isnotnull(ss_addr_sk#14)) AND isnotnull(ss_customer_sk#11)))
            :     :        :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
            :     :        :     :     :     +- Project [d_date_sk#31]
            :     :        :     :     :        +- Filter ((d_dow#38 IN (6,0) AND d_year#37 IN (1999,2000,2001)) AND isnotnull(d_date_sk#31))
            :     :        :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#31,d_date_id#32,d_date#33,d_month_seq#34,d_week_seq#35,d_quarter_seq#36,d_year#37,d_dow#38,d_moy#39,d_dom#40,d_qoy#41,d_fy_year#42,d_fy_quarter_seq#43,d_fy_week_seq#44,d_day_name#45,d_quarter_name#46,d_holiday#47,d_weekend#48,d_following_holiday#49,d_first_dom#50,d_last_dom#51,d_same_day_ly#52,d_same_day_lq#53,d_current_day#54,... 4 more fields] parquet
            :     :        :     :     +- Project [s_store_sk#59]
            :     :        :     :        +- Filter (s_city#81 IN (Fairview,Midway) AND isnotnull(s_store_sk#59))
            :     :        :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#59,s_store_id#60,s_rec_start_date#61,s_rec_end_date#62,s_closed_date_sk#63,s_store_name#64,s_number_employees#65,s_floor_space#66,s_hours#67,s_manager#68,s_market_id#69,s_geography_class#70,s_market_desc#71,s_market_manager#72,s_division_id#73,s_division_name#74,s_company_id#75,s_company_name#76,s_street_number#77,s_street_name#78,s_street_type#79,s_suite_number#80,s_city#81,s_county#82,... 5 more fields] parquet
            :     :        :     +- Project [hd_demo_sk#88]
            :     :        :        +- Filter (((hd_dep_count#91 = 4) OR (hd_vehicle_count#92 = 3)) AND isnotnull(hd_demo_sk#88))
            :     :        :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#88,hd_income_band_sk#89,hd_buy_potential#90,hd_dep_count#91,hd_vehicle_count#92] parquet
            :     :        +- Project [ca_address_sk#93, ca_city#99]
            :     :           +- Filter (isnotnull(ca_address_sk#93) AND isnotnull(ca_city#99))
            :     :              +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#93,ca_address_id#94,ca_street_number#95,ca_street_name#96,ca_street_type#97,ca_suite_number#98,ca_city#99,ca_county#100,ca_state#101,ca_zip#102,ca_country#103,ca_gmt_offset#104,ca_location_type#105] parquet
            :     +- Project [c_customer_sk#106, c_current_addr_sk#110, c_first_name#114, c_last_name#115]
            :        +- Filter (isnotnull(c_customer_sk#106) AND isnotnull(c_current_addr_sk#110))
            :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#106,c_customer_id#107,c_current_cdemo_sk#108,c_current_hdemo_sk#109,c_current_addr_sk#110,c_first_shipto_date_sk#111,c_first_sales_date_sk#112,c_salutation#113,c_first_name#114,c_last_name#115,c_preferred_cust_flag#116,c_birth_day#117,c_birth_month#118,c_birth_year#119,c_birth_country#120,c_login#121,c_email_address#122,c_last_review_date#123] parquet
            +- Project [ca_address_sk#124, ca_city#130]
               +- Filter (isnotnull(ca_address_sk#124) AND isnotnull(ca_city#130))
                  +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#124,ca_address_id#125,ca_street_number#126,ca_street_name#127,ca_street_type#128,ca_suite_number#129,ca_city#130,ca_county#131,ca_state#132,ca_zip#133,ca_country#134,ca_gmt_offset#135,ca_location_type#136] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[c_last_name#115 ASC NULLS FIRST,c_first_name#114 ASC NULLS FIRST,ca_city#130 ASC NULLS FIRST,bought_city#0 ASC NULLS FIRST,ss_ticket_number#17 ASC NULLS FIRST], output=[c_last_name#115,c_first_name#114,ca_city#130,bought_city#0,ss_ticket_number#17,amt#1,profit#2])
   +- Project [c_last_name#115, c_first_name#114, ca_city#130, bought_city#0, ss_ticket_number#17, amt#1, profit#2]
      +- BroadcastHashJoin [c_current_addr_sk#110], [ca_address_sk#124], Inner, BuildRight, NOT (ca_city#130 = bought_city#0), false
         :- Project [ss_ticket_number#17, bought_city#0, amt#1, profit#2, c_current_addr_sk#110, c_first_name#114, c_last_name#115]
         :  +- BroadcastHashJoin [ss_customer_sk#11], [c_customer_sk#106], Inner, BuildRight, false
         :     :- HashAggregate(keys=[ss_ticket_number#17, ss_customer_sk#11, ss_addr_sk#14, ca_city#99], functions=[sum(ss_coupon_amt#27), sum(ss_net_profit#30)], output=[ss_ticket_number#17, ss_customer_sk#11, bought_city#0, amt#1, profit#2])
         :     :  +- Exchange hashpartitioning(ss_ticket_number#17, ss_customer_sk#11, ss_addr_sk#14, ca_city#99, 200), ENSURE_REQUIREMENTS, [plan_id=152]
         :     :     +- HashAggregate(keys=[ss_ticket_number#17, ss_customer_sk#11, ss_addr_sk#14, ca_city#99], functions=[partial_sum(ss_coupon_amt#27), partial_sum(ss_net_profit#30)], output=[ss_ticket_number#17, ss_customer_sk#11, ss_addr_sk#14, ca_city#99, sum#148, sum#149])
         :     :        +- Project [ss_customer_sk#11, ss_addr_sk#14, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30, ca_city#99]
         :     :           +- BroadcastHashJoin [ss_addr_sk#14], [ca_address_sk#93], Inner, BuildRight, false
         :     :              :- Project [ss_customer_sk#11, ss_addr_sk#14, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30]
         :     :              :  +- BroadcastHashJoin [ss_hdemo_sk#13], [hd_demo_sk#88], Inner, BuildRight, false
         :     :              :     :- Project [ss_customer_sk#11, ss_hdemo_sk#13, ss_addr_sk#14, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30]
         :     :              :     :  +- BroadcastHashJoin [ss_store_sk#15], [s_store_sk#59], Inner, BuildRight, false
         :     :              :     :     :- Project [ss_customer_sk#11, ss_hdemo_sk#13, ss_addr_sk#14, ss_store_sk#15, ss_ticket_number#17, ss_coupon_amt#27, ss_net_profit#30]
         :     :              :     :     :  +- BroadcastHashJoin [ss_sold_date_sk#8], [d_date_sk#31], Inner, BuildRight, false
         :     :              :     :     :     :- Filter ((((isnotnull(ss_sold_date_sk#8) AND isnotnull(ss_store_sk#15)) AND isnotnull(ss_hdemo_sk#13)) AND isnotnull(ss_addr_sk#14)) AND isnotnull(ss_customer_sk#11))
         :     :              :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_customer_sk#11,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_ticket_number#17,ss_coupon_amt#27,ss_net_profit#30] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#8), isnotnull(ss_store_sk#15), isnotnull(ss_hdemo_sk#13), isnotnull(ss..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_hdemo_sk), IsNotNull(ss_addr_sk..., ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_hdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_t...
         :     :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=135]
         :     :              :     :     :        +- Project [d_date_sk#31]
         :     :              :     :     :           +- Filter ((d_dow#38 IN (6,0) AND d_year#37 IN (1999,2000,2001)) AND isnotnull(d_date_sk#31))
         :     :              :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#31,d_year#37,d_dow#38] Batched: true, DataFilters: [d_dow#38 IN (6,0), d_year#37 IN (1999,2000,2001), isnotnull(d_date_sk#31)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_dow, [0,6]), In(d_year, [1999,2000,2001]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_dow:int>
         :     :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=139]
         :     :              :     :        +- Project [s_store_sk#59]
         :     :              :     :           +- Filter (s_city#81 IN (Fairview,Midway) AND isnotnull(s_store_sk#59))
         :     :              :     :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#59,s_city#81] Batched: true, DataFilters: [s_city#81 IN (Fairview,Midway), isnotnull(s_store_sk#59)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(s_city, [Fairview,Midway]), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_city:string>
         :     :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=143]
         :     :              :        +- Project [hd_demo_sk#88]
         :     :              :           +- Filter (((hd_dep_count#91 = 4) OR (hd_vehicle_count#92 = 3)) AND isnotnull(hd_demo_sk#88))
         :     :              :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#88,hd_dep_count#91,hd_vehicle_count#92] Batched: true, DataFilters: [((hd_dep_count#91 = 4) OR (hd_vehicle_count#92 = 3)), isnotnull(hd_demo_sk#88)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(EqualTo(hd_dep_count,4),EqualTo(hd_vehicle_count,3)), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
         :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=147]
         :     :                 +- Filter (isnotnull(ca_address_sk#93) AND isnotnull(ca_city#99))
         :     :                    +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#93,ca_city#99] Batched: true, DataFilters: [isnotnull(ca_address_sk#93), isnotnull(ca_city#99)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_city)], ReadSchema: struct<ca_address_sk:int,ca_city:string>
         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=155]
         :        +- Filter (isnotnull(c_customer_sk#106) AND isnotnull(c_current_addr_sk#110))
         :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#106,c_current_addr_sk#110,c_first_name#114,c_last_name#115] Batched: true, DataFilters: [isnotnull(c_customer_sk#106), isnotnull(c_current_addr_sk#110)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string>
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=159]
            +- Filter (isnotnull(ca_address_sk#124) AND isnotnull(ca_city#130))
               +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#124,ca_city#130] Batched: true, DataFilters: [isnotnull(ca_address_sk#124), isnotnull(ca_city#130)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_city)], ReadSchema: struct<ca_address_sk:int,ca_city:string>

Time taken: 2.842 seconds, Fetched 1 row(s)
