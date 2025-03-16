Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582171762
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['c_last_name ASC NULLS FIRST, 'ss_ticket_number ASC NULLS FIRST], true
      +- 'Project ['c_last_name, 'c_first_name, 'ca_city, 'bought_city, 'ss_ticket_number, 'extended_price, 'extended_tax, 'list_price]
         +- 'Filter ((('ss_customer_sk = 'c_customer_sk) AND ('customer.c_current_addr_sk = 'current_addr.ca_address_sk)) AND NOT ('current_addr.ca_city = 'bought_city))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias dn
               :  :  +- 'Aggregate ['ss_ticket_number, 'ss_customer_sk, 'ss_addr_sk, 'ca_city], ['ss_ticket_number, 'ss_customer_sk, 'ca_city AS bought_city#0, 'sum('ss_ext_sales_price) AS extended_price#1, 'sum('ss_ext_list_price) AS list_price#2, 'sum('ss_ext_tax) AS extended_tax#3]
               :  :     +- 'Filter (((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) AND ('store_sales.ss_store_sk = 'store.s_store_sk)) AND (('store_sales.ss_hdemo_sk = 'household_demographics.hd_demo_sk) AND ('store_sales.ss_addr_sk = 'customer_address.ca_address_sk))) AND (((('date_dim.d_dom >= 1) AND ('date_dim.d_dom <= 2)) AND (('household_demographics.hd_dep_count = 4) OR ('household_demographics.hd_vehicle_count = 3))) AND ('date_dim.d_year IN (1999,(1999 + 1),(1999 + 2)) AND 'store.s_city IN (Fairview,Midway))))
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
c_last_name: string, c_first_name: string, ca_city: string, bought_city: string, ss_ticket_number: int, extended_price: double, extended_tax: double, list_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#116 ASC NULLS FIRST, ss_ticket_number#18 ASC NULLS FIRST], true
      +- Project [c_last_name#116, c_first_name#115, ca_city#131, bought_city#0, ss_ticket_number#18, extended_price#1, extended_tax#3, list_price#2]
         +- Filter (((ss_customer_sk#12 = c_customer_sk#107) AND (c_current_addr_sk#111 = ca_address_sk#125)) AND NOT (ca_city#131 = bought_city#0))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias dn
               :  :  +- Aggregate [ss_ticket_number#18, ss_customer_sk#12, ss_addr_sk#15, ca_city#100], [ss_ticket_number#18, ss_customer_sk#12, ca_city#100 AS bought_city#0, sum(ss_ext_sales_price#24) AS extended_price#1, sum(ss_ext_list_price#26) AS list_price#2, sum(ss_ext_tax#27) AS extended_tax#3]
               :  :     +- Filter ((((ss_sold_date_sk#9 = d_date_sk#32) AND (ss_store_sk#16 = s_store_sk#60)) AND ((ss_hdemo_sk#14 = hd_demo_sk#89) AND (ss_addr_sk#15 = ca_address_sk#94))) AND ((((d_dom#41 >= 1) AND (d_dom#41 <= 2)) AND ((hd_dep_count#92 = 4) OR (hd_vehicle_count#93 = 3))) AND (d_year#38 IN (1999,(1999 + 1),(1999 + 2)) AND s_city#82 IN (Fairview,Midway))))
               :  :        +- Join Inner
               :  :           :- Join Inner
               :  :           :  :- Join Inner
               :  :           :  :  :- Join Inner
               :  :           :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :           :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
               :  :           :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :           :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
               :  :           :  :  +- SubqueryAlias spark_catalog.tpcds.store
               :  :           :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#60,s_store_id#61,s_rec_start_date#62,s_rec_end_date#63,s_closed_date_sk#64,s_store_name#65,s_number_employees#66,s_floor_space#67,s_hours#68,s_manager#69,s_market_id#70,s_geography_class#71,s_market_desc#72,s_market_manager#73,s_division_id#74,s_division_name#75,s_company_id#76,s_company_name#77,s_street_number#78,s_street_name#79,s_street_type#80,s_suite_number#81,s_city#82,s_county#83,... 5 more fields] parquet
               :  :           :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
               :  :           :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#89,hd_income_band_sk#90,hd_buy_potential#91,hd_dep_count#92,hd_vehicle_count#93] parquet
               :  :           +- SubqueryAlias spark_catalog.tpcds.customer_address
               :  :              +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#94,ca_address_id#95,ca_street_number#96,ca_street_name#97,ca_street_type#98,ca_suite_number#99,ca_city#100,ca_county#101,ca_state#102,ca_zip#103,ca_country#104,ca_gmt_offset#105,ca_location_type#106] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.customer
               :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#107,c_customer_id#108,c_current_cdemo_sk#109,c_current_hdemo_sk#110,c_current_addr_sk#111,c_first_shipto_date_sk#112,c_first_sales_date_sk#113,c_salutation#114,c_first_name#115,c_last_name#116,c_preferred_cust_flag#117,c_birth_day#118,c_birth_month#119,c_birth_year#120,c_birth_country#121,c_login#122,c_email_address#123,c_last_review_date#124] parquet
               +- SubqueryAlias current_addr
                  +- SubqueryAlias spark_catalog.tpcds.customer_address
                     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#125,ca_address_id#126,ca_street_number#127,ca_street_name#128,ca_street_type#129,ca_suite_number#130,ca_city#131,ca_county#132,ca_state#133,ca_zip#134,ca_country#135,ca_gmt_offset#136,ca_location_type#137] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#116 ASC NULLS FIRST, ss_ticket_number#18 ASC NULLS FIRST], true
      +- Project [c_last_name#116, c_first_name#115, ca_city#131, bought_city#0, ss_ticket_number#18, extended_price#1, extended_tax#3, list_price#2]
         +- Join Inner, ((c_current_addr_sk#111 = ca_address_sk#125) AND NOT (ca_city#131 = bought_city#0))
            :- Project [ss_ticket_number#18, bought_city#0, extended_price#1, list_price#2, extended_tax#3, c_current_addr_sk#111, c_first_name#115, c_last_name#116]
            :  +- Join Inner, (ss_customer_sk#12 = c_customer_sk#107)
            :     :- Aggregate [ss_ticket_number#18, ss_customer_sk#12, ss_addr_sk#15, ca_city#100], [ss_ticket_number#18, ss_customer_sk#12, ca_city#100 AS bought_city#0, sum(ss_ext_sales_price#24) AS extended_price#1, sum(ss_ext_list_price#26) AS list_price#2, sum(ss_ext_tax#27) AS extended_tax#3]
            :     :  +- Project [ss_customer_sk#12, ss_addr_sk#15, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27, ca_city#100]
            :     :     +- Join Inner, (ss_addr_sk#15 = ca_address_sk#94)
            :     :        :- Project [ss_customer_sk#12, ss_addr_sk#15, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27]
            :     :        :  +- Join Inner, (ss_hdemo_sk#14 = hd_demo_sk#89)
            :     :        :     :- Project [ss_customer_sk#12, ss_hdemo_sk#14, ss_addr_sk#15, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27]
            :     :        :     :  +- Join Inner, (ss_store_sk#16 = s_store_sk#60)
            :     :        :     :     :- Project [ss_customer_sk#12, ss_hdemo_sk#14, ss_addr_sk#15, ss_store_sk#16, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27]
            :     :        :     :     :  +- Join Inner, (ss_sold_date_sk#9 = d_date_sk#32)
            :     :        :     :     :     :- Project [ss_sold_date_sk#9, ss_customer_sk#12, ss_hdemo_sk#14, ss_addr_sk#15, ss_store_sk#16, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27]
            :     :        :     :     :     :  +- Filter ((isnotnull(ss_sold_date_sk#9) AND isnotnull(ss_store_sk#16)) AND ((isnotnull(ss_hdemo_sk#14) AND isnotnull(ss_addr_sk#15)) AND isnotnull(ss_customer_sk#12)))
            :     :        :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
            :     :        :     :     :     +- Project [d_date_sk#32]
            :     :        :     :     :        +- Filter ((isnotnull(d_dom#41) AND (((d_dom#41 >= 1) AND (d_dom#41 <= 2)) AND d_year#38 IN (1999,2000,2001))) AND isnotnull(d_date_sk#32))
            :     :        :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
            :     :        :     :     +- Project [s_store_sk#60]
            :     :        :     :        +- Filter (s_city#82 IN (Fairview,Midway) AND isnotnull(s_store_sk#60))
            :     :        :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#60,s_store_id#61,s_rec_start_date#62,s_rec_end_date#63,s_closed_date_sk#64,s_store_name#65,s_number_employees#66,s_floor_space#67,s_hours#68,s_manager#69,s_market_id#70,s_geography_class#71,s_market_desc#72,s_market_manager#73,s_division_id#74,s_division_name#75,s_company_id#76,s_company_name#77,s_street_number#78,s_street_name#79,s_street_type#80,s_suite_number#81,s_city#82,s_county#83,... 5 more fields] parquet
            :     :        :     +- Project [hd_demo_sk#89]
            :     :        :        +- Filter (((hd_dep_count#92 = 4) OR (hd_vehicle_count#93 = 3)) AND isnotnull(hd_demo_sk#89))
            :     :        :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#89,hd_income_band_sk#90,hd_buy_potential#91,hd_dep_count#92,hd_vehicle_count#93] parquet
            :     :        +- Project [ca_address_sk#94, ca_city#100]
            :     :           +- Filter (isnotnull(ca_address_sk#94) AND isnotnull(ca_city#100))
            :     :              +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#94,ca_address_id#95,ca_street_number#96,ca_street_name#97,ca_street_type#98,ca_suite_number#99,ca_city#100,ca_county#101,ca_state#102,ca_zip#103,ca_country#104,ca_gmt_offset#105,ca_location_type#106] parquet
            :     +- Project [c_customer_sk#107, c_current_addr_sk#111, c_first_name#115, c_last_name#116]
            :        +- Filter (isnotnull(c_customer_sk#107) AND isnotnull(c_current_addr_sk#111))
            :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#107,c_customer_id#108,c_current_cdemo_sk#109,c_current_hdemo_sk#110,c_current_addr_sk#111,c_first_shipto_date_sk#112,c_first_sales_date_sk#113,c_salutation#114,c_first_name#115,c_last_name#116,c_preferred_cust_flag#117,c_birth_day#118,c_birth_month#119,c_birth_year#120,c_birth_country#121,c_login#122,c_email_address#123,c_last_review_date#124] parquet
            +- Project [ca_address_sk#125, ca_city#131]
               +- Filter (isnotnull(ca_address_sk#125) AND isnotnull(ca_city#131))
                  +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#125,ca_address_id#126,ca_street_number#127,ca_street_name#128,ca_street_type#129,ca_suite_number#130,ca_city#131,ca_county#132,ca_state#133,ca_zip#134,ca_country#135,ca_gmt_offset#136,ca_location_type#137] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[c_last_name#116 ASC NULLS FIRST,ss_ticket_number#18 ASC NULLS FIRST], output=[c_last_name#116,c_first_name#115,ca_city#131,bought_city#0,ss_ticket_number#18,extended_price#1,extended_tax#3,list_price#2])
   +- Project [c_last_name#116, c_first_name#115, ca_city#131, bought_city#0, ss_ticket_number#18, extended_price#1, extended_tax#3, list_price#2]
      +- BroadcastHashJoin [c_current_addr_sk#111], [ca_address_sk#125], Inner, BuildRight, NOT (ca_city#131 = bought_city#0), false
         :- Project [ss_ticket_number#18, bought_city#0, extended_price#1, list_price#2, extended_tax#3, c_current_addr_sk#111, c_first_name#115, c_last_name#116]
         :  +- BroadcastHashJoin [ss_customer_sk#12], [c_customer_sk#107], Inner, BuildRight, false
         :     :- HashAggregate(keys=[ss_ticket_number#18, ss_customer_sk#12, ss_addr_sk#15, ca_city#100], functions=[sum(ss_ext_sales_price#24), sum(ss_ext_list_price#26), sum(ss_ext_tax#27)], output=[ss_ticket_number#18, ss_customer_sk#12, bought_city#0, extended_price#1, list_price#2, extended_tax#3])
         :     :  +- Exchange hashpartitioning(ss_ticket_number#18, ss_customer_sk#12, ss_addr_sk#15, ca_city#100, 200), ENSURE_REQUIREMENTS, [plan_id=152]
         :     :     +- HashAggregate(keys=[ss_ticket_number#18, ss_customer_sk#12, ss_addr_sk#15, ca_city#100], functions=[partial_sum(ss_ext_sales_price#24), partial_sum(ss_ext_list_price#26), partial_sum(ss_ext_tax#27)], output=[ss_ticket_number#18, ss_customer_sk#12, ss_addr_sk#15, ca_city#100, sum#151, sum#152, sum#153])
         :     :        +- Project [ss_customer_sk#12, ss_addr_sk#15, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27, ca_city#100]
         :     :           +- BroadcastHashJoin [ss_addr_sk#15], [ca_address_sk#94], Inner, BuildRight, false
         :     :              :- Project [ss_customer_sk#12, ss_addr_sk#15, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27]
         :     :              :  +- BroadcastHashJoin [ss_hdemo_sk#14], [hd_demo_sk#89], Inner, BuildRight, false
         :     :              :     :- Project [ss_customer_sk#12, ss_hdemo_sk#14, ss_addr_sk#15, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27]
         :     :              :     :  +- BroadcastHashJoin [ss_store_sk#16], [s_store_sk#60], Inner, BuildRight, false
         :     :              :     :     :- Project [ss_customer_sk#12, ss_hdemo_sk#14, ss_addr_sk#15, ss_store_sk#16, ss_ticket_number#18, ss_ext_sales_price#24, ss_ext_list_price#26, ss_ext_tax#27]
         :     :              :     :     :  +- BroadcastHashJoin [ss_sold_date_sk#9], [d_date_sk#32], Inner, BuildRight, false
         :     :              :     :     :     :- Filter ((((isnotnull(ss_sold_date_sk#9) AND isnotnull(ss_store_sk#16)) AND isnotnull(ss_hdemo_sk#14)) AND isnotnull(ss_addr_sk#15)) AND isnotnull(ss_customer_sk#12))
         :     :              :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#9,ss_customer_sk#12,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_ticket_number#18,ss_ext_sales_price#24,ss_ext_list_price#26,ss_ext_tax#27] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#9), isnotnull(ss_store_sk#16), isnotnull(ss_hdemo_sk#14), isnotnull(ss..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_hdemo_sk), IsNotNull(ss_addr_sk..., ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_hdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_t...
         :     :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=135]
         :     :              :     :     :        +- Project [d_date_sk#32]
         :     :              :     :     :           +- Filter ((((isnotnull(d_dom#41) AND (d_dom#41 >= 1)) AND (d_dom#41 <= 2)) AND d_year#38 IN (1999,2000,2001)) AND isnotnull(d_date_sk#32))
         :     :              :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#32,d_year#38,d_dom#41] Batched: true, DataFilters: [isnotnull(d_dom#41), (d_dom#41 >= 1), (d_dom#41 <= 2), d_year#38 IN (1999,2000,2001), isnotnull(..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_dom), GreaterThanOrEqual(d_dom,1), LessThanOrEqual(d_dom,2), In(d_year, [1999,2000,2..., ReadSchema: struct<d_date_sk:int,d_year:int,d_dom:int>
         :     :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=139]
         :     :              :     :        +- Project [s_store_sk#60]
         :     :              :     :           +- Filter (s_city#82 IN (Fairview,Midway) AND isnotnull(s_store_sk#60))
         :     :              :     :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#60,s_city#82] Batched: true, DataFilters: [s_city#82 IN (Fairview,Midway), isnotnull(s_store_sk#60)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(s_city, [Fairview,Midway]), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_city:string>
         :     :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=143]
         :     :              :        +- Project [hd_demo_sk#89]
         :     :              :           +- Filter (((hd_dep_count#92 = 4) OR (hd_vehicle_count#93 = 3)) AND isnotnull(hd_demo_sk#89))
         :     :              :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#89,hd_dep_count#92,hd_vehicle_count#93] Batched: true, DataFilters: [((hd_dep_count#92 = 4) OR (hd_vehicle_count#93 = 3)), isnotnull(hd_demo_sk#89)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(EqualTo(hd_dep_count,4),EqualTo(hd_vehicle_count,3)), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
         :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=147]
         :     :                 +- Filter (isnotnull(ca_address_sk#94) AND isnotnull(ca_city#100))
         :     :                    +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#94,ca_city#100] Batched: true, DataFilters: [isnotnull(ca_address_sk#94), isnotnull(ca_city#100)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_city)], ReadSchema: struct<ca_address_sk:int,ca_city:string>
         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=155]
         :        +- Filter (isnotnull(c_customer_sk#107) AND isnotnull(c_current_addr_sk#111))
         :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#107,c_current_addr_sk#111,c_first_name#115,c_last_name#116] Batched: true, DataFilters: [isnotnull(c_customer_sk#107), isnotnull(c_current_addr_sk#111)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string>
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=159]
            +- Filter (isnotnull(ca_address_sk#125) AND isnotnull(ca_city#131))
               +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#125,ca_city#131] Batched: true, DataFilters: [isnotnull(ca_address_sk#125), isnotnull(ca_city#131)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_city)], ReadSchema: struct<ca_address_sk:int,ca_city:string>

Time taken: 2.682 seconds, Fetched 1 row(s)
