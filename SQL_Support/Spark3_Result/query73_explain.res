Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582365537
== Parsed Logical Plan ==
'Sort ['cnt DESC NULLS LAST, 'c_last_name ASC NULLS FIRST], true
+- 'Project ['c_last_name, 'c_first_name, 'c_salutation, 'c_preferred_cust_flag, 'ss_ticket_number, 'cnt]
   +- 'Filter (('ss_customer_sk = 'c_customer_sk) AND (('cnt >= 1) AND ('cnt <= 5)))
      +- 'Join Inner
         :- 'SubqueryAlias dj
         :  +- 'Aggregate ['ss_ticket_number, 'ss_customer_sk], ['ss_ticket_number, 'ss_customer_sk, 'count(1) AS cnt#0]
         :     +- 'Filter ((((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) AND ('store_sales.ss_store_sk = 'store.s_store_sk)) AND ('store_sales.ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND ((('date_dim.d_dom >= 1) AND ('date_dim.d_dom <= 2)) AND (('household_demographics.hd_buy_potential = >10000) OR ('household_demographics.hd_buy_potential = Unknown)))) AND ((('household_demographics.hd_vehicle_count > 0) AND (CASE WHEN ('household_demographics.hd_vehicle_count > 0) THEN ('household_demographics.hd_dep_count / 'household_demographics.hd_vehicle_count) ELSE null END > 1)) AND ('date_dim.d_year IN (1999,(1999 + 1),(1999 + 2)) AND 'store.s_county IN (Williamson County,Franklin Parish,Bronx County,Orange County))))
         :        +- 'Join Inner
         :           :- 'Join Inner
         :           :  :- 'Join Inner
         :           :  :  :- 'UnresolvedRelation [store_sales], [], false
         :           :  :  +- 'UnresolvedRelation [date_dim], [], false
         :           :  +- 'UnresolvedRelation [store], [], false
         :           +- 'UnresolvedRelation [household_demographics], [], false
         +- 'UnresolvedRelation [customer], [], false

== Analyzed Logical Plan ==
c_last_name: string, c_first_name: string, c_salutation: string, c_preferred_cust_flag: string, ss_ticket_number: int, cnt: bigint
Sort [cnt#0L DESC NULLS LAST, c_last_name#101 ASC NULLS FIRST], true
+- Project [c_last_name#101, c_first_name#100, c_salutation#99, c_preferred_cust_flag#102, ss_ticket_number#16, cnt#0L]
   +- Filter ((ss_customer_sk#10 = c_customer_sk#92) AND ((cnt#0L >= cast(1 as bigint)) AND (cnt#0L <= cast(5 as bigint))))
      +- Join Inner
         :- SubqueryAlias dj
         :  +- Aggregate [ss_ticket_number#16, ss_customer_sk#10], [ss_ticket_number#16, ss_customer_sk#10, count(1) AS cnt#0L]
         :     +- Filter (((((ss_sold_date_sk#7 = d_date_sk#30) AND (ss_store_sk#14 = s_store_sk#58)) AND (ss_hdemo_sk#12 = hd_demo_sk#87)) AND (((d_dom#39 >= 1) AND (d_dom#39 <= 2)) AND ((hd_buy_potential#89 = >10000) OR (hd_buy_potential#89 = Unknown)))) AND (((hd_vehicle_count#91 > 0) AND (CASE WHEN (hd_vehicle_count#91 > 0) THEN (cast(hd_dep_count#90 as double) / cast(hd_vehicle_count#91 as double)) ELSE cast(null as double) END > cast(1 as double))) AND (d_year#36 IN (1999,(1999 + 1),(1999 + 2)) AND s_county#81 IN (Williamson County,Franklin Parish,Bronx County,Orange County))))
         :        +- Join Inner
         :           :- Join Inner
         :           :  :- Join Inner
         :           :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
         :           :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
         :           :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
         :           :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
         :           :  +- SubqueryAlias spark_catalog.tpcds.store
         :           :     +- Relation spark_catalog.tpcds.store[s_store_sk#58,s_store_id#59,s_rec_start_date#60,s_rec_end_date#61,s_closed_date_sk#62,s_store_name#63,s_number_employees#64,s_floor_space#65,s_hours#66,s_manager#67,s_market_id#68,s_geography_class#69,s_market_desc#70,s_market_manager#71,s_division_id#72,s_division_name#73,s_company_id#74,s_company_name#75,s_street_number#76,s_street_name#77,s_street_type#78,s_suite_number#79,s_city#80,s_county#81,... 5 more fields] parquet
         :           +- SubqueryAlias spark_catalog.tpcds.household_demographics
         :              +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#87,hd_income_band_sk#88,hd_buy_potential#89,hd_dep_count#90,hd_vehicle_count#91] parquet
         +- SubqueryAlias spark_catalog.tpcds.customer
            +- Relation spark_catalog.tpcds.customer[c_customer_sk#92,c_customer_id#93,c_current_cdemo_sk#94,c_current_hdemo_sk#95,c_current_addr_sk#96,c_first_shipto_date_sk#97,c_first_sales_date_sk#98,c_salutation#99,c_first_name#100,c_last_name#101,c_preferred_cust_flag#102,c_birth_day#103,c_birth_month#104,c_birth_year#105,c_birth_country#106,c_login#107,c_email_address#108,c_last_review_date#109] parquet

== Optimized Logical Plan ==
Sort [cnt#0L DESC NULLS LAST, c_last_name#101 ASC NULLS FIRST], true
+- Project [c_last_name#101, c_first_name#100, c_salutation#99, c_preferred_cust_flag#102, ss_ticket_number#16, cnt#0L]
   +- Join Inner, (ss_customer_sk#10 = c_customer_sk#92)
      :- Filter ((cnt#0L >= 1) AND (cnt#0L <= 5))
      :  +- Aggregate [ss_ticket_number#16, ss_customer_sk#10], [ss_ticket_number#16, ss_customer_sk#10, count(1) AS cnt#0L]
      :     +- Project [ss_customer_sk#10, ss_ticket_number#16]
      :        +- Join Inner, (ss_hdemo_sk#12 = hd_demo_sk#87)
      :           :- Project [ss_customer_sk#10, ss_hdemo_sk#12, ss_ticket_number#16]
      :           :  +- Join Inner, (ss_store_sk#14 = s_store_sk#58)
      :           :     :- Project [ss_customer_sk#10, ss_hdemo_sk#12, ss_store_sk#14, ss_ticket_number#16]
      :           :     :  +- Join Inner, (ss_sold_date_sk#7 = d_date_sk#30)
      :           :     :     :- Project [ss_sold_date_sk#7, ss_customer_sk#10, ss_hdemo_sk#12, ss_store_sk#14, ss_ticket_number#16]
      :           :     :     :  +- Filter (isnotnull(ss_sold_date_sk#7) AND ((isnotnull(ss_store_sk#14) AND isnotnull(ss_hdemo_sk#12)) AND isnotnull(ss_customer_sk#10)))
      :           :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
      :           :     :     +- Project [d_date_sk#30]
      :           :     :        +- Filter ((isnotnull(d_dom#39) AND (((d_dom#39 >= 1) AND (d_dom#39 <= 2)) AND d_year#36 IN (1999,2000,2001))) AND isnotnull(d_date_sk#30))
      :           :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
      :           :     +- Project [s_store_sk#58]
      :           :        +- Filter (s_county#81 IN (Williamson County,Franklin Parish,Bronx County,Orange County) AND isnotnull(s_store_sk#58))
      :           :           +- Relation spark_catalog.tpcds.store[s_store_sk#58,s_store_id#59,s_rec_start_date#60,s_rec_end_date#61,s_closed_date_sk#62,s_store_name#63,s_number_employees#64,s_floor_space#65,s_hours#66,s_manager#67,s_market_id#68,s_geography_class#69,s_market_desc#70,s_market_manager#71,s_division_id#72,s_division_name#73,s_company_id#74,s_company_name#75,s_street_number#76,s_street_name#77,s_street_type#78,s_suite_number#79,s_city#80,s_county#81,... 5 more fields] parquet
      :           +- Project [hd_demo_sk#87]
      :              +- Filter ((isnotnull(hd_vehicle_count#91) AND ((((hd_buy_potential#89 = >10000) OR (hd_buy_potential#89 = Unknown)) AND (hd_vehicle_count#91 > 0)) AND CASE WHEN (hd_vehicle_count#91 > 0) THEN ((cast(hd_dep_count#90 as double) / cast(hd_vehicle_count#91 as double)) > 1.0) END)) AND isnotnull(hd_demo_sk#87))
      :                 +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#87,hd_income_band_sk#88,hd_buy_potential#89,hd_dep_count#90,hd_vehicle_count#91] parquet
      +- Project [c_customer_sk#92, c_salutation#99, c_first_name#100, c_last_name#101, c_preferred_cust_flag#102]
         +- Filter isnotnull(c_customer_sk#92)
            +- Relation spark_catalog.tpcds.customer[c_customer_sk#92,c_customer_id#93,c_current_cdemo_sk#94,c_current_hdemo_sk#95,c_current_addr_sk#96,c_first_shipto_date_sk#97,c_first_sales_date_sk#98,c_salutation#99,c_first_name#100,c_last_name#101,c_preferred_cust_flag#102,c_birth_day#103,c_birth_month#104,c_birth_year#105,c_birth_country#106,c_login#107,c_email_address#108,c_last_review_date#109] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [cnt#0L DESC NULLS LAST, c_last_name#101 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(cnt#0L DESC NULLS LAST, c_last_name#101 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=126]
      +- Project [c_last_name#101, c_first_name#100, c_salutation#99, c_preferred_cust_flag#102, ss_ticket_number#16, cnt#0L]
         +- BroadcastHashJoin [ss_customer_sk#10], [c_customer_sk#92], Inner, BuildRight, false
            :- Filter ((cnt#0L >= 1) AND (cnt#0L <= 5))
            :  +- HashAggregate(keys=[ss_ticket_number#16, ss_customer_sk#10], functions=[count(1)], output=[ss_ticket_number#16, ss_customer_sk#10, cnt#0L])
            :     +- Exchange hashpartitioning(ss_ticket_number#16, ss_customer_sk#10, 200), ENSURE_REQUIREMENTS, [plan_id=118]
            :        +- HashAggregate(keys=[ss_ticket_number#16, ss_customer_sk#10], functions=[partial_count(1)], output=[ss_ticket_number#16, ss_customer_sk#10, count#116L])
            :           +- Project [ss_customer_sk#10, ss_ticket_number#16]
            :              +- BroadcastHashJoin [ss_hdemo_sk#12], [hd_demo_sk#87], Inner, BuildRight, false
            :                 :- Project [ss_customer_sk#10, ss_hdemo_sk#12, ss_ticket_number#16]
            :                 :  +- BroadcastHashJoin [ss_store_sk#14], [s_store_sk#58], Inner, BuildRight, false
            :                 :     :- Project [ss_customer_sk#10, ss_hdemo_sk#12, ss_store_sk#14, ss_ticket_number#16]
            :                 :     :  +- BroadcastHashJoin [ss_sold_date_sk#7], [d_date_sk#30], Inner, BuildRight, false
            :                 :     :     :- Filter (((isnotnull(ss_sold_date_sk#7) AND isnotnull(ss_store_sk#14)) AND isnotnull(ss_hdemo_sk#12)) AND isnotnull(ss_customer_sk#10))
            :                 :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_customer_sk#10,ss_hdemo_sk#12,ss_store_sk#14,ss_ticket_number#16] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#7), isnotnull(ss_store_sk#14), isnotnull(ss_hdemo_sk#12), isnotnull(ss..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_hdemo_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_hdemo_sk:int,ss_store_sk:int,ss_ticket_number:int>
            :                 :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=105]
            :                 :     :        +- Project [d_date_sk#30]
            :                 :     :           +- Filter ((((isnotnull(d_dom#39) AND (d_dom#39 >= 1)) AND (d_dom#39 <= 2)) AND d_year#36 IN (1999,2000,2001)) AND isnotnull(d_date_sk#30))
            :                 :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#30,d_year#36,d_dom#39] Batched: true, DataFilters: [isnotnull(d_dom#39), (d_dom#39 >= 1), (d_dom#39 <= 2), d_year#36 IN (1999,2000,2001), isnotnull(..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_dom), GreaterThanOrEqual(d_dom,1), LessThanOrEqual(d_dom,2), In(d_year, [1999,2000,2..., ReadSchema: struct<d_date_sk:int,d_year:int,d_dom:int>
            :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=109]
            :                 :        +- Project [s_store_sk#58]
            :                 :           +- Filter (s_county#81 IN (Williamson County,Franklin Parish,Bronx County,Orange County) AND isnotnull(s_store_sk#58))
            :                 :              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#58,s_county#81] Batched: true, DataFilters: [s_county#81 IN (Williamson County,Franklin Parish,Bronx County,Orange County), isnotnull(s_store..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(s_county, [Bronx County,Franklin Parish,Orange County,Williamson County]), IsNotNull(s_store_..., ReadSchema: struct<s_store_sk:int,s_county:string>
            :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=113]
            :                    +- Project [hd_demo_sk#87]
            :                       +- Filter ((((isnotnull(hd_vehicle_count#91) AND ((hd_buy_potential#89 = >10000) OR (hd_buy_potential#89 = Unknown))) AND (hd_vehicle_count#91 > 0)) AND CASE WHEN (hd_vehicle_count#91 > 0) THEN ((cast(hd_dep_count#90 as double) / cast(hd_vehicle_count#91 as double)) > 1.0) END) AND isnotnull(hd_demo_sk#87))
            :                          +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#87,hd_buy_potential#89,hd_dep_count#90,hd_vehicle_count#91] Batched: true, DataFilters: [isnotnull(hd_vehicle_count#91), ((hd_buy_potential#89 = >10000) OR (hd_buy_potential#89 = Unknow..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_vehicle_count), Or(EqualTo(hd_buy_potential,>10000),EqualTo(hd_buy_potential,Unknow..., ReadSchema: struct<hd_demo_sk:int,hd_buy_potential:string,hd_dep_count:int,hd_vehicle_count:int>
            +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=122]
               +- Filter isnotnull(c_customer_sk#92)
                  +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#92,c_salutation#99,c_first_name#100,c_last_name#101,c_preferred_cust_flag#102] Batched: true, DataFilters: [isnotnull(c_customer_sk#92)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_salutation:string,c_first_name:string,c_last_name:string,c_preferred_c...

Time taken: 2.692 seconds, Fetched 1 row(s)
