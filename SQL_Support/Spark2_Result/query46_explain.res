== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['c_last_name ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'ca_city ASC NULLS FIRST, 'bought_city ASC NULLS FIRST, 'ss_ticket_number ASC NULLS FIRST], true
      +- 'Project ['c_last_name, 'c_first_name, 'ca_city, 'bought_city, 'ss_ticket_number, 'amt, 'profit]
         +- 'Filter ((('ss_customer_sk = 'c_customer_sk) && ('customer.c_current_addr_sk = 'current_addr.ca_address_sk)) && NOT ('current_addr.ca_city = 'bought_city))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias `dn`
               :  :  +- 'Aggregate ['ss_ticket_number, 'ss_customer_sk, 'ss_addr_sk, 'ca_city], ['ss_ticket_number, 'ss_customer_sk, 'ca_city AS bought_city#0, 'sum('ss_coupon_amt) AS amt#1, 'sum('ss_net_profit) AS profit#2]
               :  :     +- 'Filter (((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) && ('store_sales.ss_store_sk = 'store.s_store_sk)) && (('store_sales.ss_hdemo_sk = 'household_demographics.hd_demo_sk) && ('store_sales.ss_addr_sk = 'customer_address.ca_address_sk))) && (((('household_demographics.hd_dep_count = 4) || ('household_demographics.hd_vehicle_count = 3)) && 'date_dim.d_dow IN (6,0)) && ('date_dim.d_year IN (1999,(1999 + 1),(1999 + 2)) && 'store.s_city IN (Fairview,Midway,Fairview,Fairview,Fairview))))
               :  :        +- 'Join Inner
               :  :           :- 'Join Inner
               :  :           :  :- 'Join Inner
               :  :           :  :  :- 'Join Inner
               :  :           :  :  :  :- 'UnresolvedRelation `store_sales`
               :  :           :  :  :  +- 'UnresolvedRelation `date_dim`
               :  :           :  :  +- 'UnresolvedRelation `store`
               :  :           :  +- 'UnresolvedRelation `household_demographics`
               :  :           +- 'UnresolvedRelation `customer_address`
               :  +- 'UnresolvedRelation `customer`
               +- 'SubqueryAlias `current_addr`
                  +- 'UnresolvedRelation `customer_address`

== Analyzed Logical Plan ==
c_last_name: string, c_first_name: string, ca_city: string, bought_city: string, ss_ticket_number: int, amt: double, profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#112 ASC NULLS FIRST, c_first_name#111 ASC NULLS FIRST, ca_city#96 ASC NULLS FIRST, bought_city#0 ASC NULLS FIRST, ss_ticket_number#14 ASC NULLS FIRST], true
      +- Project [c_last_name#112, c_first_name#111, ca_city#96, bought_city#0, ss_ticket_number#14, amt#1, profit#2]
         +- Filter (((ss_customer_sk#8 = c_customer_sk#103) && (c_current_addr_sk#107 = ca_address_sk#90)) && NOT (ca_city#96 = bought_city#0))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `dn`
               :  :  +- Aggregate [ss_ticket_number#14, ss_customer_sk#8, ss_addr_sk#11, ca_city#96], [ss_ticket_number#14, ss_customer_sk#8, ca_city#96 AS bought_city#0, sum(ss_coupon_amt#24) AS amt#1, sum(ss_net_profit#27) AS profit#2]
               :  :     +- Filter ((((ss_sold_date_sk#5 = d_date_sk#28) && (ss_store_sk#12 = s_store_sk#56)) && ((ss_hdemo_sk#10 = hd_demo_sk#85) && (ss_addr_sk#11 = ca_address_sk#90))) && ((((hd_dep_count#88 = 4) || (hd_vehicle_count#89 = 3)) && d_dow#35 IN (6,0)) && (d_year#34 IN (1999,(1999 + 1),(1999 + 2)) && s_city#78 IN (Fairview,Midway,Fairview,Fairview,Fairview))))
               :  :        +- Join Inner
               :  :           :- Join Inner
               :  :           :  :- Join Inner
               :  :           :  :  :- Join Inner
               :  :           :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
               :  :           :  :  :  :  +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
               :  :           :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
               :  :           :  :  :     +- Relation[d_date_sk#28,d_date_id#29,d_date#30,d_month_seq#31,d_week_seq#32,d_quarter_seq#33,d_year#34,d_dow#35,d_moy#36,d_dom#37,d_qoy#38,d_fy_year#39,d_fy_quarter_seq#40,d_fy_week_seq#41,d_day_name#42,d_quarter_name#43,d_holiday#44,d_weekend#45,d_following_holiday#46,d_first_dom#47,d_last_dom#48,d_same_day_ly#49,d_same_day_lq#50,d_current_day#51,... 4 more fields] parquet
               :  :           :  :  +- SubqueryAlias `tpcds`.`store`
               :  :           :  :     +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
               :  :           :  +- SubqueryAlias `tpcds`.`household_demographics`
               :  :           :     +- Relation[hd_demo_sk#85,hd_income_band_sk#86,hd_buy_potential#87,hd_dep_count#88,hd_vehicle_count#89] parquet
               :  :           +- SubqueryAlias `tpcds`.`customer_address`
               :  :              +- Relation[ca_address_sk#90,ca_address_id#91,ca_street_number#92,ca_street_name#93,ca_street_type#94,ca_suite_number#95,ca_city#96,ca_county#97,ca_state#98,ca_zip#99,ca_country#100,ca_gmt_offset#101,ca_location_type#102] parquet
               :  +- SubqueryAlias `tpcds`.`customer`
               :     +- Relation[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet
               +- SubqueryAlias `current_addr`
                  +- SubqueryAlias `tpcds`.`customer_address`
                     +- Relation[ca_address_sk#90,ca_address_id#91,ca_street_number#92,ca_street_name#93,ca_street_type#94,ca_suite_number#95,ca_city#96,ca_county#97,ca_state#98,ca_zip#99,ca_country#100,ca_gmt_offset#101,ca_location_type#102] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#112 ASC NULLS FIRST, c_first_name#111 ASC NULLS FIRST, ca_city#96 ASC NULLS FIRST, bought_city#0 ASC NULLS FIRST, ss_ticket_number#14 ASC NULLS FIRST], true
      +- Project [c_last_name#112, c_first_name#111, ca_city#96, bought_city#0, ss_ticket_number#14, amt#1, profit#2]
         +- Join Inner, ((c_current_addr_sk#107 = ca_address_sk#90) && NOT (ca_city#96 = bought_city#0))
            :- Project [ss_ticket_number#14, bought_city#0, amt#1, profit#2, c_current_addr_sk#107, c_first_name#111, c_last_name#112]
            :  +- Join Inner, (ss_customer_sk#8 = c_customer_sk#103)
            :     :- Aggregate [ss_ticket_number#14, ss_customer_sk#8, ss_addr_sk#11, ca_city#96], [ss_ticket_number#14, ss_customer_sk#8, ca_city#96 AS bought_city#0, sum(ss_coupon_amt#24) AS amt#1, sum(ss_net_profit#27) AS profit#2]
            :     :  +- Project [ss_customer_sk#8, ss_addr_sk#11, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27, ca_city#96]
            :     :     +- Join Inner, (ss_addr_sk#11 = ca_address_sk#90)
            :     :        :- Project [ss_customer_sk#8, ss_addr_sk#11, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
            :     :        :  +- Join Inner, (ss_hdemo_sk#10 = hd_demo_sk#85)
            :     :        :     :- Project [ss_customer_sk#8, ss_hdemo_sk#10, ss_addr_sk#11, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
            :     :        :     :  +- Join Inner, (ss_store_sk#12 = s_store_sk#56)
            :     :        :     :     :- Project [ss_customer_sk#8, ss_hdemo_sk#10, ss_addr_sk#11, ss_store_sk#12, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
            :     :        :     :     :  +- Join Inner, (ss_sold_date_sk#5 = d_date_sk#28)
            :     :        :     :     :     :- Project [ss_sold_date_sk#5, ss_customer_sk#8, ss_hdemo_sk#10, ss_addr_sk#11, ss_store_sk#12, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
            :     :        :     :     :     :  +- Filter ((((isnotnull(ss_sold_date_sk#5) && isnotnull(ss_store_sk#12)) && isnotnull(ss_hdemo_sk#10)) && isnotnull(ss_addr_sk#11)) && isnotnull(ss_customer_sk#8))
            :     :        :     :     :     :     +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
            :     :        :     :     :     +- Project [d_date_sk#28]
            :     :        :     :     :        +- Filter ((d_dow#35 IN (6,0) && d_year#34 IN (1999,2000,2001)) && isnotnull(d_date_sk#28))
            :     :        :     :     :           +- Relation[d_date_sk#28,d_date_id#29,d_date#30,d_month_seq#31,d_week_seq#32,d_quarter_seq#33,d_year#34,d_dow#35,d_moy#36,d_dom#37,d_qoy#38,d_fy_year#39,d_fy_quarter_seq#40,d_fy_week_seq#41,d_day_name#42,d_quarter_name#43,d_holiday#44,d_weekend#45,d_following_holiday#46,d_first_dom#47,d_last_dom#48,d_same_day_ly#49,d_same_day_lq#50,d_current_day#51,... 4 more fields] parquet
            :     :        :     :     +- Project [s_store_sk#56]
            :     :        :     :        +- Filter (s_city#78 IN (Fairview,Midway) && isnotnull(s_store_sk#56))
            :     :        :     :           +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
            :     :        :     +- Project [hd_demo_sk#85]
            :     :        :        +- Filter (((hd_dep_count#88 = 4) || (hd_vehicle_count#89 = 3)) && isnotnull(hd_demo_sk#85))
            :     :        :           +- Relation[hd_demo_sk#85,hd_income_band_sk#86,hd_buy_potential#87,hd_dep_count#88,hd_vehicle_count#89] parquet
            :     :        +- Project [ca_address_sk#90, ca_city#96]
            :     :           +- Filter (isnotnull(ca_address_sk#90) && isnotnull(ca_city#96))
            :     :              +- Relation[ca_address_sk#90,ca_address_id#91,ca_street_number#92,ca_street_name#93,ca_street_type#94,ca_suite_number#95,ca_city#96,ca_county#97,ca_state#98,ca_zip#99,ca_country#100,ca_gmt_offset#101,ca_location_type#102] parquet
            :     +- Project [c_customer_sk#103, c_current_addr_sk#107, c_first_name#111, c_last_name#112]
            :        +- Filter (isnotnull(c_customer_sk#103) && isnotnull(c_current_addr_sk#107))
            :           +- Relation[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet
            +- Project [ca_address_sk#90, ca_city#96]
               +- Filter (isnotnull(ca_address_sk#90) && isnotnull(ca_city#96))
                  +- Relation[ca_address_sk#90,ca_address_id#91,ca_street_number#92,ca_street_name#93,ca_street_type#94,ca_suite_number#95,ca_city#96,ca_county#97,ca_state#98,ca_zip#99,ca_country#100,ca_gmt_offset#101,ca_location_type#102] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[c_last_name#112 ASC NULLS FIRST,c_first_name#111 ASC NULLS FIRST,ca_city#96 ASC NULLS FIRST,bought_city#0 ASC NULLS FIRST,ss_ticket_number#14 ASC NULLS FIRST], output=[c_last_name#112,c_first_name#111,ca_city#96,bought_city#0,ss_ticket_number#14,amt#1,profit#2])
+- *(8) Project [c_last_name#112, c_first_name#111, ca_city#96, bought_city#0, ss_ticket_number#14, amt#1, profit#2]
   +- *(8) BroadcastHashJoin [c_current_addr_sk#107], [ca_address_sk#90], Inner, BuildRight, NOT (ca_city#96 = bought_city#0)
      :- *(8) Project [ss_ticket_number#14, bought_city#0, amt#1, profit#2, c_current_addr_sk#107, c_first_name#111, c_last_name#112]
      :  +- *(8) BroadcastHashJoin [ss_customer_sk#8], [c_customer_sk#103], Inner, BuildRight
      :     :- *(8) HashAggregate(keys=[ss_ticket_number#14, ss_customer_sk#8, ss_addr_sk#11, ca_city#96], functions=[sum(ss_coupon_amt#24), sum(ss_net_profit#27)], output=[ss_ticket_number#14, ss_customer_sk#8, bought_city#0, amt#1, profit#2])
      :     :  +- Exchange hashpartitioning(ss_ticket_number#14, ss_customer_sk#8, ss_addr_sk#11, ca_city#96, 200)
      :     :     +- *(5) HashAggregate(keys=[ss_ticket_number#14, ss_customer_sk#8, ss_addr_sk#11, ca_city#96], functions=[partial_sum(ss_coupon_amt#24), partial_sum(ss_net_profit#27)], output=[ss_ticket_number#14, ss_customer_sk#8, ss_addr_sk#11, ca_city#96, sum#125, sum#126])
      :     :        +- *(5) Project [ss_customer_sk#8, ss_addr_sk#11, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27, ca_city#96]
      :     :           +- *(5) BroadcastHashJoin [ss_addr_sk#11], [ca_address_sk#90], Inner, BuildRight
      :     :              :- *(5) Project [ss_customer_sk#8, ss_addr_sk#11, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
      :     :              :  +- *(5) BroadcastHashJoin [ss_hdemo_sk#10], [hd_demo_sk#85], Inner, BuildRight
      :     :              :     :- *(5) Project [ss_customer_sk#8, ss_hdemo_sk#10, ss_addr_sk#11, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
      :     :              :     :  +- *(5) BroadcastHashJoin [ss_store_sk#12], [s_store_sk#56], Inner, BuildRight
      :     :              :     :     :- *(5) Project [ss_customer_sk#8, ss_hdemo_sk#10, ss_addr_sk#11, ss_store_sk#12, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
      :     :              :     :     :  +- *(5) BroadcastHashJoin [ss_sold_date_sk#5], [d_date_sk#28], Inner, BuildRight
      :     :              :     :     :     :- *(5) Project [ss_sold_date_sk#5, ss_customer_sk#8, ss_hdemo_sk#10, ss_addr_sk#11, ss_store_sk#12, ss_ticket_number#14, ss_coupon_amt#24, ss_net_profit#27]
      :     :              :     :     :     :  +- *(5) Filter ((((isnotnull(ss_sold_date_sk#5) && isnotnull(ss_store_sk#12)) && isnotnull(ss_hdemo_sk#10)) && isnotnull(ss_addr_sk#11)) && isnotnull(ss_customer_sk#8))
      :     :              :     :     :     :     +- *(5) FileScan parquet tpcds.store_sales[ss_sold_date_sk#5,ss_customer_sk#8,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_ticket_number#14,ss_coupon_amt#24,ss_net_profit#27] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_hdemo_sk), IsNotNull(ss_addr_sk..., ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_hdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_t...
      :     :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :              :     :     :        +- *(1) Project [d_date_sk#28]
      :     :              :     :     :           +- *(1) Filter ((d_dow#35 IN (6,0) && d_year#34 IN (1999,2000,2001)) && isnotnull(d_date_sk#28))
      :     :              :     :     :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#28,d_year#34,d_dow#35] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [In(d_dow, [6,0]), In(d_year, [1999,2000,2001]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_dow:int>
      :     :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :              :     :        +- *(2) Project [s_store_sk#56]
      :     :              :     :           +- *(2) Filter (s_city#78 IN (Fairview,Midway) && isnotnull(s_store_sk#56))
      :     :              :     :              +- *(2) FileScan parquet tpcds.store[s_store_sk#56,s_city#78] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [In(s_city, [Fairview,Midway]), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_city:string>
      :     :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :              :        +- *(3) Project [hd_demo_sk#85]
      :     :              :           +- *(3) Filter (((hd_dep_count#88 = 4) || (hd_vehicle_count#89 = 3)) && isnotnull(hd_demo_sk#85))
      :     :              :              +- *(3) FileScan parquet tpcds.household_demographics[hd_demo_sk#85,hd_dep_count#88,hd_vehicle_count#89] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [Or(EqualTo(hd_dep_count,4),EqualTo(hd_vehicle_count,3)), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
      :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                 +- *(4) Project [ca_address_sk#90, ca_city#96]
      :     :                    +- *(4) Filter (isnotnull(ca_address_sk#90) && isnotnull(ca_city#96))
      :     :                       +- *(4) FileScan parquet tpcds.customer_address[ca_address_sk#90,ca_city#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_city)], ReadSchema: struct<ca_address_sk:int,ca_city:string>
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :        +- *(6) Project [c_customer_sk#103, c_current_addr_sk#107, c_first_name#111, c_last_name#112]
      :           +- *(6) Filter (isnotnull(c_customer_sk#103) && isnotnull(c_current_addr_sk#107))
      :              +- *(6) FileScan parquet tpcds.customer[c_customer_sk#103,c_current_addr_sk#107,c_first_name#111,c_last_name#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string>
      +- ReusedExchange [ca_address_sk#90, ca_city#96], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.403 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 46 in stream 0 using template query46.tpl
------------------------------------------------------^^^

