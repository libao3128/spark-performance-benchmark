== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['c_last_name ASC NULLS FIRST, 'ss_ticket_number ASC NULLS FIRST], true
      +- 'Project ['c_last_name, 'c_first_name, 'ca_city, 'bought_city, 'ss_ticket_number, 'extended_price, 'extended_tax, 'list_price]
         +- 'Filter ((('ss_customer_sk = 'c_customer_sk) && ('customer.c_current_addr_sk = 'current_addr.ca_address_sk)) && NOT ('current_addr.ca_city = 'bought_city))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias `dn`
               :  :  +- 'Aggregate ['ss_ticket_number, 'ss_customer_sk, 'ss_addr_sk, 'ca_city], ['ss_ticket_number, 'ss_customer_sk, 'ca_city AS bought_city#0, 'sum('ss_ext_sales_price) AS extended_price#1, 'sum('ss_ext_list_price) AS list_price#2, 'sum('ss_ext_tax) AS extended_tax#3]
               :  :     +- 'Filter (((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) && ('store_sales.ss_store_sk = 'store.s_store_sk)) && (('store_sales.ss_hdemo_sk = 'household_demographics.hd_demo_sk) && ('store_sales.ss_addr_sk = 'customer_address.ca_address_sk))) && (((('date_dim.d_dom >= 1) && ('date_dim.d_dom <= 2)) && (('household_demographics.hd_dep_count = 4) || ('household_demographics.hd_vehicle_count = 3))) && ('date_dim.d_year IN (1999,(1999 + 1),(1999 + 2)) && 'store.s_city IN (Fairview,Midway))))
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
c_last_name: string, c_first_name: string, ca_city: string, bought_city: string, ss_ticket_number: int, extended_price: double, extended_tax: double, list_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#113 ASC NULLS FIRST, ss_ticket_number#15 ASC NULLS FIRST], true
      +- Project [c_last_name#113, c_first_name#112, ca_city#97, bought_city#0, ss_ticket_number#15, extended_price#1, extended_tax#3, list_price#2]
         +- Filter (((ss_customer_sk#9 = c_customer_sk#104) && (c_current_addr_sk#108 = ca_address_sk#91)) && NOT (ca_city#97 = bought_city#0))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `dn`
               :  :  +- Aggregate [ss_ticket_number#15, ss_customer_sk#9, ss_addr_sk#12, ca_city#97], [ss_ticket_number#15, ss_customer_sk#9, ca_city#97 AS bought_city#0, sum(ss_ext_sales_price#21) AS extended_price#1, sum(ss_ext_list_price#23) AS list_price#2, sum(ss_ext_tax#24) AS extended_tax#3]
               :  :     +- Filter ((((ss_sold_date_sk#6 = d_date_sk#29) && (ss_store_sk#13 = s_store_sk#57)) && ((ss_hdemo_sk#11 = hd_demo_sk#86) && (ss_addr_sk#12 = ca_address_sk#91))) && ((((d_dom#38 >= 1) && (d_dom#38 <= 2)) && ((hd_dep_count#89 = 4) || (hd_vehicle_count#90 = 3))) && (d_year#35 IN (1999,(1999 + 1),(1999 + 2)) && s_city#79 IN (Fairview,Midway))))
               :  :        +- Join Inner
               :  :           :- Join Inner
               :  :           :  :- Join Inner
               :  :           :  :  :- Join Inner
               :  :           :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
               :  :           :  :  :  :  +- Relation[ss_sold_date_sk#6,ss_sold_time_sk#7,ss_item_sk#8,ss_customer_sk#9,ss_cdemo_sk#10,ss_hdemo_sk#11,ss_addr_sk#12,ss_store_sk#13,ss_promo_sk#14,ss_ticket_number#15,ss_quantity#16,ss_wholesale_cost#17,ss_list_price#18,ss_sales_price#19,ss_ext_discount_amt#20,ss_ext_sales_price#21,ss_ext_wholesale_cost#22,ss_ext_list_price#23,ss_ext_tax#24,ss_coupon_amt#25,ss_net_paid#26,ss_net_paid_inc_tax#27,ss_net_profit#28] parquet
               :  :           :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
               :  :           :  :  :     +- Relation[d_date_sk#29,d_date_id#30,d_date#31,d_month_seq#32,d_week_seq#33,d_quarter_seq#34,d_year#35,d_dow#36,d_moy#37,d_dom#38,d_qoy#39,d_fy_year#40,d_fy_quarter_seq#41,d_fy_week_seq#42,d_day_name#43,d_quarter_name#44,d_holiday#45,d_weekend#46,d_following_holiday#47,d_first_dom#48,d_last_dom#49,d_same_day_ly#50,d_same_day_lq#51,d_current_day#52,... 4 more fields] parquet
               :  :           :  :  +- SubqueryAlias `tpcds`.`store`
               :  :           :  :     +- Relation[s_store_sk#57,s_store_id#58,s_rec_start_date#59,s_rec_end_date#60,s_closed_date_sk#61,s_store_name#62,s_number_employees#63,s_floor_space#64,s_hours#65,s_manager#66,s_market_id#67,s_geography_class#68,s_market_desc#69,s_market_manager#70,s_division_id#71,s_division_name#72,s_company_id#73,s_company_name#74,s_street_number#75,s_street_name#76,s_street_type#77,s_suite_number#78,s_city#79,s_county#80,... 5 more fields] parquet
               :  :           :  +- SubqueryAlias `tpcds`.`household_demographics`
               :  :           :     +- Relation[hd_demo_sk#86,hd_income_band_sk#87,hd_buy_potential#88,hd_dep_count#89,hd_vehicle_count#90] parquet
               :  :           +- SubqueryAlias `tpcds`.`customer_address`
               :  :              +- Relation[ca_address_sk#91,ca_address_id#92,ca_street_number#93,ca_street_name#94,ca_street_type#95,ca_suite_number#96,ca_city#97,ca_county#98,ca_state#99,ca_zip#100,ca_country#101,ca_gmt_offset#102,ca_location_type#103] parquet
               :  +- SubqueryAlias `tpcds`.`customer`
               :     +- Relation[c_customer_sk#104,c_customer_id#105,c_current_cdemo_sk#106,c_current_hdemo_sk#107,c_current_addr_sk#108,c_first_shipto_date_sk#109,c_first_sales_date_sk#110,c_salutation#111,c_first_name#112,c_last_name#113,c_preferred_cust_flag#114,c_birth_day#115,c_birth_month#116,c_birth_year#117,c_birth_country#118,c_login#119,c_email_address#120,c_last_review_date#121] parquet
               +- SubqueryAlias `current_addr`
                  +- SubqueryAlias `tpcds`.`customer_address`
                     +- Relation[ca_address_sk#91,ca_address_id#92,ca_street_number#93,ca_street_name#94,ca_street_type#95,ca_suite_number#96,ca_city#97,ca_county#98,ca_state#99,ca_zip#100,ca_country#101,ca_gmt_offset#102,ca_location_type#103] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_last_name#113 ASC NULLS FIRST, ss_ticket_number#15 ASC NULLS FIRST], true
      +- Project [c_last_name#113, c_first_name#112, ca_city#97, bought_city#0, ss_ticket_number#15, extended_price#1, extended_tax#3, list_price#2]
         +- Join Inner, ((c_current_addr_sk#108 = ca_address_sk#91) && NOT (ca_city#97 = bought_city#0))
            :- Project [ss_ticket_number#15, bought_city#0, extended_price#1, list_price#2, extended_tax#3, c_current_addr_sk#108, c_first_name#112, c_last_name#113]
            :  +- Join Inner, (ss_customer_sk#9 = c_customer_sk#104)
            :     :- Aggregate [ss_ticket_number#15, ss_customer_sk#9, ss_addr_sk#12, ca_city#97], [ss_ticket_number#15, ss_customer_sk#9, ca_city#97 AS bought_city#0, sum(ss_ext_sales_price#21) AS extended_price#1, sum(ss_ext_list_price#23) AS list_price#2, sum(ss_ext_tax#24) AS extended_tax#3]
            :     :  +- Project [ss_customer_sk#9, ss_addr_sk#12, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24, ca_city#97]
            :     :     +- Join Inner, (ss_addr_sk#12 = ca_address_sk#91)
            :     :        :- Project [ss_customer_sk#9, ss_addr_sk#12, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
            :     :        :  +- Join Inner, (ss_hdemo_sk#11 = hd_demo_sk#86)
            :     :        :     :- Project [ss_customer_sk#9, ss_hdemo_sk#11, ss_addr_sk#12, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
            :     :        :     :  +- Join Inner, (ss_store_sk#13 = s_store_sk#57)
            :     :        :     :     :- Project [ss_customer_sk#9, ss_hdemo_sk#11, ss_addr_sk#12, ss_store_sk#13, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
            :     :        :     :     :  +- Join Inner, (ss_sold_date_sk#6 = d_date_sk#29)
            :     :        :     :     :     :- Project [ss_sold_date_sk#6, ss_customer_sk#9, ss_hdemo_sk#11, ss_addr_sk#12, ss_store_sk#13, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
            :     :        :     :     :     :  +- Filter ((((isnotnull(ss_sold_date_sk#6) && isnotnull(ss_store_sk#13)) && isnotnull(ss_hdemo_sk#11)) && isnotnull(ss_addr_sk#12)) && isnotnull(ss_customer_sk#9))
            :     :        :     :     :     :     +- Relation[ss_sold_date_sk#6,ss_sold_time_sk#7,ss_item_sk#8,ss_customer_sk#9,ss_cdemo_sk#10,ss_hdemo_sk#11,ss_addr_sk#12,ss_store_sk#13,ss_promo_sk#14,ss_ticket_number#15,ss_quantity#16,ss_wholesale_cost#17,ss_list_price#18,ss_sales_price#19,ss_ext_discount_amt#20,ss_ext_sales_price#21,ss_ext_wholesale_cost#22,ss_ext_list_price#23,ss_ext_tax#24,ss_coupon_amt#25,ss_net_paid#26,ss_net_paid_inc_tax#27,ss_net_profit#28] parquet
            :     :        :     :     :     +- Project [d_date_sk#29]
            :     :        :     :     :        +- Filter ((((isnotnull(d_dom#38) && (d_dom#38 >= 1)) && (d_dom#38 <= 2)) && d_year#35 IN (1999,2000,2001)) && isnotnull(d_date_sk#29))
            :     :        :     :     :           +- Relation[d_date_sk#29,d_date_id#30,d_date#31,d_month_seq#32,d_week_seq#33,d_quarter_seq#34,d_year#35,d_dow#36,d_moy#37,d_dom#38,d_qoy#39,d_fy_year#40,d_fy_quarter_seq#41,d_fy_week_seq#42,d_day_name#43,d_quarter_name#44,d_holiday#45,d_weekend#46,d_following_holiday#47,d_first_dom#48,d_last_dom#49,d_same_day_ly#50,d_same_day_lq#51,d_current_day#52,... 4 more fields] parquet
            :     :        :     :     +- Project [s_store_sk#57]
            :     :        :     :        +- Filter (s_city#79 IN (Fairview,Midway) && isnotnull(s_store_sk#57))
            :     :        :     :           +- Relation[s_store_sk#57,s_store_id#58,s_rec_start_date#59,s_rec_end_date#60,s_closed_date_sk#61,s_store_name#62,s_number_employees#63,s_floor_space#64,s_hours#65,s_manager#66,s_market_id#67,s_geography_class#68,s_market_desc#69,s_market_manager#70,s_division_id#71,s_division_name#72,s_company_id#73,s_company_name#74,s_street_number#75,s_street_name#76,s_street_type#77,s_suite_number#78,s_city#79,s_county#80,... 5 more fields] parquet
            :     :        :     +- Project [hd_demo_sk#86]
            :     :        :        +- Filter (((hd_dep_count#89 = 4) || (hd_vehicle_count#90 = 3)) && isnotnull(hd_demo_sk#86))
            :     :        :           +- Relation[hd_demo_sk#86,hd_income_band_sk#87,hd_buy_potential#88,hd_dep_count#89,hd_vehicle_count#90] parquet
            :     :        +- Project [ca_address_sk#91, ca_city#97]
            :     :           +- Filter (isnotnull(ca_address_sk#91) && isnotnull(ca_city#97))
            :     :              +- Relation[ca_address_sk#91,ca_address_id#92,ca_street_number#93,ca_street_name#94,ca_street_type#95,ca_suite_number#96,ca_city#97,ca_county#98,ca_state#99,ca_zip#100,ca_country#101,ca_gmt_offset#102,ca_location_type#103] parquet
            :     +- Project [c_customer_sk#104, c_current_addr_sk#108, c_first_name#112, c_last_name#113]
            :        +- Filter (isnotnull(c_customer_sk#104) && isnotnull(c_current_addr_sk#108))
            :           +- Relation[c_customer_sk#104,c_customer_id#105,c_current_cdemo_sk#106,c_current_hdemo_sk#107,c_current_addr_sk#108,c_first_shipto_date_sk#109,c_first_sales_date_sk#110,c_salutation#111,c_first_name#112,c_last_name#113,c_preferred_cust_flag#114,c_birth_day#115,c_birth_month#116,c_birth_year#117,c_birth_country#118,c_login#119,c_email_address#120,c_last_review_date#121] parquet
            +- Project [ca_address_sk#91, ca_city#97]
               +- Filter (isnotnull(ca_address_sk#91) && isnotnull(ca_city#97))
                  +- Relation[ca_address_sk#91,ca_address_id#92,ca_street_number#93,ca_street_name#94,ca_street_type#95,ca_suite_number#96,ca_city#97,ca_county#98,ca_state#99,ca_zip#100,ca_country#101,ca_gmt_offset#102,ca_location_type#103] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[c_last_name#113 ASC NULLS FIRST,ss_ticket_number#15 ASC NULLS FIRST], output=[c_last_name#113,c_first_name#112,ca_city#97,bought_city#0,ss_ticket_number#15,extended_price#1,extended_tax#3,list_price#2])
+- *(8) Project [c_last_name#113, c_first_name#112, ca_city#97, bought_city#0, ss_ticket_number#15, extended_price#1, extended_tax#3, list_price#2]
   +- *(8) BroadcastHashJoin [c_current_addr_sk#108], [ca_address_sk#91], Inner, BuildRight, NOT (ca_city#97 = bought_city#0)
      :- *(8) Project [ss_ticket_number#15, bought_city#0, extended_price#1, list_price#2, extended_tax#3, c_current_addr_sk#108, c_first_name#112, c_last_name#113]
      :  +- *(8) BroadcastHashJoin [ss_customer_sk#9], [c_customer_sk#104], Inner, BuildRight
      :     :- *(8) HashAggregate(keys=[ss_ticket_number#15, ss_customer_sk#9, ss_addr_sk#12, ca_city#97], functions=[sum(ss_ext_sales_price#21), sum(ss_ext_list_price#23), sum(ss_ext_tax#24)], output=[ss_ticket_number#15, ss_customer_sk#9, bought_city#0, extended_price#1, list_price#2, extended_tax#3])
      :     :  +- Exchange hashpartitioning(ss_ticket_number#15, ss_customer_sk#9, ss_addr_sk#12, ca_city#97, 200)
      :     :     +- *(5) HashAggregate(keys=[ss_ticket_number#15, ss_customer_sk#9, ss_addr_sk#12, ca_city#97], functions=[partial_sum(ss_ext_sales_price#21), partial_sum(ss_ext_list_price#23), partial_sum(ss_ext_tax#24)], output=[ss_ticket_number#15, ss_customer_sk#9, ss_addr_sk#12, ca_city#97, sum#128, sum#129, sum#130])
      :     :        +- *(5) Project [ss_customer_sk#9, ss_addr_sk#12, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24, ca_city#97]
      :     :           +- *(5) BroadcastHashJoin [ss_addr_sk#12], [ca_address_sk#91], Inner, BuildRight
      :     :              :- *(5) Project [ss_customer_sk#9, ss_addr_sk#12, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
      :     :              :  +- *(5) BroadcastHashJoin [ss_hdemo_sk#11], [hd_demo_sk#86], Inner, BuildRight
      :     :              :     :- *(5) Project [ss_customer_sk#9, ss_hdemo_sk#11, ss_addr_sk#12, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
      :     :              :     :  +- *(5) BroadcastHashJoin [ss_store_sk#13], [s_store_sk#57], Inner, BuildRight
      :     :              :     :     :- *(5) Project [ss_customer_sk#9, ss_hdemo_sk#11, ss_addr_sk#12, ss_store_sk#13, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
      :     :              :     :     :  +- *(5) BroadcastHashJoin [ss_sold_date_sk#6], [d_date_sk#29], Inner, BuildRight
      :     :              :     :     :     :- *(5) Project [ss_sold_date_sk#6, ss_customer_sk#9, ss_hdemo_sk#11, ss_addr_sk#12, ss_store_sk#13, ss_ticket_number#15, ss_ext_sales_price#21, ss_ext_list_price#23, ss_ext_tax#24]
      :     :              :     :     :     :  +- *(5) Filter ((((isnotnull(ss_sold_date_sk#6) && isnotnull(ss_store_sk#13)) && isnotnull(ss_hdemo_sk#11)) && isnotnull(ss_addr_sk#12)) && isnotnull(ss_customer_sk#9))
      :     :              :     :     :     :     +- *(5) FileScan parquet tpcds.store_sales[ss_sold_date_sk#6,ss_customer_sk#9,ss_hdemo_sk#11,ss_addr_sk#12,ss_store_sk#13,ss_ticket_number#15,ss_ext_sales_price#21,ss_ext_list_price#23,ss_ext_tax#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_hdemo_sk), IsNotNull(ss_addr_sk..., ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_hdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_t...
      :     :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :              :     :     :        +- *(1) Project [d_date_sk#29]
      :     :              :     :     :           +- *(1) Filter ((((isnotnull(d_dom#38) && (d_dom#38 >= 1)) && (d_dom#38 <= 2)) && d_year#35 IN (1999,2000,2001)) && isnotnull(d_date_sk#29))
      :     :              :     :     :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#29,d_year#35,d_dom#38] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_dom), GreaterThanOrEqual(d_dom,1), LessThanOrEqual(d_dom,2), In(d_year, [1999,2000,2..., ReadSchema: struct<d_date_sk:int,d_year:int,d_dom:int>
      :     :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :              :     :        +- *(2) Project [s_store_sk#57]
      :     :              :     :           +- *(2) Filter (s_city#79 IN (Fairview,Midway) && isnotnull(s_store_sk#57))
      :     :              :     :              +- *(2) FileScan parquet tpcds.store[s_store_sk#57,s_city#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [In(s_city, [Fairview,Midway]), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_city:string>
      :     :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :              :        +- *(3) Project [hd_demo_sk#86]
      :     :              :           +- *(3) Filter (((hd_dep_count#89 = 4) || (hd_vehicle_count#90 = 3)) && isnotnull(hd_demo_sk#86))
      :     :              :              +- *(3) FileScan parquet tpcds.household_demographics[hd_demo_sk#86,hd_dep_count#89,hd_vehicle_count#90] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [Or(EqualTo(hd_dep_count,4),EqualTo(hd_vehicle_count,3)), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
      :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                 +- *(4) Project [ca_address_sk#91, ca_city#97]
      :     :                    +- *(4) Filter (isnotnull(ca_address_sk#91) && isnotnull(ca_city#97))
      :     :                       +- *(4) FileScan parquet tpcds.customer_address[ca_address_sk#91,ca_city#97] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_city)], ReadSchema: struct<ca_address_sk:int,ca_city:string>
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :        +- *(6) Project [c_customer_sk#104, c_current_addr_sk#108, c_first_name#112, c_last_name#113]
      :           +- *(6) Filter (isnotnull(c_customer_sk#104) && isnotnull(c_current_addr_sk#108))
      :              +- *(6) FileScan parquet tpcds.customer[c_customer_sk#104,c_current_addr_sk#108,c_first_name#112,c_last_name#113] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string>
      +- ReusedExchange [ca_address_sk#91, ca_city#97], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.099 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 68 in stream 0 using template query68.tpl
------------------------------------------------------^^^

