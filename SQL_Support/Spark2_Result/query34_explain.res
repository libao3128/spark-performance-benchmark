== Parsed Logical Plan ==
'Sort ['c_last_name ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'c_salutation ASC NULLS FIRST, 'c_preferred_cust_flag DESC NULLS LAST, 'ss_ticket_number ASC NULLS FIRST], true
+- 'Project ['c_last_name, 'c_first_name, 'c_salutation, 'c_preferred_cust_flag, 'ss_ticket_number, 'cnt]
   +- 'Filter (('ss_customer_sk = 'c_customer_sk) && (('cnt >= 15) && ('cnt <= 20)))
      +- 'Join Inner
         :- 'SubqueryAlias `dn`
         :  +- 'Aggregate ['ss_ticket_number, 'ss_customer_sk], ['ss_ticket_number, 'ss_customer_sk, 'count(1) AS cnt#0]
         :     +- 'Filter ((((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) && ('store_sales.ss_store_sk = 'store.s_store_sk)) && ('store_sales.ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (((('date_dim.d_dom >= 1) && ('date_dim.d_dom <= 3)) || (('date_dim.d_dom >= 25) && ('date_dim.d_dom <= 28))) && (('household_demographics.hd_buy_potential = >10000) || ('household_demographics.hd_buy_potential = Unknown)))) && ((('household_demographics.hd_vehicle_count > 0) && (CASE WHEN ('household_demographics.hd_vehicle_count > 0) THEN ('household_demographics.hd_dep_count / 'household_demographics.hd_vehicle_count) ELSE null END > 1.2)) && ('date_dim.d_year IN (1999,(1999 + 1),(1999 + 2)) && 'store.s_county IN (Williamson County,Williamson County,Williamson County,Williamson County,Williamson County,Williamson County,Williamson County,Williamson County))))
         :        +- 'Join Inner
         :           :- 'Join Inner
         :           :  :- 'Join Inner
         :           :  :  :- 'UnresolvedRelation `store_sales`
         :           :  :  +- 'UnresolvedRelation `date_dim`
         :           :  +- 'UnresolvedRelation `store`
         :           +- 'UnresolvedRelation `household_demographics`
         +- 'UnresolvedRelation `customer`

== Analyzed Logical Plan ==
c_last_name: string, c_first_name: string, c_salutation: string, c_preferred_cust_flag: string, ss_ticket_number: int, cnt: bigint
Sort [c_last_name#98 ASC NULLS FIRST, c_first_name#97 ASC NULLS FIRST, c_salutation#96 ASC NULLS FIRST, c_preferred_cust_flag#99 DESC NULLS LAST, ss_ticket_number#13 ASC NULLS FIRST], true
+- Project [c_last_name#98, c_first_name#97, c_salutation#96, c_preferred_cust_flag#99, ss_ticket_number#13, cnt#0L]
   +- Filter ((ss_customer_sk#7 = c_customer_sk#89) && ((cnt#0L >= cast(15 as bigint)) && (cnt#0L <= cast(20 as bigint))))
      +- Join Inner
         :- SubqueryAlias `dn`
         :  +- Aggregate [ss_ticket_number#13, ss_customer_sk#7], [ss_ticket_number#13, ss_customer_sk#7, count(1) AS cnt#0L]
         :     +- Filter (((((ss_sold_date_sk#4 = d_date_sk#27) && (ss_store_sk#11 = s_store_sk#55)) && (ss_hdemo_sk#9 = hd_demo_sk#84)) && ((((d_dom#36 >= 1) && (d_dom#36 <= 3)) || ((d_dom#36 >= 25) && (d_dom#36 <= 28))) && ((hd_buy_potential#86 = >10000) || (hd_buy_potential#86 = Unknown)))) && (((hd_vehicle_count#88 > 0) && (CASE WHEN (hd_vehicle_count#88 > 0) THEN (cast(hd_dep_count#87 as double) / cast(hd_vehicle_count#88 as double)) ELSE cast(null as double) END > cast(1.2 as double))) && (d_year#33 IN (1999,(1999 + 1),(1999 + 2)) && s_county#78 IN (Williamson County,Williamson County,Williamson County,Williamson County,Williamson County,Williamson County,Williamson County,Williamson County))))
         :        +- Join Inner
         :           :- Join Inner
         :           :  :- Join Inner
         :           :  :  :- SubqueryAlias `tpcds`.`store_sales`
         :           :  :  :  +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
         :           :  :  +- SubqueryAlias `tpcds`.`date_dim`
         :           :  :     +- Relation[d_date_sk#27,d_date_id#28,d_date#29,d_month_seq#30,d_week_seq#31,d_quarter_seq#32,d_year#33,d_dow#34,d_moy#35,d_dom#36,d_qoy#37,d_fy_year#38,d_fy_quarter_seq#39,d_fy_week_seq#40,d_day_name#41,d_quarter_name#42,d_holiday#43,d_weekend#44,d_following_holiday#45,d_first_dom#46,d_last_dom#47,d_same_day_ly#48,d_same_day_lq#49,d_current_day#50,... 4 more fields] parquet
         :           :  +- SubqueryAlias `tpcds`.`store`
         :           :     +- Relation[s_store_sk#55,s_store_id#56,s_rec_start_date#57,s_rec_end_date#58,s_closed_date_sk#59,s_store_name#60,s_number_employees#61,s_floor_space#62,s_hours#63,s_manager#64,s_market_id#65,s_geography_class#66,s_market_desc#67,s_market_manager#68,s_division_id#69,s_division_name#70,s_company_id#71,s_company_name#72,s_street_number#73,s_street_name#74,s_street_type#75,s_suite_number#76,s_city#77,s_county#78,... 5 more fields] parquet
         :           +- SubqueryAlias `tpcds`.`household_demographics`
         :              +- Relation[hd_demo_sk#84,hd_income_band_sk#85,hd_buy_potential#86,hd_dep_count#87,hd_vehicle_count#88] parquet
         +- SubqueryAlias `tpcds`.`customer`
            +- Relation[c_customer_sk#89,c_customer_id#90,c_current_cdemo_sk#91,c_current_hdemo_sk#92,c_current_addr_sk#93,c_first_shipto_date_sk#94,c_first_sales_date_sk#95,c_salutation#96,c_first_name#97,c_last_name#98,c_preferred_cust_flag#99,c_birth_day#100,c_birth_month#101,c_birth_year#102,c_birth_country#103,c_login#104,c_email_address#105,c_last_review_date#106] parquet

== Optimized Logical Plan ==
Sort [c_last_name#98 ASC NULLS FIRST, c_first_name#97 ASC NULLS FIRST, c_salutation#96 ASC NULLS FIRST, c_preferred_cust_flag#99 DESC NULLS LAST, ss_ticket_number#13 ASC NULLS FIRST], true
+- Project [c_last_name#98, c_first_name#97, c_salutation#96, c_preferred_cust_flag#99, ss_ticket_number#13, cnt#0L]
   +- Join Inner, (ss_customer_sk#7 = c_customer_sk#89)
      :- Filter ((cnt#0L >= 15) && (cnt#0L <= 20))
      :  +- Aggregate [ss_ticket_number#13, ss_customer_sk#7], [ss_ticket_number#13, ss_customer_sk#7, count(1) AS cnt#0L]
      :     +- Project [ss_customer_sk#7, ss_ticket_number#13]
      :        +- Join Inner, (ss_hdemo_sk#9 = hd_demo_sk#84)
      :           :- Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_ticket_number#13]
      :           :  +- Join Inner, (ss_store_sk#11 = s_store_sk#55)
      :           :     :- Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_store_sk#11, ss_ticket_number#13]
      :           :     :  +- Join Inner, (ss_sold_date_sk#4 = d_date_sk#27)
      :           :     :     :- Project [ss_sold_date_sk#4, ss_customer_sk#7, ss_hdemo_sk#9, ss_store_sk#11, ss_ticket_number#13]
      :           :     :     :  +- Filter (((isnotnull(ss_sold_date_sk#4) && isnotnull(ss_store_sk#11)) && isnotnull(ss_hdemo_sk#9)) && isnotnull(ss_customer_sk#7))
      :           :     :     :     +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
      :           :     :     +- Project [d_date_sk#27]
      :           :     :        +- Filter (((((d_dom#36 >= 1) && (d_dom#36 <= 3)) || ((d_dom#36 >= 25) && (d_dom#36 <= 28))) && d_year#33 IN (1999,2000,2001)) && isnotnull(d_date_sk#27))
      :           :     :           +- Relation[d_date_sk#27,d_date_id#28,d_date#29,d_month_seq#30,d_week_seq#31,d_quarter_seq#32,d_year#33,d_dow#34,d_moy#35,d_dom#36,d_qoy#37,d_fy_year#38,d_fy_quarter_seq#39,d_fy_week_seq#40,d_day_name#41,d_quarter_name#42,d_holiday#43,d_weekend#44,d_following_holiday#45,d_first_dom#46,d_last_dom#47,d_same_day_ly#48,d_same_day_lq#49,d_current_day#50,... 4 more fields] parquet
      :           :     +- Project [s_store_sk#55]
      :           :        +- Filter ((isnotnull(s_county#78) && (s_county#78 = Williamson County)) && isnotnull(s_store_sk#55))
      :           :           +- Relation[s_store_sk#55,s_store_id#56,s_rec_start_date#57,s_rec_end_date#58,s_closed_date_sk#59,s_store_name#60,s_number_employees#61,s_floor_space#62,s_hours#63,s_manager#64,s_market_id#65,s_geography_class#66,s_market_desc#67,s_market_manager#68,s_division_id#69,s_division_name#70,s_company_id#71,s_company_name#72,s_street_number#73,s_street_name#74,s_street_type#75,s_suite_number#76,s_city#77,s_county#78,... 5 more fields] parquet
      :           +- Project [hd_demo_sk#84]
      :              +- Filter ((((isnotnull(hd_vehicle_count#88) && ((hd_buy_potential#86 = >10000) || (hd_buy_potential#86 = Unknown))) && (hd_vehicle_count#88 > 0)) && (CASE WHEN (hd_vehicle_count#88 > 0) THEN (cast(hd_dep_count#87 as double) / cast(hd_vehicle_count#88 as double)) ELSE null END > 1.2)) && isnotnull(hd_demo_sk#84))
      :                 +- Relation[hd_demo_sk#84,hd_income_band_sk#85,hd_buy_potential#86,hd_dep_count#87,hd_vehicle_count#88] parquet
      +- Project [c_customer_sk#89, c_salutation#96, c_first_name#97, c_last_name#98, c_preferred_cust_flag#99]
         +- Filter isnotnull(c_customer_sk#89)
            +- Relation[c_customer_sk#89,c_customer_id#90,c_current_cdemo_sk#91,c_current_hdemo_sk#92,c_current_addr_sk#93,c_first_shipto_date_sk#94,c_first_sales_date_sk#95,c_salutation#96,c_first_name#97,c_last_name#98,c_preferred_cust_flag#99,c_birth_day#100,c_birth_month#101,c_birth_year#102,c_birth_country#103,c_login#104,c_email_address#105,c_last_review_date#106] parquet

== Physical Plan ==
*(7) Sort [c_last_name#98 ASC NULLS FIRST, c_first_name#97 ASC NULLS FIRST, c_salutation#96 ASC NULLS FIRST, c_preferred_cust_flag#99 DESC NULLS LAST, ss_ticket_number#13 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(c_last_name#98 ASC NULLS FIRST, c_first_name#97 ASC NULLS FIRST, c_salutation#96 ASC NULLS FIRST, c_preferred_cust_flag#99 DESC NULLS LAST, ss_ticket_number#13 ASC NULLS FIRST, 200)
   +- *(6) Project [c_last_name#98, c_first_name#97, c_salutation#96, c_preferred_cust_flag#99, ss_ticket_number#13, cnt#0L]
      +- *(6) BroadcastHashJoin [ss_customer_sk#7], [c_customer_sk#89], Inner, BuildRight
         :- *(6) Filter ((cnt#0L >= 15) && (cnt#0L <= 20))
         :  +- *(6) HashAggregate(keys=[ss_ticket_number#13, ss_customer_sk#7], functions=[count(1)], output=[ss_ticket_number#13, ss_customer_sk#7, cnt#0L])
         :     +- Exchange hashpartitioning(ss_ticket_number#13, ss_customer_sk#7, 200)
         :        +- *(4) HashAggregate(keys=[ss_ticket_number#13, ss_customer_sk#7], functions=[partial_count(1)], output=[ss_ticket_number#13, ss_customer_sk#7, count#108L])
         :           +- *(4) Project [ss_customer_sk#7, ss_ticket_number#13]
         :              +- *(4) BroadcastHashJoin [ss_hdemo_sk#9], [hd_demo_sk#84], Inner, BuildRight
         :                 :- *(4) Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_ticket_number#13]
         :                 :  +- *(4) BroadcastHashJoin [ss_store_sk#11], [s_store_sk#55], Inner, BuildRight
         :                 :     :- *(4) Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_store_sk#11, ss_ticket_number#13]
         :                 :     :  +- *(4) BroadcastHashJoin [ss_sold_date_sk#4], [d_date_sk#27], Inner, BuildRight
         :                 :     :     :- *(4) Project [ss_sold_date_sk#4, ss_customer_sk#7, ss_hdemo_sk#9, ss_store_sk#11, ss_ticket_number#13]
         :                 :     :     :  +- *(4) Filter (((isnotnull(ss_sold_date_sk#4) && isnotnull(ss_store_sk#11)) && isnotnull(ss_hdemo_sk#9)) && isnotnull(ss_customer_sk#7))
         :                 :     :     :     +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#4,ss_customer_sk#7,ss_hdemo_sk#9,ss_store_sk#11,ss_ticket_number#13] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_hdemo_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_hdemo_sk:int,ss_store_sk:int,ss_ticket_number:int>
         :                 :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                 :     :        +- *(1) Project [d_date_sk#27]
         :                 :     :           +- *(1) Filter (((((d_dom#36 >= 1) && (d_dom#36 <= 3)) || ((d_dom#36 >= 25) && (d_dom#36 <= 28))) && d_year#33 IN (1999,2000,2001)) && isnotnull(d_date_sk#27))
         :                 :     :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#27,d_year#33,d_dom#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [Or(And(GreaterThanOrEqual(d_dom,1),LessThanOrEqual(d_dom,3)),And(GreaterThanOrEqual(d_dom,25),Le..., ReadSchema: struct<d_date_sk:int,d_year:int,d_dom:int>
         :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                 :        +- *(2) Project [s_store_sk#55]
         :                 :           +- *(2) Filter ((isnotnull(s_county#78) && (s_county#78 = Williamson County)) && isnotnull(s_store_sk#55))
         :                 :              +- *(2) FileScan parquet tpcds.store[s_store_sk#55,s_county#78] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_county), EqualTo(s_county,Williamson County), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_county:string>
         :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                    +- *(3) Project [hd_demo_sk#84]
         :                       +- *(3) Filter ((((isnotnull(hd_vehicle_count#88) && ((hd_buy_potential#86 = >10000) || (hd_buy_potential#86 = Unknown))) && (hd_vehicle_count#88 > 0)) && (CASE WHEN (hd_vehicle_count#88 > 0) THEN (cast(hd_dep_count#87 as double) / cast(hd_vehicle_count#88 as double)) ELSE null END > 1.2)) && isnotnull(hd_demo_sk#84))
         :                          +- *(3) FileScan parquet tpcds.household_demographics[hd_demo_sk#84,hd_buy_potential#86,hd_dep_count#87,hd_vehicle_count#88] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_vehicle_count), Or(EqualTo(hd_buy_potential,>10000),EqualTo(hd_buy_potential,Unknow..., ReadSchema: struct<hd_demo_sk:int,hd_buy_potential:string,hd_dep_count:int,hd_vehicle_count:int>
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            +- *(5) Project [c_customer_sk#89, c_salutation#96, c_first_name#97, c_last_name#98, c_preferred_cust_flag#99]
               +- *(5) Filter isnotnull(c_customer_sk#89)
                  +- *(5) FileScan parquet tpcds.customer[c_customer_sk#89,c_salutation#96,c_first_name#97,c_last_name#98,c_preferred_cust_flag#99] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_salutation:string,c_first_name:string,c_last_name:string,c_preferred_c...
Time taken: 4.441 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 34 in stream 0 using template query34.tpl
------------------------------------------------------^^^

