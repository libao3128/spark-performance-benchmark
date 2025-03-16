== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['c_last_name ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'substr('s_city, 1, 30) ASC NULLS FIRST, 'profit ASC NULLS FIRST], true
      +- 'Project ['c_last_name, 'c_first_name, unresolvedalias('substr('s_city, 1, 30), None), 'ss_ticket_number, 'amt, 'profit]
         +- 'Filter ('ss_customer_sk = 'c_customer_sk)
            +- 'Join Inner
               :- 'SubqueryAlias `ms`
               :  +- 'Aggregate ['ss_ticket_number, 'ss_customer_sk, 'ss_addr_sk, 'store.s_city], ['ss_ticket_number, 'ss_customer_sk, 'store.s_city, 'sum('ss_coupon_amt) AS amt#0, 'sum('ss_net_profit) AS profit#1]
               :     +- 'Filter (((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) && ('store_sales.ss_store_sk = 'store.s_store_sk)) && (('store_sales.ss_hdemo_sk = 'household_demographics.hd_demo_sk) && (('household_demographics.hd_dep_count = 6) || ('household_demographics.hd_vehicle_count > 2)))) && ((('date_dim.d_dow = 1) && 'date_dim.d_year IN (1999,(1999 + 1),(1999 + 2))) && (('store.s_number_employees >= 200) && ('store.s_number_employees <= 295))))
               :        +- 'Join Inner
               :           :- 'Join Inner
               :           :  :- 'Join Inner
               :           :  :  :- 'UnresolvedRelation `store_sales`
               :           :  :  +- 'UnresolvedRelation `date_dim`
               :           :  +- 'UnresolvedRelation `store`
               :           +- 'UnresolvedRelation `household_demographics`
               +- 'UnresolvedRelation `customer`

== Analyzed Logical Plan ==
c_last_name: string, c_first_name: string, substring(s_city, 1, 30): string, ss_ticket_number: int, amt: double, profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [c_last_name#98, c_first_name#97, substring(s_city, 1, 30)#109, ss_ticket_number#13, amt#0, profit#1]
      +- Sort [c_last_name#98 ASC NULLS FIRST, c_first_name#97 ASC NULLS FIRST, substring(s_city#77, 1, 30) ASC NULLS FIRST, profit#1 ASC NULLS FIRST], true
         +- Project [c_last_name#98, c_first_name#97, substring(s_city#77, 1, 30) AS substring(s_city, 1, 30)#109, ss_ticket_number#13, amt#0, profit#1, s_city#77]
            +- Filter (ss_customer_sk#7 = c_customer_sk#89)
               +- Join Inner
                  :- SubqueryAlias `ms`
                  :  +- Aggregate [ss_ticket_number#13, ss_customer_sk#7, ss_addr_sk#10, s_city#77], [ss_ticket_number#13, ss_customer_sk#7, s_city#77, sum(ss_coupon_amt#23) AS amt#0, sum(ss_net_profit#26) AS profit#1]
                  :     +- Filter ((((ss_sold_date_sk#4 = d_date_sk#27) && (ss_store_sk#11 = s_store_sk#55)) && ((ss_hdemo_sk#9 = hd_demo_sk#84) && ((hd_dep_count#87 = 6) || (hd_vehicle_count#88 > 2)))) && (((d_dow#34 = 1) && d_year#33 IN (1999,(1999 + 1),(1999 + 2))) && ((s_number_employees#61 >= 200) && (s_number_employees#61 <= 295))))
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
GlobalLimit 100
+- LocalLimit 100
   +- Project [c_last_name#98, c_first_name#97, substring(s_city, 1, 30)#109, ss_ticket_number#13, amt#0, profit#1]
      +- Sort [c_last_name#98 ASC NULLS FIRST, c_first_name#97 ASC NULLS FIRST, substring(s_city#77, 1, 30) ASC NULLS FIRST, profit#1 ASC NULLS FIRST], true
         +- Project [c_last_name#98, c_first_name#97, substring(s_city#77, 1, 30) AS substring(s_city, 1, 30)#109, ss_ticket_number#13, amt#0, profit#1, s_city#77]
            +- Join Inner, (ss_customer_sk#7 = c_customer_sk#89)
               :- Aggregate [ss_ticket_number#13, ss_customer_sk#7, ss_addr_sk#10, s_city#77], [ss_ticket_number#13, ss_customer_sk#7, s_city#77, sum(ss_coupon_amt#23) AS amt#0, sum(ss_net_profit#26) AS profit#1]
               :  +- Project [ss_customer_sk#7, ss_addr_sk#10, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26, s_city#77]
               :     +- Join Inner, (ss_hdemo_sk#9 = hd_demo_sk#84)
               :        :- Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_addr_sk#10, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26, s_city#77]
               :        :  +- Join Inner, (ss_store_sk#11 = s_store_sk#55)
               :        :     :- Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_addr_sk#10, ss_store_sk#11, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26]
               :        :     :  +- Join Inner, (ss_sold_date_sk#4 = d_date_sk#27)
               :        :     :     :- Project [ss_sold_date_sk#4, ss_customer_sk#7, ss_hdemo_sk#9, ss_addr_sk#10, ss_store_sk#11, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26]
               :        :     :     :  +- Filter (((isnotnull(ss_sold_date_sk#4) && isnotnull(ss_store_sk#11)) && isnotnull(ss_hdemo_sk#9)) && isnotnull(ss_customer_sk#7))
               :        :     :     :     +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
               :        :     :     +- Project [d_date_sk#27]
               :        :     :        +- Filter (((isnotnull(d_dow#34) && (d_dow#34 = 1)) && d_year#33 IN (1999,2000,2001)) && isnotnull(d_date_sk#27))
               :        :     :           +- Relation[d_date_sk#27,d_date_id#28,d_date#29,d_month_seq#30,d_week_seq#31,d_quarter_seq#32,d_year#33,d_dow#34,d_moy#35,d_dom#36,d_qoy#37,d_fy_year#38,d_fy_quarter_seq#39,d_fy_week_seq#40,d_day_name#41,d_quarter_name#42,d_holiday#43,d_weekend#44,d_following_holiday#45,d_first_dom#46,d_last_dom#47,d_same_day_ly#48,d_same_day_lq#49,d_current_day#50,... 4 more fields] parquet
               :        :     +- Project [s_store_sk#55, s_city#77]
               :        :        +- Filter (((isnotnull(s_number_employees#61) && (s_number_employees#61 >= 200)) && (s_number_employees#61 <= 295)) && isnotnull(s_store_sk#55))
               :        :           +- Relation[s_store_sk#55,s_store_id#56,s_rec_start_date#57,s_rec_end_date#58,s_closed_date_sk#59,s_store_name#60,s_number_employees#61,s_floor_space#62,s_hours#63,s_manager#64,s_market_id#65,s_geography_class#66,s_market_desc#67,s_market_manager#68,s_division_id#69,s_division_name#70,s_company_id#71,s_company_name#72,s_street_number#73,s_street_name#74,s_street_type#75,s_suite_number#76,s_city#77,s_county#78,... 5 more fields] parquet
               :        +- Project [hd_demo_sk#84]
               :           +- Filter (((hd_dep_count#87 = 6) || (hd_vehicle_count#88 > 2)) && isnotnull(hd_demo_sk#84))
               :              +- Relation[hd_demo_sk#84,hd_income_band_sk#85,hd_buy_potential#86,hd_dep_count#87,hd_vehicle_count#88] parquet
               +- Project [c_customer_sk#89, c_first_name#97, c_last_name#98]
                  +- Filter isnotnull(c_customer_sk#89)
                     +- Relation[c_customer_sk#89,c_customer_id#90,c_current_cdemo_sk#91,c_current_hdemo_sk#92,c_current_addr_sk#93,c_first_shipto_date_sk#94,c_first_sales_date_sk#95,c_salutation#96,c_first_name#97,c_last_name#98,c_preferred_cust_flag#99,c_birth_day#100,c_birth_month#101,c_birth_year#102,c_birth_country#103,c_login#104,c_email_address#105,c_last_review_date#106] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[c_last_name#98 ASC NULLS FIRST,c_first_name#97 ASC NULLS FIRST,substring(s_city#77, 1, 30) ASC NULLS FIRST,profit#1 ASC NULLS FIRST], output=[c_last_name#98,c_first_name#97,substring(s_city, 1, 30)#109,ss_ticket_number#13,amt#0,profit#1])
+- *(6) Project [c_last_name#98, c_first_name#97, substring(s_city#77, 1, 30) AS substring(s_city, 1, 30)#109, ss_ticket_number#13, amt#0, profit#1, s_city#77]
   +- *(6) BroadcastHashJoin [ss_customer_sk#7], [c_customer_sk#89], Inner, BuildRight
      :- *(6) HashAggregate(keys=[ss_ticket_number#13, ss_customer_sk#7, ss_addr_sk#10, s_city#77], functions=[sum(ss_coupon_amt#23), sum(ss_net_profit#26)], output=[ss_ticket_number#13, ss_customer_sk#7, s_city#77, amt#0, profit#1])
      :  +- Exchange hashpartitioning(ss_ticket_number#13, ss_customer_sk#7, ss_addr_sk#10, s_city#77, 200)
      :     +- *(4) HashAggregate(keys=[ss_ticket_number#13, ss_customer_sk#7, ss_addr_sk#10, s_city#77], functions=[partial_sum(ss_coupon_amt#23), partial_sum(ss_net_profit#26)], output=[ss_ticket_number#13, ss_customer_sk#7, ss_addr_sk#10, s_city#77, sum#112, sum#113])
      :        +- *(4) Project [ss_customer_sk#7, ss_addr_sk#10, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26, s_city#77]
      :           +- *(4) BroadcastHashJoin [ss_hdemo_sk#9], [hd_demo_sk#84], Inner, BuildRight
      :              :- *(4) Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_addr_sk#10, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26, s_city#77]
      :              :  +- *(4) BroadcastHashJoin [ss_store_sk#11], [s_store_sk#55], Inner, BuildRight
      :              :     :- *(4) Project [ss_customer_sk#7, ss_hdemo_sk#9, ss_addr_sk#10, ss_store_sk#11, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26]
      :              :     :  +- *(4) BroadcastHashJoin [ss_sold_date_sk#4], [d_date_sk#27], Inner, BuildRight
      :              :     :     :- *(4) Project [ss_sold_date_sk#4, ss_customer_sk#7, ss_hdemo_sk#9, ss_addr_sk#10, ss_store_sk#11, ss_ticket_number#13, ss_coupon_amt#23, ss_net_profit#26]
      :              :     :     :  +- *(4) Filter (((isnotnull(ss_sold_date_sk#4) && isnotnull(ss_store_sk#11)) && isnotnull(ss_hdemo_sk#9)) && isnotnull(ss_customer_sk#7))
      :              :     :     :     +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#4,ss_customer_sk#7,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_ticket_number#13,ss_coupon_amt#23,ss_net_profit#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_hdemo_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_hdemo_sk:int,ss_addr_sk:int,ss_store_sk:int,ss_t...
      :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :     :        +- *(1) Project [d_date_sk#27]
      :              :     :           +- *(1) Filter (((isnotnull(d_dow#34) && (d_dow#34 = 1)) && d_year#33 IN (1999,2000,2001)) && isnotnull(d_date_sk#27))
      :              :     :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#27,d_year#33,d_dow#34] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_dow), EqualTo(d_dow,1), In(d_year, [1999,2000,2001]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_dow:int>
      :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :        +- *(2) Project [s_store_sk#55, s_city#77]
      :              :           +- *(2) Filter (((isnotnull(s_number_employees#61) && (s_number_employees#61 >= 200)) && (s_number_employees#61 <= 295)) && isnotnull(s_store_sk#55))
      :              :              +- *(2) FileScan parquet tpcds.store[s_store_sk#55,s_number_employees#61,s_city#77] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_number_employees), GreaterThanOrEqual(s_number_employees,200), LessThanOrEqual(s_num..., ReadSchema: struct<s_store_sk:int,s_number_employees:int,s_city:string>
      :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                 +- *(3) Project [hd_demo_sk#84]
      :                    +- *(3) Filter (((hd_dep_count#87 = 6) || (hd_vehicle_count#88 > 2)) && isnotnull(hd_demo_sk#84))
      :                       +- *(3) FileScan parquet tpcds.household_demographics[hd_demo_sk#84,hd_dep_count#87,hd_vehicle_count#88] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [Or(EqualTo(hd_dep_count,6),GreaterThan(hd_vehicle_count,2)), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         +- *(5) Project [c_customer_sk#89, c_first_name#97, c_last_name#98]
            +- *(5) Filter isnotnull(c_customer_sk#89)
               +- *(5) FileScan parquet tpcds.customer[c_customer_sk#89,c_first_name#97,c_last_name#98] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_first_name:string,c_last_name:string>
Time taken: 4.024 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 79 in stream 0 using template query79.tpl
------------------------------------------------------^^^

