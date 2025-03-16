== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_store_name ASC NULLS FIRST, 's_store_id ASC NULLS FIRST, 'sun_sales ASC NULLS FIRST, 'mon_sales ASC NULLS FIRST, 'tue_sales ASC NULLS FIRST, 'wed_sales ASC NULLS FIRST, 'thu_sales ASC NULLS FIRST, 'fri_sales ASC NULLS FIRST, 'sat_sales ASC NULLS FIRST], true
      +- 'Aggregate ['s_store_name, 's_store_id], ['s_store_name, 's_store_id, 'sum(CASE WHEN ('d_day_name = Sunday) THEN 'ss_sales_price ELSE null END) AS sun_sales#0, 'sum(CASE WHEN ('d_day_name = Monday) THEN 'ss_sales_price ELSE null END) AS mon_sales#1, 'sum(CASE WHEN ('d_day_name = Tuesday) THEN 'ss_sales_price ELSE null END) AS tue_sales#2, 'sum(CASE WHEN ('d_day_name = Wednesday) THEN 'ss_sales_price ELSE null END) AS wed_sales#3, 'sum(CASE WHEN ('d_day_name = Thursday) THEN 'ss_sales_price ELSE null END) AS thu_sales#4, 'sum(CASE WHEN ('d_day_name = Friday) THEN 'ss_sales_price ELSE null END) AS fri_sales#5, 'sum(CASE WHEN ('d_day_name = Saturday) THEN 'ss_sales_price ELSE null END) AS sat_sales#6]
         +- 'Filter ((('d_date_sk = 'ss_sold_date_sk) && ('s_store_sk = 'ss_store_sk)) && (('s_gmt_offset = -5) && ('d_year = 2000)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `store_sales`
               +- 'UnresolvedRelation `store`

== Analyzed Logical Plan ==
s_store_name: string, s_store_id: string, sun_sales: double, mon_sales: double, tue_sales: double, wed_sales: double, thu_sales: double, fri_sales: double, sat_sales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#65 ASC NULLS FIRST, s_store_id#61 ASC NULLS FIRST, sun_sales#0 ASC NULLS FIRST, mon_sales#1 ASC NULLS FIRST, tue_sales#2 ASC NULLS FIRST, wed_sales#3 ASC NULLS FIRST, thu_sales#4 ASC NULLS FIRST, fri_sales#5 ASC NULLS FIRST, sat_sales#6 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#65, s_store_id#61], [s_store_name#65, s_store_id#61, sum(CASE WHEN (d_day_name#23 = Sunday) THEN ss_sales_price#50 ELSE cast(null as double) END) AS sun_sales#0, sum(CASE WHEN (d_day_name#23 = Monday) THEN ss_sales_price#50 ELSE cast(null as double) END) AS mon_sales#1, sum(CASE WHEN (d_day_name#23 = Tuesday) THEN ss_sales_price#50 ELSE cast(null as double) END) AS tue_sales#2, sum(CASE WHEN (d_day_name#23 = Wednesday) THEN ss_sales_price#50 ELSE cast(null as double) END) AS wed_sales#3, sum(CASE WHEN (d_day_name#23 = Thursday) THEN ss_sales_price#50 ELSE cast(null as double) END) AS thu_sales#4, sum(CASE WHEN (d_day_name#23 = Friday) THEN ss_sales_price#50 ELSE cast(null as double) END) AS fri_sales#5, sum(CASE WHEN (d_day_name#23 = Saturday) THEN ss_sales_price#50 ELSE cast(null as double) END) AS sat_sales#6]
         +- Filter (((d_date_sk#9 = ss_sold_date_sk#37) && (s_store_sk#60 = ss_store_sk#44)) && ((s_gmt_offset#87 = cast(-5 as double)) && (d_year#15 = 2000)))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `tpcds`.`date_dim`
               :  :  +- Relation[d_date_sk#9,d_date_id#10,d_date#11,d_month_seq#12,d_week_seq#13,d_quarter_seq#14,d_year#15,d_dow#16,d_moy#17,d_dom#18,d_qoy#19,d_fy_year#20,d_fy_quarter_seq#21,d_fy_week_seq#22,d_day_name#23,d_quarter_name#24,d_holiday#25,d_weekend#26,d_following_holiday#27,d_first_dom#28,d_last_dom#29,d_same_day_ly#30,d_same_day_lq#31,d_current_day#32,... 4 more fields] parquet
               :  +- SubqueryAlias `tpcds`.`store_sales`
               :     +- Relation[ss_sold_date_sk#37,ss_sold_time_sk#38,ss_item_sk#39,ss_customer_sk#40,ss_cdemo_sk#41,ss_hdemo_sk#42,ss_addr_sk#43,ss_store_sk#44,ss_promo_sk#45,ss_ticket_number#46,ss_quantity#47,ss_wholesale_cost#48,ss_list_price#49,ss_sales_price#50,ss_ext_discount_amt#51,ss_ext_sales_price#52,ss_ext_wholesale_cost#53,ss_ext_list_price#54,ss_ext_tax#55,ss_coupon_amt#56,ss_net_paid#57,ss_net_paid_inc_tax#58,ss_net_profit#59] parquet
               +- SubqueryAlias `tpcds`.`store`
                  +- Relation[s_store_sk#60,s_store_id#61,s_rec_start_date#62,s_rec_end_date#63,s_closed_date_sk#64,s_store_name#65,s_number_employees#66,s_floor_space#67,s_hours#68,s_manager#69,s_market_id#70,s_geography_class#71,s_market_desc#72,s_market_manager#73,s_division_id#74,s_division_name#75,s_company_id#76,s_company_name#77,s_street_number#78,s_street_name#79,s_street_type#80,s_suite_number#81,s_city#82,s_county#83,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#65 ASC NULLS FIRST, s_store_id#61 ASC NULLS FIRST, sun_sales#0 ASC NULLS FIRST, mon_sales#1 ASC NULLS FIRST, tue_sales#2 ASC NULLS FIRST, wed_sales#3 ASC NULLS FIRST, thu_sales#4 ASC NULLS FIRST, fri_sales#5 ASC NULLS FIRST, sat_sales#6 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#65, s_store_id#61], [s_store_name#65, s_store_id#61, sum(CASE WHEN (d_day_name#23 = Sunday) THEN ss_sales_price#50 ELSE null END) AS sun_sales#0, sum(CASE WHEN (d_day_name#23 = Monday) THEN ss_sales_price#50 ELSE null END) AS mon_sales#1, sum(CASE WHEN (d_day_name#23 = Tuesday) THEN ss_sales_price#50 ELSE null END) AS tue_sales#2, sum(CASE WHEN (d_day_name#23 = Wednesday) THEN ss_sales_price#50 ELSE null END) AS wed_sales#3, sum(CASE WHEN (d_day_name#23 = Thursday) THEN ss_sales_price#50 ELSE null END) AS thu_sales#4, sum(CASE WHEN (d_day_name#23 = Friday) THEN ss_sales_price#50 ELSE null END) AS fri_sales#5, sum(CASE WHEN (d_day_name#23 = Saturday) THEN ss_sales_price#50 ELSE null END) AS sat_sales#6]
         +- Project [d_day_name#23, ss_sales_price#50, s_store_id#61, s_store_name#65]
            +- Join Inner, (s_store_sk#60 = ss_store_sk#44)
               :- Project [d_day_name#23, ss_store_sk#44, ss_sales_price#50]
               :  +- Join Inner, (d_date_sk#9 = ss_sold_date_sk#37)
               :     :- Project [d_date_sk#9, d_day_name#23]
               :     :  +- Filter ((isnotnull(d_year#15) && (d_year#15 = 2000)) && isnotnull(d_date_sk#9))
               :     :     +- Relation[d_date_sk#9,d_date_id#10,d_date#11,d_month_seq#12,d_week_seq#13,d_quarter_seq#14,d_year#15,d_dow#16,d_moy#17,d_dom#18,d_qoy#19,d_fy_year#20,d_fy_quarter_seq#21,d_fy_week_seq#22,d_day_name#23,d_quarter_name#24,d_holiday#25,d_weekend#26,d_following_holiday#27,d_first_dom#28,d_last_dom#29,d_same_day_ly#30,d_same_day_lq#31,d_current_day#32,... 4 more fields] parquet
               :     +- Project [ss_sold_date_sk#37, ss_store_sk#44, ss_sales_price#50]
               :        +- Filter (isnotnull(ss_sold_date_sk#37) && isnotnull(ss_store_sk#44))
               :           +- Relation[ss_sold_date_sk#37,ss_sold_time_sk#38,ss_item_sk#39,ss_customer_sk#40,ss_cdemo_sk#41,ss_hdemo_sk#42,ss_addr_sk#43,ss_store_sk#44,ss_promo_sk#45,ss_ticket_number#46,ss_quantity#47,ss_wholesale_cost#48,ss_list_price#49,ss_sales_price#50,ss_ext_discount_amt#51,ss_ext_sales_price#52,ss_ext_wholesale_cost#53,ss_ext_list_price#54,ss_ext_tax#55,ss_coupon_amt#56,ss_net_paid#57,ss_net_paid_inc_tax#58,ss_net_profit#59] parquet
               +- Project [s_store_sk#60, s_store_id#61, s_store_name#65]
                  +- Filter ((isnotnull(s_gmt_offset#87) && (s_gmt_offset#87 = -5.0)) && isnotnull(s_store_sk#60))
                     +- Relation[s_store_sk#60,s_store_id#61,s_rec_start_date#62,s_rec_end_date#63,s_closed_date_sk#64,s_store_name#65,s_number_employees#66,s_floor_space#67,s_hours#68,s_manager#69,s_market_id#70,s_geography_class#71,s_market_desc#72,s_market_manager#73,s_division_id#74,s_division_name#75,s_company_id#76,s_company_name#77,s_street_number#78,s_street_name#79,s_street_type#80,s_suite_number#81,s_city#82,s_county#83,... 5 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_store_name#65 ASC NULLS FIRST,s_store_id#61 ASC NULLS FIRST,sun_sales#0 ASC NULLS FIRST,mon_sales#1 ASC NULLS FIRST,tue_sales#2 ASC NULLS FIRST,wed_sales#3 ASC NULLS FIRST,thu_sales#4 ASC NULLS FIRST,fri_sales#5 ASC NULLS FIRST,sat_sales#6 ASC NULLS FIRST], output=[s_store_name#65,s_store_id#61,sun_sales#0,mon_sales#1,tue_sales#2,wed_sales#3,thu_sales#4,fri_sales#5,sat_sales#6])
+- *(4) HashAggregate(keys=[s_store_name#65, s_store_id#61], functions=[sum(CASE WHEN (d_day_name#23 = Sunday) THEN ss_sales_price#50 ELSE null END), sum(CASE WHEN (d_day_name#23 = Monday) THEN ss_sales_price#50 ELSE null END), sum(CASE WHEN (d_day_name#23 = Tuesday) THEN ss_sales_price#50 ELSE null END), sum(CASE WHEN (d_day_name#23 = Wednesday) THEN ss_sales_price#50 ELSE null END), sum(CASE WHEN (d_day_name#23 = Thursday) THEN ss_sales_price#50 ELSE null END), sum(CASE WHEN (d_day_name#23 = Friday) THEN ss_sales_price#50 ELSE null END), sum(CASE WHEN (d_day_name#23 = Saturday) THEN ss_sales_price#50 ELSE null END)], output=[s_store_name#65, s_store_id#61, sun_sales#0, mon_sales#1, tue_sales#2, wed_sales#3, thu_sales#4, fri_sales#5, sat_sales#6])
   +- Exchange hashpartitioning(s_store_name#65, s_store_id#61, 200)
      +- *(3) HashAggregate(keys=[s_store_name#65, s_store_id#61], functions=[partial_sum(CASE WHEN (d_day_name#23 = Sunday) THEN ss_sales_price#50 ELSE null END), partial_sum(CASE WHEN (d_day_name#23 = Monday) THEN ss_sales_price#50 ELSE null END), partial_sum(CASE WHEN (d_day_name#23 = Tuesday) THEN ss_sales_price#50 ELSE null END), partial_sum(CASE WHEN (d_day_name#23 = Wednesday) THEN ss_sales_price#50 ELSE null END), partial_sum(CASE WHEN (d_day_name#23 = Thursday) THEN ss_sales_price#50 ELSE null END), partial_sum(CASE WHEN (d_day_name#23 = Friday) THEN ss_sales_price#50 ELSE null END), partial_sum(CASE WHEN (d_day_name#23 = Saturday) THEN ss_sales_price#50 ELSE null END)], output=[s_store_name#65, s_store_id#61, sum#112, sum#113, sum#114, sum#115, sum#116, sum#117, sum#118])
         +- *(3) Project [d_day_name#23, ss_sales_price#50, s_store_id#61, s_store_name#65]
            +- *(3) BroadcastHashJoin [ss_store_sk#44], [s_store_sk#60], Inner, BuildRight
               :- *(3) Project [d_day_name#23, ss_store_sk#44, ss_sales_price#50]
               :  +- *(3) BroadcastHashJoin [d_date_sk#9], [ss_sold_date_sk#37], Inner, BuildLeft
               :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :  +- *(1) Project [d_date_sk#9, d_day_name#23]
               :     :     +- *(1) Filter ((isnotnull(d_year#15) && (d_year#15 = 2000)) && isnotnull(d_date_sk#9))
               :     :        +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#9,d_year#15,d_day_name#23] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_day_name:string>
               :     +- *(3) Project [ss_sold_date_sk#37, ss_store_sk#44, ss_sales_price#50]
               :        +- *(3) Filter (isnotnull(ss_sold_date_sk#37) && isnotnull(ss_store_sk#44))
               :           +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#37,ss_store_sk#44,ss_sales_price#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_sales_price:double>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(2) Project [s_store_sk#60, s_store_id#61, s_store_name#65]
                     +- *(2) Filter ((isnotnull(s_gmt_offset#87) && (s_gmt_offset#87 = -5.0)) && isnotnull(s_store_sk#60))
                        +- *(2) FileScan parquet tpcds.store[s_store_sk#60,s_store_id#61,s_store_name#65,s_gmt_offset#87] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_gmt_offset), EqualTo(s_gmt_offset,-5.0), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string,s_store_name:string,s_gmt_offset:double>
Time taken: 3.772 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 43 in stream 0 using template query43.tpl
------------------------------------------------------^^^

