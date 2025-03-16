== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['am_pm_ratio ASC NULLS FIRST], true
      +- 'Project [(cast('amc as decimal(15,4)) / cast('pmc as decimal(15,4))) AS am_pm_ratio#2]
         +- 'Join Inner
            :- 'SubqueryAlias `at1`
            :  +- 'Project ['count(1) AS amc#0]
            :     +- 'Filter (((('ws_sold_time_sk = 'time_dim.t_time_sk) && ('ws_ship_hdemo_sk = 'household_demographics.hd_demo_sk)) && ('ws_web_page_sk = 'web_page.wp_web_page_sk)) && (((('time_dim.t_hour >= 8) && ('time_dim.t_hour <= (8 + 1))) && ('household_demographics.hd_dep_count = 6)) && (('web_page.wp_char_count >= 5000) && ('web_page.wp_char_count <= 5200))))
            :        +- 'Join Inner
            :           :- 'Join Inner
            :           :  :- 'Join Inner
            :           :  :  :- 'UnresolvedRelation `web_sales`
            :           :  :  +- 'UnresolvedRelation `household_demographics`
            :           :  +- 'UnresolvedRelation `time_dim`
            :           +- 'UnresolvedRelation `web_page`
            +- 'SubqueryAlias `pt`
               +- 'Project ['count(1) AS pmc#1]
                  +- 'Filter (((('ws_sold_time_sk = 'time_dim.t_time_sk) && ('ws_ship_hdemo_sk = 'household_demographics.hd_demo_sk)) && ('ws_web_page_sk = 'web_page.wp_web_page_sk)) && (((('time_dim.t_hour >= 19) && ('time_dim.t_hour <= (19 + 1))) && ('household_demographics.hd_dep_count = 6)) && (('web_page.wp_char_count >= 5000) && ('web_page.wp_char_count <= 5200))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'UnresolvedRelation `web_sales`
                        :  :  +- 'UnresolvedRelation `household_demographics`
                        :  +- 'UnresolvedRelation `time_dim`
                        +- 'UnresolvedRelation `web_page`

== Analyzed Logical Plan ==
am_pm_ratio: decimal(35,20)
GlobalLimit 100
+- LocalLimit 100
   +- Sort [am_pm_ratio#2 ASC NULLS FIRST], true
      +- Project [CheckOverflow((promote_precision(cast(cast(amc#0L as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(pmc#1L as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) AS am_pm_ratio#2]
         +- Join Inner
            :- SubqueryAlias `at1`
            :  +- Aggregate [count(1) AS amc#0L]
            :     +- Filter ((((ws_sold_time_sk#8 = t_time_sk#46) && (ws_ship_hdemo_sk#17 = hd_demo_sk#41)) && (ws_web_page_sk#19 = wp_web_page_sk#56)) && ((((t_hour#49 >= 8) && (t_hour#49 <= (8 + 1))) && (hd_dep_count#44 = 6)) && ((wp_char_count#66 >= 5000) && (wp_char_count#66 <= 5200))))
            :        +- Join Inner
            :           :- Join Inner
            :           :  :- Join Inner
            :           :  :  :- SubqueryAlias `tpcds`.`web_sales`
            :           :  :  :  +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
            :           :  :  +- SubqueryAlias `tpcds`.`household_demographics`
            :           :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
            :           :  +- SubqueryAlias `tpcds`.`time_dim`
            :           :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
            :           +- SubqueryAlias `tpcds`.`web_page`
            :              +- Relation[wp_web_page_sk#56,wp_web_page_id#57,wp_rec_start_date#58,wp_rec_end_date#59,wp_creation_date_sk#60,wp_access_date_sk#61,wp_autogen_flag#62,wp_customer_sk#63,wp_url#64,wp_type#65,wp_char_count#66,wp_link_count#67,wp_image_count#68,wp_max_ad_count#69] parquet
            +- SubqueryAlias `pt`
               +- Aggregate [count(1) AS pmc#1L]
                  +- Filter ((((ws_sold_time_sk#8 = t_time_sk#46) && (ws_ship_hdemo_sk#17 = hd_demo_sk#41)) && (ws_web_page_sk#19 = wp_web_page_sk#56)) && ((((t_hour#49 >= 19) && (t_hour#49 <= (19 + 1))) && (hd_dep_count#44 = 6)) && ((wp_char_count#66 >= 5000) && (wp_char_count#66 <= 5200))))
                     +- Join Inner
                        :- Join Inner
                        :  :- Join Inner
                        :  :  :- SubqueryAlias `tpcds`.`web_sales`
                        :  :  :  +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                        :  :  +- SubqueryAlias `tpcds`.`household_demographics`
                        :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
                        :  +- SubqueryAlias `tpcds`.`time_dim`
                        :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
                        +- SubqueryAlias `tpcds`.`web_page`
                           +- Relation[wp_web_page_sk#56,wp_web_page_id#57,wp_rec_start_date#58,wp_rec_end_date#59,wp_creation_date_sk#60,wp_access_date_sk#61,wp_autogen_flag#62,wp_customer_sk#63,wp_url#64,wp_type#65,wp_char_count#66,wp_link_count#67,wp_image_count#68,wp_max_ad_count#69] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [am_pm_ratio#2 ASC NULLS FIRST], true
      +- Project [CheckOverflow((promote_precision(cast(amc#0L as decimal(15,4))) / promote_precision(cast(pmc#1L as decimal(15,4)))), DecimalType(35,20)) AS am_pm_ratio#2]
         +- Join Inner
            :- Aggregate [count(1) AS amc#0L]
            :  +- Project
            :     +- Join Inner, (ws_web_page_sk#19 = wp_web_page_sk#56)
            :        :- Project [ws_web_page_sk#19]
            :        :  +- Join Inner, (ws_sold_time_sk#8 = t_time_sk#46)
            :        :     :- Project [ws_sold_time_sk#8, ws_web_page_sk#19]
            :        :     :  +- Join Inner, (ws_ship_hdemo_sk#17 = hd_demo_sk#41)
            :        :     :     :- Project [ws_sold_time_sk#8, ws_ship_hdemo_sk#17, ws_web_page_sk#19]
            :        :     :     :  +- Filter ((isnotnull(ws_ship_hdemo_sk#17) && isnotnull(ws_sold_time_sk#8)) && isnotnull(ws_web_page_sk#19))
            :        :     :     :     +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
            :        :     :     +- Project [hd_demo_sk#41]
            :        :     :        +- Filter ((isnotnull(hd_dep_count#44) && (hd_dep_count#44 = 6)) && isnotnull(hd_demo_sk#41))
            :        :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
            :        :     +- Project [t_time_sk#46]
            :        :        +- Filter (((isnotnull(t_hour#49) && (t_hour#49 >= 8)) && (t_hour#49 <= 9)) && isnotnull(t_time_sk#46))
            :        :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
            :        +- Project [wp_web_page_sk#56]
            :           +- Filter (((isnotnull(wp_char_count#66) && (wp_char_count#66 >= 5000)) && (wp_char_count#66 <= 5200)) && isnotnull(wp_web_page_sk#56))
            :              +- Relation[wp_web_page_sk#56,wp_web_page_id#57,wp_rec_start_date#58,wp_rec_end_date#59,wp_creation_date_sk#60,wp_access_date_sk#61,wp_autogen_flag#62,wp_customer_sk#63,wp_url#64,wp_type#65,wp_char_count#66,wp_link_count#67,wp_image_count#68,wp_max_ad_count#69] parquet
            +- Aggregate [count(1) AS pmc#1L]
               +- Project
                  +- Join Inner, (ws_web_page_sk#19 = wp_web_page_sk#56)
                     :- Project [ws_web_page_sk#19]
                     :  +- Join Inner, (ws_sold_time_sk#8 = t_time_sk#46)
                     :     :- Project [ws_sold_time_sk#8, ws_web_page_sk#19]
                     :     :  +- Join Inner, (ws_ship_hdemo_sk#17 = hd_demo_sk#41)
                     :     :     :- Project [ws_sold_time_sk#8, ws_ship_hdemo_sk#17, ws_web_page_sk#19]
                     :     :     :  +- Filter ((isnotnull(ws_ship_hdemo_sk#17) && isnotnull(ws_sold_time_sk#8)) && isnotnull(ws_web_page_sk#19))
                     :     :     :     +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                     :     :     +- Project [hd_demo_sk#41]
                     :     :        +- Filter ((isnotnull(hd_dep_count#44) && (hd_dep_count#44 = 6)) && isnotnull(hd_demo_sk#41))
                     :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
                     :     +- Project [t_time_sk#46]
                     :        +- Filter (((isnotnull(t_hour#49) && (t_hour#49 >= 19)) && (t_hour#49 <= 20)) && isnotnull(t_time_sk#46))
                     :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
                     +- Project [wp_web_page_sk#56]
                        +- Filter (((isnotnull(wp_char_count#66) && (wp_char_count#66 >= 5000)) && (wp_char_count#66 <= 5200)) && isnotnull(wp_web_page_sk#56))
                           +- Relation[wp_web_page_sk#56,wp_web_page_id#57,wp_rec_start_date#58,wp_rec_end_date#59,wp_creation_date_sk#60,wp_access_date_sk#61,wp_autogen_flag#62,wp_customer_sk#63,wp_url#64,wp_type#65,wp_char_count#66,wp_link_count#67,wp_image_count#68,wp_max_ad_count#69] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[am_pm_ratio#2 ASC NULLS FIRST], output=[am_pm_ratio#2])
+- *(11) Project [CheckOverflow((promote_precision(cast(amc#0L as decimal(15,4))) / promote_precision(cast(pmc#1L as decimal(15,4)))), DecimalType(35,20)) AS am_pm_ratio#2]
   +- BroadcastNestedLoopJoin BuildRight, Inner
      :- *(5) HashAggregate(keys=[], functions=[count(1)], output=[amc#0L])
      :  +- Exchange SinglePartition
      :     +- *(4) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#71L])
      :        +- *(4) Project
      :           +- *(4) BroadcastHashJoin [ws_web_page_sk#19], [wp_web_page_sk#56], Inner, BuildRight
      :              :- *(4) Project [ws_web_page_sk#19]
      :              :  +- *(4) BroadcastHashJoin [ws_sold_time_sk#8], [t_time_sk#46], Inner, BuildRight
      :              :     :- *(4) Project [ws_sold_time_sk#8, ws_web_page_sk#19]
      :              :     :  +- *(4) BroadcastHashJoin [ws_ship_hdemo_sk#17], [hd_demo_sk#41], Inner, BuildRight
      :              :     :     :- *(4) Project [ws_sold_time_sk#8, ws_ship_hdemo_sk#17, ws_web_page_sk#19]
      :              :     :     :  +- *(4) Filter ((isnotnull(ws_ship_hdemo_sk#17) && isnotnull(ws_sold_time_sk#8)) && isnotnull(ws_web_page_sk#19))
      :              :     :     :     +- *(4) FileScan parquet tpcds.web_sales[ws_sold_time_sk#8,ws_ship_hdemo_sk#17,ws_web_page_sk#19] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_hdemo_sk), IsNotNull(ws_sold_time_sk), IsNotNull(ws_web_page_sk)], ReadSchema: struct<ws_sold_time_sk:int,ws_ship_hdemo_sk:int,ws_web_page_sk:int>
      :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :     :        +- *(1) Project [hd_demo_sk#41]
      :              :     :           +- *(1) Filter ((isnotnull(hd_dep_count#44) && (hd_dep_count#44 = 6)) && isnotnull(hd_demo_sk#41))
      :              :     :              +- *(1) FileScan parquet tpcds.household_demographics[hd_demo_sk#41,hd_dep_count#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_dep_count), EqualTo(hd_dep_count,6), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int>
      :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :        +- *(2) Project [t_time_sk#46]
      :              :           +- *(2) Filter (((isnotnull(t_hour#49) && (t_hour#49 >= 8)) && (t_hour#49 <= 9)) && isnotnull(t_time_sk#46))
      :              :              +- *(2) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), GreaterThanOrEqual(t_hour,8), LessThanOrEqual(t_hour,9), IsNotNull(t_time_sk)], ReadSchema: struct<t_time_sk:int,t_hour:int>
      :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                 +- *(3) Project [wp_web_page_sk#56]
      :                    +- *(3) Filter (((isnotnull(wp_char_count#66) && (wp_char_count#66 >= 5000)) && (wp_char_count#66 <= 5200)) && isnotnull(wp_web_page_sk#56))
      :                       +- *(3) FileScan parquet tpcds.web_page[wp_web_page_sk#56,wp_char_count#66] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_char_count), GreaterThanOrEqual(wp_char_count,5000), LessThanOrEqual(wp_char_count,..., ReadSchema: struct<wp_web_page_sk:int,wp_char_count:int>
      +- BroadcastExchange IdentityBroadcastMode
         +- *(10) HashAggregate(keys=[], functions=[count(1)], output=[pmc#1L])
            +- Exchange SinglePartition
               +- *(9) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#73L])
                  +- *(9) Project
                     +- *(9) BroadcastHashJoin [ws_web_page_sk#19], [wp_web_page_sk#56], Inner, BuildRight
                        :- *(9) Project [ws_web_page_sk#19]
                        :  +- *(9) BroadcastHashJoin [ws_sold_time_sk#8], [t_time_sk#46], Inner, BuildRight
                        :     :- *(9) Project [ws_sold_time_sk#8, ws_web_page_sk#19]
                        :     :  +- *(9) BroadcastHashJoin [ws_ship_hdemo_sk#17], [hd_demo_sk#41], Inner, BuildRight
                        :     :     :- *(9) Project [ws_sold_time_sk#8, ws_ship_hdemo_sk#17, ws_web_page_sk#19]
                        :     :     :  +- *(9) Filter ((isnotnull(ws_ship_hdemo_sk#17) && isnotnull(ws_sold_time_sk#8)) && isnotnull(ws_web_page_sk#19))
                        :     :     :     +- *(9) FileScan parquet tpcds.web_sales[ws_sold_time_sk#8,ws_ship_hdemo_sk#17,ws_web_page_sk#19] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_hdemo_sk), IsNotNull(ws_sold_time_sk), IsNotNull(ws_web_page_sk)], ReadSchema: struct<ws_sold_time_sk:int,ws_ship_hdemo_sk:int,ws_web_page_sk:int>
                        :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :        +- *(7) Project [t_time_sk#46]
                        :           +- *(7) Filter (((isnotnull(t_hour#49) && (t_hour#49 >= 19)) && (t_hour#49 <= 20)) && isnotnull(t_time_sk#46))
                        :              +- *(7) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), GreaterThanOrEqual(t_hour,19), LessThanOrEqual(t_hour,20), IsNotNull(t_time_sk)], ReadSchema: struct<t_time_sk:int,t_hour:int>
                        +- ReusedExchange [wp_web_page_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 3.937 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 90 in stream 0 using template query90.tpl
------------------------------------------------------^^^

