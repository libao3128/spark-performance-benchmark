== Parsed Logical Plan ==
'Sort ['i_category ASC NULLS FIRST, 'i_class ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 'revenueratio ASC NULLS FIRST], true
+- 'Aggregate ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price], ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price, 'sum('ss_ext_sales_price) AS itemrevenue#0, (('sum('ss_ext_sales_price) * 100) / 'sum('sum('ss_ext_sales_price)) windowspecdefinition('i_class, unspecifiedframe$())) AS revenueratio#1]
   +- 'Filter ((('ss_item_sk = 'i_item_sk) && 'i_category IN (Sports,Books,Home)) && (('ss_sold_date_sk = 'd_date_sk) && (('d_date >= cast(1999-02-22 as date)) && ('d_date <= 'date_add(cast(1999-02-22 as date), 30)))))
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'UnresolvedRelation `store_sales`
         :  +- 'UnresolvedRelation `item`
         +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, i_category: string, i_class: string, i_current_price: double, itemrevenue: double, revenueratio: double
Sort [i_category#39 ASC NULLS FIRST, i_class#37 ASC NULLS FIRST, i_item_id#28 ASC NULLS FIRST, i_item_desc#31 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
+- Project [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, itemrevenue#0, revenueratio#1]
   +- Project [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, itemrevenue#0, _w0#81, _w1#82, _we0#83, ((_w0#81 * cast(100 as double)) / _we0#83) AS revenueratio#1]
      +- Window [sum(_w1#82) windowspecdefinition(i_class#37, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#83], [i_class#37]
         +- Aggregate [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32], [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, sum(ss_ext_sales_price#19) AS itemrevenue#0, sum(ss_ext_sales_price#19) AS _w0#81, sum(ss_ext_sales_price#19) AS _w1#82]
            +- Filter (((ss_item_sk#6 = i_item_sk#27) && i_category#39 IN (Sports,Books,Home)) && ((ss_sold_date_sk#4 = d_date_sk#49) && ((d_date#51 >= cast(cast(1999-02-22 as date) as string)) && (d_date#51 <= cast(date_add(cast(1999-02-22 as date), 30) as string)))))
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :  +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
                  :  +- SubqueryAlias `tpcds`.`item`
                  :     +- Relation[i_item_sk#27,i_item_id#28,i_rec_start_date#29,i_rec_end_date#30,i_item_desc#31,i_current_price#32,i_wholesale_cost#33,i_brand_id#34,i_brand#35,i_class_id#36,i_class#37,i_category_id#38,i_category#39,i_manufact_id#40,i_manufact#41,i_size#42,i_formulation#43,i_color#44,i_units#45,i_container#46,i_manager_id#47,i_product_name#48] parquet
                  +- SubqueryAlias `tpcds`.`date_dim`
                     +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet

== Optimized Logical Plan ==
Sort [i_category#39 ASC NULLS FIRST, i_class#37 ASC NULLS FIRST, i_item_id#28 ASC NULLS FIRST, i_item_desc#31 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
+- Project [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, itemrevenue#0, ((_w0#81 * 100.0) / _we0#83) AS revenueratio#1]
   +- Window [sum(_w1#82) windowspecdefinition(i_class#37, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#83], [i_class#37]
      +- Aggregate [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32], [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, sum(ss_ext_sales_price#19) AS itemrevenue#0, sum(ss_ext_sales_price#19) AS _w0#81, sum(ss_ext_sales_price#19) AS _w1#82]
         +- Project [ss_ext_sales_price#19, i_item_id#28, i_item_desc#31, i_current_price#32, i_class#37, i_category#39]
            +- Join Inner, (ss_sold_date_sk#4 = d_date_sk#49)
               :- Project [ss_sold_date_sk#4, ss_ext_sales_price#19, i_item_id#28, i_item_desc#31, i_current_price#32, i_class#37, i_category#39]
               :  +- Join Inner, (ss_item_sk#6 = i_item_sk#27)
               :     :- Project [ss_sold_date_sk#4, ss_item_sk#6, ss_ext_sales_price#19]
               :     :  +- Filter (isnotnull(ss_item_sk#6) && isnotnull(ss_sold_date_sk#4))
               :     :     +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
               :     +- Project [i_item_sk#27, i_item_id#28, i_item_desc#31, i_current_price#32, i_class#37, i_category#39]
               :        +- Filter (i_category#39 IN (Sports,Books,Home) && isnotnull(i_item_sk#27))
               :           +- Relation[i_item_sk#27,i_item_id#28,i_rec_start_date#29,i_rec_end_date#30,i_item_desc#31,i_current_price#32,i_wholesale_cost#33,i_brand_id#34,i_brand#35,i_class_id#36,i_class#37,i_category_id#38,i_category#39,i_manufact_id#40,i_manufact#41,i_size#42,i_formulation#43,i_color#44,i_units#45,i_container#46,i_manager_id#47,i_product_name#48] parquet
               +- Project [d_date_sk#49]
                  +- Filter (((isnotnull(d_date#51) && (d_date#51 >= 1999-02-22)) && (d_date#51 <= 1999-03-24)) && isnotnull(d_date_sk#49))
                     +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet

== Physical Plan ==
*(7) Sort [i_category#39 ASC NULLS FIRST, i_class#37 ASC NULLS FIRST, i_item_id#28 ASC NULLS FIRST, i_item_desc#31 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(i_category#39 ASC NULLS FIRST, i_class#37 ASC NULLS FIRST, i_item_id#28 ASC NULLS FIRST, i_item_desc#31 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST, 200)
   +- *(6) Project [i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, itemrevenue#0, ((_w0#81 * 100.0) / _we0#83) AS revenueratio#1]
      +- Window [sum(_w1#82) windowspecdefinition(i_class#37, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#83], [i_class#37]
         +- *(5) Sort [i_class#37 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(i_class#37, 200)
               +- *(4) HashAggregate(keys=[i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32], functions=[sum(ss_ext_sales_price#19)], output=[i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, itemrevenue#0, _w0#81, _w1#82])
                  +- Exchange hashpartitioning(i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, 200)
                     +- *(3) HashAggregate(keys=[i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32], functions=[partial_sum(ss_ext_sales_price#19)], output=[i_item_id#28, i_item_desc#31, i_category#39, i_class#37, i_current_price#32, sum#85])
                        +- *(3) Project [ss_ext_sales_price#19, i_item_id#28, i_item_desc#31, i_current_price#32, i_class#37, i_category#39]
                           +- *(3) BroadcastHashJoin [ss_sold_date_sk#4], [d_date_sk#49], Inner, BuildRight
                              :- *(3) Project [ss_sold_date_sk#4, ss_ext_sales_price#19, i_item_id#28, i_item_desc#31, i_current_price#32, i_class#37, i_category#39]
                              :  +- *(3) BroadcastHashJoin [ss_item_sk#6], [i_item_sk#27], Inner, BuildRight
                              :     :- *(3) Project [ss_sold_date_sk#4, ss_item_sk#6, ss_ext_sales_price#19]
                              :     :  +- *(3) Filter (isnotnull(ss_item_sk#6) && isnotnull(ss_sold_date_sk#4))
                              :     :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#4,ss_item_sk#6,ss_ext_sales_price#19] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
                              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :        +- *(1) Project [i_item_sk#27, i_item_id#28, i_item_desc#31, i_current_price#32, i_class#37, i_category#39]
                              :           +- *(1) Filter (i_category#39 IN (Sports,Books,Home) && isnotnull(i_item_sk#27))
                              :              +- *(1) FileScan parquet tpcds.item[i_item_sk#27,i_item_id#28,i_item_desc#31,i_current_price#32,i_class#37,i_category#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [In(i_category, [Sports,Books,Home]), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string,i_current_price:double,i_class:string,i_...
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 +- *(2) Project [d_date_sk#49]
                                    +- *(2) Filter (((isnotnull(d_date#51) && (d_date#51 >= 1999-02-22)) && (d_date#51 <= 1999-03-24)) && isnotnull(d_date_sk#49))
                                       +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#49,d_date#51] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,1999-02-22), LessThanOrEqual(d_date,1999-03-24), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
Time taken: 3.635 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 98 in stream 0 using template query98.tpl
------------------------------------------------------^^^

