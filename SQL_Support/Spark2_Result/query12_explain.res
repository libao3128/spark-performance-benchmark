== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_category ASC NULLS FIRST, 'i_class ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 'revenueratio ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price], ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price, 'sum('ws_ext_sales_price) AS itemrevenue#0, (('sum('ws_ext_sales_price) * 100) / 'sum('sum('ws_ext_sales_price)) windowspecdefinition('i_class, unspecifiedframe$())) AS revenueratio#1]
         +- 'Filter ((('ws_item_sk = 'i_item_sk) && 'i_category IN (Sports,Books,Home)) && (('ws_sold_date_sk = 'd_date_sk) && (('d_date >= cast(1999-02-22 as date)) && ('d_date <= 'date_add(cast(1999-02-22 as date), 30)))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation `web_sales`
               :  +- 'UnresolvedRelation `item`
               +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, i_category: string, i_class: string, i_current_price: double, itemrevenue: double, revenueratio: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_category#50 ASC NULLS FIRST, i_class#48 ASC NULLS FIRST, i_item_id#39 ASC NULLS FIRST, i_item_desc#42 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
      +- Project [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, itemrevenue#0, revenueratio#1]
         +- Project [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, itemrevenue#0, _w0#92, _w1#93, _we0#94, ((_w0#92 * cast(100 as double)) / _we0#94) AS revenueratio#1]
            +- Window [sum(_w1#93) windowspecdefinition(i_class#48, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#94], [i_class#48]
               +- Aggregate [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43], [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, sum(ws_ext_sales_price#27) AS itemrevenue#0, sum(ws_ext_sales_price#27) AS _w0#92, sum(ws_ext_sales_price#27) AS _w1#93]
                  +- Filter (((ws_item_sk#7 = i_item_sk#38) && i_category#50 IN (Sports,Books,Home)) && ((ws_sold_date_sk#4 = d_date_sk#60) && ((d_date#62 >= cast(cast(1999-02-22 as date) as string)) && (d_date#62 <= cast(date_add(cast(1999-02-22 as date), 30) as string)))))
                     +- Join Inner
                        :- Join Inner
                        :  :- SubqueryAlias `tpcds`.`web_sales`
                        :  :  +- Relation[ws_sold_date_sk#4,ws_sold_time_sk#5,ws_ship_date_sk#6,ws_item_sk#7,ws_bill_customer_sk#8,ws_bill_cdemo_sk#9,ws_bill_hdemo_sk#10,ws_bill_addr_sk#11,ws_ship_customer_sk#12,ws_ship_cdemo_sk#13,ws_ship_hdemo_sk#14,ws_ship_addr_sk#15,ws_web_page_sk#16,ws_web_site_sk#17,ws_ship_mode_sk#18,ws_warehouse_sk#19,ws_promo_sk#20,ws_order_number#21,ws_quantity#22,ws_wholesale_cost#23,ws_list_price#24,ws_sales_price#25,ws_ext_discount_amt#26,ws_ext_sales_price#27,... 10 more fields] parquet
                        :  +- SubqueryAlias `tpcds`.`item`
                        :     +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
                        +- SubqueryAlias `tpcds`.`date_dim`
                           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_category#50 ASC NULLS FIRST, i_class#48 ASC NULLS FIRST, i_item_id#39 ASC NULLS FIRST, i_item_desc#42 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
      +- Project [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, itemrevenue#0, ((_w0#92 * 100.0) / _we0#94) AS revenueratio#1]
         +- Window [sum(_w1#93) windowspecdefinition(i_class#48, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#94], [i_class#48]
            +- Aggregate [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43], [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, sum(ws_ext_sales_price#27) AS itemrevenue#0, sum(ws_ext_sales_price#27) AS _w0#92, sum(ws_ext_sales_price#27) AS _w1#93]
               +- Project [ws_ext_sales_price#27, i_item_id#39, i_item_desc#42, i_current_price#43, i_class#48, i_category#50]
                  +- Join Inner, (ws_sold_date_sk#4 = d_date_sk#60)
                     :- Project [ws_sold_date_sk#4, ws_ext_sales_price#27, i_item_id#39, i_item_desc#42, i_current_price#43, i_class#48, i_category#50]
                     :  +- Join Inner, (ws_item_sk#7 = i_item_sk#38)
                     :     :- Project [ws_sold_date_sk#4, ws_item_sk#7, ws_ext_sales_price#27]
                     :     :  +- Filter (isnotnull(ws_item_sk#7) && isnotnull(ws_sold_date_sk#4))
                     :     :     +- Relation[ws_sold_date_sk#4,ws_sold_time_sk#5,ws_ship_date_sk#6,ws_item_sk#7,ws_bill_customer_sk#8,ws_bill_cdemo_sk#9,ws_bill_hdemo_sk#10,ws_bill_addr_sk#11,ws_ship_customer_sk#12,ws_ship_cdemo_sk#13,ws_ship_hdemo_sk#14,ws_ship_addr_sk#15,ws_web_page_sk#16,ws_web_site_sk#17,ws_ship_mode_sk#18,ws_warehouse_sk#19,ws_promo_sk#20,ws_order_number#21,ws_quantity#22,ws_wholesale_cost#23,ws_list_price#24,ws_sales_price#25,ws_ext_discount_amt#26,ws_ext_sales_price#27,... 10 more fields] parquet
                     :     +- Project [i_item_sk#38, i_item_id#39, i_item_desc#42, i_current_price#43, i_class#48, i_category#50]
                     :        +- Filter (i_category#50 IN (Sports,Books,Home) && isnotnull(i_item_sk#38))
                     :           +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
                     +- Project [d_date_sk#60]
                        +- Filter (((isnotnull(d_date#62) && (d_date#62 >= 1999-02-22)) && (d_date#62 <= 1999-03-24)) && isnotnull(d_date_sk#60))
                           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_category#50 ASC NULLS FIRST,i_class#48 ASC NULLS FIRST,i_item_id#39 ASC NULLS FIRST,i_item_desc#42 ASC NULLS FIRST,revenueratio#1 ASC NULLS FIRST], output=[i_item_id#39,i_item_desc#42,i_category#50,i_class#48,i_current_price#43,itemrevenue#0,revenueratio#1])
+- *(6) Project [i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, itemrevenue#0, ((_w0#92 * 100.0) / _we0#94) AS revenueratio#1]
   +- Window [sum(_w1#93) windowspecdefinition(i_class#48, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#94], [i_class#48]
      +- *(5) Sort [i_class#48 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(i_class#48, 200)
            +- *(4) HashAggregate(keys=[i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43], functions=[sum(ws_ext_sales_price#27)], output=[i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, itemrevenue#0, _w0#92, _w1#93])
               +- Exchange hashpartitioning(i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, 200)
                  +- *(3) HashAggregate(keys=[i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43], functions=[partial_sum(ws_ext_sales_price#27)], output=[i_item_id#39, i_item_desc#42, i_category#50, i_class#48, i_current_price#43, sum#96])
                     +- *(3) Project [ws_ext_sales_price#27, i_item_id#39, i_item_desc#42, i_current_price#43, i_class#48, i_category#50]
                        +- *(3) BroadcastHashJoin [ws_sold_date_sk#4], [d_date_sk#60], Inner, BuildRight
                           :- *(3) Project [ws_sold_date_sk#4, ws_ext_sales_price#27, i_item_id#39, i_item_desc#42, i_current_price#43, i_class#48, i_category#50]
                           :  +- *(3) BroadcastHashJoin [ws_item_sk#7], [i_item_sk#38], Inner, BuildRight
                           :     :- *(3) Project [ws_sold_date_sk#4, ws_item_sk#7, ws_ext_sales_price#27]
                           :     :  +- *(3) Filter (isnotnull(ws_item_sk#7) && isnotnull(ws_sold_date_sk#4))
                           :     :     +- *(3) FileScan parquet tpcds.web_sales[ws_sold_date_sk#4,ws_item_sk#7,ws_ext_sales_price#27] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ext_sales_price:double>
                           :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                           :        +- *(1) Project [i_item_sk#38, i_item_id#39, i_item_desc#42, i_current_price#43, i_class#48, i_category#50]
                           :           +- *(1) Filter (i_category#50 IN (Sports,Books,Home) && isnotnull(i_item_sk#38))
                           :              +- *(1) FileScan parquet tpcds.item[i_item_sk#38,i_item_id#39,i_item_desc#42,i_current_price#43,i_class#48,i_category#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [In(i_category, [Sports,Books,Home]), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string,i_current_price:double,i_class:string,i_...
                           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              +- *(2) Project [d_date_sk#60]
                                 +- *(2) Filter (((isnotnull(d_date#62) && (d_date#62 >= 1999-02-22)) && (d_date#62 <= 1999-03-24)) && isnotnull(d_date_sk#60))
                                    +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#60,d_date#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,1999-02-22), LessThanOrEqual(d_date,1999-03-24), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
Time taken: 3.799 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 12 in stream 0 using template query12.tpl
------------------------------------------------------^^^

