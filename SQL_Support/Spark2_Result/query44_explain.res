== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['asceding.rnk ASC NULLS FIRST], true
      +- 'Project ['asceding.rnk, 'i1.i_product_name AS best_performing#10, 'i2.i_product_name AS worst_performing#11]
         +- 'Filter ((('asceding.rnk = 'descending.rnk) && ('i1.i_item_sk = 'asceding.item_sk)) && ('i2.i_item_sk = 'descending.item_sk))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'SubqueryAlias `asceding`
               :  :  :  +- 'Project [*]
               :  :  :     +- 'Filter ('rnk < 11)
               :  :  :        +- 'SubqueryAlias `V11`
               :  :  :           +- 'Project ['item_sk, 'rank() windowspecdefinition('rank_col ASC NULLS FIRST, unspecifiedframe$()) AS rnk#4]
               :  :  :              +- 'SubqueryAlias `V1`
               :  :  :                 +- 'UnresolvedHaving ('avg('ss_net_profit) > (0.9 * scalar-subquery#3 []))
               :  :  :                    :  +- 'Aggregate ['ss_store_sk], ['avg('ss_net_profit) AS rank_col#2]
               :  :  :                    :     +- 'Filter (('ss_store_sk = 4) && isnull('ss_addr_sk))
               :  :  :                    :        +- 'UnresolvedRelation `store_sales`
               :  :  :                    +- 'Aggregate ['ss_item_sk], ['ss_item_sk AS item_sk#0, 'avg('ss_net_profit) AS rank_col#1]
               :  :  :                       +- 'Filter ('ss_store_sk = 4)
               :  :  :                          +- 'SubqueryAlias `ss1`
               :  :  :                             +- 'UnresolvedRelation `store_sales`
               :  :  +- 'SubqueryAlias `descending`
               :  :     +- 'Project [*]
               :  :        +- 'Filter ('rnk < 11)
               :  :           +- 'SubqueryAlias `V21`
               :  :              +- 'Project ['item_sk, 'rank() windowspecdefinition('rank_col DESC NULLS LAST, unspecifiedframe$()) AS rnk#9]
               :  :                 +- 'SubqueryAlias `V2`
               :  :                    +- 'UnresolvedHaving ('avg('ss_net_profit) > (0.9 * scalar-subquery#8 []))
               :  :                       :  +- 'Aggregate ['ss_store_sk], ['avg('ss_net_profit) AS rank_col#7]
               :  :                       :     +- 'Filter (('ss_store_sk = 4) && isnull('ss_addr_sk))
               :  :                       :        +- 'UnresolvedRelation `store_sales`
               :  :                       +- 'Aggregate ['ss_item_sk], ['ss_item_sk AS item_sk#5, 'avg('ss_net_profit) AS rank_col#6]
               :  :                          +- 'Filter ('ss_store_sk = 4)
               :  :                             +- 'SubqueryAlias `ss1`
               :  :                                +- 'UnresolvedRelation `store_sales`
               :  +- 'SubqueryAlias `i1`
               :     +- 'UnresolvedRelation `item`
               +- 'SubqueryAlias `i2`
                  +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
rnk: int, best_performing: string, worst_performing: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [rnk#4 ASC NULLS FIRST], true
      +- Project [rnk#4, i_product_name#62 AS best_performing#10, i_product_name#102 AS worst_performing#11]
         +- Filter (((rnk#4 = rnk#9) && (i_item_sk#41 = item_sk#0)) && (i_item_sk#81 = item_sk#5))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias `asceding`
               :  :  :  +- Project [item_sk#0, rnk#4]
               :  :  :     +- Filter (rnk#4 < 11)
               :  :  :        +- SubqueryAlias `V11`
               :  :  :           +- Project [item_sk#0, rnk#4]
               :  :  :              +- Project [item_sk#0, rank_col#1, rnk#4, rnk#4]
               :  :  :                 +- Window [rank(rank_col#1) windowspecdefinition(rank_col#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#4], [rank_col#1 ASC NULLS FIRST]
               :  :  :                    +- Project [item_sk#0, rank_col#1]
               :  :  :                       +- SubqueryAlias `V1`
               :  :  :                          +- Project [item_sk#0, rank_col#1]
               :  :  :                             +- Filter (avg(ss_net_profit#40)#69 > (cast(0.9 as double) * scalar-subquery#3 []))
               :  :  :                                :  +- Aggregate [ss_store_sk#25], [avg(ss_net_profit#40) AS rank_col#2]
               :  :  :                                :     +- Filter ((ss_store_sk#25 = 4) && isnull(ss_addr_sk#24))
               :  :  :                                :        +- SubqueryAlias `tpcds`.`store_sales`
               :  :  :                                :           +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :  :  :                                +- Aggregate [ss_item_sk#20], [ss_item_sk#20 AS item_sk#0, avg(ss_net_profit#40) AS rank_col#1, avg(ss_net_profit#40) AS avg(ss_net_profit#40)#69]
               :  :  :                                   +- Filter (ss_store_sk#25 = 4)
               :  :  :                                      +- SubqueryAlias `ss1`
               :  :  :                                         +- SubqueryAlias `tpcds`.`store_sales`
               :  :  :                                            +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :  :  +- SubqueryAlias `descending`
               :  :     +- Project [item_sk#5, rnk#9]
               :  :        +- Filter (rnk#9 < 11)
               :  :           +- SubqueryAlias `V21`
               :  :              +- Project [item_sk#5, rnk#9]
               :  :                 +- Project [item_sk#5, rank_col#6, rnk#9, rnk#9]
               :  :                    +- Window [rank(rank_col#6) windowspecdefinition(rank_col#6 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#9], [rank_col#6 DESC NULLS LAST]
               :  :                       +- Project [item_sk#5, rank_col#6]
               :  :                          +- SubqueryAlias `V2`
               :  :                             +- Project [item_sk#5, rank_col#6]
               :  :                                +- Filter (avg(ss_net_profit#40)#72 > (cast(0.9 as double) * scalar-subquery#8 []))
               :  :                                   :  +- Aggregate [ss_store_sk#25], [avg(ss_net_profit#40) AS rank_col#7]
               :  :                                   :     +- Filter ((ss_store_sk#25 = 4) && isnull(ss_addr_sk#24))
               :  :                                   :        +- SubqueryAlias `tpcds`.`store_sales`
               :  :                                   :           +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :  :                                   +- Aggregate [ss_item_sk#20], [ss_item_sk#20 AS item_sk#5, avg(ss_net_profit#40) AS rank_col#6, avg(ss_net_profit#40) AS avg(ss_net_profit#40)#72]
               :  :                                      +- Filter (ss_store_sk#25 = 4)
               :  :                                         +- SubqueryAlias `ss1`
               :  :                                            +- SubqueryAlias `tpcds`.`store_sales`
               :  :                                               +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :  +- SubqueryAlias `i1`
               :     +- SubqueryAlias `tpcds`.`item`
               :        +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
               +- SubqueryAlias `i2`
                  +- SubqueryAlias `tpcds`.`item`
                     +- Relation[i_item_sk#81,i_item_id#82,i_rec_start_date#83,i_rec_end_date#84,i_item_desc#85,i_current_price#86,i_wholesale_cost#87,i_brand_id#88,i_brand#89,i_class_id#90,i_class#91,i_category_id#92,i_category#93,i_manufact_id#94,i_manufact#95,i_size#96,i_formulation#97,i_color#98,i_units#99,i_container#100,i_manager_id#101,i_product_name#102] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [rnk#4 ASC NULLS FIRST], true
      +- Project [rnk#4, i_product_name#62 AS best_performing#10, i_product_name#102 AS worst_performing#11]
         +- Join Inner, (i_item_sk#81 = item_sk#5)
            :- Project [rnk#4, item_sk#5, i_product_name#62]
            :  +- Join Inner, (i_item_sk#41 = item_sk#0)
            :     :- Project [item_sk#0, rnk#4, item_sk#5]
            :     :  +- Join Inner, (rnk#4 = rnk#9)
            :     :     :- Project [item_sk#0, rnk#4]
            :     :     :  +- Filter ((isnotnull(rnk#4) && (rnk#4 < 11)) && isnotnull(item_sk#0))
            :     :     :     +- Window [rank(rank_col#1) windowspecdefinition(rank_col#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#4], [rank_col#1 ASC NULLS FIRST]
            :     :     :        +- Project [item_sk#0, rank_col#1]
            :     :     :           +- Filter (isnotnull(avg(ss_net_profit#40)#69) && (avg(ss_net_profit#40)#69 > (0.9 * scalar-subquery#3 [])))
            :     :     :              :  +- Aggregate [ss_store_sk#25], [avg(ss_net_profit#40) AS rank_col#2]
            :     :     :              :     +- Project [ss_store_sk#25, ss_net_profit#40]
            :     :     :              :        +- Filter ((isnotnull(ss_store_sk#25) && (ss_store_sk#25 = 4)) && isnull(ss_addr_sk#24))
            :     :     :              :           +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     :     :              +- Aggregate [ss_item_sk#20], [ss_item_sk#20 AS item_sk#0, avg(ss_net_profit#40) AS rank_col#1, avg(ss_net_profit#40) AS avg(ss_net_profit#40)#69]
            :     :     :                 +- Project [ss_item_sk#20, ss_net_profit#40]
            :     :     :                    +- Filter (isnotnull(ss_store_sk#25) && (ss_store_sk#25 = 4))
            :     :     :                       +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     :     +- Project [item_sk#5, rnk#9]
            :     :        +- Filter ((isnotnull(rnk#9) && (rnk#9 < 11)) && isnotnull(item_sk#5))
            :     :           +- Window [rank(rank_col#6) windowspecdefinition(rank_col#6 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#9], [rank_col#6 DESC NULLS LAST]
            :     :              +- Project [item_sk#5, rank_col#6]
            :     :                 +- Filter (isnotnull(avg(ss_net_profit#40)#72) && (avg(ss_net_profit#40)#72 > (0.9 * scalar-subquery#8 [])))
            :     :                    :  +- Aggregate [ss_store_sk#25], [avg(ss_net_profit#40) AS rank_col#7]
            :     :                    :     +- Project [ss_store_sk#25, ss_net_profit#40]
            :     :                    :        +- Filter ((isnotnull(ss_store_sk#25) && (ss_store_sk#25 = 4)) && isnull(ss_addr_sk#24))
            :     :                    :           +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     :                    +- Aggregate [ss_item_sk#20], [ss_item_sk#20 AS item_sk#5, avg(ss_net_profit#40) AS rank_col#6, avg(ss_net_profit#40) AS avg(ss_net_profit#40)#72]
            :     :                       +- Project [ss_item_sk#20, ss_net_profit#40]
            :     :                          +- Filter (isnotnull(ss_store_sk#25) && (ss_store_sk#25 = 4))
            :     :                             +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     +- Project [i_item_sk#41, i_product_name#62]
            :        +- Filter isnotnull(i_item_sk#41)
            :           +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
            +- Project [i_item_sk#81, i_product_name#102]
               +- Filter isnotnull(i_item_sk#81)
                  +- Relation[i_item_sk#81,i_item_id#82,i_rec_start_date#83,i_rec_end_date#84,i_item_desc#85,i_current_price#86,i_wholesale_cost#87,i_brand_id#88,i_brand#89,i_class_id#90,i_class#91,i_category_id#92,i_category#93,i_manufact_id#94,i_manufact#95,i_size#96,i_formulation#97,i_color#98,i_units#99,i_container#100,i_manager_id#101,i_product_name#102] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[rnk#4 ASC NULLS FIRST], output=[rnk#4,best_performing#10,worst_performing#11])
+- *(11) Project [rnk#4, i_product_name#62 AS best_performing#10, i_product_name#102 AS worst_performing#11]
   +- *(11) BroadcastHashJoin [item_sk#5], [i_item_sk#81], Inner, BuildRight
      :- *(11) Project [rnk#4, item_sk#5, i_product_name#62]
      :  +- *(11) BroadcastHashJoin [item_sk#0], [i_item_sk#41], Inner, BuildRight
      :     :- *(11) Project [item_sk#0, rnk#4, item_sk#5]
      :     :  +- *(11) SortMergeJoin [rnk#4], [rnk#9], Inner
      :     :     :- *(4) Sort [rnk#4 ASC NULLS FIRST], false, 0
      :     :     :  +- *(4) Project [item_sk#0, rnk#4]
      :     :     :     +- *(4) Filter ((isnotnull(rnk#4) && (rnk#4 < 11)) && isnotnull(item_sk#0))
      :     :     :        +- Window [rank(rank_col#1) windowspecdefinition(rank_col#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#4], [rank_col#1 ASC NULLS FIRST]
      :     :     :           +- *(3) Sort [rank_col#1 ASC NULLS FIRST], false, 0
      :     :     :              +- Exchange SinglePartition
      :     :     :                 +- *(2) Project [item_sk#0, rank_col#1]
      :     :     :                    +- *(2) Filter (isnotnull(avg(ss_net_profit#40)#69) && (avg(ss_net_profit#40)#69 > (0.9 * Subquery subquery3)))
      :     :     :                       :  +- Subquery subquery3
      :     :     :                       :     +- *(2) HashAggregate(keys=[ss_store_sk#25], functions=[avg(ss_net_profit#40)], output=[rank_col#2])
      :     :     :                       :        +- Exchange hashpartitioning(ss_store_sk#25, 200)
      :     :     :                       :           +- *(1) HashAggregate(keys=[ss_store_sk#25], functions=[partial_avg(ss_net_profit#40)], output=[ss_store_sk#25, sum#129, count#130L])
      :     :     :                       :              +- *(1) Project [ss_store_sk#25, ss_net_profit#40]
      :     :     :                       :                 +- *(1) Filter ((isnotnull(ss_store_sk#25) && (ss_store_sk#25 = 4)) && isnull(ss_addr_sk#24))
      :     :     :                       :                    +- *(1) FileScan parquet tpcds.store_sales[ss_addr_sk#24,ss_store_sk#25,ss_net_profit#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), EqualTo(ss_store_sk,4), IsNull(ss_addr_sk)], ReadSchema: struct<ss_addr_sk:int,ss_store_sk:int,ss_net_profit:double>
      :     :     :                       +- *(2) HashAggregate(keys=[ss_item_sk#20], functions=[avg(ss_net_profit#40)], output=[item_sk#0, rank_col#1, avg(ss_net_profit#40)#69])
      :     :     :                          +- Exchange hashpartitioning(ss_item_sk#20, 200)
      :     :     :                             +- *(1) HashAggregate(keys=[ss_item_sk#20], functions=[partial_avg(ss_net_profit#40)], output=[ss_item_sk#20, sum#121, count#122L])
      :     :     :                                +- *(1) Project [ss_item_sk#20, ss_net_profit#40]
      :     :     :                                   +- *(1) Filter (isnotnull(ss_store_sk#25) && (ss_store_sk#25 = 4))
      :     :     :                                      +- *(1) FileScan parquet tpcds.store_sales[ss_item_sk#20,ss_store_sk#25,ss_net_profit#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), EqualTo(ss_store_sk,4)], ReadSchema: struct<ss_item_sk:int,ss_store_sk:int,ss_net_profit:double>
      :     :     +- *(8) Sort [rnk#9 ASC NULLS FIRST], false, 0
      :     :        +- *(8) Project [item_sk#5, rnk#9]
      :     :           +- *(8) Filter ((isnotnull(rnk#9) && (rnk#9 < 11)) && isnotnull(item_sk#5))
      :     :              +- Window [rank(rank_col#6) windowspecdefinition(rank_col#6 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#9], [rank_col#6 DESC NULLS LAST]
      :     :                 +- *(7) Sort [rank_col#6 DESC NULLS LAST], false, 0
      :     :                    +- Exchange SinglePartition
      :     :                       +- *(6) Project [item_sk#5, rank_col#6]
      :     :                          +- *(6) Filter (isnotnull(avg(ss_net_profit#40)#72) && (avg(ss_net_profit#40)#72 > (0.9 * Subquery subquery8)))
      :     :                             :  +- Subquery subquery8
      :     :                             :     +- *(2) HashAggregate(keys=[ss_store_sk#25], functions=[avg(ss_net_profit#40)], output=[rank_col#7])
      :     :                             :        +- Exchange hashpartitioning(ss_store_sk#25, 200)
      :     :                             :           +- *(1) HashAggregate(keys=[ss_store_sk#25], functions=[partial_avg(ss_net_profit#40)], output=[ss_store_sk#25, sum#133, count#134L])
      :     :                             :              +- *(1) Project [ss_store_sk#25, ss_net_profit#40]
      :     :                             :                 +- *(1) Filter ((isnotnull(ss_store_sk#25) && (ss_store_sk#25 = 4)) && isnull(ss_addr_sk#24))
      :     :                             :                    +- *(1) FileScan parquet tpcds.store_sales[ss_addr_sk#24,ss_store_sk#25,ss_net_profit#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), EqualTo(ss_store_sk,4), IsNull(ss_addr_sk)], ReadSchema: struct<ss_addr_sk:int,ss_store_sk:int,ss_net_profit:double>
      :     :                             +- *(6) HashAggregate(keys=[ss_item_sk#20], functions=[avg(ss_net_profit#40)], output=[item_sk#5, rank_col#6, avg(ss_net_profit#40)#72])
      :     :                                +- ReusedExchange [ss_item_sk#20, sum#125, count#126L], Exchange hashpartitioning(ss_item_sk#20, 200)
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :        +- *(9) Project [i_item_sk#41, i_product_name#62]
      :           +- *(9) Filter isnotnull(i_item_sk#41)
      :              +- *(9) FileScan parquet tpcds.item[i_item_sk#41,i_product_name#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_product_name:string>
      +- ReusedExchange [i_item_sk#81, i_product_name#102], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.503 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 44 in stream 0 using template query44.tpl
------------------------------------------------------^^^

