== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_product_name ASC NULLS FIRST], true
      +- 'Distinct
         +- 'Project ['i_product_name]
            +- 'Filter ((('i_manufact_id >= 738) && ('i_manufact_id <= (738 + 40))) && (scalar-subquery#1 [] > 0))
               :  +- 'Project ['count(1) AS item_cnt#0]
               :     +- 'Filter ((('i_manufact = 'i1.i_manufact) && ((((('i_category = Women) && (('i_color = powder) || ('i_color = khaki))) && ((('i_units = Ounce) || ('i_units = Oz)) && (('i_size = medium) || ('i_size = extra large)))) || ((('i_category = Women) && (('i_color = brown) || ('i_color = honeydew))) && ((('i_units = Bunch) || ('i_units = Ton)) && (('i_size = N/A) || ('i_size = small))))) || (((('i_category = Men) && (('i_color = floral) || ('i_color = deep))) && ((('i_units = N/A) || ('i_units = Dozen)) && (('i_size = petite) || ('i_size = large)))) || ((('i_category = Men) && (('i_color = light) || ('i_color = cornflower))) && ((('i_units = Box) || ('i_units = Pound)) && (('i_size = medium) || ('i_size = extra large))))))) || (('i_manufact = 'i1.i_manufact) && ((((('i_category = Women) && (('i_color = midnight) || ('i_color = snow))) && ((('i_units = Pallet) || ('i_units = Gross)) && (('i_size = medium) || ('i_size = extra large)))) || ((('i_category = Women) && (('i_color = cyan) || ('i_color = papaya))) && ((('i_units = Cup) || ('i_units = Dram)) && (('i_size = N/A) || ('i_size = small))))) || (((('i_category = Men) && (('i_color = orange) || ('i_color = frosted))) && ((('i_units = Each) || ('i_units = Tbl)) && (('i_size = petite) || ('i_size = large)))) || ((('i_category = Men) && (('i_color = forest) || ('i_color = ghost))) && ((('i_units = Lb) || ('i_units = Bundle)) && (('i_size = medium) || ('i_size = extra large))))))))
               :        +- 'UnresolvedRelation `item`
               +- 'SubqueryAlias `i1`
                  +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
i_product_name: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_product_name#25 ASC NULLS FIRST], true
      +- Distinct
         +- Project [i_product_name#25]
            +- Filter (((i_manufact_id#17 >= 738) && (i_manufact_id#17 <= (738 + 40))) && (scalar-subquery#1 [i_manufact#18 && i_manufact#18] > cast(0 as bigint)))
               :  +- Aggregate [count(1) AS item_cnt#0L]
               :     +- Filter (((i_manufact#18 = outer(i_manufact#18)) && (((((i_category#16 = Women) && ((i_color#21 = powder) || (i_color#21 = khaki))) && (((i_units#22 = Ounce) || (i_units#22 = Oz)) && ((i_size#19 = medium) || (i_size#19 = extra large)))) || (((i_category#16 = Women) && ((i_color#21 = brown) || (i_color#21 = honeydew))) && (((i_units#22 = Bunch) || (i_units#22 = Ton)) && ((i_size#19 = N/A) || (i_size#19 = small))))) || ((((i_category#16 = Men) && ((i_color#21 = floral) || (i_color#21 = deep))) && (((i_units#22 = N/A) || (i_units#22 = Dozen)) && ((i_size#19 = petite) || (i_size#19 = large)))) || (((i_category#16 = Men) && ((i_color#21 = light) || (i_color#21 = cornflower))) && (((i_units#22 = Box) || (i_units#22 = Pound)) && ((i_size#19 = medium) || (i_size#19 = extra large))))))) || ((i_manufact#18 = outer(i_manufact#18)) && (((((i_category#16 = Women) && ((i_color#21 = midnight) || (i_color#21 = snow))) && (((i_units#22 = Pallet) || (i_units#22 = Gross)) && ((i_size#19 = medium) || (i_size#19 = extra large)))) || (((i_category#16 = Women) && ((i_color#21 = cyan) || (i_color#21 = papaya))) && (((i_units#22 = Cup) || (i_units#22 = Dram)) && ((i_size#19 = N/A) || (i_size#19 = small))))) || ((((i_category#16 = Men) && ((i_color#21 = orange) || (i_color#21 = frosted))) && (((i_units#22 = Each) || (i_units#22 = Tbl)) && ((i_size#19 = petite) || (i_size#19 = large)))) || (((i_category#16 = Men) && ((i_color#21 = forest) || (i_color#21 = ghost))) && (((i_units#22 = Lb) || (i_units#22 = Bundle)) && ((i_size#19 = medium) || (i_size#19 = extra large))))))))
               :        +- SubqueryAlias `tpcds`.`item`
               :           +- Relation[i_item_sk#4,i_item_id#5,i_rec_start_date#6,i_rec_end_date#7,i_item_desc#8,i_current_price#9,i_wholesale_cost#10,i_brand_id#11,i_brand#12,i_class_id#13,i_class#14,i_category_id#15,i_category#16,i_manufact_id#17,i_manufact#18,i_size#19,i_formulation#20,i_color#21,i_units#22,i_container#23,i_manager_id#24,i_product_name#25] parquet
               +- SubqueryAlias `i1`
                  +- SubqueryAlias `tpcds`.`item`
                     +- Relation[i_item_sk#4,i_item_id#5,i_rec_start_date#6,i_rec_end_date#7,i_item_desc#8,i_current_price#9,i_wholesale_cost#10,i_brand_id#11,i_brand#12,i_class_id#13,i_class#14,i_category_id#15,i_category#16,i_manufact_id#17,i_manufact#18,i_size#19,i_formulation#20,i_color#21,i_units#22,i_container#23,i_manager_id#24,i_product_name#25] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_product_name#25 ASC NULLS FIRST], true
      +- Aggregate [i_product_name#25], [i_product_name#25]
         +- Project [i_product_name#25]
            +- Join Inner, (i_manufact#18#27 = i_manufact#18)
               :- Project [i_manufact#18, i_product_name#25]
               :  +- Filter (((isnotnull(i_manufact_id#17) && (i_manufact_id#17 >= 738)) && (i_manufact_id#17 <= 778)) && isnotnull(i_manufact#18))
               :     +- Relation[i_item_sk#4,i_item_id#5,i_rec_start_date#6,i_rec_end_date#7,i_item_desc#8,i_current_price#9,i_wholesale_cost#10,i_brand_id#11,i_brand#12,i_class_id#13,i_class#14,i_category_id#15,i_category#16,i_manufact_id#17,i_manufact#18,i_size#19,i_formulation#20,i_color#21,i_units#22,i_container#23,i_manager_id#24,i_product_name#25] parquet
               +- Project [i_manufact#18#27]
                  +- Filter (if (isnull(alwaysTrue#28)) 0 else item_cnt#0L > 0)
                     +- Aggregate [i_manufact#18], [count(1) AS item_cnt#0L, i_manufact#18 AS i_manufact#18#27, true AS alwaysTrue#28]
                        +- Project [i_manufact#18]
                           +- Filter (((((i_category#16 = Women) && (((((i_color#21 = powder) || (i_color#21 = khaki)) && ((i_units#22 = Ounce) || (i_units#22 = Oz))) && ((i_size#19 = medium) || (i_size#19 = extra large))) || ((((i_color#21 = brown) || (i_color#21 = honeydew)) && ((i_units#22 = Bunch) || (i_units#22 = Ton))) && ((i_size#19 = N/A) || (i_size#19 = small))))) || ((i_category#16 = Men) && (((((i_color#21 = floral) || (i_color#21 = deep)) && ((i_units#22 = N/A) || (i_units#22 = Dozen))) && ((i_size#19 = petite) || (i_size#19 = large))) || ((((i_color#21 = light) || (i_color#21 = cornflower)) && ((i_units#22 = Box) || (i_units#22 = Pound))) && ((i_size#19 = medium) || (i_size#19 = extra large)))))) || (((i_category#16 = Women) && (((((i_color#21 = midnight) || (i_color#21 = snow)) && ((i_units#22 = Pallet) || (i_units#22 = Gross))) && ((i_size#19 = medium) || (i_size#19 = extra large))) || ((((i_color#21 = cyan) || (i_color#21 = papaya)) && ((i_units#22 = Cup) || (i_units#22 = Dram))) && ((i_size#19 = N/A) || (i_size#19 = small))))) || ((i_category#16 = Men) && (((((i_color#21 = orange) || (i_color#21 = frosted)) && ((i_units#22 = Each) || (i_units#22 = Tbl))) && ((i_size#19 = petite) || (i_size#19 = large))) || ((((i_color#21 = forest) || (i_color#21 = ghost)) && ((i_units#22 = Lb) || (i_units#22 = Bundle))) && ((i_size#19 = medium) || (i_size#19 = extra large))))))) && isnotnull(i_manufact#18))
                              +- Relation[i_item_sk#4,i_item_id#5,i_rec_start_date#6,i_rec_end_date#7,i_item_desc#8,i_current_price#9,i_wholesale_cost#10,i_brand_id#11,i_brand#12,i_class_id#13,i_class#14,i_category_id#15,i_category#16,i_manufact_id#17,i_manufact#18,i_size#19,i_formulation#20,i_color#21,i_units#22,i_container#23,i_manager_id#24,i_product_name#25] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_product_name#25 ASC NULLS FIRST], output=[i_product_name#25])
+- *(4) HashAggregate(keys=[i_product_name#25], functions=[], output=[i_product_name#25])
   +- Exchange hashpartitioning(i_product_name#25, 200)
      +- *(3) HashAggregate(keys=[i_product_name#25], functions=[], output=[i_product_name#25])
         +- *(3) Project [i_product_name#25]
            +- *(3) BroadcastHashJoin [i_manufact#18], [i_manufact#18#27], Inner, BuildRight
               :- *(3) Project [i_manufact#18, i_product_name#25]
               :  +- *(3) Filter (((isnotnull(i_manufact_id#17) && (i_manufact_id#17 >= 738)) && (i_manufact_id#17 <= 778)) && isnotnull(i_manufact#18))
               :     +- *(3) FileScan parquet tpcds.item[i_manufact_id#17,i_manufact#18,i_product_name#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manufact_id), GreaterThanOrEqual(i_manufact_id,738), LessThanOrEqual(i_manufact_id,7..., ReadSchema: struct<i_manufact_id:int,i_manufact:string,i_product_name:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
                  +- *(2) Project [i_manufact#18#27]
                     +- *(2) Filter (if (isnull(alwaysTrue#28)) 0 else item_cnt#0L > 0)
                        +- *(2) HashAggregate(keys=[i_manufact#18], functions=[count(1)], output=[item_cnt#0L, i_manufact#18#27, alwaysTrue#28])
                           +- Exchange hashpartitioning(i_manufact#18, 200)
                              +- *(1) HashAggregate(keys=[i_manufact#18], functions=[partial_count(1)], output=[i_manufact#18, count#30L])
                                 +- *(1) Project [i_manufact#18]
                                    +- *(1) Filter (((((i_category#16 = Women) && (((((i_color#21 = powder) || (i_color#21 = khaki)) && ((i_units#22 = Ounce) || (i_units#22 = Oz))) && ((i_size#19 = medium) || (i_size#19 = extra large))) || ((((i_color#21 = brown) || (i_color#21 = honeydew)) && ((i_units#22 = Bunch) || (i_units#22 = Ton))) && ((i_size#19 = N/A) || (i_size#19 = small))))) || ((i_category#16 = Men) && (((((i_color#21 = floral) || (i_color#21 = deep)) && ((i_units#22 = N/A) || (i_units#22 = Dozen))) && ((i_size#19 = petite) || (i_size#19 = large))) || ((((i_color#21 = light) || (i_color#21 = cornflower)) && ((i_units#22 = Box) || (i_units#22 = Pound))) && ((i_size#19 = medium) || (i_size#19 = extra large)))))) || (((i_category#16 = Women) && (((((i_color#21 = midnight) || (i_color#21 = snow)) && ((i_units#22 = Pallet) || (i_units#22 = Gross))) && ((i_size#19 = medium) || (i_size#19 = extra large))) || ((((i_color#21 = cyan) || (i_color#21 = papaya)) && ((i_units#22 = Cup) || (i_units#22 = Dram))) && ((i_size#19 = N/A) || (i_size#19 = small))))) || ((i_category#16 = Men) && (((((i_color#21 = orange) || (i_color#21 = frosted)) && ((i_units#22 = Each) || (i_units#22 = Tbl))) && ((i_size#19 = petite) || (i_size#19 = large))) || ((((i_color#21 = forest) || (i_color#21 = ghost)) && ((i_units#22 = Lb) || (i_units#22 = Bundle))) && ((i_size#19 = medium) || (i_size#19 = extra large))))))) && isnotnull(i_manufact#18))
                                       +- *(1) FileScan parquet tpcds.item[i_category#16,i_manufact#18,i_size#19,i_color#21,i_units#22] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(i_category,Women),Or(And(And(Or(EqualTo(i_color,powder),EqualTo(i_color,khaki)..., ReadSchema: struct<i_category:string,i_manufact:string,i_size:string,i_color:string,i_units:string>
Time taken: 4.024 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 41 in stream 0 using template query41.tpl
------------------------------------------------------^^^

