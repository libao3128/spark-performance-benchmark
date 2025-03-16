Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581159441
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_product_name ASC NULLS FIRST], true
      +- 'Distinct
         +- 'Project ['i_product_name]
            +- 'Filter ((('i_manufact_id >= 738) AND ('i_manufact_id <= (738 + 40))) AND (scalar-subquery#1 [] > 0))
               :  +- 'Project ['count(1) AS item_cnt#0]
               :     +- 'Filter ((('i_manufact = 'i1.i_manufact) AND ((((('i_category = Women) AND (('i_color = powder) OR ('i_color = khaki))) AND ((('i_units = Ounce) OR ('i_units = Oz)) AND (('i_size = medium) OR ('i_size = extra large)))) OR ((('i_category = Women) AND (('i_color = brown) OR ('i_color = honeydew))) AND ((('i_units = Bunch) OR ('i_units = Ton)) AND (('i_size = N/A) OR ('i_size = small))))) OR (((('i_category = Men) AND (('i_color = floral) OR ('i_color = deep))) AND ((('i_units = N/A) OR ('i_units = Dozen)) AND (('i_size = petite) OR ('i_size = large)))) OR ((('i_category = Men) AND (('i_color = light) OR ('i_color = cornflower))) AND ((('i_units = Box) OR ('i_units = Pound)) AND (('i_size = medium) OR ('i_size = extra large))))))) OR (('i_manufact = 'i1.i_manufact) AND ((((('i_category = Women) AND (('i_color = midnight) OR ('i_color = snow))) AND ((('i_units = Pallet) OR ('i_units = Gross)) AND (('i_size = medium) OR ('i_size = extra large)))) OR ((('i_category = Women) AND (('i_color = cyan) OR ('i_color = papaya))) AND ((('i_units = Cup) OR ('i_units = Dram)) AND (('i_size = N/A) OR ('i_size = small))))) OR (((('i_category = Men) AND (('i_color = orange) OR ('i_color = frosted))) AND ((('i_units = Each) OR ('i_units = Tbl)) AND (('i_size = petite) OR ('i_size = large)))) OR ((('i_category = Men) AND (('i_color = forest) OR ('i_color = ghost))) AND ((('i_units = Lb) OR ('i_units = Bundle)) AND (('i_size = medium) OR ('i_size = extra large))))))))
               :        +- 'UnresolvedRelation [item], [], false
               +- 'SubqueryAlias i1
                  +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
i_product_name: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_product_name#28 ASC NULLS FIRST], true
      +- Distinct
         +- Project [i_product_name#28]
            +- Filter (((i_manufact_id#20 >= 738) AND (i_manufact_id#20 <= (738 + 40))) AND (scalar-subquery#1 [i_manufact#21 && i_manufact#21] > cast(0 as bigint)))
               :  +- Aggregate [count(1) AS item_cnt#0L]
               :     +- Filter (((i_manufact#45 = outer(i_manufact#21)) AND (((((i_category#43 = Women) AND ((i_color#48 = powder) OR (i_color#48 = khaki))) AND (((i_units#49 = Ounce) OR (i_units#49 = Oz)) AND ((i_size#46 = medium) OR (i_size#46 = extra large)))) OR (((i_category#43 = Women) AND ((i_color#48 = brown) OR (i_color#48 = honeydew))) AND (((i_units#49 = Bunch) OR (i_units#49 = Ton)) AND ((i_size#46 = N/A) OR (i_size#46 = small))))) OR ((((i_category#43 = Men) AND ((i_color#48 = floral) OR (i_color#48 = deep))) AND (((i_units#49 = N/A) OR (i_units#49 = Dozen)) AND ((i_size#46 = petite) OR (i_size#46 = large)))) OR (((i_category#43 = Men) AND ((i_color#48 = light) OR (i_color#48 = cornflower))) AND (((i_units#49 = Box) OR (i_units#49 = Pound)) AND ((i_size#46 = medium) OR (i_size#46 = extra large))))))) OR ((i_manufact#45 = outer(i_manufact#21)) AND (((((i_category#43 = Women) AND ((i_color#48 = midnight) OR (i_color#48 = snow))) AND (((i_units#49 = Pallet) OR (i_units#49 = Gross)) AND ((i_size#46 = medium) OR (i_size#46 = extra large)))) OR (((i_category#43 = Women) AND ((i_color#48 = cyan) OR (i_color#48 = papaya))) AND (((i_units#49 = Cup) OR (i_units#49 = Dram)) AND ((i_size#46 = N/A) OR (i_size#46 = small))))) OR ((((i_category#43 = Men) AND ((i_color#48 = orange) OR (i_color#48 = frosted))) AND (((i_units#49 = Each) OR (i_units#49 = Tbl)) AND ((i_size#46 = petite) OR (i_size#46 = large)))) OR (((i_category#43 = Men) AND ((i_color#48 = forest) OR (i_color#48 = ghost))) AND (((i_units#49 = Lb) OR (i_units#49 = Bundle)) AND ((i_size#46 = medium) OR (i_size#46 = extra large))))))))
               :        +- SubqueryAlias spark_catalog.tpcds.item
               :           +- Relation spark_catalog.tpcds.item[i_item_sk#31,i_item_id#32,i_rec_start_date#33,i_rec_end_date#34,i_item_desc#35,i_current_price#36,i_wholesale_cost#37,i_brand_id#38,i_brand#39,i_class_id#40,i_class#41,i_category_id#42,i_category#43,i_manufact_id#44,i_manufact#45,i_size#46,i_formulation#47,i_color#48,i_units#49,i_container#50,i_manager_id#51,i_product_name#52] parquet
               +- SubqueryAlias i1
                  +- SubqueryAlias spark_catalog.tpcds.item
                     +- Relation spark_catalog.tpcds.item[i_item_sk#7,i_item_id#8,i_rec_start_date#9,i_rec_end_date#10,i_item_desc#11,i_current_price#12,i_wholesale_cost#13,i_brand_id#14,i_brand#15,i_class_id#16,i_class#17,i_category_id#18,i_category#19,i_manufact_id#20,i_manufact#21,i_size#22,i_formulation#23,i_color#24,i_units#25,i_container#26,i_manager_id#27,i_product_name#28] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_product_name#28 ASC NULLS FIRST], true
      +- Aggregate [i_product_name#28], [i_product_name#28]
         +- Project [i_product_name#28]
            +- Join Inner, (i_manufact#45 = i_manufact#21)
               :- Project [i_manufact#21, i_product_name#28]
               :  +- Filter ((isnotnull(i_manufact_id#20) AND ((i_manufact_id#20 >= 738) AND (i_manufact_id#20 <= 778))) AND isnotnull(i_manufact#21))
               :     +- Relation spark_catalog.tpcds.item[i_item_sk#7,i_item_id#8,i_rec_start_date#9,i_rec_end_date#10,i_item_desc#11,i_current_price#12,i_wholesale_cost#13,i_brand_id#14,i_brand#15,i_class_id#16,i_class#17,i_category_id#18,i_category#19,i_manufact_id#20,i_manufact#21,i_size#22,i_formulation#23,i_color#24,i_units#25,i_container#26,i_manager_id#27,i_product_name#28] parquet
               +- Project [i_manufact#45]
                  +- Filter (item_cnt#0L > 0)
                     +- Aggregate [i_manufact#45], [count(1) AS item_cnt#0L, i_manufact#45]
                        +- Project [i_manufact#45]
                           +- Filter (((((i_category#43 = Women) AND (((((i_color#48 = powder) OR (i_color#48 = khaki)) AND ((i_units#49 = Ounce) OR (i_units#49 = Oz))) AND ((i_size#46 = medium) OR (i_size#46 = extra large))) OR ((((i_color#48 = brown) OR (i_color#48 = honeydew)) AND ((i_units#49 = Bunch) OR (i_units#49 = Ton))) AND ((i_size#46 = N/A) OR (i_size#46 = small))))) OR ((i_category#43 = Men) AND (((((i_color#48 = floral) OR (i_color#48 = deep)) AND ((i_units#49 = N/A) OR (i_units#49 = Dozen))) AND ((i_size#46 = petite) OR (i_size#46 = large))) OR ((((i_color#48 = light) OR (i_color#48 = cornflower)) AND ((i_units#49 = Box) OR (i_units#49 = Pound))) AND ((i_size#46 = medium) OR (i_size#46 = extra large)))))) OR (((i_category#43 = Women) AND (((((i_color#48 = midnight) OR (i_color#48 = snow)) AND ((i_units#49 = Pallet) OR (i_units#49 = Gross))) AND ((i_size#46 = medium) OR (i_size#46 = extra large))) OR ((((i_color#48 = cyan) OR (i_color#48 = papaya)) AND ((i_units#49 = Cup) OR (i_units#49 = Dram))) AND ((i_size#46 = N/A) OR (i_size#46 = small))))) OR ((i_category#43 = Men) AND (((((i_color#48 = orange) OR (i_color#48 = frosted)) AND ((i_units#49 = Each) OR (i_units#49 = Tbl))) AND ((i_size#46 = petite) OR (i_size#46 = large))) OR ((((i_color#48 = forest) OR (i_color#48 = ghost)) AND ((i_units#49 = Lb) OR (i_units#49 = Bundle))) AND ((i_size#46 = medium) OR (i_size#46 = extra large))))))) AND isnotnull(i_manufact#45))
                              +- Relation spark_catalog.tpcds.item[i_item_sk#31,i_item_id#32,i_rec_start_date#33,i_rec_end_date#34,i_item_desc#35,i_current_price#36,i_wholesale_cost#37,i_brand_id#38,i_brand#39,i_class_id#40,i_class#41,i_category_id#42,i_category#43,i_manufact_id#44,i_manufact#45,i_size#46,i_formulation#47,i_color#48,i_units#49,i_container#50,i_manager_id#51,i_product_name#52] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_product_name#28 ASC NULLS FIRST], output=[i_product_name#28])
   +- HashAggregate(keys=[i_product_name#28], functions=[], output=[i_product_name#28])
      +- Exchange hashpartitioning(i_product_name#28, 200), ENSURE_REQUIREMENTS, [plan_id=65]
         +- HashAggregate(keys=[i_product_name#28], functions=[], output=[i_product_name#28])
            +- Project [i_product_name#28]
               +- BroadcastHashJoin [i_manufact#21], [i_manufact#45], Inner, BuildRight, false
                  :- Project [i_manufact#21, i_product_name#28]
                  :  +- Filter (((isnotnull(i_manufact_id#20) AND (i_manufact_id#20 >= 738)) AND (i_manufact_id#20 <= 778)) AND isnotnull(i_manufact#21))
                  :     +- FileScan parquet spark_catalog.tpcds.item[i_manufact_id#20,i_manufact#21,i_product_name#28] Batched: true, DataFilters: [isnotnull(i_manufact_id#20), (i_manufact_id#20 >= 738), (i_manufact_id#20 <= 778), isnotnull(i_m..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manufact_id), GreaterThanOrEqual(i_manufact_id,738), LessThanOrEqual(i_manufact_id,7..., ReadSchema: struct<i_manufact_id:int,i_manufact:string,i_product_name:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=60]
                     +- Project [i_manufact#45]
                        +- Filter (item_cnt#0L > 0)
                           +- HashAggregate(keys=[i_manufact#45], functions=[count(1)], output=[item_cnt#0L, i_manufact#45])
                              +- Exchange hashpartitioning(i_manufact#45, 200), ENSURE_REQUIREMENTS, [plan_id=55]
                                 +- HashAggregate(keys=[i_manufact#45], functions=[partial_count(1)], output=[i_manufact#45, count#56L])
                                    +- Project [i_manufact#45]
                                       +- Filter (((((i_category#43 = Women) AND (((((i_color#48 = powder) OR (i_color#48 = khaki)) AND ((i_units#49 = Ounce) OR (i_units#49 = Oz))) AND ((i_size#46 = medium) OR (i_size#46 = extra large))) OR ((((i_color#48 = brown) OR (i_color#48 = honeydew)) AND ((i_units#49 = Bunch) OR (i_units#49 = Ton))) AND ((i_size#46 = N/A) OR (i_size#46 = small))))) OR ((i_category#43 = Men) AND (((((i_color#48 = floral) OR (i_color#48 = deep)) AND ((i_units#49 = N/A) OR (i_units#49 = Dozen))) AND ((i_size#46 = petite) OR (i_size#46 = large))) OR ((((i_color#48 = light) OR (i_color#48 = cornflower)) AND ((i_units#49 = Box) OR (i_units#49 = Pound))) AND ((i_size#46 = medium) OR (i_size#46 = extra large)))))) OR (((i_category#43 = Women) AND (((((i_color#48 = midnight) OR (i_color#48 = snow)) AND ((i_units#49 = Pallet) OR (i_units#49 = Gross))) AND ((i_size#46 = medium) OR (i_size#46 = extra large))) OR ((((i_color#48 = cyan) OR (i_color#48 = papaya)) AND ((i_units#49 = Cup) OR (i_units#49 = Dram))) AND ((i_size#46 = N/A) OR (i_size#46 = small))))) OR ((i_category#43 = Men) AND (((((i_color#48 = orange) OR (i_color#48 = frosted)) AND ((i_units#49 = Each) OR (i_units#49 = Tbl))) AND ((i_size#46 = petite) OR (i_size#46 = large))) OR ((((i_color#48 = forest) OR (i_color#48 = ghost)) AND ((i_units#49 = Lb) OR (i_units#49 = Bundle))) AND ((i_size#46 = medium) OR (i_size#46 = extra large))))))) AND isnotnull(i_manufact#45))
                                          +- FileScan parquet spark_catalog.tpcds.item[i_category#43,i_manufact#45,i_size#46,i_color#48,i_units#49] Batched: true, DataFilters: [((((i_category#43 = Women) AND (((((i_color#48 = powder) OR (i_color#48 = khaki)) AND ((i_units#..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(i_category,Women),Or(And(And(Or(EqualTo(i_color,powder),EqualTo(i_color,khaki)..., ReadSchema: struct<i_category:string,i_manufact:string,i_size:string,i_color:string,i_units:string>

Time taken: 2.328 seconds, Fetched 1 row(s)
