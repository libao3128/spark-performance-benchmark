Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581746257
== Parsed Logical Plan ==
CTE [v1, v2]
:  :- 'SubqueryAlias v1
:  :  +- 'Aggregate ['i_category, 'i_brand, 'cc_name, 'd_year, 'd_moy], ['i_category, 'i_brand, 'cc_name, 'd_year, 'd_moy, 'sum('cs_sales_price) AS sum_sales#0, 'avg('sum('cs_sales_price)) windowspecdefinition('i_category, 'i_brand, 'cc_name, 'd_year, unspecifiedframe$()) AS avg_monthly_sales#1, 'rank() windowspecdefinition('i_category, 'i_brand, 'cc_name, 'd_year ASC NULLS FIRST, 'd_moy ASC NULLS FIRST, unspecifiedframe$()) AS rn#2]
:  :     +- 'Filter ((('cs_item_sk = 'i_item_sk) AND ('cs_sold_date_sk = 'd_date_sk)) AND (('cc_call_center_sk = 'cs_call_center_sk) AND ((('d_year = 1999) OR (('d_year = (1999 - 1)) AND ('d_moy = 12))) OR (('d_year = (1999 + 1)) AND ('d_moy = 1)))))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation [item], [], false
:  :           :  :  +- 'UnresolvedRelation [catalog_sales], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [call_center], [], false
:  +- 'SubqueryAlias v2
:     +- 'Project ['v1.i_category, 'v1.i_brand, 'v1.cc_name, 'v1.d_year, 'v1.d_moy, 'v1.avg_monthly_sales, 'v1.sum_sales, 'v1_lag.sum_sales AS psum#3, 'v1_lead.sum_sales AS nsum#4]
:        +- 'Filter (((('v1.i_category = 'v1_lag.i_category) AND ('v1.i_category = 'v1_lead.i_category)) AND (('v1.i_brand = 'v1_lag.i_brand) AND ('v1.i_brand = 'v1_lead.i_brand))) AND ((('v1.cc_name = 'v1_lag.cc_name) AND ('v1.cc_name = 'v1_lead.cc_name)) AND (('v1.rn = ('v1_lag.rn + 1)) AND ('v1.rn = ('v1_lead.rn - 1)))))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation [v1], [], false
:              :  +- 'SubqueryAlias v1_lag
:              :     +- 'UnresolvedRelation [v1], [], false
:              +- 'SubqueryAlias v1_lead
:                 +- 'UnresolvedRelation [v1], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort [('sum_sales - 'avg_monthly_sales) ASC NULLS FIRST, 3 ASC NULLS FIRST], true
         +- 'Project [*]
            +- 'Filter ((('d_year = 1999) AND ('avg_monthly_sales > 0)) AND (CASE WHEN ('avg_monthly_sales > 0) THEN ('abs(('sum_sales - 'avg_monthly_sales)) / 'avg_monthly_sales) ELSE null END > 0.1))
               +- 'UnresolvedRelation [v2], [], false

== Analyzed Logical Plan ==
i_category: string, i_brand: string, cc_name: string, d_year: int, d_moy: int, avg_monthly_sales: double, sum_sales: double, psum: double, nsum: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias v1
:     +- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, avg_monthly_sales#1, rn#2]
:        +- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, _w0#142, avg_monthly_sales#1, rn#2, avg_monthly_sales#1, rn#2]
:           +- Window [rank(d_year#74, d_moy#76) windowspecdefinition(i_category#24, i_brand#20, cc_name#102, d_year#74 ASC NULLS FIRST, d_moy#76 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#24, i_brand#20, cc_name#102], [d_year#74 ASC NULLS FIRST, d_moy#76 ASC NULLS FIRST]
:              +- Window [avg(_w0#142) windowspecdefinition(i_category#24, i_brand#20, cc_name#102, d_year#74, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#24, i_brand#20, cc_name#102, d_year#74]
:                 +- Aggregate [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76], [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum(cs_sales_price#55) AS sum_sales#0, sum(cs_sales_price#55) AS _w0#142]
:                    +- Filter (((cs_item_sk#49 = i_item_sk#12) AND (cs_sold_date_sk#34 = d_date_sk#68)) AND ((cc_call_center_sk#96 = cs_call_center_sk#45) AND (((d_year#74 = 1999) OR ((d_year#74 = (1999 - 1)) AND (d_moy#76 = 12))) OR ((d_year#74 = (1999 + 1)) AND (d_moy#76 = 1)))))
:                       +- Join Inner
:                          :- Join Inner
:                          :  :- Join Inner
:                          :  :  :- SubqueryAlias spark_catalog.tpcds.item
:                          :  :  :  +- Relation spark_catalog.tpcds.item[i_item_sk#12,i_item_id#13,i_rec_start_date#14,i_rec_end_date#15,i_item_desc#16,i_current_price#17,i_wholesale_cost#18,i_brand_id#19,i_brand#20,i_class_id#21,i_class#22,i_category_id#23,i_category#24,i_manufact_id#25,i_manufact#26,i_size#27,i_formulation#28,i_color#29,i_units#30,i_container#31,i_manager_id#32,i_product_name#33] parquet
:                          :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_sales
:                          :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#34,cs_sold_time_sk#35,cs_ship_date_sk#36,cs_bill_customer_sk#37,cs_bill_cdemo_sk#38,cs_bill_hdemo_sk#39,cs_bill_addr_sk#40,cs_ship_customer_sk#41,cs_ship_cdemo_sk#42,cs_ship_hdemo_sk#43,cs_ship_addr_sk#44,cs_call_center_sk#45,cs_catalog_page_sk#46,cs_ship_mode_sk#47,cs_warehouse_sk#48,cs_item_sk#49,cs_promo_sk#50,cs_order_number#51,cs_quantity#52,cs_wholesale_cost#53,cs_list_price#54,cs_sales_price#55,cs_ext_discount_amt#56,cs_ext_sales_price#57,... 10 more fields] parquet
:                          :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:                          :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#68,d_date_id#69,d_date#70,d_month_seq#71,d_week_seq#72,d_quarter_seq#73,d_year#74,d_dow#75,d_moy#76,d_dom#77,d_qoy#78,d_fy_year#79,d_fy_quarter_seq#80,d_fy_week_seq#81,d_day_name#82,d_quarter_name#83,d_holiday#84,d_weekend#85,d_following_holiday#86,d_first_dom#87,d_last_dom#88,d_same_day_ly#89,d_same_day_lq#90,d_current_day#91,... 4 more fields] parquet
:                          +- SubqueryAlias spark_catalog.tpcds.call_center
:                             +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#96,cc_call_center_id#97,cc_rec_start_date#98,cc_rec_end_date#99,cc_closed_date_sk#100,cc_open_date_sk#101,cc_name#102,cc_class#103,cc_employees#104,cc_sq_ft#105,cc_hours#106,cc_manager#107,cc_mkt_id#108,cc_mkt_class#109,cc_mkt_desc#110,cc_market_manager#111,cc_division#112,cc_division_name#113,cc_company#114,cc_company_name#115,cc_street_number#116,cc_street_name#117,cc_street_type#118,cc_suite_number#119,... 7 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias v2
:     +- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, avg_monthly_sales#1, sum_sales#0, sum_sales#148 AS psum#3, sum_sales#156 AS nsum#4]
:        +- Filter ((((i_category#24 = i_category#143) AND (i_category#24 = i_category#151)) AND ((i_brand#20 = i_brand#144) AND (i_brand#20 = i_brand#152))) AND (((cc_name#102 = cc_name#145) AND (cc_name#102 = cc_name#153)) AND ((rn#2 = (rn#150 + 1)) AND (rn#2 = (rn#158 - 1)))))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias v1
:              :  :  +- CTERelationRef 0, true, [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, avg_monthly_sales#1, rn#2], false
:              :  +- SubqueryAlias v1_lag
:              :     +- SubqueryAlias v1
:              :        +- CTERelationRef 0, true, [i_category#143, i_brand#144, cc_name#145, d_year#146, d_moy#147, sum_sales#148, avg_monthly_sales#149, rn#150], false
:              +- SubqueryAlias v1_lead
:                 +- SubqueryAlias v1
:                    +- CTERelationRef 0, true, [i_category#151, i_brand#152, cc_name#153, d_year#154, d_moy#155, sum_sales#156, avg_monthly_sales#157, rn#158], false
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, cc_name#102 ASC NULLS FIRST], true
         +- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, avg_monthly_sales#1, sum_sales#0, psum#3, nsum#4]
            +- Filter (((d_year#74 = 1999) AND (avg_monthly_sales#1 > cast(0 as double))) AND (CASE WHEN (avg_monthly_sales#1 > cast(0 as double)) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE cast(null as double) END > cast(0.1 as double)))
               +- SubqueryAlias v2
                  +- CTERelationRef 1, true, [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, avg_monthly_sales#1, sum_sales#0, psum#3, nsum#4], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, cc_name#102 ASC NULLS FIRST], true
      +- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, avg_monthly_sales#1, sum_sales#0, sum_sales#148 AS psum#3, sum_sales#156 AS nsum#4]
         +- Join Inner, ((((i_category#24 = i_category#531) AND (i_brand#20 = i_brand#527)) AND (cc_name#102 = cc_name#609)) AND (rn#2 = (rn#638 - 1)))
            :- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#148]
            :  +- Join Inner, ((((i_category#24 = i_category#411) AND (i_brand#20 = i_brand#407)) AND (cc_name#102 = cc_name#489)) AND (rn#2 = (rn#518 + 1)))
            :     :- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, avg_monthly_sales#1, rn#2]
            :     :  +- Filter ((isnotnull(avg_monthly_sales#1) AND (avg_monthly_sales#1 > 0.0)) AND CASE WHEN (avg_monthly_sales#1 > 0.0) THEN ((abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) > 0.1) END)
            :     :     +- Window [avg(_w0#142) windowspecdefinition(i_category#24, i_brand#20, cc_name#102, d_year#74, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#24, i_brand#20, cc_name#102, d_year#74]
            :     :        +- Filter (isnotnull(d_year#74) AND (d_year#74 = 1999))
            :     :           +- Window [rank(d_year#74, d_moy#76) windowspecdefinition(i_category#24, i_brand#20, cc_name#102, d_year#74 ASC NULLS FIRST, d_moy#76 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#24, i_brand#20, cc_name#102], [d_year#74 ASC NULLS FIRST, d_moy#76 ASC NULLS FIRST]
            :     :              +- Aggregate [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76], [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum(cs_sales_price#55) AS sum_sales#0, sum(cs_sales_price#55) AS _w0#142]
            :     :                 +- Project [i_brand#20, i_category#24, cs_sales_price#55, d_year#74, d_moy#76, cc_name#102]
            :     :                    +- Join Inner, (cc_call_center_sk#96 = cs_call_center_sk#45)
            :     :                       :- Project [i_brand#20, i_category#24, cs_call_center_sk#45, cs_sales_price#55, d_year#74, d_moy#76]
            :     :                       :  +- Join Inner, (cs_sold_date_sk#34 = d_date_sk#68)
            :     :                       :     :- Project [i_brand#20, i_category#24, cs_sold_date_sk#34, cs_call_center_sk#45, cs_sales_price#55]
            :     :                       :     :  +- Join Inner, (cs_item_sk#49 = i_item_sk#12)
            :     :                       :     :     :- Project [i_item_sk#12, i_brand#20, i_category#24]
            :     :                       :     :     :  +- Filter (isnotnull(i_item_sk#12) AND (isnotnull(i_category#24) AND isnotnull(i_brand#20)))
            :     :                       :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#12,i_item_id#13,i_rec_start_date#14,i_rec_end_date#15,i_item_desc#16,i_current_price#17,i_wholesale_cost#18,i_brand_id#19,i_brand#20,i_class_id#21,i_class#22,i_category_id#23,i_category#24,i_manufact_id#25,i_manufact#26,i_size#27,i_formulation#28,i_color#29,i_units#30,i_container#31,i_manager_id#32,i_product_name#33] parquet
            :     :                       :     :     +- Project [cs_sold_date_sk#34, cs_call_center_sk#45, cs_item_sk#49, cs_sales_price#55]
            :     :                       :     :        +- Filter (isnotnull(cs_item_sk#49) AND (isnotnull(cs_sold_date_sk#34) AND isnotnull(cs_call_center_sk#45)))
            :     :                       :     :           +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#34,cs_sold_time_sk#35,cs_ship_date_sk#36,cs_bill_customer_sk#37,cs_bill_cdemo_sk#38,cs_bill_hdemo_sk#39,cs_bill_addr_sk#40,cs_ship_customer_sk#41,cs_ship_cdemo_sk#42,cs_ship_hdemo_sk#43,cs_ship_addr_sk#44,cs_call_center_sk#45,cs_catalog_page_sk#46,cs_ship_mode_sk#47,cs_warehouse_sk#48,cs_item_sk#49,cs_promo_sk#50,cs_order_number#51,cs_quantity#52,cs_wholesale_cost#53,cs_list_price#54,cs_sales_price#55,cs_ext_discount_amt#56,cs_ext_sales_price#57,... 10 more fields] parquet
            :     :                       :     +- Project [d_date_sk#68, d_year#74, d_moy#76]
            :     :                       :        +- Filter ((((d_year#74 = 1999) OR ((d_year#74 = 1998) AND (d_moy#76 = 12))) OR ((d_year#74 = 2000) AND (d_moy#76 = 1))) AND isnotnull(d_date_sk#68))
            :     :                       :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#68,d_date_id#69,d_date#70,d_month_seq#71,d_week_seq#72,d_quarter_seq#73,d_year#74,d_dow#75,d_moy#76,d_dom#77,d_qoy#78,d_fy_year#79,d_fy_quarter_seq#80,d_fy_week_seq#81,d_day_name#82,d_quarter_name#83,d_holiday#84,d_weekend#85,d_following_holiday#86,d_first_dom#87,d_last_dom#88,d_same_day_ly#89,d_same_day_lq#90,d_current_day#91,... 4 more fields] parquet
            :     :                       +- Project [cc_call_center_sk#96, cc_name#102]
            :     :                          +- Filter (isnotnull(cc_call_center_sk#96) AND isnotnull(cc_name#102))
            :     :                             +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#96,cc_call_center_id#97,cc_rec_start_date#98,cc_rec_end_date#99,cc_closed_date_sk#100,cc_open_date_sk#101,cc_name#102,cc_class#103,cc_employees#104,cc_sq_ft#105,cc_hours#106,cc_manager#107,cc_mkt_id#108,cc_mkt_class#109,cc_mkt_desc#110,cc_market_manager#111,cc_division#112,cc_division_name#113,cc_company#114,cc_company_name#115,cc_street_number#116,cc_street_name#117,cc_street_type#118,cc_suite_number#119,... 7 more fields] parquet
            :     +- Project [i_category#411, i_brand#407, cc_name#489, sum_sales#0 AS sum_sales#148, rn#518]
            :        +- Window [rank(d_year#461, d_moy#463) windowspecdefinition(i_category#411, i_brand#407, cc_name#489, d_year#461 ASC NULLS FIRST, d_moy#463 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#518], [i_category#411, i_brand#407, cc_name#489], [d_year#461 ASC NULLS FIRST, d_moy#463 ASC NULLS FIRST]
            :           +- Aggregate [i_category#411, i_brand#407, cc_name#489, d_year#461, d_moy#463], [i_category#411, i_brand#407, cc_name#489, d_year#461, d_moy#463, sum(cs_sales_price#442) AS sum_sales#0]
            :              +- Project [i_brand#407, i_category#411, cs_sales_price#442, d_year#461, d_moy#463, cc_name#489]
            :                 +- Join Inner, (cc_call_center_sk#483 = cs_call_center_sk#432)
            :                    :- Project [i_brand#407, i_category#411, cs_call_center_sk#432, cs_sales_price#442, d_year#461, d_moy#463]
            :                    :  +- Join Inner, (cs_sold_date_sk#421 = d_date_sk#455)
            :                    :     :- Project [i_brand#407, i_category#411, cs_sold_date_sk#421, cs_call_center_sk#432, cs_sales_price#442]
            :                    :     :  +- Join Inner, (cs_item_sk#436 = i_item_sk#399)
            :                    :     :     :- Project [i_item_sk#399, i_brand#407, i_category#411]
            :                    :     :     :  +- Filter (isnotnull(i_item_sk#399) AND (isnotnull(i_category#411) AND isnotnull(i_brand#407)))
            :                    :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#399,i_item_id#400,i_rec_start_date#401,i_rec_end_date#402,i_item_desc#403,i_current_price#404,i_wholesale_cost#405,i_brand_id#406,i_brand#407,i_class_id#408,i_class#409,i_category_id#410,i_category#411,i_manufact_id#412,i_manufact#413,i_size#414,i_formulation#415,i_color#416,i_units#417,i_container#418,i_manager_id#419,i_product_name#420] parquet
            :                    :     :     +- Project [cs_sold_date_sk#421, cs_call_center_sk#432, cs_item_sk#436, cs_sales_price#442]
            :                    :     :        +- Filter ((isnotnull(cs_item_sk#436) AND isnotnull(cs_sold_date_sk#421)) AND isnotnull(cs_call_center_sk#432))
            :                    :     :           +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#421,cs_sold_time_sk#422,cs_ship_date_sk#423,cs_bill_customer_sk#424,cs_bill_cdemo_sk#425,cs_bill_hdemo_sk#426,cs_bill_addr_sk#427,cs_ship_customer_sk#428,cs_ship_cdemo_sk#429,cs_ship_hdemo_sk#430,cs_ship_addr_sk#431,cs_call_center_sk#432,cs_catalog_page_sk#433,cs_ship_mode_sk#434,cs_warehouse_sk#435,cs_item_sk#436,cs_promo_sk#437,cs_order_number#438,cs_quantity#439,cs_wholesale_cost#440,cs_list_price#441,cs_sales_price#442,cs_ext_discount_amt#443,cs_ext_sales_price#444,... 10 more fields] parquet
            :                    :     +- Project [d_date_sk#455, d_year#461, d_moy#463]
            :                    :        +- Filter ((((d_year#461 = 1999) OR ((d_year#461 = 1998) AND (d_moy#463 = 12))) OR ((d_year#461 = 2000) AND (d_moy#463 = 1))) AND isnotnull(d_date_sk#455))
            :                    :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#455,d_date_id#456,d_date#457,d_month_seq#458,d_week_seq#459,d_quarter_seq#460,d_year#461,d_dow#462,d_moy#463,d_dom#464,d_qoy#465,d_fy_year#466,d_fy_quarter_seq#467,d_fy_week_seq#468,d_day_name#469,d_quarter_name#470,d_holiday#471,d_weekend#472,d_following_holiday#473,d_first_dom#474,d_last_dom#475,d_same_day_ly#476,d_same_day_lq#477,d_current_day#478,... 4 more fields] parquet
            :                    +- Project [cc_call_center_sk#483, cc_name#489]
            :                       +- Filter (isnotnull(cc_call_center_sk#483) AND isnotnull(cc_name#489))
            :                          +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#483,cc_call_center_id#484,cc_rec_start_date#485,cc_rec_end_date#486,cc_closed_date_sk#487,cc_open_date_sk#488,cc_name#489,cc_class#490,cc_employees#491,cc_sq_ft#492,cc_hours#493,cc_manager#494,cc_mkt_id#495,cc_mkt_class#496,cc_mkt_desc#497,cc_market_manager#498,cc_division#499,cc_division_name#500,cc_company#501,cc_company_name#502,cc_street_number#503,cc_street_name#504,cc_street_type#505,cc_suite_number#506,... 7 more fields] parquet
            +- Project [i_category#531, i_brand#527, cc_name#609, sum_sales#0 AS sum_sales#156, rn#638]
               +- Window [rank(d_year#581, d_moy#583) windowspecdefinition(i_category#531, i_brand#527, cc_name#609, d_year#581 ASC NULLS FIRST, d_moy#583 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#638], [i_category#531, i_brand#527, cc_name#609], [d_year#581 ASC NULLS FIRST, d_moy#583 ASC NULLS FIRST]
                  +- Aggregate [i_category#531, i_brand#527, cc_name#609, d_year#581, d_moy#583], [i_category#531, i_brand#527, cc_name#609, d_year#581, d_moy#583, sum(cs_sales_price#562) AS sum_sales#0]
                     +- Project [i_brand#527, i_category#531, cs_sales_price#562, d_year#581, d_moy#583, cc_name#609]
                        +- Join Inner, (cc_call_center_sk#603 = cs_call_center_sk#552)
                           :- Project [i_brand#527, i_category#531, cs_call_center_sk#552, cs_sales_price#562, d_year#581, d_moy#583]
                           :  +- Join Inner, (cs_sold_date_sk#541 = d_date_sk#575)
                           :     :- Project [i_brand#527, i_category#531, cs_sold_date_sk#541, cs_call_center_sk#552, cs_sales_price#562]
                           :     :  +- Join Inner, (cs_item_sk#556 = i_item_sk#519)
                           :     :     :- Project [i_item_sk#519, i_brand#527, i_category#531]
                           :     :     :  +- Filter (isnotnull(i_item_sk#519) AND (isnotnull(i_category#531) AND isnotnull(i_brand#527)))
                           :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#519,i_item_id#520,i_rec_start_date#521,i_rec_end_date#522,i_item_desc#523,i_current_price#524,i_wholesale_cost#525,i_brand_id#526,i_brand#527,i_class_id#528,i_class#529,i_category_id#530,i_category#531,i_manufact_id#532,i_manufact#533,i_size#534,i_formulation#535,i_color#536,i_units#537,i_container#538,i_manager_id#539,i_product_name#540] parquet
                           :     :     +- Project [cs_sold_date_sk#541, cs_call_center_sk#552, cs_item_sk#556, cs_sales_price#562]
                           :     :        +- Filter ((isnotnull(cs_item_sk#556) AND isnotnull(cs_sold_date_sk#541)) AND isnotnull(cs_call_center_sk#552))
                           :     :           +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#541,cs_sold_time_sk#542,cs_ship_date_sk#543,cs_bill_customer_sk#544,cs_bill_cdemo_sk#545,cs_bill_hdemo_sk#546,cs_bill_addr_sk#547,cs_ship_customer_sk#548,cs_ship_cdemo_sk#549,cs_ship_hdemo_sk#550,cs_ship_addr_sk#551,cs_call_center_sk#552,cs_catalog_page_sk#553,cs_ship_mode_sk#554,cs_warehouse_sk#555,cs_item_sk#556,cs_promo_sk#557,cs_order_number#558,cs_quantity#559,cs_wholesale_cost#560,cs_list_price#561,cs_sales_price#562,cs_ext_discount_amt#563,cs_ext_sales_price#564,... 10 more fields] parquet
                           :     +- Project [d_date_sk#575, d_year#581, d_moy#583]
                           :        +- Filter ((((d_year#581 = 1999) OR ((d_year#581 = 1998) AND (d_moy#583 = 12))) OR ((d_year#581 = 2000) AND (d_moy#583 = 1))) AND isnotnull(d_date_sk#575))
                           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#575,d_date_id#576,d_date#577,d_month_seq#578,d_week_seq#579,d_quarter_seq#580,d_year#581,d_dow#582,d_moy#583,d_dom#584,d_qoy#585,d_fy_year#586,d_fy_quarter_seq#587,d_fy_week_seq#588,d_day_name#589,d_quarter_name#590,d_holiday#591,d_weekend#592,d_following_holiday#593,d_first_dom#594,d_last_dom#595,d_same_day_ly#596,d_same_day_lq#597,d_current_day#598,... 4 more fields] parquet
                           +- Project [cc_call_center_sk#603, cc_name#609]
                              +- Filter (isnotnull(cc_call_center_sk#603) AND isnotnull(cc_name#609))
                                 +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#603,cc_call_center_id#604,cc_rec_start_date#605,cc_rec_end_date#606,cc_closed_date_sk#607,cc_open_date_sk#608,cc_name#609,cc_class#610,cc_employees#611,cc_sq_ft#612,cc_hours#613,cc_manager#614,cc_mkt_id#615,cc_mkt_class#616,cc_mkt_desc#617,cc_market_manager#618,cc_division#619,cc_division_name#620,cc_company#621,cc_company_name#622,cc_street_number#623,cc_street_name#624,cc_street_type#625,cc_suite_number#626,... 7 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST,cc_name#102 ASC NULLS FIRST], output=[i_category#24,i_brand#20,cc_name#102,d_year#74,d_moy#76,avg_monthly_sales#1,sum_sales#0,psum#3,nsum#4])
   +- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, avg_monthly_sales#1, sum_sales#0, sum_sales#148 AS psum#3, sum_sales#156 AS nsum#4]
      +- SortMergeJoin [i_category#24, i_brand#20, cc_name#102, rn#2], [i_category#531, i_brand#527, cc_name#609, (rn#638 - 1)], Inner
         :- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#148]
         :  +- SortMergeJoin [i_category#24, i_brand#20, cc_name#102, rn#2], [i_category#411, i_brand#407, cc_name#489, (rn#518 + 1)], Inner
         :     :- Sort [i_category#24 ASC NULLS FIRST, i_brand#20 ASC NULLS FIRST, cc_name#102 ASC NULLS FIRST, rn#2 ASC NULLS FIRST], false, 0
         :     :  +- Exchange hashpartitioning(i_category#24, i_brand#20, cc_name#102, rn#2, 200), ENSURE_REQUIREMENTS, [plan_id=333]
         :     :     +- Project [i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, avg_monthly_sales#1, rn#2]
         :     :        +- Filter ((isnotnull(avg_monthly_sales#1) AND (avg_monthly_sales#1 > 0.0)) AND CASE WHEN (avg_monthly_sales#1 > 0.0) THEN ((abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) > 0.1) END)
         :     :           +- Window [avg(_w0#142) windowspecdefinition(i_category#24, i_brand#20, cc_name#102, d_year#74, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#24, i_brand#20, cc_name#102, d_year#74]
         :     :              +- Filter (isnotnull(d_year#74) AND (d_year#74 = 1999))
         :     :                 +- Window [rank(d_year#74, d_moy#76) windowspecdefinition(i_category#24, i_brand#20, cc_name#102, d_year#74 ASC NULLS FIRST, d_moy#76 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#24, i_brand#20, cc_name#102], [d_year#74 ASC NULLS FIRST, d_moy#76 ASC NULLS FIRST]
         :     :                    +- Sort [i_category#24 ASC NULLS FIRST, i_brand#20 ASC NULLS FIRST, cc_name#102 ASC NULLS FIRST, d_year#74 ASC NULLS FIRST, d_moy#76 ASC NULLS FIRST], false, 0
         :     :                       +- Exchange hashpartitioning(i_category#24, i_brand#20, cc_name#102, 200), ENSURE_REQUIREMENTS, [plan_id=304]
         :     :                          +- HashAggregate(keys=[i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76], functions=[sum(cs_sales_price#55)], output=[i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum_sales#0, _w0#142])
         :     :                             +- Exchange hashpartitioning(i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, 200), ENSURE_REQUIREMENTS, [plan_id=301]
         :     :                                +- HashAggregate(keys=[i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76], functions=[partial_sum(cs_sales_price#55)], output=[i_category#24, i_brand#20, cc_name#102, d_year#74, d_moy#76, sum#640])
         :     :                                   +- Project [i_brand#20, i_category#24, cs_sales_price#55, d_year#74, d_moy#76, cc_name#102]
         :     :                                      +- BroadcastHashJoin [cs_call_center_sk#45], [cc_call_center_sk#96], Inner, BuildRight, false
         :     :                                         :- Project [i_brand#20, i_category#24, cs_call_center_sk#45, cs_sales_price#55, d_year#74, d_moy#76]
         :     :                                         :  +- BroadcastHashJoin [cs_sold_date_sk#34], [d_date_sk#68], Inner, BuildRight, false
         :     :                                         :     :- Project [i_brand#20, i_category#24, cs_sold_date_sk#34, cs_call_center_sk#45, cs_sales_price#55]
         :     :                                         :     :  +- BroadcastHashJoin [i_item_sk#12], [cs_item_sk#49], Inner, BuildLeft, false
         :     :                                         :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=288]
         :     :                                         :     :     :  +- Filter ((isnotnull(i_item_sk#12) AND isnotnull(i_category#24)) AND isnotnull(i_brand#20))
         :     :                                         :     :     :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#12,i_brand#20,i_category#24] Batched: true, DataFilters: [isnotnull(i_item_sk#12), isnotnull(i_category#24), isnotnull(i_brand#20)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_category), IsNotNull(i_brand)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
         :     :                                         :     :     +- Filter ((isnotnull(cs_item_sk#49) AND isnotnull(cs_sold_date_sk#34)) AND isnotnull(cs_call_center_sk#45))
         :     :                                         :     :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#34,cs_call_center_sk#45,cs_item_sk#49,cs_sales_price#55] Batched: true, DataFilters: [isnotnull(cs_item_sk#49), isnotnull(cs_sold_date_sk#34), isnotnull(cs_call_center_sk#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_call_center_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_call_center_sk:int,cs_item_sk:int,cs_sales_price:double>
         :     :                                         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=292]
         :     :                                         :        +- Filter ((((d_year#74 = 1999) OR ((d_year#74 = 1998) AND (d_moy#76 = 12))) OR ((d_year#74 = 2000) AND (d_moy#76 = 1))) AND isnotnull(d_date_sk#68))
         :     :                                         :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#68,d_year#74,d_moy#76] Batched: true, DataFilters: [(((d_year#74 = 1999) OR ((d_year#74 = 1998) AND (d_moy#76 = 12))) OR ((d_year#74 = 2000) AND (d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
         :     :                                         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=296]
         :     :                                            +- Filter (isnotnull(cc_call_center_sk#96) AND isnotnull(cc_name#102))
         :     :                                               +- FileScan parquet spark_catalog.tpcds.call_center[cc_call_center_sk#96,cc_name#102] Batched: true, DataFilters: [isnotnull(cc_call_center_sk#96), isnotnull(cc_name#102)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk), IsNotNull(cc_name)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>
         :     +- Sort [i_category#411 ASC NULLS FIRST, i_brand#407 ASC NULLS FIRST, cc_name#489 ASC NULLS FIRST, (rn#518 + 1) ASC NULLS FIRST], false, 0
         :        +- Exchange hashpartitioning(i_category#411, i_brand#407, cc_name#489, (rn#518 + 1), 200), ENSURE_REQUIREMENTS, [plan_id=334]
         :           +- Project [i_category#411, i_brand#407, cc_name#489, sum_sales#0 AS sum_sales#148, rn#518]
         :              +- Window [rank(d_year#461, d_moy#463) windowspecdefinition(i_category#411, i_brand#407, cc_name#489, d_year#461 ASC NULLS FIRST, d_moy#463 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#518], [i_category#411, i_brand#407, cc_name#489], [d_year#461 ASC NULLS FIRST, d_moy#463 ASC NULLS FIRST]
         :                 +- Sort [i_category#411 ASC NULLS FIRST, i_brand#407 ASC NULLS FIRST, cc_name#489 ASC NULLS FIRST, d_year#461 ASC NULLS FIRST, d_moy#463 ASC NULLS FIRST], false, 0
         :                    +- Exchange hashpartitioning(i_category#411, i_brand#407, cc_name#489, 200), ENSURE_REQUIREMENTS, [plan_id=327]
         :                       +- HashAggregate(keys=[i_category#411, i_brand#407, cc_name#489, d_year#461, d_moy#463], functions=[sum(cs_sales_price#442)], output=[i_category#411, i_brand#407, cc_name#489, d_year#461, d_moy#463, sum_sales#0])
         :                          +- Exchange hashpartitioning(i_category#411, i_brand#407, cc_name#489, d_year#461, d_moy#463, 200), ENSURE_REQUIREMENTS, [plan_id=324]
         :                             +- HashAggregate(keys=[i_category#411, i_brand#407, cc_name#489, d_year#461, d_moy#463], functions=[partial_sum(cs_sales_price#442)], output=[i_category#411, i_brand#407, cc_name#489, d_year#461, d_moy#463, sum#642])
         :                                +- Project [i_brand#407, i_category#411, cs_sales_price#442, d_year#461, d_moy#463, cc_name#489]
         :                                   +- BroadcastHashJoin [cs_call_center_sk#432], [cc_call_center_sk#483], Inner, BuildRight, false
         :                                      :- Project [i_brand#407, i_category#411, cs_call_center_sk#432, cs_sales_price#442, d_year#461, d_moy#463]
         :                                      :  +- BroadcastHashJoin [cs_sold_date_sk#421], [d_date_sk#455], Inner, BuildRight, false
         :                                      :     :- Project [i_brand#407, i_category#411, cs_sold_date_sk#421, cs_call_center_sk#432, cs_sales_price#442]
         :                                      :     :  +- BroadcastHashJoin [i_item_sk#399], [cs_item_sk#436], Inner, BuildLeft, false
         :                                      :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=311]
         :                                      :     :     :  +- Filter ((isnotnull(i_item_sk#399) AND isnotnull(i_category#411)) AND isnotnull(i_brand#407))
         :                                      :     :     :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#399,i_brand#407,i_category#411] Batched: true, DataFilters: [isnotnull(i_item_sk#399), isnotnull(i_category#411), isnotnull(i_brand#407)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_category), IsNotNull(i_brand)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
         :                                      :     :     +- Filter ((isnotnull(cs_item_sk#436) AND isnotnull(cs_sold_date_sk#421)) AND isnotnull(cs_call_center_sk#432))
         :                                      :     :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#421,cs_call_center_sk#432,cs_item_sk#436,cs_sales_price#442] Batched: true, DataFilters: [isnotnull(cs_item_sk#436), isnotnull(cs_sold_date_sk#421), isnotnull(cs_call_center_sk#432)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_call_center_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_call_center_sk:int,cs_item_sk:int,cs_sales_price:double>
         :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=315]
         :                                      :        +- Filter ((((d_year#461 = 1999) OR ((d_year#461 = 1998) AND (d_moy#463 = 12))) OR ((d_year#461 = 2000) AND (d_moy#463 = 1))) AND isnotnull(d_date_sk#455))
         :                                      :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#455,d_year#461,d_moy#463] Batched: true, DataFilters: [(((d_year#461 = 1999) OR ((d_year#461 = 1998) AND (d_moy#463 = 12))) OR ((d_year#461 = 2000) AND..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
         :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=319]
         :                                         +- Filter (isnotnull(cc_call_center_sk#483) AND isnotnull(cc_name#489))
         :                                            +- FileScan parquet spark_catalog.tpcds.call_center[cc_call_center_sk#483,cc_name#489] Batched: true, DataFilters: [isnotnull(cc_call_center_sk#483), isnotnull(cc_name#489)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk), IsNotNull(cc_name)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>
         +- Sort [i_category#531 ASC NULLS FIRST, i_brand#527 ASC NULLS FIRST, cc_name#609 ASC NULLS FIRST, (rn#638 - 1) ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(i_category#531, i_brand#527, cc_name#609, (rn#638 - 1), 200), ENSURE_REQUIREMENTS, [plan_id=361]
               +- Project [i_category#531, i_brand#527, cc_name#609, sum_sales#0 AS sum_sales#156, rn#638]
                  +- Window [rank(d_year#581, d_moy#583) windowspecdefinition(i_category#531, i_brand#527, cc_name#609, d_year#581 ASC NULLS FIRST, d_moy#583 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#638], [i_category#531, i_brand#527, cc_name#609], [d_year#581 ASC NULLS FIRST, d_moy#583 ASC NULLS FIRST]
                     +- Sort [i_category#531 ASC NULLS FIRST, i_brand#527 ASC NULLS FIRST, cc_name#609 ASC NULLS FIRST, d_year#581 ASC NULLS FIRST, d_moy#583 ASC NULLS FIRST], false, 0
                        +- Exchange hashpartitioning(i_category#531, i_brand#527, cc_name#609, 200), ENSURE_REQUIREMENTS, [plan_id=355]
                           +- HashAggregate(keys=[i_category#531, i_brand#527, cc_name#609, d_year#581, d_moy#583], functions=[sum(cs_sales_price#562)], output=[i_category#531, i_brand#527, cc_name#609, d_year#581, d_moy#583, sum_sales#0])
                              +- Exchange hashpartitioning(i_category#531, i_brand#527, cc_name#609, d_year#581, d_moy#583, 200), ENSURE_REQUIREMENTS, [plan_id=352]
                                 +- HashAggregate(keys=[i_category#531, i_brand#527, cc_name#609, d_year#581, d_moy#583], functions=[partial_sum(cs_sales_price#562)], output=[i_category#531, i_brand#527, cc_name#609, d_year#581, d_moy#583, sum#644])
                                    +- Project [i_brand#527, i_category#531, cs_sales_price#562, d_year#581, d_moy#583, cc_name#609]
                                       +- BroadcastHashJoin [cs_call_center_sk#552], [cc_call_center_sk#603], Inner, BuildRight, false
                                          :- Project [i_brand#527, i_category#531, cs_call_center_sk#552, cs_sales_price#562, d_year#581, d_moy#583]
                                          :  +- BroadcastHashJoin [cs_sold_date_sk#541], [d_date_sk#575], Inner, BuildRight, false
                                          :     :- Project [i_brand#527, i_category#531, cs_sold_date_sk#541, cs_call_center_sk#552, cs_sales_price#562]
                                          :     :  +- BroadcastHashJoin [i_item_sk#519], [cs_item_sk#556], Inner, BuildLeft, false
                                          :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=339]
                                          :     :     :  +- Filter ((isnotnull(i_item_sk#519) AND isnotnull(i_category#531)) AND isnotnull(i_brand#527))
                                          :     :     :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#519,i_brand#527,i_category#531] Batched: true, DataFilters: [isnotnull(i_item_sk#519), isnotnull(i_category#531), isnotnull(i_brand#527)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_category), IsNotNull(i_brand)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
                                          :     :     +- Filter ((isnotnull(cs_item_sk#556) AND isnotnull(cs_sold_date_sk#541)) AND isnotnull(cs_call_center_sk#552))
                                          :     :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#541,cs_call_center_sk#552,cs_item_sk#556,cs_sales_price#562] Batched: true, DataFilters: [isnotnull(cs_item_sk#556), isnotnull(cs_sold_date_sk#541), isnotnull(cs_call_center_sk#552)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_call_center_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_call_center_sk:int,cs_item_sk:int,cs_sales_price:double>
                                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=343]
                                          :        +- Filter ((((d_year#581 = 1999) OR ((d_year#581 = 1998) AND (d_moy#583 = 12))) OR ((d_year#581 = 2000) AND (d_moy#583 = 1))) AND isnotnull(d_date_sk#575))
                                          :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#575,d_year#581,d_moy#583] Batched: true, DataFilters: [(((d_year#581 = 1999) OR ((d_year#581 = 1998) AND (d_moy#583 = 12))) OR ((d_year#581 = 2000) AND..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=347]
                                             +- Filter (isnotnull(cc_call_center_sk#603) AND isnotnull(cc_name#609))
                                                +- FileScan parquet spark_catalog.tpcds.call_center[cc_call_center_sk#603,cc_name#609] Batched: true, DataFilters: [isnotnull(cc_call_center_sk#603), isnotnull(cc_name#609)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk), IsNotNull(cc_name)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>

Time taken: 3.371 seconds, Fetched 1 row(s)
