Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581368079
== Parsed Logical Plan ==
CTE [v1, v2]
:  :- 'SubqueryAlias v1
:  :  +- 'Aggregate ['i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year, 'd_moy], ['i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year, 'd_moy, 'sum('ss_sales_price) AS sum_sales#0, 'avg('sum('ss_sales_price)) windowspecdefinition('i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year, unspecifiedframe$()) AS avg_monthly_sales#1, 'rank() windowspecdefinition('i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year ASC NULLS FIRST, 'd_moy ASC NULLS FIRST, unspecifiedframe$()) AS rn#2]
:  :     +- 'Filter ((('ss_item_sk = 'i_item_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND (('ss_store_sk = 's_store_sk) AND ((('d_year = 1999) OR (('d_year = (1999 - 1)) AND ('d_moy = 12))) OR (('d_year = (1999 + 1)) AND ('d_moy = 1)))))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation [item], [], false
:  :           :  :  +- 'UnresolvedRelation [store_sales], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [store], [], false
:  +- 'SubqueryAlias v2
:     +- 'Project ['v1.i_category, 'v1.i_brand, 'v1.s_store_name, 'v1.s_company_name, 'v1.d_year, 'v1.d_moy, 'v1.avg_monthly_sales, 'v1.sum_sales, 'v1_lag.sum_sales AS psum#3, 'v1_lead.sum_sales AS nsum#4]
:        +- 'Filter ((((('v1.i_category = 'v1_lag.i_category) AND ('v1.i_category = 'v1_lead.i_category)) AND ('v1.i_brand = 'v1_lag.i_brand)) AND (('v1.i_brand = 'v1_lead.i_brand) AND ('v1.s_store_name = 'v1_lag.s_store_name))) AND (((('v1.s_store_name = 'v1_lead.s_store_name) AND ('v1.s_company_name = 'v1_lag.s_company_name)) AND ('v1.s_company_name = 'v1_lead.s_company_name)) AND (('v1.rn = ('v1_lag.rn + 1)) AND ('v1.rn = ('v1_lead.rn - 1)))))
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
i_category: string, i_brand: string, s_store_name: string, s_company_name: string, d_year: int, d_moy: int, avg_monthly_sales: double, sum_sales: double, psum: double, nsum: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias v1
:     +- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, avg_monthly_sales#1, rn#2]
:        +- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, _w0#129, avg_monthly_sales#1, rn#2, avg_monthly_sales#1, rn#2]
:           +- Window [rank(d_year#63, d_moy#65) windowspecdefinition(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63 ASC NULLS FIRST, d_moy#65 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102], [d_year#63 ASC NULLS FIRST, d_moy#65 ASC NULLS FIRST]
:              +- Window [avg(_w0#129) windowspecdefinition(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63]
:                 +- Aggregate [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum(ss_sales_price#47) AS sum_sales#0, sum(ss_sales_price#47) AS _w0#129]
:                    +- Filter (((ss_item_sk#36 = i_item_sk#12) AND (ss_sold_date_sk#34 = d_date_sk#57)) AND ((ss_store_sk#41 = s_store_sk#85) AND (((d_year#63 = 1999) OR ((d_year#63 = (1999 - 1)) AND (d_moy#65 = 12))) OR ((d_year#63 = (1999 + 1)) AND (d_moy#65 = 1)))))
:                       +- Join Inner
:                          :- Join Inner
:                          :  :- Join Inner
:                          :  :  :- SubqueryAlias spark_catalog.tpcds.item
:                          :  :  :  +- Relation spark_catalog.tpcds.item[i_item_sk#12,i_item_id#13,i_rec_start_date#14,i_rec_end_date#15,i_item_desc#16,i_current_price#17,i_wholesale_cost#18,i_brand_id#19,i_brand#20,i_class_id#21,i_class#22,i_category_id#23,i_category#24,i_manufact_id#25,i_manufact#26,i_size#27,i_formulation#28,i_color#29,i_units#30,i_container#31,i_manager_id#32,i_product_name#33] parquet
:                          :  :  +- SubqueryAlias spark_catalog.tpcds.store_sales
:                          :  :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#34,ss_sold_time_sk#35,ss_item_sk#36,ss_customer_sk#37,ss_cdemo_sk#38,ss_hdemo_sk#39,ss_addr_sk#40,ss_store_sk#41,ss_promo_sk#42,ss_ticket_number#43,ss_quantity#44,ss_wholesale_cost#45,ss_list_price#46,ss_sales_price#47,ss_ext_discount_amt#48,ss_ext_sales_price#49,ss_ext_wholesale_cost#50,ss_ext_list_price#51,ss_ext_tax#52,ss_coupon_amt#53,ss_net_paid#54,ss_net_paid_inc_tax#55,ss_net_profit#56] parquet
:                          :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:                          :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#57,d_date_id#58,d_date#59,d_month_seq#60,d_week_seq#61,d_quarter_seq#62,d_year#63,d_dow#64,d_moy#65,d_dom#66,d_qoy#67,d_fy_year#68,d_fy_quarter_seq#69,d_fy_week_seq#70,d_day_name#71,d_quarter_name#72,d_holiday#73,d_weekend#74,d_following_holiday#75,d_first_dom#76,d_last_dom#77,d_same_day_ly#78,d_same_day_lq#79,d_current_day#80,... 4 more fields] parquet
:                          +- SubqueryAlias spark_catalog.tpcds.store
:                             +- Relation spark_catalog.tpcds.store[s_store_sk#85,s_store_id#86,s_rec_start_date#87,s_rec_end_date#88,s_closed_date_sk#89,s_store_name#90,s_number_employees#91,s_floor_space#92,s_hours#93,s_manager#94,s_market_id#95,s_geography_class#96,s_market_desc#97,s_market_manager#98,s_division_id#99,s_division_name#100,s_company_id#101,s_company_name#102,s_street_number#103,s_street_name#104,s_street_type#105,s_suite_number#106,s_city#107,s_county#108,... 5 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias v2
:     +- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, avg_monthly_sales#1, sum_sales#0, sum_sales#136 AS psum#3, sum_sales#145 AS nsum#4]
:        +- Filter (((((i_category#24 = i_category#130) AND (i_category#24 = i_category#139)) AND (i_brand#20 = i_brand#131)) AND ((i_brand#20 = i_brand#140) AND (s_store_name#90 = s_store_name#132))) AND ((((s_store_name#90 = s_store_name#141) AND (s_company_name#102 = s_company_name#133)) AND (s_company_name#102 = s_company_name#142)) AND ((rn#2 = (rn#138 + 1)) AND (rn#2 = (rn#147 - 1)))))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias v1
:              :  :  +- CTERelationRef 0, true, [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, avg_monthly_sales#1, rn#2], false
:              :  +- SubqueryAlias v1_lag
:              :     +- SubqueryAlias v1
:              :        +- CTERelationRef 0, true, [i_category#130, i_brand#131, s_store_name#132, s_company_name#133, d_year#134, d_moy#135, sum_sales#136, avg_monthly_sales#137, rn#138], false
:              +- SubqueryAlias v1_lead
:                 +- SubqueryAlias v1
:                    +- CTERelationRef 0, true, [i_category#139, i_brand#140, s_store_name#141, s_company_name#142, d_year#143, d_moy#144, sum_sales#145, avg_monthly_sales#146, rn#147], false
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, s_store_name#90 ASC NULLS FIRST], true
         +- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, avg_monthly_sales#1, sum_sales#0, psum#3, nsum#4]
            +- Filter (((d_year#63 = 1999) AND (avg_monthly_sales#1 > cast(0 as double))) AND (CASE WHEN (avg_monthly_sales#1 > cast(0 as double)) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE cast(null as double) END > cast(0.1 as double)))
               +- SubqueryAlias v2
                  +- CTERelationRef 1, true, [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, avg_monthly_sales#1, sum_sales#0, psum#3, nsum#4], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, s_store_name#90 ASC NULLS FIRST], true
      +- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, avg_monthly_sales#1, sum_sales#0, sum_sales#136 AS psum#3, sum_sales#145 AS nsum#4]
         +- Join Inner, (((((i_category#24 = i_category#481) AND (i_brand#20 = i_brand#477)) AND (s_store_name#90 = s_store_name#547)) AND (s_company_name#102 = s_company_name#559)) AND (rn#2 = (rn#575 - 1)))
            :- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#136]
            :  +- Join Inner, (((((i_category#24 = i_category#374) AND (i_brand#20 = i_brand#370)) AND (s_store_name#90 = s_store_name#440)) AND (s_company_name#102 = s_company_name#452)) AND (rn#2 = (rn#468 + 1)))
            :     :- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, avg_monthly_sales#1, rn#2]
            :     :  +- Filter ((isnotnull(avg_monthly_sales#1) AND (avg_monthly_sales#1 > 0.0)) AND CASE WHEN (avg_monthly_sales#1 > 0.0) THEN ((abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) > 0.1) END)
            :     :     +- Window [avg(_w0#129) windowspecdefinition(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63]
            :     :        +- Filter (isnotnull(d_year#63) AND (d_year#63 = 1999))
            :     :           +- Window [rank(d_year#63, d_moy#65) windowspecdefinition(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63 ASC NULLS FIRST, d_moy#65 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102], [d_year#63 ASC NULLS FIRST, d_moy#65 ASC NULLS FIRST]
            :     :              +- Aggregate [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum(ss_sales_price#47) AS sum_sales#0, sum(ss_sales_price#47) AS _w0#129]
            :     :                 +- Project [i_brand#20, i_category#24, ss_sales_price#47, d_year#63, d_moy#65, s_store_name#90, s_company_name#102]
            :     :                    +- Join Inner, (ss_store_sk#41 = s_store_sk#85)
            :     :                       :- Project [i_brand#20, i_category#24, ss_store_sk#41, ss_sales_price#47, d_year#63, d_moy#65]
            :     :                       :  +- Join Inner, (ss_sold_date_sk#34 = d_date_sk#57)
            :     :                       :     :- Project [i_brand#20, i_category#24, ss_sold_date_sk#34, ss_store_sk#41, ss_sales_price#47]
            :     :                       :     :  +- Join Inner, (ss_item_sk#36 = i_item_sk#12)
            :     :                       :     :     :- Project [i_item_sk#12, i_brand#20, i_category#24]
            :     :                       :     :     :  +- Filter (isnotnull(i_item_sk#12) AND (isnotnull(i_category#24) AND isnotnull(i_brand#20)))
            :     :                       :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#12,i_item_id#13,i_rec_start_date#14,i_rec_end_date#15,i_item_desc#16,i_current_price#17,i_wholesale_cost#18,i_brand_id#19,i_brand#20,i_class_id#21,i_class#22,i_category_id#23,i_category#24,i_manufact_id#25,i_manufact#26,i_size#27,i_formulation#28,i_color#29,i_units#30,i_container#31,i_manager_id#32,i_product_name#33] parquet
            :     :                       :     :     +- Project [ss_sold_date_sk#34, ss_item_sk#36, ss_store_sk#41, ss_sales_price#47]
            :     :                       :     :        +- Filter (isnotnull(ss_item_sk#36) AND (isnotnull(ss_sold_date_sk#34) AND isnotnull(ss_store_sk#41)))
            :     :                       :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#34,ss_sold_time_sk#35,ss_item_sk#36,ss_customer_sk#37,ss_cdemo_sk#38,ss_hdemo_sk#39,ss_addr_sk#40,ss_store_sk#41,ss_promo_sk#42,ss_ticket_number#43,ss_quantity#44,ss_wholesale_cost#45,ss_list_price#46,ss_sales_price#47,ss_ext_discount_amt#48,ss_ext_sales_price#49,ss_ext_wholesale_cost#50,ss_ext_list_price#51,ss_ext_tax#52,ss_coupon_amt#53,ss_net_paid#54,ss_net_paid_inc_tax#55,ss_net_profit#56] parquet
            :     :                       :     +- Project [d_date_sk#57, d_year#63, d_moy#65]
            :     :                       :        +- Filter ((((d_year#63 = 1999) OR ((d_year#63 = 1998) AND (d_moy#65 = 12))) OR ((d_year#63 = 2000) AND (d_moy#65 = 1))) AND isnotnull(d_date_sk#57))
            :     :                       :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#57,d_date_id#58,d_date#59,d_month_seq#60,d_week_seq#61,d_quarter_seq#62,d_year#63,d_dow#64,d_moy#65,d_dom#66,d_qoy#67,d_fy_year#68,d_fy_quarter_seq#69,d_fy_week_seq#70,d_day_name#71,d_quarter_name#72,d_holiday#73,d_weekend#74,d_following_holiday#75,d_first_dom#76,d_last_dom#77,d_same_day_ly#78,d_same_day_lq#79,d_current_day#80,... 4 more fields] parquet
            :     :                       +- Project [s_store_sk#85, s_store_name#90, s_company_name#102]
            :     :                          +- Filter (isnotnull(s_store_sk#85) AND (isnotnull(s_store_name#90) AND isnotnull(s_company_name#102)))
            :     :                             +- Relation spark_catalog.tpcds.store[s_store_sk#85,s_store_id#86,s_rec_start_date#87,s_rec_end_date#88,s_closed_date_sk#89,s_store_name#90,s_number_employees#91,s_floor_space#92,s_hours#93,s_manager#94,s_market_id#95,s_geography_class#96,s_market_desc#97,s_market_manager#98,s_division_id#99,s_division_name#100,s_company_id#101,s_company_name#102,s_street_number#103,s_street_name#104,s_street_type#105,s_suite_number#106,s_city#107,s_county#108,... 5 more fields] parquet
            :     +- Project [i_category#374, i_brand#370, s_store_name#440, s_company_name#452, sum_sales#0 AS sum_sales#136, rn#468]
            :        +- Window [rank(d_year#413, d_moy#415) windowspecdefinition(i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413 ASC NULLS FIRST, d_moy#415 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#468], [i_category#374, i_brand#370, s_store_name#440, s_company_name#452], [d_year#413 ASC NULLS FIRST, d_moy#415 ASC NULLS FIRST]
            :           +- Aggregate [i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413, d_moy#415], [i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413, d_moy#415, sum(ss_sales_price#397) AS sum_sales#0]
            :              +- Project [i_brand#370, i_category#374, ss_sales_price#397, d_year#413, d_moy#415, s_store_name#440, s_company_name#452]
            :                 +- Join Inner, (ss_store_sk#391 = s_store_sk#435)
            :                    :- Project [i_brand#370, i_category#374, ss_store_sk#391, ss_sales_price#397, d_year#413, d_moy#415]
            :                    :  +- Join Inner, (ss_sold_date_sk#384 = d_date_sk#407)
            :                    :     :- Project [i_brand#370, i_category#374, ss_sold_date_sk#384, ss_store_sk#391, ss_sales_price#397]
            :                    :     :  +- Join Inner, (ss_item_sk#386 = i_item_sk#362)
            :                    :     :     :- Project [i_item_sk#362, i_brand#370, i_category#374]
            :                    :     :     :  +- Filter (isnotnull(i_item_sk#362) AND (isnotnull(i_category#374) AND isnotnull(i_brand#370)))
            :                    :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#362,i_item_id#363,i_rec_start_date#364,i_rec_end_date#365,i_item_desc#366,i_current_price#367,i_wholesale_cost#368,i_brand_id#369,i_brand#370,i_class_id#371,i_class#372,i_category_id#373,i_category#374,i_manufact_id#375,i_manufact#376,i_size#377,i_formulation#378,i_color#379,i_units#380,i_container#381,i_manager_id#382,i_product_name#383] parquet
            :                    :     :     +- Project [ss_sold_date_sk#384, ss_item_sk#386, ss_store_sk#391, ss_sales_price#397]
            :                    :     :        +- Filter ((isnotnull(ss_item_sk#386) AND isnotnull(ss_sold_date_sk#384)) AND isnotnull(ss_store_sk#391))
            :                    :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#384,ss_sold_time_sk#385,ss_item_sk#386,ss_customer_sk#387,ss_cdemo_sk#388,ss_hdemo_sk#389,ss_addr_sk#390,ss_store_sk#391,ss_promo_sk#392,ss_ticket_number#393,ss_quantity#394,ss_wholesale_cost#395,ss_list_price#396,ss_sales_price#397,ss_ext_discount_amt#398,ss_ext_sales_price#399,ss_ext_wholesale_cost#400,ss_ext_list_price#401,ss_ext_tax#402,ss_coupon_amt#403,ss_net_paid#404,ss_net_paid_inc_tax#405,ss_net_profit#406] parquet
            :                    :     +- Project [d_date_sk#407, d_year#413, d_moy#415]
            :                    :        +- Filter ((((d_year#413 = 1999) OR ((d_year#413 = 1998) AND (d_moy#415 = 12))) OR ((d_year#413 = 2000) AND (d_moy#415 = 1))) AND isnotnull(d_date_sk#407))
            :                    :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#407,d_date_id#408,d_date#409,d_month_seq#410,d_week_seq#411,d_quarter_seq#412,d_year#413,d_dow#414,d_moy#415,d_dom#416,d_qoy#417,d_fy_year#418,d_fy_quarter_seq#419,d_fy_week_seq#420,d_day_name#421,d_quarter_name#422,d_holiday#423,d_weekend#424,d_following_holiday#425,d_first_dom#426,d_last_dom#427,d_same_day_ly#428,d_same_day_lq#429,d_current_day#430,... 4 more fields] parquet
            :                    +- Project [s_store_sk#435, s_store_name#440, s_company_name#452]
            :                       +- Filter (isnotnull(s_store_sk#435) AND (isnotnull(s_store_name#440) AND isnotnull(s_company_name#452)))
            :                          +- Relation spark_catalog.tpcds.store[s_store_sk#435,s_store_id#436,s_rec_start_date#437,s_rec_end_date#438,s_closed_date_sk#439,s_store_name#440,s_number_employees#441,s_floor_space#442,s_hours#443,s_manager#444,s_market_id#445,s_geography_class#446,s_market_desc#447,s_market_manager#448,s_division_id#449,s_division_name#450,s_company_id#451,s_company_name#452,s_street_number#453,s_street_name#454,s_street_type#455,s_suite_number#456,s_city#457,s_county#458,... 5 more fields] parquet
            +- Project [i_category#481, i_brand#477, s_store_name#547, s_company_name#559, sum_sales#0 AS sum_sales#145, rn#575]
               +- Window [rank(d_year#520, d_moy#522) windowspecdefinition(i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520 ASC NULLS FIRST, d_moy#522 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#575], [i_category#481, i_brand#477, s_store_name#547, s_company_name#559], [d_year#520 ASC NULLS FIRST, d_moy#522 ASC NULLS FIRST]
                  +- Aggregate [i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520, d_moy#522], [i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520, d_moy#522, sum(ss_sales_price#504) AS sum_sales#0]
                     +- Project [i_brand#477, i_category#481, ss_sales_price#504, d_year#520, d_moy#522, s_store_name#547, s_company_name#559]
                        +- Join Inner, (ss_store_sk#498 = s_store_sk#542)
                           :- Project [i_brand#477, i_category#481, ss_store_sk#498, ss_sales_price#504, d_year#520, d_moy#522]
                           :  +- Join Inner, (ss_sold_date_sk#491 = d_date_sk#514)
                           :     :- Project [i_brand#477, i_category#481, ss_sold_date_sk#491, ss_store_sk#498, ss_sales_price#504]
                           :     :  +- Join Inner, (ss_item_sk#493 = i_item_sk#469)
                           :     :     :- Project [i_item_sk#469, i_brand#477, i_category#481]
                           :     :     :  +- Filter (isnotnull(i_item_sk#469) AND (isnotnull(i_category#481) AND isnotnull(i_brand#477)))
                           :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#469,i_item_id#470,i_rec_start_date#471,i_rec_end_date#472,i_item_desc#473,i_current_price#474,i_wholesale_cost#475,i_brand_id#476,i_brand#477,i_class_id#478,i_class#479,i_category_id#480,i_category#481,i_manufact_id#482,i_manufact#483,i_size#484,i_formulation#485,i_color#486,i_units#487,i_container#488,i_manager_id#489,i_product_name#490] parquet
                           :     :     +- Project [ss_sold_date_sk#491, ss_item_sk#493, ss_store_sk#498, ss_sales_price#504]
                           :     :        +- Filter ((isnotnull(ss_item_sk#493) AND isnotnull(ss_sold_date_sk#491)) AND isnotnull(ss_store_sk#498))
                           :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#491,ss_sold_time_sk#492,ss_item_sk#493,ss_customer_sk#494,ss_cdemo_sk#495,ss_hdemo_sk#496,ss_addr_sk#497,ss_store_sk#498,ss_promo_sk#499,ss_ticket_number#500,ss_quantity#501,ss_wholesale_cost#502,ss_list_price#503,ss_sales_price#504,ss_ext_discount_amt#505,ss_ext_sales_price#506,ss_ext_wholesale_cost#507,ss_ext_list_price#508,ss_ext_tax#509,ss_coupon_amt#510,ss_net_paid#511,ss_net_paid_inc_tax#512,ss_net_profit#513] parquet
                           :     +- Project [d_date_sk#514, d_year#520, d_moy#522]
                           :        +- Filter ((((d_year#520 = 1999) OR ((d_year#520 = 1998) AND (d_moy#522 = 12))) OR ((d_year#520 = 2000) AND (d_moy#522 = 1))) AND isnotnull(d_date_sk#514))
                           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#514,d_date_id#515,d_date#516,d_month_seq#517,d_week_seq#518,d_quarter_seq#519,d_year#520,d_dow#521,d_moy#522,d_dom#523,d_qoy#524,d_fy_year#525,d_fy_quarter_seq#526,d_fy_week_seq#527,d_day_name#528,d_quarter_name#529,d_holiday#530,d_weekend#531,d_following_holiday#532,d_first_dom#533,d_last_dom#534,d_same_day_ly#535,d_same_day_lq#536,d_current_day#537,... 4 more fields] parquet
                           +- Project [s_store_sk#542, s_store_name#547, s_company_name#559]
                              +- Filter (isnotnull(s_store_sk#542) AND (isnotnull(s_store_name#547) AND isnotnull(s_company_name#559)))
                                 +- Relation spark_catalog.tpcds.store[s_store_sk#542,s_store_id#543,s_rec_start_date#544,s_rec_end_date#545,s_closed_date_sk#546,s_store_name#547,s_number_employees#548,s_floor_space#549,s_hours#550,s_manager#551,s_market_id#552,s_geography_class#553,s_market_desc#554,s_market_manager#555,s_division_id#556,s_division_name#557,s_company_id#558,s_company_name#559,s_street_number#560,s_street_name#561,s_street_type#562,s_suite_number#563,s_city#564,s_county#565,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST,s_store_name#90 ASC NULLS FIRST], output=[i_category#24,i_brand#20,s_store_name#90,s_company_name#102,d_year#63,d_moy#65,avg_monthly_sales#1,sum_sales#0,psum#3,nsum#4])
   +- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, avg_monthly_sales#1, sum_sales#0, sum_sales#136 AS psum#3, sum_sales#145 AS nsum#4]
      +- SortMergeJoin [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, rn#2], [i_category#481, i_brand#477, s_store_name#547, s_company_name#559, (rn#575 - 1)], Inner
         :- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#136]
         :  +- SortMergeJoin [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, rn#2], [i_category#374, i_brand#370, s_store_name#440, s_company_name#452, (rn#468 + 1)], Inner
         :     :- Sort [i_category#24 ASC NULLS FIRST, i_brand#20 ASC NULLS FIRST, s_store_name#90 ASC NULLS FIRST, s_company_name#102 ASC NULLS FIRST, rn#2 ASC NULLS FIRST], false, 0
         :     :  +- Exchange hashpartitioning(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, rn#2, 200), ENSURE_REQUIREMENTS, [plan_id=333]
         :     :     +- Project [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, avg_monthly_sales#1, rn#2]
         :     :        +- Filter ((isnotnull(avg_monthly_sales#1) AND (avg_monthly_sales#1 > 0.0)) AND CASE WHEN (avg_monthly_sales#1 > 0.0) THEN ((abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) > 0.1) END)
         :     :           +- Window [avg(_w0#129) windowspecdefinition(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63]
         :     :              +- Filter (isnotnull(d_year#63) AND (d_year#63 = 1999))
         :     :                 +- Window [rank(d_year#63, d_moy#65) windowspecdefinition(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63 ASC NULLS FIRST, d_moy#65 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#24, i_brand#20, s_store_name#90, s_company_name#102], [d_year#63 ASC NULLS FIRST, d_moy#65 ASC NULLS FIRST]
         :     :                    +- Sort [i_category#24 ASC NULLS FIRST, i_brand#20 ASC NULLS FIRST, s_store_name#90 ASC NULLS FIRST, s_company_name#102 ASC NULLS FIRST, d_year#63 ASC NULLS FIRST, d_moy#65 ASC NULLS FIRST], false, 0
         :     :                       +- Exchange hashpartitioning(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, 200), ENSURE_REQUIREMENTS, [plan_id=304]
         :     :                          +- HashAggregate(keys=[i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65], functions=[sum(ss_sales_price#47)], output=[i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum_sales#0, _w0#129])
         :     :                             +- Exchange hashpartitioning(i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, 200), ENSURE_REQUIREMENTS, [plan_id=301]
         :     :                                +- HashAggregate(keys=[i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65], functions=[partial_sum(ss_sales_price#47)], output=[i_category#24, i_brand#20, s_store_name#90, s_company_name#102, d_year#63, d_moy#65, sum#577])
         :     :                                   +- Project [i_brand#20, i_category#24, ss_sales_price#47, d_year#63, d_moy#65, s_store_name#90, s_company_name#102]
         :     :                                      +- BroadcastHashJoin [ss_store_sk#41], [s_store_sk#85], Inner, BuildRight, false
         :     :                                         :- Project [i_brand#20, i_category#24, ss_store_sk#41, ss_sales_price#47, d_year#63, d_moy#65]
         :     :                                         :  +- BroadcastHashJoin [ss_sold_date_sk#34], [d_date_sk#57], Inner, BuildRight, false
         :     :                                         :     :- Project [i_brand#20, i_category#24, ss_sold_date_sk#34, ss_store_sk#41, ss_sales_price#47]
         :     :                                         :     :  +- BroadcastHashJoin [i_item_sk#12], [ss_item_sk#36], Inner, BuildLeft, false
         :     :                                         :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=288]
         :     :                                         :     :     :  +- Filter ((isnotnull(i_item_sk#12) AND isnotnull(i_category#24)) AND isnotnull(i_brand#20))
         :     :                                         :     :     :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#12,i_brand#20,i_category#24] Batched: true, DataFilters: [isnotnull(i_item_sk#12), isnotnull(i_category#24), isnotnull(i_brand#20)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_category), IsNotNull(i_brand)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
         :     :                                         :     :     +- Filter ((isnotnull(ss_item_sk#36) AND isnotnull(ss_sold_date_sk#34)) AND isnotnull(ss_store_sk#41))
         :     :                                         :     :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#34,ss_item_sk#36,ss_store_sk#41,ss_sales_price#47] Batched: true, DataFilters: [isnotnull(ss_item_sk#36), isnotnull(ss_sold_date_sk#34), isnotnull(ss_store_sk#41)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
         :     :                                         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=292]
         :     :                                         :        +- Filter ((((d_year#63 = 1999) OR ((d_year#63 = 1998) AND (d_moy#65 = 12))) OR ((d_year#63 = 2000) AND (d_moy#65 = 1))) AND isnotnull(d_date_sk#57))
         :     :                                         :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#57,d_year#63,d_moy#65] Batched: true, DataFilters: [(((d_year#63 = 1999) OR ((d_year#63 = 1998) AND (d_moy#65 = 12))) OR ((d_year#63 = 2000) AND (d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
         :     :                                         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=296]
         :     :                                            +- Filter ((isnotnull(s_store_sk#85) AND isnotnull(s_store_name#90)) AND isnotnull(s_company_name#102))
         :     :                                               +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#85,s_store_name#90,s_company_name#102] Batched: true, DataFilters: [isnotnull(s_store_sk#85), isnotnull(s_store_name#90), isnotnull(s_company_name#102)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_store_name), IsNotNull(s_company_name)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_company_name:string>
         :     +- Sort [i_category#374 ASC NULLS FIRST, i_brand#370 ASC NULLS FIRST, s_store_name#440 ASC NULLS FIRST, s_company_name#452 ASC NULLS FIRST, (rn#468 + 1) ASC NULLS FIRST], false, 0
         :        +- Exchange hashpartitioning(i_category#374, i_brand#370, s_store_name#440, s_company_name#452, (rn#468 + 1), 200), ENSURE_REQUIREMENTS, [plan_id=334]
         :           +- Project [i_category#374, i_brand#370, s_store_name#440, s_company_name#452, sum_sales#0 AS sum_sales#136, rn#468]
         :              +- Window [rank(d_year#413, d_moy#415) windowspecdefinition(i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413 ASC NULLS FIRST, d_moy#415 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#468], [i_category#374, i_brand#370, s_store_name#440, s_company_name#452], [d_year#413 ASC NULLS FIRST, d_moy#415 ASC NULLS FIRST]
         :                 +- Sort [i_category#374 ASC NULLS FIRST, i_brand#370 ASC NULLS FIRST, s_store_name#440 ASC NULLS FIRST, s_company_name#452 ASC NULLS FIRST, d_year#413 ASC NULLS FIRST, d_moy#415 ASC NULLS FIRST], false, 0
         :                    +- Exchange hashpartitioning(i_category#374, i_brand#370, s_store_name#440, s_company_name#452, 200), ENSURE_REQUIREMENTS, [plan_id=327]
         :                       +- HashAggregate(keys=[i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413, d_moy#415], functions=[sum(ss_sales_price#397)], output=[i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413, d_moy#415, sum_sales#0])
         :                          +- Exchange hashpartitioning(i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413, d_moy#415, 200), ENSURE_REQUIREMENTS, [plan_id=324]
         :                             +- HashAggregate(keys=[i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413, d_moy#415], functions=[partial_sum(ss_sales_price#397)], output=[i_category#374, i_brand#370, s_store_name#440, s_company_name#452, d_year#413, d_moy#415, sum#579])
         :                                +- Project [i_brand#370, i_category#374, ss_sales_price#397, d_year#413, d_moy#415, s_store_name#440, s_company_name#452]
         :                                   +- BroadcastHashJoin [ss_store_sk#391], [s_store_sk#435], Inner, BuildRight, false
         :                                      :- Project [i_brand#370, i_category#374, ss_store_sk#391, ss_sales_price#397, d_year#413, d_moy#415]
         :                                      :  +- BroadcastHashJoin [ss_sold_date_sk#384], [d_date_sk#407], Inner, BuildRight, false
         :                                      :     :- Project [i_brand#370, i_category#374, ss_sold_date_sk#384, ss_store_sk#391, ss_sales_price#397]
         :                                      :     :  +- BroadcastHashJoin [i_item_sk#362], [ss_item_sk#386], Inner, BuildLeft, false
         :                                      :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=311]
         :                                      :     :     :  +- Filter ((isnotnull(i_item_sk#362) AND isnotnull(i_category#374)) AND isnotnull(i_brand#370))
         :                                      :     :     :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#362,i_brand#370,i_category#374] Batched: true, DataFilters: [isnotnull(i_item_sk#362), isnotnull(i_category#374), isnotnull(i_brand#370)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_category), IsNotNull(i_brand)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
         :                                      :     :     +- Filter ((isnotnull(ss_item_sk#386) AND isnotnull(ss_sold_date_sk#384)) AND isnotnull(ss_store_sk#391))
         :                                      :     :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#384,ss_item_sk#386,ss_store_sk#391,ss_sales_price#397] Batched: true, DataFilters: [isnotnull(ss_item_sk#386), isnotnull(ss_sold_date_sk#384), isnotnull(ss_store_sk#391)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
         :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=315]
         :                                      :        +- Filter ((((d_year#413 = 1999) OR ((d_year#413 = 1998) AND (d_moy#415 = 12))) OR ((d_year#413 = 2000) AND (d_moy#415 = 1))) AND isnotnull(d_date_sk#407))
         :                                      :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#407,d_year#413,d_moy#415] Batched: true, DataFilters: [(((d_year#413 = 1999) OR ((d_year#413 = 1998) AND (d_moy#415 = 12))) OR ((d_year#413 = 2000) AND..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
         :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=319]
         :                                         +- Filter ((isnotnull(s_store_sk#435) AND isnotnull(s_store_name#440)) AND isnotnull(s_company_name#452))
         :                                            +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#435,s_store_name#440,s_company_name#452] Batched: true, DataFilters: [isnotnull(s_store_sk#435), isnotnull(s_store_name#440), isnotnull(s_company_name#452)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_store_name), IsNotNull(s_company_name)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_company_name:string>
         +- Sort [i_category#481 ASC NULLS FIRST, i_brand#477 ASC NULLS FIRST, s_store_name#547 ASC NULLS FIRST, s_company_name#559 ASC NULLS FIRST, (rn#575 - 1) ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(i_category#481, i_brand#477, s_store_name#547, s_company_name#559, (rn#575 - 1), 200), ENSURE_REQUIREMENTS, [plan_id=361]
               +- Project [i_category#481, i_brand#477, s_store_name#547, s_company_name#559, sum_sales#0 AS sum_sales#145, rn#575]
                  +- Window [rank(d_year#520, d_moy#522) windowspecdefinition(i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520 ASC NULLS FIRST, d_moy#522 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#575], [i_category#481, i_brand#477, s_store_name#547, s_company_name#559], [d_year#520 ASC NULLS FIRST, d_moy#522 ASC NULLS FIRST]
                     +- Sort [i_category#481 ASC NULLS FIRST, i_brand#477 ASC NULLS FIRST, s_store_name#547 ASC NULLS FIRST, s_company_name#559 ASC NULLS FIRST, d_year#520 ASC NULLS FIRST, d_moy#522 ASC NULLS FIRST], false, 0
                        +- Exchange hashpartitioning(i_category#481, i_brand#477, s_store_name#547, s_company_name#559, 200), ENSURE_REQUIREMENTS, [plan_id=355]
                           +- HashAggregate(keys=[i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520, d_moy#522], functions=[sum(ss_sales_price#504)], output=[i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520, d_moy#522, sum_sales#0])
                              +- Exchange hashpartitioning(i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520, d_moy#522, 200), ENSURE_REQUIREMENTS, [plan_id=352]
                                 +- HashAggregate(keys=[i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520, d_moy#522], functions=[partial_sum(ss_sales_price#504)], output=[i_category#481, i_brand#477, s_store_name#547, s_company_name#559, d_year#520, d_moy#522, sum#581])
                                    +- Project [i_brand#477, i_category#481, ss_sales_price#504, d_year#520, d_moy#522, s_store_name#547, s_company_name#559]
                                       +- BroadcastHashJoin [ss_store_sk#498], [s_store_sk#542], Inner, BuildRight, false
                                          :- Project [i_brand#477, i_category#481, ss_store_sk#498, ss_sales_price#504, d_year#520, d_moy#522]
                                          :  +- BroadcastHashJoin [ss_sold_date_sk#491], [d_date_sk#514], Inner, BuildRight, false
                                          :     :- Project [i_brand#477, i_category#481, ss_sold_date_sk#491, ss_store_sk#498, ss_sales_price#504]
                                          :     :  +- BroadcastHashJoin [i_item_sk#469], [ss_item_sk#493], Inner, BuildLeft, false
                                          :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=339]
                                          :     :     :  +- Filter ((isnotnull(i_item_sk#469) AND isnotnull(i_category#481)) AND isnotnull(i_brand#477))
                                          :     :     :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#469,i_brand#477,i_category#481] Batched: true, DataFilters: [isnotnull(i_item_sk#469), isnotnull(i_category#481), isnotnull(i_brand#477)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_category), IsNotNull(i_brand)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
                                          :     :     +- Filter ((isnotnull(ss_item_sk#493) AND isnotnull(ss_sold_date_sk#491)) AND isnotnull(ss_store_sk#498))
                                          :     :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#491,ss_item_sk#493,ss_store_sk#498,ss_sales_price#504] Batched: true, DataFilters: [isnotnull(ss_item_sk#493), isnotnull(ss_sold_date_sk#491), isnotnull(ss_store_sk#498)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
                                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=343]
                                          :        +- Filter ((((d_year#520 = 1999) OR ((d_year#520 = 1998) AND (d_moy#522 = 12))) OR ((d_year#520 = 2000) AND (d_moy#522 = 1))) AND isnotnull(d_date_sk#514))
                                          :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#514,d_year#520,d_moy#522] Batched: true, DataFilters: [(((d_year#520 = 1999) OR ((d_year#520 = 1998) AND (d_moy#522 = 12))) OR ((d_year#520 = 2000) AND..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=347]
                                             +- Filter ((isnotnull(s_store_sk#542) AND isnotnull(s_store_name#547)) AND isnotnull(s_company_name#559))
                                                +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#542,s_store_name#547,s_company_name#559] Batched: true, DataFilters: [isnotnull(s_store_sk#542), isnotnull(s_store_name#547), isnotnull(s_company_name#559)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_store_name), IsNotNull(s_company_name)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_company_name:string>

Time taken: 3.537 seconds, Fetched 1 row(s)
