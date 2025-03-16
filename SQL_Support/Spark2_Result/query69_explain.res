== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['cd_gender ASC NULLS FIRST, 'cd_marital_status ASC NULLS FIRST, 'cd_education_status ASC NULLS FIRST, 'cd_purchase_estimate ASC NULLS FIRST, 'cd_credit_rating ASC NULLS FIRST], true
      +- 'Aggregate ['cd_gender, 'cd_marital_status, 'cd_education_status, 'cd_purchase_estimate, 'cd_credit_rating], ['cd_gender, 'cd_marital_status, 'cd_education_status, 'count(1) AS cnt1#0, 'cd_purchase_estimate, 'count(1) AS cnt2#1, 'cd_credit_rating, 'count(1) AS cnt3#2]
         +- 'Filter (((('c.c_current_addr_sk = 'ca.ca_address_sk) && 'ca_state IN (KY,GA,NM)) && ('cd_demo_sk = 'c.c_current_cdemo_sk)) && (exists#3 [] && (NOT exists#4 [] && NOT exists#5 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ss_customer_sk) && ('ss_sold_date_sk = 'd_date_sk)) && (('d_year = 2001) && (('d_moy >= 4) && ('d_moy <= (4 + 2)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation `store_sales`
            :  :        +- 'UnresolvedRelation `date_dim`
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ws_bill_customer_sk) && ('ws_sold_date_sk = 'd_date_sk)) && (('d_year = 2001) && (('d_moy >= 4) && ('d_moy <= (4 + 2)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation `web_sales`
            :  :        +- 'UnresolvedRelation `date_dim`
            :  +- 'Project [*]
            :     +- 'Filter ((('c.c_customer_sk = 'cs_ship_customer_sk) && ('cs_sold_date_sk = 'd_date_sk)) && (('d_year = 2001) && (('d_moy >= 4) && ('d_moy <= (4 + 2)))))
            :        +- 'Join Inner
            :           :- 'UnresolvedRelation `catalog_sales`
            :           +- 'UnresolvedRelation `date_dim`
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias `c`
               :  :  +- 'UnresolvedRelation `customer`
               :  +- 'SubqueryAlias `ca`
               :     +- 'UnresolvedRelation `customer_address`
               +- 'UnresolvedRelation `customer_demographics`

== Analyzed Logical Plan ==
cd_gender: string, cd_marital_status: string, cd_education_status: string, cnt1: bigint, cd_purchase_estimate: int, cnt2: bigint, cd_credit_rating: string, cnt3: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#43 ASC NULLS FIRST, cd_marital_status#44 ASC NULLS FIRST, cd_education_status#45 ASC NULLS FIRST, cd_purchase_estimate#46 ASC NULLS FIRST, cd_credit_rating#47 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47], [cd_gender#43, cd_marital_status#44, cd_education_status#45, count(1) AS cnt1#0L, cd_purchase_estimate#46, count(1) AS cnt2#1L, cd_credit_rating#47, count(1) AS cnt3#2L]
         +- Filter ((((c_current_addr_sk#15 = ca_address_sk#29) && ca_state#37 IN (KY,GA,NM)) && (cd_demo_sk#42 = c_current_cdemo_sk#13)) && (exists#3 [c_customer_sk#11] && (NOT exists#4 [c_customer_sk#11] && NOT exists#5 [c_customer_sk#11])))
            :  :- Project [ss_sold_date_sk#51, ss_sold_time_sk#52, ss_item_sk#53, ss_customer_sk#54, ss_cdemo_sk#55, ss_hdemo_sk#56, ss_addr_sk#57, ss_store_sk#58, ss_promo_sk#59, ss_ticket_number#60, ss_quantity#61, ss_wholesale_cost#62, ss_list_price#63, ss_sales_price#64, ss_ext_discount_amt#65, ss_ext_sales_price#66, ss_ext_wholesale_cost#67, ss_ext_list_price#68, ss_ext_tax#69, ss_coupon_amt#70, ss_net_paid#71, ss_net_paid_inc_tax#72, ss_net_profit#73, d_date_sk#74, ... 27 more fields]
            :  :  +- Filter (((outer(c_customer_sk#11) = ss_customer_sk#54) && (ss_sold_date_sk#51 = d_date_sk#74)) && ((d_year#80 = 2001) && ((d_moy#82 >= 4) && (d_moy#82 <= (4 + 2)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias `tpcds`.`store_sales`
            :  :        :  +- Relation[ss_sold_date_sk#51,ss_sold_time_sk#52,ss_item_sk#53,ss_customer_sk#54,ss_cdemo_sk#55,ss_hdemo_sk#56,ss_addr_sk#57,ss_store_sk#58,ss_promo_sk#59,ss_ticket_number#60,ss_quantity#61,ss_wholesale_cost#62,ss_list_price#63,ss_sales_price#64,ss_ext_discount_amt#65,ss_ext_sales_price#66,ss_ext_wholesale_cost#67,ss_ext_list_price#68,ss_ext_tax#69,ss_coupon_amt#70,ss_net_paid#71,ss_net_paid_inc_tax#72,ss_net_profit#73] parquet
            :  :        +- SubqueryAlias `tpcds`.`date_dim`
            :  :           +- Relation[d_date_sk#74,d_date_id#75,d_date#76,d_month_seq#77,d_week_seq#78,d_quarter_seq#79,d_year#80,d_dow#81,d_moy#82,d_dom#83,d_qoy#84,d_fy_year#85,d_fy_quarter_seq#86,d_fy_week_seq#87,d_day_name#88,d_quarter_name#89,d_holiday#90,d_weekend#91,d_following_holiday#92,d_first_dom#93,d_last_dom#94,d_same_day_ly#95,d_same_day_lq#96,d_current_day#97,... 4 more fields] parquet
            :  :- Project [ws_sold_date_sk#102, ws_sold_time_sk#103, ws_ship_date_sk#104, ws_item_sk#105, ws_bill_customer_sk#106, ws_bill_cdemo_sk#107, ws_bill_hdemo_sk#108, ws_bill_addr_sk#109, ws_ship_customer_sk#110, ws_ship_cdemo_sk#111, ws_ship_hdemo_sk#112, ws_ship_addr_sk#113, ws_web_page_sk#114, ws_web_site_sk#115, ws_ship_mode_sk#116, ws_warehouse_sk#117, ws_promo_sk#118, ws_order_number#119, ws_quantity#120, ws_wholesale_cost#121, ws_list_price#122, ws_sales_price#123, ws_ext_discount_amt#124, ws_ext_sales_price#125, ... 38 more fields]
            :  :  +- Filter (((outer(c_customer_sk#11) = ws_bill_customer_sk#106) && (ws_sold_date_sk#102 = d_date_sk#74)) && ((d_year#80 = 2001) && ((d_moy#82 >= 4) && (d_moy#82 <= (4 + 2)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias `tpcds`.`web_sales`
            :  :        :  +- Relation[ws_sold_date_sk#102,ws_sold_time_sk#103,ws_ship_date_sk#104,ws_item_sk#105,ws_bill_customer_sk#106,ws_bill_cdemo_sk#107,ws_bill_hdemo_sk#108,ws_bill_addr_sk#109,ws_ship_customer_sk#110,ws_ship_cdemo_sk#111,ws_ship_hdemo_sk#112,ws_ship_addr_sk#113,ws_web_page_sk#114,ws_web_site_sk#115,ws_ship_mode_sk#116,ws_warehouse_sk#117,ws_promo_sk#118,ws_order_number#119,ws_quantity#120,ws_wholesale_cost#121,ws_list_price#122,ws_sales_price#123,ws_ext_discount_amt#124,ws_ext_sales_price#125,... 10 more fields] parquet
            :  :        +- SubqueryAlias `tpcds`.`date_dim`
            :  :           +- Relation[d_date_sk#74,d_date_id#75,d_date#76,d_month_seq#77,d_week_seq#78,d_quarter_seq#79,d_year#80,d_dow#81,d_moy#82,d_dom#83,d_qoy#84,d_fy_year#85,d_fy_quarter_seq#86,d_fy_week_seq#87,d_day_name#88,d_quarter_name#89,d_holiday#90,d_weekend#91,d_following_holiday#92,d_first_dom#93,d_last_dom#94,d_same_day_ly#95,d_same_day_lq#96,d_current_day#97,... 4 more fields] parquet
            :  +- Project [cs_sold_date_sk#136, cs_sold_time_sk#137, cs_ship_date_sk#138, cs_bill_customer_sk#139, cs_bill_cdemo_sk#140, cs_bill_hdemo_sk#141, cs_bill_addr_sk#142, cs_ship_customer_sk#143, cs_ship_cdemo_sk#144, cs_ship_hdemo_sk#145, cs_ship_addr_sk#146, cs_call_center_sk#147, cs_catalog_page_sk#148, cs_ship_mode_sk#149, cs_warehouse_sk#150, cs_item_sk#151, cs_promo_sk#152, cs_order_number#153, cs_quantity#154, cs_wholesale_cost#155, cs_list_price#156, cs_sales_price#157, cs_ext_discount_amt#158, cs_ext_sales_price#159, ... 38 more fields]
            :     +- Filter (((outer(c_customer_sk#11) = cs_ship_customer_sk#143) && (cs_sold_date_sk#136 = d_date_sk#74)) && ((d_year#80 = 2001) && ((d_moy#82 >= 4) && (d_moy#82 <= (4 + 2)))))
            :        +- Join Inner
            :           :- SubqueryAlias `tpcds`.`catalog_sales`
            :           :  +- Relation[cs_sold_date_sk#136,cs_sold_time_sk#137,cs_ship_date_sk#138,cs_bill_customer_sk#139,cs_bill_cdemo_sk#140,cs_bill_hdemo_sk#141,cs_bill_addr_sk#142,cs_ship_customer_sk#143,cs_ship_cdemo_sk#144,cs_ship_hdemo_sk#145,cs_ship_addr_sk#146,cs_call_center_sk#147,cs_catalog_page_sk#148,cs_ship_mode_sk#149,cs_warehouse_sk#150,cs_item_sk#151,cs_promo_sk#152,cs_order_number#153,cs_quantity#154,cs_wholesale_cost#155,cs_list_price#156,cs_sales_price#157,cs_ext_discount_amt#158,cs_ext_sales_price#159,... 10 more fields] parquet
            :           +- SubqueryAlias `tpcds`.`date_dim`
            :              +- Relation[d_date_sk#74,d_date_id#75,d_date#76,d_month_seq#77,d_week_seq#78,d_quarter_seq#79,d_year#80,d_dow#81,d_moy#82,d_dom#83,d_qoy#84,d_fy_year#85,d_fy_quarter_seq#86,d_fy_week_seq#87,d_day_name#88,d_quarter_name#89,d_holiday#90,d_weekend#91,d_following_holiday#92,d_first_dom#93,d_last_dom#94,d_same_day_ly#95,d_same_day_lq#96,d_current_day#97,... 4 more fields] parquet
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `c`
               :  :  +- SubqueryAlias `tpcds`.`customer`
               :  :     +- Relation[c_customer_sk#11,c_customer_id#12,c_current_cdemo_sk#13,c_current_hdemo_sk#14,c_current_addr_sk#15,c_first_shipto_date_sk#16,c_first_sales_date_sk#17,c_salutation#18,c_first_name#19,c_last_name#20,c_preferred_cust_flag#21,c_birth_day#22,c_birth_month#23,c_birth_year#24,c_birth_country#25,c_login#26,c_email_address#27,c_last_review_date#28] parquet
               :  +- SubqueryAlias `ca`
               :     +- SubqueryAlias `tpcds`.`customer_address`
               :        +- Relation[ca_address_sk#29,ca_address_id#30,ca_street_number#31,ca_street_name#32,ca_street_type#33,ca_suite_number#34,ca_city#35,ca_county#36,ca_state#37,ca_zip#38,ca_country#39,ca_gmt_offset#40,ca_location_type#41] parquet
               +- SubqueryAlias `tpcds`.`customer_demographics`
                  +- Relation[cd_demo_sk#42,cd_gender#43,cd_marital_status#44,cd_education_status#45,cd_purchase_estimate#46,cd_credit_rating#47,cd_dep_count#48,cd_dep_employed_count#49,cd_dep_college_count#50] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#43 ASC NULLS FIRST, cd_marital_status#44 ASC NULLS FIRST, cd_education_status#45 ASC NULLS FIRST, cd_purchase_estimate#46 ASC NULLS FIRST, cd_credit_rating#47 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47], [cd_gender#43, cd_marital_status#44, cd_education_status#45, count(1) AS cnt1#0L, cd_purchase_estimate#46, count(1) AS cnt2#1L, cd_credit_rating#47, count(1) AS cnt3#2L]
         +- Project [cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47]
            +- Join Inner, (cd_demo_sk#42 = c_current_cdemo_sk#13)
               :- Project [c_current_cdemo_sk#13]
               :  +- Join Inner, (c_current_addr_sk#15 = ca_address_sk#29)
               :     :- Project [c_current_cdemo_sk#13, c_current_addr_sk#15]
               :     :  +- Join LeftAnti, (c_customer_sk#11 = cs_ship_customer_sk#143)
               :     :     :- Join LeftAnti, (c_customer_sk#11 = ws_bill_customer_sk#106)
               :     :     :  :- Join LeftSemi, (c_customer_sk#11 = ss_customer_sk#54)
               :     :     :  :  :- Project [c_customer_sk#11, c_current_cdemo_sk#13, c_current_addr_sk#15]
               :     :     :  :  :  +- Filter (isnotnull(c_current_addr_sk#15) && isnotnull(c_current_cdemo_sk#13))
               :     :     :  :  :     +- Relation[c_customer_sk#11,c_customer_id#12,c_current_cdemo_sk#13,c_current_hdemo_sk#14,c_current_addr_sk#15,c_first_shipto_date_sk#16,c_first_sales_date_sk#17,c_salutation#18,c_first_name#19,c_last_name#20,c_preferred_cust_flag#21,c_birth_day#22,c_birth_month#23,c_birth_year#24,c_birth_country#25,c_login#26,c_email_address#27,c_last_review_date#28] parquet
               :     :     :  :  +- Project [ss_customer_sk#54]
               :     :     :  :     +- Join Inner, (ss_sold_date_sk#51 = d_date_sk#74)
               :     :     :  :        :- Project [ss_sold_date_sk#51, ss_customer_sk#54]
               :     :     :  :        :  +- Filter isnotnull(ss_sold_date_sk#51)
               :     :     :  :        :     +- Relation[ss_sold_date_sk#51,ss_sold_time_sk#52,ss_item_sk#53,ss_customer_sk#54,ss_cdemo_sk#55,ss_hdemo_sk#56,ss_addr_sk#57,ss_store_sk#58,ss_promo_sk#59,ss_ticket_number#60,ss_quantity#61,ss_wholesale_cost#62,ss_list_price#63,ss_sales_price#64,ss_ext_discount_amt#65,ss_ext_sales_price#66,ss_ext_wholesale_cost#67,ss_ext_list_price#68,ss_ext_tax#69,ss_coupon_amt#70,ss_net_paid#71,ss_net_paid_inc_tax#72,ss_net_profit#73] parquet
               :     :     :  :        +- Project [d_date_sk#74]
               :     :     :  :           +- Filter (((((isnotnull(d_moy#82) && isnotnull(d_year#80)) && (d_year#80 = 2001)) && (d_moy#82 >= 4)) && (d_moy#82 <= 6)) && isnotnull(d_date_sk#74))
               :     :     :  :              +- Relation[d_date_sk#74,d_date_id#75,d_date#76,d_month_seq#77,d_week_seq#78,d_quarter_seq#79,d_year#80,d_dow#81,d_moy#82,d_dom#83,d_qoy#84,d_fy_year#85,d_fy_quarter_seq#86,d_fy_week_seq#87,d_day_name#88,d_quarter_name#89,d_holiday#90,d_weekend#91,d_following_holiday#92,d_first_dom#93,d_last_dom#94,d_same_day_ly#95,d_same_day_lq#96,d_current_day#97,... 4 more fields] parquet
               :     :     :  +- Project [ws_bill_customer_sk#106]
               :     :     :     +- Join Inner, (ws_sold_date_sk#102 = d_date_sk#74)
               :     :     :        :- Project [ws_sold_date_sk#102, ws_bill_customer_sk#106]
               :     :     :        :  +- Filter isnotnull(ws_sold_date_sk#102)
               :     :     :        :     +- Relation[ws_sold_date_sk#102,ws_sold_time_sk#103,ws_ship_date_sk#104,ws_item_sk#105,ws_bill_customer_sk#106,ws_bill_cdemo_sk#107,ws_bill_hdemo_sk#108,ws_bill_addr_sk#109,ws_ship_customer_sk#110,ws_ship_cdemo_sk#111,ws_ship_hdemo_sk#112,ws_ship_addr_sk#113,ws_web_page_sk#114,ws_web_site_sk#115,ws_ship_mode_sk#116,ws_warehouse_sk#117,ws_promo_sk#118,ws_order_number#119,ws_quantity#120,ws_wholesale_cost#121,ws_list_price#122,ws_sales_price#123,ws_ext_discount_amt#124,ws_ext_sales_price#125,... 10 more fields] parquet
               :     :     :        +- Project [d_date_sk#74]
               :     :     :           +- Filter (((((isnotnull(d_moy#82) && isnotnull(d_year#80)) && (d_year#80 = 2001)) && (d_moy#82 >= 4)) && (d_moy#82 <= 6)) && isnotnull(d_date_sk#74))
               :     :     :              +- Relation[d_date_sk#74,d_date_id#75,d_date#76,d_month_seq#77,d_week_seq#78,d_quarter_seq#79,d_year#80,d_dow#81,d_moy#82,d_dom#83,d_qoy#84,d_fy_year#85,d_fy_quarter_seq#86,d_fy_week_seq#87,d_day_name#88,d_quarter_name#89,d_holiday#90,d_weekend#91,d_following_holiday#92,d_first_dom#93,d_last_dom#94,d_same_day_ly#95,d_same_day_lq#96,d_current_day#97,... 4 more fields] parquet
               :     :     +- Project [cs_ship_customer_sk#143]
               :     :        +- Join Inner, (cs_sold_date_sk#136 = d_date_sk#74)
               :     :           :- Project [cs_sold_date_sk#136, cs_ship_customer_sk#143]
               :     :           :  +- Filter isnotnull(cs_sold_date_sk#136)
               :     :           :     +- Relation[cs_sold_date_sk#136,cs_sold_time_sk#137,cs_ship_date_sk#138,cs_bill_customer_sk#139,cs_bill_cdemo_sk#140,cs_bill_hdemo_sk#141,cs_bill_addr_sk#142,cs_ship_customer_sk#143,cs_ship_cdemo_sk#144,cs_ship_hdemo_sk#145,cs_ship_addr_sk#146,cs_call_center_sk#147,cs_catalog_page_sk#148,cs_ship_mode_sk#149,cs_warehouse_sk#150,cs_item_sk#151,cs_promo_sk#152,cs_order_number#153,cs_quantity#154,cs_wholesale_cost#155,cs_list_price#156,cs_sales_price#157,cs_ext_discount_amt#158,cs_ext_sales_price#159,... 10 more fields] parquet
               :     :           +- Project [d_date_sk#74]
               :     :              +- Filter (((((isnotnull(d_moy#82) && isnotnull(d_year#80)) && (d_year#80 = 2001)) && (d_moy#82 >= 4)) && (d_moy#82 <= 6)) && isnotnull(d_date_sk#74))
               :     :                 +- Relation[d_date_sk#74,d_date_id#75,d_date#76,d_month_seq#77,d_week_seq#78,d_quarter_seq#79,d_year#80,d_dow#81,d_moy#82,d_dom#83,d_qoy#84,d_fy_year#85,d_fy_quarter_seq#86,d_fy_week_seq#87,d_day_name#88,d_quarter_name#89,d_holiday#90,d_weekend#91,d_following_holiday#92,d_first_dom#93,d_last_dom#94,d_same_day_ly#95,d_same_day_lq#96,d_current_day#97,... 4 more fields] parquet
               :     +- Project [ca_address_sk#29]
               :        +- Filter (ca_state#37 IN (KY,GA,NM) && isnotnull(ca_address_sk#29))
               :           +- Relation[ca_address_sk#29,ca_address_id#30,ca_street_number#31,ca_street_name#32,ca_street_type#33,ca_suite_number#34,ca_city#35,ca_county#36,ca_state#37,ca_zip#38,ca_country#39,ca_gmt_offset#40,ca_location_type#41] parquet
               +- Project [cd_demo_sk#42, cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47]
                  +- Filter isnotnull(cd_demo_sk#42)
                     +- Relation[cd_demo_sk#42,cd_gender#43,cd_marital_status#44,cd_education_status#45,cd_purchase_estimate#46,cd_credit_rating#47,cd_dep_count#48,cd_dep_employed_count#49,cd_dep_college_count#50] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[cd_gender#43 ASC NULLS FIRST,cd_marital_status#44 ASC NULLS FIRST,cd_education_status#45 ASC NULLS FIRST,cd_purchase_estimate#46 ASC NULLS FIRST,cd_credit_rating#47 ASC NULLS FIRST], output=[cd_gender#43,cd_marital_status#44,cd_education_status#45,cnt1#0L,cd_purchase_estimate#46,cnt2#1L,cd_credit_rating#47,cnt3#2L])
+- *(15) HashAggregate(keys=[cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47], functions=[count(1)], output=[cd_gender#43, cd_marital_status#44, cd_education_status#45, cnt1#0L, cd_purchase_estimate#46, cnt2#1L, cd_credit_rating#47, cnt3#2L])
   +- Exchange hashpartitioning(cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47, 200)
      +- *(14) HashAggregate(keys=[cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47], functions=[partial_count(1)], output=[cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47, count#171L])
         +- *(14) Project [cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47]
            +- *(14) BroadcastHashJoin [c_current_cdemo_sk#13], [cd_demo_sk#42], Inner, BuildRight
               :- *(14) Project [c_current_cdemo_sk#13]
               :  +- *(14) BroadcastHashJoin [c_current_addr_sk#15], [ca_address_sk#29], Inner, BuildRight
               :     :- *(14) Project [c_current_cdemo_sk#13, c_current_addr_sk#15]
               :     :  +- SortMergeJoin [c_customer_sk#11], [cs_ship_customer_sk#143], LeftAnti
               :     :     :- SortMergeJoin [c_customer_sk#11], [ws_bill_customer_sk#106], LeftAnti
               :     :     :  :- SortMergeJoin [c_customer_sk#11], [ss_customer_sk#54], LeftSemi
               :     :     :  :  :- *(2) Sort [c_customer_sk#11 ASC NULLS FIRST], false, 0
               :     :     :  :  :  +- Exchange hashpartitioning(c_customer_sk#11, 200)
               :     :     :  :  :     +- *(1) Project [c_customer_sk#11, c_current_cdemo_sk#13, c_current_addr_sk#15]
               :     :     :  :  :        +- *(1) Filter (isnotnull(c_current_addr_sk#15) && isnotnull(c_current_cdemo_sk#13))
               :     :     :  :  :           +- *(1) FileScan parquet tpcds.customer[c_customer_sk#11,c_current_cdemo_sk#13,c_current_addr_sk#15] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk)], ReadSchema: struct<c_customer_sk:int,c_current_cdemo_sk:int,c_current_addr_sk:int>
               :     :     :  :  +- *(5) Sort [ss_customer_sk#54 ASC NULLS FIRST], false, 0
               :     :     :  :     +- Exchange hashpartitioning(ss_customer_sk#54, 200)
               :     :     :  :        +- *(4) Project [ss_customer_sk#54]
               :     :     :  :           +- *(4) BroadcastHashJoin [ss_sold_date_sk#51], [d_date_sk#74], Inner, BuildRight
               :     :     :  :              :- *(4) Project [ss_sold_date_sk#51, ss_customer_sk#54]
               :     :     :  :              :  +- *(4) Filter isnotnull(ss_sold_date_sk#51)
               :     :     :  :              :     +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#51,ss_customer_sk#54] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int>
               :     :     :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :  :                 +- *(3) Project [d_date_sk#74]
               :     :     :  :                    +- *(3) Filter (((((isnotnull(d_moy#82) && isnotnull(d_year#80)) && (d_year#80 = 2001)) && (d_moy#82 >= 4)) && (d_moy#82 <= 6)) && isnotnull(d_date_sk#74))
               :     :     :  :                       +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#74,d_year#80,d_moy#82] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_year,2001), GreaterThanOrEqual(d_moy,4), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :     :     :  +- *(8) Sort [ws_bill_customer_sk#106 ASC NULLS FIRST], false, 0
               :     :     :     +- Exchange hashpartitioning(ws_bill_customer_sk#106, 200)
               :     :     :        +- *(7) Project [ws_bill_customer_sk#106]
               :     :     :           +- *(7) BroadcastHashJoin [ws_sold_date_sk#102], [d_date_sk#74], Inner, BuildRight
               :     :     :              :- *(7) Project [ws_sold_date_sk#102, ws_bill_customer_sk#106]
               :     :     :              :  +- *(7) Filter isnotnull(ws_sold_date_sk#102)
               :     :     :              :     +- *(7) FileScan parquet tpcds.web_sales[ws_sold_date_sk#102,ws_bill_customer_sk#106] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int>
               :     :     :              +- ReusedExchange [d_date_sk#74], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     +- *(11) Sort [cs_ship_customer_sk#143 ASC NULLS FIRST], false, 0
               :     :        +- Exchange hashpartitioning(cs_ship_customer_sk#143, 200)
               :     :           +- *(10) Project [cs_ship_customer_sk#143]
               :     :              +- *(10) BroadcastHashJoin [cs_sold_date_sk#136], [d_date_sk#74], Inner, BuildRight
               :     :                 :- *(10) Project [cs_sold_date_sk#136, cs_ship_customer_sk#143]
               :     :                 :  +- *(10) Filter isnotnull(cs_sold_date_sk#136)
               :     :                 :     +- *(10) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#136,cs_ship_customer_sk#143] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ship_customer_sk:int>
               :     :                 +- ReusedExchange [d_date_sk#74], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(12) Project [ca_address_sk#29]
               :           +- *(12) Filter (ca_state#37 IN (KY,GA,NM) && isnotnull(ca_address_sk#29))
               :              +- *(12) FileScan parquet tpcds.customer_address[ca_address_sk#29,ca_state#37] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [In(ca_state, [KY,GA,NM]), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(13) Project [cd_demo_sk#42, cd_gender#43, cd_marital_status#44, cd_education_status#45, cd_purchase_estimate#46, cd_credit_rating#47]
                     +- *(13) Filter isnotnull(cd_demo_sk#42)
                        +- *(13) FileScan parquet tpcds.customer_demographics[cd_demo_sk#42,cd_gender#43,cd_marital_status#44,cd_education_status#45,cd_purchase_estimate#46,cd_credit_rating#47] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string,cd_pur...
Time taken: 4.336 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 69 in stream 0 using template query69.tpl
------------------------------------------------------^^^

