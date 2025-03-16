== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['cd_gender ASC NULLS FIRST, 'cd_marital_status ASC NULLS FIRST, 'cd_education_status ASC NULLS FIRST, 'cd_purchase_estimate ASC NULLS FIRST, 'cd_credit_rating ASC NULLS FIRST, 'cd_dep_count ASC NULLS FIRST, 'cd_dep_employed_count ASC NULLS FIRST, 'cd_dep_college_count ASC NULLS FIRST], true
      +- 'Aggregate ['cd_gender, 'cd_marital_status, 'cd_education_status, 'cd_purchase_estimate, 'cd_credit_rating, 'cd_dep_count, 'cd_dep_employed_count, 'cd_dep_college_count], ['cd_gender, 'cd_marital_status, 'cd_education_status, 'count(1) AS cnt1#0, 'cd_purchase_estimate, 'count(1) AS cnt2#1, 'cd_credit_rating, 'count(1) AS cnt3#2, 'cd_dep_count, 'count(1) AS cnt4#3, 'cd_dep_employed_count, 'count(1) AS cnt5#4, 'cd_dep_college_count, 'count(1) AS cnt6#5]
         +- 'Filter (((('c.c_current_addr_sk = 'ca.ca_address_sk) && 'ca_county IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County)) && ('cd_demo_sk = 'c.c_current_cdemo_sk)) && (exists#6 [] && (exists#7 [] || exists#8 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ss_customer_sk) && ('ss_sold_date_sk = 'd_date_sk)) && (('d_year = 2002) && (('d_moy >= 1) && ('d_moy <= (1 + 3)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation `store_sales`
            :  :        +- 'UnresolvedRelation `date_dim`
            :  :- 'Project [*]
            :  :  +- 'Filter ((('c.c_customer_sk = 'ws_bill_customer_sk) && ('ws_sold_date_sk = 'd_date_sk)) && (('d_year = 2002) && (('d_moy >= 1) && ('d_moy <= (1 + 3)))))
            :  :     +- 'Join Inner
            :  :        :- 'UnresolvedRelation `web_sales`
            :  :        +- 'UnresolvedRelation `date_dim`
            :  +- 'Project [*]
            :     +- 'Filter ((('c.c_customer_sk = 'cs_ship_customer_sk) && ('cs_sold_date_sk = 'd_date_sk)) && (('d_year = 2002) && (('d_moy >= 1) && ('d_moy <= (1 + 3)))))
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
cd_gender: string, cd_marital_status: string, cd_education_status: string, cnt1: bigint, cd_purchase_estimate: int, cnt2: bigint, cd_credit_rating: string, cnt3: bigint, cd_dep_count: int, cnt4: bigint, cd_dep_employed_count: int, cnt5: bigint, cd_dep_college_count: int, cnt6: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#49 ASC NULLS FIRST, cd_marital_status#50 ASC NULLS FIRST, cd_education_status#51 ASC NULLS FIRST, cd_purchase_estimate#52 ASC NULLS FIRST, cd_credit_rating#53 ASC NULLS FIRST, cd_dep_count#54 ASC NULLS FIRST, cd_dep_employed_count#55 ASC NULLS FIRST, cd_dep_college_count#56 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56], [cd_gender#49, cd_marital_status#50, cd_education_status#51, count(1) AS cnt1#0L, cd_purchase_estimate#52, count(1) AS cnt2#1L, cd_credit_rating#53, count(1) AS cnt3#2L, cd_dep_count#54, count(1) AS cnt4#3L, cd_dep_employed_count#55, count(1) AS cnt5#4L, cd_dep_college_count#56, count(1) AS cnt6#5L]
         +- Filter ((((c_current_addr_sk#21 = ca_address_sk#35) && ca_county#42 IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County)) && (cd_demo_sk#48 = c_current_cdemo_sk#19)) && (exists#6 [c_customer_sk#17] && (exists#7 [c_customer_sk#17] || exists#8 [c_customer_sk#17])))
            :  :- Project [ss_sold_date_sk#57, ss_sold_time_sk#58, ss_item_sk#59, ss_customer_sk#60, ss_cdemo_sk#61, ss_hdemo_sk#62, ss_addr_sk#63, ss_store_sk#64, ss_promo_sk#65, ss_ticket_number#66, ss_quantity#67, ss_wholesale_cost#68, ss_list_price#69, ss_sales_price#70, ss_ext_discount_amt#71, ss_ext_sales_price#72, ss_ext_wholesale_cost#73, ss_ext_list_price#74, ss_ext_tax#75, ss_coupon_amt#76, ss_net_paid#77, ss_net_paid_inc_tax#78, ss_net_profit#79, d_date_sk#80, ... 27 more fields]
            :  :  +- Filter (((outer(c_customer_sk#17) = ss_customer_sk#60) && (ss_sold_date_sk#57 = d_date_sk#80)) && ((d_year#86 = 2002) && ((d_moy#88 >= 1) && (d_moy#88 <= (1 + 3)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias `tpcds`.`store_sales`
            :  :        :  +- Relation[ss_sold_date_sk#57,ss_sold_time_sk#58,ss_item_sk#59,ss_customer_sk#60,ss_cdemo_sk#61,ss_hdemo_sk#62,ss_addr_sk#63,ss_store_sk#64,ss_promo_sk#65,ss_ticket_number#66,ss_quantity#67,ss_wholesale_cost#68,ss_list_price#69,ss_sales_price#70,ss_ext_discount_amt#71,ss_ext_sales_price#72,ss_ext_wholesale_cost#73,ss_ext_list_price#74,ss_ext_tax#75,ss_coupon_amt#76,ss_net_paid#77,ss_net_paid_inc_tax#78,ss_net_profit#79] parquet
            :  :        +- SubqueryAlias `tpcds`.`date_dim`
            :  :           +- Relation[d_date_sk#80,d_date_id#81,d_date#82,d_month_seq#83,d_week_seq#84,d_quarter_seq#85,d_year#86,d_dow#87,d_moy#88,d_dom#89,d_qoy#90,d_fy_year#91,d_fy_quarter_seq#92,d_fy_week_seq#93,d_day_name#94,d_quarter_name#95,d_holiday#96,d_weekend#97,d_following_holiday#98,d_first_dom#99,d_last_dom#100,d_same_day_ly#101,d_same_day_lq#102,d_current_day#103,... 4 more fields] parquet
            :  :- Project [ws_sold_date_sk#108, ws_sold_time_sk#109, ws_ship_date_sk#110, ws_item_sk#111, ws_bill_customer_sk#112, ws_bill_cdemo_sk#113, ws_bill_hdemo_sk#114, ws_bill_addr_sk#115, ws_ship_customer_sk#116, ws_ship_cdemo_sk#117, ws_ship_hdemo_sk#118, ws_ship_addr_sk#119, ws_web_page_sk#120, ws_web_site_sk#121, ws_ship_mode_sk#122, ws_warehouse_sk#123, ws_promo_sk#124, ws_order_number#125, ws_quantity#126, ws_wholesale_cost#127, ws_list_price#128, ws_sales_price#129, ws_ext_discount_amt#130, ws_ext_sales_price#131, ... 38 more fields]
            :  :  +- Filter (((outer(c_customer_sk#17) = ws_bill_customer_sk#112) && (ws_sold_date_sk#108 = d_date_sk#80)) && ((d_year#86 = 2002) && ((d_moy#88 >= 1) && (d_moy#88 <= (1 + 3)))))
            :  :     +- Join Inner
            :  :        :- SubqueryAlias `tpcds`.`web_sales`
            :  :        :  +- Relation[ws_sold_date_sk#108,ws_sold_time_sk#109,ws_ship_date_sk#110,ws_item_sk#111,ws_bill_customer_sk#112,ws_bill_cdemo_sk#113,ws_bill_hdemo_sk#114,ws_bill_addr_sk#115,ws_ship_customer_sk#116,ws_ship_cdemo_sk#117,ws_ship_hdemo_sk#118,ws_ship_addr_sk#119,ws_web_page_sk#120,ws_web_site_sk#121,ws_ship_mode_sk#122,ws_warehouse_sk#123,ws_promo_sk#124,ws_order_number#125,ws_quantity#126,ws_wholesale_cost#127,ws_list_price#128,ws_sales_price#129,ws_ext_discount_amt#130,ws_ext_sales_price#131,... 10 more fields] parquet
            :  :        +- SubqueryAlias `tpcds`.`date_dim`
            :  :           +- Relation[d_date_sk#80,d_date_id#81,d_date#82,d_month_seq#83,d_week_seq#84,d_quarter_seq#85,d_year#86,d_dow#87,d_moy#88,d_dom#89,d_qoy#90,d_fy_year#91,d_fy_quarter_seq#92,d_fy_week_seq#93,d_day_name#94,d_quarter_name#95,d_holiday#96,d_weekend#97,d_following_holiday#98,d_first_dom#99,d_last_dom#100,d_same_day_ly#101,d_same_day_lq#102,d_current_day#103,... 4 more fields] parquet
            :  +- Project [cs_sold_date_sk#142, cs_sold_time_sk#143, cs_ship_date_sk#144, cs_bill_customer_sk#145, cs_bill_cdemo_sk#146, cs_bill_hdemo_sk#147, cs_bill_addr_sk#148, cs_ship_customer_sk#149, cs_ship_cdemo_sk#150, cs_ship_hdemo_sk#151, cs_ship_addr_sk#152, cs_call_center_sk#153, cs_catalog_page_sk#154, cs_ship_mode_sk#155, cs_warehouse_sk#156, cs_item_sk#157, cs_promo_sk#158, cs_order_number#159, cs_quantity#160, cs_wholesale_cost#161, cs_list_price#162, cs_sales_price#163, cs_ext_discount_amt#164, cs_ext_sales_price#165, ... 38 more fields]
            :     +- Filter (((outer(c_customer_sk#17) = cs_ship_customer_sk#149) && (cs_sold_date_sk#142 = d_date_sk#80)) && ((d_year#86 = 2002) && ((d_moy#88 >= 1) && (d_moy#88 <= (1 + 3)))))
            :        +- Join Inner
            :           :- SubqueryAlias `tpcds`.`catalog_sales`
            :           :  +- Relation[cs_sold_date_sk#142,cs_sold_time_sk#143,cs_ship_date_sk#144,cs_bill_customer_sk#145,cs_bill_cdemo_sk#146,cs_bill_hdemo_sk#147,cs_bill_addr_sk#148,cs_ship_customer_sk#149,cs_ship_cdemo_sk#150,cs_ship_hdemo_sk#151,cs_ship_addr_sk#152,cs_call_center_sk#153,cs_catalog_page_sk#154,cs_ship_mode_sk#155,cs_warehouse_sk#156,cs_item_sk#157,cs_promo_sk#158,cs_order_number#159,cs_quantity#160,cs_wholesale_cost#161,cs_list_price#162,cs_sales_price#163,cs_ext_discount_amt#164,cs_ext_sales_price#165,... 10 more fields] parquet
            :           +- SubqueryAlias `tpcds`.`date_dim`
            :              +- Relation[d_date_sk#80,d_date_id#81,d_date#82,d_month_seq#83,d_week_seq#84,d_quarter_seq#85,d_year#86,d_dow#87,d_moy#88,d_dom#89,d_qoy#90,d_fy_year#91,d_fy_quarter_seq#92,d_fy_week_seq#93,d_day_name#94,d_quarter_name#95,d_holiday#96,d_weekend#97,d_following_holiday#98,d_first_dom#99,d_last_dom#100,d_same_day_ly#101,d_same_day_lq#102,d_current_day#103,... 4 more fields] parquet
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `c`
               :  :  +- SubqueryAlias `tpcds`.`customer`
               :  :     +- Relation[c_customer_sk#17,c_customer_id#18,c_current_cdemo_sk#19,c_current_hdemo_sk#20,c_current_addr_sk#21,c_first_shipto_date_sk#22,c_first_sales_date_sk#23,c_salutation#24,c_first_name#25,c_last_name#26,c_preferred_cust_flag#27,c_birth_day#28,c_birth_month#29,c_birth_year#30,c_birth_country#31,c_login#32,c_email_address#33,c_last_review_date#34] parquet
               :  +- SubqueryAlias `ca`
               :     +- SubqueryAlias `tpcds`.`customer_address`
               :        +- Relation[ca_address_sk#35,ca_address_id#36,ca_street_number#37,ca_street_name#38,ca_street_type#39,ca_suite_number#40,ca_city#41,ca_county#42,ca_state#43,ca_zip#44,ca_country#45,ca_gmt_offset#46,ca_location_type#47] parquet
               +- SubqueryAlias `tpcds`.`customer_demographics`
                  +- Relation[cd_demo_sk#48,cd_gender#49,cd_marital_status#50,cd_education_status#51,cd_purchase_estimate#52,cd_credit_rating#53,cd_dep_count#54,cd_dep_employed_count#55,cd_dep_college_count#56] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cd_gender#49 ASC NULLS FIRST, cd_marital_status#50 ASC NULLS FIRST, cd_education_status#51 ASC NULLS FIRST, cd_purchase_estimate#52 ASC NULLS FIRST, cd_credit_rating#53 ASC NULLS FIRST, cd_dep_count#54 ASC NULLS FIRST, cd_dep_employed_count#55 ASC NULLS FIRST, cd_dep_college_count#56 ASC NULLS FIRST], true
      +- Aggregate [cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56], [cd_gender#49, cd_marital_status#50, cd_education_status#51, count(1) AS cnt1#0L, cd_purchase_estimate#52, count(1) AS cnt2#1L, cd_credit_rating#53, count(1) AS cnt3#2L, cd_dep_count#54, count(1) AS cnt4#3L, cd_dep_employed_count#55, count(1) AS cnt5#4L, cd_dep_college_count#56, count(1) AS cnt6#5L]
         +- Project [cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56]
            +- Join Inner, (cd_demo_sk#48 = c_current_cdemo_sk#19)
               :- Project [c_current_cdemo_sk#19]
               :  +- Join Inner, (c_current_addr_sk#21 = ca_address_sk#35)
               :     :- Project [c_current_cdemo_sk#19, c_current_addr_sk#21]
               :     :  +- Filter (exists#176 || exists#177)
               :     :     +- Join ExistenceJoin(exists#177), (c_customer_sk#17 = cs_ship_customer_sk#149)
               :     :        :- Join ExistenceJoin(exists#176), (c_customer_sk#17 = ws_bill_customer_sk#112)
               :     :        :  :- Join LeftSemi, (c_customer_sk#17 = ss_customer_sk#60)
               :     :        :  :  :- Project [c_customer_sk#17, c_current_cdemo_sk#19, c_current_addr_sk#21]
               :     :        :  :  :  +- Filter (isnotnull(c_current_addr_sk#21) && isnotnull(c_current_cdemo_sk#19))
               :     :        :  :  :     +- Relation[c_customer_sk#17,c_customer_id#18,c_current_cdemo_sk#19,c_current_hdemo_sk#20,c_current_addr_sk#21,c_first_shipto_date_sk#22,c_first_sales_date_sk#23,c_salutation#24,c_first_name#25,c_last_name#26,c_preferred_cust_flag#27,c_birth_day#28,c_birth_month#29,c_birth_year#30,c_birth_country#31,c_login#32,c_email_address#33,c_last_review_date#34] parquet
               :     :        :  :  +- Project [ss_customer_sk#60]
               :     :        :  :     +- Join Inner, (ss_sold_date_sk#57 = d_date_sk#80)
               :     :        :  :        :- Project [ss_sold_date_sk#57, ss_customer_sk#60]
               :     :        :  :        :  +- Filter isnotnull(ss_sold_date_sk#57)
               :     :        :  :        :     +- Relation[ss_sold_date_sk#57,ss_sold_time_sk#58,ss_item_sk#59,ss_customer_sk#60,ss_cdemo_sk#61,ss_hdemo_sk#62,ss_addr_sk#63,ss_store_sk#64,ss_promo_sk#65,ss_ticket_number#66,ss_quantity#67,ss_wholesale_cost#68,ss_list_price#69,ss_sales_price#70,ss_ext_discount_amt#71,ss_ext_sales_price#72,ss_ext_wholesale_cost#73,ss_ext_list_price#74,ss_ext_tax#75,ss_coupon_amt#76,ss_net_paid#77,ss_net_paid_inc_tax#78,ss_net_profit#79] parquet
               :     :        :  :        +- Project [d_date_sk#80]
               :     :        :  :           +- Filter (((((isnotnull(d_moy#88) && isnotnull(d_year#86)) && (d_year#86 = 2002)) && (d_moy#88 >= 1)) && (d_moy#88 <= 4)) && isnotnull(d_date_sk#80))
               :     :        :  :              +- Relation[d_date_sk#80,d_date_id#81,d_date#82,d_month_seq#83,d_week_seq#84,d_quarter_seq#85,d_year#86,d_dow#87,d_moy#88,d_dom#89,d_qoy#90,d_fy_year#91,d_fy_quarter_seq#92,d_fy_week_seq#93,d_day_name#94,d_quarter_name#95,d_holiday#96,d_weekend#97,d_following_holiday#98,d_first_dom#99,d_last_dom#100,d_same_day_ly#101,d_same_day_lq#102,d_current_day#103,... 4 more fields] parquet
               :     :        :  +- Project [ws_bill_customer_sk#112]
               :     :        :     +- Join Inner, (ws_sold_date_sk#108 = d_date_sk#80)
               :     :        :        :- Project [ws_sold_date_sk#108, ws_bill_customer_sk#112]
               :     :        :        :  +- Filter isnotnull(ws_sold_date_sk#108)
               :     :        :        :     +- Relation[ws_sold_date_sk#108,ws_sold_time_sk#109,ws_ship_date_sk#110,ws_item_sk#111,ws_bill_customer_sk#112,ws_bill_cdemo_sk#113,ws_bill_hdemo_sk#114,ws_bill_addr_sk#115,ws_ship_customer_sk#116,ws_ship_cdemo_sk#117,ws_ship_hdemo_sk#118,ws_ship_addr_sk#119,ws_web_page_sk#120,ws_web_site_sk#121,ws_ship_mode_sk#122,ws_warehouse_sk#123,ws_promo_sk#124,ws_order_number#125,ws_quantity#126,ws_wholesale_cost#127,ws_list_price#128,ws_sales_price#129,ws_ext_discount_amt#130,ws_ext_sales_price#131,... 10 more fields] parquet
               :     :        :        +- Project [d_date_sk#80]
               :     :        :           +- Filter (((((isnotnull(d_moy#88) && isnotnull(d_year#86)) && (d_year#86 = 2002)) && (d_moy#88 >= 1)) && (d_moy#88 <= 4)) && isnotnull(d_date_sk#80))
               :     :        :              +- Relation[d_date_sk#80,d_date_id#81,d_date#82,d_month_seq#83,d_week_seq#84,d_quarter_seq#85,d_year#86,d_dow#87,d_moy#88,d_dom#89,d_qoy#90,d_fy_year#91,d_fy_quarter_seq#92,d_fy_week_seq#93,d_day_name#94,d_quarter_name#95,d_holiday#96,d_weekend#97,d_following_holiday#98,d_first_dom#99,d_last_dom#100,d_same_day_ly#101,d_same_day_lq#102,d_current_day#103,... 4 more fields] parquet
               :     :        +- Project [cs_ship_customer_sk#149]
               :     :           +- Join Inner, (cs_sold_date_sk#142 = d_date_sk#80)
               :     :              :- Project [cs_sold_date_sk#142, cs_ship_customer_sk#149]
               :     :              :  +- Filter isnotnull(cs_sold_date_sk#142)
               :     :              :     +- Relation[cs_sold_date_sk#142,cs_sold_time_sk#143,cs_ship_date_sk#144,cs_bill_customer_sk#145,cs_bill_cdemo_sk#146,cs_bill_hdemo_sk#147,cs_bill_addr_sk#148,cs_ship_customer_sk#149,cs_ship_cdemo_sk#150,cs_ship_hdemo_sk#151,cs_ship_addr_sk#152,cs_call_center_sk#153,cs_catalog_page_sk#154,cs_ship_mode_sk#155,cs_warehouse_sk#156,cs_item_sk#157,cs_promo_sk#158,cs_order_number#159,cs_quantity#160,cs_wholesale_cost#161,cs_list_price#162,cs_sales_price#163,cs_ext_discount_amt#164,cs_ext_sales_price#165,... 10 more fields] parquet
               :     :              +- Project [d_date_sk#80]
               :     :                 +- Filter (((((isnotnull(d_moy#88) && isnotnull(d_year#86)) && (d_year#86 = 2002)) && (d_moy#88 >= 1)) && (d_moy#88 <= 4)) && isnotnull(d_date_sk#80))
               :     :                    +- Relation[d_date_sk#80,d_date_id#81,d_date#82,d_month_seq#83,d_week_seq#84,d_quarter_seq#85,d_year#86,d_dow#87,d_moy#88,d_dom#89,d_qoy#90,d_fy_year#91,d_fy_quarter_seq#92,d_fy_week_seq#93,d_day_name#94,d_quarter_name#95,d_holiday#96,d_weekend#97,d_following_holiday#98,d_first_dom#99,d_last_dom#100,d_same_day_ly#101,d_same_day_lq#102,d_current_day#103,... 4 more fields] parquet
               :     +- Project [ca_address_sk#35]
               :        +- Filter (ca_county#42 IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County) && isnotnull(ca_address_sk#35))
               :           +- Relation[ca_address_sk#35,ca_address_id#36,ca_street_number#37,ca_street_name#38,ca_street_type#39,ca_suite_number#40,ca_city#41,ca_county#42,ca_state#43,ca_zip#44,ca_country#45,ca_gmt_offset#46,ca_location_type#47] parquet
               +- Filter isnotnull(cd_demo_sk#48)
                  +- Relation[cd_demo_sk#48,cd_gender#49,cd_marital_status#50,cd_education_status#51,cd_purchase_estimate#52,cd_credit_rating#53,cd_dep_count#54,cd_dep_employed_count#55,cd_dep_college_count#56] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[cd_gender#49 ASC NULLS FIRST,cd_marital_status#50 ASC NULLS FIRST,cd_education_status#51 ASC NULLS FIRST,cd_purchase_estimate#52 ASC NULLS FIRST,cd_credit_rating#53 ASC NULLS FIRST,cd_dep_count#54 ASC NULLS FIRST,cd_dep_employed_count#55 ASC NULLS FIRST,cd_dep_college_count#56 ASC NULLS FIRST], output=[cd_gender#49,cd_marital_status#50,cd_education_status#51,cnt1#0L,cd_purchase_estimate#52,cnt2#1L,cd_credit_rating#53,cnt3#2L,cd_dep_count#54,cnt4#3L,cd_dep_employed_count#55,cnt5#4L,cd_dep_college_count#56,cnt6#5L])
+- *(15) HashAggregate(keys=[cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56], functions=[count(1)], output=[cd_gender#49, cd_marital_status#50, cd_education_status#51, cnt1#0L, cd_purchase_estimate#52, cnt2#1L, cd_credit_rating#53, cnt3#2L, cd_dep_count#54, cnt4#3L, cd_dep_employed_count#55, cnt5#4L, cd_dep_college_count#56, cnt6#5L])
   +- Exchange hashpartitioning(cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56, 200)
      +- *(14) HashAggregate(keys=[cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56], functions=[partial_count(1)], output=[cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56, count#179L])
         +- *(14) Project [cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56]
            +- *(14) BroadcastHashJoin [c_current_cdemo_sk#19], [cd_demo_sk#48], Inner, BuildRight
               :- *(14) Project [c_current_cdemo_sk#19]
               :  +- *(14) BroadcastHashJoin [c_current_addr_sk#21], [ca_address_sk#35], Inner, BuildRight
               :     :- *(14) Project [c_current_cdemo_sk#19, c_current_addr_sk#21]
               :     :  +- *(14) Filter (exists#176 || exists#177)
               :     :     +- SortMergeJoin [c_customer_sk#17], [cs_ship_customer_sk#149], ExistenceJoin(exists#177)
               :     :        :- SortMergeJoin [c_customer_sk#17], [ws_bill_customer_sk#112], ExistenceJoin(exists#176)
               :     :        :  :- SortMergeJoin [c_customer_sk#17], [ss_customer_sk#60], LeftSemi
               :     :        :  :  :- *(2) Sort [c_customer_sk#17 ASC NULLS FIRST], false, 0
               :     :        :  :  :  +- Exchange hashpartitioning(c_customer_sk#17, 200)
               :     :        :  :  :     +- *(1) Project [c_customer_sk#17, c_current_cdemo_sk#19, c_current_addr_sk#21]
               :     :        :  :  :        +- *(1) Filter (isnotnull(c_current_addr_sk#21) && isnotnull(c_current_cdemo_sk#19))
               :     :        :  :  :           +- *(1) FileScan parquet tpcds.customer[c_customer_sk#17,c_current_cdemo_sk#19,c_current_addr_sk#21] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk)], ReadSchema: struct<c_customer_sk:int,c_current_cdemo_sk:int,c_current_addr_sk:int>
               :     :        :  :  +- *(5) Sort [ss_customer_sk#60 ASC NULLS FIRST], false, 0
               :     :        :  :     +- Exchange hashpartitioning(ss_customer_sk#60, 200)
               :     :        :  :        +- *(4) Project [ss_customer_sk#60]
               :     :        :  :           +- *(4) BroadcastHashJoin [ss_sold_date_sk#57], [d_date_sk#80], Inner, BuildRight
               :     :        :  :              :- *(4) Project [ss_sold_date_sk#57, ss_customer_sk#60]
               :     :        :  :              :  +- *(4) Filter isnotnull(ss_sold_date_sk#57)
               :     :        :  :              :     +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#57,ss_customer_sk#60] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int>
               :     :        :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        :  :                 +- *(3) Project [d_date_sk#80]
               :     :        :  :                    +- *(3) Filter (((((isnotnull(d_moy#88) && isnotnull(d_year#86)) && (d_year#86 = 2002)) && (d_moy#88 >= 1)) && (d_moy#88 <= 4)) && isnotnull(d_date_sk#80))
               :     :        :  :                       +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#80,d_year#86,d_moy#88] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_year,2002), GreaterThanOrEqual(d_moy,1), LessThan..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :     :        :  +- *(8) Sort [ws_bill_customer_sk#112 ASC NULLS FIRST], false, 0
               :     :        :     +- Exchange hashpartitioning(ws_bill_customer_sk#112, 200)
               :     :        :        +- *(7) Project [ws_bill_customer_sk#112]
               :     :        :           +- *(7) BroadcastHashJoin [ws_sold_date_sk#108], [d_date_sk#80], Inner, BuildRight
               :     :        :              :- *(7) Project [ws_sold_date_sk#108, ws_bill_customer_sk#112]
               :     :        :              :  +- *(7) Filter isnotnull(ws_sold_date_sk#108)
               :     :        :              :     +- *(7) FileScan parquet tpcds.web_sales[ws_sold_date_sk#108,ws_bill_customer_sk#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int>
               :     :        :              +- ReusedExchange [d_date_sk#80], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(11) Sort [cs_ship_customer_sk#149 ASC NULLS FIRST], false, 0
               :     :           +- Exchange hashpartitioning(cs_ship_customer_sk#149, 200)
               :     :              +- *(10) Project [cs_ship_customer_sk#149]
               :     :                 +- *(10) BroadcastHashJoin [cs_sold_date_sk#142], [d_date_sk#80], Inner, BuildRight
               :     :                    :- *(10) Project [cs_sold_date_sk#142, cs_ship_customer_sk#149]
               :     :                    :  +- *(10) Filter isnotnull(cs_sold_date_sk#142)
               :     :                    :     +- *(10) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#142,cs_ship_customer_sk#149] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ship_customer_sk:int>
               :     :                    +- ReusedExchange [d_date_sk#80], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(12) Project [ca_address_sk#35]
               :           +- *(12) Filter (ca_county#42 IN (Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County) && isnotnull(ca_address_sk#35))
               :              +- *(12) FileScan parquet tpcds.customer_address[ca_address_sk#35,ca_county#42] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [In(ca_county, [Rush County,Toole County,Jefferson County,Dona Ana County,La Porte County]), IsNo..., ReadSchema: struct<ca_address_sk:int,ca_county:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(13) Project [cd_demo_sk#48, cd_gender#49, cd_marital_status#50, cd_education_status#51, cd_purchase_estimate#52, cd_credit_rating#53, cd_dep_count#54, cd_dep_employed_count#55, cd_dep_college_count#56]
                     +- *(13) Filter isnotnull(cd_demo_sk#48)
                        +- *(13) FileScan parquet tpcds.customer_demographics[cd_demo_sk#48,cd_gender#49,cd_marital_status#50,cd_education_status#51,cd_purchase_estimate#52,cd_credit_rating#53,cd_dep_count#54,cd_dep_employed_count#55,cd_dep_college_count#56] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string,cd_pur...
Time taken: 4.445 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 10 in stream 0 using template query10.tpl
------------------------------------------------------^^^

