Error in query: Detected implicit cartesian product for INNER join between logical plans
Aggregate [cs_call_center_sk#141], [cs_call_center_sk#141, sum(cs_ext_sales_price#153) AS sales#18, sum(cs_net_profit#163) AS profit#19]
+- Project [cs_call_center_sk#141, cs_ext_sales_price#153, cs_net_profit#163]
   +- Join Inner, (cs_sold_date_sk#130 = d_date_sk#49)
      :- Project [cs_sold_date_sk#130, cs_call_center_sk#141, cs_ext_sales_price#153, cs_net_profit#163]
      :  +- Filter isnotnull(cs_sold_date_sk#130)
      :     +- Relation[cs_sold_date_sk#130,cs_sold_time_sk#131,cs_ship_date_sk#132,cs_bill_customer_sk#133,cs_bill_cdemo_sk#134,cs_bill_hdemo_sk#135,cs_bill_addr_sk#136,cs_ship_customer_sk#137,cs_ship_cdemo_sk#138,cs_ship_hdemo_sk#139,cs_ship_addr_sk#140,cs_call_center_sk#141,cs_catalog_page_sk#142,cs_ship_mode_sk#143,cs_warehouse_sk#144,cs_item_sk#145,cs_promo_sk#146,cs_order_number#147,cs_quantity#148,cs_wholesale_cost#149,cs_list_price#150,cs_sales_price#151,cs_ext_discount_amt#152,cs_ext_sales_price#153,... 10 more fields] parquet
      +- Project [d_date_sk#49]
         +- Filter ((isnotnull(d_date#51) && ((d_date#51 >= 2000-08-23) && (d_date#51 <= 2000-09-22))) && isnotnull(d_date_sk#49))
            +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
and
Aggregate [cr_call_center_sk#177], [sum(cr_return_amount#184) AS returns#20, sum(cr_net_loss#192) AS profit_loss#21]
+- Project [cr_call_center_sk#177, cr_return_amount#184, cr_net_loss#192]
   +- Join Inner, (cr_returned_date_sk#166 = d_date_sk#49)
      :- Project [cr_returned_date_sk#166, cr_call_center_sk#177, cr_return_amount#184, cr_net_loss#192]
      :  +- Filter isnotnull(cr_returned_date_sk#166)
      :     +- Relation[cr_returned_date_sk#166,cr_returned_time_sk#167,cr_item_sk#168,cr_refunded_customer_sk#169,cr_refunded_cdemo_sk#170,cr_refunded_hdemo_sk#171,cr_refunded_addr_sk#172,cr_returning_customer_sk#173,cr_returning_cdemo_sk#174,cr_returning_hdemo_sk#175,cr_returning_addr_sk#176,cr_call_center_sk#177,cr_catalog_page_sk#178,cr_ship_mode_sk#179,cr_warehouse_sk#180,cr_reason_sk#181,cr_order_number#182,cr_return_quantity#183,cr_return_amount#184,cr_return_tax#185,cr_return_amt_inc_tax#186,cr_fee#187,cr_return_ship_cost#188,cr_refunded_cash#189,... 3 more fields] parquet
      +- Project [d_date_sk#49]
         +- Filter ((isnotnull(d_date#51) && ((d_date#51 >= 2000-08-23) && (d_date#51 <= 2000-09-22))) && isnotnull(d_date_sk#49))
            +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
Join condition is missing or trivial.
Either: use the CROSS JOIN syntax to allow cartesian products between these
relations, or: enable implicit cartesian products by setting the configuration
variable spark.sql.crossJoin.enabled=true;
