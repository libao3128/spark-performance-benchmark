SELECT d_date, 
       make_date(2022, d_moy, d_dom) AS new_date_format,
       timestamp_millis(unix_timestamp()) AS ts_millis
FROM date_dim
LIMIT 10;
