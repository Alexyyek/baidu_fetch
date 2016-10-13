SELECT
city_code,
city_name,
district_id,
district_name,
bizcircle_id,
bizcircle_name,
resblock_id,
name
FROM
data_mining.resblock_merge_basic_day
WHERE
pt='${hiveconf:pt}'
AND city_code='${hiveconf:city_code}'
;
