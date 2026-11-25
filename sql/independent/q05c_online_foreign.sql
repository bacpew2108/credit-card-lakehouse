SELECT
    year(month) AS year,
    count(*) AS txn_count,
    count_if(loc_online) AS online_count,
    round(100 * CAST(count_if(loc_online) AS DOUBLE) / count(*), 2) AS pct_online,
    count_if(loc_foreign) AS foreign_count,
    round(100 * CAST(count_if(loc_foreign) AS DOUBLE) / count(*), 3) AS pct_foreign
FROM tx_ok
GROUP BY year(month)
ORDER BY year
