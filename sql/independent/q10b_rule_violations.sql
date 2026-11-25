SELECT
    rule_id,
    severity,
    count(*) AS months,
    count_if(failed > 0) AS months_violated,
    sum(failed) AS violations,
    count_if(status = 'BLOCK') AS months_block
FROM dq_month
GROUP BY rule_id, severity
ORDER BY violations DESC, rule_id
