-- Bảng dùng chung cho tính toán độc lập: đọc thẳng file gốc (data/raw), tự làm sạch theo định nghĩa
-- trong docs/data_model.md, không dùng code hay dữ liệu của Bronze, Silver, Gold. run_queries.py thay
-- {data_dir}, {mcc_groups} và nạp sẵn bảng mcc(mcc_code, description) từ mcc_codes.json.

-- Giao dịch. TRY_CAST: giá trị không parse được thành NULL, để R01, R02 đếm được.
CREATE TABLE tx AS
SELECT
    TRY_CAST(id AS BIGINT) AS id,
    TRY_CAST(date AS TIMESTAMP) AS ts,
    TRY_CAST(client_id AS INTEGER) AS client_id,
    TRY_CAST(card_id AS INTEGER) AS card_id,
    TRY_CAST(replace(amount, '$', '') AS DECIMAL(12, 2)) AS amount,
    CASE use_chip
        WHEN 'Swipe Transaction' THEN 'Swipe'
        WHEN 'Chip Transaction' THEN 'Chip'
        WHEN 'Online Transaction' THEN 'Online'
        ELSE use_chip
    END AS channel,
    merchant_city,
    merchant_state,
    TRY_CAST(mcc AS INTEGER) AS mcc,
    errors,
    coalesce(merchant_city = 'ONLINE', false) AS loc_online,
    coalesce(merchant_state IS NOT NULL AND NOT regexp_full_match(merchant_state, '[A-Z]{2}'), false) AS loc_foreign
FROM read_csv('{data_dir}/transactions_data.csv', header = true, all_varchar = true, quote = '"');

ALTER TABLE tx ADD COLUMN month DATE;
UPDATE tx SET month = CAST(date_trunc('month', ts) AS DATE);

CREATE TABLE customers AS
SELECT
    CAST(id AS INTEGER) AS customer_id,
    CAST(birth_year AS INTEGER) AS birth_year,
    CAST(replace(yearly_income, '$', '') AS DECIMAL(12, 2)) AS yearly_income,
    CAST(credit_score AS INTEGER) AS credit_score
FROM read_csv('{data_dir}/users_data.csv', header = true, all_varchar = true);

-- acct_open_date, expires dạng MM/YYYY: ngày đầu tháng mở thẻ, ngày cuối tháng hết hạn
CREATE TABLE cards AS
SELECT
    CAST(id AS INTEGER) AS card_id,
    CAST(client_id AS INTEGER) AS customer_id,
    card_brand,
    CASE WHEN card_type = 'Debit (Prepaid)' THEN 'Prepaid' ELSE card_type END AS card_type,
    has_chip = 'YES' AS has_chip,
    CAST(replace(credit_limit, '$', '') AS DECIMAL(12, 2)) AS credit_limit,
    make_date(CAST(split_part(acct_open_date, '/', 2) AS INTEGER),
              CAST(split_part(acct_open_date, '/', 1) AS INTEGER), 1) AS open_date,
    last_day(make_date(CAST(split_part(expires, '/', 2) AS INTEGER),
                       CAST(split_part(expires, '/', 1) AS INTEGER), 1)) AS expires_date
FROM read_csv('{data_dir}/cards_data.csv', header = true, all_varchar = true);

CREATE TABLE mcc_groups AS
SELECT CAST(mcc AS INTEGER) AS mcc_code, mcc_group
FROM read_csv('{mcc_groups}', header = true, all_varchar = true);

-- Giao dịch hợp lệ (vào Silver): bỏ dòng vi phạm R01 (id rỗng, trùng: giữ dòng sớm nhất), R02
-- (amount, date không parse được). R03 (ngoài tháng của batch) không xảy ra vì batch chính là tháng
-- của date.
CREATE TABLE tx_ok AS
WITH ranked AS (
    SELECT *, row_number() OVER (PARTITION BY id ORDER BY ts) AS rn FROM tx
)
SELECT * EXCLUDE (rn) FROM ranked
WHERE id IS NOT NULL AND rn = 1 AND ts IS NOT NULL AND amount IS NOT NULL;

-- Vi phạm từng rule theo tháng, tính độc lập theo mô tả rule ở mục 6 Design Doc
-- (batch_id, rule_id, severity, checked, failed). R10.1, R10.2, R11 là đối soát giữa các tầng, không
-- có trên file gốc: ghi failed = 0, điều kiện đó được kiểm ở q10a (số dòng nạp và số dòng fact mỗi
-- tháng phải bằng số dòng của file gốc).
CREATE TABLE dq_month AS
WITH j AS (
    SELECT
        t.month,
        t.channel,
        t.loc_online,
        CAST(t.ts AS DATE) AS day,
        t.client_id,
        c.customer_id AS cust,
        k.card_id AS card,
        k.customer_id AS card_customer,
        k.has_chip,
        k.open_date,
        k.expires_date,
        m.mcc_code AS mcc
    FROM tx_ok AS t
    LEFT JOIN customers AS c ON t.client_id = c.customer_id
    LEFT JOIN cards AS k ON t.card_id = k.card_id
    LEFT JOIN mcc AS m ON t.mcc = m.mcc_code
),
per_record AS (
    SELECT
        month,
        count(*) AS checked,
        count_if(cust IS NULL OR card IS NULL) AS r04,
        count_if(card IS NOT NULL AND card_customer IS DISTINCT FROM client_id) AS r05,
        count_if(mcc IS NULL) AS r06,
        count_if(channel NOT IN ('Swipe', 'Chip', 'Online') OR channel IS NULL
                 OR (channel = 'Online' AND NOT loc_online)
                 OR (channel <> 'Online' AND loc_online)) AS r07,
        count_if(channel = 'Chip' AND NOT has_chip) AS r08,
        count_if(day < open_date OR day > expires_date) AS r14
    FROM j
    GROUP BY month
),
raw_month AS (
    SELECT
        month,
        count(*) AS n_raw,
        count_if(id IS NULL OR rn > 1) AS r01,
        count_if(id IS NOT NULL AND rn = 1 AND (ts IS NULL OR amount IS NULL)) AS r02
    FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY ts) AS rn FROM tx)
    GROUP BY month
),
-- R12: số giao dịch, tổng tiền của tháng lệch khỏi trung bình 12 tháng liền trước quá 3 độ lệch
-- chuẩn (mẫu); chưa đủ 12 tháng thì không xét (checked = 0)
totals AS (
    SELECT month, count(*) AS n, CAST(sum(amount) AS DOUBLE) AS amt FROM tx_ok GROUP BY month
),
r12 AS (
    SELECT
        month, n, amt,
        count(*) OVER w AS k,
        avg(n) OVER w AS mu_n, stddev_samp(n) OVER w AS sd_n,
        avg(amt) OVER w AS mu_a, stddev_samp(amt) OVER w AS sd_a
    FROM totals
    WINDOW w AS (ORDER BY month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING)
),
long AS (
    SELECT month, rule_id, checked, failed
    FROM per_record
    CROSS JOIN LATERAL (VALUES ('R04', r04), ('R05', r05), ('R06', r06), ('R07', r07), ('R08', r08), ('R14', r14))
        AS v (rule_id, failed)
    UNION ALL
    SELECT month, rule_id, n_raw, failed
    FROM raw_month
    CROSS JOIN LATERAL (VALUES ('R01', r01), ('R02', r02), ('R03', 0)) AS v (rule_id, failed)
    UNION ALL
    SELECT month, 'R12', CASE WHEN k = 12 THEN 1 ELSE 0 END,
           CASE WHEN k = 12 AND (abs(n - mu_n) > 3 * sd_n OR abs(amt - mu_a) > 3 * sd_a) THEN 1 ELSE 0 END
    FROM r12
    UNION ALL
    SELECT month, rule_id, 1, 0
    FROM totals CROSS JOIN (VALUES ('R10.1'), ('R10.2'), ('R11')) AS v (rule_id)
)
SELECT
    strftime(month, '%Y-%m') AS batch_id,
    rule_id,
    CASE WHEN rule_id IN ('R01', 'R02', 'R03', 'R10.1', 'R10.2', 'R11') THEN 'BLOCK' ELSE 'WARN' END AS severity,
    checked,
    failed,
    -- Trạng thái theo quy ước dq_results: rule xét từng bản ghi BLOCK khi tỷ lệ vi phạm > 1%
    CASE
        WHEN failed = 0 THEN 'PASS'
        WHEN rule_id IN ('R10.1', 'R10.2', 'R11') THEN 'BLOCK'
        WHEN rule_id = 'R12' THEN 'WARN'
        WHEN failed / checked > 0.01 THEN 'BLOCK'
        ELSE 'WARN'
    END AS status
FROM long;
