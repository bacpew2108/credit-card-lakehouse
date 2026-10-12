-- Hệ thống nguồn giả lập core_card (mục 3.3, Hình 1 Design Doc).
-- Chạy lại nhiều lần được: docker-entrypoint chỉ chạy file này khi volume còn trống,
-- nên generators/seed_source.py cũng thực thi lại nó trước khi nạp.
--
-- Mô phỏng một hệ thống cũ: tiền và ngày MM/YYYY lưu dạng chuỗi, zip lưu như '58523.0'.
-- Việc chuẩn hóa thuộc về lớp Silver.
-- Khóa chính, khóa ngoại và index của transactions nằm ở 02_core_card_constraints.sql
-- để seed bỏ ra khi nạp 13,3 triệu dòng rồi tạo lại (nhanh hơn kiểm tra từng dòng).

CREATE TABLE IF NOT EXISTS customers (
    id                 INT PRIMARY KEY,
    current_age        INT,
    retirement_age     INT,
    birth_year         INT,
    birth_month        INT,
    gender             VARCHAR(16),
    address            VARCHAR(128),
    latitude           NUMERIC(9, 4),
    longitude          NUMERIC(9, 4),
    per_capita_income  VARCHAR(16),   -- '$29278'
    yearly_income      VARCHAR(16),
    total_debt         VARCHAR(16),
    credit_score       INT,
    num_credit_cards   INT,
    updated_at         TIMESTAMP NOT NULL DEFAULT now()
);

-- card_number, cvv là dữ liệu nhạy cảm: chỉ tồn tại ở nguồn.
-- extract_jdbc (tuần 4) token hóa card_number bằng HMAC và loại cvv.
CREATE TABLE IF NOT EXISTS cards (
    id                     INT PRIMARY KEY,
    client_id              INT NOT NULL REFERENCES customers (id),
    card_brand             VARCHAR(32),
    card_type              VARCHAR(32),   -- có 'Debit (Prepaid)'
    card_number            VARCHAR(19),   -- PAN
    expires                VARCHAR(7),    -- MM/YYYY
    cvv                    VARCHAR(4),
    has_chip               VARCHAR(3),    -- YES / NO
    num_cards_issued       INT,
    credit_limit           VARCHAR(16),   -- '$24295'
    acct_open_date         VARCHAR(7),    -- MM/YYYY
    year_pin_last_changed  INT,
    updated_at             TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cards_client_id_idx ON cards (client_id);

CREATE TABLE IF NOT EXISTS transactions (
    id              INT NOT NULL,
    date            TIMESTAMP NOT NULL,
    client_id       INT,
    card_id         INT,
    amount          VARCHAR(32),   -- '$-77.00'
    use_chip        VARCHAR(32),   -- 'Swipe Transaction' / 'Chip Transaction' / 'Online Transaction'
    merchant_id     INT,
    merchant_city   VARCHAR(64),
    merchant_state  VARCHAR(64),   -- mã bang hoặc tên quốc gia
    zip             VARCHAR(16),   -- '58523.0'
    mcc             INT,
    errors          VARCHAR(128),  -- nhiều lỗi ghép bằng dấu phẩy
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);
