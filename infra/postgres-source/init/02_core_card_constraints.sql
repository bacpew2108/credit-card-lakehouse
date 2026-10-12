-- Ràng buộc và index của core_card.transactions (Hình 1). Chạy lại nhiều lần được.
-- seed_source.py bỏ chúng trước khi nạp rồi chạy lại file này sau khi nạp xong.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_pkey') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_pkey PRIMARY KEY (id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_client_id_fkey') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_client_id_fkey
            FOREIGN KEY (client_id) REFERENCES customers (id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_card_id_fkey') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_card_id_fkey
            FOREIGN KEY (card_id) REFERENCES cards (id);
    END IF;
END $$;

-- dag_monthly trích xuất từng tháng theo khoảng date
CREATE INDEX IF NOT EXISTS transactions_date_idx ON transactions (date);
