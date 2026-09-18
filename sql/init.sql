-- DDL placeholder — schema đầy đủ sẽ được viết ở M9
-- Postgres tự chạy file này khi container khởi tạo lần đầu

CREATE TABLE IF NOT EXISTS predictions (
    id                   BIGINT,
    district             TEXT,
    area_m2              DOUBLE PRECISION,
    listing_ppm2         DOUBLE PRECISION,   -- triệu VND/m²
    predicted_ppm2       DOUBLE PRECISION,   -- triệu VND/m²
    predicted_total_vnd  BIGINT,             -- VND
    undervalued_ratio    DOUBLE PRECISION,
    is_undervalued       BOOLEAN,
    url                  TEXT,
    model_version        TEXT,
    scored_at            TIMESTAMP
);

-- Vĩ mô theo tháng (CPI + lãi suất). THAY cho cpi_rate_monthly.csv nhập tay.
-- month = 'YYYY-MM' làm PRIMARY KEY -> UI/nhập tay UPSERT 1 tháng bất kỳ:
--   INSERT INTO macro_monthly (month, cpi, rate) VALUES ('2026-02', 115.1, 4.5)
--   ON CONFLICT (month) DO UPDATE SET cpi = EXCLUDED.cpi, rate = EXCLUDED.rate;
-- macro_features.load_macro() đọc bảng này qua JDBC (không còn đọc CSV).
CREATE TABLE IF NOT EXISTS macro_monthly (
    month  TEXT PRIMARY KEY,          -- 'YYYY-MM'
    cpi    DOUBLE PRECISION NOT NULL, -- chỉ số CPI
    rate   DOUBLE PRECISION NOT NULL  -- lãi suất %/năm
);

-- Người dùng được phép đăng nhập dashboard. Tunnel công khai -> phải chặn
-- người lạ nhập CPI gây nhiễu. password_hash = bcrypt (KHÔNG lưu plaintext).
-- Tạo user bằng: python dashboard/manage_users.py <username>
-- (script tự hash bcrypt rồi UPSERT — không seed sẵn ở đây để tránh hash cứng).
CREATE TABLE IF NOT EXISTS users (
    username       TEXT PRIMARY KEY,
    password_hash  TEXT NOT NULL,             -- bcrypt hash
    created_at     TIMESTAMP DEFAULT now()
);

-- Seed = nội dung cpi_rate_monthly.csv cũ (giữ nguyên hành vi lần chạy đầu).
INSERT INTO macro_monthly (month, cpi, rate) VALUES
    ('2025-01', 110.20, 4.50),
    ('2025-02', 110.55, 4.50),
    ('2025-03', 110.90, 4.50),
    ('2025-04', 111.30, 4.50),
    ('2025-05', 111.65, 4.50),
    ('2025-06', 112.00, 4.50),
    ('2025-07', 112.40, 4.50),
    ('2025-08', 112.75, 4.50),
    ('2025-09', 113.10, 4.50),
    ('2025-10', 113.50, 4.50),
    ('2025-11', 113.90, 4.50),
    ('2025-12', 114.30, 4.50),
    ('2026-01', 114.70, 4.50)
ON CONFLICT (month) DO NOTHING;
