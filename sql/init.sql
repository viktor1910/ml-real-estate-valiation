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
