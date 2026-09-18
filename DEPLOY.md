# DEPLOY — Real Estate Valuation lên server Linux (cụm Spark phân tán)

> Hướng dẫn triển khai production: cụm **Spark standalone (master + nhiều worker)**,
> **Kafka**, **Postgres**, **MinIO (S3)** làm shared storage. Container hoá toàn bộ.
>
> Viết cho: người vận hành deploy trên máy Linux. Giả định biết Docker + SSH cơ bản.

---

## 0. Tại sao thiết kế thế này (đọc trước)

Code gốc chạy Spark `local[*]` + đọc/ghi thư mục parquet **local**. Muốn chạy
**phân tán thật** (executor ở máy/worker khác) thì mọi path phải nằm trên
**shared storage** — executor không thấy đĩa local của driver. Giải pháp:

| Tầng | Chứa gì | Ai truy cập | Đích prod |
|---|---|---|---|
| **LAKE** (`LAKE_ROOT`) | parquet: raw, pool, clean, features, scored_input, macro_raw, predictions, checkpoints | Spark r/w (executor) | `s3a://realestate-lake` (MinIO) |
| **MODEL_STORE** | `PipelineModel` binary + `feature_pipeline` | Spark save/load (executor) | `s3a://realestate-models` (MinIO) |
| **META_DIR** | `current.json`, `metrics.json`, `retrain_needed.json`, `impute_stats.json` | **chỉ driver**, đọc bằng `open()` | volume local `/app/models_meta` |
| **RAW_CSV** (`RAW_CSV_ROOT`) | CSV seed tĩnh (replay_producer) | Spark/python đọc `csv()` | `s3a://realestate-lake/raw_csv` |
| **Postgres** (`PG_*`) | bảng `predictions` (serving) + `macro_monthly` (CPI/lãi suất) | Spark JDBC r/w | container `postgres` |

**Vì sao META tách riêng, không lên S3:** mấy file JSON nhỏ này được đọc bằng
`json.load(open())` thuần Python — không nói được `s3a://`. Chỉ **driver** đụng
tới chúng nên để trên volume local là đủ và đơn giản. Model binary thì **executor
cần đọc** nên bắt buộc lên S3.

**Path & JAVA_HOME — tự động thích nghi Linux:** không còn hardcode. Tất cả đi qua
[ml/config.py](ml/config.py):
- `JAVA_HOME`: image set `ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`. Nếu chưa
  set, `config.py` tự dò (mac homebrew vs Linux OpenJDK). Path mac cũ chỉ còn là
  fallback dev — **không ảnh hưởng Linux**.
- `SPARK_MASTER`, các root path, endpoint S3, Kafka, Postgres: **đọc từ env** (`.env`).
- Mặc định env = hành vi local cũ → dev trên máy không đổi gì.

Không có Python UDF trong pipeline → **worker chạy JVM thuần**, không cần code Python.
Dù vậy vẫn dùng chung **1 image** cho master/worker/driver cho gọn.

---

## 1. Yêu cầu server

- Linux x86_64 (Ubuntu 22.04+ khuyến nghị).
- **Docker Engine 24+** và **Docker Compose v2** (`docker compose`, không phải `docker-compose`).
- Single-host: ≥ 8 GB RAM, 4 vCPU, 20 GB đĩa trống (data thật ~1160 dòng nên nhẹ; RAM
  chủ yếu cho JVM các container).
- Mở port ra ngoài nếu cần: `8080` (Spark UI), `9001` (MinIO console), `5432` (Postgres),
  `9000` (S3 API). **Production: chặn hết ở firewall, chỉ mở qua VPN/SSH tunnel.**

> ⚠️ **Quy mô:** stack Big Data này là **overkill** cho ~1160 dòng train (đúng như thiết
> kế đồ án — mục tiêu là *trình diễn công cụ phân tán*, không phải vì data lớn). Chạy ổn,
> nhưng đừng kỳ vọng tốc độ hơn pandas.

---

## 2. Chuẩn bị `.env` (secrets)

```bash
cd /opt/realestate            # thư mục chứa repo trên server
cp .env.example .env
# Sửa MỌI giá trị CHANGE_ME:
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  (dùng cả cho MinIO root và s3a)
#   PG_PASSWORD                                (đổi khỏi 'admin')
#   VNSTOCK_API_KEY                            (nếu có gói trả phí; để trống = community)
nano .env
chmod 600 .env                # chỉ owner đọc — tránh lộ trong log/ls
```

**Không bao giờ commit `.env`.** (`.gitignore` nên có `.env` — kiểm tra.)

`AWS_SECRET_ACCESS_KEY` tối thiểu 8 ký tự (MinIO bắt buộc).

---

## 3. Deploy single-host (mọi thứ trên 1 máy)

### 3.1 Build image
```bash
docker compose build
```
Bước build **pre-warm ivy cache**: tải sẵn jar `hadoop-aws`, `spark-sql-kafka`,
`postgresql` để runtime chạy offline.

> ⚠️ **Version jar PHẢI khớp Hadoop bundle của PySpark.** PySpark 4.2.0 nhúng
> `hadoop-client 3.5.0` → dùng `hadoop-aws:3.5.0` (đã đặt trong `.env` /
> `SPARK_JARS_PACKAGES`). **Nếu build lỗi "not found" cho `hadoop-aws:3.5.0`**, đổi
> sang `3.4.1` trong `.env` rồi build lại:
> ```
> SPARK_JARS_PACKAGES=org.apache.hadoop:hadoop-aws:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.4
> ```
> Kiểm tra version Hadoop thực tế:
> ```bash
> docker compose run --rm ml-app python -c "import pyspark,glob,os; print([os.path.basename(j) for j in glob.glob(os.path.join(os.path.dirname(pyspark.__file__),'jars','hadoop-client-*'))])"
> ```

### 3.2 Khởi động hạ tầng (MinIO, Kafka, Postgres, Spark master + workers)
```bash
docker compose up -d minio kafka postgres spark-master
docker compose up -d --scale spark-worker=2 spark-worker
```
`minio-init` tự chạy 1 lần: tạo bucket `realestate-lake`, `realestate-models` và
upload CSV seed lên `s3a://realestate-lake/raw_csv/`.

> **Postgres schema:** `sql/init.sql` (tạo bảng `predictions` + `macro_monthly`,
> seed CPI) chỉ chạy khi **volume mới**. Nếu `postgres_data` đã tồn tại (deploy
> cũ), áp thủ công 1 lần — idempotent:
> ```bash
> docker exec -i postgres_realestate psql -U "$PG_USER" -d "$PG_DB" < sql/init.sql
> ```
> **CPI/lãi suất** giờ nằm ở bảng `macro_monthly` (không còn CSV nhập tay).
> UI/nhập tay UPSERT bất kỳ lúc nào:
> ```sql
> INSERT INTO macro_monthly (month, cpi, rate) VALUES ('2026-02', 115.1, 4.5)
>   ON CONFLICT (month) DO UPDATE SET cpi = EXCLUDED.cpi, rate = EXCLUDED.rate;
> ```
> M6/M8 (`feature_pipeline`, `score_new`) nay đọc bảng này qua JDBC → **cần
> Postgres up trước khi chạy** (compose `ml-app` đã `depends_on` postgres healthy).

Kiểm tra:
```bash
docker compose ps                      # tất cả healthy
open http://<server>:8080              # Spark master UI: thấy 2 ALIVE workers
open http://<server>:9001              # MinIO console (login = AWS_ACCESS_KEY_ID/SECRET)
```

### 3.3 Nạp dữ liệu + model có sẵn lên MinIO (one-time bootstrap)

Pipeline cần **model đã train** (`current.json` + `v<ver>/model`) để score. Có 2 đường:

**A. Đã có model local (`models/`) — migrate lên:**
```bash
# Cài mc (MinIO client) local hoặc dùng container mc:
alias mc='docker run --rm --network realestate-net -v $PWD:/w -w /w \
  --env-file .env minio/mc:RELEASE.2024-10-08T09-37-26Z'
mc alias set m http://minio:9000 "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"

# 1) parquet lake (nếu muốn giữ pool/clean/features cũ) + raw_csv:
mc mirror data/lake m/realestate-lake/
# 2) model binary + feature_pipeline -> bucket models:
mc mirror --exclude "*.json" models/ m/realestate-models/

# 3) JSON điều khiển -> volume models_meta (driver-local):
docker compose run --rm -v "$PWD/models:/src:ro" ml-app \
  bash -c 'cp /src/current.json /src/impute_stats.json /app/models_meta/ 2>/dev/null; \
           for d in /src/v*; do mkdir -p /app/models_meta/$(basename $d); \
           cp $d/metrics.json /app/models_meta/$(basename $d)/ 2>/dev/null; done; \
           ls -R /app/models_meta'
```

**B. Chưa có model / muốn train sạch trên server:** chạy retrain trước (mục 4).
Lần đầu chưa có `current.json` → `promote.py` promote vô điều kiện.

### 3.4 Chạy pipeline hằng ngày (thủ công 1 lần để verify)
```bash
mkdir -p logs
docker compose run --rm ml-app bash run_daily.sh 2>&1 | tee logs/first-run.log
```
Kỳ vọng: fetch_macro → replay→Kafka → kafka_to_parquet → clean → impute → score
(ghi cờ drift) → (retrain nếu drift) → nạp Postgres.

Kiểm tra kết quả:
```bash
docker compose exec postgres psql -U "$PG_USER" -d "$PG_DB" \
  -c "SELECT model_version, count(*), max(scored_at) FROM predictions GROUP BY 1;"
mc ls m/realestate-lake/predictions/sale/
```

### 3.5 Lịch cron (host)
```bash
crontab -e
# 2h sáng mỗi ngày:
0 2 * * *  cd /opt/realestate && docker compose run --rm ml-app bash run_daily.sh >> logs/daily.log 2>&1
```

---

## 4. Chạy retrain thủ công (khi cần train lại toàn bộ pool)
```bash
docker compose run --rm ml-app bash ml/retrain.sh
```
Gate promote-if-better: model mới chỉ thay prod nếu RMSE ≤ model cũ (xem
[ml/promote.py](ml/promote.py)). Con trỏ prod = `current.json` trong `models_meta`.

---

## 5. Multi-node thật (worker ở máy Linux khác)

Compose ở trên là **single-host cluster**. Để worker chạy trên máy vật lý khác:

1. **Node master** (máy A) chạy: `minio`, `kafka`, `postgres`, `spark-master`, `ml-app`.
   Đảm bảo các máy khác truy cập được master qua IP (mở `7077`, `9000`, `9092`, `5432`).
2. **Node worker** (máy B, C): cài Docker, copy repo + `.env`, build image, rồi chạy
   **chỉ** worker, trỏ về IP master:
   ```bash
   # trên máy B/C, sửa .env: SPARK_MASTER=spark://<IP-master>:7077
   docker compose build spark-worker
   docker run -d --env-file .env --name spark-worker realestate-ml:latest \
     bash -c 'exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
       spark://<IP-master>:7077 --cores 4 --memory 4g'
   ```
3. **Quan trọng:** vì storage là MinIO/S3 (`s3a://`), executor ở máy B/C đọc/ghi được lake
   qua mạng — **không cần NFS/đĩa chung**. `S3_ENDPOINT` trong `.env` của máy B/C phải trỏ
   `http://<IP-master>:9000` (không phải `minio:9000` vì khác Docker network).
4. `KAFKA_BOOTSTRAP_SERVERS`, `PG_HOST` trên máy worker cũng đổi thành IP master (nếu worker
   cần — thường executor chỉ đụng S3, còn Kafka/PG là ở driver).

> Production đa máy nên dùng TLS cho MinIO (`S3_SSL_ENABLED=true`) + SASL cho Kafka +
> mạng riêng. Ngoài phạm vi hướng dẫn này.

---

## 6. Vận hành & kiểm tra

| Việc | Lệnh |
|---|---|
| Log 1 lần chạy | `docker compose run --rm ml-app bash run_daily.sh` |
| Spark cluster UI | `http://<server>:8080` |
| Xem predictions | `psql ... SELECT * FROM predictions ORDER BY scored_at DESC LIMIT 20;` |
| Cờ drift gần nhất | `docker compose run --rm ml-app cat /app/models_meta/retrain_needed.json` |
| Model prod hiện tại | `docker compose run --rm ml-app cat /app/models_meta/current.json` |
| Scale worker | `docker compose up -d --scale spark-worker=3 spark-worker` |
| Dừng tất cả | `docker compose down` (giữ data trong volume) |
| Xoá sạch (kể cả data!) | `docker compose down -v` ⚠️ |

---

## 7. Lưu ý / cạm bẫy đã biết (đọc kỹ trước khi lên prod)

1. **Python 3.11 trong container, KHÔNG 3.14.** `.venv` dev là 3.14; image chuẩn hoá 3.11
   cho ổn định + tương thích pyspark 4.2. Deps pin trong [requirements.txt](requirements.txt).
2. **Version `hadoop-aws` = version Hadoop bundle của PySpark** (mục 3.1). Đây là điểm dễ
   vỡ nhất — nếu s3a báo `ClassNotFound`/`NoSuchMethod` thì là lệch version.
3. **s3a checkpoint cho Kafka streaming** ([kafka_to_parquet.py](streaming/kafka_to_parquet.py)):
   S3 không có atomic rename → checkpoint kém tin cậy hơn HDFS. Với `trigger(availableNow)` +
   lưu lượng nhỏ thì chấp nhận được. Nếu gặp lỗi checkpoint, chuyển `CHECKPOINT` sang volume local.
4. **Kafka advertised listener = `kafka:9092`** (trong Docker network). Client ngoài host
   dùng `localhost:9092` sẽ không kết nối được — chạy producer trong network (như ml-app).
5. **Đổi mật khẩu mặc định.** `.env.example` dùng `admin`/placeholder — bắt buộc đổi
   `PG_PASSWORD` và MinIO creds trước khi mở ra mạng.
6. **`fetch_macro` idempotency check** dùng `os.path.isdir` — trên s3a luôn False nên sẽ
   fetch + overwrite mỗi ngày (vô hại, `load_macro` dedup theo date). Không phải lỗi.
7. **Bootstrap model bắt buộc** (mục 3.3): score cần `current.json` + model. Server mới toanh
   phải migrate model cũ HOẶC chạy `retrain.sh` trước lần score đầu.

---

## 8. Tóm tắt file đã thêm/sửa cho deploy

**Thêm mới:** [ml/config.py](ml/config.py) (resolver Spark+storage), [Dockerfile](Dockerfile),
[.env.example](.env.example), [.dockerignore](.dockerignore),
[requirements.txt](requirements.txt), DEPLOY.md (file này).

**Sửa:** [docker-compose.yml](docker-compose.yml) (+minio, +spark master/worker, +ml-app),
[run_daily.sh](run_daily.sh) & [ml/retrain.sh](ml/retrain.sh) (env roots, bật Kafka),
và 10 file Spark/streaming (`clean_parse, impute_fit, impute_apply, feature_pipeline,
train, score_new, promote, fetch_macro, load_predictions, kafka_to_parquet, replay_producer`)
— thay hardcode path/master/JAVA_HOME bằng `config`. **Mặc định env giữ nguyên hành vi local.**
