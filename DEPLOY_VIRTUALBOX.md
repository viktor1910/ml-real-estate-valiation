# Deploy cụm Spark phân tán trên VirtualBox (demo)

Runbook dựng hệ thống Real Estate Valuation (Spark master + 2 worker + MinIO +
Kafka + Postgres + dashboard) trên **3 máy ảo VirtualBox** trong 1 máy Windows.
Mục tiêu: chứng minh Spark chạy **phân tán trên nhiều máy vật lý** + web dashboard
public qua Cloudflare.

> Máy Windows dùng thử: 16GB RAM, chừa 8GB cho host → **8GB cấp cho 3 VM**.
> Nếu máy khoẻ hơn thì tăng RAM tương ứng.

---

## 1. Sơ đồ & phân bổ

```
Windows host
 └─ VirtualBox host-only net 192.168.56.0/24
     ├─ vm-master  192.168.56.10  RAM 4GB  MinIO+Kafka+Postgres+Spark master+driver+web
     ├─ vm-worker1 192.168.56.11  RAM 2GB  1 Spark worker
     └─ vm-worker2 192.168.56.12  RAM 2GB  1 Spark worker
```

- **2 worker trên 2 máy khác nhau** → dashboard `distinct_hosts=2` → banner "phân tán ✅".
- vm-master gánh nặng (dịch vụ + driver + web) nên **không chạy worker**.
- Spark roles (worker, driver) chạy `--network host` để quảng bá **IP host-only thật**
  (không phải IP nội bộ Docker 172.x) — bắt buộc cho multi-VM, nếu không executor
  không kết nối ngược về driver được.

---

## 2. Chuẩn bị

- Cài **Oracle VirtualBox** trên Windows.
- Tải **Ubuntu Server 22.04 LTS — amd64** (`ubuntu-22.04.x-live-server-amd64.iso`).
  Dùng bản **Server** (không GUI, tiết kiệm RAM). Chọn **amd64** (Intel/AMD).

### 2.1. Host-only network
VirtualBox → **File → Tools → Network Manager → Host-only Networks → Create**:
- IPv4: `192.168.56.1`, Mask `255.255.255.0`
- Tab **DHCP Server: tắt** (gán IP tĩnh tay).

---

## 3. Tạo vm-master

### 3.1. New VM
- Name `vm-master`, ISO = file Ubuntu Server, **tick "Skip Unattended Installation"**.
- RAM **4096 MB**, CPU **2**, Disk **40 GB** (dynamically allocated).
- **Settings → Network**:
  - Adapter 1: **NAT** (internet để cài đặt).
  - Adapter 2: **Host-only Adapter** → chọn net `192.168.56.1`.

### 3.2. Cài Ubuntu
Start VM → **Try or Install Ubuntu Server**:
- Network: card NAT (`enp0s3`) để DHCP; card host-only (`enp0s8`) để mặc định (cấu hình IP sau).
- Storage: **Use entire disk**.
- Hostname: `vm-master`.
- ✅ **Tick "Install OpenSSH server"**.
- Cài xong → Reboot.

### 3.3. IP tĩnh host-only + SSH
Login console, gán IP `.10` cho `enp0s8`:
```bash
printf 'network:\n  version: 2\n  ethernets:\n    enp0s8:\n      dhcp4: false\n      addresses: [192.168.56.10/24]\n' | sudo tee /etc/netplan/99-hostonly.yaml
sudo chmod 600 /etc/netplan/99-hostonly.yaml
sudo netplan apply
ip a show enp0s8      # phải thấy 192.168.56.10/24, state UP
```
(Cảnh báo `Open vSwitch` bỏ qua.)

Từ Windows PowerShell — SSH vào (paste được):
```powershell
ssh vboxuser@192.168.56.10
```

---

## 4. Cài Docker (vm-master, trong SSH)
```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg git
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
```
`exit` rồi SSH lại (để group docker ăn), test: `docker run --rm hello-world`.

---

## 5. Lấy code + cấu hình cho môi trường amd64 / multi-VM

```bash
cd ~
git clone https://github.com/viktor1910/ml-real-estate-valiation.git
cd ml-real-estate-valiation
git checkout deploy/windown          # nhánh deploy
```

### 5.1. Điều chỉnh docker-compose cho amd64 + multi-VM
```bash
# amd64 (repo pin arm64 cho máy Apple Silicon)
sed -i 's|linux/arm64|linux/amd64|g' docker-compose.yml

# MinIO đã rời Docker Hub -> lấy từ quay.io
sed -i 's|image: minio/minio:.*|image: quay.io/minio/minio:latest|' docker-compose.yml
sed -i 's|image: minio/mc:.*|image: quay.io/minio/mc:latest|' docker-compose.yml

# Kafka advertise bằng IP master (client/executor máy xa mới resolve được)
sed -i 's|PLAINTEXT://kafka:9092|PLAINTEXT://192.168.56.10:9092|' docker-compose.yml
```

### 5.2. Tạo .env (endpoint trỏ IP master, đổi mật khẩu mặc định)
```bash
cp .env.example .env
sed -i 's|^S3_ENDPOINT=.*|S3_ENDPOINT=http://192.168.56.10:9000|' .env
sed -i 's|^KAFKA_BOOTSTRAP_SERVERS=.*|KAFKA_BOOTSTRAP_SERVERS=192.168.56.10:9092|' .env
sed -i 's|^PG_HOST=.*|PG_HOST=192.168.56.10|' .env
sed -i 's|^SPARK_MASTER=.*|SPARK_MASTER=spark://192.168.56.10:7077|' .env
sed -i 's|^AWS_ACCESS_KEY_ID=.*|AWS_ACCESS_KEY_ID=minioadmin|' .env
sed -i 's|^AWS_SECRET_ACCESS_KEY=.*|AWS_SECRET_ACCESS_KEY=minioadmin123|' .env
sed -i 's|^PG_PASSWORD=.*|PG_PASSWORD=doi_mat_khau_manh|' .env
chmod 600 .env
```

---

## 6. Build image (vm-master, ~vài chục phút — tải Spark jars)
```bash
docker compose build spark-master
docker images | grep realestate-ml       # phải có realestate-ml:latest
```
(master/worker/driver dùng chung 1 image → build 1 lần.)

---

## 7. Clone ra 2 worker VM

`sudo poweroff` vm-master. Trong VirtualBox chuột phải `vm-master` → **Clone**:
- **Full Clone**, MAC Address Policy = **Generate new MAC addresses for all adapters**.
- Tạo `vm-worker1` và `vm-worker2`.
- RAM mỗi worker → **2048 MB**.
- Xoá port-forward (nếu có) ở clone để tránh trùng.

Start từng worker, đổi hostname + IP:

**vm-worker1:**
```bash
sudo hostnamectl set-hostname vm-worker1
sudo sed -i 's|192.168.56.10/24|192.168.56.11/24|' /etc/netplan/99-hostonly.yaml
sudo netplan apply
```
**vm-worker2:**
```bash
sudo hostnamectl set-hostname vm-worker2
sudo sed -i 's|192.168.56.10/24|192.168.56.12/24|' /etc/netplan/99-hostonly.yaml
sudo netplan apply
```

Start lại vm-master. Kiểm tra 3 VM thông nhau (từ master):
```bash
ping -c2 192.168.56.11 && ping -c2 192.168.56.12
```

---

## 8. Khởi động dịch vụ + Spark master (vm-master)
```bash
cd ~/ml-real-estate-valiation
docker compose up -d minio minio-init kafka postgres spark-master
docker compose ps                        # 4 dịch vụ Up (healthy), minio-init Exited(0)
curl -s http://localhost:8080/json/ | head -c 200   # JSON master, workers:[]
```

---

## 9. Khởi động 2 worker (`--network host`)

SSH sang **mỗi** worker (`ssh vboxuser@192.168.56.11` và `.12`), chạy:
```bash
docker run -d --name spark-worker --restart unless-stopped \
  --network host --env-file ~/ml-real-estate-valiation/.env \
  -e SPARK_LOCAL_IP=$(hostname -I | awk '{print $2}') \
  -e SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=300" \
  realestate-ml:latest \
  bash -c 'exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://192.168.56.10:7077 --cores 2 --memory 1g --host $SPARK_LOCAL_IP'
```

Verify (master): cần `aliveworkers: 2`, host `.11` + `.12`:
```bash
curl -s http://localhost:8080/json/ | grep -E '"aliveworkers"|"host"'
```

Chứng minh phân tán nhanh (task chạy trên cả 2 máy):
```bash
cat > /tmp/distproof.py <<'PY'
import os
from pyspark.sql import SparkSession
s = SparkSession.builder.appName("distproof").getOrCreate()
hosts = s.sparkContext.parallelize(range(200), 50)\
    .map(lambda _: os.environ.get("SPARK_LOCAL_IP", "?")).distinct().collect()
print("EXECUTOR_HOSTS:", sorted(hosts)); s.stop()
PY
docker run --rm --network host --env-file .env -e SPARK_LOCAL_IP=192.168.56.10 \
  -v /tmp/distproof.py:/tmp/distproof.py realestate-ml:latest \
  bash -c 'exec $SPARK_HOME/bin/spark-submit --master spark://192.168.56.10:7077 \
    --conf spark.driver.host=192.168.56.10 --conf spark.cores.max=4 --executor-memory 512m \
    /tmp/distproof.py' 2>&1 | grep EXECUTOR_HOSTS
# -> EXECUTOR_HOSTS: ['192.168.56.11', '192.168.56.12']
```

---

## 10. Bootstrap dữ liệu + train model (vm-master)

Cụm mới chưa có model → nạp data seed vào lake rồi train **trên cụm**. Driver
`--network host` để executor máy xa kết nối ngược về:
```bash
cd ~/ml-real-estate-valiation
docker run --rm --network host --env-file .env -e SPARK_LOCAL_IP=192.168.56.10 \
  -v re_meta:/app/models_meta \
  -v $PWD/data/raw_csv:/app/data/raw_csv:ro \
  realestate-ml:latest bash -c '
set -e
python ml/fetch_macro.py || echo "macro warn"
python streaming/replay_producer.py --csv data/raw_csv/chotot_bds_data.csv
python streaming/kafka_to_parquet.py
python ml/clean_parse.py --src $LAKE_ROOT/listings_raw --dt $(date +%F)
bash ml/retrain.sh
echo "== BOOTSTRAP DONE =="
'
```
Kết thúc thấy `PROMOTE (lần đầu): ... -> current.json` = model đã sẵn sàng.

### 10.1. Score + nạp dự đoán vào Postgres (cho dashboard có data)
```bash
docker run --rm --network host --env-file .env -e SPARK_LOCAL_IP=192.168.56.10 \
  -v re_meta:/app/models_meta \
  realestate-ml:latest bash -c '
set -e
D=$(date +%F)
python ml/impute_apply.py --src $LAKE_ROOT/listings_pool/dt=$D --out $LAKE_ROOT/scored_input --mode serve
SCORE_NEW_BATCH=$LAKE_ROOT/scored_input python ml/score_new.py
python ml/load_predictions.py
'
docker exec -it postgres_realestate psql -U admin -d realestate -c "SELECT count(*) FROM predictions;"
```

---

## 11. Web dashboard (vm-master)

Chạy Streamlit bằng venv host (nhẹ, không cần pyspark):
```bash
sudo apt-get install -y python3-venv python3-pip
python3 -m venv ~/dash-venv
~/dash-venv/bin/pip install -U pip
~/dash-venv/bin/pip install streamlit bcrypt psycopg2-binary pandas
```

Tạo user đăng nhập (dashboard có auth, public bắt buộc):
```bash
cd ~/ml-real-estate-valiation
set -a; source .env; set +a
~/dash-venv/bin/python dashboard/manage_users.py admin      # nhập password >=8
```

Chạy thử + test từ Windows browser `http://192.168.56.10:8501`:
```bash
cd ~/ml-real-estate-valiation/dashboard
set -a; source ../.env; set +a
~/dash-venv/bin/streamlit run Home.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
```

---

## 12. Public ra internet (Cloudflare) + auto-restart

### 12.1. Cài cloudflared
```bash
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared
chmod +x cloudflared && sudo mv cloudflared /usr/local/bin/
```

### 12.2. Restart policy dịch vụ (override, không sửa file gốc)
```bash
cd ~/ml-real-estate-valiation
cat > docker-compose.override.yml <<'EOF'
services:
  minio:        { restart: unless-stopped }
  kafka:        { restart: unless-stopped }
  postgres:     { restart: unless-stopped }
  spark-master: { restart: unless-stopped }
  minio-init:   { restart: "no" }
EOF
docker compose up -d minio kafka postgres spark-master
```
(Worker đã có `--restart unless-stopped` ở mục 9.)

### 12.3. Web + tunnel thành systemd (tự lên sau reboot)
```bash
# Dashboard
sudo tee /etc/systemd/system/dashboard.service >/dev/null <<EOF
[Unit]
Description=Streamlit dashboard
After=network.target docker.service
[Service]
User=$USER
WorkingDirectory=$HOME/ml-real-estate-valiation/dashboard
EnvironmentFile=$HOME/ml-real-estate-valiation/.env
ExecStart=$HOME/dash-venv/bin/streamlit run Home.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
Restart=always
[Install]
WantedBy=multi-user.target
EOF

# Cloudflare tunnel
sudo tee /etc/systemd/system/cftunnel.service >/dev/null <<EOF
[Unit]
Description=Cloudflare quick tunnel
After=network.target dashboard.service
[Service]
ExecStart=/usr/local/bin/cloudflared tunnel --url http://localhost:8501
Restart=always
[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now dashboard cftunnel
```

Lấy link public:
```bash
sleep 8; sudo journalctl -u cftunnel --no-pager -n 40 | grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' | tail -1
```

> ⚠️ Quick tunnel: **mỗi lần khởi động lại link `trycloudflare` đổi**. Lấy link mới
> bằng lệnh journalctl trên. Muốn link cố định cần Cloudflare account + named tunnel.

---

## 13. Vận hành nhanh

| Việc | Lệnh (trên vm-master) |
|---|---|
| Trạng thái cụm | `curl -s http://localhost:8080/json/ \| grep -E '"aliveworkers"\|"host"'` |
| Spark UI | trình duyệt `http://192.168.56.10:8080` |
| Link web public | `sudo journalctl -u cftunnel -n 40 \| grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' \| tail -1` |
| Xem dự đoán | `docker exec -it postgres_realestate psql -U admin -d realestate -c "SELECT * FROM predictions ORDER BY scored_at DESC LIMIT 10;"` |
| Trạng thái web | `systemctl status dashboard` |
| Restart web | `sudo systemctl restart dashboard` |
| Worker join lại (trên worker VM) | `docker start spark-worker` |

### Bảo mật (public = cả internet vào)
- Đổi `PG_PASSWORD` + MinIO creds trong `.env` trước khi mở tunnel.
- Password user dashboard phải mạnh.
- Chỉ public **dashboard :8501**; KHÔNG public Spark UI/MinIO/Postgres.

### Reboot
- Dịch vụ + worker + web + tunnel tự lên lại (restart policy / systemd).
- **Link tunnel đổi** sau reboot → lấy lại bằng lệnh journalctl.
- Đừng reboot giữa buổi demo nếu không cần.

---

## 14. Yêu cầu tài nguyên
- Máy Windows tối thiểu **16GB RAM** (chừa 8GB host, 8GB cho 3 VM).
- Muốn `distinct_hosts=3` (3 worker máy khác nhau) cần máy khoẻ hơn (≥16GB cho VM)
  hoặc cắt bớt dịch vụ trên master để nhường RAM cho 1 worker.
