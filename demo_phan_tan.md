# Demo hệ thống chạy phân tán — Kịch bản bảo vệ

Mục tiêu: chứng minh cho thầy thấy ứng dụng chạy **phân tán thật** trên cụm 3 node, và giải thích luồng chạy giữa các node.

Cụm:
- **Master** `172.16.239.131`
- **Worker** `172.16.239.130`
- **Worker** `172.16.239.129`

---

## Demo chứng minh chạy phân tán

Bằng chứng mạnh nhất = cho thầy thấy 1 job Spark bị chẻ ra nhiều máy vật lý khác nhau. Trình theo 3 bước.

### 1. Cụm sống — nhiều node (30s)

Mở **Spark Master UI** trên browser:

```
http://172.16.239.131:8080
```

Hoặc trang dashboard **🖥️ Trạng thái cụm Spark** (page 2). Chỉ cho thầy:

- Master `ALIVE`
- **Worker sống 2/2** — host `.130` và `.129` (IP khác master → máy vật lý khác)
- Tổng cores/RAM = cộng gộp từ 2 worker

→ Điểm chốt: hostname/IP mỗi worker khác nhau = phân tán thật, không phải 1 máy giả lập.

### 2. Chạy job → thấy phân tán realtime (2 phút)

Trên master, submit pipeline:

```bash
spark-submit --master spark://172.16.239.131:7077 ml/train.py
```

Trong lúc chạy, refresh dashboard / Master UI:

- Mục **"Ứng dụng đang chạy"** hiện app với cores lấy từ **cả 2 worker**
- Click vào app → tab **Executors**: mỗi executor gắn 1 worker host khác nhau (`.130`, `.129`) → task chạy song song trên 2 máy

### 3. Chứng minh không phải 1 node gánh (đòn kết)

Tắt thử 1 worker giữa lúc chạy:

```bash
# trên node .129
stop-worker.sh
```

Refresh dashboard → worker `.129` chuyển 🔴 DEAD, job vẫn chạy tiếp trên `.130`.
→ chứng minh fault-tolerance + distributed scheduling.

---

## Giải thích luồng node cho thầy

```
        ┌─────────────────────────────────────────┐
        │  MASTER  172.16.239.131                   │
        │  - Spark Master :7077 (điều phối task)    │
        │  - Web UI :8080  (/json/ → dashboard đọc) │
        │  - Postgres, Kafka, MinIO/S3              │
        └───────────────┬───────────────────────────┘
                        │ chia task + gom kết quả
          ┌─────────────┴─────────────┐
          ▼                           ▼
   ┌──────────────┐           ┌──────────────┐
   │ WORKER .130  │           │ WORKER .129  │
   │ executor     │           │ executor     │
   │ chạy phần    │           │ chạy phần    │
   │ dữ liệu A    │           │ dữ liệu B    │
   └──────────────┘           └──────────────┘
```

**Luồng 1 lần train:**

1. **Master** nhận `spark-submit`, làm **Driver** — chia dataset thành partitions.
2. **Master (scheduler)** giao partition cho executor trên **Worker .130** và **.129** → 2 máy tính song song.
3. Mỗi worker đọc data (từ MinIO/S3 `s3a://`), fit phần model của mình, gửi kết quả trung gian về Driver.
4. **Driver** gom lại → model cuối → ghi model lên **S3**, metadata JSON lên volume, prediction vào **Postgres**, event lên **Kafka**.
5. **Dashboard** (Streamlit) đọc `172.16.239.131:8080/json/` → vẽ trạng thái cụm realtime — chính cái thầy đang nhìn.

**Câu chốt khi thầy hỏi "sao biết phân tán?":**

> "2 worker là 2 IP/máy vật lý riêng. 1 job Spark tạo executor trên cả hai, mỗi executor xử lý partition khác nhau cùng lúc. Tắt 1 máy job vẫn chạy — đó là distributed + fault-tolerant, không phải multiprocessing 1 máy."

---

## ⚠️ Lưu ý mạng — test trước khi bảo vệ

Subnet `172.16.239.x` **không route được từ máy dev/laptop**. Nên:

- Demo **trực tiếp trên master node** (SSH vào hoặc ngồi máy đó), KHÔNG demo từ laptop dev qua mạng.
- Hoặc set `SPARK_MASTER_UI` trỏ đúng IP master khi chạy dashboard trên máy cùng subnet.

Test trước 1 lần đường mạng, đừng để chết giữa buổi bảo vệ.
