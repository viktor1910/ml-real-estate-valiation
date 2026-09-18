# YÊU CẦU THUYẾT TRÌNH

## I. Mục tiêu

1. Tìm hiểu được một hệ quản trị cơ sở dữ liệu NoSQL, một phần mềm trong hệ sinh thái Dữ liệu lớn **HOẶC** tìm hiểu một hệ thống xử lý dữ liệu lớn theo thời gian thực.
2. Rèn luyện kỹ năng xử lý dữ liệu lớn, Streaming, Machine Learning và Graph Processing.
3. Minh họa áp dụng xử lý dữ liệu lớn.

## II. Yêu cầu chung

- Bài thuyết trình được thực hiện theo nhóm. Mỗi đề tài chỉ được thực hiện bởi tối đa 1 nhóm.
- Nhóm học viên trình bày kết quả tìm hiểu được và hình ảnh mô phỏng (demo) trong tập tin báo cáo (Word). Chuẩn bị tập tin trình chiếu (PowerPoint) để báo cáo cho giảng viên hướng dẫn trên lớp.
- Nhóm trưởng các nhóm nộp trước báo cáo lên Google Classroom. Mã lớp và hướng dẫn cách thức nộp sẽ được thông báo sau.
- Các nhóm sẽ chuẩn bị cho việc thuyết trình online theo danh sách và thứ tự được thông báo sau đó.
- Nội dung cần nộp bao gồm:
  - File báo cáo (định dạng PDF và MS Word hoặc LaTeX...)
  - File trình chiếu
  - Dữ liệu và mã nguồn minh họa.
  - Video hướng dẫn cài đặt và minh họa.
- **Lưu ý:** Các nhóm thực hiện không đúng theo yêu cầu trên hoặc vi phạm quy định môn học (bài làm giống nhau...) sẽ bị 0 điểm.

## III. Yêu cầu tìm hiểu

Với mỗi đề tài, nhóm học viên cần trình bày các thông tin sau:

- Phát biểu bài toán dữ liệu lớn, mô tả dữ liệu.
- Thông tin chung về hệ quản trị NoSQL (HQT NoSQL), công cụ sử dụng (nếu có).
- Ưu/khuyết điểm, những đặc điểm khiến các công cụ trên nổi bật hơn so với các sản phẩm khác cùng loại (nếu có).
- Một vài trường hợp cụ thể đã áp dụng sản phẩm trong thực tế (case study, nếu có).
- Trình bày mô hình hệ thống.
- Cách cài đặt, kết nối với Spark, cách điều chỉnh các tham số.
- Minh họa xử lý dữ liệu lớn.

---

# Đề tài: Dự báo xu hướng giá bất động sản tự động (Real Estate Valuation)

## Ý tưởng sơ khởi

Cào dữ liệu hàng ngày từ các trang bất động sản, kết hợp dữ liệu vĩ mô để dự đoán giá trị thực của một căn nhà, giúp nhà đầu tư tìm ra bất động sản đang bị định giá thấp.

## Kiến trúc hệ thống

```text
Scrapy/BeautifulSoup -> Kafka -> Spark SQL/MLlib -> PostgreSQL
```
