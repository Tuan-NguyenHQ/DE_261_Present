# TravelHub Demo Dataset (~10 Rows Showcase)

Bộ dữ liệu mẫu rút gọn (~10 dòng) minh họa trọn vẹn các bài toán kỹ thuật dữ liệu lớn trong nền tảng TravelHub:

## Cấu trúc thư mục
- `raw/bookings_part1.csv`: 6 dòng booking dạng CSV với nhiều kiểu dữ liệu bẩn và bản ghi trùng.
- `raw/bookings_part2.xml`: 4 dòng booking dạng XML từ kênh đối tác OTA.
- `raw/hotels.xml`: 5 khách sạn dạng XML (có bản ghi hợp lệ và dữ liệu vi phạm schema/DQ).
- `raw/customers.csv`: 5 khách hàng chứa thông tin PII nhạy cảm (Tên, Email, SĐT) và bản ghi lỗi.
- `raw/payments.csv`: 6 giao dịch thanh toán và hoàn tiền cho các booking.
- `seeds/fx_rates.csv`: Bảng tỷ giá hối đoái có hiệu lực theo mốc thời gian (`valid_from`).

## Các bài toán được giải quyết
1. **Đa định dạng (Multi-format)**: Đọc đồng thời CSV & XML ở tầng Bronze.
2. **Chuẩn hóa & Ép kiểu**: Lọc bỏ ký tự tiền tệ (`$`, dấu phẩy, chữ VND/THB/SGD), ép kiểu String sang Decimal, chuẩn hóa kênh và trạng thái.
3. **Deduplication (Dedup) & Quarantine**: Loại bỏ bản ghi trùng key bằng Window Function, đẩy dữ liệu sai luật nghiệp vụ vào bảng Quarantine.
4. **Bảo mật PII**: Xóa tên thật, băm SHA-256 Email và Số điện thoại trước khi đẩy ra khỏi Lake.
5. **Tỷ giá hiệu lực (ASOF JOIN)**: Quy đổi chính xác theo ngày đặt phòng với bảng tỷ giá SCD Type 2.
6. **Báo cáo phân tích**: Tính toán doanh thu và hiệu suất khách sạn trên ClickHouse.
