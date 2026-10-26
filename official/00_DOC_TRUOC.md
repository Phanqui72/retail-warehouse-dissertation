# Khóa luận — Xây dựng Data Lakehouse hỗ trợ phân tích kinh doanh và dự báo nhu cầu trong TMĐT đa kênh
Phan Trọng Quí (23133061) · Phan Trọng Phú (23133056)

Thư mục `docs/official` là **bộ tài liệu chính thức** (đã chọn lọc). Bản nháp, bản trung gian và tài liệu chưa hoàn thiện nằm ở `docs/raw`.

## Cấu trúc thư mục `docs/official`

| Thư mục | Nội dung | File chính |
|---|---|---|
| `Diagram/data architecture of pipielien` | Kiến trúc tổng thể Data Lakehouse | `kien_truc_lakehouse.drawio` (3 trang: Trang-1 streaming chi tiết — có tên công ty, chỉ dùng nội bộ · Trang-2 kiến trúc có Kafka · trang v1 batch chưa có Kafka) · `kien_truc_lakehouse_streaming_an_danh.png` (ảnh đã ẩn danh, dùng chèn vào báo cáo) |
| `Diagram/pipiline dag` | Luồng task Airflow của DAG chính `vnb_daily_batch` + các DAG phụ | `1_pipeline_dag.drawio` |
| `Diagram/sequence diagram` | Trình tự gọi giữa các thành phần khi nạp batch 1 ngày (luồng chuẩn + khung lỗi manifest) | `2_sequence.drawio` |
| `Diagram/error handling diagram` | Lưu đồ xử lý lỗi: lỗi file → retry hạ tầng → phân loại bản ghi ở Silver → cổng chất lượng trước publish | `3_error_handling.drawio` |
| `Diagram/data lineage diagram` | Lineage mức bảng: Landing → Bronze → Silver → Gold → Power BI | `4_data_lineage.drawio` |
| `Sample Datasets (Xem trước để hiểu đơn giản)` | Bộ dữ liệu mẫu vài chục dòng/bảng (29 bảng). Trong Excel, ô tô đỏ = giá trị bị tiêm lỗi cố ý | `vn_beauty_orders_dataset_SAMPLE.xlsx` · `vn_beauty_sample_csv.zip` (kèm bản sao danh mục lỗi và yêu cầu DQ để đối chiếu nhanh) |
| `Full datasets` | Dữ liệu đầy đủ 01/01/2021 → 30/09/2026 (~314 nghìn đơn) dạng landing batch | 7 file zip: `landing_master_va_log.zip` + `landing_daily_2021…2026.zip` |
| `Data Quality Governance (watch all governance data)` | Quản trị & chất lượng dữ liệu | `vn_beauty_data_issues_catalog.xlsx` (64 lỗi cố ý) · `vn_beauty_data_quality_requirements.xlsx` (data contract, 298 luật cột, 14 luật đối soát, ngưỡng, governance) · `vn_beauty_error_handling_policy.xlsx` (nhóm lỗi E1–E9, 73 mã lỗi → hành động, retry, runbook) · `vn_beauty_injected_issues_log.csv` (từng bản ghi bị tiêm lỗi) |
| `Code to genarate dataset (if add into new value)` | Code sinh lại toàn bộ dữ liệu (seed cố định) khi cần thêm SKU / bảng / lỗi mới | `vn_beauty_generator_code.zip` |

## Cách dùng bộ dữ liệu đầy đủ
1. Giải nén **cả 7 file zip vào cùng một thư mục** `landing/` → được:
   - `landing/daily/dt=YYYY-MM-DD/<dataset>.csv` + `_manifest.json` + `_SUCCESS` (13 bảng phát sinh hằng ngày)
   - `landing/master/snapshot_date=…/<dataset>.csv` (16 bảng danh mục, full snapshot)
   - `landing/landing_file_issues_log.csv` (9 lỗi cấp file FILE-01…09)
2. Pipeline batch đọc theo từng ngày `dt` (Airflow `vnb_daily_batch`, chạy 02:00 cho ngày hôm trước).
3. Một đơn có thể xuất hiện ở nhiều ngày (bản lúc tạo + bản cập nhật cuối) → Silver phải MERGE theo `updated_at`.

## Thứ tự đọc gợi ý
1. `Diagram/data architecture of pipielien` → nắm kiến trúc
2. `Sample Datasets` → xem dữ liệu trông như thế nào
3. `Data Quality Governance` → lỗi cố ý, luật chất lượng, cách xử lý lỗi
4. `Diagram/pipiline dag` → `sequence diagram` → `error handling diagram` → `data lineage diagram` → thiết kế pipeline
5. `Full datasets` → khi bắt đầu chạy pipeline thật

## Tài liệu liên quan nằm ngoài `official`
| Tài liệu | Vị trí |
|---|---|
| Đặc tả pipeline (6 DAG, retry, SLA, kịch bản sequence) | `docs/raw/05_Pipeline_Spec/pipeline_spec.yaml` |
| Source-to-target mapping (lineage mức cột, 924 dòng) | `docs/raw/05_Pipeline_Spec/vn_beauty_source_to_target_mapping.xlsx` |
| Bản Word khóa luận (bản nháp) + link báo cáo tiến độ | `docs/raw/01_Bao_cao (Upadate Latter)/` |
| Kịch bản demo bản streaming | `docs/raw/06_Demo/` |
| Code hệ thống | repo `D:\retail-datawarehouse-dissertation` |

## Ghi chú
- 100% dữ liệu là dữ liệu sinh; thương hiệu, nhà cung cấp, đối thủ đều hư cấu; cấu trúc thuộc tính theo hệ thống thật đã ẩn danh.
- Không đưa Trang-1 của `kien_truc_lakehouse.drawio` ra bên ngoài vì có tên công ty.
