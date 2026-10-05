# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Hồ Thái Hòa, 2A202602915
**Repo:** https://github.com/thaihoaho-code/K4-Track02-Day17-HoThaiHoa-2A202602915-DataPipelineEngineering
**Commit bài nộp:** 3fc91bb07a2e783b5dd548f5e4c8a8227e27c006
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** gemini 3.1 pro
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 rows cho 12 tickets. T-91 cập nhật nhiều lần bị lưu thành 3 bản ghi rời rạc thay vì giữ trạng thái mới nhất. | Lateness P99 = 3.00 nhưng `LOOKBACK_DAYS=0`. Sự kiện của u05 đến muộn không được đếm cho ngày 08-12. | Ticket T-97 bị xoá nhưng vẫn còn thông tin ở Silver (chưa thành tombstone), chưa bị xoá khỏi Gold (training/RAG). |
| **Nguyên nhân gốc** | Dùng `INSERT INTO` thuần tuý nên dữ liệu bị nối thêm sau mỗi lần chạy (append-only), thiếu cơ chế chống trùng lặp theo khoá chính khi chạy lại batch cũ. | `LOOKBACK_DAYS=0` giả định dữ liệu đến ngay lập tức. Khi data trễ 3 ngày mới tới (ingest time trễ), batch chạy của ngày hiện tại không lùi lại để tính toán lại feature cho `event_date` 3 ngày trước. | Khi bản ghi bị xoá (op="d"), Debezium trả về trường `after = null`. Code lấy `ticket_id` từ `after` sẽ bị null và bị loại bỏ bởi mệnh đề `WHERE ticket_id IS NOT NULL`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Đổi `INSERT` thành `MERGE INTO`, `ON t.ticket_id = s.ticket_id`. Cập nhật khi `WHEN MATCHED AND s._lsn > t._lsn` (so sánh LSN). | `pipeline/config.py`: Đổi `LOOKBACK_DAYS = 3` (Bằng với trần của lateness P99 để recompute 3 ngày trước đó). | `pipeline/staging.py`: Sửa `j->'value'->'after'->>'ticket_id'` thành `j->'key'->>'ticket_id'` để luôn lấy được ID kể cả khi record bị xoá. |
| **Khái niệm trên slide** | "Silver — Có khoá" (Một thực thể cần có ID), Idempotent (Chạy lại nhiều lần không sinh bản ghi thừa), Chọn trạng thái mới nhất theo LSN. | "Data về muộn" (Late data), "Event time vs Ingest time", Dùng "Lookback window" đủ lớn bao phủ P99 để cập nhật lại quá khứ. | "CDC log-based" (Bắt trọn lịch sử thay đổi), Phân biệt "CDC delete" (value có op="d") và "Kafka tombstone" (value = null). "Xoá phải lan" đến Gold. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`. Lý do: P99 là 3.00, ta cần làm tròn lên (ceil) để cửa sổ lookback kéo dài đủ 3 ngày về quá khứ, giúp quét và tính toán lại (recompute) được 99% các sự kiện đến trễ (chẳng hạn event u05 trễ 3 ngày). P50 = 0.00, P95 = 2.90.
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE đảm bảo chống trùng lặp và giữ trạng thái mới nhất theo LSN (idempotent), còn overwrite-partition cho phép tính lại dễ dàng (recompute) toàn bộ khoảng lookback khi có late data.
- Tombstone thay vì xoá hẳn hàng trong Silver: Xoá hết PII nhưng giữ lại LSN để làm chốt chặn (chống "phục sinh" ticket nếu vô tình replay batch cũ), đồng thời truyền tín hiệu "delete" xuống tầng Gold.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Ưu tiên tính bất biến (immutable) để hệ thống AI/ML có thể tái lập (reproducible) chính xác tập dữ liệu đã dùng để huấn luyện.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu Customer Support thường ở mức vừa và nhỏ; dùng OLAP in-process như DuckDB trên một máy đơn (single-node) đem lại tốc độ cực nhanh, nhẹ, tiết kiệm chi phí và bớt độ phức tạp so với vận hành cluster Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   **Đáp:** Đánh đổi ở đây là giữ nguyên tính bất biến của các snapshot cũ để đảm bảo khả năng tái lập (reproducibility) và audit mô hình học máy, nhưng áp dụng việc xóa (loại bỏ T-97) vào các snapshot mới nhất và RAG chunks trực tiếp phục vụ hệ thống hiện tại. Tuy nhiên, đây chưa phải là cơ chế xoá PII đầy đủ và hợp chuẩn (như GDPR) cho production. Trong thực tế, cần áp dụng các biện pháp triệt để hơn như Crypto-shredding (mã hoá PII và huỷ khoá khi cần xoá) hoặc áp dụng chính sách vòng đời (Retention/TTL) để xoá hẳn các snapshot cũ sau một khoảng thời gian.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

```text
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m pytest
..................................                                                                               [100%]
34 passed in 3.92s
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe main.py --land-only
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> $env:DO_NOT_TRACK = '1'
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> Push-Location dbt_project
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering\dbt_project> try {
>>     ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
>> } finally {
>>     Pop-Location
>> }
01:11:43  Running with dbt=1.12.5
01:11:43  Registered adapter: duckdb=1.11.0
01:11:45  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
01:11:45
01:11:45  Concurrency: 1 threads (target='dev')
01:11:45
01:11:45  1 of 19 START sql view model main.stg_events ................................... [RUN]
01:11:45  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.12s]
01:11:45  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
01:11:45  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
01:11:45  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
01:11:45  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.18s]
01:11:45  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
01:11:45  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.20s]
01:11:45  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
01:11:46  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.21s]
01:11:46  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
01:11:46  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.07s]
01:11:46  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
01:11:46  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.05s]
01:11:46  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
01:11:46  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.05s]
01:11:46  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
01:11:46  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.07s]
01:11:46  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
01:11:46  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
01:11:46  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
01:11:46  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
01:11:46  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
01:11:46  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
01:11:46  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
01:11:46  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
01:11:46  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
01:11:46  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
01:11:46  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
01:11:46  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
01:11:46  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
01:11:46  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
01:11:46  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.07s]
01:11:46  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
01:11:46  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.04s]
01:11:46  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
01:11:46  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.04s]
01:11:46  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
01:11:46  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.04s]
01:11:46  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
01:11:46  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
01:11:46  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
01:11:47  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
01:11:47  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
01:11:47  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.06s]
01:11:47  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.36s]
01:11:47  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
01:11:47  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
01:11:47  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
01:11:47  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.03s]
01:11:47  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
01:11:47  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.03s]
01:11:47
01:11:47  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 2.00 seconds (2.00s).
01:11:47
01:11:47  Completed successfully
01:11:47
01:11:47  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
(.venv) PS C:\Users\thaih\Projects\Vin\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
