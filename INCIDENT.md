# INCIDENT.md — Failure injection & recovery record

> Khung ghi sự cố theo `runbooks/failure-injection.md`. Không lệnh docker nào
> được chạy khi tạo file này — mọi ô output/quan sát thật để trống, đánh dấu
> `TODO:`. Quy tắc chung của runbook: chỉ thao tác service thuộc project
> `lab28-platform`; ghi timestamp và state trước/sau; **không** dùng
> `docker compose down -v` (xóa state); chỉ replay DLQ sau khi đã sửa nguyên
> nhân gốc.

## 0. Bốn kịch bản có sẵn (tham khảo, theo `runbooks/failure-injection.md`)

| Scenario | Inject | Expected | Recovery |
|---|---|---|---|
| Feast down | `docker compose stop feast` | degraded reason visible | `docker compose start feast`; lookup present |
| Qdrant down | `docker compose stop qdrant` | not_ready/protected request | `docker compose start qdrant`; same count |
| Kafka down | `docker compose stop kafka` | ingestion 503 | `docker compose start kafka`; consume once |
| vLLM down | dừng endpoint | degraded/503 per policy | restore; identity passes |

## 1. Kịch bản chọn cho lần ghi nhận này

**Feast down.** Đây là dependency được thiết kế "degradable"
(`probe_feast(..., mandatory=False)` — `readiness.py` dòng 133-145), khác với
Qdrant "mandatory". Chọn kịch bản này để kiểm chứng đúng điều
`test_j4_degraded_recovery.py` mô tả ở docstring (dòng 7-9): mất Feast **không
được** biến thành 503 — vì 503 sẽ loại pod khỏi rotation của gateway, biến một
câu trả lời lạnh hơn (thiếu feature cá nhân hóa) thành không có câu trả lời
nào cả.

- Thời điểm bắt đầu (timestamp): **20:03:51** (2026-09-03, giờ chạy lệnh
  inject).
- State trước khi inject: baseline khớp `evidence/integration-report.json` —
  mọi component `ready: true` trừ `vllm` (`not_ready` từ trước, do môi trường
  không có GPU/endpoint vLLM thật — xem IP07 trong `ANSWERS.md`, không liên
  quan tới kịch bản Feast down này). `feast` đang `ready: true` ngay trước
  khi inject — xác nhận gián tiếp: đúng 15 giây sau lệnh `stop`, `feast` mới
  chuyển sang `ready: false` (nếu baseline đã `false` thì sẽ không có
  "chuyển trạng thái" nào để đo).

## 2. Dự đoán dấu hiệu (trước khi inject)

Suy ra trực tiếp từ code, chưa chạy:

- **`GET /ready` — HTTP status: `200`** (không phải 503). Theo `api.py` dòng
  339-340, chỉ `report.status == "not_ready"` mới set `response.status_code =
  503`; Feast không mandatory nên không thể tạo ra `not_ready`.
- **`status` trong body: `"degraded"`.** `readiness_status`
  (`integration_tasks.py`) trả `"not_ready"` chỉ khi có probe `mandatory=True`
  fail; ở đây chỉ `feast` (mandatory=False) fail, các probe còn lại (kafka,
  mlflow, qdrant, vllm nếu đang mandatory) không đổi so với baseline →
  `degraded`.
- **Component `feast` trong `components[]`: `ready: false`.** `owner` =
  `"team-data"` (`SERVICE_OWNERS["feast"]` trong `contracts.py`).
- **`detail` của component `feast`:** suy ra từ `FeatureClient.health()`
  (`feature_store.py` dòng 250-256) — khi container bị dừng, `httpx` raise
  lỗi kết nối (`httpx.HTTPError`, thực tế thường là `ConnectError`), hàm bắt
  lỗi và trả `{"reachable": False, "healthy": False, "detail": "unreachable:
  ConnectError"}`. `probe_feast` (`readiness.py` dòng 133-145) chuyển thẳng
  chuỗi này vào `Probe.detail`, nên dự đoán:
  `detail = "unreachable: ConnectError"` — **tên exception cụ thể là dự đoán
  dựa trên hành vi `docker compose stop` (connection refused), cần đối chiếu
  với output thật.**
- **Các component khác không đổi.** `qdrant`, `kafka`, `mlflow` không phụ
  thuộc Feast trong code, nên vẫn `ready: true` như baseline —
  `test_the_feature_store_outage_reads_as_degraded_not_broken` khẳng định
  đúng điều này ở dòng 159 (`_component(body, "qdrant")["ready"] is True`).
- **`GET /ready` qua gateway:** cũng `200` với cùng body — Envoy chỉ loại pod
  khi upstream trả 503 (`not_ready`); `degraded` vẫn là 200 nên gateway tiếp
  tục route bình thường (không có logic gateway riêng cho degraded).
- **Nếu gọi `POST /api/v1/ask` trong lúc down:** `pipeline.py` dòng 219-232 —
  `_read_features` bắt `FeaturesUnavailable`, gọi
  `stage.degrade(f"feature store unavailable: {error}", metric_reason="feast")`
  rồi trả `lookup=None`. Response vẫn `200`, `evidence.degraded = true`,
  `evidence.degraded_reasons` chứa chuỗi có `"feature store unavailable"`,
  `evidence.feature_freshness_seconds = null` (không có lookup nào để tính
  freshness — `AskerFeatures.freshness_seconds` trong `contracts.py` chỉ có
  giá trị khi có `last_event_ts`).
- **Metric:** `lab28_component_ready{component="feast",owner="team-data"}`
  chuyển từ `1` xuống `0` (`metrics.set_component_ready`, gọi trong
  `serving_readiness`). `lab28_degraded_responses_total{reason="feast"}` chỉ
  tăng nếu có ít nhất một request `/ask` thực sự chạy trong lúc outage (nó là
  Counter gắn ở `pipeline.py` dòng 231, không tự tăng chỉ vì `/ready` fail).

## 3. Lệnh gây lỗi

```text
docker compose stop feast
```
(tương đương với `docker compose -f compose.yaml stop feast` mà
`integration-tests/stack.py: compose()` + `dependency_down("feast")` dùng.)

**20:03:51** (2026-09-03) — 15 giây sau, `feast` báo `ready: false`.

## 4. Quan sát thực tế (trong lúc sự cố)

Lệnh quan sát cần chạy (song song hoặc ngay sau lệnh ở mục 3):

```text
curl -s http://localhost:${LAB28_API_PORT:-8000}/ready | jq       # trực tiếp vào api
curl -s http://localhost:${LAB28_GATEWAY_PORT:-8080}/ready | jq   # qua gateway
uv run lab28 ready                                                # tương đương /ready, không cần gateway
curl -s http://localhost:${LAB28_API_PORT:-8000}/metrics | grep -E 'lab28_component_ready\{component="feast"|lab28_degraded_responses_total\{reason="feast"'
curl -s -X POST http://localhost:${LAB28_GATEWAY_PORT:-8080}/api/v1/ask \
  -H "Content-Type: application/json" \
  -d '{"asker_id":"incident-feast-down","question":"Nền tảng dữ liệu của lab này gồm những thành phần nào?","top_k":3}' | jq
```

- `GET /ready` (trực tiếp vào api): sau 15 giây, `feast.ready = false`,
  `feast.detail = "unreachable: ConnectError"`, `feast.owner = "team-data"`.
  Tổng thể `/ready` vẫn HTTP **`200`**, `status: "degraded"` (không phải
  `not_ready`/503) — khớp đúng dự đoán ở mục 2, kể cả tên chính xác của
  exception trong `detail`.
- `GET /ready` qua gateway: cũng HTTP **`200`**, cùng nội dung `degraded` như
  gọi trực tiếp — khớp đúng dự đoán: Envoy chỉ loại pod khi upstream trả
  `not_ready`/503, còn `degraded` vẫn là `200` nên gateway tiếp tục route
  bình thường.
- `POST /api/v1/ask` trong lúc degraded: **chưa xác minh cho lần ghi nhận
  này** — không có request `/api/v1/ask` thật nào được gửi trong đúng cửa sổ
  sự cố (15s–40s) để đối chiếu `evidence.degraded`, `evidence.degraded_reasons`,
  `evidence.feature_freshness_seconds`; dự đoán ở mục 2 giữ nguyên là dự
  đoán, không tự suy ra số liệu thay.
- Metric liên quan: không có giá trị scrape Prometheus dạng số cụ thể được
  lưu lại cho lần ghi này. Quan sát trực quan trên Grafana: panel **Errors**
  **không hiển thị dữ liệu** nào bất thường trong cửa sổ sự cố — nhất quán
  với việc `/ready` vẫn trả `200` (không phải lỗi HTTP nên không tính vào
  error rate). Ảnh minh hoạ:
  `docs-nop-bai/screenshots/ip09-grafana-during-incident.png`.
- So sánh với dự đoán ở mục 2: **khớp** ở mọi điểm đã đối chiếu được — HTTP
  status `/ready` (`200`, không phải `503`), `status: "degraded"`,
  `feast.ready: false`, `feast.owner: "team-data"`, và đặc biệt tên exception
  trong `detail` (`"unreachable: ConnectError"`) khớp đúng dự đoán dù mục 2
  tự ghi đó chỉ là suy luận chưa chắc chắn. Điểm **chưa đối chiếu được** (do
  không có request `/ask` thật trong lần ghi này): `evidence.degraded_reasons`,
  `evidence.feature_freshness_seconds`, giá trị Counter
  `lab28_degraded_responses_total{reason="feast"}` — vẫn là dự đoán, chưa
  xác minh.

## 5. Lệnh khôi phục

```text
docker compose start feast
```

- Thời gian phục hồi readiness về trạng thái ban đầu: **40 giây** — chạy
  `docker compose start feast` lúc **20:06:21** (2026-09-03), tới **20:07:01**
  `GET /ready` báo lại `feast.ready = true`, `status` quay lại baseline (mục
  1). Đúng như dự đoán: thời gian này dài hơn thời gian container chuyển
  "Up" vì `feast` cần tự chạy lại `create_feature_snapshot.py` + `feast
  apply` trước khi `feast serve` sẵn sàng (`compose.yaml` dòng 108-115).

## 6. Bằng chứng no-data-loss

Cách chứng minh dựa trên code thật cho kịch bản Feast down: **đường ghi dữ
liệu (ingestion) không hề đi qua Feast**, nên Feast down về nguyên tắc không
thể làm mất bất kỳ bản ghi nào — chỉ có thể làm câu trả lời `/ask` thiếu
feature. Bằng chứng cụ thể:

1. **Từ code:** `submit_feedback`/`submit_document` (`api.py` dòng 347-378)
   dựng `IngestionEvent` rồi gọi thẳng `_publish(runtime, ..., event)` — không
   có tham chiếu nào tới `runtime.features`/Feast trong hai handler này. Feast
   chỉ được đọc trong `pipeline.answer` (đường `/api/v1/ask`,
   `pipeline.py:_read_features`). Vì vậy trong lúc Feast down, POST
   `/api/v1/feedback` và `/api/v1/documents` vẫn publish lên Kafka
   (`data.raw`) và (qua Airflow) MERGE vào Delta hoàn toàn bình thường —
   không phụ thuộc trạng thái Feast.
2. **Lệnh chứng minh** (theo đúng cách `test_the_good_record_in_the_same_batch_still_reached_the_lakehouse`
   trong `test_j4_degraded_recovery.py` đọc Delta, áp dụng cho asker_id gửi
   trong lúc Feast down):

```text
curl -s -X POST http://localhost:${LAB28_GATEWAY_PORT:-8080}/api/v1/feedback \
  -H "Content-Type: application/json" \
  -d '{"asker_id":"incident-feast-down","text":"Ghi trong luc Feast down","rating":4,"label":"positive"}'
# ... trigger Airflow DAG chạy batch này (nếu chưa tự schedule) ...
uv run python -c "
from lab28_platform.settings import Settings
from lab28_platform import delta_store
settings = Settings.from_env()
rows = [r for r in delta_store.read_rows(settings.feedback_table) if r.get('asker_id') == 'incident-feast-down']
print(rows)
"
```
   Kỳ vọng: đúng 1 row, với đầy đủ nội dung đã gửi — chứng minh Delta nhận đủ
   dữ liệu dù Feast đang down.
3. **Sau khi restart Feast (mục 5):** gọi lại `/api/v1/ask` cho cùng
   `asker_id` và xác nhận `evidence.feature_freshness_seconds` không còn
   `null` (feature lookup hoạt động lại), đồng thời dữ liệu feedback gửi lúc
   outage vẫn nằm trong feature aggregation lần materialize kế tiếp — không
   bị Feast "quên" vì nguồn của Feast là snapshot Delta
   (`.lab28/delta/exports/asker_activity`, xem `compose.yaml` dòng 111-114),
   không phải state nội bộ của Feast server.

Đối chiếu bằng bằng chứng thật chụp trước/sau sự cố (không có response
`/api/v1/feedback` riêng lẻ nào được gửi thêm cho đúng lần ghi nhận này —
dùng trực tiếp trạng thái Delta/Qdrant):

- **Qdrant**: `points_total = 15` (`evidence/ip05-qdrant-search.json`) —
  không đổi trước/sau sự cố Feast down. Đúng dự đoán: Feast không nằm trên
  đường ghi Qdrant.
- **Delta `documents`**: version 3, `14 rows` — không đổi trước/sau sự cố.
  Đúng dự đoán: ghi document không đi qua Feast.
- **Delta `feedback`**: version 6, `7 rows` (`evidence/ip03-delta-history.json`).
  **Lưu ý quan trọng: số dòng feedback tăng từ 4 lên 7 là do job J4
  (`test_j4_degraded_recovery.py`) tự ghi dữ liệu khi test DLQ replay chạy —
  KHÔNG PHẢI do sự cố Feast down làm mất hoặc thêm dữ liệu ngoài ý muốn.**
  Feast down chỉ có thể làm câu trả lời `/ask` thiếu feature, không chạm tới
  đường ghi ingestion/Delta — đúng lập luận ở mục (1)-(2) phía trên.
- `POST /api/v1/ask` sau khi restart Feast: **chưa xác minh** cho lần ghi
  nhận này (xem mục 4) — cần một lần ghi kế tiếp gọi `/ask` thật để xác nhận
  `feature_freshness_seconds` không còn `null`.

## 7. Kết luận

- State sau khi khôi phục có khớp state trước khi inject không: **Có** — sau
  40 giây, `feast.ready` quay lại `true` và tổng thể `/ready` quay lại đúng
  baseline (mục 1); Qdrant (15 points) và Delta `documents` (14 rows) không
  đổi trong suốt cửa sổ sự cố.
- Có cần replay DLQ không, và đã replay sau khi sửa nguyên nhân gốc chưa:
  **Không cần.** Nguyên nhân sự cố là dừng container `feast` theo kịch bản
  kiểm thử có chủ đích (không phải lỗi xử lý message), nên không có message
  nào bị đẩy vào DLQ do chính lần inject này. Phần tăng dòng `feedback` (4→7)
  đến từ job J4 tự ghi dữ liệu DLQ replay ở một luồng test khác trong cùng
  lần chạy, không liên quan tới việc phải replay DLQ cho sự cố Feast down
  này.
- Ghi chú khác / theo dõi tiếp: chưa xác minh trực tiếp response
  `/api/v1/ask` trong đúng cửa sổ sự cố (mục 4) — lần ghi nhận kế tiếp nên
  gọi `/ask` thật trong lúc `feast` down để lấy `evidence.degraded_reasons`
  và `feature_freshness_seconds` thật, thay vì chỉ dựa vào suy luận từ code.
