# Demo checklist — Day 28 Track 2

Nguồn: `docs/demo-runbook.md`, `contracts/integration-matrix.yaml`,
`integration-tests/conftest.py`, `integration-tests/stack.py` và toàn bộ
`integration-tests/*.py`. Không có lệnh docker nào được chạy khi soạn file
này; mọi URL/cổng lấy từ `compose.yaml`, `src/lab28_platform/settings.py` và
bảng URL sẵn có trong `README.md`.

Giả định: stack đã `up -d --build --wait` (profile `full` nếu cần Airflow/Spark,
profile `gpu` nếu cần vLLM thật), và đã seed lần đầu theo mục 4 (thứ tự chạy).

---

## 1. Bảng chứng minh live theo từng IP

| IP | Lệnh chính xác để chứng minh live | URL cần mở | Điều gì vừa được chứng minh (1 dòng) |
|---|---|---|---|
| IP01 | `uv run pytest integration-tests/test_j1_golden_path.py -q -k test_the_event_reached_kafka_with_its_trace_context` | Không có UI web cho Kafka trong stack này — mở `evidence/ip01-kafka-consume.json` sau khi chạy (hoặc Prometheus `http://localhost:9090/graph` với `lab28_ingestion_events_total`) | Request qua gateway đã thành 1 message thật trên topic `data.raw`, đúng key `asker_id`, còn giữ header `traceparent`. |
| IP02 | `uv run pytest integration-tests/test_j1_golden_path.py -q -k "test_the_pipeline_run_succeeded or test_the_run_published_the_lakehouse_asset_event"` | `http://localhost:8082` (Airflow UI, DAG `lab28_ingestion_pipeline` → tab Runs) | DAG chạy `success`, mọi task instance `success`/`skipped`, và phát asset event `lab28://delta/feedback`. |
| IP03 | `uv run lab28 evidence` (ghi `evidence/ip03-delta-history.json`) | Không có UI web cho Delta Lake — mở trực tiếp file evidence vừa ghi (Spark UI `http://localhost:4040` chỉ có khi job Spark đang chạy, không phải kho lưu trữ) | `DeltaTable.history()` đọc được, version tăng đơn điệu sau mỗi lần MERGE. |
| IP04 | `uv run pytest integration-tests/test_j1_golden_path.py -q -k test_the_feature_store_serves_the_new_asker` | Không có UI web cho Feast — mở `evidence/ip04-feast-online.json` (hoặc gọi trực tiếp `POST http://localhost:6566/get-online-features`) | Feast trả online feature cho đúng `asker_id` vừa ghi, kèm `delta_version` và `freshness_seconds`. |
| IP05 | `uv run pytest integration-tests/test_j1_golden_path.py -q -k test_the_document_is_retrievable_from_the_vector_store` | `http://localhost:6333/dashboard` (collection `lab28_documents`) | Document vừa gửi đã có đúng 1 point trong Qdrant, point id suy determin từ `doc_id`. |
| IP06 | `uv run pytest integration-tests/test_j3_promotion_rollback.py -q -k test_the_champion_alias_moved_to_the_new_version` | `http://localhost:5000` (MLflow → Models → `lab28-rag-release` → tab Aliases) | Alias `champion` vừa chuyển sang model version mới đăng ký, có đủ tag provenance (prompt/model/collection/feature service). |
| IP07 | `uv run pytest integration-tests/test_j1_golden_path.py -q -m gpu -k test_the_answer_comes_from_the_champion_release_and_the_pinned_model` (cần profile `gpu`) | `http://localhost:8001/version` và `http://localhost:8001/v1/models` | Câu trả lời tới từ một vLLM thật (không phải mock), đúng model id đã pin trong champion release. |
| IP08 | `uv run pytest integration-tests/test_gateway_rate_limit.py -q` | `http://localhost:8080/health` (route bị burst) và `http://localhost:9901/stats/prometheus` (đếm `envoy_http_local_rate_limit_rate_limited`) | Gateway tự chặn burst bằng 429 kèm `x-request-id`, bucket có refill, và số liệu bị chặn đã lên Envoy admin stats. |
| IP09 | `uv run pytest integration-tests/test_prometheus_targets.py -q` | `http://localhost:9090/targets` và `http://localhost:3000` (dashboard "Lab 28 Platform Overview") | Mọi target cấu hình đều `up`, có alert rule `severity` hợp lệ, và Grafana có dashboard + datasource được provision từ file cấu hình. |
| IP10 | `uv run pytest integration-tests/test_j5_trace_metrics_continuity.py -q` (đủ cho nhánh sync+async) hoặc `uv run pytest integration-tests/test_trace_span_coverage.py -q -m gpu` (đủ cả 11 span, cần GPU) | `http://localhost:16686` → tìm theo `trace_id` in `evidence/ip10-trace.json` | Một `trace_id` do client tự sinh xuất hiện xuyên ≥3 tiến trình (gateway/api/kafka/airflow/spark…), không bị đứt khi qua Kafka. |

Ghi chú: IP07 và các bài `@pytest.mark.gpu` tự `pytest.skip` (không fail) nếu
không có vLLM thật đang chạy — đó là hành vi thiết kế của `conftest.py`, không
phải lỗi. IP10 leg LangSmith (`test_the_langsmith_export_leg_is_configured_and_healthy`,
marker `langsmith`) chỉ chạy khi có `LANGSMITH_API_KEY`.

---

## 2. Kịch bản demo 5 bước (theo `docs/demo-runbook.md`)

`docs/demo-runbook.md` liệt kê 8 mục; 5 mục ở giữa (2–6) là phần **thực thi
sống** có lệnh cụ thể — đây là "kịch bản demo 5 bước" bên dưới. Mục 1 (giới
thiệu kiến trúc) đi trước, không có lệnh; mục 7 (GitOps) và 8 (Q&A) đi sau,
xem `runbooks/gitops-rollback.md` cho mục 7.

### Bước 1 — Happy path (mục 2 runbook)
```text
uv run lab28 seed --via-gateway
uv run pytest integration-tests/test_j1_golden_path.py -q
```
Output cần chỉ ra:
- Dòng tổng kết pytest: `N passed` (các test `@pytest.mark.gpu` có thể `skipped` nếu
  chưa nối vLLM thật — không phải fail).
- 3 file evidence mới: `evidence/ip01-kafka-consume.json`,
  `evidence/ip02-airflow-run.json`, `evidence/ip04-feast-online.json`.
- Trong `ip02-airflow-run.json`: `state: "success"`, danh sách `task_instances`
  toàn `success`/`skipped`.

### Bước 2 — Trace (mục 3 runbook)
```text
uv run pytest integration-tests/test_j5_trace_metrics_continuity.py -q
```
Output cần chỉ ra:
- `evidence/ip10-trace.json` có `required_spans_missing: []`.
- Mở `http://localhost:16686`, tìm theo `trace_id` trong file trên, đối chiếu
  span list với `required_spans` trong `contracts/integration-matrix.yaml`
  (11 tên, ví dụ `lab28.gateway.request`, `lab28.kafka.consume`,
  `lab28.spark.delta_merge`).

### Bước 3 — Golden signals (mục 4 runbook)
```text
# Không có lệnh CLI riêng — mở trực tiếp Grafana/Prometheus:
```
Mở `http://localhost:3000`, dashboard **"Lab 28 Platform Overview"**, 4 panel
đúng tên 4 golden signal:
- Panel **"Request rate"** — output cần chỉ: đường số request/giây > 0 trong
  lúc Bước 1/2 vừa chạy.
- Panel **"Errors"** — output cần chỉ: gần 0 (đúng alert `Lab28HighErrorRatio`
  trong `monitoring/alerts.yml` là tỉ lệ 5xx > 5%).
- Panel **"Request duration"** — output cần chỉ: p95/p99 có dữ liệu, không
  phẳng ở 0.
- Panel **"Pipeline saturation / lag"** — output cần chỉ: `lab28_consumer_lag`
  (Kafka lag, IP02) giảm về gần 0 sau khi Airflow tiêu thụ hết batch.
Saturation CPU/RAM không có exporter trong stack này (không có cAdvisor/node
exporter trong `compose.yaml`) — nếu cần, dùng `docker compose stats` thủ công
ở terminal riêng, không phải Grafana.

### Bước 4 — Incident (mục 5 runbook)
```text
# Theo runbooks/failure-injection.md, ví dụ chọn "Qdrant down":
docker compose stop qdrant
uv run lab28 ready
# ... quan sát, ghi lại theo INCIDENT.md ...
docker compose start qdrant
uv run lab28 ready
```
Output cần chỉ ra:
- `lab28 ready` lúc lỗi: `status: "not_ready"`, mã 503, `components[].name ==
  "qdrant"` có `ready: false` và `detail` giải thích.
- `lab28 ready` sau khi khôi phục: quay lại đúng `status` như trước khi inject.
- Bằng chứng no-data-loss: đếm lại Delta rows cho asker_id/test run liên quan
  trước/sau (theo mẫu `test_j4_degraded_recovery.py`), số dòng không đổi/không
  mất bản ghi hợp lệ nào.
- Điền toàn bộ output thật vào `INCIDENT.md` (đã có sẵn khung `TODO` ở gốc repo).

### Bước 5 — Promotion/rollback (mục 6 runbook)
```text
uv run pytest integration-tests/test_j3_promotion_rollback.py -q
```
Output cần chỉ ra:
- `evidence/ip06-mlflow-release.json` có `version` mới lớn hơn
  `promoted_from`.
- Trong lúc test chạy (nếu đứng xem live ở MLflow UI, hoặc chạy `lab28 ask`
  song song): metric `lab28_release_version_info{...}` trên
  `http://localhost:8000/metrics` chỉ có đúng 1 series đang active.
- Cuối test: alias `champion` quay lại đúng version trước đó (module tự phục
  hồi) — xác nhận lại trên MLflow UI (`http://localhost:5000`) rằng alias đã
  trở về version ban đầu.

---

## 3. Danh sách screenshot cần chụp

| # | URL | Cần thấy gì trong ảnh | Tên file |
|---|---|---|---|
| 1 | Sơ đồ kiến trúc (file tĩnh, không phải URL chạy — ví dụ artefact trong `docs/`) | 5 lớp, người phụ trách từng lớp, 10 điểm kết nối được đánh số IP01–IP10 | `01-architecture.png` |
| 2 | `http://localhost:8082` → DAG `lab28_ingestion_pipeline` → run detail | Run mới nhất màu xanh (`success`), toàn bộ task instance thành công, `dag_run_id` đọc được | `ip02-airflow-dagrun.png` |
| 3 | `http://localhost:6333/dashboard` → collection `lab28_documents` | Point count > 0, có thể click 1 point thấy payload chứa `doc_id` vừa gửi | `ip05-qdrant-collection.png` |
| 4 | `http://localhost:5000` → Models → `lab28-rag-release` | Cột Aliases hiển thị `champion` gắn với version mới nhất, tag đầy đủ (prompt/model/collection) | `ip06-mlflow-champion.png` |
| 5 | `http://localhost:8001/version` và `http://localhost:8001/v1/models` (chỉ khi có GPU thật) | Response JSON cho thấy đây là build vLLM thật + đúng model id đã pin | `ip07-vllm-identity.png` |
| 6 | `http://localhost:9901/stats/prometheus` (lọc `envoy_http_local_rate_limit_rate_limited`) hoặc DevTools Network tab khi burst request | Giá trị counter > 0, hoặc 1 response 429 kèm header `x-request-id` | `ip08-gateway-ratelimit.png` |
| 7 | `http://localhost:9090/targets` | Toàn bộ target liệt kê trạng thái `UP` (trừ `lab28-vllm-optional` nếu không chạy GPU) | `ip09-prometheus-targets.png` |
| 8 | `http://localhost:3000` → dashboard "Lab 28 Platform Overview" | Cả 4 panel (Request rate / Errors / Request duration / Pipeline saturation-lag) có dữ liệu, không phẳng ở 0 | `ip09-grafana-overview.png` |
| 9 | `http://localhost:16686` → trace theo `trace_id` trong `evidence/ip10-trace.json` | Timeline trace với các span tên `lab28.*`, nhiều service (process) khác nhau trên cùng 1 trace | `ip10-jaeger-trace.png` |
| 10 | `http://localhost:8000/ready` (hoặc `http://localhost:8080/ready` qua gateway) trong lúc đang inject lỗi ở Bước 4 | `status` là `"degraded"` hoặc `"not_ready"`, component tương ứng có `ready: false` và `detail` | `incident-ready-degraded.png` |
| 11 | Cùng URL trên, sau khi chạy lệnh khôi phục | `status` quay lại đúng baseline ban đầu | `incident-ready-recovered.png` |
| 12 | Terminal (không phải trình duyệt): output của `uv run lab28 evidence` | Danh sách file đã ghi (`ip03-delta-history.json`, `ip05-qdrant-search.json`, `ip06-mlflow-release.json`, `ip07-vllm-identity.json`) — dùng thay cho screenshot UI vì IP03/IP04 không có UI web | `ip03-ip04-evidence-terminal.png` |

Lưu ảnh vào `docs-nop-bai/screenshots/` (thư mục con, tự tạo khi chụp — chưa
tồn tại tại thời điểm viết file này).

---

## 4. Thứ tự chạy tối ưu (không phải seed lại nhiều lần)

Lý do chung: `lab28 seed` / `lab28 index` / `lab28 release` chỉ cần chạy
**một lần** ở đầu buổi demo. Mọi test trong `integration-tests/` tự sinh dữ
liệu riêng với suffix duy nhất (`stack.run_id()` → `it-j1-<8 hex>`,...), nên
chạy lại các test không đụng nhau và không cần seed lại. Chỉ 2 điều cần xếp
đúng chỗ: (a) `test_j4_degraded_recovery.py` tự dừng/khởi động container nên
nên chạy **cuối cùng**, để không có test khác đang chờ mạng trong lúc dependency
bị stop; (b) `lab28 evidence` (CLI) nên chạy **sau khi mọi pytest đã xong**, vì
nó đọc trạng thái champion/registry hiện tại — chạy trước sẽ chụp một
champion tạm thời do `test_j3_promotion_rollback.py` tạo ra rồi tự rollback.

1. `docker compose --env-file ports.template --profile full up -d --build --wait`
   (thêm `--profile gpu` nếu demo có vLLM thật) — khởi động toàn bộ **một lần**.
2. `uv run lab28 topics`
3. `uv run lab28 index --source file` — seed Qdrant **một lần**.
4. `uv run lab28 release` — tạo champion đầu tiên trên MLflow **một lần**.
5. `uv run lab28 seed --via-gateway` — seed feedback/document ban đầu **một lần**.
6. `uv run lab28 ready` — xác nhận baseline `ready`/`degraded` trước khi demo
   (ghi lại làm baseline cho Bước 4 — Incident).
7. **Bước 1 (Happy path)**: `uv run pytest integration-tests/test_j1_golden_path.py -q`
8. **Bước 2 (Trace)**: `uv run pytest integration-tests/test_j5_trace_metrics_continuity.py -q`
   (và `test_trace_span_coverage.py -q -m gpu` nếu có GPU, để có đủ 11 span)
9. `uv run pytest integration-tests/test_j2_idempotent_replay.py -q` — chứng
   minh idempotency (không nằm trong 5 bước runbook nhưng cùng nhóm dữ liệu
   với J1, nên chạy sát J1 để mạch demo liền nhau).
10. `uv run pytest integration-tests/test_gateway_rate_limit.py -q` và
    `uv run pytest integration-tests/test_prometheus_targets.py -q` — độc
    lập dữ liệu, chạy khi nào cũng được, xếp ở đây cho gọn vòng IP08/IP09.
11. **Bước 5 (Promotion/rollback)**: `uv run pytest integration-tests/test_j3_promotion_rollback.py -q`
    — chạy sau Bước 1/2 để evidence IP06 phản ánh đúng lần promote thật trong
    buổi demo, và vì nó tự phục hồi alias nên không ảnh hưởng các bước sau.
12. **Bước 4 (Incident)** — chạy **cuối cùng** trong số các bước sống:
    ```text
    docker compose stop qdrant   # hoặc feast/kafka/vllm, theo INCIDENT.md
    uv run lab28 ready
    docker compose start qdrant
    uv run lab28 ready
    ```
    (Có thể thay bằng `uv run pytest integration-tests/test_j4_degraded_recovery.py -q`
    để tự động hoá toàn bộ chuỗi inject → quan sát → khôi phục → no-data-loss.)
13. `uv run lab28 evidence` — chụp evidence IP03/IP05/IP06/IP07 +
    `integration-report.json` ở trạng thái ổn định cuối cùng.
14. `uv run lab28 integration` — in bảng điểm 10 IP.
15. `uv run python load-tests/run_profile.py --requests 200 --workers 8 > evidence/perf-8w.json`
    rồi lặp `--workers 16` — đo hiệu năng sau khi stack đã ổn định trở lại,
    tránh số liệu bị nhiễu bởi Bước 4 (Incident) đang chạy song song.
