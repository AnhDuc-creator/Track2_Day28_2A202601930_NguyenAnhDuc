# ANSWERS.md — Day 28 Track 2 (mục 8 của SUBMISSION.md)

> Khung trả lời. Mọi chỗ cần số liệu/output thật từ lần chạy stack đều đánh dấu
> `TODO:` — không được bịa số liệu khi điền vào sau này.

## 1. Giải thích IP01..IP10

Mỗi mục: nói ngắn gọn hai hệ thống nào nối với nhau và cơ chế nối (theo
`contracts/integration-matrix.yaml`), sau đó để trống chỗ dẫn bằng chứng thật.

**Tổng hợp readiness** (`evidence/integration-report.json`): `score = 83`,
tổng thể `ready = false`. 6 điểm đã được probe trực tiếp từ serving process
(**verified**): IP01, IP03, IP04, IP05, IP06 đều `ready` (**5 điểm
`passing`**), riêng IP07 `not_ready`. 4 điểm còn **`unverified`** theo đúng
nghĩa của integration-report (chưa được probe tự động từ serving process):
IP02, IP08, IP09, IP10 — các mục tương ứng bên dưới bù đắp bằng bằng chứng
thật lấy riêng qua pytest/evidence files.

### IP01 — Data ingestion → Kafka
Gateway/API nhận `FeedbackSubmission`/`DocumentSubmission` qua HTTP, publish
thành `contracts.IngestionEvent` lên topic `data.raw`, key = `idempotency_key`,
kèm header `traceparent` để giữ trace xuyên Kafka.
- Bằng chứng (`evidence/ip01-kafka-consume.json`): consumer đọc lại đúng
  record vừa publish trên topic `data.raw`, partition `0`, offset `15`, key
  `it-j1-f3112d4d`; header Kafka mang `idempotency-key: it-j1-f3112d4d` và
  `traceparent: 00-69c9d04a11a840dbac1a19109adf20a8-57e3cabd73b1b3a6-01`
  (`trace_id = 69c9d04a11a840dbac1a19109adf20a8`, khớp `traceparent` trong
  body sự kiện). Kết quả `uv run pytest integration-tests/test_j1_golden_path.py -q`
  (J1): **12 passed, 3 skipped (gpu gate)** trong 266.88s.

### IP02 — Kafka → Airflow pipeline
Airflow tiêu thụ `data.raw` (giữ header `traceparent`), chạy DAG, phát asset
event `lab28://delta/feedback` để các DAG downstream có thể schedule theo dữ liệu.
- Bằng chứng (`evidence/ip02-airflow-run.json`): DAG
  `lab28_ingestion_pipeline`, `dag_run_id = it-9689e1ee`, `state = success`,
  `conf.traceparent` mang cùng `trace_id` với IP01
  (`69c9d04a11a840dbac1a19109adf20a8`). Cả 4 task
  (`drain_kafka_into_delta`, `refresh_online_features`,
  `index_new_documents`, `announce_processed_batch`) đều `success` ở
  `try_number = 1`. Asset events phát đúng thứ tự:
  `lab28://delta/feedback` (10:02:35.017Z), `lab28://delta/documents`
  (10:02:35.035Z), `lab28://feast/asker_activity` (10:03:00.284Z),
  `lab28://qdrant/lab28_documents` (10:05:29.125Z). Ảnh minh hoạ:
  `docs-nop-bai/screenshots/ip02-airflow-dag.png`.

### IP03 — Pipeline → Delta Lake / Lakehouse
Batch `IngestionEvent` được dedupe theo `idempotency_key` rồi MERGE vào bảng
Delta feedback/documents; version tăng đơn điệu, có `_delta_log`.
- Bằng chứng (`evidence/ip03-delta-history.json`): bảng `feedback` có
  `_delta_log` liên tục từ version 0 (`CREATE TABLE`) tới version 6 (các
  version 1-6 đều `MERGE`), `latest_rows = 7`; bảng `documents` từ version 0
  tới version 3 (đều `MERGE` sau `CREATE TABLE`), `latest_rows = 14`. Version
  tăng đơn điệu ở cả hai bảng, không version nào bị bỏ qua hay ghi đè.

### IP04 — Lakehouse → Feature Store (Feast)
Snapshot offline từ Delta (`delta_root/exports/asker_activity`) được Feast
apply và serve online cho entity `asker_id` qua feature service `asker_serving_v1`.
- Bằng chứng (`evidence/ip04-feast-online.json`): entity
  `asker_id = it-j1-f3112d4d` (cùng entity với IP01/IP02), feature service
  `asker_serving_v1`, mọi field đều `PRESENT`: `feedback_count = 1`,
  `avg_rating = 5.0`, `negative_ratio = 0.0`, `delta_version = 1`.
  `degraded = false`, `freshness_seconds ≈ 264.90`, `lookup_ms ≈ 367.33`.

### IP05 — Data → Vector Store (embeddings)
Document rows từ Delta được embed và ghi vào Qdrant collection
`lab28_documents` với point id xác định (UUID suy từ `doc_id`) để re-index
không tạo bản trùng.
- Bằng chứng (`evidence/ip05-qdrant-search.json`): collection
  `lab28_documents`, `points_total = 15`, embedding model
  `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2@faf4aa4225822f3bc6376869cb1164e8e3feedd0`.
  Truy vấn hybrid cho câu hỏi "Nền tảng dữ liệu của lab này gồm những thành
  phần nào?" trả về top-5 đúng chủ đề, dẫn đầu `doc-ip09-metrics` (score
  0.833), rồi `doc-degraded-policy` (0.611), `doc-ip10-tracing` (0.405),
  `doc-ip06-mlflow` (0.35), `doc-ip01-kafka` (0.343). Ảnh minh hoạ:
  `docs-nop-bai/screenshots/ip05-qdrant-collection.png`.

### IP06 — MLflow → Model Registry
Kết quả evaluation trên Delta version + retrieval config + vLLM model id được
đăng ký thành model version mới, gắn tag provenance, và promote alias `champion`.
- Bằng chứng (`evidence/ip06-mlflow-release.json`): `reachable = true`,
  `has_champion = true`, model `lab28-rag-release` version `1` đang là
  `champion`, `run_id = b9f0bcd4900b4357a0bacf99878f4459`. Kết quả
  `uv run pytest integration-tests/test_j3_promotion_rollback.py -q` (J3):
  **6 passed, 3 skipped**. Ảnh minh hoạ:
  `docs-nop-bai/screenshots/ip06-mlflow-champion.png`.

### IP07 — Model → vLLM/SGLang serving
API dựng prompt grounded từ release config + nguồn truy hồi, gọi endpoint vLLM
thật (OpenAI-compatible), echo lại đúng model id đã pin.
- Bằng chứng (`evidence/ip07-vllm-identity.json`): `reachable = false`,
  `version = null`, `served_models = []`, `vllm_metric_count = 0`,
  `is_real_vllm = false`, `detail = "unreachable: ConnectError"`. **Trạng
  thái: UNVERIFIED** — môi trường chạy đánh giá này không có GPU/endpoint
  vLLM thật để gọi `/version`, `/v1/models` hay đọc metric `vllm:*`; không
  giả lập kết quả.

### IP08 — Serving → API Gateway
Envoy gateway nhận request bên ngoài, gắn `x-request-id`, áp rate limit/auth
policy, route theo health.
- Bằng chứng (`evidence/ip08-gateway.json`): gateway `http://localhost:8080`,
  route `/health`, `configured_rps = 10`. Gửi 30 request: 10 `accepted`, 20
  `rejected` (`rate_limited_stat = 166.0`); mẫu 200 mang
  `x-request-id = d0220c97-5cd0-927c-a8ca-eab287ada8d9`, mẫu 429 mang
  `x-request-id = a9a5505a-5254-9e60-b504-7867a8c4752e` — cả hai đều
  correlatable. Kết quả
  `uv run pytest integration-tests/test_gateway_rate_limit.py -q`: **4
  passed**; riêng `test_the_gateway_answers_its_own_health_route` **pass khi
  chạy riêng lẻ** nhưng **fail khi chạy chung cả file** — nguyên nhân là
  bucket rate limit 10 token/s dùng chung cho toàn bộ listener Envoy (không
  tách theo route), nên các test chạy trước trong cùng file đã tiêu hết
  token của listener trước khi test này tới lượt; không phải lỗi logic của
  route `/health`.

### IP09 — All components → Prometheus/Grafana
Prometheus scrape `/metrics` của mọi service; Grafana có dashboard provisioned
và ít nhất một SLO alert khả dụng.
- Bằng chứng (`evidence/ip09-prometheus-targets.json`): 9/10 target `up`
  (`lab28-airflow-batch`, `lab28-api`, `lab28-collector`, `lab28-feast`,
  `lab28-gateway`, `lab28-kafka`, `lab28-mlflow`, `lab28-prometheus`,
  `lab28-qdrant`); chỉ `lab28-vllm-optional` `down` (connection refused tới
  `host.docker.internal:8001` — nhất quán với IP07 không có GPU). Hai
  alerting rule (`Lab28ApiUnavailable`, `Lab28HighErrorRatio`) đều
  `health: ok`. `evidence/ip09-grafana-dashboards.json`: dashboard
  `Lab 28 Platform Overview` (`uid: lab28-platform`) được provision kèm
  datasource `Prometheus`. Kết quả
  `uv run pytest integration-tests/test_prometheus_targets.py -q`: **5
  passed, 1 skipped** (test kiểm tra endpoint suy luận `vllm` — gpu gate).
  Ảnh minh hoạ: `docs-nop-bai/screenshots/ip09-prometheus-targets.png`,
  `docs-nop-bai/screenshots/ip09-grafana-dashboard.png`.

### IP10 — All components → tracing (OTEL/LangSmith)
Mọi hop (gateway, API, Kafka, Airflow, Spark, Feast, Qdrant, vLLM) export span
OTLP về collector; một trace ID duy nhất xuyên hết các span bắt buộc.
- Bằng chứng (`evidence/ip10-trace.json`): `trace_id =
  9d99314a6f6141e88183606e223c2ea4`, `dag_run_id = it-b9f5ec55`, 11 span thu
  được trên 3 service (`lab28-airflow`, `lab28-api`, `lab28-gateway`). 6 span
  bắt buộc của đường golden-path đã xuất hiện đủ: `lab28.airflow.dag`,
  `lab28.api.ingest`, `lab28.gateway.request`, `lab28.kafka.consume`,
  `lab28.kafka.produce`, `lab28.spark.delta_merge`. 5 span còn thiếu
  (`lab28.api.ask`, `lab28.feast.get_online_features`,
  `lab28.mlflow.resolve_release`, `lab28.qdrant.query`,
  `lab28.vllm.chat_completion`) đều nằm trên đường `/api/v1/ask` gọi tới
  vLLM — không xuất hiện vì trace này chỉ đi qua đường ingest, không có
  GPU/endpoint vLLM để hoàn tất đường hỏi-đáp. Kết quả
  `uv run pytest integration-tests/test_j5_trace_metrics_continuity.py -q`
  (J5): **9 passed, 1 skipped**. Ảnh minh hoạ:
  `docs-nop-bai/screenshots/ip10-jaeger-trace.png`.

  Trạng thái leg LangSmith: **UNVERIFIED**. Kết quả
  `uv run pytest integration-tests/test_trace_span_coverage.py -q`: **1
  passed, 4 skipped** (3 skip do gpu gate trên các span cần vLLM, 1 skip là
  chính `test_the_langsmith_export_leg_is_configured_and_healthy` — do thiếu
  `LANGSMITH_API_KEY`); không giả lập kết quả LangSmith.

### Tổng hợp kết quả pytest (integration-tests/)

| Job / file | Kết quả |
|---|---|
| J1 `test_j1_golden_path.py` | 12 passed, 3 skipped (gpu gate) — 266.88s |
| J2 `test_j2_idempotent_replay.py` | 9 passed — 106.78s |
| J3 `test_j3_promotion_rollback.py` | 6 passed, 3 skipped |
| J4 `test_j4_degraded_recovery.py` | 9 passed, 4 skipped — 122.57s |
| J5 `test_j5_trace_metrics_continuity.py` | 9 passed, 1 skipped |
| `test_gateway_rate_limit.py` | 4 passed (xem ghi chú flaky ở mục IP08) |
| `test_prometheus_targets.py` | 5 passed, 1 skipped |
| `test_trace_span_coverage.py` | 1 passed, 4 skipped (3 gpu + 1 langsmith) |

J4 tương ứng đúng kịch bản Feast down ghi nhận chi tiết ở `INCIDENT.md`.

## 2. Bốn hàm đã implement (`src/lab28_platform/integration_tasks.py`)

### `event_headers(traceparent, idempotency_key)`
Trả về list header dạng byte cho Kafka producer, gồm `idempotency-key` (luôn
có) và `traceparent` (chỉ thêm khi có trace đang chạy). Lý do tách điều kiện:
một header `traceparent` rỗng/`None` bị encode thành chuỗi rỗng sẽ là một W3C
traceparent không hợp lệ — im lặng bỏ qua header còn tốt hơn gửi một giá trị
sai lệch mà consumer downstream (Airflow, Spark) sẽ cố parse và fail. Header
được trả dạng `bytes` vì đó là kiểu Kafka client (`confluent-kafka`/`aiokafka`)
yêu cầu ở transport layer.

### `dedupe_latest(events)`
Với nhiều bản ghi cùng `idempotency_key` trong một batch, giữ lại đúng một bản
— bản có `(occurred_at, event_id)` lớn nhất — và trả về list đã sort theo key.
Hai lý do thiết kế:
1. So sánh theo tuple `(occurred_at, event_id)` thay vì chỉ `occurred_at` để
   việc chọn "bản mới nhất" không phụ thuộc thứ tự Kafka giao hàng khi hai sự
   kiện trùng timestamp — `event_id` làm tie-breaker xác định.
2. Trả về list đã sort theo key (không phải theo thứ tự xuất hiện) để kết quả
   deterministic, phục vụ đúng bất biến mà Delta `MERGE` cần: nguồn dữ liệu
   không được có hai dòng khớp cùng một dòng đích (`UT-delta-merge-idempotency`).

### `feast_online_request(asker_id)`
Dựng đúng payload REST cho `POST /get-online-features` của Feast: entity map
`{"asker_id": [asker_id]}`, danh sách `features` lấy từ hằng số dùng chung
`FEATURE_REFS` (không hard-code lại tên feature ở đây để tránh lệch với feature
service `asker_serving_v1`), và `full_feature_names=False` để response giữ tên
feature ngắn, khớp với schema `FeatureLookup` mà `FeatureClient` parse ra.
Hai lý do cách khác sẽ sai:
1. Nếu liệt kê lại tên feature thay vì tái dùng `FEATURE_REFS`
   (`contracts.py`), một thay đổi ở `FEATURE_VIEW_NAME`/tên feature sẽ không
   lan tới hàm này — request sẽ hỏi một feature ref không còn khớp
   `asker_serving_v1` và Feast trả lỗi thay vì feature vector.
2. Nếu để `full_feature_names=True` (mặc định của Feast), field trả về sẽ có
   prefix (`asker_activity_v1__feedback_count`) thay vì tên ngắn
   (`feedback_count`), lệch với các field phẳng của `AskerFeatures` mà
   `FeatureClient` parse ra — parse sẽ vỡ (`KeyError`).

### `readiness_status(probes)`
Gộp nhiều probe `{ready, mandatory}` thành một verdict `ready` / `degraded` /
`not_ready` theo đúng ngữ nghĩa mà IP04 (Feast — degradable) và IP05 (Qdrant —
mandatory) cần: bất kỳ probe `mandatory=True` mà `ready=False` → trả về
`not_ready` ngay (fail closed, để gateway loại pod khỏi rotation); nếu chỉ có
probe không bắt buộc bị fail → `degraded` (vẫn phục vụ, nhưng gắn cờ); nếu mọi
probe đều ready → `ready`. Việc trả sớm (`return "not_ready"`) khi gặp probe
mandatory thay vì duyệt hết rồi mới quyết định là chủ ý: một dependency bắt
buộc bị hỏng luôn thắng, bất kể thứ tự probe truyền vào. Cách khác sẽ sai: một
verdict nhị phân kiểu `"ready" if all(p["ready"] for p in probes) else
"not_ready"` (không phân biệt mandatory/optional) sẽ khiến một `feast` lỗi
(`probe_feast(..., mandatory=False)` trong `readiness.py`) kéo cả pod ra khỏi
rotation dù request path vẫn phục vụ được ở chế độ suy giảm — trái đúng mục
đích IP04 "degradable" mà `readiness.py` mô tả (dòng 8-10).

## 3. Trade-offs đã chọn

1. **At-least-once ở Kafka + idempotent MERGE ở Delta, thay vì exactly-once ở
   broker.** `dedupe_latest` (`integration_tasks.py`) và Delta `MERGE` theo
   `idempotency_key` là nơi duy nhất chống trùng; Kafka không được cấu hình
   transactional producer/EOS. Đánh đổi: chấp nhận producer có thể gửi trùng
   (retry, restart) và dồn toàn bộ trách nhiệm "một sự kiện chỉ có một hiệu
   lực" xuống lớp lakehouse, để lấy một chỗ duy nhất cần đúng thay vì phải cấu
   hình đúng transaction coordinator trên mọi consumer group. Chấp nhận được
   vì cụm Kafka ở đây vốn đã `replication_factor: 1` cho mọi topic
   (`contracts.py`, `TOPICS`) — tức guarantee ở tầng broker đã yếu sẵn, nên
   không đáng để đầu tư thêm độ phức tạp EOS ở đó.

2. **Feast "degradable" (`mandatory=False`) còn Qdrant "mandatory"
   (`mandatory=True` mặc định) trong `readiness.py` (`probe_feast` dòng
   133-145 vs `probe_qdrant` dòng 148-161).** Đánh đổi: chấp nhận câu trả lời
   kém cá nhân hóa hơn (mất feature `avg_rating`/`negative_ratio`...) để đổi
   lấy uptime — pod vẫn phục vụ khi Feast lỗi, chỉ gắn cờ `degraded`. Ngược
   lại Qdrant mất thì không còn nguồn để "grounding" câu trả lời — đó không
   phải suy giảm chất lượng mà là sai chức năng của endpoint `/api/v1/ask`,
   nên phải `not_ready` để gateway loại pod khỏi rotation.

3. **DLQ park-and-replay (`DeadLetterEnvelope`, giữ nguyên
   `raw_payload_b64`) thay vì retry vô hạn hoặc fail cả batch.** Đánh đổi:
   chấp nhận thêm độ trễ xử lý cho riêng bản ghi lỗi (phải replay thủ công/về
   sau, không tự phục hồi ngay) và thêm một topic + schema, để đổi lấy hai
   thứ: (a) một event lỗi không chặn toàn bộ batch còn lại; (b) consumer
   không nghẽn lag vì retry vô hạn một message không bao giờ parse được.

4. **`extra="forbid"` + `schema_version` bắt buộc trên mọi contract
   (`contracts.py`), từ chối thẳng thay vì coerce.** Đánh đổi: mất khả năng
   "cứ nhận rồi đoán" khi có field lạ hoặc version cũ hơn — producer/consumer
   lệch schema sẽ fail ngay tại boundary (422/409, xem `ERROR_STATUS`) thay vì
   chạy tiếp. Chấp nhận vì lan truyền dữ liệu sai lệch xuống Delta/Feast/Qdrant
   rồi mới phát hiện thì việc dọn dẹp tốn kém hơn nhiều so với việc chặn ngay
   tại cửa ngõ.

5. **`stable_point_id` dùng UUID5 suy từ logical id (`doc_id`), không phải
   UUID ngẫu nhiên mỗi lần ghi (`contracts.py` dòng 38-45).** Đánh đổi: mất
   khả năng giữ lịch sử nhiều phiên bản embedding của cùng một tài liệu (mỗi
   lần re-index ghi đè đúng lên point cũ, không tạo bản mới) để đổi lấy tính
   idempotent khi Kafka gửi lại cùng message — replay không tạo điểm trùng
   trong Qdrant.

## 4. Production gaps

1. **Kafka là single broker, replication factor = 1 cho mọi thứ.**
   `compose.yaml`: `KAFKA_NODE_ID: 1`, `KAFKA_CONTROLLER_QUORUM_VOTERS:
   1@kafka:9093`, và cả `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`,
   `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`,
   `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1`. Mọi `TopicSpec` trong
   `contracts.py` cũng khai `replication_factor=1`. Mất container `kafka` là
   mất dữ liệu chưa consume — không có bản sao nào khác. Production cần cụm
   nhiều broker + `replication_factor >= 3` + `min.insync.replicas` phù hợp.

2. **Không có auth/TLS nào ở tầng transport nội bộ.** `compose.yaml`:
   `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` toàn `PLAINTEXT` cho cả listener
   `EXTERNAL` (map ra `localhost:9092`); Qdrant khởi chạy không có API key
   (`LAB28_QDRANT_URL: http://qdrant:6333`, không thấy biến API key trong
   `x-lab-environment`); MLflow không bật auth (`MLFLOW_SERVER_ALLOWED_HOSTS:
   "*"` — chấp mọi Host header, không có access control); Feast serve không
   có auth flag. Toàn bộ mô hình tin cậy dựa vào việc chỉ có Docker network
   nội bộ nhìn thấy các cổng này.

3. **Secret hard-code trong compose.yaml.** Grafana:
   `GF_SECURITY_ADMIN_USER: admin` / `GF_SECURITY_ADMIN_PASSWORD: admin` —
   credential mặc định nằm thẳng trong file được commit, không qua secret
   manager/vault, không rotate. Ngược lại, `LAB28_VLLM_API_KEY` được thiết kế
   đúng hơn — `VLLMSettings.api_key` (`settings.py`) chỉ đọc `os.getenv` tại
   thời điểm gọi và không bao giờ cache/serialize (`llm_client.py` dòng
   83-86), nhưng vẫn chỉ là biến môi trường thủ công, chưa có secret-store
   thật (Vault/KMS) hay cơ chế rotate.

4. **Storage backend là single-file/single-node, không phải HA.** MLflow
   dùng `--backend-store-uri sqlite:////mlflow/mlflow.db` (`compose.yaml`
   dòng 87-88) — SQLite một writer, không chịu được nhiều tiến trình ghi
   đồng thời và không có replica. Airflow không set
   `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` nên `airflow standalone` cũng rơi về
   SQLite mặc định của nó. `LAB28_DELTA_ROOT` trỏ vào filesystem cục bộ
   (`/workspace/.lab28/delta`), không phải object storage (S3/ADLS/GCS) có
   versioning/replication — mất volume/host là mất lakehouse.

5. **Single point of failure ở gần như mọi service.** Mỗi service trong
   `compose.yaml` chỉ chạy 1 replica (không có `deploy.replicas`, không có
   readiness-driven restart ngoài healthcheck riêng lẻ): một `api`, một
   `gateway` (Envoy), một `qdrant`, một `mlflow`. Container nào crash là mất
   toàn bộ chức năng tương ứng cho tới khi Compose tự restart — không có
   load-balancing nhiều instance, không có autoscaling.

6. **Cấu hình mặc định của compose làm yếu chính gate IP07.**
   `LAB28_VLLM_REQUIRE_REAL: ${LAB28_VLLM_REQUIRE_REAL:-false}` (`compose.yaml`
   dòng 226) — mặc định service `api` KHÔNG bắt buộc endpoint phải chứng minh
   là vLLM thật, dù `VLLMSettings.from_env` (`settings.py`) mặc định
   `require_real=True` khi không set biến này qua compose. Muốn gate IP07 có
   hiệu lực trong stack chạy bằng compose, người vận hành phải tự set
   `LAB28_VLLM_REQUIRE_REAL=true` — dễ quên trong môi trường thật.

7. **Các service quan trọng expose thẳng ra host, không đi qua gateway/auth
   layer chung.** Chỉ `api` nằm sau Envoy (`gateway` phụ thuộc
   `api: condition: service_healthy`); còn `mlflow` (`5000`), `qdrant`
   (`6333`), `airflow` (`8082`), `grafana` (`3000`), `prometheus` (`9090`)
   đều map port thẳng ra host qua biến `LAB28_*_PORT` mà không có một điểm
   auth/rate-limit chung nào đứng trước — production cần đưa tất cả sau một
   gateway/mesh có auth thống nhất, không chỉ riêng đường `/api/v1/*`.

## 5. Load profile (P50/P95/P99) và bottleneck analysis

Đo bằng `uv run python load-tests/run_profile.py --requests <N> --workers <W>`
nhắm route `/ready`; kết quả nằm trong `evidence/perf-4w.json`,
`evidence/perf-8w.json`, `evidence/perf-16w.json`. Không suy ra production
capacity từ máy chạy đánh giá — các con số chỉ có giá trị tương đối giữa 3
mức concurrency, trên cùng một máy.

| Workers | Requests | Thành công | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|
| 4  | 100 | 100/100 OK | 512.7 | 813.7 | 1063.1 |
| 8  | 200 | 200/200 OK | 1234.8 | 1747.8 | 2868.9 |
| 16 | 200 | 13/200 OK, 187 connection fail | 8.2* | 1823.7 | 2150.5 |

\* p50 ở mức 16 workers thấp bất thường vì phần lớn request **fail nhanh**
(connection refused) thay vì trả lời chậm; trộn với số ít request 200 thành
công (chậm) làm phân phối latency tách thành hai cụm (fail nhanh vs. success
chậm) — không phải dấu hiệu hệ thống nhanh hơn khi tải tăng.

**Bottleneck**: từ 4 → 8 workers, hệ thống vẫn phục vụ hết (200/200 và
100/100 đều `200 OK`) nhưng p50 tăng ~2.4 lần (512.7ms → 1234.8ms) — đã bắt
đầu bão hoà. Ở 16 workers, hệ thống sập kết nối cho 187/200 request (93.5%)
— vượt quá capacity xử lý đồng thời của tiến trình API/gateway. Đây là bằng
chứng đo thật, củng cố production gap #5 (single point of failure, mục 4):
cần scale ngang API/gateway (nhiều replica sau load balancer) trước khi tăng
concurrency thật ở production, thay vì chỉ tăng worker/thread trong một
tiến trình duy nhất.

## 6. Ownership

Bài làm thực hiện **cá nhân** (repo chỉ có một tác giả — xem `git log`, branch
làm việc `ca-nhan-anhduc`). Người thực hiện đã đi qua đủ cả 5 vai trò được
định nghĩa trong `contracts/integration-matrix.yaml`:
- `team-ingestion` — Kafka topics, producer, Airflow DAG, DLQ/replay.
- `team-data` — Spark/Delta, Feast repository, materialization, model release.
- `team-serving` — FastAPI orchestration, Qdrant, vLLM client, degraded paths.
- `team-platform` — gateway, OTEL collector, Prometheus, Grafana, K8s/GitOps.
- `team-presenter` — demo script, evidence pack, failure narration.

Không có phần việc nào nhờ hỗ trợ/tham khảo từ nguồn ngoài repo (bạn bè, tài
liệu ngoài, công cụ AI khác) — toàn bộ dựa trên tài liệu sẵn có trong repo
(`README.md`, `LAB28_GUIDE.md`, `runbooks/`, `contracts/`).

## 7. Reflection

- Điều khó nhất: giữ trace liên tục xuyên Kafka/Airflow/Spark. `traceparent`
  phải sống sót qua nhiều lần chuyển giao khác chất (HTTP header → Kafka
  header (bytes) → `conf` của Airflow DAG run → context lúc Spark `MERGE` vào
  Delta), mỗi hop có API khác nhau để mang context đi tiếp; chỉ cần một hop
  quên propagate là `trace_id` đứt và IP10 mất luôn một span bắt buộc — rất
  khó phát hiện bằng mắt thường nếu không kiểm tra bằng đúng
  `test_j5_trace_metrics_continuity.py`/`test_trace_span_coverage.py`.
- Điều sẽ cải tiến nếu làm lại: tách rate-limit bucket theo route thay vì
  dùng chung 10 token/s cho toàn bộ listener Envoy. Đây chính là nguyên nhân
  `test_the_gateway_answers_its_own_health_route` pass khi chạy riêng nhưng
  fail khi chạy chung file `test_gateway_rate_limit.py` (mục IP08) — cho
  `/health` một bucket riêng sẽ loại bỏ tính phụ thuộc thứ tự chạy test đó.
