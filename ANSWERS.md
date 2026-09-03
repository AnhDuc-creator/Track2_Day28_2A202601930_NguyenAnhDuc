# ANSWERS.md — Day 28 Track 2 (mục 8 của SUBMISSION.md)

> Khung trả lời. Mọi chỗ cần số liệu/output thật từ lần chạy stack đều đánh dấu
> `TODO:` — không được bịa số liệu khi điền vào sau này.

## 1. Giải thích IP01..IP10

Mỗi mục: nói ngắn gọn hai hệ thống nào nối với nhau và cơ chế nối (theo
`contracts/integration-matrix.yaml`), sau đó để trống chỗ dẫn bằng chứng thật.

### IP01 — Data ingestion → Kafka
Gateway/API nhận `FeedbackSubmission`/`DocumentSubmission` qua HTTP, publish
thành `contracts.IngestionEvent` lên topic `data.raw`, key = `idempotency_key`,
kèm header `traceparent` để giữ trace xuyên Kafka.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip01-kafka-consume.json`
  (topic, key, partition, offset, trace_id) sau khi chạy
  `uv run pytest integration-tests/test_j1_golden_path.py -q`.

### IP02 — Kafka → Airflow pipeline
Airflow tiêu thụ `data.raw` (giữ header `traceparent`), chạy DAG, phát asset
event `lab28://delta/feedback` để các DAG downstream có thể schedule theo dữ liệu.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip02-airflow-run.json`
  (dag_run_id, task states, asset_events).

### IP03 — Pipeline → Delta Lake / Lakehouse
Batch `IngestionEvent` được dedupe theo `idempotency_key` rồi MERGE vào bảng
Delta feedback/documents; version tăng đơn điệu, có `_delta_log`.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip03-delta-history.json`
  (từ `uv run lab28 evidence` — transaction log, time-travel diff).

### IP04 — Lakehouse → Feature Store (Feast)
Snapshot offline từ Delta (`delta_root/exports/asker_activity`) được Feast
apply và serve online cho entity `asker_id` qua feature service `asker_serving_v1`.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip04-feast-online.json`
  (feature row, delta_version, freshness_seconds).

### IP05 — Data → Vector Store (embeddings)
Document rows từ Delta được embed và ghi vào Qdrant collection
`lab28_documents` với point id xác định (UUID suy từ `doc_id`) để re-index
không tạo bản trùng.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip05-qdrant-search.json`
  (từ `uv run lab28 evidence` — kết quả truy vấn hybrid, điểm số, doc_id).

### IP06 — MLflow → Model Registry
Kết quả evaluation trên Delta version + retrieval config + vLLM model id được
đăng ký thành model version mới, gắn tag provenance, và promote alias `champion`.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip06-mlflow-release.json`
  (version, run_id, git sha, delta version) sau khi chạy
  `uv run pytest integration-tests/test_j3_promotion_rollback.py -q`.

### IP07 — Model → vLLM/SGLang serving
API dựng prompt grounded từ release config + nguồn truy hồi, gọi endpoint vLLM
thật (OpenAI-compatible), echo lại đúng model id đã pin.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip07-vllm-identity.json`
  (`/version`, `/v1/models`, các metric tên `vllm:`) — **yêu cầu GPU thật**,
  báo `UNVERIFIED` nếu môi trường không có GPU.

### IP08 — Serving → API Gateway
Envoy gateway nhận request bên ngoài, gắn `x-request-id`, áp rate limit/auth
policy, route theo health.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip08-gateway.json`
  (response 200 + 429 kèm `x-request-id`) sau khi chạy
  `uv run pytest integration-tests/test_gateway_rate_limit.py -q`.

### IP09 — All components → Prometheus/Grafana
Prometheus scrape `/metrics` của mọi service; Grafana có dashboard provisioned
và ít nhất một SLO alert khả dụng.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip09-prometheus-targets.json`
  và `evidence/ip09-grafana-dashboards.json` sau khi chạy
  `uv run pytest integration-tests/test_prometheus_targets.py -q`.

### IP10 — All components → tracing (OTEL/LangSmith)
Mọi hop (gateway, API, Kafka, Airflow, Spark, Feast, Qdrant, vLLM) export span
OTLP về collector; một trace ID duy nhất xuyên hết các span bắt buộc.
- Bằng chứng: TODO: dán nội dung thật của `evidence/ip10-trace.json`
  (trace_id, span_names, required_spans_present/missing) sau khi chạy
  `uv run pytest integration-tests/test_j5_trace_metrics_continuity.py -q`.
  TODO: ghi rõ trạng thái leg LangSmith (`UNVERIFIED` nếu thiếu
  `LANGSMITH_API_KEY`).

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

## 5. Ownership

Bài làm thực hiện **cá nhân** (repo chỉ có một tác giả — xem `git log`, branch
làm việc `ca-nhan-anhduc`). Người thực hiện đã đi qua đủ cả 5 vai trò được
định nghĩa trong `contracts/integration-matrix.yaml`:
- `team-ingestion` — Kafka topics, producer, Airflow DAG, DLQ/replay.
- `team-data` — Spark/Delta, Feast repository, materialization, model release.
- `team-serving` — FastAPI orchestration, Qdrant, vLLM client, degraded paths.
- `team-platform` — gateway, OTEL collector, Prometheus, Grafana, K8s/GitOps.
- `team-presenter` — demo script, evidence pack, failure narration.

TODO: nếu có phần việc nhờ hỗ trợ/tham khảo từ nguồn khác, ghi rõ ở đây.

## 6. Reflection

- Điều khó nhất: TODO: mô tả cụ thể (ví dụ: giữ trace liên tục qua Kafka, dựng
  DLQ replay an toàn, GPU gate, v.v.) — viết thật, không suy đoán hộ.
- Điều sẽ cải tiến nếu làm lại: TODO: mô tả cụ thể.
