# GitOps & Kubernetes Evidence — lab28-platform

Nguồn đọc: `deploy/kubernetes/base/*.yaml`, `deploy/kubernetes/base/kustomization.yaml`,
`gitops/application.yaml`, `scripts/validate_manifests.py`, `runbooks/gitops-rollback.md`.
Tài liệu này chỉ **mô tả những gì đã có trong repo**; không có manifest nào bị sửa,
không có lệnh docker nào được chạy để tạo ra tài liệu này.

## 1. Manifest có gì (`deploy/kubernetes/base/`)

`kustomization.yaml` liệt kê đúng 7 file, theo thứ tự apply:

| # | File | Kind | Nội dung chính |
|---|------|------|----------------|
| 1 | `namespace.yaml` | `Namespace` | tạo namespace `lab28`, gắn nhãn `pod-security.kubernetes.io/enforce: restricted` (Pod Security Admission ở mức nghiêm ngặt nhất cho toàn namespace) |
| 2 | `service-account.yaml` | `ServiceAccount` | `lab28-api`, `automountServiceAccountToken: false` — pod không tự động mount token API server nếu không cần |
| 3 | `configmap.yaml` | `ConfigMap` | `lab28-api-config` — endpoint Kafka, Qdrant, Feast, MLflow, vLLM, OTEL collector (toàn URL/host, không có secret) |
| 4 | `api.yaml` | `Deployment` + `Service` | workload chính `lab28-api`, 2 replicas, image `ghcr.io/vinuni-ai20k/day28-platform-api:3.0.0` |
| 5 | `resilience.yaml` | `HorizontalPodAutoscaler` + `PodDisruptionBudget` | HPA scale 2→8 theo CPU 70%, scale-down window 300s; PDB `minAvailable: 1` |
| 6 | `network-policy.yaml` | `NetworkPolicy` | chỉ cho ingress từ namespace của Envoy Gateway vào cổng 8000; egress giới hạn trong namespace `lab28` + DNS (UDP/TCP 53) ra ngoài |
| 7 | `gateway.yaml` | `Gateway` + `HTTPRoute` | Gateway API (`gateway.networking.k8s.io/v1`), `gatewayClassName: envoy-gateway`, route `/` → Service `lab28-api:8000` |

Tổng cộng 9 Kubernetes object trong 7 file, đúng với tập `required` kind mà
`scripts/validate_manifests.py` kiểm tra (xem mục 3).

## 2. Probe & security context được khai báo ra sao

Tất cả nằm trong container `api` của Deployment `lab28-api` (`deploy/kubernetes/base/api.yaml`).

### Probe (3 loại, đều `httpGet` trên cổng `http` = 8000)

| Probe | Path | periodSeconds | failureThreshold | Ý nghĩa |
|-------|------|---------------|-------------------|---------|
| `startupProbe` | `/health` | 5 | 24 | cho container tối đa 120s để khởi động trước khi liveness/readiness bắt đầu tính |
| `livenessProbe` | `/health` | 20 | 3 | container "sống" hay cần restart |
| `readinessProbe` | `/ready` | 10 | 3 | container có nhận traffic hay không — dùng path riêng `/ready` khác `/health`, khớp với `readiness_status()` (ready/degraded/not_ready) trong `src/lab28_platform/integration_tasks.py` |

### Security context — 2 tầng

- **Pod-level** (`spec.template.spec.securityContext`):
  - `runAsNonRoot: true`
  - `seccompProfile.type: RuntimeDefault`
- **Container-level** (`containers[0].securityContext`):
  - `allowPrivilegeEscalation: false`
  - `readOnlyRootFilesystem: true`
  - `capabilities.drop: ["ALL"]`

Cộng thêm ở cấp namespace: `pod-security.kubernetes.io/enforce: restricted`
(`namespace.yaml`) — đây là mức PSA nghiêm nhất, đòi hỏi đúng các trường trên;
`ServiceAccount` không automount token; `NetworkPolicy` giới hạn cả
ingress lẫn egress. Bốn lớp này (PSA namespace, pod securityContext,
container securityContext, NetworkPolicy) là toàn bộ hàng rào bảo mật khai
báo trong manifest — không có secret nào trong `ConfigMap` (secret phải qua
env/secret manager theo `docs/hints/operator.md`).

### `resources`

`requests` 200m CPU / 256Mi, `limits` 1 CPU / 1Gi — bắt buộc phải có vì
`validate_manifests.py` kiểm tra field `resources` tồn tại trên mọi container.

## 3. `scripts/validate_manifests.py` áp đặt hợp đồng gì

Script không phải công cụ apply/drift — nó là **static contract check**, đọc
mọi file `*.yaml` trong `deploy/kubernetes/base/` cộng `gitops/application.yaml`
và assert:

1. Đủ 9 kind bắt buộc: `Deployment, Service, ServiceAccount, ConfigMap,
   HorizontalPodAutoscaler, PodDisruptionBudget, NetworkPolicy, Gateway, HTTPRoute`.
2. Mọi resource có `apiVersion` và `metadata.name` (trừ `Kustomization`).
3. Với `Deployment`: pod phải `runAsNonRoot`; mỗi container không được dùng
   tag `:latest`; mỗi container bắt buộc có đủ 4 field
   `readinessProbe, livenessProbe, resources, securityContext`.
4. `Gateway`/`HTTPRoute` phải dùng đúng `apiVersion: gateway.networking.k8s.io/v1`
   (bản stable, không dùng `v1beta1`/`v1alpha2`).
5. `gitops/application.yaml`: `spec.source.targetRevision` **không được** là
   `HEAD`, `main`, hoặc `master` — bắt buộc phải là revision đã pin (ở đây là
   `refs/tags/v3.0.0`).

Script này chạy trong CI (`.github/workflows/ci.yml`, bước cuối:
`uv run python scripts/validate_manifests.py`, không điều kiện theo
`github.event_name` → chạy trên cả push và pull_request) — nên bất kỳ manifest
nào vi phạm 5 quy tắc trên sẽ fail CI trước khi merge.

## 4. `gitops/application.yaml` — cấu hình Argo CD

```yaml
kind: Application
metadata: {name: lab28-platform, namespace: argocd}
spec:
  source:
    repoURL: https://github.com/VinUni-AI20k/Day28-Modern-Platform-Lab-Student.git
    targetRevision: refs/tags/v3.0.0
    path: deploy/kubernetes/base
  destination: {server: https://kubernetes.default.svc, namespace: lab28}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true]
  revisionHistoryLimit: 5
```

Điểm mấu chốt cho drift/rollback:

- `selfHeal: true` → nếu live state lệch khỏi Git (drift), Argo CD tự động
  sync lại về đúng manifest trong Git, **không cần** thao tác thủ công.
- `prune: true` → resource nào bị xoá khỏi Git cũng sẽ bị xoá khỏi cluster.
- `targetRevision` là tag Git cố định (`refs/tags/v3.0.0`), không phải branch
  di động — đây chính là cơ chế "desired-state rollback": muốn rollback thì
  đổi giá trị này (hoặc tag image trong `api.yaml`) sang revision cũ hơn rồi
  commit, Argo CD sẽ sync cluster về đúng trạng thái đó.
- `revisionHistoryLimit: 5` → Argo CD giữ lịch sử 5 lần sync gần nhất, dùng
  được để `argocd app rollback` về một trong các revision đó.

## 5. Quy trình rollback cụ thể trong repo (`runbooks/gitops-rollback.md`)

Runbook có đúng 5 bước; dưới đây là ánh xạ từng bước sang lệnh thật thao tác
trên cấu hình đã đọc ở trên (không lệnh nào đã được chạy trong phiên này):

| Bước runbook | Lệnh / thao tác | Vì sao đúng với manifest hiện có |
|---|---|---|
| 1. Chạy validate | `uv run python scripts/validate_manifests.py` | phải pass trước khi đổi gì, vì đây là gate CI (mục 3) |
| 2. Build immutable image, đổi tag trong Git, review diff | đổi `image:` trong `api.yaml` sang tag mới (không dùng `:latest` — script sẽ chặn), `git diff deploy/kubernetes/base/api.yaml` | image hiện tại đã pin `ghcr.io/.../day28-platform-api:3.0.0` — đổi tag = thay đúng dòng 24 của `api.yaml` |
| 3. Argo CD sync; kiểm tra health/smoke | `argocd app sync lab28-platform`, `argocd app get lab28-platform`, `kubectl get pods,deploy -n lab28`, smoke test qua Gateway (`curl http://<gateway-addr>/ready` và `/health` — đúng 2 path khai báo ở mục 2) | `selfHeal`/`prune` đã bật nên sync tự động, nhưng runbook vẫn kiểm chứng thủ công bằng health/smoke |
| 4. Tạo drift ở field dùng cho demo, quan sát self-heal | ví dụ: `kubectl -n lab28 scale deployment/lab28-api --replicas=5` (lệch khỏi `replicas: 2` khai báo trong `api.yaml`), hoặc `kubectl -n lab28 set image deployment/lab28-api api=ghcr.io/.../day28-platform-api:2.9.0`; sau đó `argocd app diff lab28-platform` để thấy diff, rồi quan sát Argo CD tự sync lại (`selfHeal: true`) — `kubectl get deploy lab28-api -n lab28 -w` để thấy replicas quay lại 2 | drift phải đánh vào field mà Git đang khai báo tường minh (replicas, image) để self-heal có gì để so sánh và phục hồi |
| 5. Revert desired Git revision/image; kiểm tra replicas, gateway, trace | sửa `targetRevision` trong `gitops/application.yaml` (hoặc tag image trong `api.yaml`) về giá trị trước đó, `git commit` + push (hoặc `argocd app rollback lab28-platform <history-id>` dùng `revisionHistoryLimit: 5`), rồi `kubectl get deploy lab28-api -n lab28`, `kubectl get httproute,gateway -n lab28`, và tra trace ID qua OTEL collector/Jaeger (endpoint `otel-collector.lab28.svc:4317` khai báo trong `configmap.yaml`) | đây là "desired-state rollback" thật sự: sửa Git, không live-edit — đúng nguyên tắc ở `docs/hints/operator.md` dòng 4 ("GitOps rollback đổi desired revision/image, không để live edit undocumented") |

## 6. Lệnh nào tạo ra **evidence** cho drift/rollback

Phân biệt rõ 3 loại evidence cần cho demo (theo `docs/demo-runbook.md` mục 7
"GitOps: show diff, drift/self-heal và desired-state rollback"):

1. **Evidence "diff" (trước khi đổi gì)**
   - `uv run python scripts/validate_manifests.py` → output pass/fail cho contract.
   - `git diff` (sau khi sửa tag ảnh hoặc revision) → bằng chứng thay đổi desired state nằm trong Git, review được trước khi merge.

2. **Evidence "drift/self-heal"**
   - Lệnh **tạo** drift: `kubectl -n lab28 scale deployment/lab28-api --replicas=<khác 2>`
     hoặc `kubectl -n lab28 set image deployment/lab28-api api=<tag khác>`.
   - Lệnh **quan sát** drift: `argocd app diff lab28-platform` (in ra phần lệch
     giữa live state và Git) — chính output này là evidence.
   - Lệnh **quan sát self-heal**: `kubectl get deploy lab28-api -n lab28 -w`
     hoặc `argocd app get lab28-platform` lặp lại — evidence là timestamp/log
     cho thấy Argo CD tự sync do `syncPolicy.automated.selfHeal: true`, đưa
     `replicas`/`image` về đúng giá trị khai báo trong `api.yaml`.

3. **Evidence "desired-state rollback"**
   - Lệnh **thực hiện** rollback: sửa `gitops/application.yaml` (`targetRevision`)
     hoặc `api.yaml` (`image`) trong Git rồi commit/push, **hoặc**
     `argocd app rollback lab28-platform <ID>` (chọn ID trong 5 lịch sử được
     giữ theo `revisionHistoryLimit: 5`).
   - Lệnh **kiểm chứng** sau rollback: `kubectl get deploy lab28-api -n lab28
     -o jsonpath='{.spec.replicas}{"\n"}{.spec.template.spec.containers[0].image}'`
     (khớp lại với giá trị đã pin trong Git), `kubectl get httproute,gateway -n
     lab28` (Gateway/HTTPRoute vẫn đúng cấu hình), và tra `trace_id` tương ứng
     trên OTEL/Jaeger để chứng minh request vẫn xuyên hết span sau khi rollback
     (nối với yêu cầu ở `docs/demo-runbook.md` mục 3 và mục 7).

> Lưu ý: repo hiện **không có** script tự động hoá 3 nhóm lệnh trên (không có
> Makefile/CI job nào chạy `kubectl`/`argocd`) — `runbooks/gitops-rollback.md`
> chỉ mô tả quy trình narrative, các lệnh `kubectl`/`argocd` cụ thể ở mục 5–6
> là suy ra trực tiếp từ field trong manifest (`replicas`, `image`,
> `targetRevision`, probe path) để khớp đúng những gì đã khai báo, không phải
> lệnh có sẵn trong repo. Khi thực thi thật, log/output của các lệnh này chính
> là file evidence cần đính kèm (ví dụ `evidence/gitops-drift.json`,
> `evidence/gitops-rollback.json`, theo đúng quy ước `evidence/ipXX-*.json`
> đang dùng cho các integration point khác — xem `ANSWERS.md`).
