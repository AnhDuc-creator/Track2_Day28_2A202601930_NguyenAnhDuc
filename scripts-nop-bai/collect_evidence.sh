#!/usr/bin/env bash
# collect_evidence.sh — sinh đủ 10 evidence file + integration-report.json +
# load profile cho Day 28 Track 2.
#
# Giả định: stack đã chạy ở profile "full" (docker compose ... --profile full
# up -d --build --wait) VÀ đã seed (lab28 topics / index / release / seed).
# Script này KHÔNG tự khởi động Docker, KHÔNG tự seed, KHÔNG chạy
# test_j4_degraded_recovery.py (test đó dừng/khởi động container — nằm ngoài
# phạm vi "thu evidence", xem runbooks/failure-injection.md + INCIDENT.md).
#
# Nguồn thứ tự lệnh: src/lab28_platform/cli.py (hàm evidence),
# integration-tests/stack.py (write_evidence), contracts/integration-matrix.yaml.
#
# Cố ý KHÔNG dùng `set -e`: một bước (test) fail không được làm dừng cả script
# — mọi bước phải chạy hết để thu được tối đa evidence có thể trong một lần.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

EVIDENCE_DIR="${REPO_ROOT}/evidence"
LOG_DIR="${EVIDENCE_DIR}/logs"
mkdir -p "${LOG_DIR}"

STEP_FAILURES=0

# Chạy 1 lệnh, echo tiêu đề trước, ghi toàn bộ stdout+stderr vào
# evidence/logs/<ten-buoc>.log. Không dừng script nếu lệnh fail.
run_step() {
  local title="$1" logname="$2"
  shift 2
  local logfile="${LOG_DIR}/${logname}.log"

  echo ""
  echo "=============================================================="
  echo ">>> ${title}"
  echo ">>> lệnh: $*"
  echo ">>> log:  evidence/logs/${logname}.log"
  echo "=============================================================="

  "$@" >"${logfile}" 2>&1
  local status=$?

  if [ "${status}" -eq 0 ]; then
    echo "    KET QUA: OK (exit 0)"
  else
    echo "    KET QUA: THAT BAI (exit ${status}) — xem ${logfile}"
    STEP_FAILURES=$((STEP_FAILURES + 1))
  fi
  return 0
}

# Giống run_step, nhưng stdout của lệnh được ghi ra một file JSON riêng
# (outfile) thay vì log — dùng cho load-tests/run_profile.py, script này chỉ
# in JSON ra stdout chứ không tự ghi file.
run_step_capture() {
  local title="$1" logname="$2" outfile="$3"
  shift 3
  local logfile="${LOG_DIR}/${logname}.log"

  echo ""
  echo "=============================================================="
  echo ">>> ${title}"
  echo ">>> lệnh: $* > ${outfile#${REPO_ROOT}/}"
  echo ">>> log:  evidence/logs/${logname}.log"
  echo "=============================================================="

  "$@" >"${outfile}" 2>"${logfile}"
  local status=$?

  if [ "${status}" -eq 0 ]; then
    echo "    KET QUA: OK (exit 0) -> ${outfile#${REPO_ROOT}/}"
  else
    echo "    KET QUA: THAT BAI (exit ${status}) — xem ${logfile}"
    STEP_FAILURES=$((STEP_FAILURES + 1))
  fi
  return 0
}

echo "Bat dau thu evidence — repo root: ${REPO_ROOT}"
echo "Gia dinh: stack profile 'full' da 'up -d --build --wait' va da seed"
echo "(lab28 topics / index --source file / release / seed --via-gateway)."

# --------------------------------------------------------------------------
# 1) lab28 evidence — nguồn DUY NHẤT cho ip03/ip05/ip07 (không pytest nào ghi
#    3 file này). Chạy TRƯỚC các pytest bên dưới vì nó ghi một bản ip06 chỉ
#    có health() (reachable/has_champion/version) — bản đầy đủ provenance
#    (tag, git sha, delta version) sẽ đến từ test_j3 ở bước sau và ghi đè lên
#    đúng tên file này. Nếu đảo thứ tự, ip06 sẽ bị hạ cấp về bản rút gọn.
# --------------------------------------------------------------------------
run_step "1/9 lab28 evidence (ip03, ip05, ip06-tam, ip07, integration-report.json)" \
  "00-lab28-evidence" \
  uv run lab28 evidence

# --------------------------------------------------------------------------
# 2) IT-J1 golden path — nguồn DUY NHẤT cho ip01, ip02, ip04.
# --------------------------------------------------------------------------
run_step "2/9 IT-J1 golden path (ip01-kafka-consume, ip02-airflow-run, ip04-feast-online)" \
  "01-test-j1-golden-path" \
  uv run pytest integration-tests/test_j1_golden_path.py -q

# --------------------------------------------------------------------------
# 3) IT-J3 promotion/rollback — ghi đè ip06-mlflow-release.json bằng bản đầy
#    đủ provenance (prompt/model/collection/feature-service/delta-version).
# --------------------------------------------------------------------------
run_step "3/9 IT-J3 promotion/rollback (ip06-mlflow-release.json bản đầy đủ)" \
  "02-test-j3-promotion-rollback" \
  uv run pytest integration-tests/test_j3_promotion_rollback.py -q

# --------------------------------------------------------------------------
# 4) IT-gateway-rate-limit — nguồn DUY NHẤT cho ip08-gateway.json.
# --------------------------------------------------------------------------
run_step "4/9 gateway rate limit (ip08-gateway.json)" \
  "03-test-gateway-rate-limit" \
  uv run pytest integration-tests/test_gateway_rate_limit.py -q

# --------------------------------------------------------------------------
# 5) IT-prometheus-targets — nguồn DUY NHẤT cho ip09-prometheus-targets.json
#    và ip09-grafana-dashboards.json.
# --------------------------------------------------------------------------
run_step "5/9 Prometheus/Grafana (ip09-prometheus-targets.json, ip09-grafana-dashboards.json)" \
  "04-test-prometheus-targets" \
  uv run pytest integration-tests/test_prometheus_targets.py -q

# --------------------------------------------------------------------------
# 6) IT-J5 trace/metrics continuity — ghi ip10-trace.json (nhánh sync+async;
#    không cần GPU vì test ghi evidence này không mang marker gpu).
# --------------------------------------------------------------------------
run_step "6/9 IT-J5 trace/metrics continuity (ip10-trace.json — nhánh sync+async)" \
  "05-test-j5-trace-metrics-continuity" \
  uv run pytest integration-tests/test_j5_trace_metrics_continuity.py -q

# --------------------------------------------------------------------------
# 7) IT-trace-span-coverage — nếu có vLLM GPU thật, ghi đè ip10-trace.json
#    bằng bản đủ cả 11 span (kể cả nhánh serving). Không có GPU thì test
#    gpu-marked tự skip, file ip10 giữ nguyên bản từ bước 6.
# --------------------------------------------------------------------------
run_step "7/9 IT-trace-span-coverage (ip10-trace.json bản đầy đủ nếu có GPU)" \
  "06-test-trace-span-coverage" \
  uv run pytest integration-tests/test_trace_span_coverage.py -q

# --------------------------------------------------------------------------
# 8) Load profile — 8 workers. run_profile.py chỉ in JSON ra stdout, không tự
#    ghi file, nên script redirect trực tiếp vào evidence/perf-8w.json.
# --------------------------------------------------------------------------
run_step_capture "8/9 Load profile 200 requests / 8 workers" \
  "07-load-profile-8w" \
  "${EVIDENCE_DIR}/perf-8w.json" \
  uv run python load-tests/run_profile.py --requests 200 --workers 8

# --------------------------------------------------------------------------
# 9) Load profile — 16 workers (theo runbooks/performance.md: lặp lại với
#    16 workers để so sánh).
# --------------------------------------------------------------------------
run_step_capture "9/9 Load profile 200 requests / 16 workers" \
  "08-load-profile-16w" \
  "${EVIDENCE_DIR}/perf-16w.json" \
  uv run python load-tests/run_profile.py --requests 200 --workers 16

# --------------------------------------------------------------------------
# Tổng kết: liệt kê mọi file trong evidence/ kèm kích thước.
# --------------------------------------------------------------------------
echo ""
echo "=============================================================="
echo ">>> Danh sách file trong evidence/ (kèm kích thước)"
echo "=============================================================="
if [ -d "${EVIDENCE_DIR}" ]; then
  for f in "${EVIDENCE_DIR}"/*; do
    [ -f "${f}" ] || continue
    printf '%-40s %s\n' "$(basename "${f}")" "$(du -h "${f}" | cut -f1)"
  done
  echo "--- evidence/logs/ ---"
  for f in "${LOG_DIR}"/*; do
    [ -f "${f}" ] || continue
    printf '%-40s %s\n' "logs/$(basename "${f}")" "$(du -h "${f}" | cut -f1)"
  done
else
  echo "evidence/ chưa tồn tại — không có file nào được tạo."
fi

echo ""
if [ "${STEP_FAILURES}" -eq 0 ]; then
  echo "Xong. Toàn bộ ${0##*/} chạy không có bước nào exit khác 0."
else
  echo "Xong, nhưng có ${STEP_FAILURES} bước exit khác 0 — xem log tương ứng trong evidence/logs/."
fi
