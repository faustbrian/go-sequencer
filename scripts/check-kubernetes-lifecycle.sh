#!/usr/bin/env bash
set -euo pipefail

readonly KIND_VERSION="v0.31.0"
readonly KIND_NODE_IMAGE="kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f"
readonly POSTGRES_IMAGE="postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"

module_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repository_root="$(git -C "${module_directory}" rev-parse --show-toplevel)"
source_revision="$(git -C "${module_directory}" rev-parse HEAD)"
gate_input_sha256="$("${repository_root}/scripts/gate-input-digest.sh" interoperability pkg/sequencer)"
started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
temporary="$(mktemp -d "${TMPDIR:-/tmp}/sequencer-kubernetes.XXXXXX")"
cluster_name="sequencer-$$_${RANDOM}"
cluster_name="${cluster_name//_/-}"
kubeconfig="${temporary}/kubeconfig"
kind_binary="${temporary}/kind"
gocache="${temporary}/gocache"
image="sequencer-kubernetes:${cluster_name}"
artifact_directory="${repository_root}/.artifacts/pkg/sequencer/kubernetes"
cluster_created=0
image_created=0
port_forward_pid=""
report_temporary=""

cleanup() {
    local status=$?
    local cleanup_failed=0
    if [[ -n "${port_forward_pid}" ]] && kill -0 "${port_forward_pid}" 2>/dev/null; then
        kill "${port_forward_pid}" 2>/dev/null || true
        wait "${port_forward_pid}" 2>/dev/null || true
    fi
    if [[ "${cluster_created}" -eq 1 ]]; then
        "${kind_binary}" delete cluster --name "${cluster_name}" --kubeconfig "${kubeconfig}" >/dev/null 2>&1 || cleanup_failed=1
    fi
    if [[ "${image_created}" -eq 1 ]]; then
        docker image rm "${image}" >/dev/null 2>&1 || cleanup_failed=1
    fi
    chmod -R u+w "${temporary}" 2>/dev/null || true
    rm -rf "${temporary}"
    trap - EXIT HUP INT TERM
    if [[ "${status}" -eq 0 && "${cleanup_failed}" -eq 0 && -n "${report_temporary}" ]]; then
        mv "${report_temporary}" "${artifact_directory}/report.json"
        printf 'sequencer Kubernetes lifecycle evidence: %s\n' "${artifact_directory}/report.json"
    elif [[ -n "${report_temporary}" ]]; then
        rm -f "${report_temporary}"
    fi
    if [[ "${status}" -eq 0 && "${cleanup_failed}" -ne 0 ]]; then
        printf 'Kubernetes resources were not fully cleaned\n' >&2
        exit 1
    fi
    exit "${status}"
}
trap cleanup EXIT HUP INT TERM

for command in curl docker go jq kubectl shasum; do
    command -v "${command}" >/dev/null || { printf 'missing command: %s\n' "${command}" >&2; exit 1; }
done
docker info >/dev/null

case "$(uname -s)-$(uname -m)" in
    Darwin-x86_64) kind_target=darwin-amd64; kind_sha256=a8b3cf77b2ad77aec5bf710d1a2589d9117576132af812885cad41e9dede4d4e ;;
    Darwin-arm64) kind_target=darwin-arm64; kind_sha256=88bf554fe9da6311c9f8c2d082613c002911a476f6b5090e9420b35d84e70c5c ;;
    Linux-x86_64) kind_target=linux-amd64; kind_sha256=eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa ;;
    Linux-aarch64|Linux-arm64) kind_target=linux-arm64; kind_sha256=8e1014e87c34901cc422a1445866835d1e666f2a61301c27e722bdeab5a1f7e4 ;;
    *) printf 'unsupported host architecture\n' >&2; exit 1 ;;
esac
curl -fsSL "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${kind_target}" -o "${kind_binary}"
[[ "$(shasum -a 256 "${kind_binary}" | awk '{print $1}')" == "${kind_sha256}" ]]
chmod 0700 "${kind_binary}"

cluster_created=1
"${kind_binary}" create cluster --name "${cluster_name}" --image "${KIND_NODE_IMAGE}" --kubeconfig "${kubeconfig}" --wait 120s
kube() { kubectl --kubeconfig "${kubeconfig}" "$@"; }

case "$(docker version --format '{{.Server.Arch}}')" in
    amd64|x86_64) go_arch=amd64 ;;
    arm64|aarch64) go_arch=arm64 ;;
    *) printf 'unsupported Docker architecture\n' >&2; exit 1 ;;
esac
mkdir -p "${gocache}"
(
    cd "${module_directory}"
    CGO_ENABLED=0 GOOS=linux GOARCH="${go_arch}" GOCACHE="${gocache}" GOWORK=off \
        go test -c -tags=kubernetes -o "${temporary}/sequencer.test" .
)
cat >"${temporary}/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY sequencer.test /sequencer.test
USER 65532:65532
ENTRYPOINT ["/sequencer.test", "-test.run=^TestKubernetesLifecycleHelper$", "-test.v"]
DOCKERFILE
docker build --quiet -t "${image}" "${temporary}" >/dev/null
image_created=1
"${kind_binary}" load docker-image --name "${cluster_name}" "${image}"

kube apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  replicas: 1
  selector: {matchLabels: {app: postgres}}
  template:
    metadata: {labels: {app: postgres}}
    spec:
      containers:
        - name: postgres
          image: ${POSTGRES_IMAGE}
          env:
            - {name: POSTGRES_DB, value: sequencer}
            - {name: POSTGRES_USER, value: sequencer}
            - {name: POSTGRES_PASSWORD, value: sequencer}
          readinessProbe:
            exec: {command: ["pg_isready", "-U", "sequencer"]}
            periodSeconds: 1
---
apiVersion: v1
kind: Service
metadata: {name: postgres}
spec:
  selector: {app: postgres}
  ports: [{port: 5432, targetPort: 5432}]
YAML
kube rollout status deployment/postgres --timeout=120s >/dev/null

database_url='postgres://sequencer:sequencer@postgres:5432/sequencer?sslmode=disable'
kube run migrate --restart=Never --image="${image}" --image-pull-policy=Never \
    --env="DATABASE_URL=${database_url}" --env=SEQUENCER_HELPER_MODE=migrate >/dev/null
for _ in {1..120}; do
    migration_phase="$(kube get pod migrate -o jsonpath='{.status.phase}')"
    [[ "${migration_phase}" == "Succeeded" ]] && break
    if [[ "${migration_phase}" == "Failed" ]]; then
        kube logs migrate >&2 || true
        exit 1
    fi
    sleep 0.5
done
if [[ "${migration_phase}" != "Succeeded" ]]; then
    kube describe pod migrate >&2 || true
    kube logs migrate >&2 || true
    exit 1
fi

reset_ledger() {
    kube exec deployment/postgres -- psql -U sequencer -d sequencer -v ON_ERROR_STOP=1 \
        -c 'TRUNCATE sequencer_audit_events, sequencer_attempts, sequencer_operations RESTART IDENTITY CASCADE' >/dev/null
}

apply_runner() {
    local name="$1" replicas="$2" version="$3" behavior="$4" operations="$5" grace="$6"
    kube apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: ${name}}
spec:
  replicas: ${replicas}
  selector: {matchLabels: {app: ${name}}}
  template:
    metadata: {labels: {app: ${name}}}
    spec:
      terminationGracePeriodSeconds: ${grace}
      containers:
        - name: sequencer
          image: ${image}
          imagePullPolicy: Never
          env:
            - {name: DATABASE_URL, value: "${database_url}"}
            - {name: SEQUENCER_VERSION, value: "${version}"}
            - {name: SEQUENCER_BEHAVIOR, value: "${behavior}"}
            - {name: SEQUENCER_OPERATION_COUNT, value: "${operations}"}
            - name: POD_UID
              valueFrom: {fieldRef: {fieldPath: metadata.uid}}
          ports: [{name: probes, containerPort: 8080}]
          readinessProbe:
            httpGet: {path: /readyz, port: probes}
            periodSeconds: 1
            failureThreshold: 1
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: {drop: ["ALL"]}
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
YAML
    kube rollout status "deployment/${name}" --timeout=120s >/dev/null
}

wait_sql() {
    local predicate="$1" attempts="${2:-120}" result
    for ((attempt=0; attempt<attempts; attempt++)); do
        result="$(kube exec deployment/postgres -- psql -U sequencer -d sequencer -Atc "${predicate}")"
        [[ "${result}" == "t" ]] && return 0
        sleep 0.25
    done
    printf 'SQL predicate did not become true: %s (last=%s)\n' "${predicate}" "${result}" >&2
    return 1
}

# SIGTERM closes readiness before a drain-only accepted attempt completes.
reset_ledger
apply_runner graceful 1 1 drain 1 8
graceful_pod="$(kube get pods -l app=graceful -o jsonpath='{.items[0].metadata.name}')"
graceful_ip="$(kube get pod "${graceful_pod}" -o jsonpath='{.status.podIP}')"
wait_sql "SELECT state = 'running' FROM sequencer_operations WHERE operation_id = 'kubernetes.lifecycle' AND version = 1"
transition="${temporary}/transition"
docker exec "${cluster_name}-control-plane" bash -c '
    for _ in {1..200}; do
        status="$(curl -sS -o /dev/null -w "%{http_code}" --max-time 0.5 "http://$1:8080/readyz" 2>/dev/null || true)"
        [[ -n "${status}" ]] || status=000
        printf "%s\n" "${status}"
        sleep 0.02
    done
' -- "${graceful_ip}" >"${transition}" &
probe_pid=$!
for _ in {1..100}; do grep -q '^200$' "${transition}" 2>/dev/null && break; sleep 0.05; done
grep -q '^200$' "${transition}"
kube delete pod "${graceful_pod}" --wait=true >/dev/null
wait "${probe_pid}" || true
awk '$0 == "200" {ready=1} $0 == "503" && ready {draining=1} END {exit !(ready && draining)}' "${transition}"
wait_sql "SELECT state = 'succeeded' AND attempt_number = 1 FROM sequencer_operations WHERE operation_id = 'kubernetes.lifecycle' AND version = 1"
kube delete deployment graceful --wait=true >/dev/null

# An abrupt pod kill loses renewal; replacement recovers expiry and uses a higher fence.
reset_ledger
apply_runner abrupt 1 1 crash-recover 1 1
abrupt_pod="$(kube get pods -l app=abrupt -o jsonpath='{.items[0].metadata.name}')"
wait_sql "SELECT state = 'running' FROM sequencer_operations WHERE operation_id = 'kubernetes.lifecycle' AND version = 1"
kube delete pod "${abrupt_pod}" --grace-period=0 --force --wait=false >/dev/null
kube rollout status deployment/abrupt --timeout=120s >/dev/null
wait_sql "SELECT state = 'succeeded' AND attempt_number = 2 AND fencing_token = 2 FROM sequencer_operations WHERE operation_id = 'kubernetes.lifecycle' AND version = 1" 160
wait_sql "SELECT count(*) = 1 FROM sequencer_attempts WHERE operation_id = 'kubernetes.lifecycle' AND version = 1 AND state = 'indeterminate'"
kube delete deployment abrupt --wait=true >/dev/null

# Multiple replicas claim without a leader and complete each operation once.
reset_ledger
apply_runner leaderless 3 1 leaderless 12 5
wait_sql "SELECT count(*) = 12 AND bool_and(state = 'succeeded') FROM sequencer_operations WHERE operation_id LIKE 'kubernetes.leaderless-%'" 160
wait_sql "SELECT count(DISTINCT owner) >= 2 FROM sequencer_attempts WHERE operation_id LIKE 'kubernetes.leaderless-%'"
wait_sql "SELECT count(*) = 12 FROM sequencer_attempts WHERE operation_id LIKE 'kubernetes.leaderless-%'"
kube delete deployment leaderless --wait=true >/dev/null

# Old and new registries coexist; removing the new deployment leaves the old exact definition healthy.
reset_ledger
apply_runner registry-old 1 1 normal 1 5
apply_runner registry-new 1 2 normal 1 5
wait_sql "SELECT count(*) = 2 AND bool_and(state = 'succeeded') FROM sequencer_operations WHERE operation_id = 'kubernetes.lifecycle'"
kube delete deployment registry-new --wait=true >/dev/null
kube rollout status deployment/registry-old --timeout=60s >/dev/null
old_pod="$(kube get pods -l app=registry-old -o jsonpath='{.items[0].metadata.name}')"
kube get pod "${old_pod}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' | grep -q '^True$'
wait_sql "SELECT checksum = 'sha256:kubernetes-v1' FROM sequencer_operations WHERE operation_id = 'kubernetes.lifecycle' AND version = 1"
wait_sql "SELECT checksum = 'sha256:kubernetes-v2' FROM sequencer_operations WHERE operation_id = 'kubernetes.lifecycle' AND version = 2"
kube delete deployment registry-old --wait=true >/dev/null

completed_gate_input_sha256="$("${repository_root}/scripts/gate-input-digest.sh" interoperability pkg/sequencer)"
if [[ "${completed_gate_input_sha256}" != "${gate_input_sha256}" ]]; then
    printf 'Kubernetes lifecycle gate inputs changed during execution\n' >&2
    exit 1
fi

mkdir -p "${artifact_directory}"
report_temporary="$(mktemp "${artifact_directory}/.report.XXXXXX")"
helper_sha256="$(shasum -a 256 "${module_directory}/kubernetes_lifecycle_helper_test.go" | awk '{print $1}')"
script_sha256="$(shasum -a 256 "${module_directory}/scripts/check-kubernetes-lifecycle.sh" | awk '{print $1}')"
image_id="$(docker image inspect "${image}" --format '{{.Id}}')"
kubernetes_version="$(kube version -o json | jq -r '.serverVersion.gitVersion')"
jq -n \
    --arg schema 'sequencer-kubernetes-lifecycle/v2' \
    --arg result passed \
    --arg source_revision "${source_revision}" \
    --arg gate_input_sha256 "${gate_input_sha256}" \
    --arg helper_sha256 "${helper_sha256}" \
    --arg script_sha256 "${script_sha256}" \
    --arg kind_version "${KIND_VERSION}" \
    --arg node_image "${KIND_NODE_IMAGE}" \
    --arg postgres_image "${POSTGRES_IMAGE}" \
    --arg kubernetes_version "${kubernetes_version}" \
    --arg helper_image_id "${image_id}" \
    --arg started_at "${started_at}" \
    --arg completed_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    '{
      schema: $schema,
      result: $result,
      source_revision: $source_revision,
      input_sha256: {
        gate: $gate_input_sha256,
        helper: $helper_sha256,
        script: $script_sha256
      },
      environment: {
        kind: $kind_version,
        node_image: $node_image,
        postgres_image: $postgres_image,
        kubernetes: $kubernetes_version,
        helper_image_id: $helper_image_id
      },
      scenarios: [
        "sigterm-readiness-drain",
        "abrupt-kill-lease-expiry-recovery",
        "leaderless-replicas",
        "mixed-registry-rollout-rollback"
      ],
      started_at: $started_at,
      completed_at: $completed_at
    }' >"${report_temporary}"

printf 'sequencer Kubernetes lifecycle proof passed (kind %s, node %s)\n' "${KIND_VERSION}" "${KIND_NODE_IMAGE}"
