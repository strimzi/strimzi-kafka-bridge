#!/usr/bin/env bash
#
# Long-running load test: drives the mixed scenario (produce + consume + commit +
# metadata + optionally authenticated produce) for a long duration while sampling
# the bridge metrics and heap, so that the time-dependent behaviors are exercised
# and observable:
#
#   - the bridge-side token lifecycle in client-credentials mode (the bridge
#     fetches and refreshes its own tokens, so its broker connections survive
#     the whole run - use the bridge-oauth-cc.properties profile)
#   - consumer group stability (rebalance-free long sessions, committed offsets)
#   - the memory trend: the sampled heap and class histogram must PLATEAU, a
#     monotonic growth indicates a leak (see LOAD-TESTING.md, "Memory under
#     rotating tokens")
#
# NOTE on the OAuth passthrough profiles: consumers keep the token of their
# creation request until deletion, so with short-lived tokens their broker
# connections fail re-authentication mid-run (polls then return errors) - see
# LOAD-TESTING.md, "Long-running runs". For a long authenticated-passthrough
# run, raise the realm's accessTokenLifespan above the run duration first.
#
# The bridge must already be running (see the README quick start). Artifacts land
# in OUT_DIR (default ./longrun-results/<timestamp>): the k6 summary, a timestamped
# metrics sample file and (with BRIDGE_PID) heap snapshots.
#
# Environment variables:
#   DURATION            run duration of the k6 scenario (default 10m)
#   SAMPLE_INTERVAL     seconds between two bridge samples (default 15)
#   MANAGEMENT_URL      bridge management base URL (default http://localhost:8081)
#   BRIDGE_PID          bridge process id: enables jcmd heap snapshots per sample
#                       (optional; autodetected from the java command line if unset)
#   OUT_DIR             output directory (default ./longrun-results/<timestamp>)
#   ...                 all the k6 mixed.js variables pass through (PRODUCE_RATE,
#                       CONSUME_RATE, AUTH_RATE, METADATA_RATE, USERS, AUTH_SCHEME,
#                       TOKEN_ROTATION_MARGIN, BASE_URL, ...)
#
# Examples:
#   # sustainable 10-minute run (plain bridge, default rates)
#   scripts/load-test/longrun.sh
#
#   # 10-minute run over OAuth client-credentials mode: the bridge refreshes its
#   # own tokens on schedule, the HTTP clients unauthenticated (AUTH_RATE=0)
#   BRIDGE_PID=$(pgrep -f io.strimzi.kafka.bridge.Application) \
#   AUTH_RATE=0 scripts/load-test/longrun.sh   # bridge started with bridge-oauth-cc.properties
#
set -euo pipefail

DURATION="${DURATION:-10m}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-15}"
MANAGEMENT_URL="${MANAGEMENT_URL:-http://localhost:8081}"
K6_SCRIPT="$(cd "$(dirname "$0")" && pwd)/k6/mixed.js"
OUT_DIR="${OUT_DIR:-longrun-results/$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$OUT_DIR"
METRICS_FILE="$OUT_DIR/metrics-samples.txt"
HEAP_FILE="$OUT_DIR/heap-samples.txt"

# the bridge must be healthy before starting (a degraded run measures nothing)
if ! curl -sf "$MANAGEMENT_URL/healthy" > /dev/null; then
    echo "ERROR: the bridge is not healthy on $MANAGEMENT_URL - start it first (see the README quick start)" >&2
    exit 1
fi

# heap sampling target: the bridge JVM (autodetected when unset)
if [[ -z "${BRIDGE_PID:-}" ]]; then
    BRIDGE_PID="$(pgrep -f io.strimzi.kafka.bridge.Application | head -1 || true)"
fi
if [[ -n "${BRIDGE_PID:-}" ]] && ! kill -0 "$BRIDGE_PID" 2>/dev/null; then
    echo "WARNING: BRIDGE_PID=$BRIDGE_PID is not running, heap sampling disabled" >&2
    BRIDGE_PID=""
fi

# the interesting series: the auth caches and validation counters, the executor
# saturation signals and the JVM heap/GC (the full /metrics is large; this keeps
# the sample file diffable over time)
METRICS_FILTER='^(strimzi_bridge_auth_(cache_entries|cache_evictions_total|introspection_total|introspection_cache_total|introspection_duration_seconds_count|dpop_total|jwks_(keys|unknown-kid_total)|last_success)|executor_(active_threads|queue_remaining_tasks|queued_tasks)|jvm_memory_used_bytes|jvm_gc_pause_seconds_count)'

sample() {
    {
        echo "=== $(date -Is)"
        curl -sf "$MANAGEMENT_URL/metrics" | grep -E "$METRICS_FILTER" | grep -v '^#'
    } >> "$METRICS_FILE"
}

sample_heap() {
    {
        echo "=== $(date -Is)"
        jcmd "$BRIDGE_PID" GC.heap_info 2>/dev/null | head -4
        jcmd "$BRIDGE_PID" GC.class_histogram 2>/dev/null | head -12
    } >> "$HEAP_FILE"
}

echo "Long-running load test: duration=$DURATION sample_interval=${SAMPLE_INTERVAL}s" \
    $([[ -n "${BRIDGE_PID:-}" ]] && echo "heap_sampling=on (pid $BRIDGE_PID)") \
    $([[ -z "${BRIDGE_PID:-}" ]] && echo "heap_sampling=off (set BRIDGE_PID to enable)")
echo "Artifacts: $OUT_DIR/"

# the sampler runs while k6 drives the load; a failed sample (transient curl/jcmd
# error) is skipped, it must never kill the loop
(
    while true; do
        sample || true
        if [[ -n "${BRIDGE_PID:-}" ]]; then
            sample_heap || true
        fi
        sleep "$SAMPLE_INTERVAL"
    done
) &
SAMPLER_PID=$!
trap 'kill "$SAMPLER_PID" 2>/dev/null || true' EXIT

sample   # t0 snapshot, so the deltas cover the whole run

k6 run -e DURATION="$DURATION" "$K6_SCRIPT" | tee "$OUT_DIR/k6-summary.txt"

kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true

# quick trend of the sampled series (the k6 summary above carries the latencies)
python3 - "$METRICS_FILE" << 'EOF'
import re, sys

samples = open(sys.argv[1]).read().split('=== ')
first, last, peak = {}, {}, {}

for block in samples:
    for line in block.strip().splitlines():
        m = re.match(r'(\S+)\{([^}]*)\}\s+([\d.eE+-]+)$', line.strip())
        if not m:
            continue
        name, labels, value = m.group(1), m.group(2), float(m.group(3))
        key = (name, tuple(sorted(re.findall(r'(\w+)="([^"]*)"', labels))))
        if key not in first:
            first[key] = value
        last[key] = value
        peak[key] = max(peak.get(key, 0), value)

def fmt(key):
    name, labels = key
    label_s = ','.join(f'{k}="{v}"' for k, v in labels)
    return f'{name}{{{label_s}}}'

gauge_names = ['cache_entries', 'jvm_memory_used_bytes']
counter_names = ['cache_evictions_total', 'introspection_total', 'dpop_total', 'jwks_unknown-kid_total']

print('\n=== Sampled series trend (first -> last, peak) over the run')
for (name, labels) in sorted(first):
    key = (name, labels)
    if any(g in name for g in counter_names):
        delta = last[key] - first[key]
        print(f'{fmt(key):90} +{delta:.0f} over the run')
    elif any(g in name for g in gauge_names):
        print(f'{fmt(key):90} {first[key]:.5g} -> {last[key]:.5g} (peak {peak[key]:.5g})')

entries = {k: v for k, v in peak.items() if 'cache_entries' in k[0]}
if entries:
    print('\nCache bounds: peak entries per cache -', ', '.join(f'{dict(k[1]).get("cache", "?")}={v:.0f}' for k, v in sorted(entries.items())))
heap = [v for k, v in peak.items() if k[0] == 'jvm_memory_used_bytes' and dict(k[1]).get('area') == 'heap']
if heap:
    print(f'Heap used: peak {max(heap)/1e6:.0f} MB across the run (compare with the first sample: a monotonic growth = leak, a plateau = bounded)')
EOF

echo "Done. Artifacts in $OUT_DIR/ (metrics-samples.txt, k6-summary.txt$([[ -n "${BRIDGE_PID:-}" ]] && echo ', heap-samples.txt'))."
