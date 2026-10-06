#!/usr/bin/env bash
#
# Run Prometheus + Grafana (containers, host network) for a vstart crimson
# cluster, with the "Crimson OSD" dashboard provisioned.
#
# Start the cluster with the crimson Prometheus endpoint enabled, for example:
#
#   MON=1 MGR=1 OSD=3 ../src/vstart.sh -n --crimson \
#       -o 'crimson_prometheus_port_base = 9400'
#
# then, from the same build directory:
#
#   ../monitoring/crimson/vstart-monitoring.sh start
#
# Commands:
#   start     write the config, the OSD targets and the dashboard, then start
#             (or restart) the Prometheus and Grafana containers
#   targets   write the OSD target list again (after OSDs are added/removed);
#             Prometheus reads it again without a restart
#   dashboard copy crimson-osd.json again (after gen_dashboard.py);
#             Grafana reads it again without a restart
#   status    show the containers and the health of each scrape target
#   stop      remove the containers (the data volumes stay); src/stop.sh
#             does this when it stops the whole cluster
#   purge     stop, then remove the data volumes
#
# "start" saves the settings below (not the password) in $STATE_DIR/env.
# The other commands use the saved values, so set them only for "start".
#
# Environment (defaults in brackets):
#   PORT_BASE            crimson_prometheus_port_base [running osd.<first id>, else ceph.conf]
#   LISTEN_ADDR          address for Prometheus and Grafana [127.0.0.1]
#   PROM_PORT            Prometheus port [9090, or the next free port]
#   GRAFANA_PORT         Grafana port [3000, or the next free port]
#                        A port that you set must be free; start stops if it is not.
#   SCRAPE_INTERVAL      Prometheus scrape interval [15s]
#   RETENTION            Prometheus data retention [7d]
#   GRAFANA_ADMIN_PASSWORD  Grafana admin password [admin]
#   GRAFANA_ANON_ROLE    role for anonymous users: Viewer, Editor or Admin [Viewer]
#   CONTAINER_ENGINE     podman or docker [podman if installed, else docker]
#   PROM_IMAGE, GRAFANA_IMAGE  container images [same as cephadm defaults]
#   STATE_DIR            generated config [<build dir>/crimson-monitoring]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$PWD"
CEPH="$BUILD_DIR/bin/ceph"
STATE_DIR="${STATE_DIR:-$BUILD_DIR/crimson-monitoring}"

# Settings that "start" saves for the other commands
SAVED_VARS=(LISTEN_ADDR PROM_PORT GRAFANA_PORT SCRAPE_INTERVAL RETENTION
            GRAFANA_ANON_ROLE CONTAINER_ENGINE PROM_IMAGE GRAFANA_IMAGE)
# Ports set by the user must be used as they are; default/saved ports can move
PROM_PORT_SET="${PROM_PORT:+1}"
GRAFANA_PORT_SET="${GRAFANA_PORT:+1}"
if [[ -f "$STATE_DIR/env" ]]; then
    # The environment wins over the saved value
    while IFS='=' read -r k v; do
        [[ " ${SAVED_VARS[*]} " == *" $k "* && -z "${!k:-}" ]] && printf -v "$k" '%s' "$v"
    done < "$STATE_DIR/env"
fi

LISTEN_ADDR="${LISTEN_ADDR:-127.0.0.1}"
PROM_PORT="${PROM_PORT:-9090}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-15s}"
RETENTION="${RETENTION:-7d}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"
GRAFANA_ANON_ROLE="${GRAFANA_ANON_ROLE:-Viewer}"
# Same images as cephadm (src/python-common/ceph/cephadm/images.py)
PROM_IMAGE="${PROM_IMAGE:-quay.io/prometheus/prometheus:v3.6.0}"
GRAFANA_IMAGE="${GRAFANA_IMAGE:-quay.io/ceph/grafana:12.3.1}"

PROM_NAME=crimson-prometheus
GRAFANA_NAME=crimson-grafana
PROM_VOLUME=crimson-prometheus-data
GRAFANA_VOLUME=crimson-grafana-data

die() {
    # One line per argument
    printf 'ERROR: %s\n' "$1" >&2
    shift
    [[ $# -eq 0 ]] || printf '       %s\n' "$@" >&2
    exit 1
}
log() { echo "==> $*"; }

engine() {
    if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
        echo "$CONTAINER_ENGINE"
    elif command -v podman >/dev/null; then
        echo podman
    elif command -v docker >/dev/null; then
        echo docker
    else
        die "neither podman nor docker is installed"
    fi
}

require_build_dir() {
    [[ -x "$CEPH" && -f "$BUILD_DIR/ceph.conf" ]] ||
        die "run this from the vstart build directory (bin/ceph and ceph.conf not found)"
}

ceph_cmd() {
    timeout 30 "$CEPH" "$@" 2>/dev/null
}

conf_file_value() {
    # Value of option $2 for osd.$1 in ceph.conf (vstart -o writes there), from
    # [osd.N], else [osd], else [global]. Prints nothing if it is not set.
    python3 - "$BUILD_DIR/ceph.conf" "osd.$1" "$2" <<'EOF'
import re, sys
path, name, opt = sys.argv[1:]
norm = lambda s: re.sub(r'[\s_]+', '_', s.strip())
sections, cur = {}, None
for line in open(path):
    line = line.strip()
    if not line or line[0] in ';#':
        continue
    m = re.match(r'^\[(.+)\]$', line)
    if m:
        cur = sections.setdefault(m.group(1).strip(), {})
    elif cur is not None and '=' in line:
        k, v = line.split('=', 1)
        cur[norm(k)] = v.strip()
for sec in (name, 'osd', 'global'):
    if norm(opt) in sections.get(sec, {}):
        print(sections[sec][norm(opt)])
        break
EOF
}

running_value() {
    # Value of option $2 in the running osd.$1. Prints the value and returns 0;
    # returns 2 if the OSD does not know the option, 1 on other errors (the
    # error is printed on stderr).
    local out err msg rc=0
    err=$(mktemp)
    out=$(timeout 30 "$CEPH" tell "osd.$1" config get "$2" -f json 2>"$err") || rc=$?
    msg=$(grep -v 'WARNING: all dangerous and experimental features' "$err" | sed '/^$/d' || true)
    rm -f "$err"
    if [[ $rc -ne 0 ]]; then
        echo "${msg:-ceph tell osd.$1 failed (exit $rc)}" >&2
        [[ "$msg" == *ENOENT* ]] && return 2
        return 1
    fi
    if ! python3 -c 'import json, sys; print(json.loads(sys.stdin.read())[sys.argv[1]])' \
            "$2" <<< "$out" 2>/dev/null; then
        echo "unexpected output from 'ceph tell osd.$1 config get $2': $out" >&2
        return 1
    fi
}

resolve_option() {
    # Set RESOLVED to the value of option $2 for osd.$1: the running value if
    # the OSD answers, else the ceph.conf value, else $3. Not called in a
    # subshell, so that die() stops the script.
    local id="$1" opt="$2" def="$3" conf run errf err rc=0
    conf=$(conf_file_value "$id" "$opt")
    errf=$(mktemp)
    run=$(running_value "$id" "$opt" 2>"$errf") || rc=$?
    err=$(cat "$errf")
    rm -f "$errf"
    case $rc in
        0)
            if [[ -n "$conf" && "$conf" != "$run" ]]; then
                log "WARNING: ceph.conf has $opt = $conf, but osd.$id runs with $run" \
                    "(restart the OSDs to use the ceph.conf value). Using $run."
            fi
            RESOLVED="$run" ;;
        2)
            die "osd.$id does not know the option $opt:" \
                "$err" \
                "The crimson-osd binary was built without the crimson Prometheus options" \
                "(PR #71993). Build crimson-osd again, then restart vstart." ;;
        *)
            if [[ -n "$conf" ]]; then
                log "WARNING: cannot ask osd.$id for $opt ($err). Using the ceph.conf value $conf."
                RESOLVED="$conf"
            else
                log "WARNING: cannot ask osd.$id for $opt ($err), and it is not in ceph.conf."
                RESOLVED="$def"
            fi ;;
    esac
}

write_targets() {
    require_build_dir
    mkdir -p "$STATE_DIR/prometheus"
    local ids
    ids=$(ceph_cmd osd ls) || die "cannot list OSDs: is the vstart cluster running?"
    if [[ -z "$ids" ]]; then
        log "No OSDs: writing an empty target list"
        echo '[]' > "$STATE_DIR/prometheus/crimson_osds.json"
        return
    fi

    local first base addr
    first=$(head -1 <<< "$ids")
    if [[ -n "${PORT_BASE:-}" ]]; then
        base="$PORT_BASE"
    else
        resolve_option "$first" crimson_prometheus_port_base ""
        base="$RESOLVED"
    fi
    [[ -n "$base" ]] ||
        die "cannot find crimson_prometheus_port_base (osd.$first and ceph.conf); set PORT_BASE"
    [[ "$base" =~ ^[0-9]+$ ]] || die "crimson_prometheus_port_base is not a number: '$base'"
    [[ "$base" -ne 0 ]] ||
        die "crimson_prometheus_port_base is 0 (endpoint off)." \
            "Start vstart with -o 'crimson_prometheus_port_base = 9400'"
    resolve_option "$first" crimson_prometheus_address 0.0.0.0
    addr="$RESOLVED"
    [[ "$addr" == 0.0.0.0 || -z "$addr" ]] && addr=127.0.0.1

    log "Writing OSD targets ($addr, port = $base + osd id)"
    {
        echo '['
        local sep=""
        for id in $ids; do
            printf '%s  {"targets": ["%s:%d"], "labels": {"ceph_daemon": "osd.%d", "host": "%s"}}' \
                "$sep" "$addr" $((base + id)) "$id" "$(hostname -s)"
            sep=$',\n'
        done
        printf '\n]\n'
    } > "$STATE_DIR/prometheus/crimson_osds.json"
    for id in $ids; do
        if curl -sf -m 3 -o /dev/null "http://$addr:$((base + id))/metrics"; then
            echo "    osd.$id  $addr:$((base + id))  up"
        else
            echo "    osd.$id  $addr:$((base + id))  NOT REACHABLE (crimson OSD? started after the option was set? see out/osd.$id.log)"
        fi
    done
}

write_config() {
    mkdir -p "$STATE_DIR"/prometheus \
             "$STATE_DIR"/grafana/provisioning/{datasources,dashboards} \
             "$STATE_DIR"/grafana/dashboards
    cat > "$STATE_DIR/prometheus/prometheus.yml" <<EOF
# generated by $(basename "$0")
global:
  scrape_interval: ${SCRAPE_INTERVAL}

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:${PROM_PORT}']

  - job_name: crimson-osd
    file_sd_configs:
      - files: ['/etc/prometheus/crimson_osds.json']
EOF

    cat > "$STATE_DIR/grafana/provisioning/datasources/prometheus.yml" <<EOF
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    url: http://localhost:${PROM_PORT}
    isDefault: true
    jsonData:
      timeInterval: ${SCRAPE_INTERVAL}
EOF

    cat > "$STATE_DIR/grafana/provisioning/dashboards/crimson.yml" <<EOF
apiVersion: 1
providers:
  - name: crimson
    folder: Crimson
    type: file
    allowUiUpdates: true
    updateIntervalSeconds: 10
    options:
      path: /var/lib/grafana/dashboards
EOF
}

copy_dashboard() {
    mkdir -p "$STATE_DIR/grafana/dashboards"
    cp "$SCRIPT_DIR/crimson-osd.json" "$STATE_DIR/grafana/dashboards/"
    log "Dashboard copied: $STATE_DIR/grafana/dashboards/crimson-osd.json"
}

remove_containers() {
    local e
    e=$(engine)
    "$e" rm -f "$PROM_NAME" "$GRAFANA_NAME" >/dev/null 2>&1 || true
}

port_user() {
    # Prints the listener on TCP port $1 (empty if the port is free)
    if command -v ss >/dev/null; then
        ss -Hltnp "sport = :$1" 2>/dev/null | awk '{print $4, $6}'
    elif (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then
        echo "127.0.0.1:$1"
    fi
}

choose_port() {
    # $1 = variable name (PROM_PORT or GRAFANA_PORT), $2 = 1 if the user set it
    local var="$1" set_by_user="$2" port="${!1}" user
    user=$(port_user "$port")
    [[ -z "$user" ]] && return
    if [[ "$set_by_user" == 1 ]]; then
        die "$var $port is in use:" "$user" "Set $var to a free port, or unset it to use the next free port."
    fi
    local first="$port"
    while [[ -n "$(port_user "$port")" ]]; do
        port=$((port + 1))
        [[ $port -lt $((first + 100)) ]] || die "no free port in $first-$port for $var"
    done
    log "Port $first is in use ($user): using $var=$port"
    printf -v "$var" '%s' "$port"
}

save_settings() {
    mkdir -p "$STATE_DIR"
    local k
    for k in "${SAVED_VARS[@]}"; do
        [[ $k == CONTAINER_ENGINE ]] && echo "$k=$(engine)" || echo "$k=${!k}"
    done > "$STATE_DIR/env"
}

running() {
    [[ "$("$(engine)" inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == true ]]
}

wait_ready() {
    # $1 = container, $2 = URL that answers when it is ready
    local _
    for _ in $(seq 1 60); do
        running "$1" || die "$1 stopped. Last lines of '$(engine) logs $1':" \
            "$("$(engine)" logs --tail 5 "$1" 2>&1)"
        curl -sf -o /dev/null "$2" && return
        sleep 1
    done
    die "$1 is running, but $2 does not answer: see '$(engine) logs $1'"
}

start() {
    local e
    require_build_dir
    e=$(engine)
    # Remove our old containers first, so that their ports are free
    remove_containers
    choose_port PROM_PORT "$PROM_PORT_SET"
    choose_port GRAFANA_PORT "$GRAFANA_PORT_SET"
    [[ "$PROM_PORT" != "$GRAFANA_PORT" ]] || die "PROM_PORT and GRAFANA_PORT are both $PROM_PORT"
    save_settings
    write_config
    write_targets
    copy_dashboard

    log "Starting Prometheus ($e, $PROM_IMAGE)"
    "$e" run -d --name "$PROM_NAME" --net=host \
        -v "$STATE_DIR/prometheus:/etc/prometheus:ro,z" \
        -v "$PROM_VOLUME:/prometheus" \
        "$PROM_IMAGE" \
        --config.file=/etc/prometheus/prometheus.yml \
        --storage.tsdb.path=/prometheus \
        --storage.tsdb.retention.time="$RETENTION" \
        --web.listen-address="$LISTEN_ADDR:$PROM_PORT" \
        --web.enable-lifecycle >/dev/null

    log "Starting Grafana ($GRAFANA_IMAGE)"
    "$e" run -d --name "$GRAFANA_NAME" --net=host \
        -v "$STATE_DIR/grafana/provisioning:/etc/grafana/provisioning:ro,z" \
        -v "$STATE_DIR/grafana/dashboards:/var/lib/grafana/dashboards:ro,z" \
        -v "$GRAFANA_VOLUME:/var/lib/grafana" \
        -e GF_SERVER_HTTP_ADDR="$LISTEN_ADDR" \
        -e GF_SERVER_HTTP_PORT="$GRAFANA_PORT" \
        -e GF_SECURITY_ADMIN_PASSWORD="$GRAFANA_ADMIN_PASSWORD" \
        -e GF_AUTH_ANONYMOUS_ENABLED=true \
        -e GF_AUTH_ANONYMOUS_ORG_ROLE="$GRAFANA_ANON_ROLE" \
        -e GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/crimson-osd.json \
        "$GRAFANA_IMAGE" >/dev/null

    log "Waiting for Prometheus and Grafana"
    wait_ready "$PROM_NAME" "http://$LISTEN_ADDR:$PROM_PORT/-/ready"
    wait_ready "$GRAFANA_NAME" "http://$LISTEN_ADDR:$GRAFANA_PORT/api/health"

    log "Done"
    echo "    Prometheus: http://$LISTEN_ADDR:$PROM_PORT/targets"
    echo "    Grafana:    http://$LISTEN_ADDR:$GRAFANA_PORT  (anonymous $GRAFANA_ANON_ROLE; admin / \$GRAFANA_ADMIN_PASSWORD)"
    [[ "$LISTEN_ADDR" == 127.0.0.1 ]] &&
        echo "    From another machine: ssh -L $GRAFANA_PORT:localhost:$GRAFANA_PORT -L $PROM_PORT:localhost:$PROM_PORT <this host>"
    echo "    Run '$0 status' after one scrape interval to see the target health."
}

status() {
    local e
    e=$(engine)
    "$e" ps -a --filter "name=crimson-" --format '{{.Names}}\t{{.Status}}'
    if ! running "$PROM_NAME"; then
        # Do not ask the port: another Prometheus can answer there
        echo "$PROM_NAME is not running: see '$e logs $PROM_NAME', then run '$0 start'"
        return 1
    fi
    echo "Prometheus: http://$LISTEN_ADDR:$PROM_PORT  Grafana: http://$LISTEN_ADDR:$GRAFANA_PORT"
    curl -sf "http://$LISTEN_ADDR:$PROM_PORT/api/v1/targets" | python3 -c '
import json, sys
for t in json.load(sys.stdin)["data"]["activeTargets"]:
    print("    {:12} {:28} {:5} {}".format(t["labels"]["job"],
          t["labels"].get("ceph_daemon", t["scrapeUrl"]), t["health"], t["lastError"]))
' || echo "Prometheus is not reachable at $LISTEN_ADDR:$PROM_PORT"
}

case "${1:-}" in
    start)     start ;;
    targets)   write_targets ;;
    dashboard) copy_dashboard ;;
    status)    status ;;
    stop)      remove_containers; log "Containers removed" ;;
    purge)     remove_containers
               "$(engine)" volume rm -f "$PROM_VOLUME" "$GRAFANA_VOLUME" >/dev/null 2>&1 || true
               log "Containers and data volumes removed" ;;
    *)         sed -n '3,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 1 ;;
esac
