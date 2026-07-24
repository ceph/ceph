#!/bin/sh
# container-run.sh – reference script for running cephadm inside a container
# with the minimal set of privileges required (no --privileged flag).
#
# Works with both Docker and Podman; no engine-specific flags are used.
#
# Usage:
#   ./container-run.sh [cephadm arguments …]
#
# Examples:
#   ./container-run.sh --help
#   ./container-run.sh version
#   ./container-run.sh bootstrap --mon-ip 192.168.1.10
#   ./container-run.sh ceph-volume lvm list
#
# Environment variables:
#   CEPHADM_IMAGE  – container image to use (default: quay.ceph.io/ceph-ci/ceph:main)
#   CONTAINER_ENGINE – 'podman' or 'docker' (auto-detected when not set)

set -e

IMAGE="${CEPHADM_IMAGE:-quay.ceph.io/ceph-ci/ceph:main}"

# ---------------------------------------------------------------------------
# Auto-detect container engine (podman preferred, docker as fallback)
# ---------------------------------------------------------------------------
if [ -z "${CONTAINER_ENGINE}" ]; then
    if command -v podman >/dev/null 2>&1; then
        CONTAINER_ENGINE=podman
    elif command -v docker >/dev/null 2>&1; then
        CONTAINER_ENGINE=docker
    else
        echo "ERROR: neither podman nor docker found in PATH" >&2
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Linux capabilities required instead of --privileged
#
# CAP_SYS_ADMIN    – sysctl writes, mount operations, ceph-volume, namespace ops
# CAP_NET_ADMIN    – network interface inspection, firewall (firewalld/iptables)
# CAP_SYS_PTRACE   – inspect host processes (via --pid host)
# CAP_CHOWN        – set ownership of daemon data directories
# CAP_DAC_OVERRIDE – read/write host files protected by DAC
# CAP_DAC_READ_SEARCH – traverse/read protected host directories
# CAP_FOWNER       – file permission operations regardless of UID
# CAP_SETUID       – switch UID for daemons
# CAP_SETGID       – switch GID for daemons
# CAP_SYS_RAWIO    – raw block I/O for ceph-volume / SMART data
# CAP_MKNOD        – create device nodes
# CAP_SYS_RESOURCE – adjust resource limits
# CAP_AUDIT_WRITE  – write to kernel audit log
# CAP_KILL         – send signals to host processes
# ---------------------------------------------------------------------------
CAPS="\
    --cap-add SYS_ADMIN \
    --cap-add NET_ADMIN \
    --cap-add SYS_PTRACE \
    --cap-add CHOWN \
    --cap-add DAC_OVERRIDE \
    --cap-add DAC_READ_SEARCH \
    --cap-add FOWNER \
    --cap-add SETUID \
    --cap-add SETGID \
    --cap-add SYS_RAWIO \
    --cap-add MKNOD \
    --cap-add SYS_RESOURCE \
    --cap-add AUDIT_WRITE \
    --cap-add KILL \
"

# ---------------------------------------------------------------------------
# Namespace flags
#
# --network host   Required: cephadm must see host NICs/routes and bind to
#                  the correct IPs when deploying monitors and other services.
#
# --pid host       Required (for now): cephadm inspects host processes via
#                  /proc and sends signals.  Future refactoring to use only
#                  D-Bus / systemctl queries may allow removing this flag.
#
# --cgroupns host  Required (for now): cephadm/systemd unit management needs
#                  to see the real host cgroup hierarchy.  Future refactoring
#                  may allow removing this flag.
#
# --ipc host       Allows shared-memory IPC with host processes when needed.
# ---------------------------------------------------------------------------
NAMESPACES="\
    --network host \
    --pid host \
    --cgroupns host \
    --ipc host \
    --uts host \
"

# ---------------------------------------------------------------------------
# Security options
#
# --security-opt label=disable
#   Disables SELinux / AppArmor label confinement so the bind-mounts below
#   (especially /sys and /dev) are accessible.  On systems without SELinux or
#   AppArmor this flag is a no-op.  As an alternative, a custom policy could
#   be written to allow the required accesses without fully disabling labelling.
# ---------------------------------------------------------------------------
SECURITY="--security-opt label=disable"

# ---------------------------------------------------------------------------
# Bind mounts – host paths exposed inside the container
#
# Read-write mounts grant cephadm the ability to install and manage services.
# Read-only mounts (":ro") are used where cephadm only needs to read data.
# ---------------------------------------------------------------------------
MOUNTS="\
    -v /run/systemd:/run/systemd \
    -v /run/dbus/system_bus_socket:/run/dbus/system_bus_socket \
    -v /sys:/sys \
    -v /dev:/dev \
    -v /run/udev:/run/udev:ro \
    -v /proc:/proc \
    -v /var/lib/ceph:/var/lib/ceph \
    -v /var/log/ceph:/var/log/ceph \
    -v /etc/ceph:/etc/ceph \
    -v /etc/systemd/system:/etc/systemd/system \
    -v /etc/sysctl.d:/etc/sysctl.d \
    -v /etc/logrotate.d:/etc/logrotate.d \
    -v /run/cephadm:/run/cephadm \
    -v /tmp:/tmp \
"

# Container engine socket so cephadm can launch further containers.
# Podman uses a user/system socket path; Docker uses /var/run/docker.sock.
if [ "${CONTAINER_ENGINE}" = "podman" ]; then
    PODMAN_SOCK="${XDG_RUNTIME_DIR:-/run}/podman/podman.sock"
    if [ ! -S "${PODMAN_SOCK}" ]; then
        # Fall back to the system-wide socket
        PODMAN_SOCK="/run/podman/podman.sock"
    fi
    MOUNTS="${MOUNTS} -v ${PODMAN_SOCK}:/run/podman/podman.sock"
else
    MOUNTS="${MOUNTS} -v /var/run/docker.sock:/var/run/docker.sock"
fi

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
# shellcheck disable=SC2086
exec ${CONTAINER_ENGINE} run \
    --rm \
    --user root \
    ${CAPS} \
    ${NAMESPACES} \
    ${SECURITY} \
    ${MOUNTS} \
    "${IMAGE}" \
    "$@"
