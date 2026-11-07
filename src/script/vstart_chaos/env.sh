# Source this before using the vstart_chaos tools.
#   CEPH_BUILD   build dir the vstart cluster runs from (default: the main
#                checkout's build/, also when this file is in a worktree, so
#                set it explicitly when running from a worktree or another
#                tree: run_chaos.sh destroys the vstart cluster in it)
#   CHAOS_RUNS   where run output goes (default $CEPH_BUILD/chaos-runs)
#   CHAOS_SNAPS  where binary snapshots go (default $CEPH_BUILD/chaos-bins)
CHAOS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [ -z "$CEPH_BUILD" ]; then
    CEPH_BUILD=$(dirname "$(git -C "$CHAOS_DIR" rev-parse --path-format=absolute --git-common-dir)")/build
fi
: ${CHAOS_RUNS:=$CEPH_BUILD/chaos-runs}
: ${CHAOS_SNAPS:=$CEPH_BUILD/chaos-bins}
export CHAOS_DIR CEPH_BUILD CHAOS_RUNS CHAOS_SNAPS
CEPH_TOP=$(dirname "$CEPH_BUILD")
export PYTHONPATH=$CEPH_TOP/src/pybind:$CEPH_BUILD/lib/cython_modules/lib.3:$CEPH_TOP/src/python-common:$CHAOS_DIR:$PYTHONPATH
export LD_LIBRARY_PATH=$CEPH_BUILD/lib:$LD_LIBRARY_PATH
export PATH=$CEPH_BUILD/bin:$CEPH_TOP/src/bin:$PATH
export CEPH_CONF=$CEPH_BUILD/ceph.conf
export CEPH_KEYRING=$CEPH_BUILD/keyring
