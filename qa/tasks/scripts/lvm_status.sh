# lvm_status.sh
#
# args: None

set -ex

pvs --all
vgs --all
lvs --all
