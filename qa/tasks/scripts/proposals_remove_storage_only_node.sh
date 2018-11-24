# proposals_remove_storage_only_node.sh
#
# remove first storage-only node from proposals
#
# args: full path of proposals directory,
#       hostname of node to be deleted,
#       name of storage profile (e.g. "default")

set -ex

PROPOSALSDIR=$1
NODE_TO_DELETE=$2
STORAGE_PROFILE=$3

echo "Before" 2>/dev/null
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster/
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions/

basedirsls=$PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster
basediryml=$PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions
mv $basedirsls/${NODE_TO_DELETE}.sls $basedirsls/${NODE_TO_DELETE}.sls-DISABLED
mv $basediryml/${NODE_TO_DELETE}.yml $basedirsls/${NODE_TO_DELETE}.yml-DISABLED

echo "After" 2>/dev/null
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster/
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions/
