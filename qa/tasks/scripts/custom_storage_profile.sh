# custom_storage_profile.sh
#
# args: full path to proposals directory,
#       path to file containing custom storage profile

set -ex

PROPOSALSDIR=$1
SOURCEFILE=$2

function _initialize_minion_configs_array {
    local DIR=$1

    shopt -s nullglob
    pushd $DIR >/dev/null
    MINION_CONFIGS_ARRAY=(*.yaml *.yml)
    echo "Made global array containing the following files (from ->$DIR<-):"
    printf '%s\n' "${MINION_CONFIGS_ARRAY[@]}"
    popd >/dev/null
    shopt -u nullglob
}

test -f "$SOURCEFILE"
file $SOURCEFILE

# prepare new profile, which will be exactly the same as the default
# profile except the files in stack/default/ceph/minions/ will be
# overwritten with our chosen OSD configuration
#
cp -a $PROPOSALSDIR/profile-default $PROPOSALSDIR/profile-custom
DESTDIR="$PROPOSALSDIR/profile-custom/stack/default/ceph/minions"
_initialize_minion_configs_array $DESTDIR
for DESTFILE in "${MINION_CONFIGS_ARRAY[@]}" ; do
    cp $SOURCEFILE $DESTDIR/$DESTFILE
done
echo "Your custom storage profile $SOURCEFILE has the following contents:"
cat $DESTDIR/$DESTFILE
ls -lR $PROPOSALSDIR/profile-custom
echo "OK" >/dev/null
