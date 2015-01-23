#!/bin/bash -e
#
# Instead of a quilt workflow, we use a git tree that contains
# all the commits on top of a stable tarball.
#
# When updating this package, just either update the git tree
# below (use rebase!) or change the tree path and use your own
#
# That way we can easily rebase against the next stable release
# when it comes.
#
# Based on SUSE:SLE-12:GA/qemu/update_git.sh
#
GIT_TREE=https://github.com/SUSE/ceph.git
# Downloaded repos are cached here. Must be writable by you.
GIT_LOCAL_TREE=/var/tmp/osbuild-packagecache/ceph
GIT_BRANCH=distro/suse-0-80-8b
GIT_UPSTREAM_TAG=v0.80.8
GIT_DIR=/dev/shm/ceph-git-dir
CMP_DIR=/dev/shm/ceph-cmp-dir

rm -rf $GIT_DIR
rm -rf $CMP_DIR

if ! [ -d "$GIT_LOCAL_TREE" ]; then
    echo "Create locally cached clone of $GIT_BRANCH branch of remote git tree, using tag:" \
         "$GIT_UPSTREAM_TAG"
    git clone $GIT_TREE $GIT_LOCAL_TREE -b $GIT_BRANCH
    (cd $GIT_LOCAL_TREE && git remote add upstream $GIT_TREE)
    (cd $GIT_LOCAL_TREE && git remote update)
fi

if [ -d "$GIT_LOCAL_TREE" ]; then
    echo "Processing $GIT_BRANCH branch of local git tree, using tag:" \
         "$GIT_UPSTREAM_TAG"
    (cd $GIT_LOCAL_TREE && git remote update)
    if ! (cd $GIT_LOCAL_TREE && git show-branch $GIT_BRANCH &>/dev/null); then
        echo "Branch $GIT_BRANCH not found - creating locally"
        (cd $GIT_LOCAL_TREE && git checkout -b $GIT_BRANCH upstream/$GIT_BRANCH)
    fi
    git clone -ls $GIT_LOCAL_TREE $GIT_DIR -b $GIT_BRANCH
    if ! (cd $GIT_LOCAL_TREE && git remote show upstream &>/dev/null); then
        echo "Remote for upstream git tree not found. Adding named upstream ceph and updating"
        (cd $GIT_DIR && git remote add upstream $GIT_TREE)
        (cd $GIT_DIR && git remote update)
   fi
else
    echo "Local git cache creation failed, bailing out."
    exit
fi
(cd $GIT_DIR && git format-patch -N $GIT_UPSTREAM_TAG --suffix= -o $CMP_DIR >/dev/null)
CEPH_VERSION=`cd $GIT_DIR && git describe --abbrev=0 2>/dev/null`
echo "ceph version: $CEPH_VERSION"

rm -rf $GIT_DIR

(
    CHANGED_COUNT=0
    UNCHANGED_COUNT=0
    DELETED_COUNT=0
    ADDED_COUNT=0

    shopt -s nullglob

# Process patches to eliminate useless differences resulting from rebases;
# limit file names to 40 chars before extension and remove git signature.
# ('22' below gets us past dir prefix)
    for i in $CMP_DIR/*; do
        head -n -3 $i | tail -n +2 > $CMP_DIR/${i:22:40}.patch
        rm $i
    done

    for i in 0???-*.patch; do
        if [ -e $CMP_DIR/$i ]; then
            if cmp -s $CMP_DIR/$i $i; then
                rm $CMP_DIR/$i
                let UNCHANGED_COUNT+=1
            else
                mv $CMP_DIR/$i .
                let CHANGED_COUNT+=1
            fi
        else
            osc rm --force $i
            let DELETED_COUNT+=1
        fi
    done

    for i in $CMP_DIR/*; do
        mv $i .
        osc add ${i##*/}
        let ADDED_COUNT+=1
    done

    while IFS= read -r line; do
        if [ "$line" = "PATCH_FILES" ]; then
            for i in 0???-*.patch; do
                NUM=${i%%-*}
                echo -e "Patch$NUM:      $i"
            done
        elif [ "$line" = "PATCH_EXEC" ]; then
            for i in 0???-*.patch; do
                NUM=${i%%-*}
                echo "%patch$NUM -p1"
            done
        elif [ "$line" = "CEPH_VERSION" ]; then
            echo "Version:        ${CEPH_VERSION:1}"
        else
            echo "$line"
        fi
    done < ceph.spec.in > $CMP_DIR/ceph.spec
    if cmp -s ceph.spec $CMP_DIR/ceph.spec; then
        echo "ceph.spec unchanged"
    else
        mv $CMP_DIR/ceph.spec ceph.spec
        echo "ceph.spec regenerated"
    fi

    echo "git patch summary"
    echo "  unchanged: $UNCHANGED_COUNT"
    echo "    changed: $CHANGED_COUNT"
    echo "    deleted: $DELETED_COUNT"
    echo "      added: $ADDED_COUNT"
)

rm -rf $CMP_DIR
