#!/bin/bash -ex
TEUTH_PATH=${1:-"teuthology"}
TEUTH_GIT=${2:-"https://github.com/ceph/teuthology"}
TEUTH_BRANCH=${3:-"master"}

mkdir -p $TEUTH_PATH
git init $TEUTH_PATH

pushd $TEUTH_PATH

echo Fetch upstream changes from $TEUTH_GIT
git fetch --tags --progress $TEUTH_GIT +refs/heads/*:refs/remotes/origin/*
git config remote.origin.url $TEUTH_GIT
git config --add remote.origin.fetch +refs/heads/*:refs/remotes/origin/*
git config remote.origin.url $TEUTH_GIT

# Check if branch has form origin/pr/*/merge
isPR="^origin\/pr\/"
if [[ "$TEUTH_BRANCH" =~ $isPR ]] ; then

git fetch --tags --progress https://github.com/suse/teuthology +refs/pull/*:refs/remotes/origin/pr/*
rev=$(git rev-parse refs/remotes/$TEUTH_BRANCH^{commit})

git config core.sparsecheckout
git checkout -f $rev
else
git checkout $TEUTH_BRANCH
fi

./bootstrap install

popd

