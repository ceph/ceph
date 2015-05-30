#!/bin/sh -e

if [ ! -d .git ]; then
    echo "no .git present.  run this from the base dir of the git checkout."
    exit 1
fi

bindir=`dirname $0`

version=$1
[ -z "$version" ] && version=`git describe | cut -c 2-`
outfile="ceph-$version"

# update submodules
echo "updating submodules..."
force=$(if git submodule usage 2>&1 | grep --quiet 'update.*--force'; then echo --force ; fi)
if ! git submodule sync || ! git submodule update $force --init --recursive; then
    echo "Error: could not initialize submodule projects"
    echo "  Network connectivity might be required."
    exit 1
fi

# clean out old cruft...
echo "cleanup..."
rm -f $outfile.tar $outfile.tar.gz

# build new tarball
echo "building tarball..."
$bindir/git-archive-all.sh --prefix ceph-$version/ \
			   --verbose \
			   --ignore corpus \
			   $outfile.tar
gzip -9 $outfile.tar

echo "done."
