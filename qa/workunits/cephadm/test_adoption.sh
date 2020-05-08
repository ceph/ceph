#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CEPHADM_SRC_DIR=${SCRIPT_DIR}/../../../src/cephadm
CORPUS_COMMIT=97996ce562731a7260ab9d90649af3ff4b5fff3f

[ -z "$SUDO" ] && SUDO=sudo
if [ -z "$CEPHADM" ]; then
    CEPHADM=${CEPHADM_SRC_DIR}/cephadm
fi

# at this point, we need $CEPHADM set
if ! [ -x "$CEPHADM" ]; then
    echo "cephadm not found. Please set \$CEPHADM"
    exit 1
fi

# respawn ourselves with a shebang
if [ -z "$PYTHON_KLUDGE" ]; then
    # see which pythons we should test with
    PYTHONS=""
    which python3 && PYTHONS="$PYTHONS python3"
    which python2 && PYTHONS="$PYTHONS python2"
    echo "PYTHONS $PYTHONS"
    if [ -z "$PYTHONS" ]; then
	echo "No PYTHONS found!"
	exit 1
    fi

    TMPBINDIR=$(mktemp -d)
    trap "rm -rf $TMPBINDIR" EXIT
    ORIG_CEPHADM="$CEPHADM"
    CEPHADM="$TMPBINDIR/cephadm"
    for p in $PYTHONS; do
	echo "=== re-running with $p ==="
	ln -s `which $p` $TMPBINDIR/python
	echo "#!$TMPBINDIR/python" > $CEPHADM
	cat $ORIG_CEPHADM >> $CEPHADM
	chmod 700 $CEPHADM
	$TMPBINDIR/python --version
	PYTHON_KLUDGE=1 CEPHADM=$CEPHADM $0
	rm $TMPBINDIR/python
    done
    rm -rf $TMPBINDIR
    echo "PASS with all of: $PYTHONS"
    exit 0
fi

# combine into a single var
CEPHADM_BIN="$CEPHADM"
CEPHADM="$SUDO $CEPHADM_BIN"

## adopt
CORPUS_GIT_SUBMOD="cephadm-adoption-corpus"
TMPDIR=$(mktemp -d)
git clone https://github.com/ceph/$CORPUS_GIT_SUBMOD $TMPDIR
trap "$SUDO rm -rf $TMPDIR" EXIT

git -C $TMPDIR checkout $CORPUS_COMMIT
CORPUS_DIR=${TMPDIR}/archive

for subdir in `ls ${CORPUS_DIR}`; do
    for tarfile in `ls ${CORPUS_DIR}/${subdir} | grep .tgz`; do
	tarball=${CORPUS_DIR}/${subdir}/${tarfile}
	FSID_LEGACY=`echo "$tarfile" | cut -c 1-36`
	TMP_TAR_DIR=`mktemp -d -p $TMPDIR`
	$SUDO tar xzvf $tarball -C $TMP_TAR_DIR
	NAMES=$($CEPHADM ls --legacy-dir $TMP_TAR_DIR | jq -r '.[].name')
	for name in $NAMES; do
            $CEPHADM adopt \
                     --style legacy \
                     --legacy-dir $TMP_TAR_DIR \
                     --name $name
            # validate after adopt
            out=$($CEPHADM ls | jq '.[]' \
                      | jq 'select(.name == "'$name'")')
            echo $out | jq -r '.style' | grep 'cephadm'
            echo $out | jq -r '.fsid' | grep $FSID_LEGACY
	done
	# clean-up before next iter
	$CEPHADM rm-cluster --fsid $FSID_LEGACY --force
	$SUDO rm -rf $TMP_TAR_DIR
    done
done

echo "OK"
