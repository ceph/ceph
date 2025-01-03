#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CEPHADM_SRC_DIR=${SCRIPT_DIR}/../../../src/cephadm
CORPUS_COMMIT=9cd9ad020d93b0b420924fec55da307aff8bd422

[ -z "$SUDO" ] && SUDO=sudo

[ -d "$TMPDIR" ] || TMPDIR=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)
trap "$SUDO rm -rf $TMPDIR" EXIT

if [ -z "$CEPHADM" ]; then
    CEPHADM=`mktemp -p $TMPDIR tmp.cephadm.XXXXXX`
    ${CEPHADM_SRC_DIR}/build.sh "$CEPHADM"
fi

# at this point, we need $CEPHADM set
if ! [ -x "$CEPHADM" ]; then
    echo "cephadm not found. Please set \$CEPHADM"
    exit 1
fi

# combine into a single var
CEPHADM_BIN="$CEPHADM"
CEPHADM="$SUDO $CEPHADM_BIN"

## adopt
CORPUS_GIT_SUBMOD="cephadm-adoption-corpus"
GIT_CLONE_DIR=${TMPDIR}/${CORPUS_GIT_SUBMOD}
git clone https://github.com/ceph/$CORPUS_GIT_SUBMOD $GIT_CLONE_DIR

git -C $GIT_CLONE_DIR checkout $CORPUS_COMMIT
CORPUS_DIR=${GIT_CLONE_DIR}/archive

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
