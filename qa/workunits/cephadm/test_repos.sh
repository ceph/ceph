#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
CEPHADM_SRC_DIR=${SCRIPT_DIR}/../../../src/cephadm

[ -d "$TMPDIR" ] || TMPDIR=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)
trap "$SUDO rm -rf $TMPDIR" EXIT

if [ -z "$CEPHADM" ]; then
    CEPHADM=`mktemp -p $TMPDIR tmp.cephadm.XXXXXX`
    ${CEPHADM_SRC_DIR}/build.sh "$CEPHADM"
fi

# this is a pretty weak test, unfortunately, since the
# package may also be in the base OS.
function test_install_uninstall() {
    ( sudo apt update && \
	  sudo apt -y install cephadm && \
	  sudo $CEPHADM install && \
	  sudo apt -y remove cephadm ) || \
	( sudo yum -y install cephadm && \
	      sudo $CEPHADM install && \
	      sudo yum -y remove cephadm ) || \
	( sudo dnf -y install cephadm && \
	      sudo $CEPHADM install && \
	      sudo dnf -y remove cephadm ) || \
	( sudo zypper -n install cephadm && \
	      sudo $CEPHADM install && \
	      sudo zypper -n remove cephadm )
}

function test_release() {
    local release=$1
    sudo $CEPHADM -v add-repo --release "$release"
    test_install_uninstall
    sudo $CEPHADM -v rm-repo
}

REPO_URL="${REPO_URL:-https://download.ceph.com}"

function release_published() {
    local release=$1
    local base

    if grep -q debian /etc/*-release; then
        . /etc/os-release
        base="${REPO_URL}/debian-${release}/dists/${VERSION_CODENAME}"
        curl -sf -o /dev/null "${base}/InRelease" && return 0
        curl -sf -o /dev/null "${base}/Release" && return 0
        return 1
    elif grep -q rhel /etc/*-release; then
        . /etc/os-release
        curl -sf -o /dev/null \
            "${REPO_URL}/rpm-${release}/el${VERSION_ID%%.*}/noarch/repodata/repomd.xml"
    else
        return 1
    fi
}

function test_release_if_published() {
    local release=$1
    if ! release_published "$release"; then
        echo "Skipping release $release (not published for this distro)"
        return 0
    fi
    test_release "$release"
}

sudo $CEPHADM -v add-repo --dev main
test_install_uninstall
sudo $CEPHADM -v rm-repo

test_release_if_published reef
test_release_if_published 18.2.8
test_release_if_published squid
test_release_if_published 19.2.4
test_release_if_published tentacle

echo OK.
