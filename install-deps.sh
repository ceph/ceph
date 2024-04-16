#!/usr/bin/env bash
#
# Ceph distributed storage system
#
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#
set -e

if ! [ "${_SOURCED_LIB_BUILD}" = 1 ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    CEPH_ROOT="${SCRIPT_DIR}"
    . "${CEPH_ROOT}/src/script/lib-build.sh" || exit 2
fi


DIR=/tmp/install-deps.$$
trap "rm -fr $DIR" EXIT
mkdir -p $DIR
if test $(id -u) != 0 ; then
    SUDO=sudo
fi
# enable UTF-8 encoding for programs like pip that expect to
# print more than just ascii chars
export LC_ALL=C.UTF-8

ARCH=$(uname -m)


function munge_ceph_spec_in {
    local with_seastar=$1
    shift
    local for_make_check=$1
    shift
    local OUTFILE=$1
    sed -e 's/@//g' < ceph.spec.in > $OUTFILE
    # http://rpm.org/user_doc/conditional_builds.html
    if $with_seastar; then
        sed -i -e 's/%bcond_with seastar/%bcond_without seastar/g' $OUTFILE
    fi
    if $for_make_check; then
        sed -i -e 's/%bcond_with make_check/%bcond_without make_check/g' $OUTFILE
    fi
}

function munge_debian_control {
    local version=$1
    shift
    local control=$1
    case "$version" in
        *squeeze*|*wheezy*)
            control="/tmp/control.$$"
            grep -v babeltrace debian/control > $control
            ;;
    esac
    echo $control
}

function ensure_decent_gcc_on_ubuntu {
    ci_debug "Start ensure_decent_gcc_on_ubuntu() in install-deps.sh"
    # point gcc to the one offered by g++-7 if the used one is not
    # new enough
    local old=$(gcc -dumpfullversion -dumpversion)
    local new=$1
    local codename=$2
    if dpkg --compare-versions $old ge ${new}.0; then
        return
    fi

    if [ ! -f /usr/bin/g++-${new} ]; then
        $SUDO tee /etc/apt/sources.list.d/ubuntu-toolchain-r.list <<EOF
deb [lang=none] http://ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu $codename main
deb [arch=amd64 lang=none] http://mirror.nullivex.com/ppa/ubuntu-toolchain-r-test $codename main
EOF
        # import PPA's signing key into APT's keyring
        cat << ENDOFKEY | $SUDO apt-key add -
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: SKS 1.1.6
Comment: Hostname: keyserver.ubuntu.com

mI0ESuBvRwEEAMi4cDba7xlKaaoXjO1n1HX8RKrkW+HEIl79nSOSJyvzysajs7zUow/OzCQp
9NswqrDmNuH1+lPTTRNAGtK8r2ouq2rnXT1mTl23dpgHZ9spseR73s4ZBGw/ag4bpU5dNUSt
vfmHhIjVCuiSpNn7cyy1JSSvSs3N2mxteKjXLBf7ABEBAAG0GkxhdW5jaHBhZCBUb29sY2hh
aW4gYnVpbGRziLYEEwECACAFAkrgb0cCGwMGCwkIBwMCBBUCCAMEFgIDAQIeAQIXgAAKCRAe
k3eiup7yfzGKA/4xzUqNACSlB+k+DxFFHqkwKa/ziFiAlkLQyyhm+iqz80htRZr7Ls/ZRYZl
0aSU56/hLe0V+TviJ1s8qdN2lamkKdXIAFfavA04nOnTzyIBJ82EAUT3Nh45skMxo4z4iZMN
msyaQpNl/m/lNtOLhR64v5ZybofB2EWkMxUzX8D/FQ==
=LcUQ
-----END PGP PUBLIC KEY BLOCK-----
ENDOFKEY
        $SUDO env DEBIAN_FRONTEND=noninteractive apt-get update -y || true
        $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y g++-${new}
    fi
}

function ensure_python3_sphinx_on_ubuntu {
    ci_debug "Running ensure_python3_sphinx_on_ubuntu() in install-deps.sh"
    local sphinx_command=/usr/bin/sphinx-build
    # python-sphinx points $sphinx_command to
    # ../share/sphinx/scripts/python2/sphinx-build when it's installed
    # let's "correct" this
    if test -e $sphinx_command  && head -n1 $sphinx_command | grep -q python$; then
        $SUDO env DEBIAN_FRONTEND=noninteractive apt-get -y remove python-sphinx
    fi
}

function install_pkg_on_ubuntu {
    ci_debug "Running install_pkg_on_ubuntu() in install-deps.sh"
    local project=$1
    shift
    local sha1=$1
    shift
    local codename=$1
    shift
    local force=$1
    shift
    local pkgs=$@
    local missing_pkgs
    if [ $force = "force" ]; then
        missing_pkgs="$@"
    else
        for pkg in $pkgs; do
            if ! apt -qq list $pkg 2>/dev/null | grep -q installed; then
                missing_pkgs+=" $pkg"
                ci_debug "missing_pkgs=$missing_pkgs"
            fi
        done
    fi
    if test -n "$missing_pkgs"; then
        local shaman_url="https://shaman.ceph.com/api/repos/${project}/master/${sha1}/ubuntu/${codename}/repo"
        in_jenkins && echo -n "CI_DEBUG: Downloading $shaman_url ... "
        $SUDO curl --silent --fail --write-out "%{http_code}" --location $shaman_url --output /etc/apt/sources.list.d/$project.list
        $SUDO env DEBIAN_FRONTEND=noninteractive apt-get update -y -o Acquire::Languages=none -o Acquire::Translation=none || true
        $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install --allow-unauthenticated -y $missing_pkgs
    fi
}

boost_ver=1.82

function clean_boost_on_ubuntu {
    ci_debug "Running clean_boost_on_ubuntu() in install-deps.sh"
    # Find currently installed version. If there are multiple
    # versions, they end up newline separated
    local installed_ver=$(apt -qq list --installed ceph-libboost*-dev 2>/dev/null |
                              cut -d' ' -f2 |
                              cut -d'.' -f1,2 |
			      sort -u)
    # If installed_ver contains whitespace, we can't really count on it,
    # but otherwise, bail out if the version installed is the version
    # we want.
    if test -n "$installed_ver" &&
	    echo -n "$installed_ver" | tr '[:space:]' ' ' | grep -v -q ' '; then
	if echo "$installed_ver" | grep -q "^$boost_ver"; then
	    return
        fi
    fi

    # Historical packages
    $SUDO rm -f /etc/apt/sources.list.d/ceph-libboost*.list
    # Currently used
    $SUDO rm -f /etc/apt/sources.list.d/libboost.list
    # Refresh package list so things aren't in the available list.
    $SUDO env DEBIAN_FRONTEND=noninteractive apt-get update -y || true
    # Remove all ceph-libboost packages. We have an early return if
    # the desired version is already (and the only) version installed,
    # so no need to spare it.
    if test -n "$installed_ver"; then
	$SUDO env DEBIAN_FRONTEND=noninteractive apt-get -y --fix-missing remove "ceph-libboost*"
	# When an error occurs during `apt-get remove ceph-libboost*`, ceph-libboost* packages
	# may be not removed, so use `dpkg` to force remove ceph-libboost*.
	local ceph_libboost_pkgs=$(dpkg -l | grep ceph-libboost* | awk '{print $2}' |
		                        awk -F: '{print $1}')
	if test -n "$ceph_libboost_pkgs"; then
	    ci_debug "Force remove ceph-libboost* packages $ceph_libboost_pkgs"
	    $SUDO dpkg --purge --force-all $ceph_libboost_pkgs
	fi
    fi
}

function install_boost_on_ubuntu {
    ci_debug "Running install_boost_on_ubuntu() in install-deps.sh"
    # Once we get to this point, clean_boost_on_ubuntu() should ensure
    # that there is no more than one installed version.
    local installed_ver=$(apt -qq list --installed ceph-libboost*-dev 2>/dev/null |
                              grep -e 'libboost[0-9].[0-9]\+-dev' |
                              cut -d' ' -f2 |
                              cut -d'.' -f1,2)
    if test -n "$installed_ver"; then
        if echo "$installed_ver" | grep -q "^$boost_ver"; then
            return
        fi
    fi
    local codename=$1
    local project=libboost
    local sha1=2804368f5b807ba8334b0ccfeb8af191edeb996f
    install_pkg_on_ubuntu \
        $project \
        $sha1 \
        $codename \
        check \
        ceph-libboost-atomic${boost_ver}-dev \
        ceph-libboost-chrono${boost_ver}-dev \
        ceph-libboost-container${boost_ver}-dev \
        ceph-libboost-context${boost_ver}-dev \
        ceph-libboost-coroutine${boost_ver}-dev \
        ceph-libboost-date-time${boost_ver}-dev \
        ceph-libboost-filesystem${boost_ver}-dev \
        ceph-libboost-iostreams${boost_ver}-dev \
        ceph-libboost-program-options${boost_ver}-dev \
        ceph-libboost-python${boost_ver}-dev \
        ceph-libboost-random${boost_ver}-dev \
        ceph-libboost-regex${boost_ver}-dev \
        ceph-libboost-system${boost_ver}-dev \
        ceph-libboost-test${boost_ver}-dev \
        ceph-libboost-thread${boost_ver}-dev \
        ceph-libboost-timer${boost_ver}-dev \
	|| ci_debug "ceph-libboost package unavailable, you can build the submodule"

}

function version_lt {
    test $1 != $(echo -e "$1\n$2" | sort -rV | head -n 1)
}

function ensure_decent_gcc_on_rh {
    local old=$(gcc -dumpversion)
    local dts_ver=$1
    if version_lt $old $dts_ver; then
        if test -t 1; then
            # interactive shell
            cat <<EOF
Your GCC is too old. Please run following command to add DTS to your environment:

scl enable gcc-toolset-$dts_ver bash

Or add the following line to the end of ~/.bashrc and run "source ~/.bashrc" to add it permanently:

source scl_source enable gcc-toolset-$dts_ver
EOF
        else
            # non-interactive shell
            source /opt/rh/gcc-toolset-$dts_ver/enable
        fi
    fi
}

function populate_wheelhouse() {
    ci_debug "Running populate_wheelhouse() in install-deps.sh"
    local install=$1
    shift

    # although pip comes with virtualenv, having a recent version
    # of pip matters when it comes to using wheel packages
    PIP_OPTS="--timeout 300 --exists-action i"
    pip $PIP_OPTS $install \
      'setuptools >= 0.8' 'pip >= 21.0' 'wheel >= 0.24' 'tox >= 2.9.1' || return 1
    if test $# != 0 ; then
        pip $PIP_OPTS $install $@ || return 1
    fi
}

function activate_virtualenv() {
    ci_debug "Running activate_virtualenv() in install-deps.sh"
    local top_srcdir=$1
    local env_dir=$top_srcdir/install-deps-python3

    if ! test -d $env_dir ; then
        python3 -m venv ${env_dir}
        . $env_dir/bin/activate
        if ! populate_wheelhouse install ; then
            rm -rf $env_dir
            return 1
        fi
    fi
    . $env_dir/bin/activate
}

function preload_wheels_for_tox() {
    ci_debug "Running preload_wheels_for_tox() in install-deps.sh"
    local ini=$1
    shift
    pushd . > /dev/null
    cd $(dirname $ini)
    local require_files=$(ls *requirements*.txt 2>/dev/null) || true
    local constraint_files=$(ls *constraints*.txt 2>/dev/null) || true
    local require=$(echo -n "$require_files" | sed -e 's/^/-r /')
    local constraint=$(echo -n "$constraint_files" | sed -e 's/^/-c /')
    local md5=wheelhouse/md5
    if test "$require"; then
        if ! test -f $md5 || ! md5sum -c $md5 > /dev/null; then
            rm -rf wheelhouse
        fi
    fi
    if test "$require" && ! test -d wheelhouse ; then
        type python3 > /dev/null 2>&1 || continue
        activate_virtualenv $top_srcdir || exit 1
        python3 -m pip install --upgrade pip
        populate_wheelhouse "wheel -w $wip_wheelhouse" $require $constraint || exit 1
        mv $wip_wheelhouse wheelhouse
        md5sum $require_files $constraint_files > $md5
    fi
    popd > /dev/null
}

for_make_check=false
if tty -s; then
    # interactive
    for_make_check=true
elif [ $FOR_MAKE_CHECK ]; then
    for_make_check=true
else
    for_make_check=false
fi

if [ x$(uname)x = xFreeBSDx ]; then
    if [ "$INSTALL_EXTRA_PACKAGES" ]; then
        echo "Installing extra packages not supported on FreeBSD" >&2
        exit 1
    fi
    $SUDO pkg install -yq \
        devel/babeltrace \
        devel/binutils \
        devel/git \
        devel/gperf \
        devel/gmake \
        devel/cmake \
        devel/nasm \
        devel/boost-all \
        devel/boost-python-libs \
        devel/valgrind \
        devel/pkgconf \
        devel/libedit \
        devel/libtool \
        devel/google-perftools \
        lang/cython \
        net/openldap24-client \
        archivers/snappy \
        archivers/liblz4 \
        ftp/curl \
        misc/e2fsprogs-libuuid \
        misc/getopt \
        net/socat \
        textproc/expat2 \
        textproc/gsed \
        lang/gawk \
        textproc/libxml2 \
        textproc/xmlstarlet \
        textproc/jq \
        textproc/py-sphinx \
        emulators/fuse \
        java/junit \
        lang/python36 \
        devel/py-pip \
        devel/py-flake8 \
        devel/py-tox \
        devel/py-argparse \
        devel/py-nose \
        devel/py-prettytable \
        devel/py-yaml \
        www/py-routes \
        www/py-flask \
        www/node \
        www/npm \
        www/fcgi \
        security/nss \
        security/krb5 \
        security/oath-toolkit \
        sysutils/flock \
        sysutils/fusefs-libs \

        # Now use pip to install some extra python modules
        pip install pecan

    exit
else
    [ $WITH_SEASTAR ] && with_seastar=true || with_seastar=false
    [ $WITH_PMEM ] && with_pmem=true || with_pmem=false
    source /etc/os-release
    case "$ID" in
    debian|ubuntu|devuan|elementary|softiron)
        echo "Using apt-get to install dependencies"
	# Put this before any other invocation of apt so it can clean
	# up in a broken case.
        clean_boost_on_ubuntu
        if [ "$INSTALL_EXTRA_PACKAGES" ]; then
            if ! $SUDO apt-get install -y $INSTALL_EXTRA_PACKAGES ; then
                # try again. ported over from run-make.sh (orignally e278295)
                # In the case that apt-get is interrupted, like when a jenkins
                # job is cancelled, the package manager will be in an inconsistent
                # state. Run the command again after `dpkg --configure -a` to
                # bring package manager back into a clean state.
                $SUDO dpkg --configure -a
                ci_debug "trying to install $INSTALL_EXTRA_PACKAGES again"
                $SUDO apt-get install -y $INSTALL_EXTRA_PACKAGES
            fi
        fi
        $SUDO apt-get install -y devscripts equivs
        $SUDO apt-get install -y dpkg-dev
        ensure_python3_sphinx_on_ubuntu
        case "$VERSION" in
            *Bionic*)
                ensure_decent_gcc_on_ubuntu 9 bionic
                [ ! $NO_BOOST_PKGS ] && install_boost_on_ubuntu bionic
                ;;
            *Focal*)
                ensure_decent_gcc_on_ubuntu 11 focal
                [ ! $NO_BOOST_PKGS ] && install_boost_on_ubuntu focal
                ;;
            *Jammy*)
                [ ! $NO_BOOST_PKGS ] && install_boost_on_ubuntu jammy
                $SUDO apt-get install -y gcc
                ;;
            *)
                $SUDO apt-get install -y gcc
                ;;
        esac
        if ! test -r debian/control ; then
            echo debian/control is not a readable file
            exit 1
        fi
        touch $DIR/status

        ci_debug "Running munge_debian_control() in install-deps.sh"
        backports=""
        control=$(munge_debian_control "$VERSION" "debian/control")
        case "$VERSION" in
            *squeeze*|*wheezy*)
                backports="-t $codename-backports"
                ;;
        esac

        # make a metapackage that expresses the build dependencies,
        # install it, rm the .deb; then uninstall the package as its
        # work is done
        build_profiles=""
        if $for_make_check; then
            build_profiles+=",pkg.ceph.check"
        fi
        if $with_seastar; then
            build_profiles+=",pkg.ceph.crimson"
        fi
        if $with_pmem; then
            build_profiles+=",pkg.ceph.pmdk"
        fi

        ci_debug "for_make_check=$for_make_check"
        ci_debug "with_seastar=$with_seastar"
        ci_debug "with_jaeger=$with_jaeger"
        ci_debug "build_profiles=$build_profiles"
        ci_debug "Now running 'mk-build-deps' and installing ceph-build-deps package"

        $SUDO env DEBIAN_FRONTEND=noninteractive mk-build-deps \
              --build-profiles "${build_profiles#,}" \
              --install --remove \
              --tool="apt-get -y --no-install-recommends $backports" $control || exit 1
        ci_debug "Removing ceph-build-deps"
        $SUDO env DEBIAN_FRONTEND=noninteractive apt-get -y remove ceph-build-deps
        if [ "$control" != "debian/control" ] ; then rm $control; fi
        ;;
    rocky|centos|fedora|rhel|ol|virtuozzo)
        builddepcmd="dnf -y builddep --allowerasing"
        echo "Using dnf to install dependencies"
        case "$ID" in
            fedora)
                $SUDO dnf install -y dnf-utils
                ;;
            rocky|centos|rhel|ol|virtuozzo)
                MAJOR_VERSION="$(echo $VERSION_ID | cut -d. -f1)"
                $SUDO dnf install -y dnf-utils selinux-policy-targeted
                rpm --quiet --query epel-release || \
                    $SUDO dnf -y install --nogpgcheck https://dl.fedoraproject.org/pub/epel/epel-release-latest-$MAJOR_VERSION.noarch.rpm
                $SUDO rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-$MAJOR_VERSION
                $SUDO rm -f /etc/yum.repos.d/dl.fedoraproject.org*
                if test $ID = centos -a $MAJOR_VERSION = 8 ; then
                    # for grpc-devel
                    # See https://copr.fedorainfracloud.org/coprs/ceph/grpc/
                    # epel is enabled for all major versions couple of lines above
                    $SUDO dnf copr enable -y ceph/grpc

                    # Enable 'powertools' or 'PowerTools' repo
                    $SUDO dnf config-manager --set-enabled $(dnf repolist --all 2>/dev/null|gawk 'tolower($0) ~ /^powertools\s/{print $1}')
                    dts_ver=11
                    # before EPEL8 and PowerTools provide all dependencies, we use sepia for the dependencies
                    $SUDO dnf config-manager --add-repo http://apt-mirror.front.sepia.ceph.com/lab-extras/8/
                    $SUDO dnf config-manager --setopt=apt-mirror.front.sepia.ceph.com_lab-extras_8_.gpgcheck=0 --save
                    $SUDO dnf -y module enable javapackages-tools
                elif test $ID = centos -a $MAJOR_VERSION = 9 ; then
                    $SUDO dnf config-manager --set-enabled crb
                elif test $ID = rhel -a $MAJOR_VERSION = 8 ; then
                    dts_ver=11
                    $SUDO dnf config-manager --set-enabled "codeready-builder-for-rhel-8-${ARCH}-rpms"
                    $SUDO dnf config-manager --add-repo http://apt-mirror.front.sepia.ceph.com/lab-extras/8/
                    $SUDO dnf config-manager --setopt=apt-mirror.front.sepia.ceph.com_lab-extras_8_.gpgcheck=0 --save
                    $SUDO dnf -y module enable javapackages-tools

                    # Enable ceph/grpc from copr for el8, this is needed for nvmeof management.
                    $SUDO dnf copr enable -y ceph/grpc
                fi
                ;;
        esac
        if [ "$INSTALL_EXTRA_PACKAGES" ]; then
            $SUDO dnf install -y $INSTALL_EXTRA_PACKAGES
        fi
        munge_ceph_spec_in $with_seastar $for_make_check $DIR/ceph.spec
        # for python3_pkgversion macro defined by python-srpm-macros, which is required by python3-devel
        $SUDO dnf install -y python3-devel
        $SUDO $builddepcmd $DIR/ceph.spec 2>&1 | tee $DIR/yum-builddep.out
        [ ${PIPESTATUS[0]} -ne 0 ] && exit 1
        if [ -n "$dts_ver" ]; then
            ensure_decent_gcc_on_rh $dts_ver
        fi
        IGNORE_YUM_BUILDEP_ERRORS="ValueError: SELinux policy is not managed or store cannot be accessed."
        sed "/$IGNORE_YUM_BUILDEP_ERRORS/d" $DIR/yum-builddep.out | grep -i "error:" && exit 1
        ;;
    opensuse*|suse|sles)
        echo "Using zypper to install dependencies"
        zypp_install="zypper --gpg-auto-import-keys --non-interactive install --no-recommends"
        $SUDO $zypp_install systemd-rpm-macros rpm-build || exit 1
        if [ "$INSTALL_EXTRA_PACKAGES" ]; then
            $SUDO $zypp_install $INSTALL_EXTRA_PACKAGES
        fi
        munge_ceph_spec_in $with_seastar false $for_make_check $DIR/ceph.spec
        $SUDO $zypp_install $(rpmspec -q --buildrequires $DIR/ceph.spec) || exit 1
        ;;
    *)
        echo "$ID is unknown, dependencies will have to be installed manually."
        exit 1
        ;;
    esac
fi

# use pip cache if possible but do not store it outside of the source
# tree
# see https://pip.pypa.io/en/stable/reference/pip_install.html#caching
if $for_make_check; then
    mkdir -p install-deps-cache
    top_srcdir=$(pwd)
    if [ -n "$XDG_CACHE_HOME" ]; then
        ORIGINAL_XDG_CACHE_HOME=$XDG_CACHE_HOME
    fi
    export XDG_CACHE_HOME=$top_srcdir/install-deps-cache
    wip_wheelhouse=wheelhouse-wip
    #
    # preload python modules so that tox can run without network access
    #
    find . -name tox.ini | while read ini ; do
        preload_wheels_for_tox $ini
    done
    rm -rf $top_srcdir/install-deps-python3
    rm -rf $XDG_CACHE_HOME
    if [ -n "$ORIGINAL_XDG_CACHE_HOME" ]; then
        XDG_CACHE_HOME=$ORIGINAL_XDG_CACHE_HOME
    else
        unset XDG_CACHE_HOME
    fi
    type git > /dev/null || (echo "Dashboard uses git to pull dependencies." ; false)
fi

ci_debug "End install-deps.sh" || true
