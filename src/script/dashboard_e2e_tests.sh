#!/bin/bash
#
# dashboard_e2e_tests.sh
#
# Run the Ceph Dashboard E2E tests against a real Ceph cluster
#
# CAVEAT: do *not* run this script as root, but *do* run it as a user with
# passwordless sudo privilege.
#
# TODO: in its current form, this script assumes the Ceph cluster is
# "pristine". More work is needed to achieve idempotence.

if [ "$EUID" = "0" ] ; then
    echo "$0: detected attempt to run script as root - bailing out!"
    exit 1
fi

if [[ ! $(arch) =~ (x86_64|amd64) ]]; then
    echo "$0: this script uses Google Chrome, which is only available on 64-bit x86 architectures - bailing out!"
    exit 1
fi

set -ex

function _install_google_chrome_deb {
    sudo bash -c 'echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list'
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
    sudo apt-get update
    sudo apt-get install -y google-chrome-stable
    sudo rm /etc/apt/sources.list.d/google-chrome.list
}

function _install_google_chrome_rh {
    sudo dd of=/etc/yum.repos.d/google-chrome.repo status=none <<EOF
[google-chrome]
name=google-chrome
baseurl=https://dl.google.com/linux/chrome/rpm/stable/\$basearch
enabled=1
gpgcheck=1
gpgkey=https://dl-ssl.google.com/linux/linux_signing_key.pub
EOF
    sudo yum install -y google-chrome-stable
    sudo rm /etc/yum.repos.d/google-chrome.repo
}

function _install_google_chrome_suse {
    sudo zypper --non-interactive addrepo --refresh --no-gpgcheck \
        http://dl.google.com/linux/chrome/rpm/stable/x86_64 Google-Chrome
    sudo zypper --non-interactive --no-gpg-checks install --force --no-recommends \
        google-chrome-stable
}

function install_google_chrome {
    source /etc/os-release
    case "$ID" in
        debian|ubuntu)
            _install_google_chrome_deb
            ;;
        fedora|rhel|centos)
            _install_google_chrome_rh
            ;;
        opensuse*|suse|sles)
            _install_google_chrome_suse
            ;;
        *)
            echo "$0: unsupported distro $ID - bailing out!"
            exit 1
    esac
}

# get URL of the running Dashboard
URL=$(sudo ceph mgr services 2>/dev/null | jq -r .dashboard)
if [ -z "$URL" ]; then
  echo "ERROR: dashboard is not available" >/dev/null
  false
fi
if [[ $URL =~ ^http ]] ; then
    echo "$URL looks like a URL" >/dev/null
else
    echo "$URL does not look like a URL" >/dev/null
    false
fi

# set dashboard admin password
sudo ceph dashboard ac-user-set-password admin admin

# setup RGW for E2E
sudo radosgw-admin user create --uid=dev --display-name=Developer --system
sudo ceph dashboard set-rgw-api-user-id dev
sudo ceph dashboard set-rgw-api-access-key \
    $(sudo radosgw-admin user info --uid=dev | jq -r .keys[0].access_key)
sudo ceph dashboard set-rgw-api-secret-key \
    $(sudo radosgw-admin user info --uid=dev | jq -r .keys[0].secret_key)
sudo ceph dashboard set-rgw-api-ssl-verify False

# install Google Chrome (needed for E2E)
if ! type google-chrome-stable >/dev/null 2>&1 ; then
    install_google_chrome
fi

# Since the ceph-dashboard-e2e RPM installs this script and all the other
# Dashboard E2E files under /usr/lib, yet E2E itself cannot be run as root, we
# have a problem because the process of setting everything up for running E2E
# requires creating files and directories. We solve this by copying everything
# to a temporary directory.
BASEDIR=$(readlink -f "$(dirname ${0})")
TMPDIR=$(mktemp -d)
cp -a $BASEDIR/* $TMPDIR
cd $TMPDIR

# point Protractor at the running Dashboard
sed -i -e "s#http://localhost:4200/#$URL#" protractor.conf.js

# install nodeenv to get "npm" command
if ! type npm >/dev/null 2>&1 ; then
    virtualenv venv
    source venv/bin/activate
    pip install nodeenv
    nodeenv -p --node=10.13.0
fi

# install all Dashboard dependencies
timeout -v 3h npm ci

# run E2E, telling it to not start a dev server but instead use the one
# specified in protractor.conf.js
timeout -v 3h npx ng e2e --devServerTarget=''

# cleanup
rm -rf $TMPDIR
