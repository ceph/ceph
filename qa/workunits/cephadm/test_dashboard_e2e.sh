#!/bin/bash -ex

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DASHBOARD_FRONTEND_DIR=${SCRIPT_DIR}/../../../src/pybind/mgr/dashboard/frontend

[ -z "$SUDO" ] && SUDO=sudo

install_common () {
    NODEJS_VERSION="16"
    if grep -q  debian /etc/*-release; then
        $SUDO apt-get update
        # https://github.com/nodesource/distributions#manual-installation
        $SUDO apt-get install curl gpg
        KEYRING=/usr/share/keyrings/nodesource.gpg
        curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | gpg --dearmor | $SUDO tee "$KEYRING" >/dev/null
        DISTRO="$(source /etc/lsb-release; echo $DISTRIB_CODENAME)"
        VERSION="node_$NODEJS_VERSION.x"
        echo "deb [signed-by=$KEYRING] https://deb.nodesource.com/$VERSION $DISTRO main" | $SUDO tee /etc/apt/sources.list.d/nodesource.list
        echo "deb-src [signed-by=$KEYRING] https://deb.nodesource.com/$VERSION $DISTRO main" | $SUDO tee -a /etc/apt/sources.list.d/nodesource.list
        $SUDO apt-get update
        $SUDO apt-get install nodejs
    elif grep -q rhel /etc/*-release; then
        $SUDO yum module -y enable nodejs:$NODEJS_VERSION
        $SUDO yum install -y jq npm
    else
        echo "Unsupported distribution."
        exit 1
    fi
}

install_chrome () {
    if grep -q  debian /etc/*-release; then
        $SUDO bash -c 'echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list'
        curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | $SUDO apt-key add -
        $SUDO apt-get update
        $SUDO apt-get install -y google-chrome-stable
        $SUDO apt-get install -y xvfb
        $SUDO rm /etc/apt/sources.list.d/google-chrome.list
    elif grep -q rhel /etc/*-release; then
        $SUDO dd of=/etc/yum.repos.d/google-chrome.repo status=none <<EOF
[google-chrome]
name=google-chrome
baseurl=https://dl.google.com/linux/chrome/rpm/stable/\$basearch
enabled=1
gpgcheck=1
gpgkey=https://dl-ssl.google.com/linux/linux_signing_key.pub
EOF
        $SUDO yum install -y google-chrome-stable
        $SUDO rm /etc/yum.repos.d/google-chrome.repo
        # Cypress dependencies
        $SUDO yum install -y xorg-x11-server-Xvfb gtk2-devel gtk3-devel libnotify-devel GConf2 nss.x86_64 libXScrnSaver alsa-lib
    else
        echo "Unsupported distribution."
        exit 1
    fi
}

cypress_run () {
    local specs="$1"
    local timeout="$2"
    local override_config="excludeSpecPattern=*.po.ts,retries=0,specPattern=${specs}"

    if [ x"$timeout" != "x" ]; then
        override_config="${override_config},defaultCommandTimeout=${timeout}"
    fi
    npx cypress run --browser chrome --headless --config "$override_config"
}

install_common
install_chrome

CYPRESS_BASE_URL=$(ceph mgr services | jq -r .dashboard)
export CYPRESS_BASE_URL

cd $DASHBOARD_FRONTEND_DIR

# This is required for Cypress to understand typescript
npm ci --unsafe-perm
npx cypress verify
npx cypress info

# Take `orch device ls` and `orch ps` as ground truth.
ceph orch device ls --refresh
ceph orch ps --refresh
sleep 10  # the previous call is asynchronous
ceph orch device ls --format=json | tee cypress/fixtures/orchestrator/inventory.json
ceph orch ps --format=json | tee cypress/fixtures/orchestrator/services.json

DASHBOARD_ADMIN_SECRET_FILE="/tmp/dashboard-admin-secret.txt"
printf 'admin' > "${DASHBOARD_ADMIN_SECRET_FILE}"
ceph dashboard ac-user-set-password admin -i "${DASHBOARD_ADMIN_SECRET_FILE}" --force-password

# Run Dashboard e2e tests.
# These tests are designed with execution order in mind, since orchestrator operations
# are likely to change cluster state, we can't just run tests in arbitrarily order.
# See /ceph/src/pybind/mgr/dashboard/frontend/cypress/integration/orchestrator/ folder.
find cypress # List all specs

cypress_run "cypress/e2e/orchestrator/01-hosts.e2e-spec.ts"

# Hosts are removed and added in the previous step. Do a refresh again.
ceph orch device ls --refresh
sleep 10
ceph orch device ls --format=json | tee cypress/fixtures/orchestrator/inventory.json

cypress_run "cypress/e2e/orchestrator/03-inventory.e2e-spec.ts"
cypress_run "cypress/e2e/orchestrator/04-osds.e2e-spec.ts" 300000
