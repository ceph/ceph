#!/bin/bash
#
# Copyright (c) 2015 Red Hat, Inc.
#
# Author: Loic Dachary <loic@dachary.org>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

#
# Most of this file is intended to be obsoleted by the ansible equivalent
# when they are available (setting up paddles, pulpito, etc.).
#
function create_config() {
    local network="$1"
    local subnet="$2"
    local nameserver="$3"
    local labdomain="$4"
    local ip="$5"
    local flavor_select="$6"
    local archive_upload="$7"

    if test "$flavor_select" ; then
        flavor_select="flavor-select-regexp: $flavor_select"
    fi

    if test "$network" ; then
        network="network: $network"
    fi

    if test "$archive_upload" ; then
        archive_upload="archive_upload: $archive_upload"
    fi

    cat > ~/.teuthology.yaml <<EOF
$archive_upload
archive_upload_key: teuthology/openstack/archive-key
lock_server: http://localhost:8080/
results_server: http://localhost:8080/
gitbuilder_host: gitbuilder.ceph.com
check_package_signatures: false
ceph_git_url: https://github.com/ceph/ceph.git
ceph_qa_suite_git_url: https://github.com/ceph/ceph-qa-suite.git
queue_port: 11300
suite_verify_ceph_hash: false
queue_host: localhost
lab_domain: $labdomain
max_job_time: 32400 # 9 hours
teuthology_path: .
openstack:
  clone: git clone http://github.com/ceph/teuthology
  user-data: teuthology/openstack/openstack-{os_type}-{os_version}-user-data.txt
  ip: $ip
  nameserver: $nameserver
  #
  # OpenStack has predefined machine sizes (called flavors)
  # For a given job requiring N machines, the following will select
  # the smallest flavor that satisfies these requirements. For instance
  # If there are three flavors
  #
  #   F1 (10GB disk, 2000MB RAM, 1CPU)
  #   F2 (100GB disk, 7000MB RAM, 1CPU)
  #   F3 (50GB disk, 7000MB RAM, 1CPU)
  #
  # and machine: { disk: 40, ram: 7000, cpus: 1 }, F3 will be chosen.
  # F1 does not have enough RAM (2000 instead of the 7000 minimum) and
  # although F2 satisfies all the requirements, it is larger than F3
  # (100GB instead of 50GB) and presumably more expensive.
  #
  machine:
    disk: 20 # GB
    ram: 8000 # MB
    cpus: 1
  volumes:
    count: 0
    size: 1 # GB
  $flavor_select
  subnet: $subnet
  $network
EOF
    echo "OVERRIDE ~/.teuthology.yaml"
    echo 'no password' > ~/.vault_pass.txt
    echo "OVERRIDE ~/.vault_pass.txt"
    return 0
}

function teardown_paddles() {
    if pkill -f 'pecan' ; then
        echo "SHUTDOWN the paddles server"
    fi
}

function setup_paddles() {
    local ip=$1

    local public_ip=$(curl --silent http://169.254.169.254/2009-04-04/meta-data/public-ipv4/)
    if test -z "$public_ip" ; then
        public_ip=$ip
    fi

    local paddles_dir=$(dirname $0)/../../../paddles

    if ! test -d $paddles_dir ; then
        git clone https://github.com/ceph/paddles.git $paddles_dir || return 1
    fi

    sudo apt-get -qq install -y beanstalkd postgresql postgresql-contrib postgresql-server-dev-all supervisor

    if ! sudo /etc/init.d/postgresql status ; then
        sudo mkdir -p /etc/postgresql
        sudo chown postgres /etc/postgresql
        sudo -u postgres pg_createcluster 9.3 paddles
        sudo /etc/init.d/postgresql start || return 1
    fi
    if ! psql --command 'select 1' 'postgresql://paddles:paddles@localhost/paddles' > /dev/null 2>&1 ; then
        sudo -u postgres psql -c "CREATE USER paddles with PASSWORD 'paddles';" || return 1
        sudo -u postgres createdb -O paddles paddles || return 1
    fi
    (
        cd $paddles_dir || return 1
        git pull --rebase
        git clean -ffqdx
        sed -e "s|^address.*|address = 'http://localhost'|" \
            -e "s|^job_log_href_templ = 'http://qa-proxy.ceph.com/teuthology|job_log_href_templ = 'http://$public_ip|" \
            -e "/sqlite/d" \
            -e "s|.*'postgresql+psycop.*'|'url': 'postgresql://paddles:paddles@localhost/paddles'|" \
            -e "s/'host': '127.0.0.1'/'host': '0.0.0.0'/" \
            < config.py.in > config.py
        virtualenv ./virtualenv
        source ./virtualenv/bin/activate
        pip install -r requirements.txt
        pip install sqlalchemy tzlocal requests netaddr
        python setup.py develop
    )

    echo "CONFIGURED the paddles server"
}

function populate_paddles() {
    local subnet=$1
    local labdomain=$2

    local paddles_dir=$(dirname $0)/../../../paddles

    local url='postgresql://paddles:paddles@localhost/paddles'

    pkill -f 'pecan serve'

    sudo -u postgres dropdb paddles
    sudo -u postgres createdb -O paddles paddles

    (
        cd $paddles_dir || return 1
        source virtualenv/bin/activate
        pecan populate config.py

        (
            echo "begin transaction;"
            subnet_names_and_ips $subnet | while read name ip ; do
                echo "insert into nodes (name,machine_type,is_vm,locked,up) values ('${name}.${labdomain}', 'openstack', TRUE, FALSE, TRUE);"
            done
            echo "commit transaction;"
        ) | psql --quiet $url

        setsid pecan serve config.py < /dev/null > /dev/null 2>&1 &
        for i in $(seq 1 20) ; do
            if curl --silent http://localhost:8080/ > /dev/null 2>&1 ; then
                break
            else
                echo -n .
                sleep 5
            fi
        done
        echo -n ' '
    )

    echo "RESET the paddles server"
}

function teardown_pulpito() {
    if pkill -f 'python run.py' ; then
        echo "SHUTDOWN the pulpito server"
    fi
}

function setup_pulpito() {
    local pulpito=http://localhost:8081/

    local pulpito_dir=$(dirname $0)/../../../pulpito

    if curl --silent $pulpito | grep -q pulpito  ; then
        echo "OK pulpito is running"
        return 0
    fi

    if ! test -d $pulpito_dir ; then
        git clone https://github.com/ceph/pulpito.git $pulpito_dir || return 1
    fi

    sudo apt-get -qq install -y nginx
    local nginx_conf=/etc/nginx/sites-available/default
    if ! grep -qq 'autoindex on' $nginx_conf ; then
        sudo perl -pi -e 's|location / {|location / { autoindex on;|' $nginx_conf
        sudo /etc/init.d/nginx restart
        echo "ADDED autoindex on to nginx configuration"
    fi
    sudo chown $USER /usr/share/nginx/html
    (
        cd $pulpito_dir || return 1
        git pull --rebase
        git clean -ffqdx
        sed -e "s|paddles_address.*|paddles_address = 'http://localhost:8080'|" < config.py.in > prod.py
        virtualenv ./virtualenv
        source ./virtualenv/bin/activate
        pip install -r requirements.txt
        python run.py &
    )

    echo "LAUNCHED the pulpito server"
}

function setup_bashrc() {
    if test -f ~/.bashrc && grep -qq '.bashrc_teuthology' ~/.bashrc ; then
        echo "OK .bashrc_teuthology found in ~/.bashrc"
    else
        cat > ~/.bashrc_teuthology <<'EOF'
source $HOME/openrc.sh
source $HOME/teuthology/virtualenv/bin/activate
export HISTSIZE=500000
export PROMPT_COMMAND='history -a'
EOF
        echo 'source $HOME/.bashrc_teuthology' >> ~/.bashrc
        echo "ADDED .bashrc_teuthology to ~/.bashrc"
    fi
}

function setup_ssh_config() {
    if test -f ~/.ssh/config && grep -qq 'StrictHostKeyChecking no' ~/.ssh/config ; then
        echo "OK ~/.ssh/config"
    else
        cat >> ~/.ssh/config <<EOF
Host *
  StrictHostKeyChecking no
  UserKnownHostsFile=/dev/null
EOF
        echo "APPEND to ~/.ssh/config"
    fi
}

function setup_authorized_keys() {
    cat teuthology/openstack/archive-key.pub >> ~/.ssh/authorized_keys
    chmod 600 teuthology/openstack/archive-key
    echo "APPEND to ~/.ssh/authorized_keys"
}

function setup_bootscript() {
    local nworkers=$1

    local where=$(dirname $0)

    sudo cp -a $where/openstack-teuthology.init /etc/init.d/teuthology
    echo NWORKERS=$1 | sudo tee /etc/default/teuthology > /dev/null
    echo "CREATED init script /etc/init.d/teuthology"
}

function setup_crontab() {
    local where=$(dirname $0)
    crontab $where/openstack-teuthology.cron
}

function remove_crontab() {
    crontab -r
}

function get_or_create_keypair() {
    local keypair=$1

    (
        cd $HOME/.ssh
        if ! test -f $keypair.pem ; then
            openstack keypair delete $keypair || true
            openstack keypair create $keypair > $keypair.pem || return 1
            chmod 600 $keypair.pem
        fi
        if ! test -f $keypair.pub ; then
            if ! ssh-keygen -y -f $keypair.pem > $keypair.pub ; then
               cat $keypair.pub
               return 1
            fi
        fi
        if ! openstack keypair show $keypair > $keypair.keypair 2>&1 ; then
            openstack keypair create --public-key $keypair.pub $keypair || return 1 # noqa
        else
            fingerprint=$(ssh-keygen -l -f $keypair.pub | cut -d' ' -f2)
            if ! grep --quiet $fingerprint $keypair.keypair ; then
                openstack keypair delete $keypair || return 1
                openstack keypair create --public-key $keypair.pub $keypair || return 1 # noqa
            fi
        fi
        ln -f $keypair.pem id_rsa
        cat $keypair.pub >> authorized_keys
    )
}

function delete_keypair() {
    local keypair=$1

    if openstack keypair show $keypair > /dev/null 2>&1 ; then
        openstack keypair delete $keypair || return 1
        echo "REMOVED keypair $keypair"
    fi
}

function setup_dnsmasq() {

    if ! test -f /etc/dnsmasq.d/resolv ; then
        resolver=$(grep nameserver /etc/resolv.conf | head -1 | perl -ne 'print $1 if(/\s*nameserver\s+([\d\.]+)/)')
        sudo apt-get -qq install -y dnsmasq resolvconf
        echo resolv-file=/etc/dnsmasq-resolv.conf | sudo tee /etc/dnsmasq.d/resolv
        echo nameserver $resolver | sudo tee /etc/dnsmasq-resolv.conf
        # restart is not always picking up changes
        sudo /etc/init.d/dnsmasq stop || true
        sudo /etc/init.d/dnsmasq start
        sudo sed -ie 's/^#IGNORE_RESOLVCONF=yes/IGNORE_RESOLVCONF=yes/' /etc/default/dnsmasq
        echo nameserver 127.0.0.1 | sudo tee /etc/resolvconf/resolv.conf.d/head
        sudo resolvconf -u
        echo "INSTALLED dnsmasq and configured to be a resolver"
    else
        echo "OK dnsmasq installed"
    fi
}

function subnet_names_and_ips() {
    local subnet=$1
    python -c 'import netaddr; print "\n".join([str(i) for i in netaddr.IPNetwork("'$subnet'")])' |
    sed -e 's/\./ /g' | while read a b c d ; do
        printf "target%03d%03d " $c $d
        echo $a.$b.$c.$d
    done
}

function define_dnsmasq() {
    local subnet=$1
    local labdomain=$2
    local host_records=/etc/dnsmasq.d/teuthology
    if ! test -f $host_records ; then
        subnet_names_and_ips $subnet | while read name ip ; do
            echo host-record=$name.$labdomain,$ip
        done | sudo tee $host_records > /tmp/dnsmasq
        head -2 /tmp/dnsmasq
        echo 'etc.'
        # restart is not always picking up changes
        sudo /etc/init.d/dnsmasq stop || true
        sudo /etc/init.d/dnsmasq start
        echo "CREATED $host_records"
    else
        echo "OK $host_records exists"
    fi
}

function undefine_dnsmasq() {
    local host_records=/etc/dnsmasq.d/teuthology

    sudo rm -f $host_records
    echo "REMOVED $host_records"
}

function setup_ansible() {
    local subnet=$1
    local labdomain=$2
    local dir=/etc/ansible/hosts
    if ! test -f $dir/teuthology ; then
        sudo mkdir -p $dir/group_vars
        echo '[testnodes]' | sudo tee $dir/teuthology
        subnet_names_and_ips $subnet | while read name ip ; do
            echo $name.$labdomain
        done | sudo tee -a $dir/teuthology > /tmp/ansible
        head -2 /tmp/ansible
        echo 'etc.'
        echo 'modify_fstab: false' | sudo tee $dir/group_vars/all.yml
        echo "CREATED $dir/teuthology"
    else
        echo "OK $dir/teuthology exists"
    fi
}

function teardown_ansible() {
    sudo rm -fr /etc/ansible/hosts/teuthology
}

function remove_images() {
    glance image-list --property-filter ownedby=teuthology | grep -v -e ---- -e 'Disk Format' | cut -f4 -d ' ' | while read image ; do
        echo "DELETED iamge $image"
        glance image-delete $image
    done
}

function install_packages() {

    if ! test -f /etc/apt/sources.list.d/trusty-backports.list ; then
        echo deb http://archive.ubuntu.com/ubuntu trusty-backports main universe | sudo tee /etc/apt/sources.list.d/trusty-backports.list
        sudo apt-get update
    fi

    local packages="jq realpath"
    sudo apt-get -qq install -y $packages

    echo "INSTALL required packages $packages"
}

CAT=${CAT:-cat}

function verify_openstack() {
    if ! openstack server list > /dev/null ; then
        echo ERROR: the credentials from ~/openrc.sh are not working >&2
        return 1
    fi
    echo "OK $OS_TENANT_NAME can use $OS_AUTH_URL" >&2
    local provider
    if echo $OS_AUTH_URL | grep -qq cloud.ovh.net ; then
        provider=ovh
    elif echo $OS_AUTH_URL | grep -qq entercloudsuite.com ; then
        provider=entercloudsuite
    else
        provider=standardopenstack
    fi
    echo "OPENSTACK PROVIDER $provider" >&2
    echo $provider
}

function main() {
    local network
    local subnet
    local nameserver
    local labdomain=teuthology
    local nworkers=2
    local flavor_select
    local keypair=teuthology
    local archive_upload

    local do_setup_keypair=false
    local do_create_config=false
    local do_setup_dnsmasq=false
    local do_install_packages=false
    local do_setup_paddles=false
    local do_populate_paddles=false
    local do_setup_pulpito=false
    local do_clobber=false

    export LC_ALL=C

    while [ $# -ge 1 ]; do
        case $1 in
            --verbose)
                set -x
                PS4='${FUNCNAME[0]}: $LINENO: '
                ;;
            --nameserver)
                shift
                nameserver=$1
                ;;
            --subnet)
                shift
                subnet=$1
                ;;
            --labdomain)
                shift
                labdomain=$1
                ;;
            --nworkers)
                shift
                nworkers=$1
                ;;
            --archive-upload)
                shift
                archive_upload=$1
                ;;
            --install)
                do_install_packages=true
                ;;
            --config)
                do_create_config=true
                ;;
            --setup-keypair)
                do_setup_keypair=true
                ;;
            --setup-dnsmasq)
                do_setup_dnsmasq=true
                ;;
            --setup-paddles)
                do_setup_paddles=true
                ;;
            --setup-pulpito)
                do_setup_pulpito=true
                ;;
            --populate-paddles)
                do_populate_paddles=true
                ;;
            --setup-all)
                do_install_packages=true
                do_create_config=true
                do_setup_keypair=true
                do_setup_dnsmasq=true
                do_setup_paddles=true
                do_setup_pulpito=true
                do_populate_paddles=true
                ;;
            --clobber)
                do_clobber=true
                ;;
            *)
                echo $1 is not a known option
                return 1
                ;;
        esac
        shift
    done

    if $do_install_packages ; then
        install_packages || return 1
    fi

    local provider=$(verify_openstack)

    eval local default_subnet=$(neutron subnet-list -f json -c cidr -c ip_version | jq '.[] | select(.ip_version == 4) | .cidr')
    if test -z "$default_subnet" ; then
        default_subnet=$(nova tenant-network-list | grep / | cut -f6 -d' ' | head -1)
    fi
    : ${subnet:=$default_subnet}

    case $provider in
        entercloudsuite)
            eval local network=$(neutron net-list -f json | jq '.[] | select(.subnets | contains("'$subnet'")) | .name')
            ;;
    esac

    case $provider in
        ovh)
            flavor_select='^(vps|eg)-'
            ;;
    esac

    local ip=$(ip a show dev eth0 | sed -n "s:.*inet \(.*\)/.*:\1:p")
    : ${nameserver:=$ip}

    if $do_create_config ; then
        create_config "$network" "$subnet" "$nameserver" "$labdomain" "$ip" "$flavor_select" "$archive_upload" || return 1
        setup_ansible $subnet $labdomain || return 1
        setup_ssh_config || return 1
        setup_authorized_keys || return 1
        setup_bashrc || return 1
        setup_bootscript $nworkers || return 1
        setup_crontab || return 1
    fi

    if $do_setup_keypair ; then
        get_or_create_keypair $keypair || return 1
    fi

    if $do_setup_dnsmasq ; then
        setup_dnsmasq || return 1
        define_dnsmasq $subnet $labdomain || return 1
    fi

    if $do_setup_paddles ; then
        setup_paddles $ip || return 1
    fi

    if $do_populate_paddles ; then
        populate_paddles $subnet $labdomain || return 1
    fi

    if $do_setup_pulpito ; then
        setup_pulpito || return 1
    fi

    if $do_clobber ; then
        undefine_dnsmasq || return 1
        delete_keypair $keypair || return 1
        teardown_paddles || return 1
        teardown_pulpito || return 1
        teardown_ansible || return 1
        remove_images || return 1
        remove_crontab || return 1
    fi
}

main "$@"
