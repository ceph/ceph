#!/bin/bash
set -x

OSDS=1
HOSTS=0
SKIP_LOOP=0
SKIP_BOOTSTRAP=0

function print_usage() {
	echo "./bootstrap.sh [OPTIONS]"
	echo "options:"
	echo "    --hosts n: number of hosts to add"
	echo "    --osds n: number of osds to add"
	echo "    --update-ceph-image: create/update ceph image"
	echo "    --update-box-image: create/update cephadm box image"
	echo "    --skip-create-loop: skip creating loopback device"
	echo "    --skip-bootstrap: skip deploying the containers"
	echo "    -l | --list-hosts: list available cephad-box hosts/seed"
	echo "    -h | --help: this help :)"
}

function docker-ips() {
	docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}} %tab% {{.Name}} %tab% {{.Config.Hostname}}' $(docker ps -aq) | sed 's#%tab%#\t#g' | sed 's#/##g' | sort -t . -k 1,1n -k 2,2n -k 3,3n -k 4,4n
}

while [ $# -ge 1 ]; do
case $1 in
    -h | --help)
	print_usage
	exit
	;;
    -l | --list-hosts) # TODO remove when ceph-ci updated
	echo -e "IP\t\tName\t\t Hostname"
	docker-ips | grep box
	exit
        ;;
    --update-box-image)
	echo Updating box image
	docker build -t cephadm-box -f Dockerfile .
        ;;
    --update-ceph-image) # TODO remove when ceph-ci updated
	echo Updating ceph image
	source ./get_ceph_image.sh
        ;;
    --hosts)
        HOSTS="$2"
	echo "number of hosts: $HOSTS"
	shift
        ;;
    --osds)
        OSDS="$2"
	echo "number of osds: $OSDS"
	shift
        ;;
    --skip-create-loop)
	echo Skiping loop creation
        SKIP_LOOP=1
        ;;
    --skip-bootstrap)
	echo Skiping bootstrap of containers
        SKIP_BOOTSTRAP=1
        ;;
esac
shift
done

# TODO: remove when ceph-ci image has required deps
if [[ ! -a docker/ceph/image/quay.ceph.image.tar ]]
then
	echo -e "\033[33mWARNING:\033[0m run ./get_ceph_image.sh to get an updated ceph-ci/ceph image with correct deps."
	exit
fi

if [[ $OSDS -eq 0 ]]
then
	SKIP_LOOP=1
fi

if [[ $SKIP_LOOP -eq 0 ]]
then
	source setup_loop.sh
	create_loops $OSDS
fi


if [[ $SKIP_BOOTSTRAP -eq 0 ]]
then
	# loops should be created before starting docker-compose or else docker could
	# not find lvs
	docker-compose down
	DCFLAGS="-f docker-compose.yml"
        if [[ -n /sys/fs/cgroup/cgroup.controllers ]]; then
            DCFLAGS+=" -f docker-compose.cgroup1.yml"
        fi

	docker-compose $DCFLAGS up --scale hosts=$HOSTS -d
	sleep 3

	IPS=$(docker-ips | grep "box_hosts" | awk '{ print $1 }')
	echo "IPS: "
	echo $IPS

	sudo sysctl net.ipv4.conf.all.forwarding=1
	sudo iptables -P FORWARD ACCEPT

	for ((i=1;i<=$HOSTS;i++))
	do
		docker-compose exec --index=$i hosts /cephadm/box/setup_ssh.sh run-sshd
	done

	docker-compose exec -e NUM_OSDS=${OSDS} seed /cephadm/box/start

	docker-compose exec -e HOST_IPS="${IPS}" seed /cephadm/box/setup_ssh.sh copy-cluster-ssh-key
fi
