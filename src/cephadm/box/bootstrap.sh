#!/bin/bash

OSDS=1
HOSTS=0
SKIP_LOOP=0

function print_usage() {
	echo "./bootstrap.sh [OPTIONS]"
	echo "options:"
	echo "    --hosts n: number of hosts to add"
	echo "    --osds n: number of osds to add"
	echo "    --update-image: create/update ceph image"
}

function docker-ips() {
	docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}} %tab% {{.Name}}' $(docker ps -aq) | sed 's#%tab%#\t#g' | sed 's#/##g' | sort -t . -k 1,1n -k 2,2n -k 3,3n -k 4,4n
}

while [ $# -ge 1 ]; do
case $1 in
    --help)
	print_usage
	exit
	;;
    --list-hosts) # TODO remove when ceph-ci updated
    docker-ips | grep box
	exit
        ;;
    --update-image) # TODO remove when ceph-ci updated
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
        SKIP_LOOP=1
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

if [[ SKIP_LOOP -eq 0 ]]
then
	source setup_loop.sh
fi

create_loops $OSDS

# loops should be created before starting docker-compose or else docker could
# not find lvs
docker-compose down
docker-compose up --scale hosts=$HOSTS -d
sleep 3

# setup ssh in hosts
docker-compose exec hosts /cephadm/box/setup_ssh.sh
docker-compose exec -e NUM_OSDS=${OSDS} seed /cephadm/box/start
