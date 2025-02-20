#!/usr/bin/env bash

source bootstrap-cluster.sh > /dev/null 2>&1

set +x

show_help() {
  echo "Usage: ./quick-bootstrap.sh [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  -u, --use-cached-image     Uses the existing podman image in local. Only use this if there is such an image present."
  echo "  -dir, --ceph-dir             Use this to provide the local ceph directory. eg. --ceph-dir=/path/to/ceph"
  echo "  -e, --expanded-cluster     To add all the hosts and deploy OSDs on top of it."
  echo "  -c, --clusters          Number of clusters to be created. Default is 1."
  echo "  -n, --nodes           Number of nodes to be created per cluster. Default is 3."
  echo "  -h, --help             Display this help message."
  echo ""
  echo "Example:"
  echo "  ./quick-bootstrap.sh --use-cached-image"
}

GREEN='\033[0;32m'
RESET='\033[0m'

use_cached_image=false
extra_args="-P quick_install=True"
CLUSTERS=1
NODES=3

for arg in "$@"; do
  case "$arg" in
    -u|--use-cached-image)
      use_cached_image=true
      ;;
    -dir=*|--ceph-dir=*)
      extra_args+=" -P ceph_dev_folder=${arg#*=}"
      ;;
    -e|--expanded-cluster)
      extra_args+=" -P expanded_cluster=True"
      ;;
    -n=*|--nodes=*)
      NODES="${arg#*=}"
      ;;
    -c=*|--clusters=*)
      CLUSTERS="${arg#*=}"
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo "Unknown option: $arg"
      show_help
      exit 1
      ;;
  esac
done

image_name=$(echo "$CEPHADM_IMAGE")

extra_args+=" -P nodes=${NODES}"

if [[ ${use_cached_image} == false ]]; then
    printf "Pulling the image: %s\n" "$image_name"
    podman pull "${image_name}"
fi

rm -f ceph_image.tar

printf "Saving the image: %s\n" "$image_name"
podman save -o ceph_image.tar quay.ceph.io/ceph-ci/ceph:main

NODE_IP_OFFSET=100
PREFIX="ceph"
for cluster in $(seq 1 $CLUSTERS); do
    if [[ $cluster -gt 1 ]]; then
        PREFIX="ceph${cluster}"
    fi
    printf "\nCreating cluster: %s\n" "${PREFIX}"
    kcli create plan -f ceph_cluster.yml ${extra_args} -P node_ip_offset=${NODE_IP_OFFSET} \
      -P prefix=${PREFIX} ${PREFIX}
    NODE_IP_OFFSET=$((NODE_IP_OFFSET + 10))
done

attempt=0

MAX_ATTEMPTS=10
SLEEP_INTERVAL=5

NODE_IP_OFFSET=100
PREFIX="ceph"
for cluster in $(seq 1 $CLUSTERS); do
  if [[ $cluster -gt 1 ]]; then
      PREFIX="ceph${cluster}"
  fi
  printf "\nWaiting for the host to be reachable on cluster ${PREFIX}...\n"

  while [[ ${attempt} -lt ${MAX_ATTEMPTS} ]]; do
      if ssh -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=10 root@192.168.100."${NODE_IP_OFFSET}" exit; then
          printf "\n${GREEN}Host is reachable on cluster %s\n${RESET}" "${PREFIX}"
          break
      else
          echo "Waiting for ssh connection to be available..., attempt: ${attempt}"
          ((attempt++))
          sleep ${SLEEP_INTERVAL}
      fi
  done

  NODE_IP_OFFSET=$((NODE_IP_OFFSET + 10))
done

printf "\nOpening logs for first cluster"
kcli ssh -u root -- ceph-node-00 'journalctl -n all -ft cloud-init'
