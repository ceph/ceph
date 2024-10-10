#!/usr/bin/env bash

source bootstrap-cluster.sh > /dev/null 2>&1

set +x

show_help() {
  echo "Usage: ./quick-install.sh [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  --use-cached-image     Uses the existing podman image in local. Only use this if there is such an image present."
  echo "  -h, --help             Display this help message."
  echo ""
  echo "Example:"
  echo "  ./quick-install.sh --use-cached-image"
}

use_cached_image=false

for arg in "$@"; do
  case "$arg" in
    --use-cached-image)
      use_cached_image=true
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

image_name=$(echo "$CEPHADM_IMAGE" | awk -F'/' '{print $NF}')
ceph_cluster_yml='ceph_cluster.yml'
node_count=$(awk '/nodes:/ {print $2}' "${ceph_cluster_yml}")

if [[ ${use_cached_image} == false ]]; then
    printf "Pulling the image: %s\n" "$image_name"
    podman pull "${image_name}"
fi

rm -f ceph_image.tar

printf "Saving the image: %s\n" "$image_name"
podman save -o ceph_image.tar quay.ceph.io/ceph-ci/ceph:main

printf "Creating the plan\n"
kcli create plan -f ceph_cluster.yml -P ceph_dev_folder=/home/nia/projects/ceph -P quick_install=True -P expanded_cluster=True ceph

attempt=0

MAX_ATTEMPTS=10
SLEEP_INTERVAL=5

printf "Waiting for the host to be reachable\n"
while [[ ${attempt} -lt ${MAX_ATTEMPTS} ]]; do
    if ssh -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=10 root@192.168.100.100 exit; then
        break
    else
        echo "Waiting for ssh connection to be available..., attempt: ${attempt}"
        ((attempt++))
        sleep ${SLEEP_INTERVAL}
    fi
done

printf "Copying the image to the hosts\n"

for node in $(seq 0 $((node_count - 1))); do
    scp -o StrictHostKeyChecking=no ceph_image.tar root@192.168.100.10"${node}":/root/
done

rm -f ceph_image.tar
kcli ssh -u root -- ceph-node-00 'journalctl -n all -ft cloud-init'
