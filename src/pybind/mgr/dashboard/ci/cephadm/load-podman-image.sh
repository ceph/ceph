#!/usr/bin/env bash

PODMAN_IMG_LOC="/mnt/{{ ceph_dev_folder }}/src/pybind/mgr/dashboard/ci/cephadm/ceph_image.tar"
echo -e "[registries.insecure]\n\
registries = ['localhost:5000']" | sudo tee /etc/containers/registries.conf

podman run -d -p 5000:5000 --name my-registry registry:2
# Load the image and capture the output
output=$(podman load -i "${PODMAN_IMG_LOC}")

# Extract image name from output
image_name=$(echo "$output" | grep -oP '(?<=^Loaded image: ).*')

if [[ -n "$image_name" ]]; then
  echo "Image loaded: $image_name"
  podman tag "$image_name" localhost:5000/ceph
  echo "Tagged image as localhost:5000/ceph"
else
  echo "Failed to load image or extract image name."
  exit 1
fi

podman push localhost:5000/ceph
