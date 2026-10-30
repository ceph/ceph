#!/usr/bin/env bash

echo -e "[registries.insecure]\n\
registries = ['localhost:5000']" | sudo tee /etc/containers/registries.conf

podman run -d -p 5000:5000 --name my-registry registry:2
# Load the image and capture the output
output=$(podman load -i /root/ceph_image.tar)

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
rm -f /root/ceph_image.tar
