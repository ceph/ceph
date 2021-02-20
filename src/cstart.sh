#!/bin/bash -e

image_base="quay.io/ceph-ci/ceph"

if which podman 2>&1 > /dev/null; then
    runtime="podman"
else
    runtime="docker"
fi

# fsid
if [ -e fsid ] ; then
    fsid=`cat fsid`
else
    fsid=`uuidgen`
    echo $fsid > fsid
fi
echo "fsid $fsid"

shortid=`echo $fsid | cut -c 1-8`
echo "shortid $shortid"

# ip
if [ -z "$ip" ]; then
    if [ -x "$(which ip 2>/dev/null)" ]; then
	IP_CMD="ip addr"
    else
	IP_CMD="ifconfig"
    fi
    # filter out IPv4 and localhost addresses
    ip="$($IP_CMD | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p' | head -n1)"
    # if nothing left, try using localhost address, it might work
    if [ -z "$ip" ]; then ip="127.0.0.1"; fi
fi
echo "ip $ip"

# port
if [ -z "$port" ]; then
    while [ true ]
    do
        port="$(echo $(( RANDOM % 1000 + 40000 )))"
        ss -a -n | grep LISTEN | grep "${ip}:${port} " 1>/dev/null 2>&1 || break
    done
fi
echo "port $port"

# make sure we have an image
if ! $runtime image inspect $image_base:$shortid 2>/dev/null; then
    echo "building initial $image_base:$shortid image..."
    sudo ../src/script/cpatch -t $image_base:$shortid
fi

sudo ../src/cephadm/cephadm rm-cluster --force --fsid $fsid
sudo ../src/cephadm/cephadm --image ${image_base}:${shortid} bootstrap \
     --skip-pull \
     --fsid $fsid \
     --mon-addrv "[v2:$ip:$port]" \
     --output-dir . \
     --allow-overwrite

# kludge to make 'bin/ceph ...' work
sudo chmod 755 ceph.client.admin.keyring
echo 'keyring = ceph.client.admin.keyring' >> ceph.conf

echo
echo "sudo ../src/script/cpatch -t $image_base:$shortid"
echo
