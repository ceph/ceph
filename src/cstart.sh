#!/bin/bash -e

if [ -e CMakeCache.txt ]; then
    [ -z "$CEPH_BIN" ] && CEPH_BIN=bin
fi

if [ -z "$CEPHADM" ]; then
    CEPHADM="${CEPH_BIN}/cephadm"
fi

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
echo $shortid > shortid
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
if [ -e port ] ; then
    port=`cat port`
else
    while [ true ]
    do
        port="$(echo $(( RANDOM % 1000 + 40000 )))"
        ss -a -n | grep LISTEN | grep "${ip}:${port} " 2>&1 >/dev/null || break
    done
    echo $port > port
fi
echo "mon port $port"


# make sure we have an image
if ! sudo $runtime image inspect $image_base:$shortid 1>/dev/null 2>/dev/null; then
    echo "building initial $image_base:$shortid image..."
    sudo ../src/script/cpatch -t $image_base:$shortid
fi

sudo $CEPHADM rm-cluster --force --fsid $fsid
sudo $CEPHADM --image ${image_base}:${shortid} bootstrap \
     --skip-pull \
     --fsid $fsid \
     --mon-addrv "[v2:$ip:$port]" \
     --output-dir . \
     --allow-overwrite \
     $@

# kludge to make 'bin/ceph ...' work
sudo chmod 755 ceph.client.admin.keyring
echo 'keyring = ceph.client.admin.keyring' >> ceph.conf

# don't use repo digests; this implicitly does a pull and we don't want that
${CEPH_BIN}/ceph config set mgr mgr/cephadm/use_repo_digest false

echo
echo "sudo ../src/script/cpatch -t $image_base:$shortid"
echo
