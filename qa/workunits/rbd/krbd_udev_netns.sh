#!/usr/bin/env bash

set -ex

sudo ip netns add ns1
sudo ip link add veth1-ext type veth peer name veth1-int
sudo ip link set veth1-int netns ns1

sudo ip netns exec ns1 ip link set dev lo up
sudo ip netns exec ns1 ip addr add 192.168.1.2/24 dev veth1-int
sudo ip netns exec ns1 ip link set veth1-int up
sudo ip netns exec ns1 ip route add default via 192.168.1.1

sudo ip addr add 192.168.1.1/24 dev veth1-ext
sudo ip link set veth1-ext up

# Enable forwarding between the namespace and the default route
# interface and set up NAT.  In case of multiple default routes,
# just pick the first one.
if [[ $(sysctl -n net.ipv4.ip_forward) -eq 0 ]]; then
    sudo iptables -P FORWARD DROP
    sudo sysctl -w net.ipv4.ip_forward=1
fi
IFACE="$(ip route list 0.0.0.0/0 | head -n 1 | cut -d ' ' -f 5)"
sudo iptables -A FORWARD -i veth1-ext -o "$IFACE" -j ACCEPT
sudo iptables -A FORWARD -i "$IFACE" -o veth1-ext -j ACCEPT
sudo iptables -t nat -A POSTROUTING -s 192.168.1.2 -o "$IFACE" -j MASQUERADE

rbd create --size 300 img

DEV="$(sudo rbd map img)"
mkfs.ext4 "$DEV"
sudo mount "$DEV" /mnt
sudo umount /mnt
sudo rbd unmap "$DEV"

sudo ip netns exec ns1 bash <<'EOF'

set -ex

DEV="/dev/rbd/rbd/img"
[[ ! -e "$DEV" ]]

# In a network namespace, "rbd map" maps the device and hangs waiting
# for udev add uevents.  udev runs as usual (in particular creating the
# symlink which is used here because the device node is never printed),
# but the uevents it sends out never come because they don't cross
# network namespace boundaries.
set +e
timeout 30s rbd map img
RET=$?
set -e
[[ $RET -eq 124 ]]
[[ -L "$DEV" ]]
mkfs.ext4 -F "$DEV"
mount "$DEV" /mnt
umount /mnt

# In a network namespace, "rbd unmap" unmaps the device and hangs
# waiting for udev remove uevents.  udev runs as usual (removing the
# symlink), but the uevents it sends out never come because they don't
# cross network namespace boundaries.
set +e
timeout 30s rbd unmap "$DEV"
RET=$?
set -e
[[ $RET -eq 124 ]]
[[ ! -e "$DEV" ]]

# Skip waiting for udev uevents with "-o noudev".
DEV="$(rbd map -o noudev img)"
mkfs.ext4 -F "$DEV"
mount "$DEV" /mnt
umount /mnt
rbd unmap -o noudev "$DEV"

EOF

rbd rm img

sudo iptables -t nat -D POSTROUTING -s 192.168.1.2 -o "$IFACE" -j MASQUERADE
sudo iptables -D FORWARD -i "$IFACE" -o veth1-ext -j ACCEPT
sudo iptables -D FORWARD -i veth1-ext -o "$IFACE" -j ACCEPT
sudo ip netns delete ns1

echo OK
