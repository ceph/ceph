#!/bin/bash -norc

# Magic startup script for a UML instance.  As long as unique
# instances are started, more than one of them can be concurrently
# in use on a single system.  All their network interfaces are
# bridged together onto the virtual bridge "virbr0" which is
# supplied by the "libvirt" package.
#
# Note that a DHCP server is started for that interface.  It's
# configured in this file:
#	/etc/libvirt/qemu/networks/default.xml
# Unfortunately what I see there serves all possible DHCP addresses,
# so stealing them like we do here isn't really kosher.  To fix
# it, that configuration should change to serve a smaller subset
# of the available address range.
#
# Each instance uses its own tun/tap device, created using the
# "tunctl" command.  The assigned tap device will correspond with
# the guest id (a small integer representing the instance), i.e.,
# guest id 1 uses tap1, etc.  The tap device is attached to the
# virtual bridge, which will have its own subnet associated with it.
# The guest side of that interface will have the same subnet as the
# bridge interface, with the bottom bits representing (normally) 100
# more than the guest id.  So for subnet 192.168.122.0/24, guest
# id 1 will use ip 192.168.122.101, guest id 2 will use ip
# 192.168.122.102, and so on.  Because these interfaces are bridged,
# they can all communicate with each other.

# You will want to override this by setting and exporting the
# "CEPH_TOP" environment variable to be the directory that contains
# the "ceph-client" source tree.
CEPH_TOP="${CEPH_TOP:-/home/elder/ceph}"

# You may want to change this too, if you want guest UML instances
# to have a diffeerent IP address range.  The guest IP will be based
# on this plus GUEST_ID (defined below).
GUEST_IP_OFFSET="${GUEST_IP_OFFSET:-100}"

#############################

if [ $# -gt 1 ]; then
	echo "" >&2
	echo "Usage: $(basename $0) [guest_id]" >&2
	echo "" >&2
	echo "    guest_id is a small integer (default 1)" >&2
	echo "    (each UML instance needs a distinct guest_id)" >&2
	echo "" >&2
	exit 1
elif [ $# -eq 1 ]; then
	GUEST_ID="$1"
else
	GUEST_ID=1
fi

# This will be what the guest host calls itself.
GUEST_HOSTNAME="uml-${GUEST_ID}"

# This is the path to the boot disk image used by UML.
DISK_IMAGE_A="${CEPH_TOP}/ceph-client/uml.${GUEST_ID}"
if [ ! -f "${DISK_IMAGE_A}" ]; then
	echo "root disk image not found (or not a file)" >&2
	exit 2
fi

# Hostid 1 uses tun/tap device tap1, hostid 2 uses tap2, etc.
TAP_ID="${GUEST_ID}"
# This is the tap device used for this UML instance
TAP="tap${TAP_ID}"

# This is just used to mount an image temporarily
TMP_MNT="/tmp/m$$"

# Where to put a config file generated for this tap device
TAP_IFUPDOWN_CONFIG="/tmp/interface-${TAP}"

# Compute the HOST_IP and BROADCAST address values to use,
# and assign shell variables with those names to their values.
# Also compute BITS, which is the network prefix length used.
# The NETMASK is then computed using that BITS value.
eval $(
ip addr show virbr0 | awk '
/inet/ {
	split($2, a, "/")
	printf("HOST_IP=%s\n", a[1]);
	printf("BROADCAST=%s\n", $4);
	printf("BITS=%s\n", a[2]);
	exit(0);
}')

# Use bc to avoid 32-bit wrap when computing netmask
eval $(
echo -n "NETMASK="
bc <<! | fmt | sed 's/ /./g'
m = 2 ^ 32 - 2 ^ (32 - ${BITS})
for (p = 24; p >= 0; p = p - 8)
    m / (2 ^ p) % 256
!
)

# Now use the netmask and the host IP to compute the subnet address
# and from that the guest IP address to use.
eval $(
awk '
function from_quad(addr,  a, val, i) {
	if (split(addr, a, ".") != 4)
		exit(1);	# address not in dotted quad format
	val = 0;
	for (i = 1; i <= 4; i++)
		val = val * 256 + a[i];
	return val;
}
function to_quad(val,  addr, i) {
	addr = "";
	for (i = 1; i <= 4; i++) {
		addr = sprintf("%u%s%s", val % 256, i > 1 ? "." : "", addr);
		val = int(val / 256);
	}
	if ((val + 0) != 0)
		exit(1);	# value provided exceeded 32 bits
	return addr;
}
BEGIN {
	host_ip = from_quad("'${HOST_IP}'");
	netmask = from_quad("'${NETMASK}'");
	guest_net_ip = '${GUEST_IP_OFFSET}' + '${GUEST_ID}';
	if (and(netmask, guest_net_ip))
		exit(1);	# address too big for subnet
	subnet = and(host_ip, netmask);
	guest_ip = or(subnet, guest_net_ip);
	if (guest_ip == host_ip)
		exit(1);	# computed guest ip matches host ip

	printf("SUBNET=%s\n", to_quad(subnet));
	printf("GUEST_IP=%s\n", to_quad(guest_ip));
}
' < /dev/null
)

############## OK, we now know all our network parameters...

# There is a series of things that need to be done as superuser,
# so group them all into one big (and sort of nested!) sudo request.
sudo -s <<EnD_Of_sUdO
# Mount the boot disk for the UML and set up some configuration
# files there.
mkdir -p "${TMP_MNT}"
mount -o loop "${DISK_IMAGE_A}" "${TMP_MNT}"

# Arrange for loopback and eth0 to load automatically,
# and for eth0 to have our desired network parameters.
cat > "${TMP_MNT}/etc/network/interfaces" <<!
# Used by ifup(8) and ifdown(8). See the interfaces(5) manpage or
# /usr/share/doc/ifupdown/examples for more information.
auto lo
iface lo inet loopback
auto eth0
# iface eth0 inet dhcp
iface eth0 inet static
        address ${GUEST_IP}
        netmask ${NETMASK}
        broadcast ${BROADCAST}
        gateway ${HOST_IP}
!

# Have the guest start with an appropriate host name.
# Also record an entry for it in its own hosts file.
echo "${GUEST_HOSTNAME}" > "${TMP_MNT}/etc/hostname"
echo "${GUEST_IP}	${GUEST_HOSTNAME}" >> "${TMP_MNT}/etc/hosts"

# The host will serve as the name server also
cat > "${TMP_MNT}/etc/resolv.conf" <<!
nameserver ${HOST_IP}
!

# OK, done tweaking the boot image.
sync
umount "${DISK_IMAGE_A}"
rmdir "${TMP_MNT}"

# Set up a config file for "ifup" and "ifdown" (on the host) to use.
# All the backslashes below are needed because we're sitting inside
# a double here-document...
cat > "${TAP_IFUPDOWN_CONFIG}" <<!
iface ${TAP} inet manual
	up brctl addif virbr0 "\\\${IFACE}"
	up ip link set dev "\\\${IFACE}" up
	pre-down brctl delif virbr0 "\\\${IFACE}"
	pre-down ip link del dev "\\\${IFACE}"
	tunctl_user $(whoami)
!

# OK, bring up the tap device using our config file
ifup -i "${TAP_IFUPDOWN_CONFIG}" "${TAP}"

EnD_Of_sUdO

# Finally ready to launch the UML instance.
./linux \
	umid="${GUEST_HOSTNAME}" \
	ubda="${DISK_IMAGE_A}" \
	eth0="tuntap,${TAP}" \
	mem=1024M

# When we're done, clean up.  Bring down the tap interface and
# delete the config file.
#
# Note that if the above "./linux" crashes, you'll need to run the
# following commands manually in order to clean up state.
sudo ifdown -i "${TAP_IFUPDOWN_CONFIG}" "${TAP}"
sudo rm -f "${TAP_IFUPDOWN_CONFIG}"

exit 0
