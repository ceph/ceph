# iSCSI Gateway smoke test
set -x
rpm -q ceph-iscsi
systemctl is-active rbd-target-api
gwcli ls

rbd_pool=`grep pool /etc/ceph/iscsi-gateway.cfg | sed 's/pool = //g'`
ip_string=`grep trusted_ip_list /etc/ceph/iscsi-gateway.cfg | sed 's/trusted_ip_list = //g'`
IFS="," read -r -a ips <<< "$ip_string"

other_ips=()
for ip in "${ips[@]}"; do
    if ip addr | grep -q $ip; then
        my_ip=$ip
    else
        other_ips+=("$ip")
    fi
done

echo "My IP: $my_ip"
echo "Other gateways: ${other_ips[@]}"
echo "RBD pool: $rbd_pool"

gwcli /iscsi-targets create iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw
HOST=`curl -s --user admin:admin -X GET http://$my_ip:5000/api/sysinfo/hostname | jq -r .data`
if [ ! -n "$HOST" ]; then
  echo "ERROR: rbd-target-api is not running on ${my_ip}:5000"
  false
fi
gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/gateways create $HOST $my_ip skipchecks=true

for ip in "${other_ips[@]}"; do
    HOST=`curl -s --user admin:admin -X GET http://$ip:5000/api/sysinfo/hostname | jq -r .data`
    if [ -n "$HOST" ]; then
      gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/gateways create $HOST $ip skipchecks=true
    fi
done

gwcli /disks create pool=$rbd_pool image=disk_1 size=90G
gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts create iqn.1994-05.com.redhat:rh7-client
gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts/iqn.1994-05.com.redhat:rh7-client disk add $rbd_pool/disk_1
gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts auth nochap

gwcli ls
targetcli ls

ls -lR /sys/kernel/config/target/
ss --tcp --numeric state listening
echo "See 3260 there?"
set -e
zypper --non-interactive --no-gpg-checks install \
    --force --no-recommends open-iscsi multipath-tools

sed -i -e 's/InitiatorName=.\+/InitiatorName=iqn.1994-05.com.redhat:rh7-client/g' /etc/iscsi/initiatorname.iscsi

systemctl start iscsid.service
sleep 5
systemctl --no-pager --full status iscsid.service

iscsiadm -m discovery -t st -p $(hostname)
iscsiadm -m node -L all
iscsiadm -m session -P3
sleep 5

ls -l /dev/disk/by-path
ls -l /dev/disk/by-*id
iscsi_dev=/dev/disk/by-path/*${my_ip}*iscsi*
if ( mkfs -t xfs $iscsi_dev ) ; then
    :
else
    dmesg
    false
fi
test -d /mnt

mount $iscsi_dev /mnt
df -h /mnt
echo hubba > /mnt/bubba
test -s /mnt/bubba
umount /mnt

systemctl start multipathd.service
systemctl --no-pager --full status multipathd.service
sleep 5
multipath -ll  # to show multipath information
mp_dev=/dev/mapper/`multipath -ll | head -1 | sed 's/\(\w\+\).*/\1/g'`
mount $mp_dev /mnt
df -h /mnt
test -s /mnt/bubba
echo bubba > /mnt/hubba
test -s /mnt/hubba
umount /mnt

iscsiadm -m node --logout
iscsiadm -m discovery -t st -o delete -p $(hostname)

echo "OK" >/dev/null

