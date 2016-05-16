#! /bin/bash -f
echo $OS_CEPH_ISO
if [[ $# -ne 4 ]]; then
    echo "Usage: ceph_cluster mon.0 osd.0 osd.1 osd.2"
    exit -1
fi
allsites=$*
mon=$1
shift
osds=$*
ISOVAL=${OS_CEPH_ISO-rhceph-1.3.1-rhel-7-x86_64-dvd.iso}
sudo mount -o loop ${ISOVAL} /mnt

fqdn=`hostname -f`
lsetup=`ls /mnt/Installer | grep "^ice_setup"`
sudo yum -y install /mnt/Installer/${lsetup}
sudo ice_setup -d /mnt << EOF
yes
/mnt
$fqdn
http
EOF
ceph-deploy new ${mon}
ceph-deploy install --repo --release=ceph-mon ${mon}
ceph-deploy install --repo --release=ceph-osd ${allsites}
ceph-deploy install --mon ${mon}
ceph-deploy install --osd ${allsites}
ceph-deploy mon create-initial
sudo service ceph -a start osd
for d in b c d; do
    for m in $osds; do
        ceph-deploy disk zap ${m}:sd${d}
    done
    for m in $osds; do
        ceph-deploy osd prepare ${m}:sd${d}
    done
    for m in $osds; do
        ceph-deploy osd activate ${m}:sd${d}1:sd${d}2
    done
done

sudo ./ceph-pool-create.sh

hchk=`sudo ceph health`
while [[ $hchk != 'HEALTH_OK' ]]; do
    sleep 30
    hchk=`sudo ceph health`
done
