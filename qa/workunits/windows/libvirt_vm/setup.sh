#!/usr/bin/env bash
set -ex

WINDOWS_SERVER_2019_ISO_URL=${WINDOWS_SERVER_2019_ISO_URL:-"https://software-download.microsoft.com/download/pr/17763.737.190906-2324.rs5_release_svc_refresh_SERVER_EVAL_x64FRE_en-us_1.iso"}
VIRTIO_WIN_ISO_URL=${VIRTIO_WIN_ISO_URL:-"https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/stable-virtio/virtio-win.iso"}

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"

# Use build_utils.sh from ceph-build
curl --retry-max-time 30 --retry 10 -L -o ${DIR}/build_utils.sh https://raw.githubusercontent.com/ceph/ceph-build/main/scripts/build_utils.sh
source ${DIR}/build_utils.sh

# Helper function to restart the Windows VM
function restart_windows_vm() {
    echo "Restarting Windows VM"
    ssh_exec "cmd.exe /c 'shutdown.exe /r /t 0 & sc.exe stop sshd'"
    SECONDS=0
    TIMEOUT=${1:-600}
    while true; do
        if [[ $SECONDS -gt $TIMEOUT ]]; then
            echo "Timeout waiting for the VM to start"
            exit 1
        fi
        ssh_exec hostname || {
            echo "Cannot execute SSH commands yet"
            sleep 10
            continue
        }
        break
    done
    echo "Windows VM restarted"
}

# Install libvirt with KVM
retrycmd_if_failure 5 0 5m sudo apt-get update
retrycmd_if_failure 5 0 10m sudo apt-get install -y qemu-kvm libvirt-daemon-system libvirt-clients virtinst

# Download ISO images
echo "Downloading virtio-win ISO"
retrycmd_if_failure 5 0 30m curl -C - -L $VIRTIO_WIN_ISO_URL -o ${DIR}/virtio-win.iso
echo "Downloading Windows Server 2019 ISO"
retrycmd_if_failure 5 0 60m curl -C - -L $WINDOWS_SERVER_2019_ISO_URL -o ${DIR}/windows-server-2019.iso

# Create virtual floppy image with the unattended instructions to install Windows Server 2019
echo "Creating floppy image"
qemu-img create -f raw ${DIR}/floppy.img 1440k
mkfs.msdos -s 1 ${DIR}/floppy.img
mkdir ${DIR}/floppy
sudo mount ${DIR}/floppy.img ${DIR}/floppy
ssh-keygen -b 2048 -t rsa -f ${DIR}/id_rsa -q -N ""
sudo cp \
    ${DIR}/autounattend.xml \
    ${DIR}/first-logon.ps1 \
    ${DIR}/id_rsa.pub \
    ${DIR}/utils.ps1 \
    ${DIR}/setup.ps1 \
    ${DIR}/floppy/
sudo umount ${DIR}/floppy
rmdir ${DIR}/floppy

echo "Starting libvirt VM"
qemu-img create -f qcow2 ${DIR}/ceph-win-ltsc2019.qcow2 50G
VM_NAME="ceph-win-ltsc2019"
sudo virt-install \
    --name $VM_NAME \
    --os-variant win2k19 \
    --boot hd,cdrom \
    --virt-type kvm \
    --graphics spice \
    --cpu host \
    --vcpus 4 \
    --memory 4096 \
    --disk ${DIR}/floppy.img,device=floppy \
    --disk ${DIR}/ceph-win-ltsc2019.qcow2,bus=virtio \
    --disk ${DIR}/windows-server-2019.iso,device=cdrom \
    --disk ${DIR}/virtio-win.iso,device=cdrom \
    --network network=default,model=virtio \
    --controller type=virtio-serial \
    --channel unix,target_type=virtio,name=org.qemu.guest_agent.0 \
    --noautoconsole

export SSH_USER="administrator"
export SSH_KNOWN_HOSTS_FILE="${DIR}/known_hosts"
export SSH_KEY="${DIR}/id_rsa"

SECONDS=0
TIMEOUT=1800
SLEEP_SECS=30
while true; do
    if [[ $SECONDS -gt $TIMEOUT ]]; then
        echo "Timeout waiting for the VM to start"
        exit 1
    fi
    VM_IP=$(sudo virsh domifaddr --source agent --interface Ethernet --full $VM_NAME | grep ipv4 | awk '{print $4}' | cut -d '/' -f1) || {
        echo "Retrying in $SLEEP_SECS seconds"
        sleep $SLEEP_SECS
        continue
    }
    ssh-keyscan -H $VM_IP &> $SSH_KNOWN_HOSTS_FILE || {
        echo "SSH is not reachable yet"
        sleep $SLEEP_SECS
        continue
    }
    SSH_ADDRESS=$VM_IP ssh_exec hostname || {
        echo "Cannot execute SSH commands yet"
        sleep $SLEEP_SECS
        continue
    }
    break
done
export SSH_ADDRESS=$VM_IP

scp_upload ${DIR}/utils.ps1 /utils.ps1
scp_upload ${DIR}/setup.ps1 /setup.ps1
SSH_TIMEOUT=1h ssh_exec /setup.ps1

cd $DIR

# Get the helper script to download Chacra builds
retrycmd_if_failure 10 5 1m curl -L -o ./get-chacra-bin.py https://raw.githubusercontent.com/ceph/ceph-win32-tests/main/get-bin.py
chmod +x ./get-chacra-bin.py

# Download latest WNBD build from Chacra
retrycmd_if_failure 10 0 10m ./get-chacra-bin.py --project wnbd --filename wnbd.zip
scp_upload wnbd.zip /wnbd.zip
ssh_exec tar.exe xzvf /wnbd.zip -C /

# Install WNBD driver
ssh_exec Import-Certificate -FilePath /wnbd/driver/wnbd.cer -Cert Cert:\\LocalMachine\\Root
ssh_exec Import-Certificate -FilePath /wnbd/driver/wnbd.cer -Cert Cert:\\LocalMachine\\TrustedPublisher
ssh_exec /wnbd/binaries/wnbd-client.exe install-driver /wnbd/driver/wnbd.inf
restart_windows_vm
ssh_exec wnbd-client.exe -v

# Download Ceph Windows build from Chacra
CEPH_REPO_FILE="/etc/apt/sources.list.d/ceph.list"
PROJECT=$(cat $CEPH_REPO_FILE | cut -d ' ' -f3 | tr '\/', ' ' | awk '{print $4}')
BRANCH=$(cat $CEPH_REPO_FILE | cut -d ' ' -f3 | tr '\/', ' ' | awk '{print $5}')
SHA1=$(cat $CEPH_REPO_FILE | cut -d ' ' -f3 | tr '\/', ' ' | awk '{print $6}')
retrycmd_if_failure 10 0 10m ./get-chacra-bin.py --project $PROJECT --branchname $BRANCH --sha1 $SHA1 --filename ceph.zip

# Install Ceph on Windows
SSH_TIMEOUT=5m scp_upload ./ceph.zip /ceph.zip
SSH_TIMEOUT=10m ssh_exec tar.exe xzvf /ceph.zip -C /
ssh_exec "New-Service -Name ceph-rbd -BinaryPathName 'c:\ceph\rbd-wnbd.exe service'"
ssh_exec Start-Service -Name ceph-rbd
ssh_exec rbd.exe -v

# Setup Ceph configs and directories
ssh_exec mkdir -force /etc/ceph, /var/run/ceph, /var/log/ceph
for i in $(ls /etc/ceph); do
    scp_upload /etc/ceph/$i /etc/ceph/$i
done

cat << EOF > ${DIR}/connection_info.sh
export SSH_USER="${SSH_USER}"
export SSH_KNOWN_HOSTS_FILE="${SSH_KNOWN_HOSTS_FILE}"
export SSH_KEY="${SSH_KEY}"
export SSH_ADDRESS="${SSH_ADDRESS}"
EOF

echo "Windows Server 2019 libvirt testing VM is ready"
