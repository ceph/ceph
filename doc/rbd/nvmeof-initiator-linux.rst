==============================
 NVMe/TCP Initiator for Linux
==============================

Prerequisites
=============

- Kernel 5.0 or later
- RHEL 9.2 or later
- Ubuntu 24.04 or later
- SLES 15 SP3 or later

Installation
============

1. Install the nvme-cli:

   .. prompt:: bash #
   
      yum install nvme-cli

2. Load the NVMe-oF module:

   .. prompt:: bash # 
   
      modprobe nvme-fabrics

3. Verify the NVMe/TCP target is reachable:

   .. prompt::  bash #
   
      nvme discover -t tcp -a GATEWAY_IP -s 4420

4. Connect to the NVMe/TCP target:

   .. prompt:: bash #
   
      nvme connect -t tcp -a GATEWAY_IP -n SUBSYSTEM_NQN

Next steps
==========

Verify that the initiator is set up correctly:

1. List the NVMe block devices:

   .. prompt:: bash #
   
      nvme list

2. Create a filesystem on the desired device:

   .. prompt:: bash #
   
      mkfs.ext4 NVME_NODE_PATH

3. Mount the filesystem:

   .. prompt:: bash #
   
      mkdir /mnt/nvmeof

   .. prompt:: bash #
   
      mount NVME_NODE_PATH /mnt/nvmeof

4. List the NVME-oF files:

   .. prompt:: bash #
   
      ls /mnt/nvmeof

5. Create a text file in the ``/mnt/nvmeof`` directory:

   .. prompt:: bash #
   
      echo "Hello NVME-oF" > /mnt/nvmeof/hello.text

6. Verify that the file can be accessed:

   .. prompt:: bash #
   
      cat /mnt/nvmeof/hello.text
