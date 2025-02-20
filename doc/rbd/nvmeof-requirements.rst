============================
NVME-oF Gateway Requirements
============================

- At least 8 GB of RAM dedicated to the GW  (on each node running an NVME-oF GW) 
- It is hightly recommended to dedicate at least 4 cores to the GW (1 can work but perofrmance will be accordingly) 
- For high availability, provision at least 2 GWs in a GW group. 
- A minimum a single 10Gb Ethernet link in the Ceph public network for the gateway. For higher performance use 25 or 100 Gb links in the public network. 
- Provision at least two NVMe/TCP gateways on different Ceph nodes for highly-availability Ceph NVMe/TCP solution. 

