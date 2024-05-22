---------------------------------
NVMe/TCP Initiator for VMware ESX
---------------------------------

Prerequisites
=============

- A VMware ESXi host running VMware vSphere Hypervisor (ESXi) 7.0U3 version or later.
- Deployed Ceph NVMe-oF gateway.
- Ceph cluster with NVMe-oF configuration.
- Subsystem defined in the gateway.

Configuration
=============

The following instructions will use the default vSphere web client and esxcli.

1. Enable NVMe/TCP on a NIC:

   .. prompt:: bash #
    
      esxcli nvme fabric enable --protocol TCP --device vmnicN

   Replace ``N`` with the number of the NIC.

2. Tag a VMKernel NIC to permit NVMe/TCP traffic:

   .. prompt:: bash #
    
      esxcli network uip interface tag add --interface-nme vmkN --tagname NVMeTCP

   Replace ``N`` with the ID of the VMkernel.

3. Configure the VMware ESXi host for NVMe/TCP:

    #. List the NVMe-oF adapter:
    
       .. prompt:: bash #
        
          esxcli nvme adapter list

    #. Discover NVMe-oF subsystems:
    
       .. prompt:: bash #
        
          esxcli nvme fabric discover -a NVME_TCP_ADAPTER -i GATEWAY_IP -p 4420
    
    #. Connect to NVME-oF gateway subsystem:
    
       .. prompt:: bash #
        
          esxcli nvme connect -a NVME_TCP_ADAPTER -i GATEWAY_IP -p 4420 -s SUBSYSTEM_NQN

    #. List the NVMe/TCP controllers:
    
       .. prompt:: bash #
        
          esxcli nvme controller list

    #. List the NVMe-oF namespaces in the subsystem:
    
       .. prompt:: bash #
        
          esxcli nvme namespace list

4. Verify that the initiator has been set up correctly:

    #. From the vSphere client go to the ESXi host.
    #. On the Storage page go to the Devices tab.
    #. Verify that the NVME/TCP disks are listed in the table.
