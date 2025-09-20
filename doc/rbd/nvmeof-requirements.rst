============================
NVMe-oF Gateway Requirements
============================

- At least 8 GB of RAM dedicated to each NVME-oF gateway instance
- We highly recommend dedicating at least four CPU threads or vcores to each
  NVME-oF gateway. A setup with only one CPU thread or vcore can work, but
  performance may be below expectations.  It is preferable to dedicate servers
  to the NVMe-oF gateway service so that these and other Ceph services do not
  degrade each other.
- Provide at minimum a 10 Gb/s network link to the Ceph public network. For
  best latency and throughput, we recommend 25 Gb/s or 100 Gb/s links.
- Bonding of network links, with an appropriate xmit hash policy, is ideal for
  high availability. Note that the throughput of a given NVMe-oF client can be
  no higher than that of a single link within a bond. Thus, if four 10 Gb/s
  links are bonded together on gateway nodes, no one client will realize more
  than 10 Gb/s throughput. Remember that Ceph NVMe-oF gateways also communicate
  with backing OSDs over the public network at the same time, which contends
  with traffic between clients and gateways. Make sure to provision networking
  resources generously to avoid congestion and saturation.  
- Provision at least two NVMe-oF gateways in a gateway group, on separate Ceph
  cluster nodes, for a highly-available Ceph NVMe/TCP solution.
- Ceph NVMe-oF gateway containers comprise multiple components that communicate
  with each other. If the nodes that run these containers require HTTP/HTTPS
  proxy configuration to reach container registries or other external
  resources, these settings may confound this internal communication. If you
  experience gRPC or other errors when provisioning NVMe-oF gateways, you may
  need to adjust your proxy configuration.
