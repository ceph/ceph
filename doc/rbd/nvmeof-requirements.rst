============================
NVMe-oF Gateway Requirements
============================

- At least 8 GB of RAM dedicated to each gateway instance
- It is hightly recommended to dedicate at least four CPU threads / vcores to each
  gateway.  One can work but performance may be below expectations.  It is
  ideal to dedicate servers to NVMe-oF gateway service so that these
  and other Ceph services do not degrade each other.
- At minimum a 10 Gb/s network link to the Ceph public network. For best
  latency and throughput we recommend 25 Gb/s or 100 Gb/s links.
- Bonding of network links, with an appropriate xmit hash policy, is ideal
  for high availability.  Note that the throughput of a given NVMe-oF client
  can be no higher than that of a single link within a bond.  Thus, if four
  10 Gb/s links are bonded together on gateway nodes, no one client will
  realize more than 10 Gb/s throughput.  Moreover, remember that Ceph
  NVMe-oF gateways also communicate with backing OSDs over the public
  network at the same time, which contends with traffic between clients
  and gateways. Provision networking generously to avoid congestion and
  saturation.
- Provision at least two NVMe-oF gateways in a gateway group, on separate
  Ceph cluster nodes, for a highly-availability Ceph NVMe/TCP solution.
- Ceph NVMe-oF gateway containers comprise multiple components that communicate
  with each other.  If the nodes running these containers require HTTP/HTTPS
  proxy configuration to reach container registries or other external resources,
  these settings may confound this internal communication.  If you experience
  gRPC or other errors when provisioning NVMe-oF gateways, you may need to
  adjust your proxy configuration.

