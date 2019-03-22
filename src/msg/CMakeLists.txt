set(msg_srcs
  DispatchQueue.cc
  Message.cc
  Messenger.cc
  QueueStrategy.cc
  msg_types.cc)

list(APPEND msg_srcs
  async/AsyncConnection.cc
  async/AsyncMessenger.cc
  async/Protocol.cc
  async/ProtocolV1.cc
  async/ProtocolV2.cc
  async/Event.cc
  async/EventSelect.cc
  async/PosixStack.cc
  async/Stack.cc
  async/crypto_onwire.cc
  async/net_handler.cc)

if(LINUX)
  list(APPEND msg_srcs
    async/EventEpoll.cc)
elseif(FREEBSD OR APPLE)
  list(APPEND msg_srcs
    async/EventKqueue.cc)
endif(LINUX)

if(HAVE_RDMA)
  list(APPEND msg_srcs
    async/rdma/Infiniband.cc
    async/rdma/RDMAConnectedSocketImpl.cc
    async/rdma/RDMAIWARPConnectedSocketImpl.cc
    async/rdma/RDMAServerSocketImpl.cc
    async/rdma/RDMAIWARPServerSocketImpl.cc
    async/rdma/RDMAStack.cc)
endif()

add_library(common-msg-objs OBJECT ${msg_srcs})

if(WITH_DPDK)
  set(async_dpdk_srcs
    async/dpdk/ARP.cc
    async/dpdk/DPDK.cc
    async/dpdk/dpdk_rte.cc
    async/dpdk/DPDKStack.cc
    async/dpdk/EventDPDK.cc
    async/dpdk/IP.cc
    async/dpdk/net.cc
    async/dpdk/IPChecksum.cc
    async/dpdk/Packet.cc
    async/dpdk/TCP.cc
    async/dpdk/UserspaceEvent.cc
    async/dpdk/ethernet.cc)
  add_library(common_async_dpdk STATIC
    ${async_dpdk_srcs})
  target_link_libraries(common_async_dpdk PRIVATE
    dpdk::dpdk)
  # Stack.cc includes DPDKStack.h, which includes rte_config.h indirectly
  target_include_directories(common-msg-objs PRIVATE
    $<TARGET_PROPERTY:dpdk::dpdk,INTERFACE_INCLUDE_DIRECTORIES>)
endif(WITH_DPDK)
