// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_REPLICA_CLIENT_H
#define CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_REPLICA_CLIENT_H

#include <string>
#include <vector>
#include <set>
#include <condition_variable>
#include <mutex>

#include "include/rados/librados.hpp"
#include "Types.h"
#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"
#include "TimerPing.h"

namespace librbd::cache::pwl::rwl::replica {

class PrimaryPing;
class ReplicaClient {
  uint64_t _id;
  uint64_t _size;
  uint32_t _copies;

  epoch_t _cache_id;

  std::string _pool_name;
  std::string _image_name;

  using ClientHandlerPtr = std::shared_ptr<ClientHandler>;
  using ReactorPtr = std::shared_ptr<Reactor>;
  struct DaemonInfo {
    uint64_t id;
    std::string rdma_ip;
    std::string rdma_port;
    ClientHandlerPtr client_handler;
  };
  std::vector<DaemonInfo> _daemons;
  std::set<uint64_t> _need_free_daemons;

  CephContext *_cct;

  ReactorPtr _reactor;
  std::unique_ptr<std::thread> _reactor_thread;

  std::unique_ptr<PrimaryPing> _ping;

  librados::Rados rados;
  librados::IoCtx io_ctx;

  void* _local_head_ptr{nullptr};

 public:
  ReplicaClient(CephContext *cct, uint64_t size, uint32_t copies, std::string pool_name, std::string image_name, librados::IoCtx& ioctx);
  ~ReplicaClient();
  int write(size_t offset, size_t len);
  int write(const void* addr, size_t len);
  int flush();
  int init(void *head_ptr, uint64_t size);
  void close();

 public:
  void shutdown();
  int init_ioctx();
  int cache_request();
  int cache_free();
  void disconnect();
  int set_head(void *head_ptr, uint64_t size);
  int replica_init();
  int replica_close();

  bool single_ping();
};

}

#endif //CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_REPLICA_CLIENT_H
