// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_TIMER_PING_H
#define CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_TIMER_PING_H

#include "Types.h"
#include <string>
#include <unordered_map>
#include <set>
#include <memory>
#include <filesystem>

#include "Reactor.h"
#include "ReplicaClient.h"

#include "include/rados/librados.hpp"
#include "common/ceph_context.h"
#include "common/Timer.h"
#include "include/Context.h"
#include "common/ceph_mutex.h"

namespace librbd::cache::pwl::rwl::replica {

class DaemonPing : public std::enable_shared_from_this<DaemonPing> {
public:
  CephContext *_cct;
  librados::Rados &_rados;
  librados::IoCtx &_io_ctx;
  ceph::mutex _mutex;
  SafeTimer _ping_timer;
  std::shared_ptr<Reactor> _reactor;
  std::set<epoch_t> need_free_caches;
  std::set<epoch_t> freed_caches;
  std::unordered_map<epoch_t, RwlCacheInfo> infos;
  static int get_cache_info_from_filename(std::filesystem::path file, struct RwlCacheInfo& info);
  void update_cacheinfos();
public:
  DaemonPing(CephContext *cct, librados::Rados &rados, librados::IoCtx &io_ctx);
  ~DaemonPing();
  int single_ping();
  int timer_ping();
  int free_caches();
  void init(std::shared_ptr<Reactor> reactor);
private:
  struct C_Ping : public Context {
    C_Ping(std::shared_ptr<DaemonPing> dp);
    ~C_Ping() override;
    void finish(int r) override;

    std::shared_ptr<DaemonPing> dp;
  };
};

class ReplicaClient;
class PrimaryPing {
public:
  CephContext *_cct;
  librados::IoCtx &_io_ctx;
  ceph::mutex _mutex;
  SafeTimer _ping_timer;
  ReplicaClient* _client;
public:
  PrimaryPing(CephContext *cct, librados::IoCtx &io_ctx, ReplicaClient* client);
  ~PrimaryPing();
  int timer_ping();
private:
  struct C_Ping : public Context {
    C_Ping(PrimaryPing* ping);
    ~C_Ping() override;
    void finish(int r) override;

    PrimaryPing* pping;
  };
};

}

#endif