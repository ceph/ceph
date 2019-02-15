// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

#include "crimson/mon/MonClient.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/osd/chained_dispatchers.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/state.h"

#include "osd/OSDMap.h"

class MOSDMap;
class OSDMap;
class OSDMeta;
class PG;
class Heartbeat;

namespace ceph::net {
  class Messenger;
}

namespace ceph::os {
  class CyanStore;
  struct Collection;
  class Transaction;
}

template<typename T> using Ref = boost::intrusive_ptr<T>;

class OSD : public ceph::net::Dispatcher,
	    private OSDMapService {
  seastar::gate gate;
  seastar::timer<seastar::lowres_clock> beacon_timer;
  const int whoami;
  const uint32_t nonce;
  // talk with osd
  ceph::net::Messenger* cluster_msgr = nullptr;
  // talk with client/mon/mgr
  ceph::net::Messenger* public_msgr = nullptr;
  ChainedDispatchers dispatchers;
  std::unique_ptr<ceph::mon::Client> monc;

  std::unique_ptr<Heartbeat> heartbeat;
  seastar::timer<seastar::lowres_clock> heartbeat_timer;

  // TODO: use LRU cache
  std::map<epoch_t, seastar::lw_shared_ptr<OSDMap>> osdmaps;
  std::map<epoch_t, bufferlist> map_bl_cache;
  seastar::lw_shared_ptr<OSDMap> osdmap;
  // TODO: use a wrapper for ObjectStore
  std::unique_ptr<ceph::os::CyanStore> store;
  std::unique_ptr<OSDMeta> meta_coll;

  std::unordered_map<spg_t, Ref<PG>> pgs;
  OSDState state;

  /// _first_ epoch we were marked up (after this process started)
  epoch_t boot_epoch = 0;
  /// _most_recent_ epoch we were marked up
  epoch_t up_epoch = 0;
  //< epoch we last did a bind to new ip:ports
  epoch_t bind_epoch = 0;
  //< since when there is no more pending pg creates from mon
  epoch_t last_pg_create_epoch = 0;

  OSDSuperblock superblock;

  // Dispatcher methods
  seastar::future<> ms_dispatch(ceph::net::ConnectionRef conn, MessageRef m) override;
  seastar::future<> ms_handle_connect(ceph::net::ConnectionRef conn) override;
  seastar::future<> ms_handle_reset(ceph::net::ConnectionRef conn) override;
  seastar::future<> ms_handle_remote_reset(ceph::net::ConnectionRef conn) override;

public:
  OSD(int id, uint32_t nonce);
  ~OSD() override;

  seastar::future<> mkfs(uuid_d fsid);

  seastar::future<> start();
  seastar::future<> stop();

private:
  seastar::future<> start_boot();
  seastar::future<> _preboot(version_t newest_osdmap, version_t oldest_osdmap);
  seastar::future<> _send_boot();

  seastar::future<Ref<PG>> load_pg(spg_t pgid);
  seastar::future<> load_pgs();

  // OSDMapService methods
  seastar::future<seastar::lw_shared_ptr<OSDMap>> get_map(epoch_t e) override;
  seastar::lw_shared_ptr<OSDMap> get_map() const override;

  seastar::future<bufferlist> load_map_bl(epoch_t e);
  void store_map_bl(ceph::os::Transaction& t,
                    epoch_t e, bufferlist&& bl);
  seastar::future<> store_maps(ceph::os::Transaction& t,
                               epoch_t start, Ref<MOSDMap> m);
  seastar::future<> osdmap_subscribe(version_t epoch, bool force_request);

  void write_superblock(ceph::os::Transaction& t);
  seastar::future<> read_superblock();

  seastar::future<> handle_osd_map(ceph::net::ConnectionRef conn,
                                   Ref<MOSDMap> m);
  seastar::future<> committed_osd_maps(version_t first,
                                       version_t last,
                                       Ref<MOSDMap> m);
  bool should_restart() const;
  seastar::future<> restart();
  seastar::future<> shutdown();

  seastar::future<> send_beacon();
  void update_heartbeat_peers();
};
