// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/timer.hh>

#include "crimson/common/simple_lru.h"
#include "crimson/common/shared_lru.h"
#include "crimson/mgr/client.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/osd/chained_dispatchers.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/state.h"

#include "osd/osd_types.h"

class MOSDMap;
class MOSDOp;
class OSDMap;
class OSDMeta;
class PG;
class PGPeeringEvent;
class Heartbeat;

namespace ceph::mon {
  class Client;
}

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
	    private OSDMapService,
	    private ceph::mgr::WithStats {
  seastar::gate gate;
  const int whoami;
  const uint32_t nonce;
  seastar::timer<seastar::lowres_clock> beacon_timer;
  // talk with osd
  ceph::net::Messenger& cluster_msgr;
  // talk with client/mon/mgr
  ceph::net::Messenger& public_msgr;
  ChainedDispatchers dispatchers;
  std::unique_ptr<ceph::mon::Client> monc;
  std::unique_ptr<ceph::mgr::Client> mgrc;

  std::unique_ptr<Heartbeat> heartbeat;
  seastar::timer<seastar::lowres_clock> heartbeat_timer;

  SharedLRU<epoch_t, OSDMap> osdmaps;
  SimpleLRU<epoch_t, bufferlist, false> map_bl_cache;
  cached_map_t osdmap;
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
  // mgr::WithStats methods
  MessageRef get_stats() override;

public:
  OSD(int id, uint32_t nonce,
      ceph::net::Messenger& cluster_msgr,
      ceph::net::Messenger& client_msgr,
      ceph::net::Messenger& hb_front_msgr,
      ceph::net::Messenger& hb_back_msgr);
  ~OSD() override;

  seastar::future<> mkfs(uuid_d fsid);

  seastar::future<> start();
  seastar::future<> stop();

private:
  seastar::future<> start_boot();
  seastar::future<> _preboot(version_t oldest_osdmap, version_t newest_osdmap);
  seastar::future<> _send_boot();

  seastar::future<Ref<PG>> load_pg(spg_t pgid);
  seastar::future<> load_pgs();

  epoch_t up_thru_wanted = 0;
  seastar::future<> _send_alive(epoch_t want);

  // OSDMapService methods
  seastar::future<cached_map_t> get_map(epoch_t e) override;
  cached_map_t get_map() const override;
  seastar::future<std::unique_ptr<OSDMap>> load_map(epoch_t e);
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
  seastar::future<> handle_osd_op(ceph::net::ConnectionRef conn,
				  Ref<MOSDOp> m);
  seastar::future<> handle_pg_log(ceph::net::ConnectionRef conn,
				  Ref<MOSDPGLog> m);
  seastar::future<> handle_pg_notify(ceph::net::ConnectionRef conn,
				     Ref<MOSDPGNotify> m);
  seastar::future<> handle_pg_info(ceph::net::ConnectionRef conn,
				   Ref<MOSDPGInfo> m);
  seastar::future<> handle_pg_query(ceph::net::ConnectionRef conn,
				    Ref<MOSDPGQuery> m);

  seastar::future<> committed_osd_maps(version_t first,
                                       version_t last,
                                       Ref<MOSDMap> m);

  // order the promises in descending order of the waited osdmap epoch,
  // so we can access all the waiters expecting a map whose epoch is less
  // than a given epoch
  using waiting_peering_t = std::map<epoch_t, seastar::shared_promise<epoch_t>,
				     std::greater<epoch_t>>;
  waiting_peering_t waiting_peering;
  // wait for an osdmap whose epoch is greater or equal to given epoch
  seastar::future<epoch_t> wait_for_map(epoch_t epoch);
  seastar::future<> consume_map(epoch_t epoch);
  seastar::future<> do_peering_event(spg_t pgid,
				     std::unique_ptr<PGPeeringEvent> evt);
  seastar::future<> advance_pg_to(Ref<PG> pg, epoch_t to);

  bool should_restart() const;
  seastar::future<> restart();
  seastar::future<> shutdown();

  seastar::future<> send_beacon();
  void update_heartbeat_peers();
};
