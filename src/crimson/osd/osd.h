// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/timer.hh>

#include "crimson/common/type_helpers.h"
#include "crimson/common/auth_handler.h"
#include "crimson/common/gated.h"
#include "crimson/net/chained_dispatchers.h"
#include "crimson/admin/admin_socket.h"
#include "crimson/admin/osd_admin.h"
#include "crimson/common/simple_lru.h"
#include "crimson/common/shared_lru.h"
#include "crimson/mgr/client.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/state.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/pg_map.h"
#include "crimson/osd/osd_operations/peering_event.h"

#include "messages/MOSDOp.h"
#include "osd/PeeringState.h"
#include "osd/osd_types.h"
#include "osd/osd_perf_counters.h"
#include "osd/PGPeeringEvent.h"

class MCommand;
class MOSDMap;
class MOSDRepOpReply;
class MOSDRepOp;
class OSDMap;
class OSDMeta;
class Heartbeat;

namespace ceph::os {
  class Transaction;
}

namespace crimson::mon {
  class Client;
}

namespace crimson::net {
  class Messenger;
}

namespace crimson::os {
  class FuturizedStore;
}

namespace crimson::osd {
class PG;

class OSD final : public crimson::net::Dispatcher,
		  private OSDMapService,
		  private crimson::common::AuthHandler,
		  private crimson::mgr::WithStats {
  const int whoami;
  const uint32_t nonce;
  seastar::timer<seastar::lowres_clock> beacon_timer;
  // talk with osd
  crimson::net::MessengerRef cluster_msgr;
  // talk with client/mon/mgr
  crimson::net::MessengerRef public_msgr;
  std::unique_ptr<crimson::mon::Client> monc;
  std::unique_ptr<crimson::mgr::Client> mgrc;

  SharedLRU<epoch_t, OSDMap> osdmaps;
  SimpleLRU<epoch_t, bufferlist, false> map_bl_cache;
  cached_map_t osdmap;
  // TODO: use a wrapper for ObjectStore
  std::unique_ptr<crimson::os::FuturizedStore> store;
  std::unique_ptr<OSDMeta> meta_coll;

  OSDState state;

  /// _first_ epoch we were marked up (after this process started)
  epoch_t boot_epoch = 0;
  /// _most_recent_ epoch we were marked up
  epoch_t up_epoch = 0;
  //< epoch we last did a bind to new ip:ports
  epoch_t bind_epoch = 0;
  //< since when there is no more pending pg creates from mon
  epoch_t last_pg_create_epoch = 0;

  ceph::mono_time startup_time;

  OSDSuperblock superblock;

  // Dispatcher methods
  seastar::future<> ms_dispatch(crimson::net::Connection* conn, MessageRef m) final;
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) final;
  void ms_handle_remote_reset(crimson::net::ConnectionRef conn) final;

  // mgr::WithStats methods
  MessageRef get_stats() final;

  // AuthHandler methods
  void handle_authentication(const EntityName& name,
			     const AuthCapsInfo& caps) final;

  crimson::osd::ShardServices shard_services;

  std::unique_ptr<Heartbeat> heartbeat;
  seastar::timer<seastar::lowres_clock> heartbeat_timer;

  // admin-socket
  seastar::lw_shared_ptr<crimson::admin::AdminSocket> asok;

public:
  OSD(int id, uint32_t nonce,
      crimson::net::MessengerRef cluster_msgr,
      crimson::net::MessengerRef client_msgr,
      crimson::net::MessengerRef hb_front_msgr,
      crimson::net::MessengerRef hb_back_msgr);
  ~OSD() final;

  seastar::future<> mkfs(uuid_d osd_uuid, uuid_d cluster_fsid);

  seastar::future<> start();
  seastar::future<> stop();

  void dump_status(Formatter*) const;

  void print(std::ostream&) const;
private:
  seastar::future<> start_boot();
  seastar::future<> _preboot(version_t oldest_osdmap, version_t newest_osdmap);
  seastar::future<> _send_boot();
  seastar::future<> _add_me_to_crush();

  seastar::future<Ref<PG>> make_pg(cached_map_t create_map,
				   spg_t pgid,
				   bool do_create);
  seastar::future<Ref<PG>> load_pg(spg_t pgid);
  seastar::future<> load_pgs();

  epoch_t up_thru_wanted = 0;
  seastar::future<> _send_alive();

  // OSDMapService methods
  epoch_t get_up_epoch() const final {
    return up_epoch;
  }
  seastar::future<cached_map_t> get_map(epoch_t e) final;
  cached_map_t get_map() const final;
  seastar::future<std::unique_ptr<OSDMap>> load_map(epoch_t e);
  seastar::future<bufferlist> load_map_bl(epoch_t e);
  void store_map_bl(ceph::os::Transaction& t,
                    epoch_t e, bufferlist&& bl);
  seastar::future<> store_maps(ceph::os::Transaction& t,
                               epoch_t start, Ref<MOSDMap> m);
  seastar::future<> osdmap_subscribe(version_t epoch, bool force_request);

  void write_superblock(ceph::os::Transaction& t);
  seastar::future<> read_superblock();

  bool require_mon_peer(crimson::net::Connection *conn, Ref<Message> m);

  seastar::future<Ref<PG>> handle_pg_create_info(
    std::unique_ptr<PGCreateInfo> info);

  seastar::future<> handle_osd_map(crimson::net::Connection* conn,
                                   Ref<MOSDMap> m);
  seastar::future<> handle_osd_op(crimson::net::Connection* conn,
				  Ref<MOSDOp> m);
  seastar::future<> handle_rep_op(crimson::net::Connection* conn,
				  Ref<MOSDRepOp> m);
  seastar::future<> handle_rep_op_reply(crimson::net::Connection* conn,
					Ref<MOSDRepOpReply> m);
  seastar::future<> handle_peering_op(crimson::net::Connection* conn,
				      Ref<MOSDPeeringOp> m);
  seastar::future<> handle_recovery_subreq(crimson::net::Connection* conn,
					   Ref<MOSDFastDispatchOp> m);


  seastar::future<> committed_osd_maps(version_t first,
                                       version_t last,
                                       Ref<MOSDMap> m);

  void check_osdmap_features();

  seastar::future<> handle_command(crimson::net::Connection* conn,
				   Ref<MCommand> m);
  seastar::future<> start_asok_admin();

public:
  OSDMapGate osdmap_gate;

  ShardServices &get_shard_services() {
    return shard_services;
  }

  seastar::future<> consume_map(epoch_t epoch);

private:
  PGMap pg_map;
  crimson::common::Gated gate;
public:
  blocking_future<Ref<PG>> get_or_create_pg(
    spg_t pgid,
    epoch_t epoch,
    std::unique_ptr<PGCreateInfo> info);
  blocking_future<Ref<PG>> wait_for_pg(
    spg_t pgid);

  bool should_restart() const;
  seastar::future<> restart();
  seastar::future<> shutdown();

  seastar::future<> send_beacon();
  void update_heartbeat_peers();

  friend class PGAdvanceMap;
};

inline std::ostream& operator<<(std::ostream& out, const OSD& osd) {
  osd.print(out);
  return out;
}

}
