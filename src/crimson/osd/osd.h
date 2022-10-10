// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/timer.hh>

#include "crimson/common/logclient.h"
#include "crimson/common/type_helpers.h"
#include "crimson/common/auth_handler.h"
#include "crimson/common/gated.h"
#include "crimson/admin/admin_socket.h"
#include "crimson/common/simple_lru.h"
#include "crimson/mgr/client.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/pg_shard_manager.h"
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
class MOSDScrub2;
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
		  private crimson::common::AuthHandler,
		  private crimson::mgr::WithStats {
  const int whoami;
  const uint32_t nonce;
  seastar::abort_source& abort_source;
  seastar::timer<seastar::lowres_clock> beacon_timer;
  // talk with osd
  crimson::net::MessengerRef cluster_msgr;
  // talk with client/mon/mgr
  crimson::net::MessengerRef public_msgr;

  // HB Messengers
  crimson::net::MessengerRef hb_front_msgr;
  crimson::net::MessengerRef hb_back_msgr;

  std::unique_ptr<crimson::mon::Client> monc;
  std::unique_ptr<crimson::mgr::Client> mgrc;

  // TODO: use a wrapper for ObjectStore
  OSDMapService::cached_map_t osdmap;
  crimson::os::FuturizedStore& store;

  /// _first_ epoch we were marked up (after this process started)
  epoch_t boot_epoch = 0;
  //< epoch we last did a bind to new ip:ports
  epoch_t bind_epoch = 0;
  //< since when there is no more pending pg creates from mon
  epoch_t last_pg_create_epoch = 0;

  ceph::mono_time startup_time;

  OSDSuperblock superblock;

  // Dispatcher methods
  std::optional<seastar::future<>> ms_dispatch(crimson::net::ConnectionRef, MessageRef) final;
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) final;
  void ms_handle_remote_reset(crimson::net::ConnectionRef conn) final;

  // mgr::WithStats methods
  // pg statistics including osd ones
  osd_stat_t osd_stat;
  uint32_t osd_stat_seq = 0;
  void update_stats();
  seastar::future<MessageURef> get_stats() const final;

  // AuthHandler methods
  void handle_authentication(const EntityName& name,
			     const AuthCapsInfo& caps) final;

  crimson::osd::PGShardManager pg_shard_manager;

  std::unique_ptr<Heartbeat> heartbeat;
  seastar::timer<seastar::lowres_clock> tick_timer;

  // admin-socket
  seastar::lw_shared_ptr<crimson::admin::AdminSocket> asok;

public:
  OSD(int id, uint32_t nonce,
      seastar::abort_source& abort_source,
      crimson::os::FuturizedStore& store,
      crimson::net::MessengerRef cluster_msgr,
      crimson::net::MessengerRef client_msgr,
      crimson::net::MessengerRef hb_front_msgr,
      crimson::net::MessengerRef hb_back_msgr);
  ~OSD() final;

  seastar::future<> open_meta_coll();
  static seastar::future<OSDMeta> open_or_create_meta_coll(
    crimson::os::FuturizedStore &store
  );
  static seastar::future<> mkfs(
    crimson::os::FuturizedStore &store,
    unsigned whoami,
    uuid_d osd_uuid,
    uuid_d cluster_fsid,
    std::string osdspec_affinity);

  seastar::future<> start();
  seastar::future<> stop();

  void dump_status(Formatter*) const;
  void print(std::ostream&) const;

  /// @return the seq id of the pg stats being sent
  uint64_t send_pg_stats();

private:
  static seastar::future<> _write_superblock(
    crimson::os::FuturizedStore &store,
    OSDMeta meta,
    OSDSuperblock superblock);
  static seastar::future<> _write_key_meta(
    crimson::os::FuturizedStore &store
  );
  seastar::future<> start_boot();
  seastar::future<> _preboot(version_t oldest_osdmap, version_t newest_osdmap);
  seastar::future<> _send_boot();
  seastar::future<> _add_me_to_crush();

  seastar::future<> osdmap_subscribe(version_t epoch, bool force_request);

  void write_superblock(ceph::os::Transaction& t);
  seastar::future<> read_superblock();

  bool require_mon_peer(crimson::net::Connection *conn, Ref<Message> m);

  seastar::future<> handle_osd_map(crimson::net::ConnectionRef conn,
                                   Ref<MOSDMap> m);
  seastar::future<> handle_pg_create(crimson::net::ConnectionRef conn,
				     Ref<MOSDPGCreate2> m);
  seastar::future<> handle_osd_op(crimson::net::ConnectionRef conn,
				  Ref<MOSDOp> m);
  seastar::future<> handle_rep_op(crimson::net::ConnectionRef conn,
				  Ref<MOSDRepOp> m);
  seastar::future<> handle_rep_op_reply(crimson::net::ConnectionRef conn,
					Ref<MOSDRepOpReply> m);
  seastar::future<> handle_peering_op(crimson::net::ConnectionRef conn,
				      Ref<MOSDPeeringOp> m);
  seastar::future<> handle_recovery_subreq(crimson::net::ConnectionRef conn,
					   Ref<MOSDFastDispatchOp> m);
  seastar::future<> handle_scrub(crimson::net::ConnectionRef conn,
				 Ref<MOSDScrub2> m);
  seastar::future<> handle_mark_me_down(crimson::net::ConnectionRef conn,
					Ref<MOSDMarkMeDown> m);

  seastar::future<> committed_osd_maps(version_t first,
                                       version_t last,
                                       Ref<MOSDMap> m);

  seastar::future<> check_osdmap_features();

  seastar::future<> handle_command(crimson::net::ConnectionRef conn,
				   Ref<MCommand> m);
  seastar::future<> start_asok_admin();
  seastar::future<> handle_update_log_missing(crimson::net::ConnectionRef conn,
                                              Ref<MOSDPGUpdateLogMissing> m);
  seastar::future<> handle_update_log_missing_reply(
    crimson::net::ConnectionRef conn,
    Ref<MOSDPGUpdateLogMissingReply> m);
public:
  auto &get_pg_shard_manager() {
    return pg_shard_manager;
  }
  ShardServices &get_shard_services() {
    return pg_shard_manager.get_shard_services();
  }

private:
  crimson::common::Gated gate;

  seastar::promise<> stop_acked;
  void got_stop_ack() {
    stop_acked.set_value();
  }
  seastar::future<> prepare_to_stop();
  bool should_restart() const;
  seastar::future<> restart();
  seastar::future<> shutdown();
  seastar::future<> update_heartbeat_peers();
  friend class PGAdvanceMap;

public:
  seastar::future<> send_beacon();

private:
  LogClient log_client;
  LogChannelRef clog;
};

inline std::ostream& operator<<(std::ostream& out, const OSD& osd) {
  osd.print(out);
  return out;
}

}
