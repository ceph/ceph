// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd.h"

#include <sys/utsname.h>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/join.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/timer.hh>

#include "common/pick_address.h"
#include "include/util.h"

#include "messages/MCommand.h"
#include "messages/MOSDBeacon.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDPeeringOp.h"
#include "messages/MOSDPGCreate2.h"
#include "messages/MOSDPGUpdateLogMissing.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDScrub2.h"
#include "messages/MPGStats.h"

#include "os/Transaction.h"
#include "osd/ClassHandler.h"
#include "osd/OSDCap.h"
#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"

#include "crimson/admin/osd_admin.h"
#include "crimson/admin/pg_commands.h"
#include "crimson/common/buffer_io.h"
#include "crimson/common/exception.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/heartbeat.h"
#include "crimson/osd/osd_meta.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/pg_meta.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/osd_operation_external_tracking.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
  static constexpr int TICK_INTERVAL = 1;
}

using std::make_unique;
using std::map;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

using crimson::common::local_conf;
using crimson::os::FuturizedStore;

namespace crimson::osd {

OSD::OSD(int id, uint32_t nonce,
	 seastar::abort_source& abort_source,
         crimson::os::FuturizedStore& store,
         crimson::net::MessengerRef cluster_msgr,
         crimson::net::MessengerRef public_msgr,
         crimson::net::MessengerRef hb_front_msgr,
         crimson::net::MessengerRef hb_back_msgr)
  : whoami{id},
    nonce{nonce},
    abort_source{abort_source},
    // do this in background
    beacon_timer{[this] { (void)send_beacon(); }},
    cluster_msgr{cluster_msgr},
    public_msgr{public_msgr},
    hb_front_msgr{hb_front_msgr},
    hb_back_msgr{hb_back_msgr},
    monc{new crimson::mon::Client{*public_msgr, *this}},
    mgrc{new crimson::mgr::Client{*public_msgr, *this}},
    store{store},
    // do this in background -- continuation rearms timer when complete
    tick_timer{[this] {
      std::ignore = update_heartbeat_peers(
      ).then([this] {
	update_stats();
	tick_timer.arm(
	  std::chrono::seconds(TICK_INTERVAL));
      });
    }},
    asok{seastar::make_lw_shared<crimson::admin::AdminSocket>()},
    log_client(cluster_msgr.get(), LogClient::NO_FLAGS),
    clog(log_client.create_channel())
{
  for (auto msgr : {std::ref(cluster_msgr), std::ref(public_msgr),
                    std::ref(hb_front_msgr), std::ref(hb_back_msgr)}) {
    msgr.get()->set_auth_server(monc.get());
    msgr.get()->set_auth_client(monc.get());
  }

  if (local_conf()->osd_open_classes_on_start) {
    const int r = ClassHandler::get_instance().open_all_classes();
    if (r) {
      logger().warn("{} warning: got an error loading one or more classes: {}",
                    __func__, cpp_strerror(r));
    }
  }
  logger().info("{}: nonce is {}", __func__, nonce);
  monc->set_log_client(&log_client);
  clog->set_log_to_monitors(true);
}

OSD::~OSD() = default;

namespace {
// Initial features in new superblock.
// Features here are also automatically upgraded
CompatSet get_osd_initial_compat_set()
{
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_OLOC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BIGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_HINTS);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGMETA);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_MISSING);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_FASTINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_RECOVERY_DELETES);
  return CompatSet(ceph_osd_feature_compat,
                   ceph_osd_feature_ro_compat,
                   ceph_osd_feature_incompat);
}
}

seastar::future<> OSD::open_meta_coll()
{
  return store.open_collection(
    coll_t::meta()
  ).then([this](auto ch) {
    pg_shard_manager.init_meta_coll(ch, store);
    return seastar::now();
  });
}

seastar::future<OSDMeta> OSD::open_or_create_meta_coll(FuturizedStore &store)
{
  return store.open_collection(coll_t::meta()).then([&store](auto ch) {
    if (!ch) {
      return store.create_new_collection(
	coll_t::meta()
      ).then([&store](auto ch) {
	return OSDMeta(ch, store);
      });
    } else {
      return seastar::make_ready_future<OSDMeta>(ch, store);
    }
  });
}

seastar::future<> OSD::mkfs(
  FuturizedStore &store,
  unsigned whoami,
  uuid_d osd_uuid,
  uuid_d cluster_fsid,
  std::string osdspec_affinity)
{
  return store.start().then([&store, osd_uuid] {
    return store.mkfs(osd_uuid).handle_error(
      crimson::stateful_ec::handle([] (const auto& ec) {
        logger().error("error creating empty object store in {}: ({}) {}",
                       local_conf().get_val<std::string>("osd_data"),
                       ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([&store] {
    return store.mount().handle_error(
      crimson::stateful_ec::handle([](const auto& ec) {
        logger().error("error mounting object store in {}: ({}) {}",
                       local_conf().get_val<std::string>("osd_data"),
                       ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([&store] {
    return open_or_create_meta_coll(store);
  }).then([&store, whoami, cluster_fsid](auto meta_coll) {
    OSDSuperblock superblock;
    superblock.cluster_fsid = cluster_fsid;
    superblock.osd_fsid = store.get_fsid();
    superblock.whoami = whoami;
    superblock.compat_features = get_osd_initial_compat_set();
    return _write_superblock(
      store, std::move(meta_coll), std::move(superblock));
  }).then([&store, cluster_fsid] {
    return store.write_meta("ceph_fsid", cluster_fsid.to_string());
  }).then([&store] {
    return store.write_meta("magic", CEPH_OSD_ONDISK_MAGIC);
  }).then([&store, whoami] {
    return store.write_meta("whoami", std::to_string(whoami));
  }).then([&store] {
    return _write_key_meta(store);
  }).then([&store, osdspec_affinity=std::move(osdspec_affinity)] {
    return store.write_meta("osdspec_affinity", osdspec_affinity);
  }).then([&store] {
    return store.write_meta("ready", "ready");
  }).then([&store, whoami, cluster_fsid] {
    fmt::print("created object store {} for osd.{} fsid {}\n",
               local_conf().get_val<std::string>("osd_data"),
               whoami, cluster_fsid);
    return store.umount();
  }).then([&store] {
    return store.stop();
  });
}

seastar::future<> OSD::_write_superblock(
  FuturizedStore &store,
  OSDMeta meta_coll,
  OSDSuperblock superblock)
{
  return seastar::do_with(
    std::move(meta_coll),
    std::move(superblock),
    [&store](auto &meta_coll, auto &superblock) {
      return meta_coll.load_superblock(
      ).safe_then([&superblock](OSDSuperblock&& sb) {
	if (sb.cluster_fsid != superblock.cluster_fsid) {
	  logger().error("provided cluster fsid {} != superblock's {}",
			 sb.cluster_fsid, superblock.cluster_fsid);
	  throw std::invalid_argument("mismatched fsid");
	}
	if (sb.whoami != superblock.whoami) {
	  logger().error("provided osd id {} != superblock's {}",
			 sb.whoami, superblock.whoami);
	  throw std::invalid_argument("mismatched osd id");
	}
      }).handle_error(
	crimson::ct_error::enoent::handle([&store, &meta_coll, &superblock] {
	  // meta collection does not yet, create superblock
	  logger().info(
	    "{} writing superblock cluster_fsid {} osd_fsid {}",
	    "_write_superblock",
	    superblock.cluster_fsid,
	    superblock.osd_fsid);
	  ceph::os::Transaction t;
	  meta_coll.create(t);
	  meta_coll.store_superblock(t, superblock);
	  logger().debug("OSD::_write_superblock: do_transaction...");
	  return store.do_transaction(
	    meta_coll.collection(),
	    std::move(t));
	}),
	crimson::ct_error::assert_all("_write_superbock error")
      );
    });
}

// this `to_string` sits in the `crimson::osd` namespace, so we don't brake
// the language rule on not overloading in `std::`.
static std::string to_string(const seastar::temporary_buffer<char>& temp_buf)
{
  return {temp_buf.get(), temp_buf.size()};
}

seastar::future<> OSD::_write_key_meta(FuturizedStore &store)
{

  if (auto key = local_conf().get_val<std::string>("key"); !std::empty(key)) {
    return store.write_meta("osd_key", key);
  } else if (auto keyfile = local_conf().get_val<std::string>("keyfile");
             !std::empty(keyfile)) {
    return read_file(keyfile).then([&store](const auto& temp_buf) {
      // it's on a truly cold path, so don't worry about memcpy.
      return store.write_meta("osd_key", to_string(temp_buf));
    }).handle_exception([keyfile] (auto ep) {
      logger().error("_write_key_meta: failed to handle keyfile {}: {}",
                     keyfile, ep);
      ceph_abort();
    });
  } else {
    return seastar::now();
  }
}

namespace {
  entity_addrvec_t pick_addresses(int what) {
    entity_addrvec_t addrs;
    crimson::common::CephContext cct;
    // we're interested solely in v2; crimson doesn't do v1
    const auto flags = what | CEPH_PICK_ADDRESS_MSGR2;
    if (int r = ::pick_addresses(&cct, flags, &addrs, -1); r < 0) {
      throw std::runtime_error("failed to pick address");
    }
    for (auto addr : addrs.v) {
      logger().info("picked address {}", addr);
    }
    return addrs;
  }
  std::pair<entity_addrvec_t, bool>
  replace_unknown_addrs(entity_addrvec_t maybe_unknowns,
                        const entity_addrvec_t& knowns) {
    bool changed = false;
    auto maybe_replace = [&](entity_addr_t addr) {
      if (!addr.is_blank_ip()) {
        return addr;
      }
      for (auto& b : knowns.v) {
        if (addr.get_family() == b.get_family()) {
          auto a = b;
          a.set_nonce(addr.get_nonce());
          a.set_type(addr.get_type());
          a.set_port(addr.get_port());
          changed = true;
          return a;
        }
      }
      throw std::runtime_error("failed to replace unknown address");
    };
    entity_addrvec_t replaced;
    std::transform(maybe_unknowns.v.begin(),
                   maybe_unknowns.v.end(),
                   std::back_inserter(replaced.v),
                   maybe_replace);
    return {replaced, changed};
  }
}

seastar::future<> OSD::start()
{
  logger().info("start");

  startup_time = ceph::mono_clock::now();

  return pg_shard_manager.start(
    whoami, *cluster_msgr,
    *public_msgr, *monc, *mgrc, store
  ).then([this] {
    heartbeat.reset(new Heartbeat{
	whoami, get_shard_services(),
	*monc, *hb_front_msgr, *hb_back_msgr});
    return store.start();
  }).then([this] {
    return store.mount().handle_error(
      crimson::stateful_ec::handle([] (const auto& ec) {
        logger().error("error mounting object store in {}: ({}) {}",
                       local_conf().get_val<std::string>("osd_data"),
                       ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([this] {
    return open_meta_coll();
  }).then([this] {
    return pg_shard_manager.get_meta_coll().load_superblock(
    ).handle_error(
      crimson::ct_error::assert_all("open_meta_coll error")
    );
  }).then([this](OSDSuperblock&& sb) {
    superblock = std::move(sb);
    pg_shard_manager.set_superblock(sb);
    return pg_shard_manager.get_local_map(superblock.current_epoch);
  }).then([this](OSDMapService::local_cached_map_t&& map) {
    osdmap = make_local_shared_foreign(OSDMapService::local_cached_map_t(map));
    return pg_shard_manager.update_map(std::move(map));
  }).then([this] {
    pg_shard_manager.got_map(osdmap->get_epoch());
    bind_epoch = osdmap->get_epoch();
    return pg_shard_manager.load_pgs();
  }).then([this] {

    uint64_t osd_required =
      CEPH_FEATURE_UID |
      CEPH_FEATURE_PGID64 |
      CEPH_FEATURE_OSDENC;
    using crimson::net::SocketPolicy;

    public_msgr->set_default_policy(SocketPolicy::stateless_server(0));
    public_msgr->set_policy(entity_name_t::TYPE_MON,
                            SocketPolicy::lossy_client(osd_required));
    public_msgr->set_policy(entity_name_t::TYPE_MGR,
                            SocketPolicy::lossy_client(osd_required));
    public_msgr->set_policy(entity_name_t::TYPE_OSD,
                            SocketPolicy::stateless_server(0));

    cluster_msgr->set_default_policy(SocketPolicy::stateless_server(0));
    cluster_msgr->set_policy(entity_name_t::TYPE_MON,
                             SocketPolicy::lossy_client(0));
    cluster_msgr->set_policy(entity_name_t::TYPE_OSD,
                             SocketPolicy::lossless_peer(osd_required));
    cluster_msgr->set_policy(entity_name_t::TYPE_CLIENT,
                             SocketPolicy::stateless_server(0));

    crimson::net::dispatchers_t dispatchers{this, monc.get(), mgrc.get()};
    return seastar::when_all_succeed(
      cluster_msgr->bind(pick_addresses(CEPH_PICK_ADDRESS_CLUSTER))
        .safe_then([this, dispatchers]() mutable {
	  return cluster_msgr->start(dispatchers);
        }, crimson::net::Messenger::bind_ertr::all_same_way(
            [] (const std::error_code& e) {
          logger().error("cluster messenger bind(): {}", e);
          ceph_abort();
        })),
      public_msgr->bind(pick_addresses(CEPH_PICK_ADDRESS_PUBLIC))
        .safe_then([this, dispatchers]() mutable {
	  return public_msgr->start(dispatchers);
        }, crimson::net::Messenger::bind_ertr::all_same_way(
            [] (const std::error_code& e) {
          logger().error("public messenger bind(): {}", e);
          ceph_abort();
        })));
  }).then_unpack([this] {
    return seastar::when_all_succeed(monc->start(),
                                     mgrc->start());
  }).then_unpack([this] {
    return _add_me_to_crush();
  }).then([this] {
    monc->sub_want("osd_pg_creates", last_pg_create_epoch, 0);
    monc->sub_want("mgrmap", 0, 0);
    monc->sub_want("osdmap", 0, 0);
    return monc->renew_subs();
  }).then([this] {
    if (auto [addrs, changed] =
        replace_unknown_addrs(cluster_msgr->get_myaddrs(),
                              public_msgr->get_myaddrs()); changed) {
      logger().debug("replacing unkwnown addrs of cluster messenger");
      return cluster_msgr->set_myaddrs(addrs);
    } else {
      return seastar::now();
    }
  }).then([this] {
    return heartbeat->start(pick_addresses(CEPH_PICK_ADDRESS_PUBLIC),
                            pick_addresses(CEPH_PICK_ADDRESS_CLUSTER));
  }).then([this] {
    // create the admin-socket server, and the objects that register
    // to handle incoming commands
    return start_asok_admin();
  }).then([this] {
    return log_client.set_fsid(monc->get_fsid());
  }).then([this] {
    return start_boot();
  });
}

seastar::future<> OSD::start_boot()
{
  pg_shard_manager.set_preboot();
  return monc->get_version("osdmap").then([this](auto&& ret) {
    auto [newest, oldest] = ret;
    return _preboot(oldest, newest);
  });
}

seastar::future<> OSD::_preboot(version_t oldest, version_t newest)
{
  logger().info("osd.{}: _preboot", whoami);
  if (osdmap->get_epoch() == 0) {
    logger().info("waiting for initial osdmap");
  } else if (osdmap->is_destroyed(whoami)) {
    logger().warn("osdmap says I am destroyed");
    // provide a small margin so we don't livelock seeing if we
    // un-destroyed ourselves.
    if (osdmap->get_epoch() > newest - 1) {
      throw std::runtime_error("i am destroyed");
    }
  } else if (osdmap->is_noup(whoami)) {
    logger().warn("osdmap NOUP flag is set, waiting for it to clear");
  } else if (!osdmap->test_flag(CEPH_OSDMAP_SORTBITWISE)) {
    logger().error("osdmap SORTBITWISE OSDMap flag is NOT set; please set it");
  } else if (osdmap->require_osd_release < ceph_release_t::octopus) {
    logger().error("osdmap require_osd_release < octopus; please upgrade to octopus");
  } else if (false) {
    // TODO: update mon if current fullness state is different from osdmap
  } else if (version_t n = local_conf()->osd_map_message_max;
             osdmap->get_epoch() >= oldest - 1 &&
             osdmap->get_epoch() + n > newest) {
    return _send_boot();
  }
  // get all the latest maps
  if (osdmap->get_epoch() + 1 >= oldest) {
    return get_shard_services().osdmap_subscribe(osdmap->get_epoch() + 1, false);
  } else {
    return get_shard_services().osdmap_subscribe(oldest - 1, true);
  }
}

seastar::future<> OSD::_send_boot()
{
  pg_shard_manager.set_booting();

  entity_addrvec_t public_addrs = public_msgr->get_myaddrs();
  entity_addrvec_t cluster_addrs = cluster_msgr->get_myaddrs();
  entity_addrvec_t hb_back_addrs = heartbeat->get_back_addrs();
  entity_addrvec_t hb_front_addrs = heartbeat->get_front_addrs();
  if (cluster_msgr->set_addr_unknowns(public_addrs)) {
    cluster_addrs = cluster_msgr->get_myaddrs();
  }
  if (heartbeat->get_back_msgr().set_addr_unknowns(cluster_addrs)) {
    hb_back_addrs = heartbeat->get_back_addrs();
  }
  if (heartbeat->get_front_msgr().set_addr_unknowns(public_addrs)) {
    hb_front_addrs = heartbeat->get_front_addrs();
  }
  logger().info("hb_back_msgr: {}", hb_back_addrs);
  logger().info("hb_front_msgr: {}", hb_front_addrs);
  logger().info("cluster_msgr: {}", cluster_addrs);

  auto m = crimson::make_message<MOSDBoot>(superblock,
                                  osdmap->get_epoch(),
                                  boot_epoch,
                                  hb_back_addrs,
                                  hb_front_addrs,
                                  cluster_addrs,
                                  CEPH_FEATURES_ALL);
  collect_sys_info(&m->metadata, NULL);
  return monc->send_message(std::move(m));
}

seastar::future<> OSD::_add_me_to_crush()
{
  if (!local_conf().get_val<bool>("osd_crush_update_on_start")) {
    return seastar::now();
  }
  auto get_weight = [this] {
    if (auto w = local_conf().get_val<double>("osd_crush_initial_weight");
	w >= 0) {
      return seastar::make_ready_future<double>(w);
    } else {
       return store.stat().then([](auto st) {
         auto total = st.total;
	 return seastar::make_ready_future<double>(
           std::max(.00001,
		    double(total) / double(1ull << 40))); // TB
       });
    }
  };
  return get_weight().then([this](auto weight) {
    const crimson::crush::CrushLocation loc{make_unique<CephContext>().get()};
    logger().info("{} crush location is {}", __func__, loc);
    string cmd = fmt::format(R"({{
      "prefix": "osd crush create-or-move",
      "id": {},
      "weight": {:.4f},
      "args": [{}]
    }})", whoami, weight, loc);
    return monc->run_command(std::move(cmd), {});
  }).then([](auto&& command_result) {
    [[maybe_unused]] auto [code, message, out] = std::move(command_result);
    if (code) {
      logger().warn("fail to add to crush: {} ({})", message, code);
      throw std::runtime_error("fail to add to crush");
    } else {
      logger().info("added to crush: {}", message);
    }
    return seastar::now();
  });
}

seastar::future<> OSD::handle_command(crimson::net::ConnectionRef conn,
				      Ref<MCommand> m)
{
  return asok->handle_command(conn, std::move(m));
}

/*
  The OSD's Admin Socket object created here has two servers (i.e. - blocks of commands
  to handle) registered to it:
  - OSD's specific commands are handled by the OSD object;
  - there are some common commands registered to be directly handled by the AdminSocket object
    itself.
*/
seastar::future<> OSD::start_asok_admin()
{
  auto asok_path = local_conf().get_val<std::string>("admin_socket");
  using namespace crimson::admin;
  return asok->start(asok_path).then([this] {
    asok->register_admin_commands();
    asok->register_command(make_asok_hook<OsdStatusHook>(std::as_const(*this)));
    asok->register_command(make_asok_hook<SendBeaconHook>(*this));
    asok->register_command(make_asok_hook<FlushPgStatsHook>(*this));
    asok->register_command(
      make_asok_hook<DumpPGStateHistory>(std::as_const(pg_shard_manager)));
    asok->register_command(make_asok_hook<DumpMetricsHook>());
    asok->register_command(make_asok_hook<DumpPerfCountersHook>());
    asok->register_command(make_asok_hook<InjectDataErrorHook>(get_shard_services()));
    asok->register_command(make_asok_hook<InjectMDataErrorHook>(get_shard_services()));
    // PG commands
    asok->register_command(make_asok_hook<pg::QueryCommand>(*this));
    asok->register_command(make_asok_hook<pg::MarkUnfoundLostCommand>(*this));
    // ops commands
    asok->register_command(
      make_asok_hook<DumpInFlightOpsHook>(
	std::as_const(pg_shard_manager)));
    asok->register_command(
      make_asok_hook<DumpHistoricOpsHook>(
	std::as_const(get_shard_services().get_registry())));
    asok->register_command(
      make_asok_hook<DumpSlowestHistoricOpsHook>(
	std::as_const(get_shard_services().get_registry())));
    asok->register_command(
      make_asok_hook<DumpRecoveryReservationsHook>(get_shard_services()));
  });
}

seastar::future<> OSD::stop()
{
  logger().info("stop");
  beacon_timer.cancel();
  tick_timer.cancel();
  // see also OSD::shutdown()
  return prepare_to_stop().then([this] {
    pg_shard_manager.set_stopping();
    logger().debug("prepared to stop");
    public_msgr->stop();
    cluster_msgr->stop();
    auto gate_close_fut = gate.close();
    return asok->stop().then([this] {
      return heartbeat->stop();
    }).then([this] {
      return pg_shard_manager.stop_registries();
    }).then([this] {
      return store.umount();
    }).then([this] {
      return store.stop();
    }).then([this] {
      return pg_shard_manager.stop_pgs();
    }).then([this] {
      return monc->stop();
    }).then([this] {
      return mgrc->stop();
    }).then([this] {
      return pg_shard_manager.stop();
    }).then([fut=std::move(gate_close_fut)]() mutable {
      return std::move(fut);
    }).then([this] {
      return when_all_succeed(
	  public_msgr->shutdown(),
	  cluster_msgr->shutdown()).discard_result();
    }).handle_exception([](auto ep) {
      logger().error("error while stopping osd: {}", ep);
    });
  });
}

void OSD::dump_status(Formatter* f) const
{
  f->dump_stream("cluster_fsid") << superblock.cluster_fsid;
  f->dump_stream("osd_fsid") << superblock.osd_fsid;
  f->dump_unsigned("whoami", superblock.whoami);
  f->dump_string("state", pg_shard_manager.get_osd_state_string());
  f->dump_unsigned("oldest_map", superblock.oldest_map);
  f->dump_unsigned("newest_map", superblock.newest_map);
  f->dump_unsigned("num_pgs", pg_shard_manager.get_num_pgs());
}

void OSD::print(std::ostream& out) const
{
  out << "{osd." << superblock.whoami << " "
      << superblock.osd_fsid << " [" << superblock.oldest_map
      << "," << superblock.newest_map << "] " << pg_shard_manager.get_num_pgs()
      << " pgs}";
}

std::optional<seastar::future<>>
OSD::ms_dispatch(crimson::net::ConnectionRef conn, MessageRef m)
{
  if (pg_shard_manager.is_stopping()) {
    return {};
  }
  // XXX: we're assuming the `switch` part is executed immediately, and thus
  // we won't smash the stack. Taking into account how `seastar::with_gate`
  // is currently implemented, this seems to be the case (Summer 2022).
  bool dispatched = true;
  gate.dispatch_in_background(__func__, *this, [this, conn=std::move(conn),
                                                m=std::move(m), &dispatched] {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_MAP:
      return handle_osd_map(conn, boost::static_pointer_cast<MOSDMap>(m));
    case CEPH_MSG_OSD_OP:
      return handle_osd_op(conn, boost::static_pointer_cast<MOSDOp>(m));
    case MSG_OSD_PG_CREATE2:
      return handle_pg_create(
	conn, boost::static_pointer_cast<MOSDPGCreate2>(m));
      return seastar::now();
    case MSG_COMMAND:
      return handle_command(conn, boost::static_pointer_cast<MCommand>(m));
    case MSG_OSD_MARK_ME_DOWN:
      return handle_mark_me_down(conn, boost::static_pointer_cast<MOSDMarkMeDown>(m));
    case MSG_OSD_PG_PULL:
      [[fallthrough]];
    case MSG_OSD_PG_PUSH:
      [[fallthrough]];
    case MSG_OSD_PG_PUSH_REPLY:
      [[fallthrough]];
    case MSG_OSD_PG_RECOVERY_DELETE:
      [[fallthrough]];
    case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
      [[fallthrough]];
    case MSG_OSD_PG_SCAN:
      [[fallthrough]];
    case MSG_OSD_PG_BACKFILL:
      [[fallthrough]];
    case MSG_OSD_PG_BACKFILL_REMOVE:
      return handle_recovery_subreq(conn, boost::static_pointer_cast<MOSDFastDispatchOp>(m));
    case MSG_OSD_PG_LEASE:
      [[fallthrough]];
    case MSG_OSD_PG_LEASE_ACK:
      [[fallthrough]];
    case MSG_OSD_PG_NOTIFY2:
      [[fallthrough]];
    case MSG_OSD_PG_INFO2:
      [[fallthrough]];
    case MSG_OSD_PG_QUERY2:
      [[fallthrough]];
    case MSG_OSD_BACKFILL_RESERVE:
      [[fallthrough]];
    case MSG_OSD_RECOVERY_RESERVE:
      [[fallthrough]];
    case MSG_OSD_PG_LOG:
      return handle_peering_op(conn, boost::static_pointer_cast<MOSDPeeringOp>(m));
    case MSG_OSD_REPOP:
      return handle_rep_op(conn, boost::static_pointer_cast<MOSDRepOp>(m));
    case MSG_OSD_REPOPREPLY:
      return handle_rep_op_reply(conn, boost::static_pointer_cast<MOSDRepOpReply>(m));
    case MSG_OSD_SCRUB2:
      return handle_scrub(conn, boost::static_pointer_cast<MOSDScrub2>(m));
    case MSG_OSD_PG_UPDATE_LOG_MISSING:
      return handle_update_log_missing(conn, boost::static_pointer_cast<
        MOSDPGUpdateLogMissing>(m));
    case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
      return handle_update_log_missing_reply(conn, boost::static_pointer_cast<
        MOSDPGUpdateLogMissingReply>(m));
    default:
      dispatched = false;
      return seastar::now();
    }
  });
  return (dispatched ? std::make_optional(seastar::now()) : std::nullopt);
}

void OSD::ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace)
{
  // TODO: cleanup the session attached to this connection
  logger().warn("ms_handle_reset");
}

void OSD::ms_handle_remote_reset(crimson::net::ConnectionRef conn)
{
  logger().warn("ms_handle_remote_reset");
}

void OSD::handle_authentication(const EntityName& name,
				const AuthCapsInfo& caps_info)
{
  // TODO: store the parsed cap and associate it with the connection
  if (caps_info.allow_all) {
    logger().debug("{} {} has all caps", __func__, name);
    return;
  }
  if (caps_info.caps.length() > 0) {
    auto p = caps_info.caps.cbegin();
    string str;
    try {
      decode(str, p);
    } catch (ceph::buffer::error& e) {
      logger().warn("{} {} failed to decode caps string", __func__, name);
      return;
    }
    OSDCap caps;
    if (caps.parse(str)) {
      logger().debug("{} {} has caps {}", __func__, name, str);
    } else {
      logger().warn("{} {} failed to parse caps {}", __func__, name, str);
    }
  }
}

void OSD::update_stats()
{
  osd_stat_seq++;
  osd_stat.up_from = get_shard_services().get_up_epoch();
  osd_stat.hb_peers = heartbeat->get_peers();
  osd_stat.seq = (
    static_cast<uint64_t>(get_shard_services().get_up_epoch()) << 32
  ) | osd_stat_seq;
  gate.dispatch_in_background("statfs", *this, [this] {
    (void) store.stat().then([this](store_statfs_t&& st) {
      osd_stat.statfs = st;
    });
  });
}

seastar::future<MessageURef> OSD::get_stats() const
{
  // MPGStats::had_map_for is not used since PGMonitor was removed
  auto m = crimson::make_message<MPGStats>(monc->get_fsid(), osdmap->get_epoch());
  m->osd_stat = osd_stat;
  return pg_shard_manager.get_pg_stats(
  ).then([m=std::move(m)](auto &&stats) mutable {
    m->pg_stat = std::move(stats);
    return seastar::make_ready_future<MessageURef>(std::move(m));
  });
}

uint64_t OSD::send_pg_stats()
{
  // mgr client sends the report message in background
  mgrc->report();
  return osd_stat.seq;
}

bool OSD::require_mon_peer(crimson::net::Connection *conn, Ref<Message> m)
{
  if (!conn->peer_is_mon()) {
    logger().info("{} received from non-mon {}, {}",
		  __func__,
		  conn->get_peer_addr(),
		  *m);
    return false;
  }
  return true;
}

seastar::future<> OSD::handle_osd_map(crimson::net::ConnectionRef conn,
                                      Ref<MOSDMap> m)
{
  logger().info("handle_osd_map {}", *m);
  if (m->fsid != superblock.cluster_fsid) {
    logger().warn("fsid mismatched");
    return seastar::now();
  }
  if (pg_shard_manager.is_initializing()) {
    logger().warn("i am still initializing");
    return seastar::now();
  }

  const auto first = m->get_first();
  const auto last = m->get_last();
  logger().info("handle_osd_map epochs [{}..{}], i have {}, src has [{}..{}]",
                first, last, superblock.newest_map, m->oldest_map, m->newest_map);
  // make sure there is something new, here, before we bother flushing
  // the queues and such
  if (last <= superblock.newest_map) {
    return seastar::now();
  }
  // missing some?
  bool skip_maps = false;
  epoch_t start = superblock.newest_map + 1;
  if (first > start) {
    logger().info("handle_osd_map message skips epochs {}..{}",
                  start, first - 1);
    if (m->oldest_map <= start) {
      return get_shard_services().osdmap_subscribe(start, false);
    }
    // always try to get the full range of maps--as many as we can.  this
    //  1- is good to have
    //  2- is at present the only way to ensure that we get a *full* map as
    //     the first map!
    if (m->oldest_map < first) {
      return get_shard_services().osdmap_subscribe(m->oldest_map - 1, true);
    }
    skip_maps = true;
    start = first;
  }

  return seastar::do_with(ceph::os::Transaction{},
                          [=, this](auto& t) {
    return pg_shard_manager.store_maps(t, start, m).then([=, this, &t] {
      // even if this map isn't from a mon, we may have satisfied our subscription
      monc->sub_got("osdmap", last);
      if (!superblock.oldest_map || skip_maps) {
        superblock.oldest_map = first;
      }
      superblock.newest_map = last;
      superblock.current_epoch = last;

      // note in the superblock that we were clean thru the prior epoch
      if (boot_epoch && boot_epoch >= superblock.mounted) {
        superblock.mounted = boot_epoch;
        superblock.clean_thru = last;
      }
      pg_shard_manager.get_meta_coll().store_superblock(t, superblock);
      pg_shard_manager.set_superblock(superblock);
      logger().debug("OSD::handle_osd_map: do_transaction...");
      return store.do_transaction(
	pg_shard_manager.get_meta_coll().collection(),
	std::move(t));
    });
  }).then([=, this] {
    // TODO: write to superblock and commit the transaction
    return committed_osd_maps(start, last, m);
  });
}

seastar::future<> OSD::committed_osd_maps(version_t first,
                                          version_t last,
                                          Ref<MOSDMap> m)
{
  logger().info("osd.{}: committed_osd_maps({}, {})", whoami, first, last);
  // advance through the new maps
  return seastar::do_for_each(boost::make_counting_iterator(first),
                              boost::make_counting_iterator(last + 1),
                              [this](epoch_t cur) {
    return pg_shard_manager.get_local_map(
      cur
    ).then([this](OSDMapService::local_cached_map_t&& o) {
      osdmap = make_local_shared_foreign(OSDMapService::local_cached_map_t(o));
      return pg_shard_manager.update_map(std::move(o));
    }).then([this] {
      if (get_shard_services().get_up_epoch() == 0 &&
	  osdmap->is_up(whoami) &&
	  osdmap->get_addrs(whoami) == public_msgr->get_myaddrs()) {
	return pg_shard_manager.set_up_epoch(
	  osdmap->get_epoch()
	).then([this] {
	  if (!boot_epoch) {
	    boot_epoch = osdmap->get_epoch();
	  }
	});
      } else {
	return seastar::now();
      }
    });
  }).then([m, this] {
    if (osdmap->is_up(whoami)) {
      const auto up_from = osdmap->get_up_from(whoami);
      logger().info("osd.{}: map e {} marked me up: up_from {}, bind_epoch {}, state {}",
                    whoami, osdmap->get_epoch(), up_from, bind_epoch,
		    pg_shard_manager.get_osd_state_string());
      if (bind_epoch < up_from &&
          osdmap->get_addrs(whoami) == public_msgr->get_myaddrs() &&
          pg_shard_manager.is_booting()) {
        logger().info("osd.{}: activating...", whoami);
        pg_shard_manager.set_active();
        beacon_timer.arm_periodic(
          std::chrono::seconds(local_conf()->osd_beacon_report_interval));
	// timer continuation rearms when complete
        tick_timer.arm(
          std::chrono::seconds(TICK_INTERVAL));
      }
    } else {
      if (pg_shard_manager.is_prestop()) {
	got_stop_ack();
	return seastar::now();
      }
    }
    return check_osdmap_features().then([this] {
      // yay!
      return pg_shard_manager.broadcast_map_to_pgs(osdmap->get_epoch());
    });
  }).then([m, this] {
    if (pg_shard_manager.is_active()) {
      logger().info("osd.{}: now active", whoami);
      if (!osdmap->exists(whoami) ||
	  osdmap->is_stop(whoami)) {
        return shutdown();
      }
      if (should_restart()) {
        return restart();
      } else {
        return seastar::now();
      }
    } else if (pg_shard_manager.is_preboot()) {
      logger().info("osd.{}: now preboot", whoami);

      if (m->get_source().is_mon()) {
        return _preboot(m->oldest_map, m->newest_map);
      } else {
        logger().info("osd.{}: start_boot", whoami);
        return start_boot();
      }
    } else {
      logger().info("osd.{}: now {}", whoami,
		    pg_shard_manager.get_osd_state_string());
      // XXX
      return seastar::now();
    }
  });
}

seastar::future<> OSD::handle_osd_op(crimson::net::ConnectionRef conn,
                                     Ref<MOSDOp> m)
{
  (void) pg_shard_manager.start_pg_operation<ClientRequest>(
    get_shard_services(),
    conn,
    std::move(m));
  return seastar::now();
}

seastar::future<> OSD::handle_pg_create(crimson::net::ConnectionRef conn,
					Ref<MOSDPGCreate2> m)
{
  for (auto& [pgid, when] : m->pgs) {
    const auto &[created, created_stamp] = when;
    auto q = m->pg_extra.find(pgid);
    ceph_assert(q != m->pg_extra.end());
    auto& [history, pi] = q->second;
    logger().debug(
      "{}: {} e{} @{} "
      "history {} pi {}",
      __func__, pgid, created, created_stamp,
      history, pi);
    if (!pi.empty() &&
	m->epoch < pi.get_bounds().second) {
      logger().error(
        "got pg_create on {} epoch {}  "
        "unmatched past_intervals {} (history {})",
        pgid, m->epoch,
        pi, history);
    } else {
      std::ignore = pg_shard_manager.start_pg_operation<RemotePeeringEvent>(
	  conn,
	  pg_shard_t(),
	  pgid,
	  m->epoch,
	  m->epoch,
	  NullEvt(),
	  true,
	  new PGCreateInfo(pgid, m->epoch, history, pi, true));
    }
  }
  return seastar::now();
}

seastar::future<> OSD::handle_update_log_missing(
  crimson::net::ConnectionRef conn,
  Ref<MOSDPGUpdateLogMissing> m)
{
  m->decode_payload();
  (void) pg_shard_manager.start_pg_operation<LogMissingRequest>(
    std::move(conn),
    std::move(m));
  return seastar::now();
}

seastar::future<> OSD::handle_update_log_missing_reply(
  crimson::net::ConnectionRef conn,
  Ref<MOSDPGUpdateLogMissingReply> m)
{
  m->decode_payload();
  (void) pg_shard_manager.start_pg_operation<LogMissingRequestReply>(
    std::move(conn),
    std::move(m));
  return seastar::now();
}

seastar::future<> OSD::handle_rep_op(crimson::net::ConnectionRef conn,
				     Ref<MOSDRepOp> m)
{
  m->finish_decode();
  std::ignore = pg_shard_manager.start_pg_operation<RepRequest>(
    std::move(conn),
    std::move(m));
  return seastar::now();
}

seastar::future<> OSD::handle_rep_op_reply(crimson::net::ConnectionRef conn,
					   Ref<MOSDRepOpReply> m)
{
  spg_t pgid = m->get_spg();
  return pg_shard_manager.with_pg(
    pgid,
    [m=std::move(m)](auto &&pg) {
      if (pg) {
	m->finish_decode();
	pg->handle_rep_op_reply(*m);
      } else {
	logger().warn("stale reply: {}", *m);
      }
      return seastar::now();
    });
}

seastar::future<> OSD::handle_scrub(crimson::net::ConnectionRef conn,
				    Ref<MOSDScrub2> m)
{
  if (m->fsid != superblock.cluster_fsid) {
    logger().warn("fsid mismatched");
    return seastar::now();
  }
  return seastar::parallel_for_each(std::move(m->scrub_pgs),
    [m, conn, this](spg_t pgid) {
    pg_shard_t from_shard{static_cast<int>(m->get_source().num()),
                          pgid.shard};
    PeeringState::RequestScrub scrub_request{m->deep, m->repair};
    return pg_shard_manager.start_pg_operation<RemotePeeringEvent>(
      conn,
      from_shard,
      pgid,
      PGPeeringEvent{m->epoch, m->epoch, scrub_request}).second;
  });
}

seastar::future<> OSD::handle_mark_me_down(crimson::net::ConnectionRef conn,
					   Ref<MOSDMarkMeDown> m)
{
  if (pg_shard_manager.is_prestop()) {
    got_stop_ack();
  }
  return seastar::now();
}

seastar::future<> OSD::handle_recovery_subreq(crimson::net::ConnectionRef conn,
				   Ref<MOSDFastDispatchOp> m)
{
  std::ignore = pg_shard_manager.start_pg_operation<RecoverySubRequest>(
    conn, std::move(m));
  return seastar::now();
}

bool OSD::should_restart() const
{
  if (!osdmap->is_up(whoami)) {
    logger().info("map e {} marked osd.{} down",
                  osdmap->get_epoch(), whoami);
    return true;
  } else if (osdmap->get_addrs(whoami) != public_msgr->get_myaddrs()) {
    logger().error("map e {} had wrong client addr ({} != my {})",
                   osdmap->get_epoch(),
                   osdmap->get_addrs(whoami),
                   public_msgr->get_myaddrs());
    return true;
  } else if (osdmap->get_cluster_addrs(whoami) != cluster_msgr->get_myaddrs()) {
    logger().error("map e {} had wrong cluster addr ({} != my {})",
                   osdmap->get_epoch(),
                   osdmap->get_cluster_addrs(whoami),
                   cluster_msgr->get_myaddrs());
    return true;
  } else {
    return false;
  }
}

seastar::future<> OSD::restart()
{
  beacon_timer.cancel();
  tick_timer.cancel();
  return pg_shard_manager.set_up_epoch(
    0
  ).then([this] {
    bind_epoch = osdmap->get_epoch();
    // TODO: promote to shutdown if being marked down for multiple times
    // rebind messengers
    return start_boot();
  });
}

seastar::future<> OSD::shutdown()
{
  logger().info("shutting down per osdmap");
  abort_source.request_abort();
  return seastar::now();
}

seastar::future<> OSD::send_beacon()
{
  if (!pg_shard_manager.is_active()) {
    return seastar::now();
  }
  // FIXME: min lec should be calculated from pg_stat
  //        and should set m->pgs
  epoch_t min_last_epoch_clean = osdmap->get_epoch();
  auto m = crimson::make_message<MOSDBeacon>(osdmap->get_epoch(),
                                    min_last_epoch_clean,
                                    superblock.last_purged_snaps_scrub,
                                    local_conf()->osd_beacon_report_interval);
  return monc->send_message(std::move(m));
}

seastar::future<> OSD::update_heartbeat_peers()
{
  if (!pg_shard_manager.is_active()) {
    return seastar::now();;
  }

  pg_shard_manager.for_each_pgid([this](auto &pgid) {
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(pgid.pgid,
                                 &up, nullptr,
                                 &acting, nullptr);
    for (int osd : boost::join(up, acting)) {
      if (osd == CRUSH_ITEM_NONE || osd == whoami) {
        continue;
      } else {
        heartbeat->add_peer(osd, osdmap->get_epoch());
      }
    }
  });
  heartbeat->update_peers(whoami);
  return seastar::now();
}

seastar::future<> OSD::handle_peering_op(
  crimson::net::ConnectionRef conn,
  Ref<MOSDPeeringOp> m)
{
  const int from = m->get_source().num();
  logger().debug("handle_peering_op on {} from {}", m->get_spg(), from);
  std::unique_ptr<PGPeeringEvent> evt(m->get_event());
  (void) pg_shard_manager.start_pg_operation<RemotePeeringEvent>(
    conn,
    pg_shard_t{from, m->get_spg().shard},
    m->get_spg(),
    std::move(*evt));
  return seastar::now();
}

seastar::future<> OSD::check_osdmap_features()
{
  heartbeat->set_require_authorizer(true);
  return store.write_meta("require_osd_release",
                          stringify((int)osdmap->require_osd_release));
}

seastar::future<> OSD::prepare_to_stop()
{
  if (osdmap && osdmap->is_up(whoami)) {
    pg_shard_manager.set_prestop();
    const auto timeout =
      std::chrono::duration_cast<std::chrono::milliseconds>(
	std::chrono::duration<double>(
	  local_conf().get_val<double>("osd_mon_shutdown_timeout")));

    return seastar::with_timeout(
      seastar::timer<>::clock::now() + timeout,
      monc->send_message(
	  crimson::make_message<MOSDMarkMeDown>(
	    monc->get_fsid(),
	    whoami,
	    osdmap->get_addrs(whoami),
	    osdmap->get_epoch(),
	    true)).then([this] {
	return stop_acked.get_future();
      })
    ).handle_exception_type(
      [](seastar::timed_out_error&) {
      return seastar::now();
    });
  }
  return seastar::now();
}

}
