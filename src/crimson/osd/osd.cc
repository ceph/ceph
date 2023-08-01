// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd.h"

#include <sys/utsname.h>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/join.hpp>
#include <fmt/format.h>
#include <fmt/os.h>
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
#include "crimson/common/log.h"
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
#include "crimson/osd/osd_operations/ecrep_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/osd_operations/scrub_events.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/crush/CrushLocation.h"

SET_SUBSYS(osd);

namespace {
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
    pg_shard_manager{osd_singleton_state,
                     shard_services,
                     pg_to_shard_mappings},
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
  LOG_PREFIX(OSD::OSD);
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  for (auto msgr : {std::ref(cluster_msgr), std::ref(public_msgr),
                    std::ref(hb_front_msgr), std::ref(hb_back_msgr)}) {
    msgr.get()->set_auth_server(monc.get());
    msgr.get()->set_auth_client(monc.get());
  }

  if (local_conf()->osd_open_classes_on_start) {
    const int r = ClassHandler::get_instance().open_all_classes();
    if (r) {
      WARN("warning: got an error loading one or more classes: {}",
	   cpp_strerror(r));
    }
  }
  INFO("nonce is {}", nonce);
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
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return store.get_sharded_store().open_collection(
    coll_t::meta()
  ).then([this](auto ch) {
    pg_shard_manager.init_meta_coll(ch, store.get_sharded_store());
    return seastar::now();
  });
}

seastar::future<OSDMeta> OSD::open_or_create_meta_coll(FuturizedStore &store)
{
  return store.get_sharded_store().open_collection(coll_t::meta()).then([&store](auto ch) {
    if (!ch) {
      return store.get_sharded_store().create_new_collection(
	coll_t::meta()
      ).then([&store](auto ch) {
	return OSDMeta(ch, store.get_sharded_store());
      });
    } else {
      return seastar::make_ready_future<OSDMeta>(ch, store.get_sharded_store());
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
  LOG_PREFIX(OSD::mkfs);
  return store.start().then([&store, FNAME, osd_uuid] {
    return store.mkfs(osd_uuid).handle_error(
      crimson::stateful_ec::handle([FNAME] (const auto& ec) {
        ERROR("error creating empty object store in {}: ({}) {}",
	      local_conf().get_val<std::string>("osd_data"),
	      ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([&store, FNAME] {
    return store.mount().handle_error(
      crimson::stateful_ec::handle([FNAME](const auto& ec) {
        ERROR("error mounting object store in {}: ({}) {}",
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
  LOG_PREFIX(OSD::_write_superblock);
  return seastar::do_with(
    std::move(meta_coll),
    std::move(superblock),
    [&store, FNAME](auto &meta_coll, auto &superblock) {
      return meta_coll.load_superblock(
      ).safe_then([&superblock, FNAME](OSDSuperblock&& sb) {
	if (sb.cluster_fsid != superblock.cluster_fsid) {
	  ERROR("provided cluster fsid {} != superblock's {}",
		sb.cluster_fsid, superblock.cluster_fsid);
	  throw std::invalid_argument("mismatched fsid");
	}
	if (sb.whoami != superblock.whoami) {
	  ERROR("provided osd id {} != superblock's {}",
		sb.whoami, superblock.whoami);
	  throw std::invalid_argument("mismatched osd id");
	}
      }).handle_error(
	crimson::ct_error::enoent::handle([&store, &meta_coll, &superblock,
					   FNAME] {
	  // meta collection does not yet, create superblock
	  INFO("{} writing superblock cluster_fsid {} osd_fsid {}",
	       "_write_superblock",
	       superblock.cluster_fsid,
	       superblock.osd_fsid);
	  ceph::os::Transaction t;
	  meta_coll.create(t);
	  meta_coll.store_superblock(t, superblock);
	  DEBUG("OSD::_write_superblock: do_transaction...");
	  return store.get_sharded_store().do_transaction(
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
  LOG_PREFIX(OSD::_write_key_meta);
  if (auto key = local_conf().get_val<std::string>("key"); !std::empty(key)) {
    return store.write_meta("osd_key", key);
  } else if (auto keyfile = local_conf().get_val<std::string>("keyfile");
             !std::empty(keyfile)) {
    return read_file(keyfile).then([&store](const auto& temp_buf) {
      // it's on a truly cold path, so don't worry about memcpy.
      return store.write_meta("osd_key", to_string(temp_buf));
    }).handle_exception([FNAME, keyfile] (auto ep) {
      ERROR("_write_key_meta: failed to handle keyfile {}: {}",
	    keyfile, ep);
      ceph_abort();
    });
  } else {
    return seastar::now();
  }
}

namespace {
  entity_addrvec_t pick_addresses(int what) {
    LOG_PREFIX(osd.cc:pick_addresses);
    entity_addrvec_t addrs;
    crimson::common::CephContext cct;
    // we're interested solely in v2; crimson doesn't do v1
    const auto flags = what | CEPH_PICK_ADDRESS_MSGR2;
    if (int r = ::pick_addresses(&cct, flags, &addrs, -1); r < 0) {
      throw std::runtime_error("failed to pick address");
    }
    for (auto addr : addrs.v) {
      INFO("picked address {}", addr);
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
  LOG_PREFIX(OSD::start);
  INFO("seastar::smp::count {}", seastar::smp::count);

  startup_time = ceph::mono_clock::now();
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return store.start().then([this] {
    return pg_to_shard_mappings.start(0, seastar::smp::count
    ).then([this] {
      return osd_singleton_state.start_single(
        whoami, std::ref(*cluster_msgr), std::ref(*public_msgr),
        std::ref(*monc), std::ref(*mgrc));
    }).then([this] {
      return osd_states.start();
    }).then([this] {
      ceph::mono_time startup_time = ceph::mono_clock::now();
      return shard_services.start(
        std::ref(osd_singleton_state),
        std::ref(pg_to_shard_mappings),
        whoami,
        startup_time,
        osd_singleton_state.local().perf,
        osd_singleton_state.local().recoverystate_perf,
        std::ref(store),
        std::ref(osd_states));
    });
  }).then([this, FNAME] {
    heartbeat.reset(new Heartbeat{
	whoami, get_shard_services(),
	*monc, *hb_front_msgr, *hb_back_msgr});
    return store.mount().handle_error(
      crimson::stateful_ec::handle([FNAME] (const auto& ec) {
        ERROR("error mounting object store in {}: ({}) {}",
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
    if (!superblock.cluster_osdmap_trim_lower_bound) {
      superblock.cluster_osdmap_trim_lower_bound = superblock.get_oldest_map();
    }
    return pg_shard_manager.set_superblock(superblock);
  }).then([this] {
    return pg_shard_manager.get_local_map(superblock.current_epoch);
  }).then([this](OSDMapService::local_cached_map_t&& map) {
    osdmap = make_local_shared_foreign(OSDMapService::local_cached_map_t(map));
    return pg_shard_manager.update_map(std::move(map));
  }).then([this] {
    return shard_services.invoke_on_all([this](auto &local_service) {
      local_service.local_state.osdmap_gate.got_map(osdmap->get_epoch());
    });
  }).then([this] {
    bind_epoch = osdmap->get_epoch();
    return pg_shard_manager.load_pgs(store);
  }).then([this, FNAME] {
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
            [FNAME] (const std::error_code& e) {
          ERROR("cluster messenger bind(): {}", e);
          ceph_abort();
        })),
      public_msgr->bind(pick_addresses(CEPH_PICK_ADDRESS_PUBLIC))
        .safe_then([this, dispatchers]() mutable {
	  return public_msgr->start(dispatchers);
        }, crimson::net::Messenger::bind_ertr::all_same_way(
            [FNAME] (const std::error_code& e) {
          ERROR("public messenger bind(): {}", e);
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
  }).then([FNAME, this] {
    if (auto [addrs, changed] =
        replace_unknown_addrs(cluster_msgr->get_myaddrs(),
                              public_msgr->get_myaddrs()); changed) {
      DEBUG("replacing unkwnown addrs of cluster messenger");
      cluster_msgr->set_myaddrs(addrs);
    }
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
  LOG_PREFIX(OSD::_preboot);
  INFO("osd.{}", whoami);
  if (osdmap->get_epoch() == 0) {
    INFO("waiting for initial osdmap");
  } else if (osdmap->is_destroyed(whoami)) {
    INFO("osdmap says I am destroyed");
    // provide a small margin so we don't livelock seeing if we
    // un-destroyed ourselves.
    if (osdmap->get_epoch() > newest - 1) {
      throw std::runtime_error("i am destroyed");
    }
  } else if (osdmap->is_noup(whoami)) {
    WARN("osdmap NOUP flag is set, waiting for it to clear");
  } else if (!osdmap->test_flag(CEPH_OSDMAP_SORTBITWISE)) {
    ERROR("osdmap SORTBITWISE OSDMap flag is NOT set; please set it");
  } else if (osdmap->require_osd_release < ceph_release_t::octopus) {
    ERROR("osdmap require_osd_release < octopus; please upgrade to octopus");
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
  LOG_PREFIX(OSD::_send_boot);
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
  INFO("hb_back_msgr: {}", hb_back_addrs);
  INFO("hb_front_msgr: {}", hb_front_addrs);
  INFO("cluster_msgr: {}", cluster_addrs);

  auto m = crimson::make_message<MOSDBoot>(superblock,
                                  osdmap->get_epoch(),
                                  boot_epoch,
                                  hb_back_addrs,
                                  hb_front_addrs,
                                  cluster_addrs,
                                  CEPH_FEATURES_ALL);
  collect_sys_info(&m->metadata, NULL);

  // See OSDMonitor::preprocess_boot, prevents boot without allow_crimson
  // OSDMap flag
  m->metadata["osd_type"] = "crimson";
  return monc->send_message(std::move(m));
}

seastar::future<> OSD::_add_me_to_crush()
{
  LOG_PREFIX(OSD::_add_me_to_crush);
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
  return get_weight().then([FNAME, this](auto weight) {
    const crimson::crush::CrushLocation loc;
    return seastar::do_with(
      std::move(loc),
      [FNAME, this, weight] (crimson::crush::CrushLocation& loc) {
      return loc.init_on_startup().then([FNAME, this, weight, &loc]() {
        INFO("crush location is {}", loc);
        string cmd = fmt::format(R"({{
          "prefix": "osd crush create-or-move",
          "id": {},
          "weight": {:.4f},
          "args": [{}]
        }})", whoami, weight, loc);
        return monc->run_command(std::move(cmd), {});
      });
    });
  }).then([FNAME](auto&& command_result) {
    [[maybe_unused]] auto [code, message, out] = std::move(command_result);
    if (code) {
      WARN("fail to add to crush: {} ({})", message, code);
      throw std::runtime_error("fail to add to crush");
    } else {
      INFO("added to crush: {}", message);
    }
    return seastar::now();
  });
}

seastar::future<> OSD::handle_command(
  crimson::net::ConnectionRef conn,
  Ref<MCommand> m)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
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
    asok->register_command(make_asok_hook<pg::ScrubCommand<true>>(*this));
    asok->register_command(make_asok_hook<pg::ScrubCommand<false>>(*this));
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
  LOG_PREFIX(OSD::stop);
  INFO();
  beacon_timer.cancel();
  tick_timer.cancel();
  // see also OSD::shutdown()
  return prepare_to_stop().then([this] {
    return pg_shard_manager.set_stopping();
  }).then([FNAME, this] {
    DEBUG("prepared to stop");
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
      return shard_services.stop();
    }).then([this] {
      return osd_states.stop();
    }).then([this] {
      return osd_singleton_state.stop();
    }).then([this] {
      return pg_to_shard_mappings.stop();
    }).then([fut=std::move(gate_close_fut)]() mutable {
      return std::move(fut);
    }).then([this] {
      return when_all_succeed(
	  public_msgr->shutdown(),
	  cluster_msgr->shutdown()).discard_result();
    }).handle_exception([FNAME](auto ep) {
      ERROR("error while stopping osd: {}", ep);
    });
  });
}

void OSD::dump_status(Formatter* f) const
{
  f->dump_stream("cluster_fsid") << superblock.cluster_fsid;
  f->dump_stream("osd_fsid") << superblock.osd_fsid;
  f->dump_unsigned("whoami", superblock.whoami);
  f->dump_string("state", pg_shard_manager.get_osd_state_string());
  f->dump_stream("maps") << superblock.maps;
  f->dump_stream("oldest_map") << superblock.get_oldest_map();
  f->dump_stream("newest_map") << superblock.get_newest_map();
  f->dump_unsigned("cluster_osdmap_trim_lower_bound",
                   superblock.cluster_osdmap_trim_lower_bound);
  f->dump_unsigned("num_pgs", pg_shard_manager.get_num_pgs());
}

void OSD::print(std::ostream& out) const
{
  out << "{osd." << superblock.whoami << " "
      << superblock.osd_fsid << " maps " << superblock.maps
      << " tlb:" << superblock.cluster_osdmap_trim_lower_bound
      << " pgs:" << pg_shard_manager.get_num_pgs()
      << "}";
}

std::optional<seastar::future<>>
OSD::ms_dispatch(crimson::net::ConnectionRef conn, MessageRef m)
{
  if (pg_shard_manager.is_stopping()) {
    return seastar::now();
  }
  auto maybe_ret = do_ms_dispatch(conn, std::move(m));
  if (!maybe_ret.has_value()) {
    return std::nullopt;
  }

  gate.dispatch_in_background(
      __func__, *this, [ret=std::move(maybe_ret.value())]() mutable {
    return std::move(ret);
  });
  return seastar::now();
}

std::optional<seastar::future<>>
OSD::do_ms_dispatch(
   crimson::net::ConnectionRef conn,
   MessageRef m)
{
  if (seastar::this_shard_id() != PRIMARY_CORE) {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_MAP:
    case MSG_COMMAND:
    case MSG_OSD_MARK_ME_DOWN:
      // FIXME: order is not guaranteed in this path
      return conn.get_foreign(
      ).then([this, m=std::move(m)](auto f_conn) {
        return seastar::smp::submit_to(PRIMARY_CORE,
            [f_conn=std::move(f_conn), m=std::move(m), this]() mutable {
          auto conn = make_local_shared_foreign(std::move(f_conn));
          auto ret = do_ms_dispatch(conn, std::move(m));
          assert(ret.has_value());
          return std::move(ret.value());
        });
      });
    }
  }

  switch (m->get_type()) {
  case CEPH_MSG_OSD_MAP:
    return handle_osd_map(boost::static_pointer_cast<MOSDMap>(m));
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
    return handle_scrub_command(
      conn, boost::static_pointer_cast<MOSDScrub2>(m));
  case MSG_OSD_REP_SCRUB:
  case MSG_OSD_REP_SCRUBMAP:
    return handle_scrub_message(
      conn,
      boost::static_pointer_cast<MOSDFastDispatchOp>(m));
  case MSG_OSD_PG_UPDATE_LOG_MISSING:
    return handle_update_log_missing(conn, boost::static_pointer_cast<
      MOSDPGUpdateLogMissing>(m));
  case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    return handle_update_log_missing_reply(conn, boost::static_pointer_cast<
      MOSDPGUpdateLogMissingReply>(m));
  case MSG_OSD_EC_WRITE:
    return handle_some_ec_messages(conn, boost::static_pointer_cast<MOSDECSubOpWrite>(m));
  case MSG_OSD_EC_WRITE_REPLY:
    return handle_some_ec_messages(conn, boost::static_pointer_cast<MOSDECSubOpWriteReply>(m));
  case MSG_OSD_EC_READ:
    return handle_some_ec_messages(conn, boost::static_pointer_cast<MOSDECSubOpRead>(m));
  case MSG_OSD_EC_READ_REPLY:
    return handle_some_ec_messages(conn, boost::static_pointer_cast<MOSDECSubOpReadReply>(m));
#if 0
  case MSG_OSD_PG_PUSH:
    [[fallthrough]];
  case MSG_OSD_PG_PUSH_REPLY:
    return handle_ec_messages(conn, m);
#endif
  default:
    return std::nullopt;
  }
}

void OSD::ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace)
{
  // TODO: cleanup the session attached to this connection
  LOG_PREFIX(OSD::ms_handle_reset);
  WARN("{}", *conn);
}

void OSD::ms_handle_remote_reset(crimson::net::ConnectionRef conn)
{
  LOG_PREFIX(OSD::ms_handle_remote_reset);
  WARN("{}", *conn);
}

void OSD::handle_authentication(const EntityName& name,
				const AuthCapsInfo& caps_info)
{
  LOG_PREFIX(OSD::handle_authentication);
  // TODO: store the parsed cap and associate it with the connection
  if (caps_info.allow_all) {
    DEBUG("{} has all caps", name);
    return;
  }
  if (caps_info.caps.length() > 0) {
    auto p = caps_info.caps.cbegin();
    string str;
    try {
      decode(str, p);
    } catch (ceph::buffer::error& e) {
      WARN("{} failed to decode caps string", name);
      return;
    }
    OSDCap caps;
    if (caps.parse(str)) {
      DEBUG("{} has caps {}", name, str);
    } else {
      WARN("{} failed to parse caps {}", name, str);
    }
  }
}

const char** OSD::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_beacon_report_interval",
    nullptr
  };
  return KEYS;
}

void OSD::handle_conf_change(
  const crimson::common::ConfigProxy& conf,
  const std::set <std::string> &changed)
{
  if (changed.count("osd_beacon_report_interval")) {
    beacon_timer.rearm_periodic(
      std::chrono::seconds(conf->osd_beacon_report_interval));
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

seastar::future<MessageURef> OSD::get_stats()
{
  // MPGStats::had_map_for is not used since PGMonitor was removed
  auto m = crimson::make_message<MPGStats>(monc->get_fsid(), osdmap->get_epoch());
  m->osd_stat = osd_stat;
  return pg_shard_manager.get_pg_stats(
  ).then([this, m=std::move(m)](auto &&stats) mutable {
    min_last_epoch_clean = osdmap->get_epoch();
    min_last_epoch_clean_pgs.clear();
    for (auto [pgid, stat] : stats) {
      min_last_epoch_clean = std::min(min_last_epoch_clean,
                                      stat.get_effective_last_epoch_clean());
      min_last_epoch_clean_pgs.push_back(pgid);
    }
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

seastar::future<> OSD::handle_osd_map(Ref<MOSDMap> m)
{
  /* Ensure that only one MOSDMap is processed at a time.  Allowing concurrent
  * processing may eventually be worthwhile, but such an implementation would
  * need to ensure (among other things)
  * 1. any particular map is only processed once
  * 2. PGAdvanceMap operations are processed in order for each PG
  * As map handling is not presently a bottleneck, we stick to this
  * simpler invariant for now.
  * See https://tracker.ceph.com/issues/59165
  */
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return handle_osd_map_lock.lock().then([this, m] {
    return _handle_osd_map(m);
  }).finally([this] {
    return handle_osd_map_lock.unlock();
  });
}

seastar::future<> OSD::_handle_osd_map(Ref<MOSDMap> m)
{
  LOG_PREFIX(OSD::_handle_osd_map);
  INFO("{}", *m);
  if (m->fsid != superblock.cluster_fsid) {
    WARN("fsid mismatched");
    return seastar::now();
  }
  if (pg_shard_manager.is_initializing()) {
    WARN("i am still initializing");
    return seastar::now();
  }

  const auto first = m->get_first();
  const auto last = m->get_last();
  INFO(" epochs [{}..{}], i have {}, src has [{}..{}]",
       first, last, superblock.get_newest_map(),
       m->cluster_osdmap_trim_lower_bound, m->newest_map);

  if (superblock.cluster_osdmap_trim_lower_bound <
      m->cluster_osdmap_trim_lower_bound) {
    superblock.cluster_osdmap_trim_lower_bound =
      m->cluster_osdmap_trim_lower_bound;
    DEBUG("superblock cluster_osdmap_trim_lower_bound new epoch is: {}",
	  superblock.cluster_osdmap_trim_lower_bound);
    ceph_assert(
      superblock.cluster_osdmap_trim_lower_bound >= superblock.get_oldest_map());
  }
  // make sure there is something new, here, before we bother flushing
  // the queues and such
  if (last <= superblock.get_newest_map()) {
    return seastar::now();
  }
  // missing some?
  epoch_t start = superblock.get_newest_map() + 1;
  if (first > start) {
    INFO("message skips epochs {}..{}",
	 start, first - 1);
    if (m->cluster_osdmap_trim_lower_bound <= start) {
      return get_shard_services().osdmap_subscribe(start, false);
    }
    // always try to get the full range of maps--as many as we can.  this
    //  1- is good to have
    //  2- is at present the only way to ensure that we get a *full* map as
    //     the first map!
    if (m->cluster_osdmap_trim_lower_bound < first) {
      return get_shard_services().osdmap_subscribe(
        m->cluster_osdmap_trim_lower_bound - 1, true);
    }
  }

  return seastar::do_with(ceph::os::Transaction{},
                          [=, this](auto& t) {
    return pg_shard_manager.store_maps(t, start, m).then([=, this, &t] {
      // even if this map isn't from a mon, we may have satisfied our subscription
      monc->sub_got("osdmap", last);

      if (!superblock.maps.empty()) {
        pg_shard_manager.trim_maps(t, superblock);
        // TODO: once we support pg splitting, update pg_num_history here
        //pg_num_history.prune(superblock.get_oldest_map());
      }

      superblock.insert_osdmap_epochs(first, last);
      superblock.current_epoch = last;

      // note in the superblock that we were clean thru the prior epoch
      if (boot_epoch && boot_epoch >= superblock.mounted) {
        superblock.mounted = boot_epoch;
        superblock.clean_thru = last;
      }
      pg_shard_manager.get_meta_coll().store_superblock(t, superblock);
      return pg_shard_manager.set_superblock(superblock).then(
      [FNAME, this, &t] {
        DEBUG("submitting transaction");
        return store.get_sharded_store().do_transaction(
          pg_shard_manager.get_meta_coll().collection(),
          std::move(t));
      });
    });
  }).then([=, this] {
    // TODO: write to superblock and commit the transaction
    return committed_osd_maps(start, last, m);
  });
}

seastar::future<> OSD::committed_osd_maps(
  version_t first,
  version_t last,
  Ref<MOSDMap> m)
{
  LOG_PREFIX(OSD::committed_osd_maps);
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  INFO("osd.{} ({}, {})", whoami, first, last);
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
  }).then([FNAME, m, this] {
    auto fut = seastar::now();
    if (osdmap->is_up(whoami)) {
      const auto up_from = osdmap->get_up_from(whoami);
      INFO("osd.{}: map e {} marked me up: up_from {}, bind_epoch {}, state {}",
	   whoami, osdmap->get_epoch(), up_from, bind_epoch,
	   pg_shard_manager.get_osd_state_string());
      if (bind_epoch < up_from &&
          osdmap->get_addrs(whoami) == public_msgr->get_myaddrs() &&
          pg_shard_manager.is_booting()) {
        INFO("osd.{}: activating...", whoami);
        fut = pg_shard_manager.set_active().then([this] {
          beacon_timer.arm_periodic(
            std::chrono::seconds(local_conf()->osd_beacon_report_interval));
	  // timer continuation rearms when complete
          tick_timer.arm(
            std::chrono::seconds(TICK_INTERVAL));
        });
      }
    } else {
      if (pg_shard_manager.is_prestop()) {
	got_stop_ack();
	return seastar::now();
      }
    }
    return fut.then([FNAME, this] {
      return check_osdmap_features().then([FNAME, this] {
        // yay!
        INFO("osd.{}: committed_osd_maps: broadcasting osdmaps up"
	     " to {} epoch to pgs", whoami, osdmap->get_epoch());
        return pg_shard_manager.broadcast_map_to_pgs(osdmap->get_epoch());
      });
    });
  }).then([FNAME, m, this] {
    if (pg_shard_manager.is_active()) {
      INFO("osd.{}: now active", whoami);
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
      INFO("osd.{}: now preboot", whoami);

      if (m->get_source().is_mon()) {
        return _preboot(
          m->cluster_osdmap_trim_lower_bound, m->newest_map);
      } else {
        INFO("osd.{}: start_boot", whoami);
        return start_boot();
      }
    } else {
      INFO("osd.{}: now {}", whoami,
	   pg_shard_manager.get_osd_state_string());
      // XXX
      return seastar::now();
    }
  });
}

seastar::future<> OSD::handle_osd_op(
  crimson::net::ConnectionRef conn,
  Ref<MOSDOp> m)
{
  return pg_shard_manager.start_pg_operation<ClientRequest>(
    get_shard_services(),
    conn,
    std::move(m)).second;
}

seastar::future<> OSD::handle_pg_create(
  crimson::net::ConnectionRef conn,
  Ref<MOSDPGCreate2> m)
{
  LOG_PREFIX(OSD::handle_pg_create);
  return seastar::do_for_each(m->pgs, [FNAME, this, conn, m](auto& pg) {
    auto& [pgid, when] = pg;
    const auto &[created, created_stamp] = when;
    auto q = m->pg_extra.find(pgid);
    ceph_assert(q != m->pg_extra.end());
    auto& [history, pi] = q->second;
    DEBUG(
      "e{} @{} "
      "history {} pi {}",
      pgid, created, created_stamp,
      history, pi);
    if (!pi.empty() &&
	m->epoch < pi.get_bounds().second) {
      ERROR(
        "got pg_create on {} epoch {}  "
        "unmatched past_intervals {} (history {})",
        pgid, m->epoch,
        pi, history);
        return seastar::now();
    } else {
      return pg_shard_manager.start_pg_operation<RemotePeeringEvent>(
	  conn,
	  pg_shard_t(),
	  pgid,
	  m->epoch,
	  m->epoch,
	  NullEvt(),
	  true,
	  new PGCreateInfo(pgid, m->epoch, history, pi, true)).second;
    }
  });
}

seastar::future<> OSD::handle_update_log_missing(
  crimson::net::ConnectionRef conn,
  Ref<MOSDPGUpdateLogMissing> m)
{
  return pg_shard_manager.start_pg_operation<LogMissingRequest>(
    std::move(conn),
    std::move(m)).second;
}

seastar::future<> OSD::handle_update_log_missing_reply(
  crimson::net::ConnectionRef conn,
  Ref<MOSDPGUpdateLogMissingReply> m)
{
  return pg_shard_manager.start_pg_operation<LogMissingRequestReply>(
    std::move(conn),
    std::move(m)).second;
}

seastar::future<> OSD::handle_rep_op(
  crimson::net::ConnectionRef conn,
  Ref<MOSDRepOp> m)
{
  m->finish_decode();
  return pg_shard_manager.start_pg_operation<RepRequest>(
    std::move(conn),
    std::move(m)).second;
}

seastar::future<> OSD::handle_rep_op_reply(
  crimson::net::ConnectionRef conn,
  Ref<MOSDRepOpReply> m)
{
  LOG_PREFIX(OSD::handle_rep_op_reply);
  spg_t pgid = m->get_spg();
  return pg_shard_manager.with_pg(
    pgid,
    [FNAME, m=std::move(m)](auto &&pg) {
      if (pg) {
	m->finish_decode();
	pg->handle_rep_op_reply(*m);
      } else {
	WARN("stale reply: {}", *m);
      }
      return seastar::now();
    });
}

seastar::future<> OSD::handle_scrub_command(
  crimson::net::ConnectionRef conn,
  Ref<MOSDScrub2> m)
{
  LOG_PREFIX(OSD::handle_scrub_command);
  if (m->fsid != superblock.cluster_fsid) {
    WARN("fsid mismatched");
    return seastar::now();
  }
  return seastar::parallel_for_each(std::move(m->scrub_pgs),
    [m, conn, this](spg_t pgid) {
    return pg_shard_manager.start_pg_operation<
      crimson::osd::ScrubRequested
      >(m->deep, conn, m->epoch, pgid).second;
  });
}

seastar::future<> OSD::handle_scrub_message(
  crimson::net::ConnectionRef conn,
  Ref<MOSDFastDispatchOp> m)
{
  return pg_shard_manager.start_pg_operation<
    crimson::osd::ScrubMessage
    >(m, conn, m->get_min_epoch(), m->get_spg()).second;
}

seastar::future<> OSD::handle_mark_me_down(
  crimson::net::ConnectionRef conn,
  Ref<MOSDMarkMeDown> m)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  if (pg_shard_manager.is_prestop()) {
    got_stop_ack();
  }
  return seastar::now();
}

seastar::future<> OSD::handle_recovery_subreq(
  crimson::net::ConnectionRef conn,
  Ref<MOSDFastDispatchOp> m)
{
  return pg_shard_manager.start_pg_operation<RecoverySubRequest>(
    conn, std::move(m)).second;
}

bool OSD::should_restart() const
{
  LOG_PREFIX(OSD::should_restart);
  if (!osdmap->is_up(whoami)) {
    INFO("map e {} marked osd.{} down",
	 osdmap->get_epoch(), whoami);
    return true;
  } else if (osdmap->get_addrs(whoami) != public_msgr->get_myaddrs()) {
    ERROR("map e {} had wrong client addr ({} != my {})",
	  osdmap->get_epoch(),
	  osdmap->get_addrs(whoami),
	  public_msgr->get_myaddrs());
    return true;
  } else if (osdmap->get_cluster_addrs(whoami) != cluster_msgr->get_myaddrs()) {
    ERROR("map e {} had wrong cluster addr ({} != my {})",
	  osdmap->get_epoch(),
	  osdmap->get_cluster_addrs(whoami),
	  cluster_msgr->get_myaddrs());
    return true;
  } else {
    return false;
  }
}

template <class MessageRefT>
seastar::future<>
OSD::handle_some_ec_messages(crimson::net::ConnectionRef conn, MessageRefT&& m)
{
  m->decode_payload();
  //m->set_features(conn->get_features());
  (void) pg_shard_manager.start_pg_operation<ECRepRequest>(
    std::move(conn),
    std::forward<MessageRefT>(m));
  return seastar::now();
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
  LOG_PREFIX(OSD::shutdown);
  INFO("shutting down per osdmap");
  abort_source.request_abort();
  return seastar::now();
}

seastar::future<> OSD::send_beacon()
{
  LOG_PREFIX(OSD::send_beacon);
  if (!pg_shard_manager.is_active()) {
    return seastar::now();
  }
  auto beacon = crimson::make_message<MOSDBeacon>(osdmap->get_epoch(),
                                    min_last_epoch_clean,
                                    superblock.last_purged_snaps_scrub,
                                    local_conf()->osd_beacon_report_interval);
  beacon->pgs = min_last_epoch_clean_pgs;
  DEBUG("{}", *beacon);
  return monc->send_message(std::move(beacon));
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
  LOG_PREFIX(OSD::handle_peering_op);
  const int from = m->get_source().num();
  DEBUG("{} from {}", m->get_spg(), from);
  m->set_features(conn->get_features());
  std::unique_ptr<PGPeeringEvent> evt(m->get_event());
  return pg_shard_manager.start_pg_operation<RemotePeeringEvent>(
    conn,
    pg_shard_t{from, m->get_spg().shard},
    m->get_spg(),
    std::move(*evt)).second;
}

seastar::future<> OSD::check_osdmap_features()
{
  assert(seastar::this_shard_id() == PRIMARY_CORE);
  return store.write_meta(
      "require_osd_release",
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
