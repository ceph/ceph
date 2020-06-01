// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd.h"

#include <sys/utsname.h>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/join.hpp>
#include <boost/smart_ptr/make_local_shared.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/timer.hh>

#include "common/pick_address.h"
#include "include/util.h"

#include "messages/MCommand.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDBeacon.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MPGStats.h"

#include "os/Transaction.h"
#include "osd/ClassHandler.h"
#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"

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
#include "crimson/osd/osd_operations/compound_peering_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_operations/replicated_request.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
  static constexpr int TICK_INTERVAL = 1;
}

using crimson::common::local_conf;
using crimson::os::FuturizedStore;

namespace crimson::osd {

OSD::OSD(int id, uint32_t nonce,
         crimson::net::MessengerRef cluster_msgr,
         crimson::net::MessengerRef public_msgr,
         crimson::net::MessengerRef hb_front_msgr,
         crimson::net::MessengerRef hb_back_msgr)
  : whoami{id},
    nonce{nonce},
    // do this in background
    beacon_timer{[this] { (void)send_beacon(); }},
    cluster_msgr{cluster_msgr},
    public_msgr{public_msgr},
    monc{new crimson::mon::Client{*public_msgr, *this}},
    mgrc{new crimson::mgr::Client{*public_msgr, *this}},
    store{crimson::os::FuturizedStore::create(
      local_conf().get_val<std::string>("osd_objectstore"),
      local_conf().get_val<std::string>("osd_data"),
      local_conf().get_config_values())},
    shard_services{*this, *cluster_msgr, *public_msgr, *monc, *mgrc, *store},
    heartbeat{new Heartbeat{shard_services, *monc, hb_front_msgr, hb_back_msgr}},
    // do this in background
    heartbeat_timer{[this] { update_heartbeat_peers(); }},
    asok{seastar::make_lw_shared<crimson::admin::AdminSocket>()},
    osdmap_gate("OSD::osdmap_gate", std::make_optional(std::ref(shard_services)))
{
  osdmaps[0] = boost::make_local_shared<OSDMap>();
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

seastar::future<> OSD::mkfs(uuid_d osd_uuid, uuid_d cluster_fsid)
{
  return store->start().then([this, osd_uuid] {
    return store->mkfs(osd_uuid);
  }).then([this] {
    return store->mount();
  }).then([cluster_fsid, this] {
    superblock.cluster_fsid = cluster_fsid;
    superblock.osd_fsid = store->get_fsid();
    superblock.whoami = whoami;
    superblock.compat_features = get_osd_initial_compat_set();

    logger().info(
      "{} writing superblock cluster_fsid {} osd_fsid {}",
      __func__,
      cluster_fsid,
      superblock.osd_fsid);
    return store->create_new_collection(coll_t::meta());
  }).then([this] (auto ch) {
    meta_coll = make_unique<OSDMeta>(ch , store.get());
    ceph::os::Transaction t;
    meta_coll->create(t);
    meta_coll->store_superblock(t, superblock);
    return store->do_transaction(meta_coll->collection(), std::move(t));
  }).then([cluster_fsid, this] {
    return when_all_succeed(
      store->write_meta("ceph_fsid", cluster_fsid.to_string()),
      store->write_meta("whoami", std::to_string(whoami)));
  }).then([cluster_fsid, this] {
    fmt::print("created object store {} for osd.{} fsid {}\n",
               local_conf().get_val<std::string>("osd_data"),
               whoami, cluster_fsid);
    return seastar::now();
  });
}

namespace {
  entity_addrvec_t pick_addresses(int what) {
    entity_addrvec_t addrs;
    crimson::common::CephContext cct;
    if (int r = ::pick_addresses(&cct, what, &addrs, -1); r < 0) {
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

  return store->start().then([this] {
    return store->mount();
  }).then([this] {
    return store->open_collection(coll_t::meta());
  }).then([this](auto ch) {
    meta_coll = make_unique<OSDMeta>(ch, store.get());
    return meta_coll->load_superblock();
  }).then([this](OSDSuperblock&& sb) {
    superblock = std::move(sb);
    return get_map(superblock.current_epoch);
  }).then([this](cached_map_t&& map) {
    shard_services.update_map(map);
    osdmap_gate.got_map(map->get_epoch());
    osdmap = std::move(map);
    return load_pgs();
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

    auto chained_dispatchers = seastar::make_lw_shared<ChainedDispatchers>();
    chained_dispatchers->push_front(*mgrc);
    chained_dispatchers->push_front(*monc);
    chained_dispatchers->push_front(*this);
    return seastar::when_all_succeed(
      cluster_msgr->try_bind(pick_addresses(CEPH_PICK_ADDRESS_CLUSTER),
                             local_conf()->ms_bind_port_min,
                             local_conf()->ms_bind_port_max)
        .then([this, chained_dispatchers]() mutable {
	  return cluster_msgr->start(chained_dispatchers);
	}),
      public_msgr->try_bind(pick_addresses(CEPH_PICK_ADDRESS_PUBLIC),
                            local_conf()->ms_bind_port_min,
                            local_conf()->ms_bind_port_max)
        .then([this, chained_dispatchers]() mutable {
	  return public_msgr->start(chained_dispatchers);
	}));
  }).then([this] {
    return seastar::when_all_succeed(monc->start(),
                                     mgrc->start());
  }).then([this] {
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
      return cluster_msgr->set_myaddrs(addrs);
    } else {
      return seastar::now();
    }
  }).then([this] {
    return heartbeat->start(public_msgr->get_myaddrs(),
                            cluster_msgr->get_myaddrs());
  }).then([this] {
    // create the admin-socket server, and the objects that register
    // to handle incoming commands
    return start_asok_admin();
  }).then([this] {
    return start_boot();
  });
}

seastar::future<> OSD::start_boot()
{
  state.set_preboot();
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
    return shard_services.osdmap_subscribe(osdmap->get_epoch() + 1, false);
  } else {
    return shard_services.osdmap_subscribe(oldest - 1, true);
  }
}

seastar::future<> OSD::_send_boot()
{
  state.set_booting();

  logger().info("hb_back_msgr: {}", heartbeat->get_back_addrs());
  logger().info("hb_front_msgr: {}", heartbeat->get_front_addrs());
  logger().info("cluster_msgr: {}", cluster_msgr->get_myaddr());
  auto m = make_message<MOSDBoot>(superblock,
                                  osdmap->get_epoch(),
                                  osdmap->get_epoch(),
                                  heartbeat->get_back_addrs(),
                                  heartbeat->get_front_addrs(),
                                  cluster_msgr->get_myaddrs(),
                                  CEPH_FEATURES_ALL);
  collect_sys_info(&m->metadata, NULL);
  return monc->send_message(m);
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
       return store->stat().then([](auto st) {
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
    return monc->run_command({cmd}, {});
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

seastar::future<> OSD::_send_alive()
{
  auto want = osdmap->get_epoch();
  logger().info(
    "{} want {} up_thru_wanted {}",
    __func__,
    want,
    up_thru_wanted);
  if (!osdmap->exists(whoami)) {
    logger().warn("{} DNE", __func__);
    return seastar::now();
  } else if (want <= up_thru_wanted) {
    logger().debug("{} {} <= {}", __func__, want, up_thru_wanted);
    return seastar::now();
  } else {
    up_thru_wanted = want;
    auto m = make_message<MOSDAlive>(osdmap->get_epoch(), want);
    return monc->send_message(std::move(m));
  }
}

seastar::future<> OSD::handle_command(crimson::net::Connection* conn,
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
    return seastar::when_all_succeed(
      asok->register_admin_commands(),
      asok->register_command(make_asok_hook<OsdStatusHook>(*this)),
      asok->register_command(make_asok_hook<SendBeaconHook>(*this)),
      asok->register_command(make_asok_hook<ConfigShowHook>()),
      asok->register_command(make_asok_hook<ConfigGetHook>()),
      asok->register_command(make_asok_hook<ConfigSetHook>()));
  });
}

seastar::future<> OSD::stop()
{
  logger().info("stop");
  // see also OSD::shutdown()
  return prepare_to_stop().then([this] {
    state.set_stopping();
    logger().debug("prepared to stop");
    if (!public_msgr->dispatcher_chain_empty()) {
      public_msgr->remove_dispatcher(*this);
      public_msgr->remove_dispatcher(*mgrc);
      public_msgr->remove_dispatcher(*monc);
    }
    if (!cluster_msgr->dispatcher_chain_empty()) {
      cluster_msgr->remove_dispatcher(*this);
      cluster_msgr->remove_dispatcher(*mgrc);
      cluster_msgr->remove_dispatcher(*monc);
    }
    auto gate_close_fut = gate.close();
    return asok->stop().then([this] {
      return heartbeat->stop();
    }).then([this] {
      return store->umount();
    }).then([this] {
      return store->stop();
    }).then([this] {
      return seastar::parallel_for_each(pg_map.get_pgs(),
	[](auto& p) {
	return p.second->stop();
      });
    }).then([this] {
      return monc->stop();
    }).then([this] {
      return mgrc->stop();
    }).then([fut=std::move(gate_close_fut)]() mutable {
      return std::move(fut);
    }).then([this] {
      return when_all_succeed(
	  public_msgr->shutdown(),
	  cluster_msgr->shutdown());
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
  f->dump_string("state", state.to_string());
  f->dump_unsigned("oldest_map", superblock.oldest_map);
  f->dump_unsigned("newest_map", superblock.newest_map);
  f->dump_unsigned("num_pgs", pg_map.get_pgs().size());
}

void OSD::print(std::ostream& out) const
{
  out << "{osd." << superblock.whoami << " "
    << superblock.osd_fsid << " [" << superblock.oldest_map
    << "," << superblock.newest_map << "] " << pg_map.get_pgs().size()
    << " pgs}";
}

seastar::future<> OSD::load_pgs()
{
  return store->list_collections().then([this](auto colls) {
    return seastar::parallel_for_each(colls, [this](auto coll) {
      spg_t pgid;
      if (coll.is_pg(&pgid)) {
        return load_pg(pgid).then([pgid, this](auto&& pg) {
          logger().info("load_pgs: loaded {}", pgid);
          pg_map.pg_loaded(pgid, std::move(pg));
          shard_services.inc_pg_num();
          return seastar::now();
        });
      } else if (coll.is_temp(&pgid)) {
        // TODO: remove the collection
        return seastar::now();
      } else {
        logger().warn("ignoring unrecognized collection: {}", coll);
        return seastar::now();
      }
    });
  });
}

seastar::future<Ref<PG>> OSD::make_pg(cached_map_t create_map,
				      spg_t pgid,
				      bool do_create)
{
  using ec_profile_t = map<string,string>;
  auto get_pool_info = [create_map, pgid, this] {
    if (create_map->have_pg_pool(pgid.pool())) {
      pg_pool_t pi = *create_map->get_pg_pool(pgid.pool());
      string name = create_map->get_pool_name(pgid.pool());
      ec_profile_t ec_profile;
      if (pi.is_erasure()) {
	ec_profile = create_map->get_erasure_code_profile(pi.erasure_code_profile);
      }
      return seastar::make_ready_future<std::tuple<pg_pool_t, string, ec_profile_t>>(
        std::make_tuple(std::move(pi),
			std::move(name),
			std::move(ec_profile)));
    } else {
      // pool was deleted; grab final pg_pool_t off disk.
      return meta_coll->load_final_pool_info(pgid.pool());
    }
  };
  auto get_collection = [pgid, do_create, this] {
    const coll_t cid{pgid};
    if (do_create) {
      return store->create_new_collection(cid);
    } else {
      return store->open_collection(cid);
    }
  };
  return seastar::when_all(
    std::move(get_pool_info),
    std::move(get_collection)
  ).then([pgid, create_map, this] (auto&& ret) {
    auto [pool, name, ec_profile] = std::move(std::get<0>(ret).get0());
    auto coll = std::move(std::get<1>(ret).get0());
    return seastar::make_ready_future<Ref<PG>>(
      new PG{pgid,
	     pg_shard_t{whoami, pgid.shard},
	     std::move(coll),
	     std::move(pool),
	     std::move(name),
	     create_map,
	     shard_services,
	     ec_profile});
  });
}

seastar::future<Ref<PG>> OSD::load_pg(spg_t pgid)
{
  return seastar::do_with(PGMeta(store.get(), pgid), [] (auto& pg_meta) {
    return pg_meta.get_epoch();
  }).then([this](epoch_t e) {
    return get_map(e);
  }).then([pgid, this] (auto&& create_map) {
    return make_pg(std::move(create_map), pgid, false);
  }).then([this](Ref<PG> pg) {
    return pg->read_state(store.get()).then([pg] {
	return seastar::make_ready_future<Ref<PG>>(std::move(pg));
    });
  }).handle_exception([pgid](auto ep) {
    logger().info("pg {} saw exception on load {}", pgid, ep);
    ceph_abort("Could not load pg" == 0);
    return seastar::make_exception_future<Ref<PG>>(ep);
  });
}

seastar::future<> OSD::ms_dispatch(crimson::net::Connection* conn, MessageRef m)
{
  return gate.dispatch(__func__, *this, [this, conn, &m] {
    if (state.is_stopping()) {
      return seastar::now();
    }
    switch (m->get_type()) {
    case CEPH_MSG_OSD_MAP:
      return handle_osd_map(conn, boost::static_pointer_cast<MOSDMap>(m));
    case CEPH_MSG_OSD_OP:
      return handle_osd_op(conn, boost::static_pointer_cast<MOSDOp>(m));
    case MSG_OSD_PG_CREATE2:
      shard_services.start_operation<CompoundPeeringRequest>(
	*this,
	conn->get_shared(),
	m);
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
    default:
      logger().info("ms_dispatch unhandled message {}", *m);
      return seastar::now();
    }
  });
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
				const AuthCapsInfo& caps)
{
  // todo
}

MessageRef OSD::get_stats()
{
  // todo: m-to-n: collect stats using map-reduce
  // MPGStats::had_map_for is not used since PGMonitor was removed
  auto m = make_message<MPGStats>(monc->get_fsid(), osdmap->get_epoch());

  for (auto [pgid, pg] : pg_map.get_pgs()) {
    if (pg->is_primary()) {
      auto stats = pg->get_stats();
      // todo: update reported_epoch,reported_seq,last_fresh
      stats.reported_epoch = osdmap->get_epoch();
      m->pg_stat.emplace(pgid.pgid, std::move(stats));
    }
  }
  return m;
}

OSD::cached_map_t OSD::get_map() const
{
  return osdmap;
}

seastar::future<OSD::cached_map_t> OSD::get_map(epoch_t e)
{
  // TODO: use LRU cache for managing osdmap, fallback to disk if we have to
  if (auto found = osdmaps.find(e); found) {
    return seastar::make_ready_future<cached_map_t>(std::move(found));
  } else {
    return load_map(e).then([e, this](unique_ptr<OSDMap> osdmap) {
      return seastar::make_ready_future<cached_map_t>(
        osdmaps.insert(e, std::move(osdmap)));
    });
  }
}

void OSD::store_map_bl(ceph::os::Transaction& t,
                       epoch_t e, bufferlist&& bl)
{
  meta_coll->store_map(t, e, bl);
  map_bl_cache.insert(e, std::move(bl));
}

seastar::future<bufferlist> OSD::load_map_bl(epoch_t e)
{
  if (std::optional<bufferlist> found = map_bl_cache.find(e); found) {
    return seastar::make_ready_future<bufferlist>(*found);
  } else {
    return meta_coll->load_map(e);
  }
}

seastar::future<std::unique_ptr<OSDMap>> OSD::load_map(epoch_t e)
{
  auto o = std::make_unique<OSDMap>();
  if (e > 0) {
    return load_map_bl(e).then([o=std::move(o)](bufferlist bl) mutable {
      o->decode(bl);
      return seastar::make_ready_future<unique_ptr<OSDMap>>(std::move(o));
    });
  } else {
    return seastar::make_ready_future<unique_ptr<OSDMap>>(std::move(o));
  }
}

seastar::future<> OSD::store_maps(ceph::os::Transaction& t,
                                  epoch_t start, Ref<MOSDMap> m)
{
  return seastar::do_for_each(boost::make_counting_iterator(start),
                              boost::make_counting_iterator(m->get_last() + 1),
                              [&t, m, this](epoch_t e) {
    if (auto p = m->maps.find(e); p != m->maps.end()) {
      auto o = std::make_unique<OSDMap>();
      o->decode(p->second);
      logger().info("store_maps osdmap.{}", e);
      store_map_bl(t, e, std::move(std::move(p->second)));
      osdmaps.insert(e, std::move(o));
      return seastar::now();
    } else if (auto p = m->incremental_maps.find(e);
               p != m->incremental_maps.end()) {
      return load_map(e - 1).then([e, bl=p->second, &t, this](auto o) {
        OSDMap::Incremental inc;
        auto i = bl.cbegin();
        inc.decode(i);
        o->apply_incremental(inc);
        bufferlist fbl;
        o->encode(fbl, inc.encode_features | CEPH_FEATURE_RESERVED);
        store_map_bl(t, e, std::move(fbl));
        osdmaps.insert(e, std::move(o));
        return seastar::now();
      });
    } else {
      logger().error("MOSDMap lied about what maps it had?");
      return seastar::now();
    }
  });
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

seastar::future<Ref<PG>> OSD::handle_pg_create_info(
  std::unique_ptr<PGCreateInfo> info) {
  return seastar::do_with(
    std::move(info),
    [this](auto &info) -> seastar::future<Ref<PG>> {
      return get_map(info->epoch).then(
	[&info, this](cached_map_t startmap) ->
	seastar::future<std::tuple<Ref<PG>, cached_map_t>> {
	  const spg_t &pgid = info->pgid;
	  if (info->by_mon) {
	    int64_t pool_id = pgid.pgid.pool();
	    const pg_pool_t *pool = osdmap->get_pg_pool(pool_id);
	    if (!pool) {
	      logger().debug(
		"{} ignoring pgid {}, pool dne",
		__func__,
		pgid);
	      return seastar::make_ready_future<std::tuple<Ref<PG>, cached_map_t>>(
                std::make_tuple(Ref<PG>(), startmap));
	    }
	    ceph_assert(osdmap->require_osd_release >= ceph_release_t::octopus);
	    if (!pool->has_flag(pg_pool_t::FLAG_CREATING)) {
	      // this ensures we do not process old creating messages after the
	      // pool's initial pgs have been created (and pg are subsequently
	      // allowed to split or merge).
	      logger().debug(
		"{} dropping {} create, pool does not have CREATING flag set",
		__func__,
		pgid);
	      return seastar::make_ready_future<std::tuple<Ref<PG>, cached_map_t>>(
                std::make_tuple(Ref<PG>(), startmap));
	    }
	  }
	  return make_pg(startmap, pgid, true).then(
	    [startmap=std::move(startmap)](auto pg) mutable {
	      return seastar::make_ready_future<std::tuple<Ref<PG>, cached_map_t>>(
                std::make_tuple(std::move(pg), std::move(startmap)));
	    });
      }).then([this, &info](auto&& ret) ->
              seastar::future<Ref<PG>> {
        auto [pg, startmap] = std::move(ret);
        if (!pg)
          return seastar::make_ready_future<Ref<PG>>(Ref<PG>());
        PeeringCtx rctx{ceph_release_t::octopus};
        const pg_pool_t* pp = startmap->get_pg_pool(info->pgid.pool());

        int up_primary, acting_primary;
        vector<int> up, acting;
        startmap->pg_to_up_acting_osds(
          info->pgid.pgid, &up, &up_primary, &acting, &acting_primary);

        int role = startmap->calc_pg_role(pg_shard_t(whoami, info->pgid.shard),
                                          acting);

        create_pg_collection(
          rctx.transaction,
          info->pgid,
          info->pgid.get_split_bits(pp->get_pg_num()));
        init_pg_ondisk(
          rctx.transaction,
          info->pgid,
          pp);

        pg->init(
          role,
          up,
          up_primary,
          acting,
          acting_primary,
          info->history,
          info->past_intervals,
          false,
          rctx.transaction);

        return shard_services.start_operation<PGAdvanceMap>(
          *this, pg, pg->get_osdmap_epoch(),
          osdmap->get_epoch(), std::move(rctx), true).second.then([pg=pg] {
            return seastar::make_ready_future<Ref<PG>>(pg);
        });
      });
  });
}

seastar::future<> OSD::handle_osd_map(crimson::net::Connection* conn,
                                      Ref<MOSDMap> m)
{
  logger().info("handle_osd_map {}", *m);
  if (m->fsid != superblock.cluster_fsid) {
    logger().warn("fsid mismatched");
    return seastar::now();
  }
  if (state.is_initializing()) {
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
      return shard_services.osdmap_subscribe(start, false);
    }
    // always try to get the full range of maps--as many as we can.  this
    //  1- is good to have
    //  2- is at present the only way to ensure that we get a *full* map as
    //     the first map!
    if (m->oldest_map < first) {
      return shard_services.osdmap_subscribe(m->oldest_map - 1, true);
    }
    skip_maps = true;
    start = first;
  }

  return seastar::do_with(ceph::os::Transaction{},
                          [=](auto& t) {
    return store_maps(t, start, m).then([=, &t] {
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
      meta_coll->store_superblock(t, superblock);
      return store->do_transaction(meta_coll->collection(), std::move(t));
    });
  }).then([=] {
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
    return get_map(cur).then([this](cached_map_t&& o) {
      osdmap = std::move(o);
      shard_services.update_map(osdmap);
      if (up_epoch == 0 &&
          osdmap->is_up(whoami) &&
          osdmap->get_addrs(whoami) == public_msgr->get_myaddrs()) {
        up_epoch = osdmap->get_epoch();
        if (!boot_epoch) {
          boot_epoch = osdmap->get_epoch();
        }
      }
    });
  }).then([m, this] {
    if (osdmap->is_up(whoami) &&
        osdmap->get_addrs(whoami) == public_msgr->get_myaddrs() &&
        bind_epoch < osdmap->get_up_from(whoami)) {
      if (state.is_booting()) {
        logger().info("osd.{}: activating...", whoami);
        state.set_active();
        beacon_timer.arm_periodic(
          std::chrono::seconds(local_conf()->osd_beacon_report_interval));
        heartbeat_timer.arm_periodic(
          std::chrono::seconds(TICK_INTERVAL));
      }
    } else if (!osdmap->is_up(whoami)) {
      if (state.is_prestop()) {
	got_stop_ack();
	return seastar::now();
      }
    }
    check_osdmap_features();
    // yay!
    return consume_map(osdmap->get_epoch());
  }).then([m, this] {
    if (state.is_active()) {
      logger().info("osd.{}: now active", whoami);
      if (!osdmap->exists(whoami)) {
        return shutdown();
      }
      if (should_restart()) {
        return restart();
      } else {
        return seastar::now();
      }
    } else if (state.is_preboot()) {
      logger().info("osd.{}: now preboot", whoami);

      if (m->get_source().is_mon()) {
        return _preboot(m->oldest_map, m->newest_map);
      } else {
        logger().info("osd.{}: start_boot", whoami);
        return start_boot();
      }
    } else {
      logger().info("osd.{}: now {}", whoami, state);
      // XXX
      return seastar::now();
    }
  });
}

seastar::future<> OSD::handle_osd_op(crimson::net::Connection* conn,
                                     Ref<MOSDOp> m)
{
  (void) shard_services.start_operation<ClientRequest>(
    *this,
    conn->get_shared(),
    std::move(m));
  return seastar::now();
}

seastar::future<> OSD::handle_rep_op(crimson::net::Connection* conn,
				     Ref<MOSDRepOp> m)
{
  m->finish_decode();
  (void) shard_services.start_operation<RepRequest>(
    *this,
    conn->get_shared(),
    std::move(m));
  return seastar::now();
}

seastar::future<> OSD::handle_rep_op_reply(crimson::net::Connection* conn,
					   Ref<MOSDRepOpReply> m)
{
  const auto& pgs = pg_map.get_pgs();
  if (auto pg = pgs.find(m->get_spg()); pg != pgs.end()) {
    m->finish_decode();
    pg->second->handle_rep_op_reply(conn, *m);
  } else {
    logger().warn("stale reply: {}", *m);
  }
  return seastar::now();
}

seastar::future<> OSD::handle_mark_me_down(crimson::net::Connection* conn,
					   Ref<MOSDMarkMeDown> m)
{
  if (state.is_prestop()) {
    got_stop_ack();
  }
  return seastar::now();
}

seastar::future<> OSD::handle_recovery_subreq(crimson::net::Connection* conn,
				   Ref<MOSDFastDispatchOp> m)
{
  (void) shard_services.start_operation<RecoverySubRequest>(
    *this,
    conn->get_shared(),
    std::move(m));
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
  heartbeat_timer.cancel();
  up_epoch = 0;
  bind_epoch = osdmap->get_epoch();
  // TODO: promote to shutdown if being marked down for multiple times
  // rebind messengers
  return start_boot();
}

seastar::future<> OSD::shutdown()
{
  // TODO
  superblock.mounted = boot_epoch;
  superblock.clean_thru = osdmap->get_epoch();
  return seastar::now();
}

seastar::future<> OSD::send_beacon()
{
  if (!state.is_active()) {
    return seastar::now();
  }
  // FIXME: min lec should be calculated from pg_stat
  //        and should set m->pgs
  epoch_t min_last_epoch_clean = osdmap->get_epoch();
  auto m = make_message<MOSDBeacon>(osdmap->get_epoch(),
                                    min_last_epoch_clean,
				    superblock.last_purged_snaps_scrub);
  return monc->send_message(m);
}

void OSD::update_heartbeat_peers()
{
  if (!state.is_active()) {
    return;
  }
  for (auto& pg : pg_map.get_pgs()) {
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(pg.first.pgid,
                                 &up, nullptr,
                                 &acting, nullptr);
    for (int osd : boost::join(up, acting)) {
      if (osd == CRUSH_ITEM_NONE || osd == whoami) {
        continue;
      } else {
        heartbeat->add_peer(osd, osdmap->get_epoch());
      }
    }
  }
  heartbeat->update_peers(whoami);
}

seastar::future<> OSD::handle_peering_op(
  crimson::net::Connection* conn,
  Ref<MOSDPeeringOp> m)
{
  const int from = m->get_source().num();
  logger().debug("handle_peering_op on {} from {}", m->get_spg(), from);
  std::unique_ptr<PGPeeringEvent> evt(m->get_event());
  (void) shard_services.start_operation<RemotePeeringEvent>(
    *this,
    conn->get_shared(),
    shard_services,
    pg_shard_t{from, m->get_spg().shard},
    m->get_spg(),
    std::move(*evt));
  return seastar::now();
}

void OSD::check_osdmap_features()
{
  heartbeat->set_require_authorizer(true);
}

seastar::future<> OSD::consume_map(epoch_t epoch)
{
  // todo: m-to-n: broadcast this news to all shards
  auto &pgs = pg_map.get_pgs();
  return seastar::parallel_for_each(pgs.begin(), pgs.end(), [=](auto& pg) {
    return shard_services.start_operation<PGAdvanceMap>(
      *this, pg.second, pg.second->get_osdmap_epoch(), epoch,
      PeeringCtx{ceph_release_t::octopus}, false).second;
  }).then([epoch, this] {
    osdmap_gate.got_map(epoch);
    return seastar::make_ready_future();
  });
}


blocking_future<Ref<PG>>
OSD::get_or_create_pg(
  spg_t pgid,
  epoch_t epoch,
  std::unique_ptr<PGCreateInfo> info)
{
  auto [fut, creating] = pg_map.get_pg(pgid, bool(info));
  if (!creating && info) {
    pg_map.set_creating(pgid);
    (void)handle_pg_create_info(std::move(info));
  }
  return std::move(fut);
}

blocking_future<Ref<PG>> OSD::wait_for_pg(
  spg_t pgid)
{
  return pg_map.get_pg(pgid).first;
}

seastar::future<> OSD::prepare_to_stop()
{
  if (osdmap && osdmap->is_up(whoami)) {
    state.set_prestop();
    return monc->send_message(
	  make_message<MOSDMarkMeDown>(
	    monc->get_fsid(),
	    whoami,
	    osdmap->get_addrs(whoami),
	    osdmap->get_epoch(),
	    true)).then([this] {
      const auto timeout =
	std::chrono::duration_cast<std::chrono::milliseconds>(
	  std::chrono::duration<double>(
	    local_conf().get_val<double>("osd_mon_shutdown_timeout")));
      return seastar::with_timeout(
      seastar::timer<>::clock::now() + timeout,
      stop_acked.get_future());
    }).handle_exception_type([this](seastar::timed_out_error&) {
      return seastar::now();
    });
  }
  return seastar::now();
}

}
