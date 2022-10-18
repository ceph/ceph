// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"

#include "shard_stores.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

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

namespace crimson::osd {

seastar::future<>
ShardStores::create_store(
  const std::string& type,
  const std::string& data,
  const ConfigValues& values)
{
  return crimson::os::FuturizedStore::create(
    type,
    data,
    values).then([this](auto st) {
      store = std::move(st);
  });
}

seastar::future<> ShardStores:: mkfs(
    unsigned whoami,
    uuid_d osd_uuid,
    uuid_d cluster_fsid,
    std::string osdspec_affinity)
{
    return store->start().then([this, osd_uuid] {
    return store->mkfs(osd_uuid).handle_error(
      crimson::stateful_ec::handle([] (const auto& ec) {
        logger().error("error creating empty object store in {}: ({}) {}",
                       crimson::common::local_conf().get_val<std::string>("osd_data"),
                       ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([this] {
    return store->mount().handle_error(
      crimson::stateful_ec::handle([](const auto& ec) {
        logger().error("error mounting object store in {}: ({}) {}",
                       crimson::common::local_conf().get_val<std::string>("osd_data"),
                       ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([this] {
    return open_or_create_meta_coll(*store);
  }).then([this, whoami, cluster_fsid](auto meta_coll) {
    OSDSuperblock superblock;
    superblock.cluster_fsid = cluster_fsid;
    superblock.osd_fsid = store->get_fsid();
    superblock.whoami = whoami;
    superblock.compat_features = get_osd_initial_compat_set();
    return _write_superblock(
      *store, std::move(meta_coll), std::move(superblock));
  }).then([this, cluster_fsid] {
    return store->write_meta("ceph_fsid", cluster_fsid.to_string());
  }).then([this] {
    return store->write_meta("magic", CEPH_OSD_ONDISK_MAGIC);
  }).then([this, whoami] {
    return store->write_meta("whoami", std::to_string(whoami));
  }).then([this] {
    return _write_key_meta(*store);
  }).then([this, osdspec_affinity=std::move(osdspec_affinity)] {
    return store->write_meta("osdspec_affinity", osdspec_affinity);
  }).then([this] {
    return store->write_meta("ready", "ready");
  }).then([this, whoami, cluster_fsid] {
    fmt::print("created object store {} for osd.{} fsid {}\n",
               crimson::common::local_conf().get_val<std::string>("osd_data"),
               whoami, cluster_fsid);
    return store->umount();
  }).then([this] {
    return store->stop();
  });
}

seastar::future<> ShardStores::start()
{
  return store->start();
}

crimson::os::FuturizedStore::mount_ertr::future<> ShardStores::mount()
{
  return store->mount();
}

seastar::future<OSDMeta> ShardStores::open_or_create_meta_coll(
  crimson::os::FuturizedStore &store)
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

seastar::future<> ShardStores::_write_superblock(
  crimson::os::FuturizedStore &store,
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

static std::string to_string(const seastar::temporary_buffer<char>& temp_buf)
{
  return {temp_buf.get(), temp_buf.size()};
}

seastar::future<> ShardStores::_write_key_meta(
  crimson::os::FuturizedStore &store)
{

  if (auto key = crimson::common::local_conf().get_val<std::string>("key"); !std::empty(key)) {
    return store.write_meta("osd_key", key);
  } else if (auto keyfile = crimson::common::local_conf().get_val<std::string>("keyfile");
             !std::empty(keyfile)) {
    return crimson::read_file(keyfile).then([&store](const auto& temp_buf) {
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
};
