// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/config_proxy.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/osd_meta.h"

static inline bool multicore_store() {
  return (crimson::common::local_conf().get_val<std::string>
    ("osd_objectstore") == "cyanstore");
}

namespace crimson::osd {

class ShardStores {

public:
  std::unique_ptr<crimson::os::FuturizedStore> store;

  ShardStores(){}

  seastar::future<> create_store(
    const std::string& type,
    const std::string& data,
    const ConfigValues& values);

  seastar::future<> mkfs(
    unsigned whoami,
    uuid_d osd_uuid,
    uuid_d cluster_fsid,
    std::string osdspec_affinity);

  seastar::future<> start();
  crimson::os::FuturizedStore::mount_ertr::future<> mount();

private:
  static seastar::future<OSDMeta> open_or_create_meta_coll(
    crimson::os::FuturizedStore &store
  );

  static seastar::future<> _write_superblock(
    crimson::os::FuturizedStore &store,
    OSDMeta meta,
    OSDSuperblock superblock);

  static seastar::future<> _write_key_meta(
    crimson::os::FuturizedStore &store
  );
};

}
