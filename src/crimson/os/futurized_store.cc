// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "futurized_store.h"
#include "cyanstore/cyan_store.h"
#ifdef WITH_BLUESTORE
#include "alienstore/alien_store.h"
#endif
#include "relaystore/relay_store.h"
#include "seastore/seastore.h"

SET_SUBSYS(osd);

namespace crimson::os {

std::unique_ptr<FuturizedStore>
FuturizedStore::create(const std::string& type,
                       const std::string& data,
                       const ConfigValues& values)
{
  LOG_PREFIX(FuturizedStore::create);
  const bool same_no_shards =
    crimson::common::get_conf<bool>("seastore_require_partition_count_match_reactor_count");
  DEBUG("creating object store: type={}, same_no_shards={}",
        type, same_no_shards);
  if (type == "cyanstore") {
    using crimson::os::CyanStore;
    if (same_no_shards) {
      return std::make_unique<CyanStore>(data);
    } else {
      return std::make_unique<_RelayStore>(
        std::make_unique<CyanStore>(data));
    }
  } else if (type == "seastore") {
    if (same_no_shards) {
      return crimson::os::seastore::make_seastore(data);
    } else {
      using crimson::os::_RelayStore;
      return std::make_unique<_RelayStore>(
        crimson::os::seastore::make_seastore(data));
    }
  } else {
    using crimson::os::AlienStore;
#ifdef WITH_BLUESTORE
    // use AlienStore as a fallback. It adapts e.g. BlueStore.
    return std::make_unique<AlienStore>(type, data, values);
#else
    ceph_abort_msgf("unsupported objectstore type: %s", type.c_str());
    return {};
#endif
  }
}

seastar::future<> with_store_do_transaction(
  FuturizedStore::Shard& shard,
  boost::intrusive_ptr<FuturizedCollection> ch, // TODO: move back to `FuturizedStore::Shard::CollectionRef ch,`
  ceph::os::Transaction&& txn)
{
  if (crimson::common::get_conf<bool>("seastore_require_partition_count_match_reactor_count")) {
    std::unique_ptr<Context> on_commit(
      ceph::os::Transaction::collect_all_contexts(txn));
    return shard.do_transaction_no_callbacks(
      std::move(ch), std::move(txn)
    ).then([on_commit=std::move(on_commit)]() mutable {
      auto c = on_commit.release();
      if (c) c->complete(0);
      return seastar::now();
    });
  } else {
    return crimson::os::_RelayStore::Shard::with_store_do_transaction(
      static_cast<crimson::os::_RelayStore::Shard&>(shard), ch, std::move(txn));
  }
}
}
