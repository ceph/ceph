// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "futurized_store.h"
#include "cyanstore/cyan_store.h"
#ifdef WITH_BLUESTORE
#include "alienstore/alien_store.h"
#endif
#include "seastore/seastore.h"

namespace crimson::os {

std::unique_ptr<FuturizedStore>
FuturizedStore::create(const std::string& type,
                       const std::string& data,
                       const ConfigValues& values)
{
  if (type == "cyanstore") {
    using crimson::os::CyanStore;
    return std::make_unique<CyanStore>(data);
  } else if (type == "seastore") {
    return crimson::os::seastore::make_seastore(
      data);
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
  crimson::os::FuturizedStore::StoreShardRef store,
  FuturizedStore::Shard::CollectionRef ch,
  ceph::os::Transaction&& txn)
{
  std::unique_ptr<Context> on_commit(
    ceph::os::Transaction::collect_all_contexts(txn));
  const auto original_core = seastar::this_shard_id();
  if (store.get_owner_shard() == seastar::this_shard_id()) {
    return store->do_transaction_no_callbacks(
      std::move(ch), std::move(txn)
    ).then([on_commit=std::move(on_commit)]() mutable {
      auto c = on_commit.release();
      if (c) c->complete(0);
      return seastar::now();
    });
  } else {
    return seastar::smp::submit_to(
      store.get_owner_shard(),
      [f_store=store.get(), ch=std::move(ch), txn=std::move(txn)]() mutable {
      return f_store->do_transaction_no_callbacks(
        std::move(ch), std::move(txn));
    }).then([original_core, on_commit=std::move(on_commit)]() mutable {
      return seastar::smp::submit_to(original_core, [on_commit=std::move(on_commit)]() mutable {
        auto c = on_commit.release();
        if (c) c->complete(0);
        return seastar::now();
      });
    });
  }
}
}
