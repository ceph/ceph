#include "crimson/os/futurized_store.h"
#include "relay_store.h"

#include "common/JSONFormatter.h"
#include "common/safe_io.h"
#include "os/Transaction.h"

#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/perf_counters_collection.h"
#include "crimson/common/config_proxy.h"

#include <string>
#include <unordered_map>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"
#include "osd/osd_types.h"

#include "crimson/os/futurized_collection.h"

namespace crimson::os {

seastar::future<> _RelayStore::Shard::with_store_do_transaction(
  FuturizedStore::Shard::CollectionRef ch,
  ceph::os::Transaction&& txn)
{
  std::unique_ptr<Context> on_commit(
    ceph::os::Transaction::collect_all_contexts(txn));
  const auto original_core = seastar::this_shard_id();
  const auto store_shard_id = original_core % shard_count;
  if (store_shard_id == original_core || store_shard_id == GLOBAL_STORE) {
    return do_transaction_no_callbacks(
      std::move(ch), std::move(txn)
    ).then([on_commit=std::move(on_commit)]() mutable {
      auto c = on_commit.release();
      if (c) c->complete(0);
      return seastar::now();
    });
  } else {
    return seastar::smp::submit_to(
      store_shard_id,
      [this, ch=std::move(ch), txn=std::move(txn)]() mutable {
      return do_transaction_no_callbacks(
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
} // namespace crimson::os
