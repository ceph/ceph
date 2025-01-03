// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "os/ObjectStore.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "alien_store.h"

namespace crimson::os {

class AlienCollection final : public FuturizedCollection {
public:
  AlienCollection(ObjectStore::CollectionHandle ch)
  : FuturizedCollection(ch->cid),
    collection(ch) {}

  ~AlienCollection() {}

  template <typename Func, typename Result = std::invoke_result_t<Func>>
  seastar::futurize_t<Result> with_lock(Func&& func) {
    // newer versions of Seastar provide two variants of `with_lock`
    //   - generic, friendly towards throwing move constructors of Func,
    //   - specialized for `noexcept`.
    // unfortunately, the former has a limitation: the return value
    // of `Func` must be compatible with `current_exception_as_future()`
    // which boils down to returning `seastar::future<void>`.
    static_assert(std::is_nothrow_move_constructible_v<Func>);
    return seastar::with_lock(mutex, std::forward<Func>(func));
  }

private:
  ObjectStore::CollectionHandle collection;
  seastar::shared_mutex mutex;
  friend AlienStore;
};
}
