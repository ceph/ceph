// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "crimson/os/futurized_store.h"

#define TRAMPOLINE(method_name) \
  template <typename... Args> \
  decltype(auto) method_name(this auto&& self, Args&&... args) { \
    return std::forward_like<decltype(self)>(self.wrapped).method_name(std::forward<Args>(args)...); \
  }


namespace crimson::os {

class _RelayStore final : public FuturizedStore {
  class Shard : public FuturizedStore::Shard {
  };

  TRAMPOLINE(start);
  TRAMPOLINE(stop);
  TRAMPOLINE(mount);
  TRAMPOLINE(umount);
  TRAMPOLINE(mkfs);
  TRAMPOLINE(stat);
  TRAMPOLINE(pool_statfs);
  TRAMPOLINE(report_stats);
  TRAMPOLINE(get_fsid);
  TRAMPOLINE(write_meta);
  TRAMPOLINE(read_meta);
  TRAMPOLINE(list_collections);
  TRAMPOLINE(get_default_device_class);
  TRAMPOLINE(get_sharded_store);
  TRAMPOLINE(get_storage_shard_count)
};

} // namespace crimson::os
