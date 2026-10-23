// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string_view>
#include <variant>

#include "common/config_proxy.h"

namespace ceph {

/// Scale factor for OSD limits that are sized for the default
/// osd_memory_target (4 GiB): 1 at or above it, target / 4 GiB below it.
/// These limits (RocksDB WAL, client message bytes, PG log length) bound
/// memory the cache autotuner cannot shrink, so a smaller target has to
/// shrink them too.
inline double osd_memory_target_scale(const ConfigProxy& conf)
{
  constexpr uint64_t reference = 4ull << 30;  // osd_memory_target's default
  const uint64_t target = conf.get_val<Option::size_t>("osd_memory_target");
  return std::min(1.0, static_cast<double>(target) / reference);
}

/// v scaled by osd_memory_target_scale() raised to power, never below 1
/// unless v is 0.  A power below 1 shrinks v more gently.
inline uint64_t osd_memory_target_scaled(const ConfigProxy& conf, uint64_t v,
                                         double power = 1.0)
{
  if (v == 0) {
    return 0;
  }
  const double scale = std::pow(osd_memory_target_scale(conf), power);
  return std::max<uint64_t>(1, v * scale);
}

/// The value of option key, scaled by osd_memory_target_scale() while it is
/// still at its default.  A value that was set explicitly is used as is.
/// T is the option's type: uint64_t or Option::size_t.
template <typename T>
uint64_t osd_memory_target_scaled(const ConfigProxy& conf, std::string_view key)
{
  const uint64_t v = conf.get_val<T>(key);
  const Option *o = conf.get_schema(key);
  if (!o) {
    return v;
  }
  const auto& def = o->daemon_value != Option::value_t{} ?
    o->daemon_value : o->value;
  const T *d = std::get_if<T>(&def);
  if (!d || v != static_cast<uint64_t>(*d)) {
    return v;
  }
  return osd_memory_target_scaled(conf, v);
}

} // namespace ceph
