// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>

#include "osd/osd_types.h"

namespace crimson::osd {
  struct peer_shard_t {
    pg_shard_t shard;
    eversion_t last_complete_ondisk;
  };
  using acked_peers_t = std::vector<peer_shard_t>;

  std::ostream& operator<<(std::ostream &out, const peer_shard_t &pshard);
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::peer_shard_t> : fmt::ostream_formatter {};
#endif
