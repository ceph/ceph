// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <vector>

namespace crimson::osd {
  struct peer_shard_t {
    pg_shard_t shard;
    eversion_t last_complete_ondisk;
  };
  using acked_peers_t = std::vector<peer_shard_t>;
}
