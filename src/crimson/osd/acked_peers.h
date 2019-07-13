// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/version.hpp>
#if BOOST_VERSION >= 106900
#include <boost/container/small_vector.hpp>
#else
#include <vector>
#endif

namespace ceph::osd {
  struct peer_shard_t {
    pg_shard_t shard;
    eversion_t last_complete_ondisk;
  };
#if BOOST_VERSION >= 106900
  // small_vector is is_nothrow_move_constructible<> since 1.69
  // 2 + 1 = 3, which is the default value of "osd_pool_default_size"
  using acked_peers_t = boost::container::small_vector<peer_shard_t, 2>;
#else
  using acked_peers_t = std::vector<peer_shard_t>;
#endif
}
