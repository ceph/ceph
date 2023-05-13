// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
#include <random>

#include <seastar/core/future.hh>

#include "common/ceph_argparse.h"
#include "include/expected.hpp"
#include "include/random.h"

namespace crimson::osd {

void usage(const char* prog);

inline uint64_t get_nonce()
{
  return ceph::util::generate_random_number<uint64_t>();
}

seastar::future<> populate_config_from_mon();

struct early_config_t {
  std::vector<std::string> early_args;
  std::vector<std::string> ceph_args;

  std::string cluster_name{"ceph"};
  std::string conf_file_list;
  CephInitParameters init_params{CEPH_ENTITY_TYPE_OSD};

  /// Returned vector must not outlive in
  auto to_ptr_vector(const std::vector<std::string> &in) {
    std::vector<const char *> ret;
    ret.reserve(in.size());
    std::transform(
      std::begin(in), std::end(in),
      std::back_inserter(ret),
      [](const auto &str) { return str.c_str(); });
    return ret;
  }

  std::vector<const char *> get_early_args() {
    return to_ptr_vector(early_args);
  }

  std::vector<const char *> get_ceph_args() {
    return to_ptr_vector(ceph_args);
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(early_args, bl);
    encode(ceph_args, bl);
    encode(cluster_name, bl);
    encode(conf_file_list, bl);
    encode(init_params, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(early_args, bl);
    decode(ceph_args, bl);
    decode(cluster_name, bl);
    decode(conf_file_list, bl);
    decode(init_params, bl);
    DECODE_FINISH(bl);
  }
};

/**
 * get_early_config
 *
 * Compile initial configuration information from command line arguments,
 * config files, and monitors.
 *
 * This implementation forks off a worker process to do this work and must
 * therefore be called very early in main().  (See implementation for an
 * explanation).
 */
tl::expected<early_config_t, int>
get_early_config(int argc, const char *argv[]);

}

WRITE_CLASS_ENCODER(crimson::osd::early_config_t)
