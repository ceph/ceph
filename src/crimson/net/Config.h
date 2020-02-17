// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// XXX: a poor man's md_config_t
#pragma once

#include "include/msgr.h"
#include <chrono>

namespace crimson::net {

using namespace std::literals::chrono_literals;

constexpr struct simple_md_config_t {
  uint32_t host_type = CEPH_ENTITY_TYPE_OSD;
  bool cephx_require_signatures = false;
  bool cephx_cluster_require_signatures = false;
  bool cephx_service_require_signatures = false;
  bool ms_die_on_old_message = true;
  bool ms_die_on_skipped_message = true;
  double ms_initial_backoff = .2;
  double ms_max_backoff = 15.0;
  std::chrono::milliseconds threadpool_empty_queue_max_wait = 100ms;
  size_t osd_client_message_size_cap = 500ULL << 20;
} conf;
}
