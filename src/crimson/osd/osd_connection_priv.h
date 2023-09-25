// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/smp.hh>

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/replicated_request.h"

namespace crimson::osd {

/**
 * crosscore_ordering_t
 *
 * To preserve the event order from 1 source to n target cores.
 */
class crosscore_ordering_t {
public:
  using seq_t = uint64_t;

  crosscore_ordering_t()
    : out_seqs(seastar::smp::count, 0),
      in_controls(seastar::smp::count) {}

  ~crosscore_ordering_t() = default;

  // Called by the original core to get the ordering sequence
  seq_t prepare_submit(core_id_t target_core) {
    auto &out_seq = out_seqs[target_core];
    ++out_seq;
    return out_seq;
  }

  /*
   * Called by the target core to preserve the ordering
   */

  seq_t get_in_seq() const {
    auto core = seastar::this_shard_id();
    return in_controls[core].seq;
  }

  bool proceed_or_wait(seq_t seq) {
    auto core = seastar::this_shard_id();
    auto &in_control = in_controls[core];
    if (seq == in_control.seq + 1) {
      ++in_control.seq;
      if (unlikely(in_control.pr_wait.has_value())) {
        in_control.pr_wait->set_value();
        in_control.pr_wait = std::nullopt;
      }
      return true;
    } else {
      return false;
    }
  }

  seastar::future<> wait(seq_t seq) {
    auto core = seastar::this_shard_id();
    auto &in_control = in_controls[core];
    assert(seq != in_control.seq + 1);
    if (!in_control.pr_wait.has_value()) {
      in_control.pr_wait = seastar::shared_promise<>();
    }
    return in_control.pr_wait->get_shared_future();
  }

private:
  struct in_control_t {
    seq_t seq = 0;
    std::optional<seastar::shared_promise<>> pr_wait;
  };

  // source-side
  std::vector<seq_t> out_seqs;
  // target-side
  std::vector<in_control_t> in_controls;
};

struct OSDConnectionPriv : public crimson::net::Connection::user_private_t {
  ConnectionPipeline client_request_conn_pipeline;
  ConnectionPipeline peering_request_conn_pipeline;
  ConnectionPipeline replicated_request_conn_pipeline;
  crosscore_ordering_t crosscore_ordering;
};

static inline OSDConnectionPriv &get_osd_priv(crimson::net::Connection *conn) {
  if (!conn->has_user_private()) {
    conn->set_user_private(std::make_unique<OSDConnectionPriv>());
  }
  return static_cast<OSDConnectionPriv&>(conn->get_user_private());
}

}
