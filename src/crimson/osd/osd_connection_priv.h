// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <memory>

#include "crimson/common/smp_helpers.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/replicated_request.h"

namespace crimson::osd {

// Forward-declared and held by pointer so this widely-included header need not
// pull in watch.h (which drags in pg.h). Defined in watch.h; the out-of-line
// members below live in watch_conn.cc, where it is complete.
class WatchConState;

struct OSDConnectionPriv : public crimson::net::Connection::user_private_t {
  using crosscore_ordering_t = smp_crosscore_ordering_t<crosscore_type_t::ONE_N>;

  ConnectionPipeline client_request_conn_pipeline;
  ConnectionPipeline peering_request_conn_pipeline;
  ConnectionPipeline replicated_request_conn_pipeline;
  crosscore_ordering_t crosscore_ordering;

  // Per-connection registry of the watches reachable over this connection,
  // used by OSD::ms_handle_reset() to disconnect them on reset. Lazily created
  // on the first watch (most connections never watch anything).
  std::unique_ptr<WatchConState> watch_conn_state;
  WatchConState &ensure_watch_conn_state();

  // out-of-line (defined in watch_conn.cc): WatchConState is incomplete here, so
  // the unique_ptr member's construction/destruction must not be instantiated in
  // includers of this header.
  OSDConnectionPriv();
  ~OSDConnectionPriv();
};

static inline OSDConnectionPriv &get_osd_priv(crimson::net::Connection *conn) {
  if (!conn->has_user_private()) {
    conn->set_user_private(std::make_unique<OSDConnectionPriv>());
  }
  return static_cast<OSDConnectionPriv&>(conn->get_user_private());
}

}
