// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/smp_helpers.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/replicated_request.h"

namespace crimson::osd {

struct OSDConnectionPriv : public crimson::net::Connection::user_private_t {
  using crosscore_ordering_t = smp_crosscore_ordering_t<crosscore_type_t::ONE_N>;

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
