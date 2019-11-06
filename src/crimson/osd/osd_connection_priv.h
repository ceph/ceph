// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/replicated_request.h"

namespace crimson::osd {

struct OSDConnectionPriv : public crimson::net::Connection::user_private_t {
  ClientRequest::ConnectionPipeline client_request_conn_pipeline;
  RemotePeeringEvent::ConnectionPipeline peering_request_conn_pipeline;
  RepRequest::ConnectionPipeline replicated_request_conn_pipeline;
};

static OSDConnectionPriv &get_osd_priv(crimson::net::Connection *conn) {
  if (!conn->has_user_private()) {
    conn->set_user_private(std::make_unique<OSDConnectionPriv>());
  }
  return static_cast<OSDConnectionPriv&>(conn->get_user_private());
}

}
