// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_operations/replicated_request.h"

namespace crimson::osd {

// Just the boilerplate currently. Implementing
struct LttngBackend
  : ClientRequest::StartEvent::Backend,
    ClientRequest::ConnectionPipeline::AwaitMap::BlockingEvent::Backend,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    ClientRequest::CompletionEvent::Backend
{
  void handle(ClientRequest::StartEvent&,
              const Operation&) override {}

  void handle(ClientRequest::ConnectionPipeline::AwaitMap::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::ConnectionPipeline::AwaitMap& blocker) override {
  }

  void handle(OSD_OSDMapGate::OSDMapBlocker::BlockingEvent&,
              const Operation&,
              const OSD_OSDMapGate::OSDMapBlocker&) override {
  }

  void handle(ClientRequest::CompletionEvent&,
              const Operation&) override {}
};

} // namespace crimson::osd

namespace crimson {

template <>
struct EventBackendRegistry<osd::ClientRequest> {
  static std::tuple<osd::LttngBackend/*, HistoricBackend*/> get_backends() {
    return { {} };
  }
};

template <>
struct EventBackendRegistry<osd::PeeringEvent> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::RepRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::RecoverySubRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

} // namespace crimson
