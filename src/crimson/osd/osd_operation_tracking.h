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

struct LttngBackend
  : EnqueuedEvent::Backend,
    ResponseEvent::Backend,
    ClientRequest::ConnectionPipeline::AwaitMap::TimedPtr::Backend,
    OSDMapGate<OSDMapGateType::OSD>::OSDMapBlocker::TimedPtr::Backend,
    ClientRequest::ConnectionPipeline::GetPG::TimedPtr::Backend,
    OSDMapGate<OSDMapGateType::PG>::OSDMapBlocker::TimedPtr::Backend,
    OperationDone::Backend
{
  void handle(EnqueuedEvent&, const Operation&) override {
    //tracepoint(...);
  }

  void handle(ResponseEvent&, const Operation&) override {
    //tracepoint(...);
  }

  void handle(ClientRequest::ConnectionPipeline::AwaitMap::TimedPtr&,
              const Operation&) override {
  }

  void handle(OSDMapGate<OSDMapGateType::OSD>::OSDMapBlocker::TimedPtr&,
              const Operation&) override {
  }

  void handle(ClientRequest::ConnectionPipeline::GetPG::TimedPtr&,
              const Operation&) override {
  }

  void handle(OSDMapGate<OSDMapGateType::PG>::OSDMapBlocker::TimedPtr&,
              const Operation&) override {
  }

  void handle(OperationDone&, const Operation&) override {
    //tracepoint(...);
  }
};

struct HistoricBackend
  : EnqueuedEvent::Backend,
    ResponseEvent::Backend,
    ClientRequest::ConnectionPipeline::AwaitMap::TimedPtr::Backend,
    OSDMapGate<OSDMapGateType::OSD>::OSDMapBlocker::TimedPtr::Backend,
    ClientRequest::ConnectionPipeline::GetPG::TimedPtr::Backend,
    OSDMapGate<OSDMapGateType::PG>::OSDMapBlocker::TimedPtr::Backend,
    OperationDone::Backend
{
  void handle(EnqueuedEvent&, const Operation&) override {
    //tracepoint(...);
  }

  void handle(ResponseEvent&, const Operation&) override {
    //tracepoint(...);
  }

  void handle(ClientRequest::ConnectionPipeline::AwaitMap::TimedPtr&,
              const Operation&) override {
  }

  void handle(OSDMapGate<OSDMapGateType::OSD>::OSDMapBlocker::TimedPtr&,
              const Operation&) override {
  }

  void handle(ClientRequest::ConnectionPipeline::GetPG::TimedPtr&,
              const Operation&) override {
  }

  void handle(OSDMapGate<OSDMapGateType::PG>::OSDMapBlocker::TimedPtr&,
              const Operation&) override {
  }

  void handle(OperationDone&, const Operation&) override {
    //tracepoint(...);
  }
};

template <>
struct EventBackendRegistry<ClientRequest> {
  static std::tuple<LttngBackend, HistoricBackend> get_backends() {
    return { {}, {} };
  }
};

template <>
struct EventBackendRegistry<PeeringEvent> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<RepRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<RecoverySubRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

} // namespace crimson::osd
