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
    ClientRequest::DoneEvent::Backend
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

  void handle(ClientRequest::DoneEvent&, const Operation&) override {
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
    ClientRequest::DoneEvent::Backend
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

  // TODO: make this a configuration parameter.
  static const std::size_t historic_op_registry_max_size = 42;

  void handle(ClientRequest::DoneEvent&, const Operation& op) override {
    // TODO: static_cast for production builds
    const auto& client_request = dynamic_cast<const ClientRequest&>(op);
    auto& main_registry = client_request.osd.get_shard_services().registry;
    // create a historic op and release the smart pointer. It will be
    // re-acquired (via the historic registry) when it's the purge time.
    main_registry.create_operation<HistoricClientRequest>(
      ClientRequest::ICRef(&client_request, /* add_ref= */true)
    ).detach();

    // check whether the history size limit is not exceeded; if so, then
    // purge the oldest op.
    // NOTE: Operation uses the auto-unlink feature of boost::intrusive.
    constexpr auto historic_registry_index =
      static_cast<int>(OperationTypeCode::historic_client_request);
    const auto& historic_registry =
      main_registry.registries[historic_registry_index];
    if (historic_registry.size() > historic_op_registry_max_size) {
      const auto& oldest_historic_op =
        static_cast<const HistoricClientRequest&>(historic_registry.front());
      HistoricClientRequest::ICRef(&oldest_historic_op, /* add_ref= */false);
    }
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
