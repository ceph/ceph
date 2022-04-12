// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "crimson/osd/pg_map.h"

namespace crimson::osd {

// Just the boilerplate currently. Implementing
struct LttngBackend
  : ClientRequest::StartEvent::Backend,
    ClientRequest::ConnectionPipeline::AwaitMap::BlockingEvent::Backend,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    ClientRequest::ConnectionPipeline::GetPG::BlockingEvent::Backend,
    PGMap::PGCreationBlockingEvent::Backend,
    ClientRequest::PGPipeline::AwaitMap::BlockingEvent::Backend,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitForActive::BlockingEvent::Backend,
    PGActivationBlocker::BlockingEvent::Backend,
    ClientRequest::PGPipeline::RecoverMissing::BlockingEvent::Backend,
    ClientRequest::PGPipeline::GetOBC::BlockingEvent::Backend,
    ClientRequest::PGPipeline::Process::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitRepop::BlockingEvent::Backend,
    ClientRequest::PGPipeline::SendReply::BlockingEvent::Backend,
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

  void handle(ClientRequest::ConnectionPipeline::GetPG::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::ConnectionPipeline::GetPG& blocker) override {
  }

  void handle(PGMap::PGCreationBlockingEvent&,
              const Operation&,
              const PGMap::PGCreationBlocker&) override {
  }

  void handle(ClientRequest::PGPipeline::AwaitMap::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::AwaitMap& blocker) override {
  }

  void handle(PG_OSDMapGate::OSDMapBlocker::BlockingEvent&,
              const Operation&,
              const PG_OSDMapGate::OSDMapBlocker&) override {
  }

  void handle(ClientRequest::PGPipeline::WaitForActive::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::WaitForActive& blocker) override {
  }

  void handle(PGActivationBlocker::BlockingEvent& ev,
              const Operation& op,
              const PGActivationBlocker& blocker) override {
  }

  void handle(ClientRequest::PGPipeline::RecoverMissing::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::RecoverMissing& blocker) override {
  }

  void handle(ClientRequest::PGPipeline::GetOBC::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::GetOBC& blocker) override {
  }

  void handle(ClientRequest::PGPipeline::Process::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::Process& blocker) override {
  }

  void handle(ClientRequest::PGPipeline::WaitRepop::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::WaitRepop& blocker) override {
  }

  void handle(ClientRequest::PGPipeline::SendReply::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::SendReply& blocker) override {
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
