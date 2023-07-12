// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/ecrep_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/osd_operations/snaptrim_event.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "crimson/osd/pg_map.h"
#include "crimson/osd/scrub/pg_scrubber.h"

namespace crimson::osd {

// Just the boilerplate currently. Implementing
struct LttngBackend
  : ClientRequest::StartEvent::Backend,
    ConnectionPipeline::AwaitActive::BlockingEvent::Backend,
    ConnectionPipeline::AwaitMap::BlockingEvent::Backend,
    ConnectionPipeline::GetPGMapping::BlockingEvent::Backend,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent::Backend,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    PGMap::PGCreationBlockingEvent::Backend,
    ClientRequest::PGPipeline::AwaitMap::BlockingEvent::Backend,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitForActive::BlockingEvent::Backend,
    PGActivationBlocker::BlockingEvent::Backend,
    scrub::PGScrubber::BlockingEvent::Backend,
    ClientRequest::PGPipeline::RecoverMissing::BlockingEvent::Backend,
    ClientRequest::PGPipeline::GetOBC::BlockingEvent::Backend,
    ClientRequest::PGPipeline::Process::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitRepop::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent::Backend,
    ClientRequest::PGPipeline::SendReply::BlockingEvent::Backend,
    ClientRequest::CompletionEvent::Backend
{
  void handle(ClientRequest::StartEvent&,
              const Operation&) override {}

  void handle(ConnectionPipeline::AwaitActive::BlockingEvent& ev,
              const Operation& op,
              const ConnectionPipeline::AwaitActive& blocker) override {
  }

  void handle(ConnectionPipeline::AwaitMap::BlockingEvent& ev,
              const Operation& op,
              const ConnectionPipeline::AwaitMap& blocker) override {
  }

  void handle(OSD_OSDMapGate::OSDMapBlocker::BlockingEvent&,
              const Operation&,
              const OSD_OSDMapGate::OSDMapBlocker&) override {
  }

  void handle(ConnectionPipeline::GetPGMapping::BlockingEvent& ev,
              const Operation& op,
              const ConnectionPipeline::GetPGMapping& blocker) override {
  }

  void handle(PerShardPipeline::CreateOrWaitPG::BlockingEvent& ev,
              const Operation& op,
              const PerShardPipeline::CreateOrWaitPG& blocker) override {
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

  void handle(scrub::PGScrubber::BlockingEvent& ev,
              const Operation& op,
              const scrub::PGScrubber& blocker) override {
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

  void handle(ClientRequest::PGPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(ClientRequest::PGPipeline::SendReply::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::SendReply& blocker) override {
  }

  void handle(ClientRequest::CompletionEvent&,
              const Operation&) override {}
};

struct HistoricBackend
  : ClientRequest::StartEvent::Backend,
    ConnectionPipeline::AwaitActive::BlockingEvent::Backend,
    ConnectionPipeline::AwaitMap::BlockingEvent::Backend,
    ConnectionPipeline::GetPGMapping::BlockingEvent::Backend,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent::Backend,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    PGMap::PGCreationBlockingEvent::Backend,
    ClientRequest::PGPipeline::AwaitMap::BlockingEvent::Backend,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitForActive::BlockingEvent::Backend,
    PGActivationBlocker::BlockingEvent::Backend,
    scrub::PGScrubber::BlockingEvent::Backend,
    ClientRequest::PGPipeline::RecoverMissing::BlockingEvent::Backend,
    ClientRequest::PGPipeline::GetOBC::BlockingEvent::Backend,
    ClientRequest::PGPipeline::Process::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitRepop::BlockingEvent::Backend,
    ClientRequest::PGPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent::Backend,
    ClientRequest::PGPipeline::SendReply::BlockingEvent::Backend,
    ClientRequest::CompletionEvent::Backend
{
  void handle(ClientRequest::StartEvent&,
              const Operation&) override {}

  void handle(ConnectionPipeline::AwaitActive::BlockingEvent& ev,
              const Operation& op,
              const ConnectionPipeline::AwaitActive& blocker) override {
  }

  void handle(ConnectionPipeline::AwaitMap::BlockingEvent& ev,
              const Operation& op,
              const ConnectionPipeline::AwaitMap& blocker) override {
  }

  void handle(OSD_OSDMapGate::OSDMapBlocker::BlockingEvent&,
              const Operation&,
              const OSD_OSDMapGate::OSDMapBlocker&) override {
  }

  void handle(ConnectionPipeline::GetPGMapping::BlockingEvent& ev,
              const Operation& op,
              const ConnectionPipeline::GetPGMapping& blocker) override {
  }

  void handle(PerShardPipeline::CreateOrWaitPG::BlockingEvent& ev,
              const Operation& op,
              const PerShardPipeline::CreateOrWaitPG& blocker) override {
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

  void handle(scrub::PGScrubber::BlockingEvent& ev,
              const Operation& op,
              const scrub::PGScrubber& blocker) override {
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

  void handle(ClientRequest::PGPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(ClientRequest::PGPipeline::SendReply::BlockingEvent& ev,
              const Operation& op,
              const ClientRequest::PGPipeline::SendReply& blocker) override {
  }

  static const ClientRequest& to_client_request(const Operation& op) {
#ifdef NDEBUG
    return static_cast<const ClientRequest&>(op);
#else
    return dynamic_cast<const ClientRequest&>(op);
#endif
  }

  void handle(ClientRequest::CompletionEvent&, const Operation& op) override {
    if (crimson::common::local_conf()->osd_op_history_size) {
      to_client_request(op).put_historic();
    }
  }
};

} // namespace crimson::osd

namespace crimson {

template <>
struct EventBackendRegistry<osd::ECRepRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::ClientRequest> {
  static std::tuple<osd::LttngBackend, osd::HistoricBackend> get_backends() {
    return { {}, {} };
  }
};

template <>
struct EventBackendRegistry<osd::RemotePeeringEvent> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::LocalPeeringEvent> {
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
struct EventBackendRegistry<osd::LogMissingRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::LogMissingRequestReply> {
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

template <>
struct EventBackendRegistry<osd::BackfillRecovery> {
  static std::tuple<> get_backends() {
    return {};
  }
};

template <>
struct EventBackendRegistry<osd::PGAdvanceMap> {
  static std::tuple<> get_backends() {
    return {};
  }
};

template <>
struct EventBackendRegistry<osd::SnapTrimObjSubEvent> {
  static std::tuple<> get_backends() {
    return {};
  }
};

} // namespace crimson
