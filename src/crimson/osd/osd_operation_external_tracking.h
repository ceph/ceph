// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/client_request.h"
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
    CommonPGPipeline::WaitPGReady::BlockingEvent::Backend,
    CommonPGPipeline::WaitPGReady::BlockingEvent::ExitBarrierEvent::Backend,
    CommonPGPipeline::GetOBC::BlockingEvent::Backend,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    PGMap::PGCreationBlockingEvent::Backend,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    PGActivationBlocker::BlockingEvent::Backend,
    scrub::PGScrubber::BlockingEvent::Backend,
    ClientRequest::CompletionEvent::Backend,
    CommonOBCPipeline::Process::BlockingEvent::Backend,
    CommonOBCPipeline::WaitRepop::BlockingEvent::Backend,
    CommonOBCPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent::Backend,
    CommonOBCPipeline::SendReply::BlockingEvent::Backend,
    PGRepopPipeline::Process::BlockingEvent::Backend,
    PGRepopPipeline::WaitCommit::BlockingEvent::Backend,
    PGRepopPipeline::WaitCommit::BlockingEvent::ExitBarrierEvent::Backend,
    PGRepopPipeline::SendReply::BlockingEvent::Backend
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

  void handle(CommonPGPipeline::WaitPGReady::BlockingEvent& ev,
              const Operation& op,
              const CommonPGPipeline::WaitPGReady& blocker) override {
  }

  void handle(CommonPGPipeline::WaitPGReady::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(CommonPGPipeline::GetOBC::BlockingEvent& ev,
              const Operation& op,
              const CommonPGPipeline::GetOBC& blocker) override {
  }

  void handle(PGMap::PGCreationBlockingEvent&,
              const Operation&,
              const PGMap::PGCreationBlocker&) override {
  }

  void handle(PG_OSDMapGate::OSDMapBlocker::BlockingEvent&,
              const Operation&,
              const PG_OSDMapGate::OSDMapBlocker&) override {
  }

  void handle(PGActivationBlocker::BlockingEvent& ev,
              const Operation& op,
              const PGActivationBlocker& blocker) override {
  }

  void handle(scrub::PGScrubber::BlockingEvent& ev,
              const Operation& op,
              const scrub::PGScrubber& blocker) override {
  }

  void handle(CommonOBCPipeline::Process::BlockingEvent& ev,
              const Operation& op,
              const CommonOBCPipeline::Process& blocker) override {
  }

  void handle(CommonOBCPipeline::WaitRepop::BlockingEvent& ev,
              const Operation& op,
              const CommonOBCPipeline::WaitRepop& blocker) override {
  }

  void handle(CommonOBCPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(CommonOBCPipeline::SendReply::BlockingEvent& ev,
              const Operation& op,
              const CommonOBCPipeline::SendReply& blocker) override {
  }

  void handle(PGRepopPipeline::Process::BlockingEvent& ev,
              const Operation& op,
              const PGRepopPipeline::Process& blocker) override {
  }

  void handle(PGRepopPipeline::WaitCommit::BlockingEvent& ev,
              const Operation& op,
              const PGRepopPipeline::WaitCommit& blocker) override {
  }

  void handle(PGRepopPipeline::WaitCommit::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(PGRepopPipeline::SendReply::BlockingEvent& ev,
              const Operation& op,
              const PGRepopPipeline::SendReply& blocker) override {
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
    CommonPGPipeline::WaitPGReady::BlockingEvent::Backend,
    CommonPGPipeline::WaitPGReady::BlockingEvent::ExitBarrierEvent::Backend,
    CommonPGPipeline::GetOBC::BlockingEvent::Backend,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    PGMap::PGCreationBlockingEvent::Backend,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent::Backend,
    PGActivationBlocker::BlockingEvent::Backend,
    scrub::PGScrubber::BlockingEvent::Backend,
    ClientRequest::CompletionEvent::Backend,
    CommonOBCPipeline::Process::BlockingEvent::Backend,
    CommonOBCPipeline::WaitRepop::BlockingEvent::Backend,
    CommonOBCPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent::Backend,
    CommonOBCPipeline::SendReply::BlockingEvent::Backend,
    PGRepopPipeline::Process::BlockingEvent::Backend,
    PGRepopPipeline::WaitCommit::BlockingEvent::Backend,
    PGRepopPipeline::WaitCommit::BlockingEvent::ExitBarrierEvent::Backend,
    PGRepopPipeline::SendReply::BlockingEvent::Backend
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

  void handle(CommonPGPipeline::WaitPGReady::BlockingEvent& ev,
              const Operation& op,
              const CommonPGPipeline::WaitPGReady& blocker) override {
  }

  void handle(CommonPGPipeline::WaitPGReady::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(CommonPGPipeline::GetOBC::BlockingEvent& ev,
              const Operation& op,
              const CommonPGPipeline::GetOBC& blocker) override {
  }

  void handle(PGMap::PGCreationBlockingEvent&,
              const Operation&,
              const PGMap::PGCreationBlocker&) override {
  }

  void handle(PG_OSDMapGate::OSDMapBlocker::BlockingEvent&,
              const Operation&,
              const PG_OSDMapGate::OSDMapBlocker&) override {
  }

  void handle(PGActivationBlocker::BlockingEvent& ev,
              const Operation& op,
              const PGActivationBlocker& blocker) override {
  }

  void handle(scrub::PGScrubber::BlockingEvent& ev,
              const Operation& op,
              const scrub::PGScrubber& blocker) override {
  }

  static const ClientRequest& to_client_request(const Operation& op) {
#ifdef NDEBUG
    return static_cast<const ClientRequest&>(op);
#else
    return dynamic_cast<const ClientRequest&>(op);
#endif
  }

  void handle(CommonOBCPipeline::Process::BlockingEvent& ev,
              const Operation& op,
              const CommonOBCPipeline::Process& blocker) override {
  }

  void handle(CommonOBCPipeline::WaitRepop::BlockingEvent& ev,
              const Operation& op,
              const CommonOBCPipeline::WaitRepop& blocker) override {
  }

  void handle(CommonOBCPipeline::WaitRepop::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(CommonOBCPipeline::SendReply::BlockingEvent& ev,
              const Operation& op,
              const CommonOBCPipeline::SendReply& blocker) override {
  }

  void handle(PGRepopPipeline::Process::BlockingEvent& ev,
              const Operation& op,
              const PGRepopPipeline::Process& blocker) override {
  }

  void handle(PGRepopPipeline::WaitCommit::BlockingEvent& ev,
              const Operation& op,
              const PGRepopPipeline::WaitCommit& blocker) override {
  }

  void handle(PGRepopPipeline::WaitCommit::BlockingEvent::ExitBarrierEvent& ev,
              const Operation& op) override {
  }

  void handle(PGRepopPipeline::SendReply::BlockingEvent& ev,
              const Operation& op,
              const PGRepopPipeline::SendReply& blocker) override {
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
