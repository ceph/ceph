// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "osd/osd_types.h"
#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class OSD;
class ShardServices;
class PG;
class BackfillRecovery;

  struct PGPeeringPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name = "PeeringEvent::PGPipeline::await_map";
    } await_map;
    struct Process : OrderedExclusivePhaseT<Process> {
      static constexpr auto type_name = "PeeringEvent::PGPipeline::process";
    } process;
  };

template <class T>
class PeeringEvent : public PhasedOperationT<T> {
  T* that() {
    return static_cast<T*>(this);
  }
  const T* that() const {
    return static_cast<const T*>(this);
  }

public:
  static constexpr OperationTypeCode type = OperationTypeCode::peering_event;

protected:
  PGPeeringPipeline &peering_pp(PG &pg);

  PeeringCtx ctx;
  pg_shard_t from;
  spg_t pgid;
  float delay = 0;
  PGPeeringEvent evt;

  const pg_shard_t get_from() const {
    return from;
  }

  const spg_t get_pgid() const {
    return pgid;
  }

  const PGPeeringEvent &get_event() const {
    return evt;
  }

  virtual void on_pg_absent(ShardServices &);

  virtual typename PeeringEvent::template interruptible_future<>
  complete_rctx(ShardServices &, Ref<PG>);

  virtual seastar::future<> complete_rctx_no_pg(
    ShardServices &shard_services
  ) { return seastar::now();}

public:
  template <typename... Args>
  PeeringEvent(
    const pg_shard_t &from, const spg_t &pgid,
    Args&&... args) :
    from(from),
    pgid(pgid),
    evt(std::forward<Args>(args)...)
  {}
  template <typename... Args>
  PeeringEvent(
    const pg_shard_t &from, const spg_t &pgid,
    float delay, Args&&... args) :
    from(from),
    pgid(pgid),
    delay(delay),
    evt(std::forward<Args>(args)...)
  {}

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);
};

class RemotePeeringEvent : public PeeringEvent<RemotePeeringEvent> {
protected:
  crimson::net::ConnectionRef l_conn;
  crimson::net::ConnectionXcoreRef r_conn;

  // must be after conn due to ConnectionPipeline's life-time
  PipelineHandle handle;

  void on_pg_absent(ShardServices &) final;
  PeeringEvent::interruptible_future<> complete_rctx(
    ShardServices &shard_services,
    Ref<PG> pg) override;
  seastar::future<> complete_rctx_no_pg(
    ShardServices &shard_services
  ) override;

public:
  template <typename... Args>
  RemotePeeringEvent(crimson::net::ConnectionRef conn, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    l_conn(conn)
  {}

  std::tuple<
    StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    ConnectionPipeline::GetPGMapping::BlockingEvent,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    PGPeeringPipeline::AwaitMap::BlockingEvent,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
    PGPeeringPipeline::Process::BlockingEvent,
    CompletionEvent
  > tracking_events;

  static constexpr bool can_create() { return true; }
  auto get_create_info() { return std::move(evt.create_info); }
  spg_t get_pgid() const {
    return pgid;
  }
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return evt.get_epoch_sent(); }

  ConnectionPipeline &get_connection_pipeline();

  PerShardPipeline &get_pershard_pipeline(ShardServices &);

  crimson::net::Connection &get_local_connection() {
    assert(l_conn);
    assert(!r_conn);
    return *l_conn;
  };

  crimson::net::Connection &get_foreign_connection() {
    assert(r_conn);
    assert(!l_conn);
    return *r_conn;
  };

  crimson::net::ConnectionFFRef prepare_remote_submission() {
    assert(l_conn);
    assert(!r_conn);
    auto ret = seastar::make_foreign(std::move(l_conn));
    l_conn.reset();
    return ret;
  }

  void finish_remote_submission(crimson::net::ConnectionFFRef conn) {
    assert(conn);
    assert(!l_conn);
    assert(!r_conn);
    r_conn = make_local_shared_foreign(std::move(conn));
  }
};

class LocalPeeringEvent final : public PeeringEvent<LocalPeeringEvent> {
protected:
  Ref<PG> pg;
  PipelineHandle handle;

public:
  template <typename... Args>
  LocalPeeringEvent(Ref<PG> pg, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    pg(pg)
  {}

  seastar::future<> start();
  virtual ~LocalPeeringEvent();

  PipelineHandle &get_handle() { return handle; }

  std::tuple<
    StartEvent,
    PGPeeringPipeline::AwaitMap::BlockingEvent,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
    PGPeeringPipeline::Process::BlockingEvent,
    CompletionEvent
  > tracking_events;
};


}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::LocalPeeringEvent> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::osd::RemotePeeringEvent> : fmt::ostream_formatter {};
template <class T> struct fmt::formatter<crimson::osd::PeeringEvent<T>> : fmt::ostream_formatter {};
#endif
