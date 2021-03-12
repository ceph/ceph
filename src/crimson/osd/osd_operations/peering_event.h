// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

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

class PeeringEvent : public OperationT<PeeringEvent> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::peering_event;

  class PGPipeline {
    OrderedExclusivePhase await_map = {
      "PeeringEvent::PGPipeline::await_map"
    };
    OrderedExclusivePhase process = {
      "PeeringEvent::PGPipeline::process"
    };
    friend class PeeringEvent;
    friend class PGAdvanceMap;
  };

protected:
  PipelineHandle handle;
  PGPipeline &pp(PG &pg);

  ShardServices &shard_services;
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

  virtual void on_pg_absent();
  virtual seastar::future<> complete_rctx(Ref<PG>);
  virtual seastar::future<Ref<PG>> get_pg() = 0;

public:
  template <typename... Args>
  PeeringEvent(
    ShardServices &shard_services, const pg_shard_t &from, const spg_t &pgid,
    Args&&... args) :
    shard_services(shard_services),
    from(from),
    pgid(pgid),
    evt(std::forward<Args>(args)...)
  {}
  template <typename... Args>
  PeeringEvent(
    ShardServices &shard_services, const pg_shard_t &from, const spg_t &pgid,
    float delay, Args&&... args) :
    shard_services(shard_services),
    from(from),
    pgid(pgid),
    delay(delay),
    evt(std::forward<Args>(args)...)
  {}

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();
};

class RemotePeeringEvent : public PeeringEvent {
protected:
  OSD &osd;
  crimson::net::ConnectionRef conn;

  void on_pg_absent() final;
  seastar::future<> complete_rctx(Ref<PG> pg) override;
  seastar::future<Ref<PG>> get_pg() final;

public:
  class ConnectionPipeline {
    OrderedExclusivePhase await_map = {
      "PeeringRequest::ConnectionPipeline::await_map"
    };
    OrderedExclusivePhase get_pg = {
      "PeeringRequest::ConnectionPipeline::get_pg"
    };
    friend class RemotePeeringEvent;
  };

  template <typename... Args>
  RemotePeeringEvent(OSD &osd, crimson::net::ConnectionRef conn, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    osd(osd),
    conn(conn)
  {}

private:
  ConnectionPipeline &cp();
};

class LocalPeeringEvent final : public PeeringEvent {
protected:
  seastar::future<Ref<PG>> get_pg() final;

  Ref<PG> pg;

public:
  template <typename... Args>
  LocalPeeringEvent(Ref<PG> pg, Args&&... args) :
    PeeringEvent(std::forward<Args>(args)...),
    pg(pg)
  {}

  virtual ~LocalPeeringEvent();
};


}
