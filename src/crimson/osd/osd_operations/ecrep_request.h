// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <variant>

#include "crimson/net/Connection.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/pg_map.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"


namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;

class OSD;
class PG;

class ECRepRequest final : public PhasedOperationT<ECRepRequest> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::ecrep_request;

  template <class MessageRefT>
  ECRepRequest(crimson::net::ConnectionRef&& conn,
               MessageRefT &&req)
    : l_conn{std::move(conn)},
      req{std::forward<MessageRefT>(req)}
  {}

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return std::visit([] (const auto& concrete_req) {
      return concrete_req->get_spg();
    }, req);
  }
  epoch_t get_epoch() const {
    return std::visit([] (const auto& concrete_req) {
      return concrete_req->get_min_epoch();
    }, req);
  }
  PipelineHandle &get_handle() { return handle; }

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

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  std::tuple<
    StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    ConnectionPipeline::GetPGMapping::BlockingEvent,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent
  > tracking_events;

private:
  ClientRequest::PGPipeline &pp(PG &pg);

  crimson::net::ConnectionRef l_conn;
  crimson::net::ConnectionXcoreRef r_conn;
  // must be after `conn` to ensure the ConnectionPipeline's is alive
  PipelineHandle handle;
  std::variant<
    Ref<MOSDECSubOpWrite>,
    Ref<MOSDECSubOpWriteReply>,
    Ref<MOSDECSubOpRead>,
    Ref<MOSDECSubOpReadReply>
  > req;
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::ECRepRequest> : fmt::ostream_formatter {};
#endif
