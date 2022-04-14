// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/osd_operations/common/pg_pipeline.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "crimson/osd/pg_map.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class OSD;
class ShardServices;

class ClientRequest final : public PhasedOperationT<ClientRequest>,
                            private CommonClientRequest {
  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDOp> m;
  OpInfo op_info;
  seastar::promise<> on_complete;
  unsigned instance_id = 0;

public:
  class PGPipeline : public CommonPGPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name = "ClientRequest::PGPipeline::await_map";
    } await_map;
    struct WaitRepop : OrderedConcurrentPhaseT<WaitRepop> {
      static constexpr auto type_name = "ClientRequest::PGPipeline::wait_repop";
    } wait_repop;
    struct SendReply : OrderedExclusivePhaseT<SendReply> {
      static constexpr auto type_name = "ClientRequest::PGPipeline::send_reply";
    } send_reply;
    friend class ClientRequest;
    friend class LttngBackend;
    friend class HistoricBackend;
  };

  using ordering_hook_t = boost::intrusive::list_member_hook<>;
  ordering_hook_t ordering_hook;
  class Orderer {
    using list_t = boost::intrusive::list<
      ClientRequest,
      boost::intrusive::member_hook<
	ClientRequest,
	typename ClientRequest::ordering_hook_t,
	&ClientRequest::ordering_hook>
      >;
    list_t list;

  public:
    void add_request(ClientRequest &request) {
      assert(!request.ordering_hook.is_linked());
      intrusive_ptr_add_ref(&request);
      list.push_back(request);
    }
    void remove_request(ClientRequest &request) {
      assert(request.ordering_hook.is_linked());
      list.erase(list_t::s_iterator_to(request));
      intrusive_ptr_release(&request);
    }
    void requeue(ShardServices &shard_services, Ref<PG> pg);
    void clear_and_cancel();
  };
  void complete_request();

  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  ClientRequest(OSD &osd, crimson::net::ConnectionRef, Ref<MOSDOp> &&m);
  ~ClientRequest();

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return m->get_spg();
  }
  ConnectionPipeline &get_connection_pipeline();
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return m->get_min_epoch(); }

  seastar::future<> with_pg_int(
    ShardServices &shard_services, Ref<PG> pg);
public:
  bool same_session_and_pg(const ClientRequest& other_op) const;

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pgref);
private:
  template <typename FuncT>
  interruptible_future<> with_sequencer(FuncT&& func);
  auto reply_op_error(Ref<PG>& pg, int err);

  enum class seq_mode_t {
    IN_ORDER,
    OUT_OF_ORDER
  };

  interruptible_future<seq_mode_t> do_process(
    Ref<PG>& pg,
    crimson::osd::ObjectContextRef obc);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition> process_pg_op(
    Ref<PG> &pg);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition, seq_mode_t> process_op(
    Ref<PG> &pg);
  bool is_pg_op() const;

  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);

  template <typename Errorator>
  using interruptible_errorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      Errorator>;

  bool is_misdirected(const PG& pg) const;

public:
  std::tuple<
    StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    ConnectionPipeline::GetPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    PGPipeline::AwaitMap::BlockingEvent,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
    PGPipeline::WaitForActive::BlockingEvent,
    PGActivationBlocker::BlockingEvent,
    PGPipeline::RecoverMissing::BlockingEvent,
    PGPipeline::GetOBC::BlockingEvent,
    PGPipeline::Process::BlockingEvent,
    PGPipeline::WaitRepop::BlockingEvent,
    PGPipeline::SendReply::BlockingEvent,
    CompletionEvent
  > tracking_events;

  friend class LttngBackend;
  friend class HistoricBackend;
};

}
