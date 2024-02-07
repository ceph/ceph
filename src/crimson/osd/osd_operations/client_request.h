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
#include "crimson/osd/scrub/pg_scrubber.h"
#include "crimson/common/type_helpers.h"
#include "crimson/common/utility.h"
#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class OSD;
class ShardServices;

class ClientRequest final : public PhasedOperationT<ClientRequest>,
                            private CommonClientRequest {
  // Initially set to primary core, updated to pg core after with_pg()
  ShardServices *shard_services = nullptr;

  crimson::net::ConnectionRef l_conn;
  crimson::net::ConnectionXcoreRef r_conn;

  // must be after conn due to ConnectionPipeline's life-time
  Ref<MOSDOp> m;
  OpInfo op_info;
  seastar::promise<> on_complete;
  unsigned instance_id = 0;

public:
  class PGPipeline : public CommonPGPipeline {
    public:
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
    friend class ReqRequest;
    friend class LogMissingRequest;
    friend class LogMissingRequestReply;
  };

  /**
   * instance_handle_t
   *
   * Client request is, at present, the only Operation which can be requeued.
   * This is, mostly, fine.  However, reusing the PipelineHandle or
   * BlockingEvent structures before proving that the prior instance has stopped
   * can create hangs or crashes due to violations of the BlockerT and
   * PipelineHandle invariants.
   *
   * To solve this, we create an instance_handle_t which contains the events
   * for the portion of execution that can be rerun as well as the
   * PipelineHandle.  ClientRequest::with_pg_int grabs a reference to the current
   * instance_handle_t and releases its PipelineHandle in the finally block.
   * On requeue, we create a new instance_handle_t with a fresh PipelineHandle
   * and events tuple and use it and use it for the next invocation of
   * with_pg_int.
   */
  std::tuple<
    StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    ConnectionPipeline::GetPGMapping::BlockingEvent,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    CompletionEvent
  > tracking_events;

  class instance_handle_t : public boost::intrusive_ref_counter<instance_handle_t> {
  public:
    // intrusive_ptr because seastar::lw_shared_ptr includes a cpu debug check
    // that we will fail since the core on which we allocate the request may not
    // be the core on which we perform with_pg_int.  This is harmless, since we
    // don't leave any references on the source core, so we just bypass it by using
    // intrusive_ptr instead.
    using ref_t = boost::intrusive_ptr<instance_handle_t>;
    PipelineHandle handle;

    std::tuple<
      PGPipeline::AwaitMap::BlockingEvent,
      PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
      PGPipeline::WaitForActive::BlockingEvent,
      PGActivationBlocker::BlockingEvent,
      PGPipeline::RecoverMissing::BlockingEvent,
      scrub::PGScrubber::BlockingEvent,
      PGPipeline::GetOBC::BlockingEvent,
      PGPipeline::Process::BlockingEvent,
      PGPipeline::WaitRepop::BlockingEvent,
      PGPipeline::SendReply::BlockingEvent,
      CompletionEvent
      > pg_tracking_events;

    template <typename BlockingEventT, typename InterruptorT=void, typename F>
    auto with_blocking_event(F &&f, ClientRequest &op) {
      auto ret = std::forward<F>(f)(
	typename BlockingEventT::template Trigger<ClientRequest>{
	  std::get<BlockingEventT>(pg_tracking_events), op
	});
      if constexpr (std::is_same_v<InterruptorT, void>) {
	return ret;
      } else {
	using ret_t = decltype(ret);
	return typename InterruptorT::template futurize_t<ret_t>{std::move(ret)};
      }
    }

    template <typename InterruptorT=void, typename StageT>
    auto enter_stage(StageT &stage, ClientRequest &op) {
      return this->template with_blocking_event<
	typename StageT::BlockingEvent,
	InterruptorT>(
	  [&stage, this](auto &&trigger) {
	    return handle.template enter<ClientRequest>(
	      stage, std::move(trigger));
	  }, op);
    }

    template <
      typename InterruptorT=void, typename BlockingObj, typename Method,
      typename... Args>
    auto enter_blocker(
      ClientRequest &op, BlockingObj &obj, Method method, Args&&... args) {
      return this->template with_blocking_event<
	typename BlockingObj::Blocker::BlockingEvent,
	InterruptorT>(
	  [&obj, method,
	   args=std::forward_as_tuple(std::move(args)...)](auto &&trigger) mutable {
	    return apply_method_to_tuple(
	      obj, method,
	      std::tuple_cat(
		std::forward_as_tuple(std::move(trigger)),
		std::move(args))
	    );
	  }, op);
    }
  };
  instance_handle_t::ref_t instance_handle;
  void reset_instance_handle() {
    instance_handle = new instance_handle_t;
  }
  auto get_instance_handle() { return instance_handle; }

  std::set<snapid_t> snaps_need_to_recover() {
    std::set<snapid_t> ret;
    auto target = m->get_hobj();
    if (!target.is_head()) {
      ret.insert(target.snap);
    }
    for (auto &op : m->ops) {
      if (op.op.op == CEPH_OSD_OP_ROLLBACK) {
	ret.insert((snapid_t)op.op.snap.snapid);
      }
    }
    return ret;
  }

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
    void requeue(Ref<PG> pg);
    void clear_and_cancel(PG &pg);
  };
  void complete_request();

  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  ClientRequest(
    ShardServices &shard_services,
    crimson::net::ConnectionRef, Ref<MOSDOp> &&m);
  ~ClientRequest();

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return m->get_spg();
  }
  PipelineHandle &get_handle() { return instance_handle->handle; }
  epoch_t get_epoch() const { return m->get_min_epoch(); }

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

  interruptible_future<> with_pg_process_interruptible(
    Ref<PG> pgref, const unsigned instance_id, instance_handle_t &ihref);

  seastar::future<> with_pg_process(Ref<PG> pg);

public:
  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pgref);

private:
  template <typename FuncT>
  interruptible_future<> with_sequencer(FuncT&& func);
  interruptible_future<> reply_op_error(const Ref<PG>& pg, int err);

  interruptible_future<> do_process(
    instance_handle_t &ihref,
    Ref<PG> pg,
    crimson::osd::ObjectContextRef obc,
    unsigned this_instance_id);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition> process_pg_op(
    Ref<PG> pg);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition> process_op(
      instance_handle_t &ihref,
      Ref<PG> pg,
      unsigned this_instance_id);
  bool is_pg_op() const;

  PGPipeline &client_pp(PG &pg);

  template <typename Errorator>
  using interruptible_errorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      Errorator>;

  bool is_misdirected(const PG& pg) const;

  const SnapContext get_snapc(
    PG &pg,
    crimson::osd::ObjectContextRef obc) const;

public:

  friend class LttngBackend;
  friend class HistoricBackend;

  auto get_started() const {
    return get_event<StartEvent>().get_timestamp();
  };

  auto get_completed() const {
    return get_event<CompletionEvent>().get_timestamp();
  };

  void put_historic() const;
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::ClientRequest> : fmt::ostream_formatter {};
#endif
