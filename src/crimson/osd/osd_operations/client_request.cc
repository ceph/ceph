// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "crimson/common/coroutine.h"
#include "crimson/common/exception.h"
#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_connection_priv.h"
#include "osd/object_state_fmt.h"
#include "osd/osd_perf_counters.h"

SET_SUBSYS(osd);

namespace crimson::osd {


void ClientRequest::Orderer::requeue(Ref<PG> pg)
{
  LOG_PREFIX(ClientRequest::Orderer::requeue);
  std::list<ClientRequest*> to_requeue;
  for (auto &req : list) {
    to_requeue.emplace_back(&req);
  }
  // Client requests might be destroyed in the following
  // iteration leading to short lived dangling pointers
  // to those requests, but this doesn't hurt as we won't
  // dereference those dangling pointers.
  for (auto req: to_requeue) {
    DEBUGDPP("requeueing {}", *pg, *req);
    req->reset_instance_handle();
    std::ignore = req->with_pg_process(pg);
  }
}

void ClientRequest::Orderer::clear_and_cancel(PG &pg)
{
  LOG_PREFIX(ClientRequest::Orderer::clear_and_cancel);
  for (auto i = list.begin(); i != list.end(); ) {
    auto &req = *i;
    DEBUGDPP("{}", pg, req);
    ++i;
    req.complete_request(pg);
  }
}

void ClientRequest::complete_request(PG &pg)
{
  track_event<CompletionEvent>();
  pg.client_request_orderer.remove_request(*this);
  on_complete.set_value();
}

ClientRequest::ClientRequest(
  ShardServices &_shard_services, crimson::net::ConnectionRef conn,
  Ref<MOSDOp> &&m)
  : shard_services(&_shard_services),
    l_conn(std::move(conn)),
    m(std::move(m)),
    instance_handle(new instance_handle_t)
{}

ClientRequest::~ClientRequest()
{
  LOG_PREFIX(~ClientRequest);
  DEBUG("{}: destroying", *this);
}

void ClientRequest::print(std::ostream &lhs) const
{
  lhs << "m=[" << *m << "]";
}

void ClientRequest::dump_detail(Formatter *f) const
{
  LOG_PREFIX(ClientRequest::dump_detail);
  TRACE("{}: dumping", *this);
  std::apply([f] (auto... event) {
    (..., event.dump(f));
  }, tracking_events);
  std::apply([f] (auto... event) {
    (..., event.dump(f));
  }, get_instance_handle()->pg_tracking_events);
}

ConnectionPipeline &ClientRequest::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).client_request_conn_pipeline;
}

PerShardPipeline &ClientRequest::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_client_request_pipeline();
}

CommonPGPipeline &ClientRequest::client_pp(PG &pg)
{
  return pg.request_pg_pipeline;
}

bool ClientRequest::is_pg_op() const
{
  return std::any_of(
    begin(m->ops), end(m->ops),
    [](auto& op) { return ceph_osd_op_type_pg(op.op.op); });
}

ClientRequest::interruptible_future<>
ClientRequest::reply_op_error(const Ref<PG>& pg, int err)
{
  LOG_PREFIX(ClientRequest::reply_op_error);
  DEBUGDPP("{}: replying with error {}", *pg, *this, err);
  auto reply = crimson::make_message<MOSDOpReply>(
    m.get(), err, pg->get_osdmap_epoch(),
    m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK),
    !m->has_flag(CEPH_OSD_FLAG_RETURNVEC));
  reply->set_reply_versions(eversion_t(), 0);
  reply->set_op_returns(std::vector<pg_log_op_return_item_t>{});
  // TODO: gate the crosscore sending
  return interruptor::make_interruptible(
    get_foreign_connection().send_with_throttling(std::move(reply))
  );
}

ClientRequest::interruptible_future<> ClientRequest::with_pg_process_interruptible(
  Ref<PG> pgref, const unsigned this_instance_id, instance_handle_t &ihref)
{
  LOG_PREFIX(ClientRequest::with_pg_process);
  DEBUGDPP(
    "{}: same_interval_since: {}",
    *pgref, *this, pgref->get_interval_start_epoch());

  DEBUGDPP("{} start", *pgref, *this);
  PG &pg = *pgref;

  DEBUGDPP("{}.{}: entering wait_pg_ready stage",
	   *pgref, *this, this_instance_id);
  // The prior stage is OrderedExclusive (PerShardPipeline::create_or_wait_pg)
  // and wait_pg_ready is OrderedConcurrent.  This transition, therefore, cannot
  // block and using enter_stage_sync is legal and more efficient than
  // enter_stage.
  ihref.enter_stage_sync(client_pp(pg).wait_pg_ready, *this);

  if (!m->get_hobj().get_key().empty()) {
    // There are no users of locator. It was used to ensure that multipart-upload
    // parts would end up in the same PG so that they could be clone_range'd into
    // the same object via librados, but that's not how multipart upload works
    // anymore and we no longer support clone_range via librados.
    co_await reply_op_error(pgref, -ENOTSUP);
    co_return;
  }
  if (pg.can_discard_op(*m)) {
    co_await interruptor::make_interruptible(
      shard_services->send_incremental_map(
	std::ref(get_foreign_connection()), m->get_map_epoch()
      ));
    DEBUGDPP("{}: discarding {}", *pgref, *this, this_instance_id);
    co_return;
  }

  auto map_epoch = co_await interruptor::make_interruptible(
    ihref.enter_blocker(
      *this, pg.osdmap_gate, &decltype(pg.osdmap_gate)::wait_for_map,
      m->get_min_epoch(), nullptr));

  DEBUGDPP("{}.{}: waited for epoch {}, waiting for active",
	   pg, *this, this_instance_id, map_epoch);
  co_await interruptor::make_interruptible(
    ihref.enter_blocker(
      *this,
      pg.wait_for_active_blocker,
      &decltype(pg.wait_for_active_blocker)::wait));

  co_await ihref.enter_stage<interruptor>(client_pp(pg).get_obc, *this);

  if (int res = op_info.set_from_op(&*m, *pg.get_osdmap());
      res != 0) {
    co_await reply_op_error(pgref, res);
    co_return;
  }

  if (!pg.is_primary()) {
    // primary can handle both normal ops and balanced reads
    if (is_misdirected(pg)) {
      DEBUGDPP("{}.{}: dropping misdirected op",
	       pg, *this, this_instance_id);
      co_return;
    }

    pg.get_perf_logger().inc(l_osd_replica_read);
    if (pg.is_unreadable_object(m->get_hobj())) {
      DEBUGDPP("{}.{}: {} missing on replica, bouncing to primary",
	       pg, *this, this_instance_id, m->get_hobj());
      pg.get_perf_logger().inc(l_osd_replica_read_redirect_missing);
      co_await reply_op_error(pgref, -EAGAIN);
      co_return;
    } else if (!pg.get_peering_state().can_serve_replica_read(m->get_hobj())) {
      DEBUGDPP("{}.{}: unstable write on replica, bouncing to primary",
	       pg, *this, this_instance_id);
      pg.get_perf_logger().inc(l_osd_replica_read_redirect_conflict);
      co_await reply_op_error(pgref, -EAGAIN);
      co_return;
    } else {
      DEBUGDPP("{}.{}: serving replica read on oid {}",
	       pg, *this, this_instance_id, m->get_hobj());
      pg.get_perf_logger().inc(l_osd_replica_read_served);
    }
  }

  DEBUGDPP("{}.{}: pg active, entering process[_pg]_op",
	   *pgref, *this, this_instance_id);

  {
    /* The following works around two different gcc bugs:
     *  1. https://gcc.gnu.org/bugzilla/show_bug.cgi?id=101244
     *     This example isn't preciesly as described in the bug, but it seems
     *     similar.  It causes the generated code to incorrectly execute
     *     process_pg_op unconditionally before the predicate.  It seems to be
     *     fixed in gcc 12.2.1.
     *  2. https://gcc.gnu.org/bugzilla/show_bug.cgi?id=102217
     *     This one appears to cause the generated code to double-free
     *     awaiter holding the future.  This one seems to be fixed
     *     in gcc 13.2.1.
     *
     * Assigning the intermediate result and moving it into the co_await
     * expression bypasses both bugs.
     */
    auto fut = (is_pg_op() ? process_pg_op(pgref) :
		process_op(ihref, pgref, this_instance_id));
    co_await std::move(fut);
  }

  DEBUGDPP("{}.{}: process[_pg]_op complete, completing handle",
	   *pgref, *this, this_instance_id);
  co_await interruptor::make_interruptible(ihref.handle.complete());
}

seastar::future<> ClientRequest::with_pg_process(
  Ref<PG> pgref)
{
  ceph_assert_always(shard_services);
  LOG_PREFIX(ClientRequest::with_pg_process);

  epoch_t same_interval_since = pgref->get_interval_start_epoch();
  DEBUGDPP("{}: same_interval_since: {}", *pgref, *this, same_interval_since);
  const auto this_instance_id = instance_id++;
  OperationRef opref{this};
  auto instance_handle = get_instance_handle();
  auto &ihref = *instance_handle;
  return interruptor::with_interruption(
    [FNAME, this, pgref, this_instance_id, &ihref]() mutable {
      return with_pg_process_interruptible(
	pgref, this_instance_id, ihref
      ).then_interruptible([FNAME, this, this_instance_id, pgref] {
	DEBUGDPP("{}.{}: with_pg_process_interruptible complete,"
		 " completing request",
		 *pgref, *this, this_instance_id);
	complete_request(*pgref);
      });
    }, [FNAME, this, this_instance_id, pgref](std::exception_ptr eptr) {
      DEBUGDPP("{}.{}: interrupted due to {}",
	       *pgref, *this, this_instance_id, eptr);
    }, pgref, pgref->get_osdmap_epoch()).finally(
      [this, FNAME, opref=std::move(opref), pgref,
       this_instance_id, instance_handle=std::move(instance_handle), &ihref]() mutable {
	DEBUGDPP("{}.{}: exit", *pgref, *this, this_instance_id);
	return ihref.handle.complete(
	).finally([instance_handle=std::move(instance_handle)] {});
    });
}

seastar::future<> ClientRequest::with_pg(
  ShardServices &_shard_services, Ref<PG> pgref)
{
  shard_services = &_shard_services;
  pgref->client_request_orderer.add_request(*this);

  if (m->finish_decode()) {
    m->clear_payload();
  }

  auto ret = on_complete.get_future();
  std::ignore = with_pg_process(std::move(pgref));
  return ret;
}

ClientRequest::interruptible_future<>
ClientRequest::process_pg_op(
  Ref<PG> pg)
{
  auto reply = co_await pg->do_pg_ops(m);
  // TODO: gate the crosscore sending
  co_await interruptor::make_interruptible(
    get_foreign_connection().send_with_throttling(std::move(reply)));
}

ClientRequest::interruptible_future<>
ClientRequest::recover_missing_snaps(
  Ref<PG> pg,
  std::set<snapid_t> &snaps)
{
  LOG_PREFIX(ClientRequest::recover_missing_snaps);

  std::vector<hobject_t> ret;
  auto resolve_oids = pg->obc_loader.with_obc<RWState::RWREAD>(
    m->get_hobj().get_head(),
    [&snaps, &ret](auto head, auto) {
    for (auto &snap : snaps) {
      auto coid = head->obs.oi.soid;
      coid.snap = snap;
      auto oid = resolve_oid(head->get_head_ss(), coid);
      /* Rollback targets may legitimately not exist if, for instance,
       * the object is an rbd block which happened to be sparse and
       * therefore non-existent at the time of the specified snapshot.
       * In such a case, rollback will simply delete the object.  Here,
       * we skip the oid as there is no corresponding clone to recover.
       * See https://tracker.ceph.com/issues/63821 */
      if (oid) {
        ret.emplace_back(std::move(*oid));
      }
    }
    return seastar::now();
  }).handle_error_interruptible(
    crimson::ct_error::assert_all("unexpected error")
  );
  co_await std::move(resolve_oids);

  for (auto &oid : ret) {
    auto unfound = co_await do_recover_missing(pg, oid, m->get_reqid());
    if (unfound) {
      DEBUGDPP("{} unfound, hang it for now", *pg, oid);
      co_await interruptor::make_interruptible(
        pg->get_recovery_backend()->add_unfound(oid));
    }
  }
}

ClientRequest::interruptible_future<>
ClientRequest::process_op(
  instance_handle_t &ihref, Ref<PG> pg, unsigned this_instance_id)
{
  LOG_PREFIX(ClientRequest::process_op);
  ihref.obc_orderer = pg->obc_loader.get_obc_orderer(m->get_hobj());
  auto obc_manager = pg->obc_loader.get_obc_manager(
    *(ihref.obc_orderer),
    m->get_hobj());
  co_await ihref.enter_stage<interruptor>(
    ihref.obc_orderer->obc_pp().process, *this);

  if (!pg->is_primary()) {
    DEBUGDPP(
      "Skipping recover_missings on non primary pg for soid {}",
      *pg, m->get_hobj());
  } else {
    auto unfound = co_await do_recover_missing(
      pg, m->get_hobj().get_head(), m->get_reqid());
    if (unfound) {
      DEBUGDPP("{} unfound, hang it for now", *pg, m->get_hobj().get_head());
      co_await interruptor::make_interruptible(
        pg->get_recovery_backend()->add_unfound(m->get_hobj().get_head()));
    }

    std::set<snapid_t> snaps = snaps_need_to_recover();
    if (!snaps.empty()) {
      co_await recover_missing_snaps(pg, snaps);
    }
  }

  DEBUGDPP("{}.{}: checking already_complete",
	   *pg, *this, this_instance_id);
  auto completed = co_await pg->already_complete(m->get_reqid());

  if (completed) {
    DEBUGDPP("{}.{}: already completed, sending reply",
	     *pg, *this, this_instance_id);
    auto reply = crimson::make_message<MOSDOpReply>(
      m.get(), completed->err, pg->get_osdmap_epoch(),
      CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, false);
    reply->set_reply_versions(completed->version, completed->user_version);
    // TODO: gate the crosscore sending
    co_await interruptor::make_interruptible(
      get_foreign_connection().send_with_throttling(std::move(reply))
    );
    co_return;
  }

  DEBUGDPP("{}.{}: not completed, about to wait_scrub",
	   *pg, *this, this_instance_id);
  co_await ihref.enter_blocker(
    *this, pg->scrubber, &decltype(pg->scrubber)::wait_scrub,
    m->get_hobj());

  DEBUGDPP("{}.{}: past scrub blocker, getting obc",
	   *pg, *this, this_instance_id);

  int load_err = co_await pg->obc_loader.load_and_lock(
    obc_manager, pg->get_lock_type(op_info)
  ).si_then([]() -> int {
    return 0;
  }).handle_error_interruptible(
    PG::load_obc_ertr::all_same_way(
      [](const auto &code) -> int {
	return -code.value();
      })
  );
  if (load_err) {
    DEBUGDPP("{}.{}: saw error code loading obc {}",
	     *pg, *this, this_instance_id, load_err);
    co_await reply_op_error(pg, load_err);
    co_return;
  }

  DEBUGDPP("{}.{}: obc {} loaded and locked, calling do_process",
	   *pg, *this, this_instance_id, obc_manager.get_obc()->obs);
  co_await do_process(
    ihref, pg, obc_manager.get_obc(), this_instance_id
  );
}

ClientRequest::interruptible_future<>
ClientRequest::do_process(
  instance_handle_t &ihref,
  Ref<PG> pg, crimson::osd::ObjectContextRef obc,
  unsigned this_instance_id)
{
  LOG_PREFIX(ClientRequest::do_process);
  if (m->has_flag(CEPH_OSD_FLAG_PARALLELEXEC)) {
    co_await reply_op_error(pg, -EINVAL);
    co_return;
  }
  const pg_pool_t pool = pg->get_pgpool().info;
  if (pool.has_flag(pg_pool_t::FLAG_EIO)) {
    // drop op on the floor; the client will handle returning EIO
    if (m->has_flag(CEPH_OSD_FLAG_SUPPORTSPOOLEIO)) {
      DEBUGDPP("{}.{}: discarding op due to pool EIO flag",
	       *pg, *this, this_instance_id);
      co_return;
    } else {
      DEBUGDPP("{}.{}: replying EIO due to pool EIO flag",
	       *pg, *this, this_instance_id);
      co_await reply_op_error(pg, -EIO);
      co_return;
    }
  }
  if (m->get_oid().name.size()
    > crimson::common::local_conf()->osd_max_object_name_len) {
    co_await reply_op_error(pg, -ENAMETOOLONG);
    co_return;
  } else if (m->get_hobj().get_key().size()
    > crimson::common::local_conf()->osd_max_object_name_len) {
    co_await reply_op_error(pg, -ENAMETOOLONG);
    co_return;
  } else if (m->get_hobj().nspace.size()
    > crimson::common::local_conf()->osd_max_object_namespace_len) {
    co_await reply_op_error(pg, -ENAMETOOLONG);
    co_return;
  } else if (m->get_hobj().oid.name.empty()) {
    co_await reply_op_error(pg, -EINVAL);
    co_return;
  } else if (m->get_hobj().is_internal_pg_local()) {
    // clients are not allowed to write to hobject_t::INTERNAL_PG_LOCAL_NS
    co_await reply_op_error(pg, -EINVAL);
    co_return;
  } else if (pg->get_osdmap()->is_blocklisted(
        get_foreign_connection().get_peer_addr())) {
    DEBUGDPP("{}.{}: {} is blocklisted",
	     *pg, *this, this_instance_id, get_foreign_connection().get_peer_addr());
    co_await reply_op_error(pg, -EBLOCKLISTED);
    co_return;
  }

  if (!obc->obs.exists && !op_info.may_write()) {
    co_await reply_op_error(pg, -ENOENT);
    co_return;
  }

  SnapContext snapc = get_snapc(*pg,obc);

  if ((m->has_flag(CEPH_OSD_FLAG_ORDERSNAP)) &&
       snapc.seq < obc->ssc->snapset.seq) {
    DEBUGDPP("{}.{}: ORDERSNAP flag set "
	     "and snapc seq {} < snapset seq {} on {}",
	     *pg, *this, this_instance_id,
	     snapc.seq, obc->ssc->snapset.seq,
	     obc->obs.oi.soid);
    co_await reply_op_error(pg, -EOLDSNAPC);
    co_return;
  }

  OpsExecuter ox(pg, obc, op_info, *m, r_conn, snapc);
  auto ret = co_await pg->run_executer(
    ox, obc, op_info, m->ops
  ).si_then([]() -> std::optional<std::error_code> {
    return std::nullopt;
  }).handle_error_interruptible(crimson::ct_error::all_same_way(
    [](auto e) -> std::optional<std::error_code> {
      return e;
    })
  );

  auto should_log_error = [](std::error_code e) -> bool {
    switch (e.value()) {
    case EDQUOT:
    case ENOSPC:
    case EAGAIN:
      return false;
    default:
      return true;
    }
  };

  if (ret && !should_log_error(*ret)) {
    co_await reply_op_error(pg, -ret->value());
    co_return;
  }

  {
    auto all_completed = interruptor::now();
    if (ret) {
      assert(should_log_error(*ret));
      if (op_info.may_write()) {
	auto rep_tid = pg->shard_services.get_tid();
	auto version = co_await pg->submit_error_log(
	  m, op_info, obc, *ret, rep_tid);

	all_completed = pg->complete_error_log(
	  rep_tid, version);
      }
      // simply return the error below, leaving all_completed alone
    } else {
      auto submitted = interruptor::now();
      std::tie(submitted, all_completed) = co_await pg->submit_executer(
	std::move(ox), m->ops);
      co_await std::move(submitted);
    }
    co_await ihref.enter_stage<interruptor>(
      ihref.obc_orderer->obc_pp().wait_repop, *this);

    co_await std::move(all_completed);
  }

  co_await ihref.enter_stage<interruptor>(
    ihref.obc_orderer->obc_pp().send_reply, *this);

  if (ret) {
    int err = -ret->value();
    DEBUGDPP("{}: replying with error {}", *pg, *this, err);

    auto reply = crimson::make_message<MOSDOpReply>(
      m.get(), err, pg->get_osdmap_epoch(), 0, false);

    if (!m->ops.empty() && m->ops.back().op.flags & CEPH_OSD_OP_FLAG_FAILOK) {
      reply->set_result(0);
    }

    // For all ops except for CMPEXT, the correct error value is encoded
    // in e. For CMPEXT, osdop.rval has the actual error value.
    if (err == -ct_error::cmp_fail_error_value) {
      assert(!m->ops.empty());
      for (auto &osdop : m->ops) {
	if (osdop.rval < 0) {
	  reply->set_result(osdop.rval);
	  break;
	}
      }
    }

    reply->set_enoent_reply_versions(
      pg->peering_state.get_info().last_update,
      pg->peering_state.get_info().last_user_version);
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    
    // TODO: gate the crosscore sending
    co_await interruptor::make_interruptible(
      get_foreign_connection().send_with_throttling(std::move(reply)));
  } else {
    int result = m->ops.empty() ? 0 : m->ops.back().rval.code;
    if (op_info.may_read() && result >= 0) {
      for (auto &osdop : m->ops) {
	if (osdop.rval < 0 && !(osdop.op.flags & CEPH_OSD_OP_FLAG_FAILOK)) {
	  result = osdop.rval.code;
	  break;
	}
      }
    } else if (result > 0 && op_info.may_write() && !op_info.allows_returnvec()) {
      result = 0;
    } else if (result < 0 &&
	     (m->ops.empty() ?
	      0 : m->ops.back().op.flags & CEPH_OSD_OP_FLAG_FAILOK)) {
      result = 0;
    }
    auto reply = crimson::make_message<MOSDOpReply>(
      m.get(),
      result,
      pg->get_osdmap_epoch(),
      0,
      false);
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    if (obc->obs.exists) {
      reply->set_reply_versions(pg->peering_state.get_info().last_update,
				obc->obs.oi.user_version);
    } else {
      reply->set_reply_versions(pg->peering_state.get_info().last_update,
				pg->peering_state.get_info().last_user_version);
    }
    
    DEBUGDPP("{}.{}: sending response {}",
	     *pg, *this, this_instance_id, *m);
    // TODO: gate the crosscore sending
    co_await interruptor::make_interruptible(
      get_foreign_connection().send_with_throttling(std::move(reply))
    );
  }
}

bool ClientRequest::is_misdirected(const PG& pg) const
{
  // otherwise take a closer look
  if (const int flags = m->get_flags();
      flags & CEPH_OSD_FLAG_BALANCE_READS ||
      flags & CEPH_OSD_FLAG_LOCALIZE_READS) {
    if (!op_info.may_read()) {
      // no read found, so it can't be balanced read
      return true;
    }
    if (op_info.may_write() || op_info.may_cache()) {
      // write op, but i am not primary
      return true;
    }
    // balanced reads; any replica will do
    return false;
  }
  // neither balanced nor localize reads
  return true;
}

void ClientRequest::put_historic() const
{
  ceph_assert_always(shard_services);
  shard_services->get_registry().put_historic(*this);
}

const SnapContext ClientRequest::get_snapc(
  PG &pg,
  crimson::osd::ObjectContextRef obc) const
{
  LOG_PREFIX(ClientRequest::get_snapc);
  SnapContext snapc;
  if (op_info.may_write() || op_info.may_cache()) {
    // snap
    if (pg.get_pgpool().info.is_pool_snaps_mode()) {
      // use pool's snapc
      snapc = pg.get_pgpool().snapc;
      DEBUGDPP("{} using pool's snapc snaps={}",
	       pg, *this, snapc.snaps);
    } else {
      // client specified snapc
      snapc.seq = m->get_snap_seq();
      snapc.snaps = m->get_snaps();
      DEBUGDPP("{}: client specified snapc seq={} snaps={}",
	       pg, *this, snapc.seq, snapc.snaps);
    }
  }
  return snapc;
}

}
