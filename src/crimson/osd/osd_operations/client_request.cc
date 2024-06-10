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

SET_SUBSYS(osd);

namespace crimson::osd {


void ClientRequest::Orderer::requeue(Ref<PG> pg)
{
  LOG_PREFIX(ClientRequest::Orderer::requeue);
  for (auto &req: list) {
    DEBUGDPP("requeueing {}", *pg, req);
    req.reset_instance_handle();
    std::ignore = req.with_pg_process(pg);
  }
}

void ClientRequest::Orderer::clear_and_cancel(PG &pg)
{
  LOG_PREFIX(ClientRequest::Orderer::clear_and_cancel);
  for (auto i = list.begin(); i != list.end(); ) {
    DEBUGDPP("{}", pg, *i);
    i->complete_request();
    remove_request(*(i++));
  }
}

void ClientRequest::complete_request()
{
  track_event<CompletionEvent>();
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

ClientRequest::PGPipeline &ClientRequest::client_pp(PG &pg)
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
  if (!m->get_hobj().get_key().empty()) {
    // There are no users of locator. It was used to ensure that multipart-upload
    // parts would end up in the same PG so that they could be clone_range'd into
    // the same object via librados, but that's not how multipart upload works
    // anymore and we no longer support clone_range via librados.
    get_handle().exit();
    co_await reply_op_error(pgref, -ENOTSUP);
    co_return;
  }
  if (pg.can_discard_op(*m)) {
    co_await interruptor::make_interruptible(
      shard_services->send_incremental_map(
	std::ref(get_foreign_connection()), m->get_map_epoch()
      ));
    DEBUGDPP("{}: discarding {}", *pgref, *this, this_instance_id);
    pgref->client_request_orderer.remove_request(*this);
    complete_request();
    co_return;
  }
  DEBUGDPP("{}.{}: entering await_map stage",
	   *pgref, *this, this_instance_id);
  co_await ihref.enter_stage<interruptor>(client_pp(pg).await_map, *this);
  DEBUGDPP("{}.{}: entered await_map stage, waiting for map",
	   pg, *this, this_instance_id);
  auto map_epoch = co_await interruptor::make_interruptible(
    ihref.enter_blocker(
      *this, pg.osdmap_gate, &decltype(pg.osdmap_gate)::wait_for_map,
      m->get_min_epoch(), nullptr));

  DEBUGDPP("{}.{}: map epoch got {}, entering wait_for_active",
	   pg, *this, this_instance_id, map_epoch);
  co_await ihref.enter_stage<interruptor>(client_pp(pg).wait_for_active, *this);

  DEBUGDPP("{}.{}: entered wait_for_active stage, waiting for active",
	   pg, *this, this_instance_id);
  co_await interruptor::make_interruptible(
    ihref.enter_blocker(
      *this,
      pg.wait_for_active_blocker,
      &decltype(pg.wait_for_active_blocker)::wait));

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

  DEBUGDPP("{}.{}: process[_pg]_op complete,"
	   "removing request from orderer",
	   *pgref, *this, this_instance_id);
  pgref->client_request_orderer.remove_request(*this);
  complete_request();
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
    [this, pgref, this_instance_id, &ihref]() mutable {
      return with_pg_process_interruptible(pgref, this_instance_id, ihref);
    }, [FNAME, this, this_instance_id, pgref](std::exception_ptr eptr) {
      DEBUGDPP("{}.{}: interrupted due to {}",
	       *pgref, *this, this_instance_id, eptr);
    }, pgref).finally(
      [this, FNAME, opref=std::move(opref), pgref,
       this_instance_id, instance_handle=std::move(instance_handle), &ihref] {
	DEBUGDPP("{}.{}: exit", *pgref, *this, this_instance_id);
	ihref.handle.exit();
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
  instance_handle_t &ihref,
  ObjectContextRef head,
  std::set<snapid_t> &snaps)
{
  co_await ihref.enter_stage<interruptor>(
    client_pp(*pg).recover_missing_snaps, *this);
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
      co_await do_recover_missing(pg, *oid, m->get_reqid());
    }
  }
}

ClientRequest::interruptible_future<>
ClientRequest::process_op(
  instance_handle_t &ihref, Ref<PG> pg, unsigned this_instance_id)
{
  LOG_PREFIX(ClientRequest::process_op);
  co_await ihref.enter_stage<interruptor>(
    client_pp(*pg).recover_missing, *this
  );
  if (!pg->is_primary()) {
    DEBUGDPP(
      "Skipping recover_missings on non primary pg for soid {}",
      *pg, m->get_hobj());
  } else {
    co_await do_recover_missing(pg, m->get_hobj().get_head(), m->get_reqid());
    std::set<snapid_t> snaps = snaps_need_to_recover();
    if (!snaps.empty()) {
      // call with_obc() in order, but wait concurrently for loading.
      ihref.enter_stage_sync(
          client_pp(*pg).recover_missing_lock_obc, *this);
      auto with_obc = pg->obc_loader.with_obc<RWState::RWREAD>(
        m->get_hobj().get_head(),
        [&snaps, &ihref, pg, this](auto head, auto) {
        return recover_missing_snaps(pg, ihref, head, snaps);
      }).handle_error_interruptible(
        crimson::ct_error::assert_all("unexpected error")
      );
      // see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=98401
      co_await std::move(with_obc);
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

  DEBUGDPP("{}.{}: not completed, entering get_obc stage",
	   *pg, *this, this_instance_id);
  co_await ihref.enter_stage<interruptor>(client_pp(*pg).get_obc, *this);

  DEBUGDPP("{}.{}: entered get_obc stage, about to wait_scrub",
	   *pg, *this, this_instance_id);
  if (int res = op_info.set_from_op(&*m, *pg->get_osdmap());
      res != 0) {
    co_await reply_op_error(pg, res);
    co_return;
  }
  co_await ihref.enter_blocker(
    *this, pg->scrubber, &decltype(pg->scrubber)::wait_scrub,
    m->get_hobj());

  DEBUGDPP("{}.{}: past scrub blocker, getting obc",
	   *pg, *this, this_instance_id);
  // call with_locked_obc() in order, but wait concurrently for loading.
  ihref.enter_stage_sync(
      client_pp(*pg).lock_obc, *this);
  auto process = pg->with_locked_obc(
    m->get_hobj(), op_info,
    [FNAME, this, pg, this_instance_id, &ihref] (
      auto head, auto obc
    ) -> interruptible_future<> {
      DEBUGDPP("{}.{}: got obc {}, entering process stage",
	       *pg, *this, this_instance_id, obc->obs);
      return ihref.enter_stage<interruptor>(
	client_pp(*pg).process, *this
      ).then_interruptible(
	[FNAME, this, pg, this_instance_id, obc, &ihref]() mutable {
	  DEBUGDPP("{}.{}: in process stage, calling do_process",
		   *pg, *this, this_instance_id);
	  return do_process(
	    ihref, pg, obc, this_instance_id
	  ).handle_error_interruptible(
	    crimson::ct_error::eagain::handle(
	      [this, pg, this_instance_id, &ihref]() mutable {
		return process_op(ihref, pg, this_instance_id);
	      })
	  );
	}
      );
    }).handle_error_interruptible(
      PG::load_obc_ertr::all_same_way(
	[FNAME, this, pg=std::move(pg), this_instance_id](
	  const auto &code
	) -> interruptible_future<> {
	  DEBUGDPP("{}.{}: saw error code {}",
		   *pg, *this, this_instance_id, code);
	  assert(code.value() > 0);
	  return reply_op_error(pg, -code.value());
	})
    );

  /* The following works around gcc bug
   * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=98401.
   * The specific symptom I observed is the pg param being
   * destructed multiple times resulting in the refcount going
   * rapidly to 0 destoying the PG prematurely.
   *
   * This bug seems to be resolved in gcc 13.2.1.
   *
   * Assigning the intermediate result and moving it into the co_await
   * expression bypasses both bugs.
   */
  co_await std::move(process);
}

ClientRequest::do_process_iertr::future<>
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

  if (!pg->is_primary()) {
    // primary can handle both normal ops and balanced reads
    if (is_misdirected(*pg)) {
      DEBUGDPP("{}.{}: dropping misdirected op",
	       *pg, *this, this_instance_id);
      co_return;
    } else if (const hobject_t& hoid = m->get_hobj();
               !pg->get_peering_state().can_serve_replica_read(hoid)) {
      DEBUGDPP("{}.{}: unstable write on replica, bouncing to primary",
	       *pg, *this, this_instance_id);
      co_await reply_op_error(pg, -EAGAIN);
      co_return;
    } else {
      DEBUGDPP("{}.{}: serving replica read on oid {}",
	       *pg, *this, this_instance_id, m->get_hobj());
    }
  }

  auto [submitted, all_completed] = co_await pg->do_osd_ops(
    m, r_conn, obc, op_info, snapc
  );
  co_await std::move(submitted);

  co_await ihref.enter_stage<interruptor>(client_pp(*pg).wait_repop, *this);

  auto reply = co_await std::move(all_completed);

  co_await ihref.enter_stage<interruptor>(client_pp(*pg).send_reply, *this);
  DEBUGDPP("{}.{}: sending response",
	   *pg, *this, this_instance_id);
  // TODO: gate the crosscore sending
  co_await interruptor::make_interruptible(
    get_foreign_connection().send_with_throttling(std::move(reply))
  );
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
