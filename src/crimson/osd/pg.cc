// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg.h"

#include <functional>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <boost/range/numeric.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"

#include "osd/OSDMap.h"

#include "os/Transaction.h"

#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/os/cyan_collection.h"
#include "os/Transaction.h"
#include "crimson/os/cyan_store.h"

#include "crimson/osd/exceptions.h"
#include "crimson/osd/pg_meta.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/osd_operations/peering_event.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

using ceph::common::local_conf;

class RecoverablePredicate : public IsPGRecoverablePredicate {
public:
  bool operator()(const set<pg_shard_t> &have) const override {
    return !have.empty();
  }
};

class ReadablePredicate: public IsPGReadablePredicate {
  pg_shard_t whoami;
public:
  explicit ReadablePredicate(pg_shard_t whoami) : whoami(whoami) {}
  bool operator()(const set<pg_shard_t> &have) const override {
    return have.count(whoami);
  }
};

PG::PG(
  spg_t pgid,
  pg_shard_t pg_shard,
  pg_pool_t&& pool,
  std::string&& name,
  cached_map_t osdmap,
  ShardServices &shard_services,
  ec_profile_t profile)
  : pgid{pgid},
    pg_whoami{pg_shard},
    coll_ref(shard_services.get_store().open_collection(coll)),
    pgmeta_oid{pgid.make_pgmeta_oid()},
    osdmap_gate("PG::osdmap_gate", std::nullopt),
    shard_services{shard_services},
    osdmap{osdmap},
    backend(
      PGBackend::create(
	pgid.pgid,
	pg_shard,
	pool,
	coll_ref,
	shard_services,
	profile)),
    peering_state(
      shard_services.get_cct(),
      pg_shard,
      pgid,
      PGPool(
	shard_services.get_cct(),
	osdmap,
	pgid.pool(),
	pool,
	osdmap->get_pool_name(pgid.pool())),
      osdmap,
      this,
      this)
{
  peering_state.set_backend_predicates(
    new ReadablePredicate(pg_whoami),
    new RecoverablePredicate());
  osdmap_gate.got_map(osdmap->get_epoch());
}

PG::~PG() {}

bool PG::try_flush_or_schedule_async() {
  shard_services.get_store().do_transaction(
    coll_ref,
    ObjectStore::Transaction()).then(
      [this, epoch=get_osdmap_epoch()]() {
	return shard_services.start_operation<LocalPeeringEvent>(
	  this,
	  shard_services,
	  pg_whoami,
	  pgid,
	  epoch,
	  epoch,
	  PeeringState::IntervalFlush());
      });
  return false;
}

void PG::on_activate(interval_set<snapid_t>)
{
  projected_last_update = peering_state.get_info().last_update;
}

void PG::on_activate_complete()
{
  active_promise.set_value();
  active_promise = {};

  if (peering_state.needs_recovery()) {
    shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::DoRecovery{});
  } else if (peering_state.needs_backfill()) {
    shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::RequestBackfill{});
  } else {
    shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::AllReplicasRecovered{});
  }
}

void PG::log_state_enter(const char *state) {
  logger().info("Entering state: {}", state);
}

void PG::log_state_exit(
  const char *state_name, utime_t enter_time,
  uint64_t events, utime_t event_dur) {
  logger().info(
    "Exiting state: {}, entered at {}, {} spent on {} events",
    state_name,
    enter_time,
    event_dur,
    events);
}

ceph::signedspan PG::get_mnow()
{
  return shard_services.get_mnow();
}

HeartbeatStampsRef PG::get_hb_stamps(int peer)
{
  return shard_services.get_hb_stamps(peer);
}

void PG::init(
  ceph::os::CollectionRef coll,
  int role,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  const pg_history_t& history,
  const PastIntervals& pi,
  bool backfill,
  ObjectStore::Transaction &t)
{
  coll_ref = coll;
  peering_state.init(
    role, newup, new_up_primary, newacting,
    new_acting_primary, history, pi, backfill, t);
}

seastar::future<> PG::read_state(ceph::os::FuturizedStore* store)
{
  coll_ref = store->open_collection(coll_t(pgid));
  return PGMeta{store, pgid}.load().then(
    [this, store](pg_info_t pg_info, PastIntervals past_intervals) {
      return peering_state.init_from_disk_state(
	std::move(pg_info),
	std::move(past_intervals),
	[this, store, &pg_info] (PGLog &pglog) {
	  return pglog.read_log_and_missing_crimson(
	    *store,
	    coll_ref,
	    peering_state.get_info(),
	    pgmeta_oid);
	});
    }).then([this, store]() {
      int primary, up_primary;
      vector<int> acting, up;
      peering_state.get_osdmap()->pg_to_up_acting_osds(
	pgid.pgid, &up, &up_primary, &acting, &primary);
      peering_state.init_primary_up_acting(
	up,
	acting,
	up_primary,
	primary);
      int rr = OSDMap::calc_pg_role(pg_whoami.osd, acting);
      if (peering_state.get_pool().info.is_replicated() || rr == pg_whoami.shard)
	peering_state.set_role(rr);
      else
	peering_state.set_role(-1);

      epoch_t epoch = get_osdmap_epoch();
      shard_services.start_operation<LocalPeeringEvent>(
	this,
	shard_services,
	pg_whoami,
	pgid,
	epoch,
	epoch,
	PeeringState::Initialize());

      return seastar::now();
    });
}

void PG::do_peering_event(
  const boost::statechart::event_base &evt,
  PeeringCtx &rctx)
{
  peering_state.handle_event(
    evt,
    &rctx);
  peering_state.write_if_dirty(rctx.transaction);
}

void PG::do_peering_event(
  PGPeeringEvent& evt, PeeringCtx &rctx)
{
  if (!peering_state.pg_has_reset_since(evt.get_epoch_requested())) {
    logger().debug("{} handling {}", __func__, evt.get_desc());
    do_peering_event(evt.get_event(), rctx);
  } else {
    logger().debug("{} ignoring {} -- pg has reset", __func__, evt.get_desc());
  }
}

void PG::handle_advance_map(
  cached_map_t next_map, PeeringCtx &rctx)
{
  vector<int> newup, newacting;
  int up_primary, acting_primary;
  next_map->pg_to_up_acting_osds(
    pgid.pgid,
    &newup, &up_primary,
    &newacting, &acting_primary);
  peering_state.advance_map(
    next_map,
    peering_state.get_osdmap(),
    newup,
    up_primary,
    newacting,
    acting_primary,
    rctx);
  osdmap_gate.got_map(next_map->get_epoch());
}

void PG::handle_activate_map(PeeringCtx &rctx)
{
  peering_state.activate_map(rctx);
}

void PG::handle_initialize(PeeringCtx &rctx)
{
  PeeringState::Initialize evt;
  peering_state.handle_event(evt, &rctx);
}


void PG::print(ostream& out) const
{
  out << peering_state << " ";
}


std::ostream& operator<<(std::ostream& os, const PG& pg)
{
  os << " pg_epoch " << pg.get_osdmap_epoch() << " ";
  pg.print(os);
  return os;
}

seastar::future<> PG::wait_for_active()
{
  logger().debug("wait_for_active: {}", peering_state.get_pg_state_string());
  if (peering_state.is_active()) {
    return seastar::now();
  } else {
    return active_promise.get_shared_future();
  }
}

// TODO: split the method accordingly to os' constness needs
seastar::future<>
PG::do_osd_op(ObjectState& os, OSDOp& osd_op, ceph::os::Transaction& txn)
{
  // TODO: dispatch via call table?
  // TODO: we might want to find a way to unify both input and output
  // of each op.
  logger().debug("handling op {}", ceph_osd_op_name(osd_op.op.op));
  switch (const ceph_osd_op& op = osd_op.op; op.op) {
  case CEPH_OSD_OP_SYNC_READ:
    [[fallthrough]];
  case CEPH_OSD_OP_READ:
    return backend->read(os.oi,
                         op.extent.offset,
                         op.extent.length,
                         op.extent.truncate_size,
                         op.extent.truncate_seq,
                         op.flags).then([&osd_op](bufferlist bl) {
      osd_op.rval = bl.length();
      osd_op.outdata = std::move(bl);
      return seastar::now();
    });
  case CEPH_OSD_OP_WRITE:
    return backend->write(os, osd_op, txn);
  case CEPH_OSD_OP_WRITEFULL:
    // XXX: os = backend->write(std::move(os), ...) instead?
    return backend->writefull(os, osd_op, txn);
  case CEPH_OSD_OP_SETALLOCHINT:
    return seastar::now();
  case CEPH_OSD_OP_PGNLS:
    return do_pgnls(osd_op.indata, os.oi.soid.get_namespace(), op.pgls.count)
      .then([&osd_op](bufferlist bl) {
        osd_op.outdata = std::move(bl);
	return seastar::now();
    });
  case CEPH_OSD_OP_DELETE:
    return backend->remove(os, txn);
  case CEPH_OSD_OP_CALL:
    return backend->call(os, osd_op, txn);
  case CEPH_OSD_OP_STAT:
    return backend->stat(os, osd_op);
  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

seastar::future<bufferlist> PG::do_pgnls(bufferlist& indata,
                                         const std::string& nspace,
                                         uint64_t limit)
{
  hobject_t lower_bound;
  try {
    ceph::decode(lower_bound, indata);
  } catch (const buffer::error& e) {
    throw std::invalid_argument("unable to decode PGNLS handle");
  }
  const auto pg_start = pgid.pgid.get_hobj_start();
  const auto pg_end = pgid.pgid.get_hobj_end(peering_state.get_pool().info.get_pg_num());
  if (!(lower_bound.is_min() ||
        lower_bound.is_max() ||
        (lower_bound >= pg_start && lower_bound < pg_end))) {
    // this should only happen with a buggy client.
    throw std::invalid_argument("outside of PG bounds");
  }
  return backend->list_objects(lower_bound, limit).then(
    [lower_bound, pg_end, nspace](auto objects, auto next) {
      auto in_my_namespace = [&nspace](const hobject_t& o) {
        if (o.get_namespace() == local_conf()->osd_hit_set_namespace) {
          return false;
        } else if (nspace == librados::all_nspaces) {
          return true;
        } else {
          return o.get_namespace() == nspace;
        }
      };
      pg_nls_response_t response;
      boost::copy(objects |
        boost::adaptors::filtered(in_my_namespace) |
        boost::adaptors::transformed([](const hobject_t& o) {
          return librados::ListObjectImpl{o.get_namespace(),
                                          o.oid.name,
                                          o.get_key()}; }),
        std::back_inserter(response.entries));
      response.handle = next.is_max() ? pg_end : next;
      bufferlist bl;
      encode(response, bl);
      return seastar::make_ready_future<bufferlist>(std::move(bl));
  });
}

seastar::future<> PG::submit_transaction(boost::local_shared_ptr<ObjectState>&& os,
					 ceph::os::Transaction&& txn,
					 const MOSDOp& req)
{
  epoch_t map_epoch = get_osdmap_epoch();
  eversion_t at_version{map_epoch, projected_last_update.version + 1};
  return backend->mutate_object(peering_state.get_acting_recovery_backfill(),
				std::move(os),
				std::move(txn),
				req,
				peering_state.get_last_peering_reset(),
				map_epoch,
				at_version).then([this](auto acked) {
    for (const auto& peer : acked) {
      peering_state.update_peer_last_complete_ondisk(
        peer.shard, peer.last_complete_ondisk);
    }
    return seastar::now();
  });
}

seastar::future<Ref<MOSDOpReply>> PG::do_osd_ops(Ref<MOSDOp> m)
{
  return seastar::do_with(std::move(m), ceph::os::Transaction{},
                          [this](auto& m, auto& txn) {
    const auto oid = m->get_snapid() == CEPH_SNAPDIR ? m->get_hobj().get_head()
                                                     : m->get_hobj();
    return backend->get_object_state(oid).then([m,&txn,this](auto os) {
      // TODO: issue requests in parallel if they don't write,
      // with writes being basically a synchronization barrier
      return seastar::do_for_each(std::begin(m->ops), std::end(m->ops),
                                  [m,&txn,this,pos=os.get()](OSDOp& osd_op) {
        return do_osd_op(*pos, osd_op, txn);
      }).then([&txn,m,this,os=std::move(os)]() mutable {
        // XXX: the entire lambda could be scheduled conditionally. ::if_then()?
	if (txn.empty()) {
	  return seastar::now();
	} else {
	  return submit_transaction(std::move(os), std::move(txn), *m);
	}
      });
    }).then([m,this] {
      auto reply = make_message<MOSDOpReply>(m.get(), 0, get_osdmap_epoch(),
                                             0, false);
      reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
      return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
    }).handle_exception_type([=](const ceph::osd::error& e) {
      logger().debug("got ceph::osd::error while handling object {}: {} ({})",
                     oid, e.code(), e.what());

      backend->evict_object_state(oid);
      auto reply = make_message<MOSDOpReply>(
        m.get(), -e.code().value(), get_osdmap_epoch(), 0, false);
      reply->set_enoent_reply_versions(peering_state.get_info().last_update,
                                       peering_state.get_info().last_user_version);
      return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
    });
  });
}

seastar::future<> PG::handle_op(ceph::net::Connection* conn,
                                Ref<MOSDOp> m)
{
  return wait_for_active().then([conn, m, this] {
    if (m->finish_decode()) {
      m->clear_payload();
    }
    return do_osd_ops(m);
  }).then([conn](Ref<MOSDOpReply> reply) {
    return conn->send(reply);
  });
}

seastar::future<> PG::handle_rep_op(Ref<MOSDRepOp> req)
{
  ceph::os::Transaction txn;
  auto encoded_txn = req->get_data().cbegin();
  decode(txn, encoded_txn);
  return shard_services.get_store().do_transaction(coll_ref, std::move(txn))
    .then([req, lcod=peering_state.get_info().last_complete, this] {
      peering_state.update_last_complete_ondisk(lcod);
      const auto map_epoch = get_osdmap_epoch();
      auto reply = make_message<MOSDRepOpReply>(
        req.get(), pg_whoami, 0,
	map_epoch, req->get_min_epoch(), CEPH_OSD_FLAG_ONDISK);
      reply->set_last_complete_ondisk(lcod);
      return shard_services.send_to_osd(req->from.osd, reply, map_epoch);
    });
}

void PG::handle_rep_op_reply(ceph::net::Connection* conn,
			     const MOSDRepOpReply& m)
{
  backend->got_rep_op_reply(m);
}

}
