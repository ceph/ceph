// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "osd/PeeringState.h"

#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGCreate2.h"

#include "common/Formatter.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/osd_operations/compound_peering_request.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace {
using namespace ceph::osd;

struct compound_state {
  seastar::promise<BufferedRecoveryMessages> promise;
  BufferedRecoveryMessages ctx;
  ~compound_state() {
    promise.set_value(std::move(ctx));
  }
};
using compound_state_ref = seastar::lw_shared_ptr<compound_state>;

class PeeringSubEvent : public RemotePeeringEvent {
  compound_state_ref state;
public:
  template <typename... Args>
  PeeringSubEvent(compound_state_ref state, Args &&... args) :
    RemotePeeringEvent(std::forward<Args>(args)...), state(state) {}

  seastar::future<> complete_rctx(Ref<ceph::osd::PG> pg) final {
    logger().debug("{}: submitting ctx transaction", *this);
    state->ctx.accept_buffered_messages(ctx);
    state = {};
    if (!pg) {
      ceph_assert(ctx.transaction.empty());
      return seastar::now();
    } else {
      return osd.get_shard_services().dispatch_context_transaction(
	pg->get_collection_ref(), ctx);
    }
  }
};

std::vector<OperationRef> handle_pg_create(
  OSD &osd,
  ceph::net::ConnectionRef conn,
  compound_state_ref state,
  Ref<MOSDPGCreate2> m)
{
  std::vector<OperationRef> ret;
  for (auto &p : m->pgs) {
    const spg_t &pgid = p.first;
    const auto &[created, created_stamp] = p.second;
    auto q = m->pg_extra.find(pgid);
    ceph_assert(q != m->pg_extra.end());
    logger().debug(
      "{}, {} {} e{} @{} history {} pi {}",
      __func__,
      pgid,
      created,
      created_stamp,
      q->second.first,
      q->second.second);
    if (!q->second.second.empty() &&
	m->epoch < q->second.second.get_bounds().second) {
      logger().error(
	"got pg_create on {} epoch {} unmatched past_intervals (history {})",
	pgid,
	m->epoch,
	q->second.second,
	q->second.first);
    } else {
      auto op = osd.get_shard_services().start_operation<PeeringSubEvent>(
	  state,
	  osd,
	  conn,
	  osd.get_shard_services(),
	  pg_shard_t(),
	  pgid,
	  m->epoch,
	  m->epoch,
	  NullEvt(),
	  true,
	  new PGCreateInfo(
	    pgid,
	    m->epoch,
	    q->second.first,
	    q->second.second,
	    true)).first;
      ret.push_back(op);
    }
  }
  return ret;
}

std::vector<OperationRef> handle_pg_notify(
  OSD &osd,
  ceph::net::ConnectionRef conn,
  compound_state_ref state,
  Ref<MOSDPGNotify> m)
{
  std::vector<OperationRef> ret;
  ret.reserve(m->get_pg_list().size());
  const int from = m->get_source().num();
  for (auto& pg_notify : m->get_pg_list()) {
    spg_t pgid{pg_notify.info.pgid.pgid, pg_notify.to};
    MNotifyRec notify{pgid,
		      pg_shard_t{from, pg_notify.from},
		      pg_notify,
		      0}; // the features is not used
    logger().debug("handle_pg_notify on {} from {}", pgid.pgid, from);
    auto create_info = new PGCreateInfo{
      pgid,
      pg_notify.query_epoch,
      pg_notify.info.history,
      pg_notify.past_intervals,
      false};
    auto op = osd.get_shard_services().start_operation<PeeringSubEvent>(
      state,
      osd,
      conn,
      osd.get_shard_services(),
      pg_shard_t(from, pg_notify.from),
      pgid,
      pg_notify.epoch_sent,
      pg_notify.query_epoch,
      notify,
      true, // requires_pg
      create_info).first;
    ret.push_back(op);
  }
  return ret;
}

std::vector<OperationRef> handle_pg_info(
  OSD &osd,
  ceph::net::ConnectionRef conn,
  compound_state_ref state,
  Ref<MOSDPGInfo> m)
{
  std::vector<OperationRef> ret;
  ret.reserve(m->pg_list.size());
  const int from = m->get_source().num();
  for (auto& pg_notify : m->pg_list) {
    spg_t pgid{pg_notify.info.pgid.pgid, pg_notify.to};
    logger().debug("handle_pg_info on {} from {}", pgid.pgid, from);
    MInfoRec info{pg_shard_t{from, pg_notify.from},
		  pg_notify.info,
		  pg_notify.epoch_sent};
    auto op = osd.get_shard_services().start_operation<PeeringSubEvent>(
	state,
	osd,
	conn,
	osd.get_shard_services(),
	pg_shard_t(from, pg_notify.from),
	pgid,
	pg_notify.epoch_sent,
	pg_notify.query_epoch,
	std::move(info)).first;
    ret.push_back(op);
  }
  return ret;
}

class QuerySubEvent : public PeeringSubEvent {
public:
  template <typename... Args>
  QuerySubEvent(Args &&... args) :
    PeeringSubEvent(std::forward<Args>(args)...) {}

  void on_pg_absent() final {
    logger().debug("handle_pg_query on absent pg {} from {}", pgid, from);
    pg_info_t empty(pgid);
    ctx.notify_list[from.osd].emplace_back(
      pg_notify_t(
	from.shard, pgid.shard,
	evt.get_epoch_sent(),
	osd.get_shard_services().get_osdmap()->get_epoch(),
	empty,
	PastIntervals()));
  }
};

std::vector<OperationRef> handle_pg_query(
  OSD &osd,
  ceph::net::ConnectionRef conn,
  compound_state_ref state,
  Ref<MOSDPGQuery> m)
{
  std::vector<OperationRef> ret;
  ret.reserve(m->pg_list.size());
  const int from = m->get_source().num();
  for (auto &p : m->pg_list) {
    auto& [pgid, pg_query] = p;
    MQuery query{pgid, pg_shard_t{from, pg_query.from},
		 pg_query, pg_query.epoch_sent};
    logger().debug("handle_pg_query on {} from {}", pgid, from);
    auto op = osd.get_shard_services().start_operation<QuerySubEvent>(
	state,
	osd,
	conn,
	osd.get_shard_services(),
	pg_shard_t(from, pg_query.from),
	pgid,
	pg_query.epoch_sent,
	pg_query.epoch_sent,
	std::move(query)).first;
    ret.push_back(op);
  }
  return ret;
}

struct SubOpBlocker : BlockerT<SubOpBlocker> {
  static constexpr const char * type_name = "CompoundOpBlocker";

  std::vector<OperationRef> subops;
  SubOpBlocker(std::vector<OperationRef> &&subops) : subops(subops) {}

  virtual void dump_detail(Formatter *f) const {
    f->open_array_section("dependent_operations");
    {
      for (auto &i : subops) {
	i->dump_brief(f);
      }
    }
    f->close_section();
  }
};

} // namespace

namespace ceph::osd {

CompoundPeeringRequest::CompoundPeeringRequest(
  OSD &osd, ceph::net::ConnectionRef conn, Ref<Message> m)
  : osd(osd),
    conn(conn),
    m(m)
{}

void CompoundPeeringRequest::print(std::ostream &lhs) const
{
  lhs << *m;
}

void CompoundPeeringRequest::dump_detail(Formatter *f) const
{
  f->dump_stream("message") << *m;
}

seastar::future<> CompoundPeeringRequest::start()
{
  logger().info("{}: starting", *this);
  auto state = seastar::make_lw_shared<compound_state>();
  auto blocker = std::make_unique<SubOpBlocker>(
    [&] {
      switch (m->get_type()) {
      case MSG_OSD_PG_CREATE2:
	return handle_pg_create(
	  osd,
	  conn,
	  state,
	  boost::static_pointer_cast<MOSDPGCreate2>(m));
      case MSG_OSD_PG_NOTIFY:
	return handle_pg_notify(
	  osd,
	  conn,
	  state,
	  boost::static_pointer_cast<MOSDPGNotify>(m));
      case MSG_OSD_PG_INFO:
	return handle_pg_info(
	  osd,
	  conn,
	  state,
	  boost::static_pointer_cast<MOSDPGInfo>(m));
      case MSG_OSD_PG_QUERY:
	return handle_pg_query(
	  osd,
	  conn,
	  state,
	  boost::static_pointer_cast<MOSDPGQuery>(m));
      default:
	ceph_assert("Invalid message type" == 0);
	return std::vector<OperationRef>();
      }
    }());

  add_blocker(blocker.get());
  IRef ref = this;
  logger().info("{}: about to fork future", *this);
  return state->promise.get_future().then(
    [this, blocker=std::move(blocker)](auto &&ctx) {
      clear_blocker(blocker.get());
      logger().info("{}: sub events complete", *this);
      return osd.get_shard_services().dispatch_context_messages(std::move(ctx));
    }).then([this, ref=std::move(ref)] {
      logger().info("{}: complete", *this);
    });
}

} // namespace ceph::osd
