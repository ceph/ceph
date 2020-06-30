// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "osd/PeeringState.h"

#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGCreate2.h"

#include "common/Formatter.h"

#include "crimson/common/exception.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/osd_operations/compound_peering_request.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace {
using namespace crimson::osd;

struct compound_state {
  seastar::promise<BufferedRecoveryMessages> promise;
  // assuming crimson-osd won't need to be compatible with pre-octopus
  // releases
  BufferedRecoveryMessages ctx{ceph_release_t::octopus};
  compound_state() = default;
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

  seastar::future<> complete_rctx(Ref<crimson::osd::PG> pg) final {
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
  crimson::net::ConnectionRef conn,
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

namespace crimson::osd {

CompoundPeeringRequest::CompoundPeeringRequest(
  OSD &osd, crimson::net::ConnectionRef conn, Ref<Message> m)
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
      assert((m->get_type() == MSG_OSD_PG_CREATE2));
      return handle_pg_create(
        osd,
	conn,
	state,
	boost::static_pointer_cast<MOSDPGCreate2>(m));
    }());

  IRef ref = this;
  logger().info("{}: about to fork future", *this);
  return crimson::common::handle_system_shutdown(
    [this, ref, blocker=std::move(blocker), state]() mutable {
    return with_blocking_future(
      blocker->make_blocking_future(state->promise.get_future())
    ).then([this, blocker=std::move(blocker)](auto &&ctx) {
      logger().info("{}: sub events complete", *this);
      return osd.get_shard_services().dispatch_context_messages(std::move(ctx));
    }).then([this, ref=std::move(ref)] {
      logger().info("{}: complete", *this);
    });
  });
}

} // namespace crimson::osd
