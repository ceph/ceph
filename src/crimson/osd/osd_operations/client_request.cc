// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <seastar/core/future.hh>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "crimson/common/exception.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

ClientRequest::ClientRequest(
  OSD &osd, crimson::net::ConnectionRef conn, Ref<MOSDOp> &&m)
  : osd(osd),
    conn(conn),
    m(m),
    sequencer(get_osd_priv(conn.get()).op_sequencer[m->get_spg()])
{}

ClientRequest::~ClientRequest()
{
  logger().debug("{}: destroying", *this);
}

void ClientRequest::print(std::ostream &lhs) const
{
  lhs << "m=[" << *m << "], prev_op_id=" << prev_op_id;
}

void ClientRequest::dump_detail(Formatter *f) const
{
}

ClientRequest::ConnectionPipeline &ClientRequest::cp()
{
  return get_osd_priv(conn.get()).client_request_conn_pipeline;
}

ClientRequest::PGPipeline &ClientRequest::pp(PG &pg)
{
  return pg.client_request_pg_pipeline;
}

bool ClientRequest::is_pg_op() const
{
  return std::any_of(
    begin(m->ops), end(m->ops),
    [](auto& op) { return ceph_osd_op_type_pg(op.op.op); });
}

void ClientRequest::may_set_prev_op()
{
  // set prev_op_id if it's not set yet
  if (__builtin_expect(!prev_op_id.has_value(), true)) {
    prev_op_id.emplace(sequencer.get_last_issued());
  }
}

seastar::future<> ClientRequest::start()
{
  logger().debug("{}: start", *this);

  return seastar::repeat([this, opref=IRef{this}]() mutable {
      logger().debug("{}: in repeat", *this);
      return with_blocking_future(handle.enter(cp().await_map))
      .then([this]() {
	return with_blocking_future(
	    osd.osdmap_gate.wait_for_map(
	      m->get_min_epoch()));
      }).then([this](epoch_t epoch) {
	return with_blocking_future(handle.enter(cp().get_pg));
      }).then([this] {
	return with_blocking_future(osd.wait_for_pg(m->get_spg()));
      }).then([this](Ref<PG> pgref) mutable {
	return interruptor::with_interruption([this, pgref]() mutable {
          epoch_t same_interval_since = pgref->get_interval_start_epoch();
          logger().debug("{} same_interval_since: {}", *this, same_interval_since);
          if (m->finish_decode()) {
            m->clear_payload();
          }
          const bool has_pg_op = is_pg_op();
          auto interruptible_do_op = interruptor::wrap_function([=] {
            PG &pg = *pgref;
            if (pg.can_discard_op(*m)) {
              return interruptible_future<>(
                osd.send_incremental_map(conn, m->get_map_epoch()));
            }
            return with_blocking_future_interruptible<IOInterruptCondition>(
              handle.enter(pp(pg).await_map)
            ).then_interruptible([this, &pg] {
              return with_blocking_future_interruptible<IOInterruptCondition>(
                pg.osdmap_gate.wait_for_map(m->get_min_epoch()));
            }).then_interruptible([this, &pg](auto map) {
              return with_blocking_future_interruptible<IOInterruptCondition>(
                handle.enter(pp(pg).wait_for_active));
            }).then_interruptible([this, &pg]() {
              return with_blocking_future_interruptible<IOInterruptCondition>(
                pg.wait_for_active_blocker.wait());
            }).then_interruptible([this,
                                   has_pg_op,
                                   pgref=std::move(pgref)]() mutable {
              return (has_pg_op ?
                      process_pg_op(pgref) :
                      process_op(pgref));
            });
          });
          // keep the ordering of non-pg ops when across pg internvals
          return (has_pg_op ?
                  interruptible_do_op() :
                  with_sequencer(std::move(interruptible_do_op)))
            .then_interruptible([pgref]() {
              return seastar::stop_iteration::yes;
            });
	}, [this, pgref](std::exception_ptr eptr) {
          if (should_abort_request(*this, std::move(eptr))) {
            sequencer.abort();
            return seastar::stop_iteration::yes;
          } else {
            sequencer.maybe_reset(*this);
            return seastar::stop_iteration::no;
          }
	}, pgref);
      });
    });
}

ClientRequest::interruptible_future<>
ClientRequest::process_pg_op(
  Ref<PG> &pg)
{
  return pg->do_pg_ops(m)
    .then_interruptible([this, pg=std::move(pg)](MURef<MOSDOpReply> reply) {
      return conn->send(std::move(reply));
    });
}

ClientRequest::interruptible_future<>
ClientRequest::process_op(Ref<PG> &pg)
{
  return with_blocking_future_interruptible<IOInterruptCondition>(
      handle.enter(pp(*pg).recover_missing))
  .then_interruptible(
    [this, pg]() mutable {
    return do_recover_missing(pg, m->get_hobj());
  }).then_interruptible([this, pg]() mutable {
    return pg->already_complete(m->get_reqid()).then_unpack_interruptible(
      [this, pg](bool completed, int ret) mutable
      -> PG::load_obc_iertr::future<> {
      if (completed) {
        auto reply = crimson::make_message<MOSDOpReply>(
          m.get(), ret, pg->get_osdmap_epoch(),
          CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, false);
        return conn->send(std::move(reply));
      } else {
        return with_blocking_future_interruptible<IOInterruptCondition>(
            handle.enter(pp(*pg).get_obc)).then_interruptible(
          [this, pg]() mutable -> PG::load_obc_iertr::future<> {
          logger().debug("{}: got obc lock", *this);
          op_info.set_from_op(&*m, *pg->get_osdmap());
          return pg->with_locked_obc(m->get_hobj(), op_info,
                                     [this, pg](auto obc) mutable {
            return with_blocking_future_interruptible<IOInterruptCondition>(
              handle.enter(pp(*pg).process)
            ).then_interruptible([this, pg, obc]() mutable {
              return do_process(pg, obc);
            });
          });
        });
      }
    });
  }).safe_then_interruptible([pg=std::move(pg)] {
    return seastar::now();
  }, PG::load_obc_ertr::all_same_way([](auto &code) {
    logger().error("ClientRequest saw error code {}", code);
    return seastar::now();
  }));
}

ClientRequest::interruptible_future<>
ClientRequest::do_process(Ref<PG>& pg, crimson::osd::ObjectContextRef obc)
{
  if (!pg->is_primary()) {
    // primary can handle both normal ops and balanced reads
    if (is_misdirected(*pg)) {
      logger().trace("do_process: dropping misdirected op");
      return seastar::now();
    } else if (const hobject_t& hoid = m->get_hobj();
               !pg->get_peering_state().can_serve_replica_read(hoid)) {
      auto reply = crimson::make_message<MOSDOpReply>(
	m.get(), -EAGAIN, pg->get_osdmap_epoch(),
	m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK),
	!m->has_flag(CEPH_OSD_FLAG_RETURNVEC));
      return conn->send(std::move(reply));
    }
  }
  return pg->do_osd_ops(m, obc, op_info).safe_then_unpack_interruptible(
    [this, pg](auto submitted, auto all_completed) mutable {
    return submitted.then_interruptible(
      [this, pg] {
        return with_blocking_future_interruptible<IOInterruptCondition>(
            handle.enter(pp(*pg).wait_repop));
    }).then_interruptible(
      [this, pg, all_completed=std::move(all_completed)]() mutable {
      return all_completed.safe_then_interruptible(
        [this, pg](MURef<MOSDOpReply> reply) {
        return with_blocking_future_interruptible<IOInterruptCondition>(
            handle.enter(pp(*pg).send_reply)).then_interruptible(
              [this, reply=std::move(reply)]() mutable{
              return conn->send(std::move(reply));
            });
      }, crimson::ct_error::eagain::handle([this, pg]() mutable {
        return process_op(pg);
      }));
    });
  }, crimson::ct_error::eagain::handle([this, pg]() mutable {
    return process_op(pg);
  }));
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
    return pg.is_nonprimary();
  }
  // neither balanced nor localize reads
  return true;
}

}
