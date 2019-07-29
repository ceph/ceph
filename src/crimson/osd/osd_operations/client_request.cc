// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "messages/MOSDOp.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

ClientRequest::ClientRequest(
  OSD &osd, ceph::net::ConnectionRef conn, Ref<MOSDOp> &&m)
  : osd(osd), conn(conn), m(m)
{}

void ClientRequest::print(std::ostream &lhs) const
{
  lhs << *m;
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

seastar::future<> ClientRequest::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  return with_blocking_future(handle.enter(cp().await_map))
    .then([this]() {
      return with_blocking_future(osd.osdmap_gate.wait_for_map(m->get_min_epoch()));
    }).then([this](epoch_t epoch) {
      return with_blocking_future(handle.enter(cp().get_pg));
    }).then([this] {
      return with_blocking_future(osd.wait_for_pg(m->get_spg()));
    }).then([this, ref=std::move(ref)](Ref<PG> pg) {
      return seastar::do_with(
	std::move(pg), std::move(ref), [this](auto pg, auto op) {
	return with_blocking_future(
	  handle.enter(pp(*pg).await_map)
	).then([this, pg] {
	  return with_blocking_future(
	    pg->osdmap_gate.wait_for_map(m->get_map_epoch()));
	}).then([this, pg] (auto) {
	  return with_blocking_future(handle.enter(pp(*pg).process));
	}).then([this, pg] {
	  return pg->handle_op(conn.get(), std::move(m));
	});
      });
    });
}

}
