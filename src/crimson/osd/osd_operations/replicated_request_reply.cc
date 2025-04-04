// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_request_reply.h"

#include "common/Formatter.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/replicated_backend.h"

SET_SUBSYS(osd);

namespace crimson::osd {

ReplicatedRequestReply::ReplicatedRequestReply(
  crimson::net::ConnectionRef&& conn,
  Ref<MOSDRepOpReply> &&req)
  : RemoteOperation{std::move(conn)},
    req{std::move(req)}
{}

void ReplicatedRequestReply::print(std::ostream& os) const
{
  os << "ReplicatedRequestReply("
     << " req=" << *req
     << ")";
}

void ReplicatedRequestReply::dump_detail(Formatter *f) const
{
  f->open_object_section("ReplicatedRequestReply");
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->close_section();
}

seastar::future<> ReplicatedRequestReply::with_pg(
  ShardServices &shard_services, Ref<PG> pgref)
{
  LOG_PREFIX(ReplicatedRequestReply::with_pg);
  DEBUGDPP("{}", *pgref, *this);
  req->finish_decode();
  pgref->handle_rep_op_reply(*req);
  return seastar::now();
}

}
