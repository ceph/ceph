// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/PGPeeringEvent.h"
#include "include/mempool.h"
#include "messages/MOSDPGLog.h"

MEMPOOL_DEFINE_OBJECT_FACTORY(PGPeeringEvent, pg_peering_evt, osd);

MLogRec::MLogRec(pg_shard_t from, MOSDPGLog *msg)
  : from(from), msg(msg) {}

void MLogRec::print(std::ostream *out) const
{
  *out << "MLogRec from " << from << " ";
  msg->inner_print(*out);
}
