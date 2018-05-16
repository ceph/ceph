// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/mempool.h"
#include "osd/PGPeeringEvent.h"
#include "messages/MOSDPGLog.h"

MEMPOOL_DEFINE_OBJECT_FACTORY(PGPeeringEvent, pg_peering_evt, osd);
