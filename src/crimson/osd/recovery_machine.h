// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/state_machine.hpp>

#include "osd/osd_types.h"

class PG;

namespace recovery {

struct Initial;
class State;
class Context;

struct Machine : public boost::statechart::state_machine<Machine,
							 Initial> {
  Machine(State& state, PG& pg)
    : state{state},
      pg{pg}
  {}
  void send_notify(pg_shard_t to,
		   const pg_notify_t& info,
		   const PastIntervals& pi);
  void send_query(pg_shard_t to,
		  const pg_query_t& query);
  recovery::Context* get_context();
  State& state;
  PG& pg;
};
}
