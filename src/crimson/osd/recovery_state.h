// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include <map>

#include "recovery_machine.h"

class PG;

namespace recovery {

// RecoveryMachine::handle_event() could send multiple notifications to a
// certain peer OSD before it reaches the last state. for better performance,
// we send them in batch. the pending messages are collected in RecoveryCtx
// before being dispatched upon returning of handle_event().
struct Context
{
  using osd_id_t = int;

  using notify_t = std::pair<pg_notify_t, PastIntervals>;
  std::map<osd_id_t, std::vector<notify_t>> notifies;

  using queries_t = std::map<spg_t, pg_query_t>;
  std::map<osd_id_t, queries_t> queries;

  using infos_t = std::vector<pair<pg_notify_t, PastIntervals>>;
  std::map<osd_id_t, infos_t> infos;
};

/// Encapsulates PG recovery process,
class State {
public:
  explicit State(PG& pg);
  Context handle_event(const boost::statechart::event_base& evt);
private:
  friend class Machine;
  Machine machine;
  Context context;
};
}
