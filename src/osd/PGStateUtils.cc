// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PGStateUtils.h"
#include "PG.h"

/*------NamedState----*/
NamedState::NamedState(
  PG *pg_, const char *state_name_)
  : state_name(state_name_), enter_time(ceph_clock_now()), pg(pg_) {
  pg->pgstate_history.enter(pg, enter_time, state_name);
}

NamedState::~NamedState() {
  pg->pgstate_history.exit(state_name);
}

/*---------PGStateHistory---------*/
void PGStateHistory::enter(PG* pg, const utime_t entime, const char* state)
{
  // Ignore trimming state machine for now
  if (::strstr(state, "Trimming") != NULL) {
    return;
  } else if (pi != nullptr) {
    pi->enter_state(entime, state);
  } else {
    // Store current state since we can't reliably take the PG lock here
    if ( tmppi == nullptr) {
      tmppi = std::unique_ptr<PGStateInstance>(new PGStateInstance);
    }

    thispg = pg;
    tmppi->enter_state(entime, state);
  }
}

void PGStateHistory::exit(const char* state) {
  // Ignore trimming state machine for now
  // Do nothing if PG is being destroyed!
  if (::strstr(state, "Trimming") != NULL || pg_in_destructor) {
    return;
  } else {
    bool ilocked = false;
    if(!thispg->is_locked()) {
      thispg->lock();
      ilocked = true;
    }
    if (pi == nullptr) {
      buffer.push_back(std::unique_ptr<PGStateInstance>(tmppi.release()));
      pi = buffer.back().get();
      pi->setepoch(thispg->get_osdmap_epoch());
    }

    pi->exit_state(ceph_clock_now());
    if (::strcmp(state, "Reset") == 0) {
      this->reset();
    }
    if(ilocked) {
      thispg->unlock();
    }
  }
}

void PGStateHistory::dump(Formatter* f) const {
  f->open_array_section("history");
  for (auto pi = buffer.begin(); pi != buffer.end(); ++pi) {
    f->open_object_section("states");
    f->dump_stream("epoch") << (*pi)->this_epoch;
    for (auto she : (*pi)->state_history) {
      f->dump_string("state", std::get<2>(she));
      f->dump_stream("enter") << std::get<0>(she);
      f->dump_stream("exit") << std::get<1>(she);
    }
    f->close_section();
  }
  f->close_section();
}
