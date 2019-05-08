// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PGStateUtils.h"
#include "common/Clock.h"

/*------NamedState----*/
NamedState::NamedState(PGStateHistory *pgsh, const char *state_name)
  : pgsh(pgsh), state_name(state_name) {
  if(pgsh) {
    pgsh->enter(ceph_clock_now(), state_name);
  }
}

NamedState::~NamedState() {
  if(pgsh) {
    pgsh->exit(state_name);
  }
}

/*---------PGStateHistory---------*/
void PGStateHistory::enter(const utime_t entime, const char* state)
{
  if (pi == nullptr) {
    pi = std::make_unique<PGStateInstance>();
  }
  pi->enter_state(entime, state);
}

void PGStateHistory::exit(const char* state) {
  pi->setepoch(es.get_osdmap_epoch());
  pi->exit_state(ceph_clock_now());
  if (pi->empty()) {
    reset();
  }
}

void PGStateHistory::dump(Formatter* f) const {
  f->open_array_section("history");
  for (auto pi = buffer.begin(); pi != buffer.end(); ++pi) {
    f->open_object_section("epochs");
    f->dump_stream("epoch") << (*pi)->this_epoch;
    f->open_array_section("states");
    for (auto she : (*pi)->state_history) {
      f->open_object_section("state");
      f->dump_string("state", std::get<2>(she));
      f->dump_stream("enter") << std::get<0>(she);
      f->dump_stream("exit") << std::get<1>(she);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
}
