// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/utime.h"

#include <stack>
#include <vector>
#include <boost/circular_buffer.hpp>

class PG;

struct NamedState {
  const char *state_name;
  utime_t enter_time;
  PG* pg;
  const char *get_state_name() { return state_name; }
  NamedState(PG *pg_, const char *state_name_);
  virtual ~NamedState();
};

using state_history_entry = std::tuple<utime_t, utime_t, const char*>;
using embedded_state = std::pair<utime_t, const char*>;

struct PGStateInstance {
  // Time spent in pg states

  void setepoch(const epoch_t current_epoch) {
    this_epoch = current_epoch;
  }

  void enter_state(const utime_t entime, const char* state) {
    embedded_states.push(std::make_pair(entime, state));
  }

  void exit_state(const utime_t extime) {
    embedded_state this_state = embedded_states.top();
    state_history.push_back(state_history_entry{
        this_state.first, extime, this_state.second});
    embedded_states.pop();
  }

  epoch_t this_epoch;
  utime_t enter_time;
  std::vector<state_history_entry> state_history;
  std::stack<embedded_state> embedded_states;
};

class PGStateHistory {
  // Member access protected with the PG lock
public:
  PGStateHistory() : buffer(10) {}

  void enter(PG* pg, const utime_t entime, const char* state);

  void exit(const char* state);

  void reset() {
    pi = nullptr;
  }

  void set_pg_in_destructor() { pg_in_destructor = true; }

  void dump(Formatter* f) const;

  string get_current_state() {
    if (pi == nullptr) return "unknown";
    return std::get<1>(pi->embedded_states.top());
  }

private:
  bool pg_in_destructor = false;
  PG* thispg = nullptr;
  std::unique_ptr<PGStateInstance> tmppi;
  PGStateInstance* pi = nullptr;
  boost::circular_buffer<std::unique_ptr<PGStateInstance>> buffer;

};
