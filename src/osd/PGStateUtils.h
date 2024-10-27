// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h" // for epoch_t
#include "include/utime.h"
#include "common/Formatter.h"

#include <stack>
#include <tuple>
#include <vector>
#include <boost/circular_buffer.hpp>

class PGStateHistory;

struct EpochSource {
  virtual epoch_t get_osdmap_epoch() const = 0;
  virtual ~EpochSource() {}
};

struct NamedState {
  PGStateHistory *pgsh;
  const char *state_name;
  utime_t enter_time;
  const char *get_state_name() { return state_name; }
  NamedState(
    PGStateHistory *pgsh,
    const char *state_name_);
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

  bool empty() const {
    return embedded_states.empty();
  }

  epoch_t this_epoch;
  std::vector<state_history_entry> state_history;
  std::stack<embedded_state> embedded_states;
};

class PGStateHistory {
public:
  PGStateHistory(const EpochSource &es) : buffer(10), es(es) {}

  void enter(const utime_t entime, const char* state);

  void exit(const char* state);

  void reset() {
    buffer.push_back(std::move(pi));
    pi = nullptr;
  }

  void dump(ceph::Formatter* f) const;

  const char *get_current_state() const {
    if (pi == nullptr) return "unknown";
    return std::get<1>(pi->embedded_states.top());
  }

private:
  std::unique_ptr<PGStateInstance> pi;
  boost::circular_buffer<std::unique_ptr<PGStateInstance>> buffer;
  const EpochSource &es;
};
