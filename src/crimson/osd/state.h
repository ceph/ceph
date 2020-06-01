// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string_view>
#include <ostream>

class OSDMap;

class OSDState {

  enum class State {
    INITIALIZING,
    PREBOOT,
    BOOTING,
    ACTIVE,
    PRESTOP,
    STOPPING,
    WAITING_FOR_HEALTHY,
  };

  State state = State::INITIALIZING;

public:
  bool is_initializing() const {
    return state == State::INITIALIZING;
  }
  bool is_preboot() const {
    return state == State::PREBOOT;
  }
  bool is_booting() const {
    return state == State::BOOTING;
  }
  bool is_active() const {
    return state == State::ACTIVE;
  }
  bool is_prestop() const {
    return state == State::PRESTOP;
  }
  bool is_stopping() const {
    return state == State::STOPPING;
  }
  bool is_waiting_for_healthy() const {
    return state == State::WAITING_FOR_HEALTHY;
  }
  void set_preboot() {
    state = State::PREBOOT;
  }
  void set_booting() {
    state = State::BOOTING;
  }
  void set_active() {
    state = State::ACTIVE;
  }
  void set_prestop() {
    state = State::PRESTOP;
  }
  void set_stopping() {
    state = State::STOPPING;
  }
  std::string_view to_string() const {
    switch (state) {
    case State::INITIALIZING: return "initializing";
    case State::PREBOOT: return "preboot";
    case State::BOOTING: return "booting";
    case State::ACTIVE: return "active";
    case State::PRESTOP: return "prestop";
    case State::STOPPING: return "stopping";
    case State::WAITING_FOR_HEALTHY: return "waiting_for_healthy";
    default: return "???";
    }
  }
};

inline std::ostream&
operator<<(std::ostream& os, const OSDState& s) {
  return os << s.to_string();
}
