// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef SCOPE_GUARD
#define SCOPE_GUARD

#include <type_traits>
#include <utility>

template <typename F>
class scope_guard {
public:
  [[nodiscard("Unassigned scope guards will execute immediately")]]
  explicit scope_guard(F f) : f(std::move(f)) {}
  template <typename... Args>
  [[nodiscard("Unassigned scope guards will execute immediately")]]
  scope_guard(std::in_place_t, Args&&... args) : f(std::forward<Args>(args)...) {}
  scope_guard(const scope_guard&) = delete;
  scope_guard& operator=(const scope_guard&) = delete;
  ~scope_guard() {
    if (active) {
      std::move(f)(); // Support at-most-once functions
    }
  }
  // Keep the action from running, e.g. once the operation it would undo
  // has succeeded.
  void dismiss() noexcept {
    active = false;
  }
private:
  [[no_unique_address]] F f;
  bool active = true;
};

template <typename F>
[[nodiscard("Unassigned scope guards will execute immediately")]]
scope_guard<std::decay_t<F>> make_scope_guard(F&& f) {
  return scope_guard<std::decay_t<F>>(std::forward<F>(f));
}

#endif
