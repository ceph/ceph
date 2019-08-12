// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include <utility>

template <typename F>
struct scope_guard {
  F f;
  scope_guard() = delete;
  scope_guard(const scope_guard &) = delete;
  scope_guard(scope_guard &&) = default;
  scope_guard & operator=(const scope_guard &) = delete;
  scope_guard & operator=(scope_guard &&) = default;
  scope_guard(const F& f) : f(f) {}
  scope_guard(F &&f) : f(std::move(f)) {}
  template<typename... Args>
  scope_guard(std::in_place_t, Args&& ...args) : f(std::forward<Args>(args)...) {}
  ~scope_guard() {
    std::move(f)(); // Support at-most-once functions
  }
};

template <typename F>
scope_guard<F> make_scope_guard(F &&f) {
  return scope_guard<F>(std::forward<F>(f));
}

template<typename F, typename... Args>
scope_guard<F> make_scope_guard(std::in_place_type_t<F>, Args&& ...args) {
  return { std::in_place, std::forward<Args>(args)... };
}

#endif
