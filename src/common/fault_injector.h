// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <type_traits>
#include <boost/type_traits/has_equal_to.hpp>
#include <boost/type_traits/has_left_shift.hpp>
#include <variant>
#include "include/ceph_assert.h"
#include "common/dout.h"

/// @file

/// A failure type that aborts the process with a failed assertion.
struct InjectAbort {};

/// A failure type that injects an error code and optionally logs a message.
struct InjectError {
  /// error code to inject
  int error;
  /// an optional log channel to print an error message
  const DoutPrefixProvider* dpp = nullptr;
};

/** @class FaultInjector
 * @brief Used to instrument a code path with deterministic fault injection
 * by making one or more calls to check().
 *
 * A default-constructed FaultInjector contains no failure. It can also be
 * constructed with a failure of type InjectAbort or InjectError, along with
 * a location to inject that failure.
 *
 * The contained failure can be overwritten with a call to inject() or clear().
 * This is not thread-safe with respect to other member functions on the same
 * instance.
 *
 * @tparam Key  The location can be represented by any Key type that is
 * movable, default-constructible, inequality-comparable and stream-outputable.
 * A string or string_view Key may be preferable when the location comes from
 * user input, or to describe the steps like "before-foo" and "after-foo".
 * An integer Key may be preferable for a code path with many steps, where you
 * just want to check 1, 2, 3, etc. without inventing names for each.
 */
template <typename Key>
class FaultInjector {
 public:
  /// Default-construct with no injected failure.
  constexpr FaultInjector() noexcept : location() {}

  /// Construct with an injected assertion failure at the given location.
  constexpr FaultInjector(Key location, InjectAbort a)
    : location(std::move(location)), failure(a) {}

  /// Construct with an injected error code at the given location.
  constexpr FaultInjector(Key location, InjectError e)
    : location(std::move(location)), failure(e) {}

  /// Inject an assertion failure at the given location.
  void inject(Key location, InjectAbort a) {
    this->location = std::move(location);
    this->failure = a;
  }

  /// Inject an error at the given location.
  void inject(Key location, InjectError e) {
    this->location = std::move(location);
    this->failure = e;
  }

  /// Clear any injected failure.
  void clear() {
    this->failure = Empty{};
  }

  /// Check for an injected failure at the given location. If the location
  /// matches an InjectAbort failure, the process aborts here with an assertion
  /// failure.
  /// @returns 0 or InjectError::error if the location matches an InjectError
  /// failure
  [[nodiscard]] constexpr int check(const Key& location) const {
    struct visitor {
      const Key& check_location;
      const Key& this_location;
      constexpr int operator()(const std::monostate&) const {
        return 0;
      }
      int operator()(const InjectAbort&) const {
        if (check_location == this_location) {
          ceph_assert_always(!"FaultInjector");
        }
        return 0;
      }
      int operator()(const InjectError& e) const {
        if (check_location == this_location) {
          ldpp_dout(e.dpp, -1) << "Injecting error=" << e.error
              << " at location=" << this_location << dendl;
          return e.error;
        }
        return 0;
      }
    };
    return std::visit(visitor{location, this->location}, failure);
  }

 private:
  // Key requirements:
  static_assert(std::is_default_constructible_v<Key>,
                "Key must be default-constrible");
  static_assert(std::is_move_constructible_v<Key>,
                "Key must be move-constructible");
  static_assert(std::is_move_assignable_v<Key>,
                "Key must be move-assignable");
  static_assert(boost::has_equal_to<Key, Key, bool>::value,
                "Key must be equality-comparable");
  static_assert(boost::has_left_shift<std::ostream, Key, std::ostream&>::value,
                "Key must have an ostream operator<<");

  Key location; // location of the check that should fail

  using Empty = std::monostate; // empty state for std::variant

  std::variant<Empty, InjectAbort, InjectError> failure;
};
