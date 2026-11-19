// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <utility>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

/**
 * trans_intr
 *
 * Transparent, zero-overhead pass-through for standard Seastar futures.
 * Under single-writer shard execution, transactions never conflict and do not
 * require interruption wrappers.
 */
struct trans_intr {
  template <typename Fut>
  static constexpr auto&& make_interruptible(Fut&& fut) noexcept {
    return std::forward<Fut>(fut);
  }

  template <typename Container, typename Func>
  static auto do_for_each(Container&& c, Func&& f) {
    return seastar::do_for_each(std::forward<Container>(c), std::forward<Func>(f));
  }

  template <typename Iterator, typename Func>
  static auto do_for_each(Iterator begin, Iterator end, Func&& f) {
    return seastar::do_for_each(begin, end, std::forward<Func>(f));
  }

  template <typename Container, typename Func>
  static auto parallel_for_each(Container&& c, Func&& f) {
    return seastar::parallel_for_each(std::forward<Container>(c), std::forward<Func>(f));
  }

  template <typename Iterator, typename Func>
  static auto parallel_for_each(Iterator begin, Iterator end, Func&& f) {
    return seastar::parallel_for_each(begin, end, std::forward<Func>(f));
  }

  template <typename Func>
  static auto repeat(Func&& f) {
    return seastar::repeat(std::forward<Func>(f));
  }
};

template <typename E>
using trans_iertr = E;

using base_iertr = base_ertr;

template <typename T>
using with_trans_ertr = typename T::base_ertr;

} // namespace crimson::os::seastore
