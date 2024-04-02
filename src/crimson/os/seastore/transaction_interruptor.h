// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

#include "crimson/common/errorator.h"
#include "crimson/common/interruptible_future.h"

namespace crimson::os::seastore {

class Transaction;

struct TransactionConflictCondition {
  class transaction_conflict final : public std::exception {
  public:
    const char* what() const noexcept final {
      return "transaction conflict detected";
    }
  };

public:
  TransactionConflictCondition(Transaction &t) : t(t) {}

  template <typename Fut>
  std::optional<Fut> may_interrupt() {
    if (is_conflicted()) {
      return seastar::futurize<Fut>::make_exception_future(
	transaction_conflict());
    } else {
      return std::optional<Fut>();
    }
  }

  template <typename T>
  static constexpr bool is_interruption_v =
    std::is_same_v<T, transaction_conflict>;


  static bool is_interruption(std::exception_ptr& eptr) {
    return *eptr.__cxa_exception_type() == typeid(transaction_conflict);
  }

private:
  bool is_conflicted() const;

  Transaction &t;
};

using trans_intr = crimson::interruptible::interruptor<
  TransactionConflictCondition
  >;

template <typename E>
using trans_iertr =
  crimson::interruptible::interruptible_errorator<
    TransactionConflictCondition,
    E
  >;

template <typename F, typename... Args>
auto with_trans_intr(Transaction &t, F &&f, Args&&... args) {
  return trans_intr::with_interruption_to_error<crimson::ct_error::eagain>(
    std::move(f),
    TransactionConflictCondition(t),
    t,
    std::forward<Args>(args)...);
}

template <typename T>
using with_trans_ertr = typename T::base_ertr::template extend<crimson::ct_error::eagain>;

} // namespace crimson::os::seastore
