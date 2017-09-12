// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_PERIOD_HISTORY_H
#define RGW_PERIOD_HISTORY_H

#include <deque>
#include <mutex>
#include <system_error>
#include <boost/intrusive/avl_set.hpp>
#include "include/assert.h"
#include "include/types.h"

namespace bi = boost::intrusive;

class RGWPeriod;

/**
 * RGWPeriodHistory tracks the relative history of all inserted periods,
 * coordinates the pulling of missing intermediate periods, and provides a
 * Cursor object for traversing through the connected history.
 */
class RGWPeriodHistory final {
 private:
  /// an ordered history of consecutive periods
  class History;

  // comparisons for avl_set ordering
  friend bool operator<(const History& lhs, const History& rhs);
  friend struct NewestEpochLess;

  class Impl;
  std::unique_ptr<Impl> impl;

 public:
  /**
   * Puller is a synchronous interface for pulling periods from the master
   * zone. The abstraction exists mainly to support unit testing.
   */
  class Puller {
   public:
    virtual ~Puller() = default;

    virtual int pull(const std::string& period_id, RGWPeriod& period) = 0;
  };

  RGWPeriodHistory(CephContext* cct, Puller* puller,
                   const RGWPeriod& current_period);
  ~RGWPeriodHistory();

  /**
   * Cursor tracks a position in the period history and allows forward and
   * backward traversal. Only periods that are fully connected to the
   * current_period are reachable via a Cursor, because other histories are
   * temporary and can be merged away. Cursors to periods in disjoint
   * histories, as provided by insert() or lookup(), are therefore invalid and
   * their operator bool() will return false.
   */
  class Cursor final {
   public:
    Cursor() = default;
    explicit Cursor(int error) : error(error) {}

    int get_error() const { return error; }

    /// return false for a default-constructed or error Cursor
    operator bool() const { return history != nullptr; }

    epoch_t get_epoch() const { return epoch; }
    const RGWPeriod& get_period() const;

    bool has_prev() const;
    bool has_next() const;

    void prev() { epoch--; }
    void next() { epoch++; }

    friend bool operator==(const Cursor& lhs, const Cursor& rhs);
    friend bool operator!=(const Cursor& lhs, const Cursor& rhs);

   private:
    // private constructors for RGWPeriodHistory
    friend class RGWPeriodHistory::Impl;

    Cursor(const History* history, std::mutex* mutex, epoch_t epoch)
      : history(history), mutex(mutex), epoch(epoch) {}

    int error{0};
    const History* history{nullptr};
    std::mutex* mutex{nullptr};
    epoch_t epoch{0}; //< realm epoch of cursor position
  };

  /// return a cursor to the current period
  Cursor get_current() const;

  /// build up a connected period history that covers the span between
  /// current_period and the given period, reading predecessor periods or
  /// fetching them from the master as necessary. returns a cursor at the
  /// given period that can be used to traverse the current_history
  Cursor attach(RGWPeriod&& period);

  /// insert the given period into an existing history, or create a new
  /// unconnected history. similar to attach(), but it doesn't try to fetch
  /// missing periods. returns a cursor to the inserted period iff it's in
  /// the current_history
  Cursor insert(RGWPeriod&& period);

  /// search for a period by realm epoch, returning a valid Cursor iff it's in
  /// the current_history
  Cursor lookup(epoch_t realm_epoch);
};

#endif // RGW_PERIOD_HISTORY_H
