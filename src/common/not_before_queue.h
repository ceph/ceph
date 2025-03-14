// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "include/utime.h"

#include <numeric> // for std::accumulate()
#include <optional>

/**
 * not_before_queue_t
 *
 * Implements a generic priority queue with two additional properties:
 * - Items are not eligible to be dequeued until their not_before value
 *   is after the current time (see project_not_before and advance_time)
 * - Items can be dequeued efficiently by removal_class (see
 *   project_removal_class and remove_by_class)
 *
 * User must define the following free functions:
 *  - bool operator<(const V &lhs, const V &rhs)
 *  - const T &project_not_before(const V&)
 *  - const K &project_removal_class(const V&)
 *
 * operator< above should be defined such that if lhs is more urgent than
 * rhs, lhs < rhs evaluates to true.
 *
 * project_removal_class returns a reference to a type K used in
 * remove_by_class.
 *
 * project_not_before returns a time value comparable to the time type T.
 *
 * V must also have a copy constructor.
 *
 * The purpose of this queue implementation is to add a not_before concept
 * to allow specifying a point in time before which the item will not be
 * eligible for dequeueing orthogonal to the main priority.  Once that point
 * is passed, ordering is determined by priority as defined by the operator<
 * definition.
 */
template <typename V, typename T=utime_t>
class not_before_queue_t {

  enum class status_t {
    INVALID,  // Not queued, only possible during construction and destruction
    INELIGIBLE,	 // Queued in ineligible_queue
    ELIGIBLE	 // Queued in eligible_queue
  };

  /**
   * container_t
   *
   * Each item has a single container_t.  Every container_t is linked
   * into and owned by removal_registry_t.  Additionally, every element
   * will be linked into exactly one of ineligible_queue and eligible_queue.
   */
  struct container_t : boost::intrusive::set_base_hook<> // see removal_registry
  {
    // see ineligible_queue and eligible_queue
    using queue_hook_t = boost::intrusive::set_member_hook<>;
    queue_hook_t queue_hook;

    status_t status = status_t::INVALID;

    const V v;

    template <typename... Args>
    container_t(Args&&... args) : v(std::forward<Args>(args)...) {}
    ~container_t() {
      assert(status == status_t::INVALID);
    }
  };

  using queue_hook_option_t = boost::intrusive::member_hook<
    container_t,
    typename container_t::queue_hook_t,
    &container_t::queue_hook>;

  /**
   * ineligible_queue
   *
   * - Contained items have project_not_before(v) > current_time.
   * - Contained elements have status set to INELIGIBLE.
   * - Contained elements are contained and owned by removal_registry_t
   * - Uses same hook as and is mutually exclusive with eligible_queue.
   */
  struct compare_by_nb_t {
    bool operator()(const container_t &lhs, const container_t &rhs) const {
      return project_not_before(lhs.v) < project_not_before(rhs.v);
    }
  };
  using ineligible_queue_t = boost::intrusive::multiset<
    container_t,
    queue_hook_option_t,
    boost::intrusive::compare<compare_by_nb_t>>;
  ineligible_queue_t ineligible_queue;

  /**
   * eligible_queue
   *
   * - Contains items where project_not_before(v) <= current_time.
   * - Contained elements have status set to ELIGIBLE.
   * - Contained elements are contained and owned by removal_registry_t
   * - Uses same hook as and is mutually exclusive with ineligible_queue.
   */
  struct compare_by_user_order_t {
    bool operator()(const container_t &lhs, const container_t &rhs) const {
      return lhs.v < rhs.v;
    }
  };
  using eligible_queue_t = boost::intrusive::multiset<
    container_t,
    queue_hook_option_t,
    boost::intrusive::compare<compare_by_user_order_t>>;
  eligible_queue_t eligible_queue;

  /**
   * removal_registry_t
   *
   * - Used to efficiently remove items by removal_class.
   * - Contains an entry for every item in not_before_queue_t
   *   (ELIGIBLE or INELIGIBLE)
   * - Owns every contained item.
   */
  struct compare_by_removal_class_t {
    bool operator()(const container_t &lhs, const container_t &rhs) const {
      return project_removal_class(lhs.v) < project_removal_class(rhs.v);
    }

    template <typename U>
    bool operator()(const U &lhs, const container_t &rhs) const {
      return lhs < project_removal_class(rhs.v);
    }

    template <typename U>
    bool operator()(const container_t &lhs, const U &rhs) const {
      return project_removal_class(lhs.v) < rhs;
    }
  };
  struct removal_registry_disposer_t {
    void operator()(container_t *p) { delete p; }
  };
  using removal_registry_t = boost::intrusive::multiset<
    container_t,
    boost::intrusive::compare<compare_by_removal_class_t>>;
  removal_registry_t removal_registry;

  /// current time, see advance_time
  T current_time;
public:
  /// Enqueue item constructed constructible from args...
  template <typename... Args>
  void enqueue(Args&&... args) {
    auto *item = new container_t(std::forward<Args>(args)...);
    removal_registry.insert(*item);

    if (project_not_before(item->v) > current_time) {
      item->status = status_t::INELIGIBLE;
      ineligible_queue.insert(*item);
    } else {
      item->status = status_t::ELIGIBLE;
      eligible_queue.insert(*item);
    }
  }

  /// Dequeue next item, return std::nullopt there are no eligible items
  std::optional<V> dequeue() {
    if (eligible_queue.empty()) {
      return std::nullopt;
    }

    auto iter = eligible_queue.begin();
    assert(iter->status == status_t::ELIGIBLE);

    eligible_queue.erase(
      typename eligible_queue_t::const_iterator(iter));
    iter->status = status_t::INVALID;

    std::optional<V> ret(iter->v);
    removal_registry.erase_and_dispose(
      removal_registry_t::s_iterator_to(std::as_const(*iter)),
      removal_registry_disposer_t{});
    return ret;
  }

  /// Dequeue 1st eligible item that satisfies pred, std::nullopt if none
  template <typename PRED>
  std::optional<V> dequeue_by_pred(const PRED& pred) {
    auto iter = std::find_if(
	eligible_queue.begin(), eligible_queue.end(),
	[&pred](const auto &i) { return pred(i.v); });

    if (iter == eligible_queue.end()) {
      return std::nullopt;
    }

    assert(iter->status == status_t::ELIGIBLE);
    eligible_queue.erase(typename eligible_queue_t::const_iterator(iter));
    iter->status = status_t::INVALID;

    std::optional<V> ret(iter->v);
    removal_registry.erase_and_dispose(
	removal_registry_t::s_iterator_to(std::as_const(*iter)),
	removal_registry_disposer_t{});
    return ret;
  }

  /**
   * advance_time
   *
   * Advances the eligibility cutoff.
   * Argument should be non-decreasing in successive calls.
   *
   * As for the scrub queue the cutoff is a time value, and as we have
   * encountered rare cases of clock resets (even the monotonic clock
   * can be slightly adjusted backwards on some kernels), "backward"
   * updates will be tolerated - ignored and not assert.
   *
   * \retval: true if the cutoff was advanced. False if we
   *          had to ignore the update.
   */
  bool advance_time(T next_time) {
    if (next_time < current_time) {
      return false;
    }
    current_time = next_time;
    while (true) {
      if (ineligible_queue.empty()) {
	break;
      }

      auto iter = ineligible_queue.begin();
      auto &item = *iter;
      assert(item.status == status_t::INELIGIBLE);

      if (project_not_before(item.v) > current_time) {
	break;
      }

      item.status = status_t::ELIGIBLE;
      ineligible_queue.erase(typename ineligible_queue_t::const_iterator(iter));
      eligible_queue.insert(item);
    }
    return true;
  }

  /**
   * remove_by_class
   *
   * Remove all items such that project_removal_class(item) == k
   */
  template <typename K>
  void remove_by_class(const K &k) {
    for (auto iter = removal_registry.lower_bound(
	   k, compare_by_removal_class_t{});
	 iter != removal_registry.upper_bound(
	   k, compare_by_removal_class_t{}); ) {
      if (iter->status == status_t::INELIGIBLE) {
	ineligible_queue.erase(
	  ineligible_queue_t::s_iterator_to(std::as_const(*iter)));
      } else if (iter->status == status_t::ELIGIBLE) {
	eligible_queue.erase(
	  eligible_queue_t::s_iterator_to(std::as_const(*iter)));
      } else {
	assert(0 == "impossible status");
      }
      iter->status = status_t::INVALID;
      removal_registry.erase_and_dispose(
	typename removal_registry_t::const_iterator(iter++),
	removal_registry_disposer_t{});
    }
  }

  /**
   * remove_if_by_class
   *
   * Remove up to 'max_removed' items for which project_removal_class(item) == k
   * AND PRED(item) == true
   *
   * Returns the number of items removed
   */
  template <typename K, typename PRED>
  int remove_if_by_class(
      const K& k,
      PRED&& pred,
      std::optional<int> max_removed = std::nullopt) {
    int removed = 0;
    for (auto iter =
	     removal_registry.lower_bound(k, compare_by_removal_class_t{});
	 iter !=
	 removal_registry.upper_bound(k, compare_by_removal_class_t{});) {

      if (!pred(iter->v)) {
	++iter;
	continue;
      }

      if (iter->status == not_before_queue_t::status_t::INELIGIBLE) {
	ineligible_queue.erase(
	    ineligible_queue_t::s_iterator_to(std::as_const(*iter)));
      } else if (iter->status == not_before_queue_t::status_t::ELIGIBLE) {
	eligible_queue.erase(
	    eligible_queue_t::s_iterator_to(std::as_const(*iter)));
      } else {
	assert(0 == "impossible status");
      }
      iter->status = not_before_queue_t::status_t::INVALID;
      removal_registry.erase_and_dispose(
	typename removal_registry_t::const_iterator(iter++),
	removal_registry_disposer_t{});
      removed++;
      if (max_removed && removed >= *max_removed) {
	break;
      }
    }
    return removed;
  }

  /**
   * accumulate
   *
   * (mimics std::accumulate() for a binary operator)
   * Accumulate (performing a 'left fold') over all entries.  Invokes passed
   * function with three params:
   * f(acc, v, eligible_for_dequeue);
   */
  template <class ACC, typename BOP>
  ACC accumulate(BOP&& op) const {
    ACC acc;
    acc = std::accumulate(
	eligible_queue.begin(), eligible_queue.end(), std::move(acc),
	[op](ACC&& acc, const auto& i) {
	  return op(std::move(acc), i.v, true);
	});
    acc = std::accumulate(
	ineligible_queue.begin(), ineligible_queue.end(), std::move(acc),
	[op](ACC&& acc, const auto& i) {
	  return op(std::move(acc), i.v, false);
	});
    return acc;
  }

  /**
   * for_each
   *
   * Traverse contents of queue.  Invokes passed function with two params:
   * f(val, eligible_for_dequeue);
   */
  template <typename F>
  void for_each(F&& f) const {
    for (auto&& i : eligible_queue) {
      std::invoke(f, i.v, true);
    }
    for (auto&& i : ineligible_queue) {
      std::invoke(f, i.v, false);
    }
  }

  template <typename F>
  void for_each_n(F&& f, int up_to) const {
    for (auto&& i : eligible_queue) {
      if (up_to-- <= 0) {
	return;
      }
      std::invoke(f, i.v, true);
    }
    for (auto&& i : ineligible_queue) {
      if (up_to-- <= 0) {
	return;
      }
      std::invoke(f, i.v, false);
    }
  }

  int total_count() const {
    return ineligible_queue.size() + eligible_queue.size();
  }

  int eligible_count() const { return eligible_queue.size(); }
};
