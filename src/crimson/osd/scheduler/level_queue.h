// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <array>
#include <list>
#include <utility>

#include "crimson/osd/scheduler/scheduler.h"

namespace crimson::osd::scheduler {

// A FIFO within one QoS domain. Separate level queues let dependencies
// bypass blocked work without scanning its backlog or splitting its QoS
// budget. The oldest eligible head competes with the other QoS domains.
class LevelQueue {
public:
  struct Entry {
    item_t item;
    double arrival;
    int64_t order;
    seastar::optimized_optional<seastar::abort_source::subscription> subscription;
  };
  using iterator = std::list<Entry>::iterator;

private:
  std::array<std::list<Entry>, std::to_underlying(throttle_level_t::count)> items;
  int64_t first = 0;
  int64_t last = 0;
  size_t count = 0;

public:
  iterator push(item_t &&item, bool front = false) {
    auto &queue = items[std::to_underlying(item.level)];
    Entry entry{std::move(item), crimson::dmclock::get_time(),
                front ? --first : last++, {}};
    auto position = queue.insert(front ? queue.begin() : queue.end(),
                                 std::move(entry));
    ++count;
    return position;
  }

  // The subscription belongs to the queue entry. Dequeue unregisters it
  // before granting a slot, so cancellation cannot withdraw admitted work.
  template <typename F>
  void make_abortable(iterator entry, F &&cancel) {
    if (auto *abort = entry->item.abort) {
      if (abort->abort_requested()) {
        cancel();
      } else {
        entry->subscription = abort->subscribe(
          [cancel = std::forward<F>(cancel)]() mutable noexcept {
            // Removing the entry also destroys this subscription.
            auto callback = std::move(cancel);
            callback();
          });
      }
    }
  }

  item_t erase(iterator entry) {
    auto &queue = items[std::to_underlying(entry->item.level)];
    auto item = std::move(entry->item);
    queue.erase(entry);
    --count;
    return item;
  }

  Entry *front(throttle_level_t max_level) {
    Entry *next = nullptr;
    for (size_t level = 0; level <= std::to_underlying(max_level); ++level) {
      auto &queue = items[level];
      if (!queue.empty() && (!next || queue.front().order < next->order)) {
        next = &queue.front();
      }
    }
    return next;
  }

  item_t pop(Entry *entry) {
    auto &queue = items[std::to_underlying(entry->item.level)];
    ceph_assert(&queue.front() == entry);
    return erase(queue.begin());
  }

  void abort(std::exception_ptr error) {
    for (auto &queue : items) {
      for (auto &entry : queue) {
        entry.item.wake.set_exception(error);
      }
      queue.clear();
    }
    count = 0;
  }

  bool empty() const { return count == 0; }
  size_t size() const { return count; }
};

}
