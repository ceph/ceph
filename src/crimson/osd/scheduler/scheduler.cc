// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <ostream>

#include <seastar/core/print.hh>

#include "crimson/osd/scheduler/scheduler.h"
#include "crimson/osd/scheduler/level_queue.h"
#include "crimson/osd/scheduler/mclock_scheduler.h"
#include "common/WeightedPriorityQueue.h"
#include "common/mclock_common.h"

namespace crimson::osd::scheduler {

/**
 * Implements Scheduler in terms of OpQueue
 *
 * Keep one eligible head per priority and owner in WPQ. Dependency levels
 * share that owner's turn, rather than becoming extra weighted clients.
 */
class ClassedOpQueueScheduler final : public Scheduler {
  const SchedulerClass cutoff;
  struct QueueKey {
    bool strict;
    unsigned priority;
    client_t owner;

    auto operator<=>(const QueueKey&) const = default;
  };
  std::map<QueueKey, LevelQueue> waiting;
  WeightedPriorityQueue<LevelQueue*, QueueKey> queue;

  using priority_t = uint64_t;
  std::array<
    priority_t,
    static_cast<size_t>(SchedulerClass::immediate)
  > priority_map = {
    // Placeholder, gets replaced with configured values
    0, 0, 0
  };

  static SchedulerClass get_io_prio_cut(ConfigProxy &conf) {
    if (conf.get_val<std::string>("osd_op_queue_cut_off") == "debug_random") {
      srand(time(NULL));
      return (rand() % 2 < 1) ?
        SchedulerClass::repop : SchedulerClass::immediate;
    } else if (conf.get_val<std::string>("osd_op_queue_cut_off") == "high") {
      return SchedulerClass::immediate;
    } else {
      return SchedulerClass::repop;
    }
  }

  bool use_strict(SchedulerClass kl) const {
    return static_cast<uint8_t>(kl) >= static_cast<uint8_t>(cutoff);
  }

  priority_t get_priority(SchedulerClass kl) const {
    ceph_assert(static_cast<size_t>(kl) <
		static_cast<size_t>(SchedulerClass::immediate));
    return priority_map[static_cast<size_t>(kl)];
  }

public:
  template <typename... Args>
  ClassedOpQueueScheduler(ConfigProxy &conf, Args&&... args) :
    cutoff(get_io_prio_cut(conf)),
    queue(std::forward<Args>(args)...)
  {
    priority_map[
      static_cast<size_t>(SchedulerClass::background_best_effort)
    ] = conf.get_val<uint64_t>("osd_scrub_priority");
    priority_map[
      static_cast<size_t>(SchedulerClass::background_recovery)
    ] = conf.get_val<uint64_t>("osd_recovery_op_priority");
    priority_map[
      static_cast<size_t>(SchedulerClass::client)
    ] = conf.get_val<uint64_t>("osd_client_op_priority");
    priority_map[
      static_cast<size_t>(SchedulerClass::repop)
    ] = conf.get_val<uint64_t>("osd_client_op_priority");
  }

private:
  QueueKey queue_key(const params_t &params) const {
    return {
      .strict = use_strict(params.klass),
      .priority = params.klass == SchedulerClass::immediate
        ? params.priority : static_cast<unsigned>(get_priority(params.klass)),
      .owner = params.owner,
    };
  }

  void enqueue_queue(const QueueKey &key, LevelQueue *pending) {
    if (key.strict) {
      queue.enqueue_strict(key, key.priority, std::move(pending));
    } else {
      auto cost = pending->front(throttle_level_t::foreground)->item.get_cost();
      queue.enqueue(key, key.priority, cost, std::move(pending));
    }
  }

  void enqueue(item_t &&item, bool front) {
    auto [it, inserted] = waiting.try_emplace(queue_key(item.params));
    it->second.push(std::move(item), front);
    if (inserted) {
      enqueue_queue(it->first, &it->second);
    }
  }

public:
  void enqueue(item_t &&item) final {
    enqueue(std::move(item), false);
  }

  void enqueue_front(item_t &&item) final {
    enqueue(std::move(item), true);
  }

  bool empty() const final {
    return waiting.empty();
  }

  WorkItem dequeue(throttle_level_t max_level) final {
    auto next = queue.dequeue_eligible([max_level](LevelQueue *pending)
        -> std::optional<unsigned> {
      if (auto *candidate = pending->front(max_level)) {
        return candidate->item.get_cost();
      }
      return std::nullopt;
    });
    if (!next) {
      return std::monostate{};
    }
    auto *pending = *next;
    auto item = pending->pop(pending->front(max_level));
    auto key = queue_key(item.params);
    if (pending->empty()) {
      waiting.erase(key);
    } else {
      enqueue_queue(key, pending);
    }
    return item;
  }

  void abort(std::exception_ptr error) final {
    for (auto &[key, pending] : waiting) {
      queue.remove_by_class(key);
      pending.abort(error);
    }
    waiting.clear();
  }

  void dump(ceph::Formatter &f) const final {
    return queue.dump(&f);
  }

  void print(std::ostream &out) const final {
    out << "ClassedOpQueueScheduler(queue=";
    queue.print(out);
    out << ", cutoff=" << cutoff << ")";
  }

  ~ClassedOpQueueScheduler() final {};
};

SchedulerRef make_scheduler(CephContext *cct, ConfigProxy &conf, int whoami, uint32_t nshards, int sid,
                            bool is_rotational, bool perf_cnt)
{
  const std::string _type = conf.get_val<std::string>("osd_op_queue");
  const std::string *type = &_type;
  if (*type == "debug_random") {
    static const std::string index_lookup[] = { "mclock_scheduler",
						"wpq" };
    srand(time(NULL));
    unsigned which = rand() % (sizeof(index_lookup) / sizeof(index_lookup[0]));
    type = &index_lookup[which];
  }

  if (*type == "wpq" ) {
    // default is 'wpq'
    return std::make_unique<ClassedOpQueueScheduler>(
	conf,
	conf.get_val<uint64_t>("osd_op_pq_max_tokens_per_priority"),
	conf->osd_op_pq_min_cost
      );
  } else if (*type == "mclock_scheduler") {
    return std::make_unique<mClockScheduler>(cct, whoami, nshards, sid, is_rotational, perf_cnt);
  } else {
    ceph_assert("Invalid choice of wq" == 0);
    return std::unique_ptr<mClockScheduler>();
  }
}

std::ostream &operator<<(std::ostream &lhs, const Scheduler &rhs) {
  rhs.print(lhs);
  return lhs;
}

}
