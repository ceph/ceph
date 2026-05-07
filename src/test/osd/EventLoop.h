// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <iostream>
#include <functional>
#include <deque>
#include <map>
#include <set>
#include <list>
#include <vector>
#include "include/types.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"
#include "osd/PeeringState.h"
#include "os/ObjectStore.h"

/**
 * EventLoop - Unified single-threaded event loop for OSD tests.
 *
 * Combines EC backend messages, ObjectStore transactions, peering messages,
 * and peering events into a single deterministic queue.  This allows tests
 * to properly interleave peering state changes with EC backend operations.
 */
class EventLoop {
public:
  using GenericEvent = std::function<void()>;
  
  enum class EventType {
    GENERIC,
    OSD_MESSAGE,
    TRANSACTION,
    PEERING_MESSAGE,
    CLUSTER_MESSAGE,
    PEERING_EVENT
  };
  
private:
  struct Event {
    EventType type;
    int from_osd;  // -1 for generic events or when not applicable
    int to_osd;    // -1 for generic events or when not applicable
    GenericEvent callback;
    
    Event(EventType t, int from, int to, GenericEvent cb)
      : type(t), from_osd(from), to_osd(to), callback(std::move(cb)) {}
  };
  
  std::deque<Event> events;
  bool verbose = false;
  int events_executed = 0;
  std::map<EventType, int> events_by_type;
  std::set<int> suspended_from_osds;
  std::set<int> suspended_to_osds;
  std::set<std::pair<int, int>> suspended_from_to_osds;
  int current_osd = -1;
  
  // Map from OSD number to its queue of suspended events
  std::map<int, std::deque<Event>> suspended_events;
  
  // Callback to check for more work when idle (returns true if more work was generated)
  std::list<std::function<bool()>> idle_callbacks;
  
  static constexpr const char* event_type_name(EventType type) {
    switch (type) {
      case EventType::GENERIC: return "GENERIC";
      case EventType::OSD_MESSAGE: return "OSD_MESSAGE";
      case EventType::TRANSACTION: return "TRANSACTION";
      case EventType::PEERING_MESSAGE: return "PEERING_MESSAGE";
      case EventType::CLUSTER_MESSAGE: return "CLUSTER_MESSAGE";
      case EventType::PEERING_EVENT: return "PEERING_EVENT";
      default: return "UNKNOWN";
    }
  }
  
public:
  EventLoop(bool verbose = false) : verbose(verbose) {}
  
  void schedule_generic(GenericEvent event) {
    events.emplace_back(EventType::GENERIC, -1, -1, std::move(event));
  }
  
  void schedule_osd_message(int from_osd, int to_osd, GenericEvent callback) {
    ceph_assert(from_osd >= 0);
    ceph_assert(to_osd >= 0);
    events.emplace_back(EventType::OSD_MESSAGE, from_osd, to_osd, std::move(callback));
  }
  
  void schedule_transaction(int osd, GenericEvent callback) {
    ceph_assert(osd >= 0);
    events.emplace_back(EventType::TRANSACTION, -1, osd, std::move(callback));
  }
  
  void schedule_peering_message(int from_osd, int to_osd, GenericEvent callback) {
    ceph_assert(from_osd >= 0);
    ceph_assert(to_osd >= 0);
    events.emplace_back(EventType::PEERING_MESSAGE, from_osd, to_osd, std::move(callback));
  }
  
  void schedule_cluster_message(int from_osd, int to_osd, GenericEvent callback) {
    ceph_assert(from_osd >= 0);
    ceph_assert(to_osd >= 0);
    events.emplace_back(EventType::CLUSTER_MESSAGE, from_osd, to_osd, std::move(callback));
  }
  
  void schedule_peering_event(int osd, GenericEvent callback) {
    ceph_assert(osd >= 0);
    events.emplace_back(EventType::PEERING_EVENT, -1, osd, std::move(callback));
  }
  
  bool has_events() const {
    return !events.empty();
  }
  
  void register_idle_callback(std::function<bool()> callback) {
    idle_callbacks.emplace_back(std::move(callback));
  }
  
  size_t queued_event_count() const {
    return events.size();
  }
  
  int get_events_executed() const {
    return events_executed;
  }
  
  const std::map<EventType, int>& get_stats_by_type() const {
    return events_by_type;
  }
  
  void reset_stats() {
    events_executed = 0;
    events_by_type.clear();
  }
  
  bool run_one() {
    if (events.empty()) {
      return false;
    }
    
    Event event = std::move(events.front());
    events.pop_front();
    
    // Check if this event should be suspended based on from_osd, to_osd, or from-to pair
    bool should_suspend = false;
    int suspend_osd = -1;
    
    if (event.from_osd >= 0 && is_from_osd_suspended(event.from_osd)) {
      should_suspend = true;
      suspend_osd = event.from_osd;
    } else if (event.to_osd >= 0 && is_to_osd_suspended(event.to_osd)) {
      should_suspend = true;
      suspend_osd = event.to_osd;
    } else if (event.from_osd >= 0 && event.to_osd >= 0 &&
               is_from_to_osd_suspended(event.from_osd, event.to_osd)) {
      should_suspend = true;
      suspend_osd = event.to_osd;  // Use to_osd for queue key
    }
    
    if (should_suspend) {
      // Move to suspended queue
      if (verbose) {
        std::cout << "  [Event " << (events_executed + 1) << "] "
                  << event_type_name(event.type);
        if (event.from_osd >= 0 && event.to_osd >= 0) {
          std::cout << " (OSD." << event.from_osd << " -> OSD." << event.to_osd << ")";
        } else if (event.to_osd >= 0) {
          std::cout << " (to OSD." << event.to_osd << ")";
        } else if (event.from_osd >= 0) {
          std::cout << " (from OSD." << event.from_osd << ")";
        }
        std::cout << " *** SUSPENDED - moving to suspended queue ***" << std::endl;
      }
      suspended_events[suspend_osd].push_back(std::move(event));
      return true;  // We processed an event (by suspending it)
    }
    
    // Print banner if switching to a different OSD
    int active_osd = (event.to_osd >= 0) ? event.to_osd : event.from_osd;
    if (active_osd >= 0 && active_osd != current_osd) {
      current_osd = active_osd;
      if (verbose) {
        std::cout << "\n==== Processing events for OSD." << current_osd
                  << " ====" << std::endl;
      }
    }
    
    if (verbose) {
      std::cout << "  [Event " << (events_executed + 1) << "] "
                << event_type_name(event.type);
      if (event.from_osd >= 0 && event.to_osd >= 0) {
        std::cout << " (OSD." << event.from_osd << " -> OSD." << event.to_osd << ")";
      } else if (event.to_osd >= 0) {
        std::cout << " (to OSD." << event.to_osd << ")";
      } else if (event.from_osd >= 0) {
        std::cout << " (from OSD." << event.from_osd << ")";
      }
      std::cout << " Executing..." << std::endl;
    }
    
    // Execute the event
    event.callback();
    events_executed++;
    events_by_type[event.type]++;
    
    return true;
  }
  
  int run_many(int count) {
    if (verbose) {
      std::cout << "\n=== Running " << count << " events ===" << std::endl;
    }
    
    int executed = 0;
    for (int i = 0; i < count && run_one(); i++) {
      executed++;
    }
    
    if (verbose) {
      std::cout << "=== Executed " << executed << " events, "
                << events.size() << " remaining ===" << std::endl;
    }
    
    return executed;
  }

  bool do_idle_callbacks() {
    bool new_work = false;
    for (auto cb : idle_callbacks) {
      if (cb()) {
        new_work = true;
      }
    }

    return new_work;
  }

  /**
   * Run until the queue is empty or max_events is reached.
   * Returns -1 if max_events was reached before the queue emptied.
   */
  void run_until_idle(int max_events = 10000) {
    if (verbose) {
      std::cout << "\n=== Running until idle";
      if (max_events > 0) {
        std::cout << " (max " << max_events << " events)";
      }
      std::cout << " ===" << std::endl;
    }

    do {
      ceph_assert(--max_events);
      while (has_events()) {
        run_one();
      }
    } while (do_idle_callbacks());
  }

  void clear() {
    events.clear();
    suspended_events.clear();
  }
  
  void set_verbose(bool v) {
    verbose = v;
  }
  
  // OSD management methods
  /**
   * Suspend events FROM an OSD - events originating from this OSD will be queued
   * but not executed until the OSD is unsuspended.
   */
  void suspend_from_osd(int osd) {
    suspended_from_osds.insert(osd);
    if (verbose) {
      std::cout << "*** Events FROM OSD." << osd << " marked as SUSPENDED ***" << std::endl;
    }
  }
  
  /**
   * Unsuspend events FROM an OSD - queued events from this OSD will be processed
   * on subsequent run_one() calls.
   */
  void unsuspend_from_osd(int osd) {
    suspended_from_osds.erase(osd);
    
    // Move all suspended events for this OSD back to the main queue
    auto it = suspended_events.find(osd);
    if (it != suspended_events.end()) {
      if (verbose) {
        std::cout << "*** Events FROM OSD." << osd << " marked as UNSUSPENDED - restoring "
                  << it->second.size() << " suspended events ***" << std::endl;
      }
      
      // Append suspended events to the main queue
      for (auto& event : it->second) {
        events.push_back(std::move(event));
      }
      
      suspended_events.erase(it);
    } else {
      if (verbose) {
        std::cout << "*** Events FROM OSD." << osd << " marked as UNSUSPENDED ***" << std::endl;
      }
    }
  }
  
  /**
   * Suspend events TO an OSD - events destined for this OSD will be queued
   * but not executed until the OSD is unsuspended.
   */
  void suspend_to_osd(int osd) {
    suspended_to_osds.insert(osd);
    if (verbose) {
      std::cout << "*** Events TO OSD." << osd << " marked as SUSPENDED ***" << std::endl;
    }
  }
  
  /**
   * Unsuspend events TO an OSD - queued events to this OSD will be processed
   * on subsequent run_one() calls.
   */
  void unsuspend_to_osd(int osd) {
    suspended_to_osds.erase(osd);
    
    // Move all suspended events for this OSD back to the main queue
    auto it = suspended_events.find(osd);
    if (it != suspended_events.end()) {
      if (verbose) {
        std::cout << "*** Events TO OSD." << osd << " marked as UNSUSPENDED - restoring "
                  << it->second.size() << " suspended events ***" << std::endl;
      }
      
      // Append suspended events to the main queue
      for (auto& event : it->second) {
        events.push_back(std::move(event));
      }
      
      suspended_events.erase(it);
    } else {
      if (verbose) {
        std::cout << "*** Events TO OSD." << osd << " marked as UNSUSPENDED ***" << std::endl;
      }
    }
  }
  
  /**
   * Suspend events FROM one OSD TO another OSD - events from from_osd to to_osd
   * will be queued but not executed until unsuspended.
   */
  void suspend_from_to_osd(int from_osd, int to_osd) {
    suspended_from_to_osds.insert({from_osd, to_osd});
    if (verbose) {
      std::cout << "*** Events FROM OSD." << from_osd << " TO OSD." << to_osd
                << " marked as SUSPENDED ***" << std::endl;
    }
  }
  
  /**
   * Unsuspend events FROM one OSD TO another OSD - queued events will be processed
   * on subsequent run_one() calls.
   */
  void unsuspend_from_to_osd(int from_osd, int to_osd) {
    suspended_from_to_osds.erase({from_osd, to_osd});
    
    // Move all suspended events for this OSD pair back to the main queue
    auto it = suspended_events.find(to_osd);
    if (it != suspended_events.end()) {
      if (verbose) {
        std::cout << "*** Events FROM OSD." << from_osd << " TO OSD." << to_osd
                  << " marked as UNSUSPENDED - restoring "
                  << it->second.size() << " suspended events ***" << std::endl;
      }
      
      // Append suspended events to the main queue
      for (auto& event : it->second) {
        events.push_back(std::move(event));
      }
      
      suspended_events.erase(it);
    } else {
      if (verbose) {
        std::cout << "*** Events FROM OSD." << from_osd << " TO OSD." << to_osd
                  << " marked as UNSUSPENDED ***" << std::endl;
      }
    }
  }
  
  bool is_from_osd_suspended(int osd) const {
    return suspended_from_osds.find(osd) != suspended_from_osds.end();
  }
  
  bool is_to_osd_suspended(int osd) const {
    return suspended_to_osds.find(osd) != suspended_to_osds.end();
  }
  
  bool is_from_to_osd_suspended(int from_osd, int to_osd) const {
    return suspended_from_to_osds.find({from_osd, to_osd}) != suspended_from_to_osds.end();
  }
  
  size_t get_suspended_event_count(int osd) const {
    auto it = suspended_events.find(osd);
    return (it != suspended_events.end()) ? it->second.size() : 0;
  }
  
  size_t get_total_suspended_events() const {
    size_t total = 0;
    for (const auto& pair : suspended_events) {
      total += pair.second.size();
    }
    return total;
  }
  
  void print_stats() const {
    if (events_by_type.empty()) {
      return;
    }
    
    std::cout << "=== Event Statistics ===" << std::endl;
    for (const auto& [type, count] : events_by_type) {
      std::cout << "  " << event_type_name(type) << ": " << count << std::endl;
    }
    std::cout << "  TOTAL: " << events_executed << std::endl;
  }
};

