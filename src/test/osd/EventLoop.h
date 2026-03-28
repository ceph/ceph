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
#include <queue>
#include <map>
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
    int osd;  // -1 for generic events
    GenericEvent callback;
    
    Event(EventType t, int o, GenericEvent cb)
      : type(t), osd(o), callback(std::move(cb)) {}
  };
  
  std::queue<Event> event_queue;
  bool verbose = false;
  int events_executed = 0;
  std::map<EventType, int> events_by_type;
  
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
    event_queue.emplace(EventType::GENERIC, -1, std::move(event));
  }
  
  void schedule_osd_message(int osd, GenericEvent callback) {
    event_queue.emplace(EventType::OSD_MESSAGE, osd, std::move(callback));
  }
  
  void schedule_transaction(int osd, GenericEvent callback) {
    event_queue.emplace(EventType::TRANSACTION, osd, std::move(callback));
  }
  
  void schedule_peering_message(int to_osd, GenericEvent callback) {
    event_queue.emplace(EventType::PEERING_MESSAGE, to_osd, std::move(callback));
  }
  
  void schedule_cluster_message(int to_osd, GenericEvent callback) {
    event_queue.emplace(EventType::CLUSTER_MESSAGE, to_osd, std::move(callback));
  }
  
  void schedule_peering_event(int osd, GenericEvent callback) {
    event_queue.emplace(EventType::PEERING_EVENT, osd, std::move(callback));
  }
  
  bool has_events() const {
    return !event_queue.empty();
  }
  
  size_t queued_event_count() const {
    return event_queue.size();
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
    if (event_queue.empty()) {
      return false;
    }
    
    Event event = std::move(event_queue.front());
    event_queue.pop();
    
    if (verbose) {
      std::cout << "  [Event " << (events_executed + 1) << "] "
                << event_type_name(event.type);
      if (event.osd >= 0) {
        std::cout << " (OSD " << event.osd << ")";
      }
      std::cout << " Executing..." << std::endl;
    }
    
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
                << event_queue.size() << " remaining ===" << std::endl;
    }
    
    return executed;
  }
  
  /**
   * Run until the queue is empty or max_events is reached.
   * Returns -1 if max_events was reached before the queue emptied.
   */
  int run_until_idle(int max_events = 0) {
    if (verbose) {
      std::cout << "\n=== Running until idle";
      if (max_events > 0) {
        std::cout << " (max " << max_events << " events)";
      }
      std::cout << " ===" << std::endl;
    }
    
    int executed = 0;
    while (has_events()) {
      if (max_events > 0 && executed >= max_events) {
        if (verbose) {
          std::cout << "=== Max events (" << max_events << ") reached, " 
                    << event_queue.size() << " events remaining ===" << std::endl;
        }
        return -1;  // Timeout
      }
      
      run_one();
      executed++;
    }
    
    if (verbose) {
      std::cout << "=== Idle: Executed " << executed << " events ===" << std::endl;
      print_stats();
    }
    
    return executed;
  }
  
  /**
   * Run until a condition is met, idle, or max_events is reached.
   * The condition is checked after each event execution.
   * Returns -1 if max_events was reached.
   */
  int run_until(int max_events, std::function<bool()> condition) {
    if (verbose) {
      std::cout << "\n=== Running until condition";
      if (max_events > 0) {
        std::cout << " (max " << max_events << " events)";
      }
      std::cout << " ===" << std::endl;
    }
    
    int executed = 0;
    while (has_events()) {
      if (max_events > 0 && executed >= max_events) {
        if (verbose) {
          std::cout << "=== Max events (" << max_events << ") reached ===" << std::endl;
        }
        return -1;  // Timeout
      }
      
      run_one();
      executed++;
      
      if (condition()) {
        if (verbose) {
          std::cout << "=== Condition met after " << executed << " events ===" << std::endl;
        }
        return executed;
      }
    }
    
    if (verbose) {
      std::cout << "=== Idle: Executed " << executed << " events, condition not met ===" << std::endl;
    }
    
    return executed;
  }
  
  void clear() {
    while (!event_queue.empty()) {
      event_queue.pop();
    }
  }
  
  void set_verbose(bool v) {
    verbose = v;
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

