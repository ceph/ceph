// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
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
 * EventLoop - Unified event loop for OSD tests
 * 
 * This class unifies two previously separate event dispatch systems:
 * 1. TestRunner::run_until_idle() - EC backend messages + ObjectStore transactions
 * 2. dispatch_all() - Peering messages + peering events
 * 
 * By combining these into a single event loop, tests can properly interleave
 * peering state changes with EC backend operations, enabling comprehensive
 * testing of failover scenarios.
 * 
 * Key features:
 * - Single unified event queue with multiple event types
 * - Deterministic, single-threaded execution
 * - Support for generic events, EC messages, transactions, peering messages, and events
 * - Multiple execution modes: run_one, run_many, run_until_idle, run_until
 * - Per-OSD queue management for multi-OSD tests
 * 
 * Event Types:
 * - Generic: std::function<void()> callbacks
 * - EC Messages: Backend messages with OpRequests
 * - Transactions: ObjectStore transactions
 * - Peering Messages: Messages between OSDs for peering
 * - Peering Events: Local peering state machine events
 * 
 * Example usage:
 *   EventLoop loop;
 *   loop.schedule_generic([](){ std::cout << "Event\n"; });
 *   loop.schedule_ec_message(0, msg, op);
 *   loop.schedule_peering_message(0, 1, peering_msg);
 *   loop.run_until_idle();
 */
class EventLoop {
public:
  using GenericEvent = std::function<void()>;
  
  // Event type enumeration for priority and debugging
  enum class EventType {
    GENERIC,
    EC_MESSAGE,
    TRANSACTION,
    PEERING_MESSAGE,
    CLUSTER_MESSAGE,
    PEERING_EVENT
  };
  
private:
  // Internal event wrapper that tracks type and OSD
  struct Event {
    EventType type;
    int osd;  // -1 for generic events
    GenericEvent callback;
    
    Event(EventType t, int o, GenericEvent cb)
      : type(t), osd(o), callback(std::move(cb)) {}
  };
  
  // Main event queue - all events go here
  std::queue<Event> event_queue;
  
  // Statistics and debugging
  bool verbose = false;
  int events_executed = 0;
  std::map<EventType, int> events_by_type;
  
  // Helper to get event type name for logging
  static constexpr const char* event_type_name(EventType type) {
    switch (type) {
      case EventType::GENERIC: return "GENERIC";
      case EventType::EC_MESSAGE: return "EC_MESSAGE";
      case EventType::TRANSACTION: return "TRANSACTION";
      case EventType::PEERING_MESSAGE: return "PEERING_MESSAGE";
      case EventType::CLUSTER_MESSAGE: return "CLUSTER_MESSAGE";
      case EventType::PEERING_EVENT: return "PEERING_EVENT";
      default: return "UNKNOWN";
    }
  }
  
public:
  EventLoop(bool verbose = false) : verbose(verbose) {}
  
  // ========================================================================
  // Scheduling Methods - Add events to the queue
  // ========================================================================
  
  /**
   * Schedule a generic event (lambda, functor, etc.)
   */
  void schedule_generic(GenericEvent event) {
    event_queue.emplace(EventType::GENERIC, -1, std::move(event));
  }
  
  /**
   * Schedule an EC backend message for processing
   * 
   * @param osd The OSD that will process this message
   * @param callback The function to execute (typically calls backend._handle_message)
   */
  void schedule_ec_message(int osd, GenericEvent callback) {
    event_queue.emplace(EventType::EC_MESSAGE, osd, std::move(callback));
  }
  
  /**
   * Schedule an ObjectStore transaction
   * 
   * @param osd The OSD that owns this transaction
   * @param callback The function to execute (typically processes the transaction)
   */
  void schedule_transaction(int osd, GenericEvent callback) {
    event_queue.emplace(EventType::TRANSACTION, osd, std::move(callback));
  }
  
  /**
   * Schedule a peering message to be sent between OSDs
   * 
   * @param from_osd Source OSD
   * @param to_osd Destination OSD
   * @param callback The function to execute (typically delivers the message)
   */
  void schedule_peering_message(int from_osd, int to_osd, GenericEvent callback) {
    event_queue.emplace(EventType::PEERING_MESSAGE, to_osd, std::move(callback));
  }
  
  /**
   * Schedule a cluster message to be sent between OSDs
   * 
   * @param from_osd Source OSD
   * @param to_osd Destination OSD
   * @param callback The function to execute (typically delivers the message)
   */
  void schedule_cluster_message(int from_osd, int to_osd, GenericEvent callback) {
    event_queue.emplace(EventType::CLUSTER_MESSAGE, to_osd, std::move(callback));
  }
  
  /**
   * Schedule a peering event for local processing
   * 
   * @param osd The OSD that will process this event
   * @param callback The function to execute (typically calls ps->handle_event)
   */
  void schedule_peering_event(int osd, GenericEvent callback) {
    event_queue.emplace(EventType::PEERING_EVENT, osd, std::move(callback));
  }
  
  // ========================================================================
  // Query Methods
  // ========================================================================
  
  /**
   * Check if there are pending events
   */
  bool has_events() const {
    return !event_queue.empty();
  }
  
  /**
   * Get the number of pending events
   */
  size_t pending_count() const {
    return event_queue.size();
  }
  
  /**
   * Get the total number of events executed
   */
  int get_events_executed() const {
    return events_executed;
  }
  
  /**
   * Get statistics by event type
   */
  const std::map<EventType, int>& get_stats_by_type() const {
    return events_by_type;
  }
  
  /**
   * Reset the event counter and statistics
   */
  void reset_stats() {
    events_executed = 0;
    events_by_type.clear();
  }
  
  // ========================================================================
  // Execution Methods
  // ========================================================================
  
  /**
   * Run a single event
   * 
   * Executes the next event in the queue. Events can schedule more events.
   * 
   * @return true if an event was executed, false if queue was empty
   */
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
  
  /**
   * Run multiple events
   * 
   * Executes up to 'count' events from the queue. Stops early if the queue
   * becomes empty. Events can schedule more events during execution.
   * 
   * @param count Maximum number of events to execute
   * @return Number of events actually executed
   */
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
   * Run until the queue is empty or max_events is reached
   * 
   * Continues executing events until either:
   * - The queue becomes empty (idle), or
   * - max_events have been executed (to prevent infinite loops)
   * 
   * @param max_events Maximum number of events to execute (0 = unlimited)
   * @return Number of events executed, or -1 if max_events was reached
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
   * Run until a condition is met, idle, or max_events is reached
   * 
   * Continues executing events until either:
   * - The condition returns true, or
   * - The queue becomes empty (idle), or
   * - max_events have been executed
   * 
   * The condition is checked after each event execution.
   * 
   * @param max_events Maximum number of events to execute (0 = unlimited)
   * @param condition Function that returns true when done
   * @return Number of events executed, or -1 if max_events was reached
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
  
  /**
   * Clear all pending events
   */
  void clear() {
    while (!event_queue.empty()) {
      event_queue.pop();
    }
  }
  
  /**
   * Set verbose logging
   */
  void set_verbose(bool v) {
    verbose = v;
  }
  
  /**
   * Print statistics about executed events
   */
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

