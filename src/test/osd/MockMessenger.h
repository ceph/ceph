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

#include <functional>
#include <vector>
#include "msg/Message.h"
#include "test/osd/EventLoop.h"
#include "test/osd/MockConnection.h"
#include "common/dout.h"
#include "common/TrackedOp.h"
#include "osd/OpRequest.h"
#include "msg/async/frames_v2.h"

#define dout_subsys ceph_subsys_osd

/**
 * MockMessenger - Routes messages through EventLoop with registered handlers
 *
 * This class provides a simple message routing mechanism for OSD tests.
 * Event handlers are registered as lambdas that process messages, and
 * messages are scheduled on the EventLoop for asynchronous processing.
 *
 * Epoch Handling:
 * - Messages are tagged with the sender's epoch when sent
 * - Messages are dropped if the receiver's current epoch doesn't match
 * - This simulates real OSD behavior where stale messages are ignored
 *
 * OpRequest Handling:
 * - Messages are wrapped in OpRequestRef before being passed to handlers
 * - This ensures proper lifetime management and prevents use-after-free issues
 * - Handlers receive OpRequestRef instead of raw Message* pointers
 */
class MockMessenger {
public:
  using MessageHandler = std::function<bool(int from_osd, int to_osd, MessageRef)>;
  using EpochGetter = std::function<epoch_t(int osd)>;
  
private:
  EventLoop* event_loop = nullptr;
  std::map<int, MessageHandler> handlers;
  EpochGetter epoch_getter;
  DoutPrefixProvider *dpp = nullptr;
  
public:
  MockMessenger(EventLoop* loop, CephContext* cct, DoutPrefixProvider *dpp = nullptr)
    : event_loop(loop), dpp(dpp) {
    ceph_assert(event_loop != nullptr);
    ceph_assert(cct != nullptr);
  }
  
  /**
   * Set the epoch getter callback.
   * This callback is used to get the current epoch for a given OSD.
   *
   * @param getter Lambda that accepts an OSD number and returns its current epoch
   */
  void set_epoch_getter(EpochGetter getter) {
    epoch_getter = std::move(getter);
  }
  
  /**
   * Set the DoutPrefixProvider for logging
   */
  void set_dpp(DoutPrefixProvider *d) {
    dpp = d;
  }
  
  /**
   * Register an event handler for processing messages.
   * Handlers are called in registration order until one returns true.
   *
   * @param type Message type code, or -1 for a catch-all handler
   * @param handler Lambda that accepts (from_osd, to_osd, Message*) and returns true if handled
   *                Handler is responsible for wrapping the message in appropriate tracker/reference type
   */
  void register_handler(int type, MessageHandler handler) {
    ceph_assert(!handlers.contains(type));
    handlers.emplace(type, std::move(handler));
  }

  /**
   * Register a type-safe handler for a specific message type.
   * Uses C++20 concepts to ensure type safety at compile time.
   * The handler receives a typed intrusive_ptr; lifetime is managed by
   * the smart pointer.  If the handler wants to extend the message's
   * lifetime (e.g. by storing it in an OpRequest that consumes a
   * refcount), it can call `.detach()` to transfer ownership without
   * touching the refcount manually.
   *
   * @tparam MsgType The specific message type (e.g., MOSDECSubOpWrite)
   * @param msg_type The message type code (e.g., MSG_OSD_EC_WRITE)
   * @param handler Lambda that accepts (from_osd, to_osd,
   *                boost::intrusive_ptr<MsgType>) and returns true if handled
   */
  template<typename MsgType>
  requires std::derived_from<MsgType, Message>
  void register_typed_handler(
      int msg_type,
      std::function<bool(int, int, boost::intrusive_ptr<MsgType>)> handler) {
    // Wrap the typed handler in a generic handler that performs the cast
    register_handler(msg_type,
      [handler](int from_osd, int to_osd, MessageRef m) -> bool {
        auto typed = boost::dynamic_pointer_cast<MsgType>(m);
        if (!typed) {
          return false;  // Wrong type, let other handlers try
        }
        return handler(from_osd, to_osd, std::move(typed));
      });
  }
  
  /**
   * Send a message from one OSD to another.
   * The message will be scheduled on the EventLoop and processed by
   * registered handlers. The sender's epoch is captured at send time.
   * Messages are dropped if the receiver's epoch doesn't match the message epoch.
   * Handlers receive the raw Message* and are responsible for wrapping it in
   * appropriate tracker/reference types (e.g., OpRequestRef).
   * Panics if no handler processes the message.
   *
   * @param from_osd Source OSD number
   * @param to_osd Destination OSD number
   * @param m Message to send (takes ownership)
   */
  void send_message(int from_osd, int to_osd, Message* m) {
    ceph_assert(from_osd >= 0);
    ceph_assert(to_osd >= 0);
    ceph_assert(m != nullptr);
    
    // Wrap in MessageRef to manage lifetime
    MessageRef mref(m);
    
    // Capture the receiver's epoch at send time for epoch checking
    epoch_t send_epoch = 0;
    if (epoch_getter) {
      send_epoch = epoch_getter(to_osd);
    }
    
    // Set the message header's source to the sender OSD BEFORE encoding
    // This is critical for peering messages - MOSDPGInfo2::get_event() uses
    // get_source().num() to construct the pg_shard_t for MInfoRec
    ceph_msg_header h = m->get_header();
    h.src.num = from_osd;
    m->set_header(h);
    
    if (dpp) {
      ldpp_dout(dpp, 10) << "MockMessenger: Scheduling message from OSD." << from_osd
                         << " to OSD." << to_osd << " (type: " << m->get_type()
                         << ", epoch: " << send_epoch << ")" << dendl;
    }
    
    // Encode the message payload to prepare it for transmission
    // This ensures internal structures like txn_payload are properly serialized
    m->encode(CEPH_FEATURES_ALL, 0);
    
    // Copy the message components to simulate network transmission
    // This creates independent copies that can be decoded into a new message object
    // on the receiver side, avoiding use-after-free issues

    ceph_msg_header &header = m->get_header();
    ceph_msg_footer &footer = m->get_footer();

    ceph_msg_header2 header2{header.seq,        header.tid,
                             header.type,       header.priority,
                             header.version,
                             ceph_le32(0),      header.data_off,
                             ceph_le64(0),
                             footer.flags,      header.compat_version,
                             header.reserved};

    auto mf = ceph::msgr::v2::MessageFrame::Encode(
                               header2,
                               m->get_payload(),
                               m->get_middle(),
                               m->get_data());


    // Schedule event on EventLoop with the copied message components
    event_loop->schedule_osd_message(from_osd, to_osd,
      [this, header, footer, mf, from_osd, to_osd, send_epoch]() mutable {
        // Get the receiver's current epoch when processing
        epoch_t current_epoch = 0;
        if (epoch_getter) {
          current_epoch = epoch_getter(to_osd);
        }
        
        // Drop messages from different epochs (both old and new)
        if (current_epoch != send_epoch) {
          if (dpp) {
            ldpp_dout(dpp, 10) << "MockMessenger: Dropping message from OSD." << from_osd
                               << " to OSD." << to_osd << " (type: " << header.type
                               << ") - epoch mismatch: send_epoch=" << send_epoch
                               << " current_epoch=" << current_epoch << dendl;
          }
          return;
        }
        
        // Decode the message components into a new message object
        // This uses the proper decode_message() function that real messengers use,
        // which supports the new footer format with message authentication.
        // ConnectionRef takes the new MockConnection with add_ref=false so the
        // +1 from `new` is consumed exactly once (decode_message moves the
        // ConnectionRef into the Message; the Message destructor releases it).
        Message::ConnectionRef con{new MockConnection(from_osd), /*add_ref=*/false};
        Message *decoded_msg = decode_message(g_ceph_context, 0, header, footer,
                                              mf.front(), mf.middle(), mf.data(), con);

        ceph_assert(decoded_msg);
        // Wrap decode_message's +1 in a MessageRef so the Message's lifetime
        // is managed automatically: the ref drops when this lambda returns,
        // which releases the +1.  Handlers that want to extend lifetime
        // receive the MessageRef by value (or a typed intrusive_ptr) and can
        // .detach() to transfer ownership cleanly.
        MessageRef decoded{decoded_msg, /*add_ref=*/false};

        // Try specific handler first, then catch-all handler (-1)
        if (!handlers.contains(header.type)) {
          std::cerr << "ERROR: No handler registered for message type " << header.type
                    << " (0x" << std::hex << header.type << std::dec << ")" << std::endl;
          std::cerr << "Registered handlers: ";
          for (const auto& [type, _] : handlers) {
            std::cerr << type << " (0x" << std::hex << type << std::dec << "), ";
          }
          std::cerr << std::endl;
        }
        ceph_assert(handlers.contains(header.type));
        ceph_assert(handlers.at(header.type)(from_osd, to_osd, decoded));
      });
  }
  
  /**
   * Get the number of registered handlers
   */
  size_t handler_count() const {
    return handlers.size();
  }
  
  /**
   * Clear all registered handlers
   */
  void clear_handlers() {
    handlers.clear();
  }
  
};

// Made with Bob
