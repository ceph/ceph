// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#ifndef CEPH_WATCH_H
#define CEPH_WATCH_H

#include <set>
#include "msg/Connection.h"
#include "include/Context.h"

enum WatcherState {
  WATCHER_PENDING,
  WATCHER_NOTIFIED,
};

class OSDService;
class PrimaryLogPG;
void intrusive_ptr_add_ref(PrimaryLogPG *pg);
void intrusive_ptr_release(PrimaryLogPG *pg);
struct ObjectContext;
class MWatchNotify;

class Watch;
typedef std::shared_ptr<Watch> WatchRef;
typedef std::weak_ptr<Watch> WWatchRef;

class Notify;
typedef std::shared_ptr<Notify> NotifyRef;
typedef std::weak_ptr<Notify> WNotifyRef;

struct CancelableContext;

/**
 * Notify tracks the progress of a particular notify
 *
 * References are held by Watch and the timeout callback.
 */
class Notify {
  friend class NotifyTimeoutCB;
  friend class Watch;
  WNotifyRef self;
  ConnectionRef client;
  uint64_t client_gid;
  bool complete;
  bool discarded;
  bool timed_out;  ///< true if the notify timed out
  std::set<WatchRef> watchers;

  ceph::buffer::list payload;
  uint32_t timeout;
  uint64_t cookie;
  uint64_t notify_id;
  uint64_t version;

  OSDService *osd;
  CancelableContext *cb;
  ceph::mutex lock = ceph::make_mutex("Notify::lock");

  /// (gid,cookie) -> reply_bl for everyone who acked the notify
  std::multimap<std::pair<uint64_t,uint64_t>, ceph::buffer::list> notify_replies;

  /// true if this notify is being discarded
  bool is_discarded() {
    return discarded || complete;
  }

  /// Sends notify completion if watchers.empty() or timeout
  void maybe_complete_notify();

  /// Called on Notify timeout
  void do_timeout();

  Notify(
    ConnectionRef client,
    uint64_t client_gid,
    ceph::buffer::list& payload,
    uint32_t timeout,
    uint64_t cookie,
    uint64_t notify_id,
    uint64_t version,
    OSDService *osd);

  /// registers a timeout callback with the watch_timer
  void register_cb();

  /// removes the timeout callback, called on completion or cancellation
  void unregister_cb();
public:

  std::ostream& gen_dbg_prefix(std::ostream& out) {
    return out << "Notify(" << std::make_pair(cookie, notify_id) << " "
        << " watchers=" << watchers.size()
        << ") ";
  }
  void set_self(NotifyRef _self) {
    self = _self;
  }
  static NotifyRef makeNotifyRef(
    ConnectionRef client,
    uint64_t client_gid,
    ceph::buffer::list &payload,
    uint32_t timeout,
    uint64_t cookie,
    uint64_t notify_id,
    uint64_t version,
    OSDService *osd);

  /// Call after creation to initialize
  void init();

  /// Called once per watcher prior to init()
  void start_watcher(
    WatchRef watcher ///< [in] watcher to complete
    );

  /// Called once per NotifyAck
  void complete_watcher(
    WatchRef watcher, ///< [in] watcher to complete
    ceph::buffer::list& reply_bl ///< [in] reply buffer from the notified watcher
    );
  /// Called when a watcher unregisters or times out
  void complete_watcher_remove(
    WatchRef watcher ///< [in] watcher to complete
    );

  /// Called when the notify is canceled due to a new peering interval
  void discard();
};

/**
 * Watch is a mapping between a Connection and an ObjectContext
 *
 * References are held by ObjectContext and the timeout callback
 */
class HandleWatchTimeout;
class HandleDelayedWatchTimeout;
class Watch {
  WWatchRef self;
  friend class HandleWatchTimeout;
  friend class HandleDelayedWatchTimeout;
  ConnectionRef conn;
  CancelableContext *cb;

  OSDService *osd;
  boost::intrusive_ptr<PrimaryLogPG> pg;
  std::shared_ptr<ObjectContext> obc;

  std::map<uint64_t, NotifyRef> in_progress_notifies;

  // Could have watch_info_t here, but this file includes osd_types.h
  uint32_t timeout; ///< timeout in seconds
  uint64_t cookie;
  entity_addr_t addr;

  bool will_ping;    ///< is client new enough to ping the watch
  utime_t last_ping; ///< last client ping

  entity_name_t entity;
  bool discarded;

  Watch(
    PrimaryLogPG *pg, OSDService *osd,
    std::shared_ptr<ObjectContext> obc, uint32_t timeout,
    uint64_t cookie, entity_name_t entity,
    const entity_addr_t& addr);

  /// Registers the timeout callback with watch_timer
  void register_cb();

  /// send a Notify message when connected for notif
  void send_notify(NotifyRef notif);

  /// Cleans up state on discard or remove (including Connection state, obc)
  void discard_state();
public:
  /// Unregisters the timeout callback
  void unregister_cb();

  /// note receipt of a ping
  void got_ping(utime_t t);

  /// True if currently connected
  bool is_connected() const {
    return conn.get() != NULL;
  }
  bool is_connected(Connection *con) const {
    return conn.get() == con;
  }

  /// NOTE: must be called with pg lock held
  ~Watch();

  uint64_t get_watcher_gid() const {
    return entity.num();
  }

  std::ostream& gen_dbg_prefix(std::ostream& out);
  static WatchRef makeWatchRef(
    PrimaryLogPG *pg, OSDService *osd,
    std::shared_ptr<ObjectContext> obc, uint32_t timeout, uint64_t cookie, entity_name_t entity, const entity_addr_t &addr);
  void set_self(WatchRef _self) {
    self = _self;
  }

  /// Does not grant a ref count!
  boost::intrusive_ptr<PrimaryLogPG> get_pg() { return pg; }

  std::shared_ptr<ObjectContext> get_obc() { return obc; }

  uint64_t get_cookie() const { return cookie; }
  entity_name_t get_entity() const { return entity; }
  entity_addr_t get_peer_addr() const { return addr; }
  uint32_t get_timeout() const { return timeout; }

  /// Generates context for use if watch timeout is delayed by scrub or recovery
  Context *get_delayed_cb();

  /// Transitions Watch to connected, unregister_cb, resends pending Notifies
  void connect(
    ConnectionRef con, ///< [in] Reference to new connection
    bool will_ping     ///< [in] client is new and will send pings
    );

  /// Transitions watch to disconnected, register_cb
  void disconnect();

  /// Called if Watch state is discarded due to new peering interval
  void discard();

  /// True if removed or discarded
  bool is_discarded() const;

  /// Called on unwatch
  void remove(bool send_disconnect);

  /// Adds notif as in-progress notify
  void start_notify(
    NotifyRef notif ///< [in] Reference to new in-progress notify
    );

  /// Removes timed out notify
  void cancel_notify(
    NotifyRef notif ///< [in] notify which timed out
    );

  /// Call when notify_ack received on notify_id
  void notify_ack(
    uint64_t notify_id, ///< [in] id of acked notify
    ceph::buffer::list& reply_bl ///< [in] notify reply buffer
    );
};

/**
 * Holds weak refs to Watch structures corresponding to a connection
 * Lives in the Session object of an OSD connection
 */
class WatchConState {
  ceph::mutex lock = ceph::make_mutex("WatchConState");
  std::set<WatchRef> watches;
public:
  CephContext* cct;
  explicit WatchConState(CephContext* cct) : cct(cct) {}

  /// Add a watch
  void addWatch(
    WatchRef watch ///< [in] Ref to new watch object
    );

  /// Remove a watch
  void removeWatch(
    WatchRef watch ///< [in] Ref to watch object to remove
    );

  /// Called on session reset, disconnects watchers
  void reset(Connection *con);
};

#endif
