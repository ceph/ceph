// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef DAEMON_METADATA_H_
#define DAEMON_METADATA_H_

// TODO: rename me to DaemonState from DaemonMetadata

#include <map>
#include <string>
#include <memory>
#include <set>

#include "common/Mutex.h"

#include "msg/msg_types.h"

// For PerfCounterType
#include "messages/MMgrReport.h"


// Unique reference to a daemon within a cluster
typedef std::pair<entity_type_t, std::string> DaemonKey;

// An instance of a performance counter type, within
// a particular daemon.
class PerfCounterInstance
{
  // TODO: store some short history or whatever
  uint64_t current;
  public:
  void push(uint64_t const &v) {current = v;}
};


typedef std::map<std::string, PerfCounterType> PerfCounterTypes;

// Performance counters for one daemon
class DaemonPerfCounters
{
  public:
  // The record of perf stat types, shared between daemons
  PerfCounterTypes &types;

  DaemonPerfCounters(PerfCounterTypes &types_)
    : types(types_)
  {}

  std::map<std::string, PerfCounterInstance> instances;

  // FIXME: this state is really local to DaemonServer, it's part
  // of the protocol rather than being part of what other classes
  // mgiht want to read.  Maybe have a separate session object
  // inside DaemonServer instead of stashing session-ish state here?
  std::set<std::string> declared_types;

  void update(MMgrReport *report);
};

// The state that we store about one daemon
class DaemonMetadata
{
  public:
  DaemonKey key;

  // The hostname where daemon was last seen running (extracted
  // from the metadata)
  std::string hostname;

  // The metadata (hostname, version, etc) sent from the daemon
  std::map<std::string, std::string> metadata;

  // The perf counters received in MMgrReport messages
  DaemonPerfCounters perf_counters;

  DaemonMetadata(PerfCounterTypes &types_)
    : perf_counters(types_)
  {
  }
};

typedef std::shared_ptr<DaemonMetadata> DaemonMetadataPtr;
typedef std::map<DaemonKey, DaemonMetadataPtr> DaemonMetadataCollection;




/**
 * Fuse the collection of per-daemon metadata from Ceph into
 * a view that can be queried by service type, ID or also
 * by server (aka fqdn).
 */
class DaemonMetadataIndex
{
  private:
  std::map<std::string, DaemonMetadataCollection> by_server;
  DaemonMetadataCollection all;

  std::set<DaemonKey> updating;

  mutable Mutex lock;

  public:

  DaemonMetadataIndex() : lock("DaemonState") {}

  // FIXME: shouldn't really be public, maybe construct DaemonMetadata
  // objects internally to avoid this.
  PerfCounterTypes types;

  void insert(DaemonMetadataPtr dm);
  void _erase(DaemonKey dmk);

  bool exists(const DaemonKey &key) const;
  DaemonMetadataPtr get(const DaemonKey &key);
  DaemonMetadataCollection get_by_server(const std::string &hostname) const;
  DaemonMetadataCollection get_by_type(uint8_t type) const;

  const DaemonMetadataCollection &get_all() const {return all;}
  const std::map<std::string, DaemonMetadataCollection> &get_all_servers() const
  {
    return by_server;
  }

  void notify_updating(const DaemonKey &k) { updating.insert(k); }
  void clear_updating(const DaemonKey &k) { updating.erase(k); }
  bool is_updating(const DaemonKey &k) { return updating.count(k) > 0; }

  /**
   * Remove state for all daemons of this type whose names are
   * not present in `names_exist`.  Use this function when you have
   * a cluster map and want to ensure that anything absent in the map
   * is also absent in this class.
   */
  void cull(entity_type_t daemon_type, std::set<std::string> names_exist);
};

#endif

