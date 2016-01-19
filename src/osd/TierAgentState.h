// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_TIERAGENT_H
#define CEPH_OSD_TIERAGENT_H

struct TierAgentState {
  /// current position iterating across pool
  hobject_t position;
  /// Count of agent_work since "start" position of object hash space
  int started;
  hobject_t start;
  bool delaying;

  /// histogram of ages we've encountered
  pow2_hist_t temp_hist;
  int hist_age;

  /// past HitSet(s) (not current)
  map<time_t,HitSetRef> hit_set_map;

  /// a few recent things we've seen that are clean
  list<hobject_t> recent_clean;

  enum flush_mode_t {
    FLUSH_MODE_IDLE,   // nothing to flush
    FLUSH_MODE_LOW, // flush dirty objects with a low speed
    FLUSH_MODE_HIGH, //flush dirty objects with a high speed
  } flush_mode;     ///< current flush behavior
  static const char *get_flush_mode_name(flush_mode_t m) {
    switch (m) {
    case FLUSH_MODE_IDLE: return "idle";
    case FLUSH_MODE_LOW: return "low";
    case FLUSH_MODE_HIGH: return "high";
    default: assert(0 == "bad flush mode");
    }
  }
  const char *get_flush_mode_name() const {
    return get_flush_mode_name(flush_mode);
  }

  enum evict_mode_t {
    EVICT_MODE_IDLE,      // no need to evict anything
    EVICT_MODE_SOME,      // evict some things as we are near the target
    EVICT_MODE_FULL,      // evict anything
  } evict_mode;     ///< current evict behavior
  static const char *get_evict_mode_name(evict_mode_t m) {
    switch (m) {
    case EVICT_MODE_IDLE: return "idle";
    case EVICT_MODE_SOME: return "some";
    case EVICT_MODE_FULL: return "full";
    default: assert(0 == "bad evict mode");
    }
  }
  const char *get_evict_mode_name() const {
    return get_evict_mode_name(evict_mode);
  }

  /// approximate ratio of objects (assuming they are uniformly
  /// distributed) that i should aim to evict.
  unsigned evict_effort;

  TierAgentState()
    : started(0),
      delaying(false),
      hist_age(0),
      flush_mode(FLUSH_MODE_IDLE),
      evict_mode(EVICT_MODE_IDLE),
      evict_effort(0)
  {}

  /// false if we have any work to do
  bool is_idle() const {
    return
      delaying ||
      (flush_mode == FLUSH_MODE_IDLE &&
      evict_mode == EVICT_MODE_IDLE);
  }

  /// add archived HitSet
  void add_hit_set(time_t start, HitSetRef hs) {
    hit_set_map.insert(make_pair(start, hs));
  }

  /// remove old/trimmed HitSet
  void remove_oldest_hit_set() {
    if (!hit_set_map.empty())
      hit_set_map.erase(hit_set_map.begin());
  }

  /// discard all open hit sets
  void discard_hit_sets() {
    hit_set_map.clear();
  }

  void dump(Formatter *f) const {
    f->dump_string("flush_mode", get_flush_mode_name());
    f->dump_string("evict_mode", get_evict_mode_name());
    f->dump_unsigned("evict_effort", evict_effort);
    f->dump_stream("position") << position;
    f->open_object_section("temp_hist");
    temp_hist.dump(f);
    f->close_section();
  }
};

#endif
