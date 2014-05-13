/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * Copyright 2013 Inktank
 */

#ifndef CLS_REPLICA_LOG_TYPES_H_
#define CLS_REPLICA_LOG_TYPES_H_

#include "include/utime.h"
#include "include/encoding.h"
#include "include/types.h"
#include <errno.h>

class JSONObj;

struct cls_replica_log_item_marker {
  string item_name; // the name of the item we're marking
  utime_t item_timestamp; // the time stamp at which the item was outdated

  cls_replica_log_item_marker() {}
  cls_replica_log_item_marker(const string& name, const utime_t& time) :
    item_name(name), item_timestamp(time) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(item_name, bl);
    ::encode(item_timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(item_name, bl);
    ::decode(item_timestamp, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<cls_replica_log_item_marker*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_item_marker)

struct cls_replica_log_progress_marker {
  string entity_id; // the name of the entity setting the progress marker
  string position_marker; // represents a log listing position on the master
  utime_t position_time; // the timestamp associated with the position marker
  std::list<cls_replica_log_item_marker> items; /* any items not caught up
						   to the position marker*/

  cls_replica_log_progress_marker() {}
  cls_replica_log_progress_marker(const string& entity, const string& marker,
                                  const utime_t& time ) :
                                    entity_id(entity), position_marker(marker),
                                    position_time(time) {}
  cls_replica_log_progress_marker(const string& entity, const string& marker,
                                  const utime_t& time,
                                  const std::list<cls_replica_log_item_marker>& b) :
                                    entity_id(entity), position_marker(marker),
                                    position_time(time),
                                    items(b) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entity_id, bl);
    ::encode(position_marker, bl);
    ::encode(position_time, bl);
    ::encode(items, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entity_id, bl);
    ::decode(position_marker, bl);
    ::decode(position_time, bl);
    ::decode(items, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<cls_replica_log_progress_marker*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_progress_marker)

class cls_replica_log_bound {
  /**
   * Right now, we are lazy and only support a single marker at a time. In the
   * future, we might support more than one, so the interface is designed to
   * let that work.
   */
  string position_marker; // represents a log listing position on the master
  utime_t position_time; // the timestamp associated with the position marker
  bool marker_exists; // has the marker been set?
  cls_replica_log_progress_marker marker; // the status of the current locker

public:
  cls_replica_log_bound() : marker_exists(false) {}

  int update_marker(const cls_replica_log_progress_marker& new_mark) {
    // only one marker at a time right now
    if (marker_exists && (marker.entity_id != new_mark.entity_id)) {
      return -EEXIST;
    }
    // can't go backwards with our one marker!
    if (marker_exists && (marker.position_time > new_mark.position_time)) {
      return -EINVAL;
    }

    marker = new_mark;
    position_marker = new_mark.position_marker;
    position_time = new_mark.position_time;
    marker_exists = true;
    // hey look, updating is idempotent; did you notice that?
    return 0;
  }

  int delete_marker(const string& entity_id) {
    if (marker_exists) {
      // ENOENT if our marker doesn't match the passed ID
      if (marker.entity_id != entity_id) {
	return -ENOENT;
      }
      // you can't delete it if there are unclean entries
      if (!marker.items.empty()) {
	return -ENOTEMPTY;
      }
    }

    marker_exists = false;
    marker = cls_replica_log_progress_marker();
    // hey look, deletion is idempotent! Hurray.
    return 0;
  }

  std::string get_lowest_marker_bound() {
    return position_marker;
  }

  utime_t get_lowest_time_bound() {
    return position_time;
  }

  utime_t get_oldest_time() {
    utime_t oldest = position_time;
    list<cls_replica_log_item_marker>::const_iterator i;
    for ( i = marker.items.begin(); i != marker.items.end(); ++i) {
      if (i->item_timestamp < oldest)
	oldest = i->item_timestamp;
    }
    return oldest;
  }

  void get_markers(list<cls_replica_log_progress_marker>& ls) {
    if (marker_exists) {
      ls.push_back(marker);
    }
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(position_marker, bl);
    ::encode(position_time, bl);
    ::encode(marker_exists, bl);
    if (marker_exists) {
      ::encode(marker, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(position_marker, bl);
    ::decode(position_time, bl);
    ::decode(marker_exists, bl);
    if (marker_exists) {
      ::decode(marker, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<cls_replica_log_bound*>& ls);
};
WRITE_CLASS_ENCODER(cls_replica_log_bound)

#endif /* CLS_REPLICA_LOG_TYPES_H_ */
