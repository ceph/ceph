#ifndef RGW_META_SYNC_STATUS_H
#define RGW_META_SYNC_STATUS_H

#include <string>

#include "common/ceph_time.h"

struct rgw_meta_sync_info {
  enum SyncState {
    StateInit = 0,
    StateBuildingFullSyncMaps = 1,
    StateSync = 2,
  };

  uint16_t state;
  uint32_t num_shards;
  std::string period; //< period id of current metadata log
  epoch_t realm_epoch = 0; //< realm epoch of period

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(state, bl);
    ::encode(num_shards, bl);
    ::encode(period, bl);
    ::encode(realm_epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(state, bl);
    ::decode(num_shards, bl);
    if (struct_v >= 2) {
      ::decode(period, bl);
      ::decode(realm_epoch, bl);
    }
    DECODE_FINISH(bl);
  }

  void decode_json(JSONObj *obj);
  void dump(Formatter *f) const;

  rgw_meta_sync_info() : state((int)StateInit), num_shards(0) {}
};
WRITE_CLASS_ENCODER(rgw_meta_sync_info)

struct rgw_meta_sync_marker {
  enum SyncState {
    FullSync = 0,
    IncrementalSync = 1,
  };
  uint16_t state;
  string marker;
  string next_step_marker;
  uint64_t total_entries;
  uint64_t pos;
  real_time timestamp;
  epoch_t realm_epoch{0}; //< realm_epoch of period marker

  rgw_meta_sync_marker() : state(FullSync), total_entries(0), pos(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(state, bl);
    ::encode(marker, bl);
    ::encode(next_step_marker, bl);
    ::encode(total_entries, bl);
    ::encode(pos, bl);
    ::encode(timestamp, bl);
    ::encode(realm_epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(state, bl);
    ::decode(marker, bl);
    ::decode(next_step_marker, bl);
    ::decode(total_entries, bl);
    ::decode(pos, bl);
    ::decode(timestamp, bl);
    if (struct_v >= 2) {
      ::decode(realm_epoch, bl);
    }
    DECODE_FINISH(bl);
  }

  void decode_json(JSONObj *obj);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_meta_sync_marker)

struct rgw_meta_sync_status {
  rgw_meta_sync_info sync_info;
  map<uint32_t, rgw_meta_sync_marker> sync_markers;

  rgw_meta_sync_status() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(sync_info, bl);
    ::encode(sync_markers, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
    ::decode(sync_info, bl);
    ::decode(sync_markers, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(rgw_meta_sync_status)

#endif
