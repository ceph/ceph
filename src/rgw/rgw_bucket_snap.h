// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <map>
#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_time.h"

#include "rgw_bucket_snap_types.h"


class RGWBucketSnapMgr
{
  bool enabled = false;
  rgw_bucket_snap_id cur_snap{rgw_bucket_snap_id::SNAP_MIN};

  std::map<rgw_bucket_snap_id, rgw_bucket_snap> snaps;
  std::map<rgw_bucket_snap_id, rgw_bucket_snap> rm_snaps;

  std::map<std::string, rgw_bucket_snap_id> names_to_ids;

public:
  RGWBucketSnapMgr();

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(enabled, bl);
    encode(cur_snap, bl);
    encode(snaps, bl);
    encode(rm_snaps, bl);
    encode(names_to_ids, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(enabled, bl);
    decode(cur_snap, bl);
    decode(snaps, bl);
    decode(rm_snaps, bl);
    decode(names_to_ids, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;

  rgw_bucket_snap_id get_cur_snap_id() const {
    return cur_snap;
  }

  int create_snap(CephContext *cct,
                  const rgw_bucket_snap_info& info, rgw_bucket_snap_id *psnap_id);
  int remove_snap(rgw_bucket_snap_id snap_id);
  void cleanup_snap(rgw_bucket_snap_id snap_id);

  /* check if there is a live snapshot in the range
   * this is used to check if an object still needs to
   * exist, when min would be the snapshot where it was
   * created and max would be the one it was removed
   */
  bool live_snapshot_at_range(rgw_bucket_snap_id min, rgw_bucket_snap_id max) const;

  bool has_live_snapshots() const {
    return !snaps.empty();
  }

  bool find_snap(const std::string& snap_name, rgw_bucket_snap_id *snap_id) const {
    auto iter = names_to_ids.find(snap_name);
    if (iter == names_to_ids.end()) {
      return false;
    }

    *snap_id = iter->second;
    return true;
  }

  const std::map<rgw_bucket_snap_id, rgw_bucket_snap>& get_snaps() const {
    return snaps;
  }

  bool is_enabled() const {
    return enabled;
  }

  void set_enabled(bool flag) {
    enabled = flag;
  }

  std::map<rgw_bucket_snap_id, rgw_bucket_snap> get_removed_snaps() const {
    return rm_snaps;
  }
};
WRITE_CLASS_ENCODER(RGWBucketSnapMgr)
