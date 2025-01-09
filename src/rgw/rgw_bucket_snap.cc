#include "rgw_bucket_snap.h"
#include "common/ceph_json.h"


void rgw_bucket_snap_info::dump(Formatter *f) const {
  encode_json("name", name, f);
  encode_json("description", description, f);
  encode_json("creation_time", creation_time, f);
}

void rgw_bucket_snap::dump(Formatter *f) const {
  encode_json("id", id, f);
  encode_json("info", info, f);
}


RGWBucketSnapMgr::RGWBucketSnapMgr() {}

void RGWBucketSnapMgr::dump(Formatter *f) const {
  encode_json("enabled", enabled, f);
  encode_json("cur_snap", cur_snap, f);
  encode_json("snaps", snaps, f);
  encode_json("names_to_ids", names_to_ids, f);
}

int RGWBucketSnapMgr::create_snap(const rgw_bucket_snap_info& info)
{
  auto iter = names_to_ids.find(info.name);
  if (iter != names_to_ids.end()) {
    return -EEXIST;
  }

  rgw_bucket_snap snap;
  snap.id = cur_snap++;
  snap.info = info;

  snaps[snap.id] = snap;
  names_to_ids[info.name] = snap.id;

  return 0;
}
