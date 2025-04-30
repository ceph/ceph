#include "rgw_bucket_snap.h"
#include "rgw_xml.h"
#include "rgw_rest.h"
#include "common/ceph_json.h"


void rgw_bucket_snap_info::dump(Formatter *f) const {
  encode_json("name", name, f);
  encode_json("description", description, f);
  encode_json("creation_time", creation_time, f);
  encode_json("flags", flags, f);
}

void rgw_bucket_snap_info::dump_xml(Formatter *f) const {
  encode_xml("Name", name, f);
  encode_xml("Description", description, f);
  encode_xml("CreationTime", dump_time_to_str(creation_time), f);
  encode_xml("Flags", flags, f);
}

void rgw_bucket_snap::dump(Formatter *f) const {
  encode_json("id", id, f);
  encode_json("info", info, f);
}

void rgw_bucket_snap::dump_xml(Formatter *f) const {
  encode_xml("ID", id, f);
  encode_xml("Info", info, f);
}


RGWBucketSnapMgr::RGWBucketSnapMgr() {}

void RGWBucketSnapMgr::dump(Formatter *f) const {
  encode_json("enabled", enabled, f);
  encode_json("cur_snap", cur_snap, f);
  encode_json("snaps", snaps, f);
  encode_json("rm_snaps", rm_snaps, f);
  encode_json("names_to_ids", names_to_ids, f);
}

void RGWBucketSnapMgr::dump_xml(Formatter *f) const {
  encode_xml("Enabled", enabled, f);
  encode_xml("CurrentSnapshot", cur_snap, f);
  {
    Formatter::ArraySection as(*f, "Snapshots");
    for (auto& i : snaps) {
      encode_xml("Snapshot", i.second, f);
    }
  }
  {
    Formatter::ArraySection as(*f, "RemovedSnapshots");
    for (auto& i : rm_snaps) {
      encode_xml("Snapshot", i.second, f);
    }
  }
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

int RGWBucketSnapMgr::remove_snap(rgw_bucket_snap_id snap_id)
{
  auto iter = snaps.find(snap_id);
  if (iter == snaps.end()) {
    return 0;
  }

  auto& snap = iter->second;

  snap.info.flags |= rgw_bucket_snap_info::Flags::MARKED_FOR_REMOVAL;
  rm_snaps[snap_id] = snap;

  snaps.erase(iter);

  return 0;
}

bool RGWBucketSnapMgr::live_snapshot_at_range(rgw_bucket_snap_id min, rgw_bucket_snap_id max) const
{
  if (min >= cur_snap) {
    return true;
  }
  auto iter = snaps.lower_bound(min);
  if (iter == snaps.end()) {
    return false;
  }

  return !max.is_set() || (iter->first < max);
}
