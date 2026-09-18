// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "Checkpoint.h"
#include "Utils.h"

#include "common/strtol.h"

#include <cstdio>
#include <vector>

namespace cephfs {
namespace mirror {

namespace {

const std::vector<std::string> CHECKPOINT_METADATA_KEY_LIST = {
  CHECKPOINT_STATUS_KEY,
  CHECKPOINT_CREATED_AT_KEY,
  CHECKPOINT_UPDATED_AT_KEY,
  CHECKPOINT_ERROR_MSG_KEY,
};

std::map<std::string, std::string> decode_snap_metadata(snap_metadata *md,
                                                        size_t nr_snap_metadata) {
  std::map<std::string, std::string> metadata;
  for (size_t i = 0; i < nr_snap_metadata; ++i) {
    metadata.emplace(md[i].key, md[i].value);
  }
  return metadata;
}

} // anonymous namespace

std::string utime_to_epoch_string(const utime_t &t) {
  double epoch = (double)t.sec() + (double)t.nsec() / 1000000000.0;
  char buf[32];
  snprintf(buf, sizeof(buf), "%.9f", epoch);
  return std::string(buf);
}

bool utime_from_epoch_string(const std::string &s, utime_t *t) {
  std::string err;
  double epoch = strict_strtod(s, &err);
  if (!err.empty()) {
    return false;
  }
  t->set_from_double(epoch);
  return true;
}

CheckpointInfo::CheckpointInfo()
  : snap_id(0),
    status(CheckpointStatus::CREATED) {
}

CheckpointInfo::CheckpointInfo(uint64_t snap_id_, const std::string &snap_name_)
  : snap_id(snap_id_),
    snap_name(snap_name_),
    status(CheckpointStatus::CREATED) {
}

std::map<std::string, std::string> CheckpointInfo::to_metadata() const {
  std::map<std::string, std::string> metadata;
  metadata[CHECKPOINT_STATUS_KEY] = std::to_string(static_cast<uint8_t>(status));
  metadata[CHECKPOINT_CREATED_AT_KEY] = utime_to_epoch_string(created_at);
  metadata[CHECKPOINT_UPDATED_AT_KEY] = utime_to_epoch_string(updated_at);
  if (!error_msg.empty()) {
    metadata[CHECKPOINT_ERROR_MSG_KEY] = error_msg;
  }
  return metadata;
}

CheckpointInfo CheckpointInfo::from_metadata(uint64_t snap_id, const std::string &snap_name,
                                              const std::map<std::string, std::string> &metadata) {
  CheckpointInfo info(snap_id, snap_name);

  auto it = metadata.find(CHECKPOINT_STATUS_KEY);
  if (it != metadata.end()) {
    info.status = static_cast<CheckpointStatus>(std::stoul(it->second));
  }

  it = metadata.find(CHECKPOINT_CREATED_AT_KEY);
  if (it != metadata.end()) {
    utime_from_epoch_string(it->second, &info.created_at);
  }

  it = metadata.find(CHECKPOINT_UPDATED_AT_KEY);
  if (it != metadata.end()) {
    utime_from_epoch_string(it->second, &info.updated_at);
  }

  it = metadata.find(CHECKPOINT_ERROR_MSG_KEY);
  if (it != metadata.end()) {
    info.error_msg = it->second;
  }

  return info;
}

int read_snap_metadata(MountRef mnt, const std::string &snap_path,
                       std::map<std::string, std::string> *metadata) {
  snap_info info;
  int r = ceph_get_snap_info(mnt, snap_path.c_str(), &info);
  if (r < 0) {
    return r;
  }

  metadata->clear();
  if (info.nr_snap_metadata) {
    *metadata = decode_snap_metadata(info.snap_metadata, info.nr_snap_metadata);
    ceph_free_snap_info_buffer(&info);
  }
  return 0;
}

CheckpointInfo read_checkpoint_metadata(uint64_t snap_id,
                                        const std::string &snap_name,
                                        const std::map<std::string, std::string> &snap_metadata) {
  return CheckpointInfo::from_metadata(snap_id, snap_name, snap_metadata);
}

int write_checkpoint_metadata(CephContext *cct, MountRef mnt,
                               const std::string &dir_root,
                               const std::string &snap_name,
                               const std::map<std::string, std::string> &snap_metadata,
                               const CheckpointInfo &info) {
  auto snap_path = snapshot_path(cct, dir_root, snap_name);
  auto checkpoint_metadata = info.to_metadata();

  // Write/update checkpoint metadata keys
  for (const auto &[key, val] : checkpoint_metadata) {
    // created_at is set by the mgr when the checkpoint is created; mirror
    // daemon updates must not rewrite it.
    if (key == CHECKPOINT_CREATED_AT_KEY && snap_metadata.count(key)) {
      continue;
    }

    auto it = snap_metadata.find(key);
    if (it != snap_metadata.end() && it->second == val) {
      continue;
    }

    // For updates: use CREATE (allows both create and update)
    // For new creates: use CREATE | EXCL (create only, reject update)
    unsigned int op_flag = it != snap_metadata.end() ?
      CEPH_SNAP_MD_OP_CREATE : (CEPH_SNAP_MD_OP_CREATE | CEPH_SNAP_MD_OP_EXCL);
    int r = ceph_do_snap_md_op(mnt, snap_path.c_str(), key.c_str(), val.c_str(),
                               op_flag);
    if (r < 0) {
      return r;
    }
  }

  // Remove checkpoint metadata keys that are no longer present
  for (const auto &key : CHECKPOINT_METADATA_KEY_LIST) {
    if (snap_metadata.count(key) && !checkpoint_metadata.count(key)) {
      int r = ceph_do_snap_md_op(mnt, snap_path.c_str(), key.c_str(), "",
                                 CEPH_SNAP_MD_OP_REMOVE);
      if (r < 0) {
        return r;
      }
    }
  }

  return 0;
}

int remove_checkpoint_metadata(MountRef mnt, const std::string &snap_path,
                               const std::map<std::string, std::string> &snap_metadata) {
  for (const auto &key : CHECKPOINT_METADATA_KEY_LIST) {
    if (!snap_metadata.count(key)) {
      continue;
    }
    int r = ceph_do_snap_md_op(mnt, snap_path.c_str(), key.c_str(), "",
                               CEPH_SNAP_MD_OP_REMOVE);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

} // namespace mirror
} // namespace cephfs
