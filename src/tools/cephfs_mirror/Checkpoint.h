// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPHFS_MIRROR_CHECKPOINT_H
#define CEPHFS_MIRROR_CHECKPOINT_H

#include <string>
#include <map>

#include "include/utime.h"
#include "Types.h"

namespace cephfs {
namespace mirror {

// Checkpoint status enumeration
enum class CheckpointStatus : uint8_t {
  CREATED = 0,   // Checkpoint created but not yet synced
  COMPLETE = 1,  // Checkpoint successfully synced to remote
  FAILED = 2     // Checkpoint sync failed
};

// Convert checkpoint status to string
inline std::string checkpoint_status_to_string(CheckpointStatus status) {
  switch (status) {
    case CheckpointStatus::CREATED:
      return "created";
    case CheckpointStatus::COMPLETE:
      return "complete";
    case CheckpointStatus::FAILED:
      return "failed";
    default:
      return "unknown";
  }
}

// Checkpoint metadata keys stored in snapshot metadata
inline static const std::string CHECKPOINT_STATUS_KEY = "cephfs.mirror.checkpoint.status";
inline static const std::string CHECKPOINT_CREATED_AT_KEY = "cephfs.mirror.checkpoint.created_at";
inline static const std::string CHECKPOINT_UPDATED_AT_KEY = "cephfs.mirror.checkpoint.updated_at";
inline static const std::string CHECKPOINT_ERROR_MSG_KEY = "cephfs.mirror.checkpoint.error_msg";

// Snapshot metadata stores created_at/updated_at as UNIX epoch strings with
// 9 decimal places (Python get_checkpoint_epoch() or utime_to_epoch_string()).
std::string utime_to_epoch_string(const utime_t &t);
bool utime_from_epoch_string(const std::string &s, utime_t *t);

// Checkpoint information structure
struct CheckpointInfo {
  uint64_t snap_id;              // Snapshot ID (unique identifier)
  std::string snap_name;         // Snapshot name
  CheckpointStatus status;       // Current status of the checkpoint
  utime_t created_at;            // When the checkpoint was created (epoch in metadata)
  utime_t updated_at;            // Last status update time (epoch in metadata)
  std::string error_msg;         // Error message if status is FAILED

  CheckpointInfo();
  CheckpointInfo(uint64_t snap_id_, const std::string &snap_name_);

  // Convert to/from snapshot metadata map
  std::map<std::string, std::string> to_metadata() const;
  static CheckpointInfo from_metadata(uint64_t snap_id, const std::string &snap_name,
                                       const std::map<std::string, std::string> &metadata);
};


// Helper functions for checkpoint metadata operations

// Read snapshot metadata
int read_snap_metadata(MountRef mnt, const std::string &snap_path,
                       std::map<std::string, std::string> *metadata);

// Read checkpoint metadata from a snapshot
CheckpointInfo read_checkpoint_metadata(uint64_t snap_id,
                                        const std::string &snap_name,
                                        const std::map<std::string, std::string> &snap_metadata);

// Write checkpoint metadata to a snapshot
int write_checkpoint_metadata(CephContext *cct, MountRef mnt,
                              const std::string &dir_root,
                              const std::string &snap_name,
                              const std::map<std::string, std::string> &snap_metadata,
                              const CheckpointInfo &info);

// Check if checkpoint key exists in snapshot metadata
inline bool has_checkpoint(const std::map<std::string, std::string> &snap_metadata) {
  return snap_metadata.find(CHECKPOINT_STATUS_KEY) != snap_metadata.end();
}

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_CHECKPOINT_H
