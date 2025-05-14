#pragma once
#include "common/ceph_context.h"
#include "include/Context.h"
#include "include/buffer.h"
#include <bits/stdc++.h>

struct MDSNotificationMessage {
  bufferlist message;
  uint64_t seq_id;
  MDSNotificationMessage(uint64_t seq_id);
  void create_message(int32_t whoami, const std::string &session_id,
                      uint64_t mask, const std::string &path);
  void create_move_message(int32_t whoami, const std::string &session_id,
                           uint64_t mask, const std::string &src_path,
                           const std::string &dest_path);
  void create_link_message(int32_t whoami, const std::string &session_id,
                           uint64_t target_mask, uint64_t link_mask,
                           const std::string &target_path,
                           const std::string &link_path);
  void create_snap_message(int32_t whoami, const std::string &session_id,
                           uint64_t mask, const std::string &path,
                           const std::string &snapshot_name);
};