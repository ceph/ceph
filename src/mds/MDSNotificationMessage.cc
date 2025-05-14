#include "MDSNotificationMessage.h"
#include "common/Clock.h"
#include "common/ceph_json.h"

#define dout_subsys ceph_subsys_mds

MDSNotificationMessage::MDSNotificationMessage(uint64_t seq_id)
    : seq_id(seq_id) {}

void MDSNotificationMessage::create_message(int32_t whoami,
                                            const std::string &session_id,
                                            const uint64_t mask,
                                            const std::string &path) {
  JSONFormatter f;
  f.open_object_section("");
  ceph_clock_now().gmtime_nsec(f.dump_stream("timestamp"));
  f.dump_int("mds_id", (int64_t)whoami);
  f.dump_string("session_id", session_id);
  f.dump_unsigned("seq_id", seq_id);
  f.dump_unsigned("mask", mask);
  f.dump_string("path", path);
  f.close_section();
  f.flush(message);
}

void MDSNotificationMessage::create_move_message(int32_t whoami,
                                                 const std::string &session_id,
                                                 uint64_t mask,
                                                 const std::string &src_path,
                                                 const std::string &dest_path) {
  JSONFormatter f;
  f.open_object_section("");
  ceph_clock_now().gmtime_nsec(f.dump_stream("timestamp"));
  f.dump_int("mds_id", (int64_t)whoami);
  f.dump_string("session_id", session_id);
  f.dump_unsigned("seq_id", seq_id);
  f.dump_unsigned("mask", mask);
  f.dump_string("src_path", src_path);
  f.dump_string("dest_path", dest_path);
  f.close_section();
  f.flush(message);
}

void MDSNotificationMessage::create_link_message(int32_t whoami,
                                                 const std::string &session_id,
                                                 uint64_t target_mask,
                                                 uint64_t link_mask,
                                                 const std::string &target_path,
                                                 const std::string &link_path) {
  JSONFormatter f;
  f.open_object_section("");
  ceph_clock_now().gmtime_nsec(f.dump_stream("timestamp"));
  f.dump_int("mds_id", (int64_t)whoami);
  f.dump_string("session_id", session_id);
  f.dump_unsigned("seq_id", seq_id);
  f.dump_unsigned("target_mask", target_mask);
  f.dump_unsigned("link_mask", link_mask);
  f.dump_string("target_path", target_path);
  f.dump_string("link_path", link_path);
  f.close_section();
  f.flush(message);
}

void MDSNotificationMessage::create_snap_message(
    int32_t whoami, const std::string &session_id, uint64_t mask,
    const std::string &path, const std::string &snapshot_name) {
  JSONFormatter f;
  f.open_object_section("");
  ceph_clock_now().gmtime_nsec(f.dump_stream("timestamp"));
  f.dump_int("mds_id", (int64_t)whoami);
  f.dump_string("session_id", session_id);
  f.dump_unsigned("seq_id", seq_id);
  f.dump_unsigned("mask", mask);
  f.dump_string("path", path);
  f.dump_string("snapshot_name", snapshot_name);
  f.close_section();
  f.flush(message);
}