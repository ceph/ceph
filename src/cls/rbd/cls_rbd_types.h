// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_RBD_TYPES_H
#define CEPH_CLS_RBD_TYPES_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/utime.h"
#include <iosfwd>
#include <string>

namespace ceph { class Formatter; }

namespace cls {
namespace rbd {

static const uint32_t MAX_OBJECT_MAP_OBJECT_COUNT = 256000000;

enum MirrorMode {
  MIRROR_MODE_DISABLED = 0,
  MIRROR_MODE_IMAGE    = 1,
  MIRROR_MODE_POOL     = 2
};

struct MirrorPeer {
  MirrorPeer() {
  }
  MirrorPeer(const std::string &uuid, const std::string &cluster_name,
             const std::string &client_name, int64_t pool_id)
    : uuid(uuid), cluster_name(cluster_name), client_name(client_name),
      pool_id(pool_id) {
  }

  std::string uuid;
  std::string cluster_name;
  std::string client_name;
  int64_t pool_id = -1;

  inline bool is_valid() const {
    return (!uuid.empty() && !cluster_name.empty() && !client_name.empty());
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<MirrorPeer*> &o);

  bool operator==(const MirrorPeer &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorMode& mirror_mode);
std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer);

WRITE_CLASS_ENCODER(MirrorPeer);

enum MirrorImageState {
  MIRROR_IMAGE_STATE_DISABLING = 0,
  MIRROR_IMAGE_STATE_ENABLED   = 1
};

struct MirrorImage {
  MirrorImage() {}
  MirrorImage(const std::string &global_image_id, MirrorImageState state)
    : global_image_id(global_image_id), state(state) {}

  std::string global_image_id;
  MirrorImageState state = MIRROR_IMAGE_STATE_DISABLING;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<MirrorImage*> &o);

  bool operator==(const MirrorImage &rhs) const;
  bool operator<(const MirrorImage &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorImageState& mirror_state);
std::ostream& operator<<(std::ostream& os, const MirrorImage& mirror_image);

WRITE_CLASS_ENCODER(MirrorImage);

enum MirrorImageStatusState {
  MIRROR_IMAGE_STATUS_STATE_UNKNOWN         = 0,
  MIRROR_IMAGE_STATUS_STATE_ERROR           = 1,
  MIRROR_IMAGE_STATUS_STATE_SYNCING         = 2,
  MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY = 3,
  MIRROR_IMAGE_STATUS_STATE_REPLAYING       = 4,
  MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY = 5,
  MIRROR_IMAGE_STATUS_STATE_STOPPED         = 6,
};

inline void encode(const MirrorImageStatusState &state, bufferlist& bl,
		   uint64_t features=0)
{
  ::encode(static_cast<uint8_t>(state), bl);
}

inline void decode(MirrorImageStatusState &state, bufferlist::iterator& it)
{
  uint8_t int_state;
  ::decode(int_state, it);
  state = static_cast<MirrorImageStatusState>(int_state);
}

struct MirrorImageStatus {
  MirrorImageStatus() {}
  MirrorImageStatus(MirrorImageStatusState state,
		    const std::string &description = "")
    : state(state), description(description) {}

  MirrorImageStatusState state = MIRROR_IMAGE_STATUS_STATE_UNKNOWN;
  std::string description;
  utime_t last_update;
  bool up = false;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  std::string state_to_string() const;

  static void generate_test_instances(std::list<MirrorImageStatus*> &o);

  bool operator==(const MirrorImageStatus &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorImageStatus& status);
std::ostream& operator<<(std::ostream& os, const MirrorImageStatusState& state);

WRITE_CLASS_ENCODER(MirrorImageStatus);

} // namespace rbd
} // namespace cls

using cls::rbd::encode;
using cls::rbd::decode;

#endif // CEPH_CLS_RBD_TYPES_H
