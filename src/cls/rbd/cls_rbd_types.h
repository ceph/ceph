// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_RBD_TYPES_H
#define CEPH_CLS_RBD_TYPES_H

#include <boost/variant.hpp>
#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "include/utime.h"
#include <iosfwd>
#include <string>

#define RBD_GROUP_REF "rbd_group_ref"

namespace ceph { class Formatter; }

namespace cls {
namespace rbd {

static const uint32_t MAX_OBJECT_MAP_OBJECT_COUNT = 256000000;
static const string RBD_GROUP_IMAGE_KEY_PREFIX = "image_";

enum MirrorMode {
  MIRROR_MODE_DISABLED = 0,
  MIRROR_MODE_IMAGE    = 1,
  MIRROR_MODE_POOL     = 2
};

enum GroupImageLinkState {
  GROUP_IMAGE_LINK_STATE_ATTACHED,
  GROUP_IMAGE_LINK_STATE_INCOMPLETE
};

inline void encode(const GroupImageLinkState &state, bufferlist& bl,
		   uint64_t features=0)
{
  ::encode(static_cast<uint8_t>(state), bl);
}

inline void decode(GroupImageLinkState &state, bufferlist::iterator& it)
{
  uint8_t int_state;
  ::decode(int_state, it);
  state = static_cast<GroupImageLinkState>(int_state);
}

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
  MIRROR_IMAGE_STATE_ENABLED   = 1,
  MIRROR_IMAGE_STATE_DISABLED  = 2,
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

struct GroupImageSpec {
  GroupImageSpec() {}

  GroupImageSpec(const std::string &image_id, int64_t pool_id)
    : image_id(image_id), pool_id(pool_id) {}

  static int from_key(const std::string &image_key, GroupImageSpec *spec);

  std::string image_id;
  int64_t pool_id = -1;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  std::string image_key();

};

WRITE_CLASS_ENCODER(GroupImageSpec);

struct GroupImageStatus {
  GroupImageStatus() {}
  GroupImageStatus(const std::string &image_id,
		   int64_t pool_id,
		   GroupImageLinkState state)
    : spec(image_id, pool_id), state(state) {}

  GroupImageStatus(GroupImageSpec spec,
		   GroupImageLinkState state)
    : spec(spec), state(state) {}

  GroupImageSpec spec;
  GroupImageLinkState state = GROUP_IMAGE_LINK_STATE_INCOMPLETE;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  std::string state_to_string() const;
};

WRITE_CLASS_ENCODER(GroupImageStatus);

struct GroupSpec {
  GroupSpec() {}
  GroupSpec(const std::string &group_id, int64_t pool_id)
    : group_id(group_id), pool_id(pool_id) {}

  std::string group_id;
  int64_t pool_id = -1;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;
  bool is_valid() const;
};

WRITE_CLASS_ENCODER(GroupSpec);

enum SnapshotNamespaceType {
  SNAPSHOT_NAMESPACE_TYPE_USER = 0,
  SNAPSHOT_NAMESPACE_TYPE_GROUP = 1
};

struct UserSnapshotNamespace {
  static const uint32_t SNAPSHOT_NAMESPACE_TYPE = SNAPSHOT_NAMESPACE_TYPE_USER;

  UserSnapshotNamespace() {}

  void encode(bufferlist& bl) const {}
  void decode(bufferlist::iterator& it) {}

  void dump(Formatter *f) const {}

  inline bool operator==(const UserSnapshotNamespace& usn) const {
    return true;
  }

  inline bool operator<(const UserSnapshotNamespace& usn) const {
    return false;
  }

};

std::ostream& operator<<(std::ostream& os, const UserSnapshotNamespace& ns);

struct GroupSnapshotNamespace {
  static const uint32_t SNAPSHOT_NAMESPACE_TYPE = SNAPSHOT_NAMESPACE_TYPE_GROUP;

  GroupSnapshotNamespace() {}

  GroupSnapshotNamespace(int64_t _group_pool,
			 const string &_group_id,
			 const snapid_t &_snapshot_id) :group_pool(_group_pool),
							group_id(_group_id),
							snapshot_id(_snapshot_id) {}

  int64_t group_pool;
  string group_id;
  snapid_t snapshot_id;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);

  void dump(Formatter *f) const;

  inline bool operator==(const GroupSnapshotNamespace& gsn) const {
    return group_pool == gsn.group_pool &&
	   group_id == gsn.group_id &&
	   snapshot_id == gsn.snapshot_id;
  }

  inline bool operator<(const GroupSnapshotNamespace& gsn) const {
    if (group_pool < gsn.group_pool) {
      return true;
    } else if (group_id < gsn.group_id) {
      return true;
    } else {
      return snapshot_id < gsn.snapshot_id;
    }
  }

};

std::ostream& operator<<(std::ostream& os, const GroupSnapshotNamespace& ns);

struct UnknownSnapshotNamespace {
  static const uint32_t SNAPSHOT_NAMESPACE_TYPE = static_cast<uint32_t>(-1);

  UnknownSnapshotNamespace() {}

  void encode(bufferlist& bl) const {}
  void decode(bufferlist::iterator& it) {}
  void dump(Formatter *f) const {}
  inline bool operator==(const UnknownSnapshotNamespace& gsn) const {
    return true;
  }

  inline bool operator<(const UnknownSnapshotNamespace& usn) const {
    return false;
  }
};

std::ostream& operator<<(std::ostream& os, const UnknownSnapshotNamespace& ns);

typedef boost::variant<UserSnapshotNamespace, GroupSnapshotNamespace, UnknownSnapshotNamespace> SnapshotNamespace;


struct SnapshotNamespaceOnDisk {

  SnapshotNamespaceOnDisk() : snapshot_namespace(UnknownSnapshotNamespace()) {}
  SnapshotNamespaceOnDisk(const SnapshotNamespace &sn) : snapshot_namespace(sn) {}

  SnapshotNamespace snapshot_namespace;

  SnapshotNamespaceType get_namespace_type() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<SnapshotNamespaceOnDisk *> &o);

  inline bool operator==(const SnapshotNamespaceOnDisk& gsn) const {
    return snapshot_namespace == gsn.snapshot_namespace;
  }
};
WRITE_CLASS_ENCODER(SnapshotNamespaceOnDisk);

enum TrashImageSource {
  TRASH_IMAGE_SOURCE_USER = 0,
  TRASH_IMAGE_SOURCE_MIRRORING = 1
};

inline void encode(const TrashImageSource &source, bufferlist& bl,
		   uint64_t features=0)
{
  ::encode(static_cast<uint8_t>(source), bl);
}

inline void decode(TrashImageSource &source, bufferlist::iterator& it)
{
  uint8_t int_source;
  ::decode(int_source, it);
  source = static_cast<TrashImageSource>(int_source);
}

struct TrashImageSpec {
  TrashImageSource source = TRASH_IMAGE_SOURCE_USER;
  std::string name;
  utime_t deletion_time; // time of deletion
  utime_t deferment_end_time;

  TrashImageSpec() {}
  TrashImageSpec(TrashImageSource source, const std::string &name,
                   utime_t deletion_time, utime_t deferment_end_time) :
    source(source), name(name), deletion_time(deletion_time),
    deferment_end_time(deferment_end_time) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(TrashImageSpec);

} // namespace rbd
} // namespace cls

using cls::rbd::encode;
using cls::rbd::decode;

#endif // CEPH_CLS_RBD_TYPES_H
