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
#include <set>

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
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(GroupImageLinkState &state, bufferlist::iterator& it)
{
  uint8_t int_state;
  using ceph::decode;
  decode(int_state, it);
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
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(MirrorImageStatusState &state, bufferlist::iterator& it)
{
  uint8_t int_state;
  using ceph::decode;
  decode(int_state, it);
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

struct ChildImageSpec {
  int64_t pool_id = -1;
  std::string image_id;

  ChildImageSpec() {}
  ChildImageSpec(int64_t pool_id, const std::string& image_id)
    : pool_id(pool_id), image_id(image_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ChildImageSpec*> &o);

  inline bool operator==(const ChildImageSpec& rhs) const {
    return (pool_id == rhs.pool_id &&
            image_id == rhs.image_id);
  }
  inline bool operator<(const ChildImageSpec& rhs) const {
    if (pool_id != rhs.pool_id) {
      return pool_id < rhs.pool_id;
    }
    return image_id < rhs.image_id;
  }
};
WRITE_CLASS_ENCODER(ChildImageSpec);

typedef std::set<ChildImageSpec> ChildImageSpecs;

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

  static void generate_test_instances(std::list<GroupImageSpec*> &o);

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

  static void generate_test_instances(std::list<GroupImageStatus*> &o);

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

  static void generate_test_instances(std::list<GroupSpec *> &o);
};

WRITE_CLASS_ENCODER(GroupSpec);

enum SnapshotNamespaceType {
  SNAPSHOT_NAMESPACE_TYPE_USER  = 0,
  SNAPSHOT_NAMESPACE_TYPE_GROUP = 1,
  SNAPSHOT_NAMESPACE_TYPE_TRASH = 2
};

struct UserSnapshotNamespace {
  static const SnapshotNamespaceType SNAPSHOT_NAMESPACE_TYPE =
    SNAPSHOT_NAMESPACE_TYPE_USER;

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

struct GroupSnapshotNamespace {
  static const SnapshotNamespaceType SNAPSHOT_NAMESPACE_TYPE =
    SNAPSHOT_NAMESPACE_TYPE_GROUP;

  GroupSnapshotNamespace() {}

  GroupSnapshotNamespace(int64_t _group_pool,
			 const string &_group_id,
			 const string &_group_snapshot_id)
    : group_id(_group_id), group_pool(_group_pool),
      group_snapshot_id(_group_snapshot_id) {}

  string group_id;
  int64_t group_pool = 0;
  string group_snapshot_id;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);

  void dump(Formatter *f) const;

  inline bool operator==(const GroupSnapshotNamespace& gsn) const {
    return group_pool == gsn.group_pool &&
	   group_id == gsn.group_id &&
	   group_snapshot_id == gsn.group_snapshot_id;
  }

  inline bool operator<(const GroupSnapshotNamespace& gsn) const {
    if (group_pool < gsn.group_pool) {
      return true;
    } else if (group_id < gsn.group_id) {
      return true;
    } else {
      return (group_snapshot_id < gsn.group_snapshot_id);
    }
    return false;
  }
};

struct TrashSnapshotNamespace {
  static const SnapshotNamespaceType SNAPSHOT_NAMESPACE_TYPE =
    SNAPSHOT_NAMESPACE_TYPE_TRASH;

  std::string original_name;
  SnapshotNamespaceType original_snapshot_namespace_type =
    SNAPSHOT_NAMESPACE_TYPE_USER;

  TrashSnapshotNamespace() {}
  TrashSnapshotNamespace(SnapshotNamespaceType original_snapshot_namespace_type,
                         const std::string& original_name)
    : original_name(original_name),
      original_snapshot_namespace_type(original_snapshot_namespace_type) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  inline bool operator==(const TrashSnapshotNamespace& usn) const {
    return true;
  }
  inline bool operator<(const TrashSnapshotNamespace& usn) const {
    return false;
  }
};

struct UnknownSnapshotNamespace {
  static const SnapshotNamespaceType SNAPSHOT_NAMESPACE_TYPE =
    static_cast<SnapshotNamespaceType>(-1);

  UnknownSnapshotNamespace() {}

  void encode(bufferlist& bl) const {}
  void decode(bufferlist::iterator& it) {}
  void dump(Formatter *f) const {}

  inline bool operator==(const UnknownSnapshotNamespace& gsn) const {
    return true;
  }

  inline bool operator<(const UnknownSnapshotNamespace& gsn) const {
    return false;
  }
};

std::ostream& operator<<(std::ostream& os, const SnapshotNamespaceType& type);
std::ostream& operator<<(std::ostream& os, const UserSnapshotNamespace& ns);
std::ostream& operator<<(std::ostream& os, const GroupSnapshotNamespace& ns);
std::ostream& operator<<(std::ostream& os, const TrashSnapshotNamespace& ns);
std::ostream& operator<<(std::ostream& os, const UnknownSnapshotNamespace& ns);

typedef boost::variant<UserSnapshotNamespace,
                       GroupSnapshotNamespace,
                       TrashSnapshotNamespace,
                       UnknownSnapshotNamespace> SnapshotNamespaceVariant;

struct SnapshotNamespace : public SnapshotNamespaceVariant {
  SnapshotNamespace() {
  }

  template <typename T>
  SnapshotNamespace(T&& t) : SnapshotNamespaceVariant(std::forward<T>(t)) {
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<SnapshotNamespace*> &o);

  inline bool operator==(const SnapshotNamespaceVariant& sn) const {
    return static_cast<const SnapshotNamespaceVariant&>(*this) == sn;
  }
  inline bool operator<(const SnapshotNamespaceVariant& sn) const {
    return static_cast<const SnapshotNamespaceVariant&>(*this) < sn;
  }
};
WRITE_CLASS_ENCODER(SnapshotNamespace);

SnapshotNamespaceType get_snap_namespace_type(
    const SnapshotNamespace& snapshot_namespace);

struct SnapshotInfo {
  snapid_t id = CEPH_NOSNAP;
  cls::rbd::SnapshotNamespace snapshot_namespace = {UserSnapshotNamespace{}};
  std::string name;
  uint64_t image_size = 0;
  utime_t timestamp;
  uint32_t child_count = 0;

  SnapshotInfo() {
  }
  SnapshotInfo(snapid_t id,
               const cls::rbd::SnapshotNamespace& snapshot_namespace,
               const std::string& name, uint64_t image_size,
               const utime_t& timestamp, uint32_t child_count)
    : id(id), snapshot_namespace(snapshot_namespace),
      name(name), image_size(image_size), timestamp(timestamp),
      child_count(child_count) {
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<SnapshotInfo*> &o);
};
WRITE_CLASS_ENCODER(SnapshotInfo);

enum GroupSnapshotState {
  GROUP_SNAPSHOT_STATE_INCOMPLETE = 0,
  GROUP_SNAPSHOT_STATE_COMPLETE = 1,
};

inline void encode(const GroupSnapshotState &state, bufferlist& bl, uint64_t features=0)
{
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(GroupSnapshotState &state, bufferlist::iterator& it)
{
  using ceph::decode;
  uint8_t int_state;
  decode(int_state, it);
  state = static_cast<GroupSnapshotState>(int_state);
}

struct ImageSnapshotSpec {
  int64_t pool;
  string image_id;
  snapid_t snap_id;

  ImageSnapshotSpec() {}
  ImageSnapshotSpec(int64_t _pool,
		    string _image_id,
		    snapid_t _snap_id) : pool(_pool),
					 image_id(_image_id),
					 snap_id(_snap_id) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);

  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ImageSnapshotSpec *> &o);
};
WRITE_CLASS_ENCODER(ImageSnapshotSpec);

struct GroupSnapshot {
  std::string id;
  std::string name;
  GroupSnapshotState state = GROUP_SNAPSHOT_STATE_INCOMPLETE;

  GroupSnapshot() {}
  GroupSnapshot(std::string _id,
		std::string _name,
		GroupSnapshotState _state) : id(_id),
					     name(_name),
					     state(_state) {}

  vector<ImageSnapshotSpec> snaps;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<GroupSnapshot *> &o);
};
WRITE_CLASS_ENCODER(GroupSnapshot);
enum TrashImageSource {
  TRASH_IMAGE_SOURCE_USER = 0,
  TRASH_IMAGE_SOURCE_MIRRORING = 1
};

inline void encode(const TrashImageSource &source, bufferlist& bl,
		   uint64_t features=0)
{
  using ceph::encode;
  encode(static_cast<uint8_t>(source), bl);
}

inline void decode(TrashImageSource &source, bufferlist::iterator& it)
{
  uint8_t int_source;
  using ceph::decode;
  decode(int_source, it);
  source = static_cast<TrashImageSource>(int_source);
}

struct TrashImageSpec {
  TrashImageSource source = TRASH_IMAGE_SOURCE_USER;
  std::string name;
  utime_t deletion_time; // time of deletion
  utime_t deferment_end_time;

  TrashImageSpec() {}
  TrashImageSpec(TrashImageSource source, const std::string &name,
                 const utime_t& deletion_time,
                 const utime_t& deferment_end_time)
    : source(source), name(name), deletion_time(deletion_time),
      deferment_end_time(deferment_end_time) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  inline bool operator==(const TrashImageSpec& rhs) const {
    return (source == rhs.source &&
            name == rhs.name &&
            deletion_time == rhs.deletion_time &&
            deferment_end_time == rhs.deferment_end_time);
  }
};
WRITE_CLASS_ENCODER(TrashImageSpec);

struct MirrorImageMap {
  MirrorImageMap() {
  }

  MirrorImageMap(const std::string &instance_id, utime_t mapped_time,
                 const bufferlist &data)
    : instance_id(instance_id),
      mapped_time(mapped_time),
      data(data) {
  }

  std::string instance_id;
  utime_t mapped_time;
  bufferlist data;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<MirrorImageMap*> &o);

  bool operator==(const MirrorImageMap &rhs) const;
  bool operator<(const MirrorImageMap &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorImageMap &image_map);

WRITE_CLASS_ENCODER(MirrorImageMap);

} // namespace rbd
} // namespace cls

#endif // CEPH_CLS_RBD_TYPES_H
