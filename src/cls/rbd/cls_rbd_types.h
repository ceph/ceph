// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_RBD_TYPES_H
#define CEPH_CLS_RBD_TYPES_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "include/utime.h"
#include "msg/msg_types.h"
#include <iosfwd>
#include <string>
#include <set>
#include <variant>

#define RBD_GROUP_REF "rbd_group_ref"

namespace ceph { class Formatter; }

namespace cls {
namespace rbd {

static const uint32_t MAX_OBJECT_MAP_OBJECT_COUNT = 256000000;
static const std::string RBD_GROUP_IMAGE_KEY_PREFIX = "image_";

enum DirectoryState {
  DIRECTORY_STATE_READY         = 0,
  DIRECTORY_STATE_ADD_DISABLED  = 1
};

inline void encode(DirectoryState state, ceph::buffer::list& bl,
		   uint64_t features=0)
{
  ceph::encode(static_cast<uint8_t>(state), bl);
}

inline void decode(DirectoryState &state, ceph::buffer::list::const_iterator& it)
{
  uint8_t int_state;
  ceph::decode(int_state, it);
  state = static_cast<DirectoryState>(int_state);
}

enum MirrorMode {
  MIRROR_MODE_DISABLED = 0,
  MIRROR_MODE_IMAGE    = 1,
  MIRROR_MODE_POOL     = 2
};

enum GroupImageLinkState {
  GROUP_IMAGE_LINK_STATE_ATTACHED,
  GROUP_IMAGE_LINK_STATE_INCOMPLETE
};

inline void encode(const GroupImageLinkState &state, ceph::buffer::list& bl,
		   uint64_t features=0)
{
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(GroupImageLinkState &state, ceph::buffer::list::const_iterator& it)
{
  uint8_t int_state;
  using ceph::decode;
  decode(int_state, it);
  state = static_cast<GroupImageLinkState>(int_state);
}

enum MirrorPeerDirection {
  MIRROR_PEER_DIRECTION_RX    = 0,
  MIRROR_PEER_DIRECTION_TX    = 1,
  MIRROR_PEER_DIRECTION_RX_TX = 2
};

std::ostream& operator<<(std::ostream& os,
                         MirrorPeerDirection mirror_peer_direction);

struct MirrorPeer {
  MirrorPeer() {
  }
  MirrorPeer(const std::string &uuid,
             MirrorPeerDirection mirror_peer_direction,
             const std::string& site_name,
             const std::string& client_name,
             const std::string& mirror_uuid)
    : uuid(uuid), mirror_peer_direction(mirror_peer_direction),
      site_name(site_name), client_name(client_name),
      mirror_uuid(mirror_uuid) {
  }

  std::string uuid;

  MirrorPeerDirection mirror_peer_direction = MIRROR_PEER_DIRECTION_RX_TX;
  std::string site_name;
  std::string client_name;  // RX property
  std::string mirror_uuid;
  utime_t last_seen;

  inline bool is_valid() const {
    switch (mirror_peer_direction) {
    case MIRROR_PEER_DIRECTION_TX:
      break;
    case MIRROR_PEER_DIRECTION_RX:
    case MIRROR_PEER_DIRECTION_RX_TX:
      if (client_name.empty()) {
        return false;
      }
      break;
    default:
      return false;
    }
    return (!uuid.empty() && !site_name.empty());
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<MirrorPeer*> &o);

  bool operator==(const MirrorPeer &rhs) const;
  bool operator!=(const MirrorPeer &rhs) const {
    return (!(*this == rhs));
  }
};

std::ostream& operator<<(std::ostream& os, const MirrorMode& mirror_mode);
std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer);

WRITE_CLASS_ENCODER(MirrorPeer);

enum MirrorImageMode {
  MIRROR_IMAGE_MODE_JOURNAL  = 0,
  MIRROR_IMAGE_MODE_SNAPSHOT = 1,
};

enum MirrorImageState {
  MIRROR_IMAGE_STATE_DISABLING = 0,
  MIRROR_IMAGE_STATE_ENABLED   = 1,
  MIRROR_IMAGE_STATE_DISABLED  = 2,
  MIRROR_IMAGE_STATE_CREATING  = 3,
};

struct MirrorImage {
  MirrorImage() {
  }
  MirrorImage(MirrorImageMode mode, const std::string &global_image_id,
              MirrorImageState state)
    : mode(mode), global_image_id(global_image_id), state(state) {
  }

  MirrorImageMode mode = MIRROR_IMAGE_MODE_JOURNAL;
  std::string global_image_id;
  MirrorImageState state = MIRROR_IMAGE_STATE_DISABLING;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<MirrorImage*> &o);

  bool operator==(const MirrorImage &rhs) const;
  bool operator<(const MirrorImage &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorImageMode& mirror_mode);
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

inline void encode(const MirrorImageStatusState &state, ceph::buffer::list& bl,
		   uint64_t features=0)
{
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(MirrorImageStatusState &state,
                   ceph::buffer::list::const_iterator& it)
{
  uint8_t int_state;
  using ceph::decode;
  decode(int_state, it);
  state = static_cast<MirrorImageStatusState>(int_state);
}

std::ostream& operator<<(std::ostream& os, const MirrorImageStatusState& state);

struct MirrorImageSiteStatus {
  static const std::string LOCAL_MIRROR_UUID;

  MirrorImageSiteStatus() {}
  MirrorImageSiteStatus(const std::string& mirror_uuid,
                        MirrorImageStatusState state,
                        const std::string &description)
    : mirror_uuid(mirror_uuid), state(state), description(description) {
  }

  std::string mirror_uuid = LOCAL_MIRROR_UUID;
  MirrorImageStatusState state = MIRROR_IMAGE_STATUS_STATE_UNKNOWN;
  std::string description;
  utime_t last_update;
  bool up = false;

  void encode_meta(uint8_t version, ceph::buffer::list &bl) const;
  void decode_meta(uint8_t version, ceph::buffer::list::const_iterator &it);

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  std::string state_to_string() const;

  bool operator==(const MirrorImageSiteStatus &rhs) const;

  static void generate_test_instances(std::list<MirrorImageSiteStatus*> &o);
};
WRITE_CLASS_ENCODER(MirrorImageSiteStatus);

std::ostream& operator<<(std::ostream& os, const MirrorImageSiteStatus& status);

struct MirrorImageSiteStatusOnDisk : cls::rbd::MirrorImageSiteStatus {
  entity_inst_t origin;

  MirrorImageSiteStatusOnDisk() {
  }
  MirrorImageSiteStatusOnDisk(const cls::rbd::MirrorImageSiteStatus &status) :
    cls::rbd::MirrorImageSiteStatus(status) {
  }

  void encode_meta(ceph::buffer::list &bl, uint64_t features) const;
  void decode_meta(ceph::buffer::list::const_iterator &it);

  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &it);

  static void generate_test_instances(
      std::list<MirrorImageSiteStatusOnDisk*> &o);
};
WRITE_CLASS_ENCODER_FEATURES(MirrorImageSiteStatusOnDisk)

struct MirrorImageStatus {
  typedef std::list<MirrorImageSiteStatus> MirrorImageSiteStatuses;

  MirrorImageStatus() {}
  MirrorImageStatus(const MirrorImageSiteStatuses& statuses)
    : mirror_image_site_statuses(statuses) {
  }

  MirrorImageSiteStatuses mirror_image_site_statuses;

  int get_local_mirror_image_site_status(MirrorImageSiteStatus* status) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  bool operator==(const MirrorImageStatus& rhs) const;

  static void generate_test_instances(std::list<MirrorImageStatus*> &o);
};
WRITE_CLASS_ENCODER(MirrorImageStatus);

std::ostream& operator<<(std::ostream& os, const MirrorImageStatus& status);

struct ParentImageSpec {
  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_id;
  snapid_t snap_id = CEPH_NOSNAP;

  ParentImageSpec() {
  }
  ParentImageSpec(int64_t pool_id, const std::string& pool_namespace,
                  const std::string& image_id, snapid_t snap_id)
    : pool_id(pool_id), pool_namespace(pool_namespace), image_id(image_id),
      snap_id(snap_id) {
  }

  bool exists() const {
    return (pool_id >= 0 && !image_id.empty() && snap_id != CEPH_NOSNAP);
  }

  bool operator==(const ParentImageSpec& rhs) const {
    return ((pool_id == rhs.pool_id) &&
            (pool_namespace == rhs.pool_namespace) &&
            (image_id == rhs.image_id) &&
            (snap_id == rhs.snap_id));
  }

  bool operator!=(const ParentImageSpec& rhs) const {
    return !(*this == rhs);
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<ParentImageSpec*> &o);
};

WRITE_CLASS_ENCODER(ParentImageSpec);

std::ostream& operator<<(std::ostream& os, const ParentImageSpec& rhs);

struct ChildImageSpec {
  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_id;

  ChildImageSpec() {}
  ChildImageSpec(int64_t pool_id, const std::string& pool_namespace,
                 const std::string& image_id)
    : pool_id(pool_id), pool_namespace(pool_namespace), image_id(image_id) {
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<ChildImageSpec*> &o);

  inline bool operator==(const ChildImageSpec& rhs) const {
    return (pool_id == rhs.pool_id &&
            pool_namespace == rhs.pool_namespace &&
            image_id == rhs.image_id);
  }
  inline bool operator<(const ChildImageSpec& rhs) const {
    if (pool_id != rhs.pool_id) {
      return pool_id < rhs.pool_id;
    }
    if (pool_namespace != rhs.pool_namespace) {
      return pool_namespace < rhs.pool_namespace;
    }
    return image_id < rhs.image_id;
  }
};
WRITE_CLASS_ENCODER(ChildImageSpec);

std::ostream& operator<<(std::ostream& os, const ChildImageSpec& rhs);

typedef std::set<ChildImageSpec> ChildImageSpecs;

struct GroupImageSpec {
  GroupImageSpec() {}

  GroupImageSpec(const std::string &image_id, int64_t pool_id)
    : image_id(image_id), pool_id(pool_id) {}

  static int from_key(const std::string &image_key, GroupImageSpec *spec);

  std::string image_id;
  int64_t pool_id = -1;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<GroupImageSpec*> &o);

  std::string image_key();

  bool operator==(const GroupImageSpec&) const = default;
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

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

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

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;
  bool is_valid() const;

  static void generate_test_instances(std::list<GroupSpec *> &o);
};

WRITE_CLASS_ENCODER(GroupSpec);

enum SnapshotNamespaceType {
  SNAPSHOT_NAMESPACE_TYPE_USER   = 0,
  SNAPSHOT_NAMESPACE_TYPE_GROUP  = 1,
  SNAPSHOT_NAMESPACE_TYPE_TRASH  = 2,
  SNAPSHOT_NAMESPACE_TYPE_MIRROR = 3,
};

struct UserSnapshotNamespace {
  static const SnapshotNamespaceType SNAPSHOT_NAMESPACE_TYPE =
    SNAPSHOT_NAMESPACE_TYPE_USER;

  UserSnapshotNamespace() {}

  void encode(ceph::buffer::list& bl) const {}
  void decode(ceph::buffer::list::const_iterator& it) {}

  void dump(ceph::Formatter *f) const {}

  inline bool operator==(const UserSnapshotNamespace& usn) const {
    return true;
  }

  inline bool operator!=(const UserSnapshotNamespace& usn) const {
    return false;
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
			 const std::string &_group_id,
			 const std::string &_group_snapshot_id)
    : group_id(_group_id), group_pool(_group_pool),
      group_snapshot_id(_group_snapshot_id) {}

  std::string group_id;
  int64_t group_pool = 0;
  std::string group_snapshot_id;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& it);

  void dump(ceph::Formatter *f) const;

  inline bool operator==(const GroupSnapshotNamespace& gsn) const {
    return group_pool == gsn.group_pool &&
	   group_id == gsn.group_id &&
	   group_snapshot_id == gsn.group_snapshot_id;
  }

  inline bool operator!=(const GroupSnapshotNamespace& gsn) const {
    return !operator==(gsn);
  }

  inline bool operator<(const GroupSnapshotNamespace& gsn) const {
    if (group_pool != gsn.group_pool) {
      return group_pool < gsn.group_pool;
    }
    if (group_id != gsn.group_id) {
      return group_id < gsn.group_id;
    }
    return group_snapshot_id < gsn.group_snapshot_id;
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

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& it);
  void dump(ceph::Formatter *f) const;

  inline bool operator==(const TrashSnapshotNamespace& usn) const {
    return true;
  }
  inline bool operator!=(const TrashSnapshotNamespace& usn) const {
    return false;
  }
  inline bool operator<(const TrashSnapshotNamespace& usn) const {
    return false;
  }
};

enum MirrorSnapshotState {
  MIRROR_SNAPSHOT_STATE_PRIMARY             = 0,
  MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED     = 1,
  MIRROR_SNAPSHOT_STATE_NON_PRIMARY         = 2,
  MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED = 3,
};

inline void encode(const MirrorSnapshotState &state, ceph::buffer::list& bl,
                   uint64_t features=0) {
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(MirrorSnapshotState &state, ceph::buffer::list::const_iterator& it) {
  using ceph::decode;
  uint8_t int_state;
  decode(int_state, it);
  state = static_cast<MirrorSnapshotState>(int_state);
}

std::ostream& operator<<(std::ostream& os, MirrorSnapshotState type);

typedef std::map<uint64_t, uint64_t> SnapSeqs;

struct MirrorSnapshotNamespace {
  static const SnapshotNamespaceType SNAPSHOT_NAMESPACE_TYPE =
    SNAPSHOT_NAMESPACE_TYPE_MIRROR;

  MirrorSnapshotState state = MIRROR_SNAPSHOT_STATE_NON_PRIMARY;
  bool complete = false;
  std::set<std::string> mirror_peer_uuids;

  std::string primary_mirror_uuid;
  union {
    snapid_t primary_snap_id = CEPH_NOSNAP;
    snapid_t clean_since_snap_id;
  };
  uint64_t last_copied_object_number = 0;
  SnapSeqs snap_seqs;

  MirrorSnapshotNamespace() {
  }
  MirrorSnapshotNamespace(MirrorSnapshotState state,
                          const std::set<std::string> &mirror_peer_uuids,
                          const std::string& primary_mirror_uuid,
                          snapid_t primary_snap_id)
    : state(state), mirror_peer_uuids(mirror_peer_uuids),
      primary_mirror_uuid(primary_mirror_uuid),
      primary_snap_id(primary_snap_id) {
  }
  MirrorSnapshotNamespace(MirrorSnapshotState state,
                          const std::set<std::string> &mirror_peer_uuids,
                          const std::string& primary_mirror_uuid,
                          snapid_t primary_snap_id,
                          bool complete,
                          uint64_t last_copied_object_number,
                          const SnapSeqs& snap_seqs)
    : state(state), complete(complete), mirror_peer_uuids(mirror_peer_uuids),
      primary_mirror_uuid(primary_mirror_uuid),
      primary_snap_id(primary_snap_id),
      last_copied_object_number(last_copied_object_number),
      snap_seqs(snap_seqs) {
  }

  inline bool is_primary() const {
    return (state == MIRROR_SNAPSHOT_STATE_PRIMARY ||
            state == MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED);
  }

  inline bool is_non_primary() const {
    return (state == MIRROR_SNAPSHOT_STATE_NON_PRIMARY ||
            state == MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED);
  }

  inline bool is_demoted() const {
    return (state == MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED ||
            state == MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED);
  }

  inline bool is_orphan() const {
    return (is_non_primary() &&
            primary_mirror_uuid.empty() &&
            primary_snap_id == CEPH_NOSNAP);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& it);

  void dump(ceph::Formatter *f) const;

  inline bool operator==(const MirrorSnapshotNamespace& rhs) const {
    return state == rhs.state &&
           complete == rhs.complete &&
           mirror_peer_uuids == rhs.mirror_peer_uuids &&
           primary_mirror_uuid == rhs.primary_mirror_uuid &&
           primary_snap_id == rhs.primary_snap_id &&
           last_copied_object_number == rhs.last_copied_object_number &&
           snap_seqs == rhs.snap_seqs;
  }

  inline bool operator!=(const MirrorSnapshotNamespace& rhs) const {
    return !operator==(rhs);
  }

  inline bool operator<(const MirrorSnapshotNamespace& rhs) const {
    if (state != rhs.state) {
      return state < rhs.state;
    } else if (complete != rhs.complete) {
      return complete < rhs.complete;
    } else if (mirror_peer_uuids != rhs.mirror_peer_uuids) {
      return mirror_peer_uuids < rhs.mirror_peer_uuids;
    } else if (primary_mirror_uuid != rhs.primary_mirror_uuid) {
      return primary_mirror_uuid < rhs.primary_mirror_uuid;
    } else if (primary_snap_id != rhs.primary_snap_id) {
      return primary_snap_id < rhs.primary_snap_id;
    } else if (last_copied_object_number != rhs.last_copied_object_number) {
      return last_copied_object_number < rhs.last_copied_object_number;
    } else {
      return snap_seqs < rhs.snap_seqs;
    }
  }
};

struct UnknownSnapshotNamespace {
  static const SnapshotNamespaceType SNAPSHOT_NAMESPACE_TYPE =
    static_cast<SnapshotNamespaceType>(-1);

  UnknownSnapshotNamespace() {}

  void encode(ceph::buffer::list& bl) const {}
  void decode(ceph::buffer::list::const_iterator& it) {}
  void dump(ceph::Formatter *f) const {}

  inline bool operator==(const UnknownSnapshotNamespace& gsn) const {
    return true;
  }

  inline bool operator!=(const UnknownSnapshotNamespace& gsn) const {
    return false;
  }

  inline bool operator<(const UnknownSnapshotNamespace& gsn) const {
    return false;
  }
};

std::ostream& operator<<(std::ostream& os, const SnapshotNamespaceType& type);
std::ostream& operator<<(std::ostream& os, const UserSnapshotNamespace& ns);
std::ostream& operator<<(std::ostream& os, const GroupSnapshotNamespace& ns);
std::ostream& operator<<(std::ostream& os, const TrashSnapshotNamespace& ns);
std::ostream& operator<<(std::ostream& os, const MirrorSnapshotNamespace& ns);
std::ostream& operator<<(std::ostream& os, const UnknownSnapshotNamespace& ns);

typedef std::variant<UserSnapshotNamespace,
		     GroupSnapshotNamespace,
		     TrashSnapshotNamespace,
		     MirrorSnapshotNamespace,
		     UnknownSnapshotNamespace> SnapshotNamespaceVariant;

struct SnapshotNamespace : public SnapshotNamespaceVariant {
  using SnapshotNamespaceVariant::SnapshotNamespaceVariant;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& it);
  void dump(ceph::Formatter *f) const;

  template <typename F>
  decltype(auto) visit(F&& f) const & {
    return std::visit(std::forward<F>(f), static_cast<const SnapshotNamespaceVariant&>(*this));
  }
  template <typename F>
  decltype(auto) visit(F&& f) & {
    return std::visit(std::forward<F>(f), static_cast<SnapshotNamespaceVariant&>(*this));
  }
  static void generate_test_instances(std::list<SnapshotNamespace*> &o);
};
WRITE_CLASS_ENCODER(SnapshotNamespace);

std::ostream& operator<<(std::ostream& os, const SnapshotNamespace& ns);

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

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<SnapshotInfo*> &o);
};
WRITE_CLASS_ENCODER(SnapshotInfo);

enum GroupSnapshotState {
  GROUP_SNAPSHOT_STATE_INCOMPLETE = 0,
  GROUP_SNAPSHOT_STATE_COMPLETE = 1,
};

inline void encode(const GroupSnapshotState &state, ceph::buffer::list& bl, uint64_t features=0)
{
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(GroupSnapshotState &state, ceph::buffer::list::const_iterator& it)
{
  using ceph::decode;
  uint8_t int_state;
  decode(int_state, it);
  state = static_cast<GroupSnapshotState>(int_state);
}

struct ImageSnapshotSpec {
  int64_t pool;
  std::string image_id;
  snapid_t snap_id;

  ImageSnapshotSpec() {}
  ImageSnapshotSpec(int64_t _pool,
		    std::string _image_id,
		    snapid_t _snap_id) : pool(_pool),
					 image_id(_image_id),
					 snap_id(_snap_id) {}

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& it);

  void dump(ceph::Formatter *f) const;

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

  std::vector<ImageSnapshotSpec> snaps;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<GroupSnapshot *> &o);
};
WRITE_CLASS_ENCODER(GroupSnapshot);
enum TrashImageSource {
  TRASH_IMAGE_SOURCE_USER = 0,
  TRASH_IMAGE_SOURCE_MIRRORING = 1,
  TRASH_IMAGE_SOURCE_MIGRATION = 2,
  TRASH_IMAGE_SOURCE_REMOVING = 3,
  TRASH_IMAGE_SOURCE_USER_PARENT= 4,
};

inline std::ostream& operator<<(std::ostream& os,
                                const TrashImageSource& source) {
  switch (source) {
  case TRASH_IMAGE_SOURCE_USER:
    os << "user";
    break;
  case TRASH_IMAGE_SOURCE_MIRRORING:
    os << "mirroring";
    break;
  case TRASH_IMAGE_SOURCE_MIGRATION:
    os << "migration";
    break;
  case TRASH_IMAGE_SOURCE_REMOVING:
    os << "removing";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(source) << ")";
    break;
  }
  return os;
}

inline void encode(const TrashImageSource &source, ceph::buffer::list& bl,
		   uint64_t features=0)
{
  using ceph::encode;
  encode(static_cast<uint8_t>(source), bl);
}

inline void decode(TrashImageSource &source, ceph::buffer::list::const_iterator& it)
{
  uint8_t int_source;
  using ceph::decode;
  decode(int_source, it);
  source = static_cast<TrashImageSource>(int_source);
}

enum TrashImageState {
  TRASH_IMAGE_STATE_NORMAL    = 0,
  TRASH_IMAGE_STATE_MOVING    = 1,
  TRASH_IMAGE_STATE_REMOVING  = 2,
  TRASH_IMAGE_STATE_RESTORING = 3
};

inline void encode(const TrashImageState &state, ceph::buffer::list &bl)
{
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(TrashImageState &state, ceph::buffer::list::const_iterator &it)
{
  uint8_t int_state;
  using ceph::decode;
  decode(int_state, it);
  state = static_cast<TrashImageState>(int_state);
}

struct TrashImageSpec {
  TrashImageSource source = TRASH_IMAGE_SOURCE_USER;
  std::string name;
  utime_t deletion_time; // time of deletion
  utime_t deferment_end_time;
  TrashImageState state = TRASH_IMAGE_STATE_NORMAL;

  TrashImageSpec() {}
  TrashImageSpec(TrashImageSource source, const std::string &name,
                 const utime_t& deletion_time,
                 const utime_t& deferment_end_time)
    : source(source), name(name), deletion_time(deletion_time),
      deferment_end_time(deferment_end_time) {
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& it);
  void dump(ceph::Formatter *f) const;

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
                 const ceph::buffer::list &data)
    : instance_id(instance_id),
      mapped_time(mapped_time),
      data(data) {
  }

  std::string instance_id;
  utime_t mapped_time;
  ceph::buffer::list data;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<MirrorImageMap*> &o);

  bool operator==(const MirrorImageMap &rhs) const;
  bool operator<(const MirrorImageMap &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorImageMap &image_map);

WRITE_CLASS_ENCODER(MirrorImageMap);

enum MigrationHeaderType {
  MIGRATION_HEADER_TYPE_SRC = 1,
  MIGRATION_HEADER_TYPE_DST = 2,
};

inline void encode(const MigrationHeaderType &type, ceph::buffer::list& bl) {
  using ceph::encode;
  encode(static_cast<uint8_t>(type), bl);
}

inline void decode(MigrationHeaderType &type, ceph::buffer::list::const_iterator& it) {
  uint8_t int_type;
  using ceph::decode;
  decode(int_type, it);
  type = static_cast<MigrationHeaderType>(int_type);
}

enum MigrationState {
  MIGRATION_STATE_ERROR = 0,
  MIGRATION_STATE_PREPARING = 1,
  MIGRATION_STATE_PREPARED = 2,
  MIGRATION_STATE_EXECUTING = 3,
  MIGRATION_STATE_EXECUTED = 4,
  MIGRATION_STATE_ABORTING = 5,
};

inline void encode(const MigrationState &state, ceph::buffer::list& bl) {
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(MigrationState &state, ceph::buffer::list::const_iterator& it) {
  uint8_t int_state;
  using ceph::decode;
  decode(int_state, it);
  state = static_cast<MigrationState>(int_state);
}

std::ostream& operator<<(std::ostream& os,
                         const MigrationState& migration_state);

struct MigrationSpec {
  MigrationHeaderType header_type = MIGRATION_HEADER_TYPE_SRC;
  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_name;
  std::string image_id;
  std::string source_spec;
  std::map<uint64_t, uint64_t> snap_seqs;
  uint64_t overlap = 0;
  bool flatten = false;
  bool mirroring = false;
  MirrorImageMode mirror_image_mode = MIRROR_IMAGE_MODE_JOURNAL;
  MigrationState state = MIGRATION_STATE_ERROR;
  std::string state_description;

  MigrationSpec() {
  }
  MigrationSpec(MigrationHeaderType header_type, int64_t pool_id,
                const std::string& pool_namespace,
                const std::string& image_name, const std::string &image_id,
                const std::string& source_spec,
                const std::map<uint64_t, uint64_t> &snap_seqs, uint64_t overlap,
                bool mirroring, MirrorImageMode mirror_image_mode, bool flatten,
                MigrationState state, const std::string &state_description)
    : header_type(header_type), pool_id(pool_id),
      pool_namespace(pool_namespace), image_name(image_name),
      image_id(image_id), source_spec(source_spec), snap_seqs(snap_seqs),
      overlap(overlap), flatten(flatten), mirroring(mirroring),
      mirror_image_mode(mirror_image_mode), state(state),
      state_description(state_description) {
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& it);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<MigrationSpec*> &o);

  inline bool operator==(const MigrationSpec& ms) const {
    return header_type == ms.header_type && pool_id == ms.pool_id &&
      pool_namespace == ms.pool_namespace && image_name == ms.image_name &&
      image_id == ms.image_id && source_spec == ms.source_spec &&
      snap_seqs == ms.snap_seqs && overlap == ms.overlap &&
      flatten == ms.flatten && mirroring == ms.mirroring &&
      mirror_image_mode == ms.mirror_image_mode && state == ms.state &&
      state_description == ms.state_description;
  }
};

std::ostream& operator<<(std::ostream& os, const MigrationSpec& migration_spec);

WRITE_CLASS_ENCODER(MigrationSpec);

enum AssertSnapcSeqState {
  ASSERT_SNAPC_SEQ_GT_SNAPSET_SEQ = 0,
  ASSERT_SNAPC_SEQ_LE_SNAPSET_SEQ = 1,
};

inline void encode(const AssertSnapcSeqState &state, ceph::buffer::list& bl) {
  using ceph::encode;
  encode(static_cast<uint8_t>(state), bl);
}

inline void decode(AssertSnapcSeqState &state, ceph::buffer::list::const_iterator& it) {
  uint8_t int_state;
  using ceph::decode;
  decode(int_state, it);
  state = static_cast<AssertSnapcSeqState>(int_state);
}

std::ostream& operator<<(std::ostream& os, const AssertSnapcSeqState& state);

void sanitize_entity_inst(entity_inst_t* entity_inst);

} // namespace rbd
} // namespace cls

#endif // CEPH_CLS_RBD_TYPES_H
