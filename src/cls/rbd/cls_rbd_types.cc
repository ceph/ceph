// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/variant.hpp>
#include "cls/rbd/cls_rbd_types.h"
#include "common/Formatter.h"

namespace cls {
namespace rbd {

std::ostream& operator<<(std::ostream& os,
                         MirrorPeerDirection mirror_peer_direction) {
  switch (mirror_peer_direction) {
  case MIRROR_PEER_DIRECTION_RX:
    os << "RX";
    break;
  case MIRROR_PEER_DIRECTION_TX:
    os << "TX";
    break;
  case MIRROR_PEER_DIRECTION_RX_TX:
    os << "RX/TX";
    break;
  default:
    os << "unknown";
    break;
  }
  return os;
}

void MirrorPeer::encode(bufferlist &bl) const {
  ENCODE_START(2, 1, bl);
  encode(uuid, bl);
  encode(site_name, bl);
  encode(client_name, bl);
  int64_t pool_id = -1;
  encode(pool_id, bl);

  // v2
  encode(static_cast<uint8_t>(mirror_peer_direction), bl);
  encode(fsid, bl);
  encode(last_seen, bl);
  ENCODE_FINISH(bl);
}

void MirrorPeer::decode(bufferlist::const_iterator &it) {
  DECODE_START(2, it);
  decode(uuid, it);
  decode(site_name, it);
  decode(client_name, it);
  int64_t pool_id;
  decode(pool_id, it);

  if (struct_v >= 2) {
    uint8_t mpd;
    decode(mpd, it);
    mirror_peer_direction = static_cast<MirrorPeerDirection>(mpd);
    decode(fsid, it);
    decode(last_seen, it);
  }

  DECODE_FINISH(it);
}

void MirrorPeer::dump(Formatter *f) const {
  f->dump_string("uuid", uuid);
  f->dump_stream("direction") << mirror_peer_direction;
  f->dump_string("site_name", site_name);
  f->dump_string("fsid", fsid);
  f->dump_string("client_name", client_name);
  f->dump_stream("last_seen") << last_seen;
}

void MirrorPeer::generate_test_instances(std::list<MirrorPeer*> &o) {
  o.push_back(new MirrorPeer());
  o.push_back(new MirrorPeer("uuid-123", MIRROR_PEER_DIRECTION_RX, "site A",
                             "client name", ""));
  o.push_back(new MirrorPeer("uuid-234", MIRROR_PEER_DIRECTION_TX, "site B",
                             "", "fsid"));
  o.push_back(new MirrorPeer("uuid-345", MIRROR_PEER_DIRECTION_RX_TX, "site C",
                             "client name", "fsid"));
}

bool MirrorPeer::operator==(const MirrorPeer &rhs) const {
  return (uuid == rhs.uuid &&
          mirror_peer_direction == rhs.mirror_peer_direction &&
          site_name == rhs.site_name &&
          client_name == rhs.client_name &&
          fsid == rhs.fsid &&
          last_seen == rhs.last_seen);
}

std::ostream& operator<<(std::ostream& os, const MirrorMode& mirror_mode) {
  switch (mirror_mode) {
  case MIRROR_MODE_DISABLED:
    os << "disabled";
    break;
  case MIRROR_MODE_IMAGE:
    os << "image";
    break;
  case MIRROR_MODE_POOL:
    os << "pool";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(mirror_mode) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer) {
  os << "["
     << "uuid=" << peer.uuid << ", "
     << "direction=" << peer.mirror_peer_direction << ", "
     << "site_name=" << peer.site_name << ", "
     << "client_name=" << peer.client_name << ", "
     << "fsid=" << peer.fsid << ", "
     << "last_seen=" << peer.last_seen
     << "]";
  return os;
}

void MirrorImage::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  encode(global_image_id, bl);
  encode(static_cast<uint8_t>(state), bl);
  ENCODE_FINISH(bl);
}

void MirrorImage::decode(bufferlist::const_iterator &it) {
  uint8_t int_state;
  DECODE_START(1, it);
  decode(global_image_id, it);
  decode(int_state, it);
  state = static_cast<MirrorImageState>(int_state);
  DECODE_FINISH(it);
}

void MirrorImage::dump(Formatter *f) const {
  f->dump_string("global_image_id", global_image_id);
  f->dump_int("state", state);
}

void MirrorImage::generate_test_instances(std::list<MirrorImage*> &o) {
  o.push_back(new MirrorImage());
  o.push_back(new MirrorImage("uuid-123", MIRROR_IMAGE_STATE_ENABLED));
  o.push_back(new MirrorImage("uuid-abc", MIRROR_IMAGE_STATE_DISABLING));
}

bool MirrorImage::operator==(const MirrorImage &rhs) const {
  return global_image_id == rhs.global_image_id && state == rhs.state;
}

bool MirrorImage::operator<(const MirrorImage &rhs) const {
  return global_image_id < rhs.global_image_id ||
	(global_image_id == rhs.global_image_id  && state < rhs.state);
}

std::ostream& operator<<(std::ostream& os, const MirrorImageState& mirror_state) {
  switch (mirror_state) {
  case MIRROR_IMAGE_STATE_DISABLING:
    os << "disabling";
    break;
  case MIRROR_IMAGE_STATE_ENABLED:
    os << "enabled";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(mirror_state) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const MirrorImage& mirror_image) {
  os << "["
     << "global_image_id=" << mirror_image.global_image_id << ", "
     << "state=" << mirror_image.state << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os,
                         const MirrorImageStatusState& state) {
  switch (state) {
  case MIRROR_IMAGE_STATUS_STATE_UNKNOWN:
    os << "unknown";
    break;
  case MIRROR_IMAGE_STATUS_STATE_ERROR:
    os << "error";
    break;
  case MIRROR_IMAGE_STATUS_STATE_SYNCING:
    os << "syncing";
    break;
  case MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY:
    os << "starting_replay";
    break;
  case MIRROR_IMAGE_STATUS_STATE_REPLAYING:
    os << "replaying";
    break;
  case MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY:
    os << "stopping_replay";
    break;
  case MIRROR_IMAGE_STATUS_STATE_STOPPED:
    os << "stopped";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

const std::string MirrorImageSiteStatus::LOCAL_FSID(""); // empty fsid

void MirrorImageSiteStatus::encode_meta(uint8_t version, bufferlist &bl) const {
  if (version >= 2) {
    ceph::encode(fsid, bl);
  }
  cls::rbd::encode(state, bl);
  ceph::encode(description, bl);
  ceph::encode(last_update, bl);
  ceph::encode(up, bl);
}

void MirrorImageSiteStatus::decode_meta(uint8_t version,
                                        bufferlist::const_iterator &it) {
  if (version < 2) {
    fsid = LOCAL_FSID;
  } else {
    ceph::decode(fsid, it);
  }

  cls::rbd::decode(state, it);
  ceph::decode(description, it);
  ::decode(last_update, it);
  ceph::decode(up, it);
}

void MirrorImageSiteStatus::encode(bufferlist &bl) const {
  // break compatibility when site-name is provided
  uint8_t version = (fsid == LOCAL_FSID ? 1 : 2);
  ENCODE_START(version, version, bl);
  encode_meta(version, bl);
  ENCODE_FINISH(bl);
}

void MirrorImageSiteStatus::decode(bufferlist::const_iterator &it) {
  DECODE_START(2, it);
  decode_meta(struct_v, it);
  DECODE_FINISH(it);
}

void MirrorImageSiteStatus::dump(Formatter *f) const {
  f->dump_string("state", state_to_string());
  f->dump_string("description", description);
  f->dump_stream("last_update") << last_update;
}

std::string MirrorImageSiteStatus::state_to_string() const {
  std::stringstream ss;
  ss << (up ? "up+" : "down+") << state;
  return ss.str();
}

void MirrorImageSiteStatus::generate_test_instances(
  std::list<MirrorImageSiteStatus*> &o) {
  o.push_back(new MirrorImageSiteStatus());
  o.push_back(new MirrorImageSiteStatus("", MIRROR_IMAGE_STATUS_STATE_REPLAYING,
                                        ""));
  o.push_back(new MirrorImageSiteStatus("", MIRROR_IMAGE_STATUS_STATE_ERROR,
                                        "error"));
  o.push_back(new MirrorImageSiteStatus("2fb68ca9-1ba0-43b3-8cdf-8c5a9db71e65",
                                        MIRROR_IMAGE_STATUS_STATE_STOPPED, ""));
}

bool MirrorImageSiteStatus::operator==(const MirrorImageSiteStatus &rhs) const {
  return state == rhs.state && description == rhs.description && up == rhs.up;
}

std::ostream& operator<<(std::ostream& os,
                         const MirrorImageSiteStatus& status) {
  os << "{"
     << "state=" << status.state_to_string() << ", "
     << "description=" << status.description << ", "
     << "last_update=" << status.last_update << "]}";
  return os;
}

void MirrorImageSiteStatusOnDisk::encode_meta(bufferlist &bl,
                                              uint64_t features) const {
  ENCODE_START(1, 1, bl);
  auto sanitized_origin = origin;
  sanitize_entity_inst(&sanitized_origin);
  encode(sanitized_origin, bl, features);
  ENCODE_FINISH(bl);
}

void MirrorImageSiteStatusOnDisk::encode(bufferlist &bl,
                                         uint64_t features) const {
  encode_meta(bl, features);
  cls::rbd::MirrorImageSiteStatus::encode(bl);
}

void MirrorImageSiteStatusOnDisk::decode_meta(bufferlist::const_iterator &it) {
  DECODE_START(1, it);
  decode(origin, it);
  sanitize_entity_inst(&origin);
  DECODE_FINISH(it);
}

void MirrorImageSiteStatusOnDisk::decode(bufferlist::const_iterator &it) {
  decode_meta(it);
  cls::rbd::MirrorImageSiteStatus::decode(it);
}

void MirrorImageSiteStatusOnDisk::generate_test_instances(
    std::list<MirrorImageSiteStatusOnDisk*> &o) {
  o.push_back(new MirrorImageSiteStatusOnDisk());
  o.push_back(new MirrorImageSiteStatusOnDisk(
    {"", MIRROR_IMAGE_STATUS_STATE_ERROR, "error"}));
  o.push_back(new MirrorImageSiteStatusOnDisk(
    {"siteA", MIRROR_IMAGE_STATUS_STATE_STOPPED, ""}));
}

int MirrorImageStatus::get_local_mirror_image_site_status(
    MirrorImageSiteStatus* status) const {
  auto it = std::find_if(
    mirror_image_site_statuses.begin(),
    mirror_image_site_statuses.end(),
    [](const MirrorImageSiteStatus& status) {
      return status.fsid == MirrorImageSiteStatus::LOCAL_FSID;
    });
  if (it == mirror_image_site_statuses.end()) {
    return -ENOENT;
  }

  *status = *it;
  return 0;
}

void MirrorImageStatus::encode(bufferlist &bl) const {
  // don't break compatibility for extra site statuses
  ENCODE_START(2, 1, bl);

  // local site status
  MirrorImageSiteStatus local_status;
  int r = get_local_mirror_image_site_status(&local_status);
  local_status.encode_meta(1, bl);

  bool local_status_valid = (r >= 0);
  encode(local_status_valid, bl);

  // remote site statuses
  __u32 n = mirror_image_site_statuses.size();
  if (local_status_valid) {
    --n;
  }
  encode(n, bl);

  for (auto& status : mirror_image_site_statuses) {
    if (status.fsid == MirrorImageSiteStatus::LOCAL_FSID) {
      continue;
    }
    status.encode_meta(2, bl);
  }
  ENCODE_FINISH(bl);
}

void MirrorImageStatus::decode(bufferlist::const_iterator &it) {
  DECODE_START(2, it);

  // local site status
  MirrorImageSiteStatus local_status;
  local_status.decode_meta(1, it);

  if (struct_v < 2) {
    mirror_image_site_statuses.push_back(local_status);
  } else {
    bool local_status_valid;
    decode(local_status_valid, it);

    __u32 n;
    decode(n, it);
    if (local_status_valid) {
      ++n;
    }

    mirror_image_site_statuses.resize(n);
    for (auto status_it = mirror_image_site_statuses.begin();
         status_it != mirror_image_site_statuses.end(); ++status_it) {
      if (local_status_valid &&
          status_it == mirror_image_site_statuses.begin()) {
        *status_it = local_status;
        continue;
      }

      // remote site status
      status_it->decode_meta(struct_v, it);
    }
  }
  DECODE_FINISH(it);
}

void MirrorImageStatus::dump(Formatter *f) const {
  MirrorImageSiteStatus local_status;
  int r = get_local_mirror_image_site_status(&local_status);
  if (r >= 0) {
    local_status.dump(f);
  }

  f->open_array_section("remotes");
  for (auto& status : mirror_image_site_statuses) {
    if (status.fsid == MirrorImageSiteStatus::LOCAL_FSID) {
      continue;
    }

    f->open_object_section("remote");
    status.dump(f);
    f->close_section();
  }
  f->close_section();
}

bool MirrorImageStatus::operator==(const MirrorImageStatus &rhs) const {
  return (mirror_image_site_statuses == rhs.mirror_image_site_statuses);
}

void MirrorImageStatus::generate_test_instances(
    std::list<MirrorImageStatus*> &o) {
  o.push_back(new MirrorImageStatus());
  o.push_back(new MirrorImageStatus({{"", MIRROR_IMAGE_STATUS_STATE_ERROR, ""}}));
  o.push_back(new MirrorImageStatus({{"", MIRROR_IMAGE_STATUS_STATE_STOPPED, ""},
                                     {"siteA", MIRROR_IMAGE_STATUS_STATE_REPLAYING, ""}}));
}

std::ostream& operator<<(std::ostream& os,
                         const MirrorImageStatus& status) {
  os << "{";
  MirrorImageSiteStatus local_status;
  int r = status.get_local_mirror_image_site_status(&local_status);
  if (r >= 0) {
    os << "state=" << local_status.state_to_string() << ", "
       << "description=" << local_status.description << ", "
       << "last_update=" << local_status.last_update << ", ";
  }

  os << "remotes=[";
  for (auto& remote_status : status.mirror_image_site_statuses) {
    if (remote_status.fsid == MirrorImageSiteStatus::LOCAL_FSID) {
      continue;
    }

    os << "{"
       << "fsid=" << remote_status.fsid << ", "
       << "state=" << remote_status.state_to_string() << ", "
       << "description=" << remote_status.description << ", "
       << "last_update=" << remote_status.last_update
       << "}";
  }
  os << "]}";
  return os;
}

void ParentImageSpec::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(pool_id, bl);
  encode(pool_namespace, bl);
  encode(image_id, bl);
  encode(snap_id, bl);
  ENCODE_FINISH(bl);
}

void ParentImageSpec::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  decode(pool_id, bl);
  decode(pool_namespace, bl);
  decode(image_id, bl);
  decode(snap_id, bl);
  DECODE_FINISH(bl);
}

void ParentImageSpec::dump(Formatter *f) const {
  f->dump_int("pool_id", pool_id);
  f->dump_string("pool_namespace", pool_namespace);
  f->dump_string("image_id", image_id);
  f->dump_unsigned("snap_id", snap_id);
}

void ParentImageSpec::generate_test_instances(std::list<ParentImageSpec*>& o) {
  o.push_back(new ParentImageSpec{});
  o.push_back(new ParentImageSpec{1, "", "foo", 3});
  o.push_back(new ParentImageSpec{1, "ns", "foo", 3});
}

void ChildImageSpec::encode(bufferlist &bl) const {
  ENCODE_START(2, 1, bl);
  encode(pool_id, bl);
  encode(image_id, bl);
  encode(pool_namespace, bl);
  ENCODE_FINISH(bl);
}

void ChildImageSpec::decode(bufferlist::const_iterator &it) {
  DECODE_START(2, it);
  decode(pool_id, it);
  decode(image_id, it);
  if (struct_v >= 2) {
    decode(pool_namespace, it);
  }
  DECODE_FINISH(it);
}

void ChildImageSpec::dump(Formatter *f) const {
  f->dump_int("pool_id", pool_id);
  f->dump_string("pool_namespace", pool_namespace);
  f->dump_string("image_id", image_id);
}

void ChildImageSpec::generate_test_instances(std::list<ChildImageSpec*> &o) {
  o.push_back(new ChildImageSpec());
  o.push_back(new ChildImageSpec(123, "", "abc"));
  o.push_back(new ChildImageSpec(123, "ns", "abc"));
}

void GroupImageSpec::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  encode(image_id, bl);
  encode(pool_id, bl);
  ENCODE_FINISH(bl);
}

void GroupImageSpec::decode(bufferlist::const_iterator &it) {
  DECODE_START(1, it);
  decode(image_id, it);
  decode(pool_id, it);
  DECODE_FINISH(it);
}

void GroupImageSpec::dump(Formatter *f) const {
  f->dump_string("image_id", image_id);
  f->dump_int("pool_id", pool_id);
}

int GroupImageSpec::from_key(const std::string &image_key,
				    GroupImageSpec *spec) {
  if (nullptr == spec) return -EINVAL;
  int prefix_len = cls::rbd::RBD_GROUP_IMAGE_KEY_PREFIX.size();
  std::string data_string = image_key.substr(prefix_len,
					     image_key.size() - prefix_len);
  size_t p = data_string.find("_");
  if (std::string::npos == p) {
    return -EIO;
  }
  data_string[p] = ' ';

  istringstream iss(data_string);
  uint64_t pool_id;
  string image_id;
  iss >> std::hex >> pool_id >> image_id;

  spec->image_id = image_id;
  spec->pool_id = pool_id;
  return 0;
}

std::string GroupImageSpec::image_key() {
  if (-1 == pool_id)
    return "";
  else {
    ostringstream oss;
    oss << RBD_GROUP_IMAGE_KEY_PREFIX << std::setw(16)
	<< std::setfill('0') << std::hex << pool_id << "_" << image_id;
    return oss.str();
  }
}

void GroupImageSpec::generate_test_instances(std::list<GroupImageSpec*> &o) {
  o.push_back(new GroupImageSpec("10152ae8944a", 0));
  o.push_back(new GroupImageSpec("1018643c9869", 3));
}

void GroupImageStatus::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  encode(spec, bl);
  encode(state, bl);
  ENCODE_FINISH(bl);
}

void GroupImageStatus::decode(bufferlist::const_iterator &it) {
  DECODE_START(1, it);
  decode(spec, it);
  decode(state, it);
  DECODE_FINISH(it);
}

std::string GroupImageStatus::state_to_string() const {
  std::stringstream ss;
  if (state == GROUP_IMAGE_LINK_STATE_INCOMPLETE) {
    ss << "incomplete";
  }
  if (state == GROUP_IMAGE_LINK_STATE_ATTACHED) {
    ss << "attached";
  }
  return ss.str();
}

void GroupImageStatus::dump(Formatter *f) const {
  spec.dump(f);
  f->dump_string("state", state_to_string());
}

void GroupImageStatus::generate_test_instances(std::list<GroupImageStatus*> &o) {
  o.push_back(new GroupImageStatus(GroupImageSpec("10152ae8944a", 0), GROUP_IMAGE_LINK_STATE_ATTACHED));
  o.push_back(new GroupImageStatus(GroupImageSpec("1018643c9869", 3), GROUP_IMAGE_LINK_STATE_ATTACHED));
  o.push_back(new GroupImageStatus(GroupImageSpec("10152ae8944a", 0), GROUP_IMAGE_LINK_STATE_INCOMPLETE));
  o.push_back(new GroupImageStatus(GroupImageSpec("1018643c9869", 3), GROUP_IMAGE_LINK_STATE_INCOMPLETE));
}


void GroupSpec::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  encode(pool_id, bl);
  encode(group_id, bl);
  ENCODE_FINISH(bl);
}

void GroupSpec::decode(bufferlist::const_iterator &it) {
  DECODE_START(1, it);
  decode(pool_id, it);
  decode(group_id, it);
  DECODE_FINISH(it);
}

void GroupSpec::dump(Formatter *f) const {
  f->dump_string("group_id", group_id);
  f->dump_int("pool_id", pool_id);
}

bool GroupSpec::is_valid() const {
  return (!group_id.empty()) && (pool_id != -1);
}

void GroupSpec::generate_test_instances(std::list<GroupSpec *> &o) {
  o.push_back(new GroupSpec("10152ae8944a", 0));
  o.push_back(new GroupSpec("1018643c9869", 3));
}

void GroupSnapshotNamespace::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(group_pool, bl);
  encode(group_id, bl);
  encode(group_snapshot_id, bl);
}

void GroupSnapshotNamespace::decode(bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(group_pool, it);
  decode(group_id, it);
  decode(group_snapshot_id, it);
}

void GroupSnapshotNamespace::dump(Formatter *f) const {
  f->dump_int("group_pool", group_pool);
  f->dump_string("group_id", group_id);
  f->dump_string("group_snapshot_id", group_snapshot_id);
}

void TrashSnapshotNamespace::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(original_name, bl);
  encode(static_cast<uint32_t>(original_snapshot_namespace_type), bl);
}

void TrashSnapshotNamespace::decode(bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(original_name, it);
  uint32_t snap_type;
  decode(snap_type, it);
  original_snapshot_namespace_type = static_cast<SnapshotNamespaceType>(
    snap_type);
}

void TrashSnapshotNamespace::dump(Formatter *f) const {
  f->dump_string("original_name", original_name);
  f->dump_stream("original_snapshot_namespace")
    << original_snapshot_namespace_type;
}

class EncodeSnapshotNamespaceVisitor : public boost::static_visitor<void> {
public:
  explicit EncodeSnapshotNamespaceVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename T>
  inline void operator()(const T& t) const {
    using ceph::encode;
    encode(static_cast<uint32_t>(T::SNAPSHOT_NAMESPACE_TYPE), m_bl);
    t.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class DecodeSnapshotNamespaceVisitor : public boost::static_visitor<void> {
public:
  DecodeSnapshotNamespaceVisitor(bufferlist::const_iterator &iter)
    : m_iter(iter) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    t.decode(m_iter);
  }
private:
  bufferlist::const_iterator &m_iter;
};

class DumpSnapshotNamespaceVisitor : public boost::static_visitor<void> {
public:
  explicit DumpSnapshotNamespaceVisitor(Formatter *formatter, const std::string &key)
    : m_formatter(formatter), m_key(key) {}

  template <typename T>
  inline void operator()(const T& t) const {
    auto type = T::SNAPSHOT_NAMESPACE_TYPE;
    m_formatter->dump_string(m_key.c_str(), stringify(type));
    t.dump(m_formatter);
  }
private:
  ceph::Formatter *m_formatter;
  std::string m_key;
};

class GetTypeVisitor : public boost::static_visitor<SnapshotNamespaceType> {
public:
  template <typename T>
  inline SnapshotNamespaceType operator()(const T&) const {
    return static_cast<SnapshotNamespaceType>(T::SNAPSHOT_NAMESPACE_TYPE);
  }
};

SnapshotNamespaceType get_snap_namespace_type(
    const SnapshotNamespace& snapshot_namespace) {
  return static_cast<SnapshotNamespaceType>(boost::apply_visitor(
    GetTypeVisitor(), snapshot_namespace));
}

void SnapshotInfo::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(id, bl);
  encode(snapshot_namespace, bl);
  encode(name, bl);
  encode(image_size, bl);
  encode(timestamp, bl);
  encode(child_count, bl);
  ENCODE_FINISH(bl);
}

void SnapshotInfo::decode(bufferlist::const_iterator& it) {
  DECODE_START(1, it);
  decode(id, it);
  decode(snapshot_namespace, it);
  decode(name, it);
  decode(image_size, it);
  decode(timestamp, it);
  decode(child_count, it);
  DECODE_FINISH(it);
}

void SnapshotInfo::dump(Formatter *f) const {
  f->dump_unsigned("id", id);
  f->open_object_section("namespace");
  boost::apply_visitor(DumpSnapshotNamespaceVisitor(f, "type"),
                       snapshot_namespace);
  f->close_section();
  f->dump_string("name", name);
  f->dump_unsigned("image_size", image_size);
  f->dump_stream("timestamp") << timestamp;
}

void SnapshotInfo::generate_test_instances(std::list<SnapshotInfo*> &o) {
  o.push_back(new SnapshotInfo(1ULL, UserSnapshotNamespace{}, "snap1", 123,
                               {123456, 0}, 12));
  o.push_back(new SnapshotInfo(2ULL,
                               GroupSnapshotNamespace{567, "group1", "snap1"},
                               "snap1", 123, {123456, 0}, 987));
  o.push_back(new SnapshotInfo(3ULL,
                               TrashSnapshotNamespace{
                                 SNAPSHOT_NAMESPACE_TYPE_USER, "snap1"},
                               "12345", 123, {123456, 0}, 429));
}

void SnapshotNamespace::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  boost::apply_visitor(EncodeSnapshotNamespaceVisitor(bl), *this);
  ENCODE_FINISH(bl);
}

void SnapshotNamespace::decode(bufferlist::const_iterator &p)
{
  DECODE_START(1, p);
  uint32_t snap_type;
  decode(snap_type, p);
  switch (snap_type) {
    case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER:
      *this = UserSnapshotNamespace();
      break;
    case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_GROUP:
      *this = GroupSnapshotNamespace();
      break;
    case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH:
      *this = TrashSnapshotNamespace();
      break;
    default:
      *this = UnknownSnapshotNamespace();
      break;
  }
  boost::apply_visitor(DecodeSnapshotNamespaceVisitor(p), *this);
  DECODE_FINISH(p);
}

void SnapshotNamespace::dump(Formatter *f) const {
  boost::apply_visitor(
    DumpSnapshotNamespaceVisitor(f, "snapshot_namespace_type"), *this);
}

void SnapshotNamespace::generate_test_instances(std::list<SnapshotNamespace*> &o) {
  o.push_back(new SnapshotNamespace(UserSnapshotNamespace()));
  o.push_back(new SnapshotNamespace(GroupSnapshotNamespace(0, "10152ae8944a",
                                                           "2118643c9732")));
  o.push_back(new SnapshotNamespace(GroupSnapshotNamespace(5, "1018643c9869",
                                                           "33352be8933c")));
  o.push_back(new SnapshotNamespace(TrashSnapshotNamespace()));
}

std::ostream& operator<<(std::ostream& os, const SnapshotNamespaceType& type) {
  switch (type) {
  case SNAPSHOT_NAMESPACE_TYPE_USER:
    os << "user";
    break;
  case SNAPSHOT_NAMESPACE_TYPE_GROUP:
    os << "group";
    break;
  case SNAPSHOT_NAMESPACE_TYPE_TRASH:
    os << "trash";
    break;
  default:
    os << "unknown";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const UserSnapshotNamespace& ns) {
  os << "[" << SNAPSHOT_NAMESPACE_TYPE_USER << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const GroupSnapshotNamespace& ns) {
  os << "[" << SNAPSHOT_NAMESPACE_TYPE_GROUP << " "
     << "group_pool=" << ns.group_pool << ", "
     << "group_id=" << ns.group_id << ", "
     << "group_snapshot_id=" << ns.group_snapshot_id << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const TrashSnapshotNamespace& ns) {
  os << "[" << SNAPSHOT_NAMESPACE_TYPE_TRASH << " "
     << "original_name=" << ns.original_name << ", "
     << "original_snapshot_namespace=" << ns.original_snapshot_namespace_type
     << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const UnknownSnapshotNamespace& ns) {
  os << "[unknown]";
  return os;
}

void ImageSnapshotSpec::encode(bufferlist& bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(pool, bl);
  encode(image_id, bl);
  encode(snap_id, bl);
  ENCODE_FINISH(bl);
}

void ImageSnapshotSpec::decode(bufferlist::const_iterator& it) {
  using ceph::decode;
  DECODE_START(1, it);
  decode(pool, it);
  decode(image_id, it);
  decode(snap_id, it);
  DECODE_FINISH(it);
}

void ImageSnapshotSpec::dump(Formatter *f) const {
  f->dump_int("pool", pool);
  f->dump_string("image_id", image_id);
  f->dump_int("snap_id", snap_id);
}

void ImageSnapshotSpec::generate_test_instances(std::list<ImageSnapshotSpec *> &o) {
  o.push_back(new ImageSnapshotSpec(0, "myimage", 2));
  o.push_back(new ImageSnapshotSpec(1, "testimage", 7));
}

void GroupSnapshot::encode(bufferlist& bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(id, bl);
  encode(name, bl);
  encode(state, bl);
  encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void GroupSnapshot::decode(bufferlist::const_iterator& it) {
  using ceph::decode;
  DECODE_START(1, it);
  decode(id, it);
  decode(name, it);
  decode(state, it);
  decode(snaps, it);
  DECODE_FINISH(it);
}

void GroupSnapshot::dump(Formatter *f) const {
  f->dump_string("id", id);
  f->dump_string("name", name);
  f->dump_int("state", state);
}

void GroupSnapshot::generate_test_instances(std::list<GroupSnapshot *> &o) {
  o.push_back(new GroupSnapshot("10152ae8944a", "groupsnapshot1", GROUP_SNAPSHOT_STATE_INCOMPLETE));
  o.push_back(new GroupSnapshot("1018643c9869", "groupsnapshot2", GROUP_SNAPSHOT_STATE_COMPLETE));
}
void TrashImageSpec::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  encode(source, bl);
  encode(name, bl);
  encode(deletion_time, bl);
  encode(deferment_end_time, bl);
  encode(state, bl);
  ENCODE_FINISH(bl);
}

void TrashImageSpec::decode(bufferlist::const_iterator &it) {
  DECODE_START(2, it);
  decode(source, it);
  decode(name, it);
  decode(deletion_time, it);
  decode(deferment_end_time, it);
  if (struct_v >= 2) {
    decode(state, it);
  }
  DECODE_FINISH(it);
}

void TrashImageSpec::dump(Formatter *f) const {
  f->dump_stream("source") << source;
  f->dump_string("name", name);
  f->dump_unsigned("deletion_time", deletion_time);
  f->dump_unsigned("deferment_end_time", deferment_end_time);
}

void MirrorImageMap::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  encode(instance_id, bl);
  encode(mapped_time, bl);
  encode(data, bl);
  ENCODE_FINISH(bl);
}

void MirrorImageMap::decode(bufferlist::const_iterator &it) {
  DECODE_START(1, it);
  decode(instance_id, it);
  decode(mapped_time, it);
  decode(data, it);
  DECODE_FINISH(it);
}

void MirrorImageMap::dump(Formatter *f) const {
  f->dump_string("instance_id", instance_id);
  f->dump_stream("mapped_time") << mapped_time;

  std::stringstream data_ss;
  data.hexdump(data_ss);
  f->dump_string("data", data_ss.str());
}

void MirrorImageMap::generate_test_instances(
  std::list<MirrorImageMap*> &o) {
  bufferlist data;
  data.append(std::string(128, '1'));

  o.push_back(new MirrorImageMap("uuid-123", utime_t(), data));
  o.push_back(new MirrorImageMap("uuid-abc", utime_t(), data));
}

bool MirrorImageMap::operator==(const MirrorImageMap &rhs) const {
  return instance_id == rhs.instance_id && mapped_time == rhs.mapped_time &&
    data.contents_equal(rhs.data);
}

bool MirrorImageMap::operator<(const MirrorImageMap &rhs) const {
  return instance_id < rhs.instance_id ||
        (instance_id == rhs.instance_id && mapped_time < rhs.mapped_time);
}

std::ostream& operator<<(std::ostream& os,
                         const MirrorImageMap &image_map) {
  return os << "[" << "instance_id=" << image_map.instance_id << ", mapped_time="
            << image_map.mapped_time << "]";
}

std::ostream& operator<<(std::ostream& os,
                         const MigrationHeaderType& type) {
  switch (type) {
  case MIGRATION_HEADER_TYPE_SRC:
    os << "source";
    break;
  case MIGRATION_HEADER_TYPE_DST:
    os << "destination";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os,
                         const MigrationState& migration_state) {
  switch (migration_state) {
  case MIGRATION_STATE_ERROR:
    os << "error";
    break;
  case MIGRATION_STATE_PREPARING:
    os << "preparing";
    break;
  case MIGRATION_STATE_PREPARED:
    os << "prepared";
    break;
  case MIGRATION_STATE_EXECUTING:
    os << "executing";
    break;
  case MIGRATION_STATE_EXECUTED:
    os << "executed";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(migration_state) << ")";
    break;
  }
  return os;
}

void MigrationSpec::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(header_type, bl);
  encode(pool_id, bl);
  encode(pool_namespace, bl);
  encode(image_name, bl);
  encode(image_id, bl);
  encode(snap_seqs, bl);
  encode(overlap, bl);
  encode(flatten, bl);
  encode(mirroring, bl);
  encode(state, bl);
  encode(state_description, bl);
  ENCODE_FINISH(bl);
}

void MigrationSpec::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  decode(header_type, bl);
  decode(pool_id, bl);
  decode(pool_namespace, bl);
  decode(image_name, bl);
  decode(image_id, bl);
  decode(snap_seqs, bl);
  decode(overlap, bl);
  decode(flatten, bl);
  decode(mirroring, bl);
  decode(state, bl);
  decode(state_description, bl);
  DECODE_FINISH(bl);
}

std::ostream& operator<<(std::ostream& os,
                         const std::map<uint64_t, uint64_t>& snap_seqs) {
  os << "{";
  size_t count = 0;
  for (auto &it : snap_seqs) {
    os << (count++ > 0 ? ", " : "") << "(" << it.first << ", " << it.second
       << ")";
  }
  os << "}";
  return os;
}

void MigrationSpec::dump(Formatter *f) const {
  f->dump_stream("header_type") << header_type;
  f->dump_int("pool_id", pool_id);
  f->dump_string("pool_namespace", pool_namespace);
  f->dump_string("image_name", image_name);
  f->dump_string("image_id", image_id);
  f->dump_stream("snap_seqs") << snap_seqs;
  f->dump_unsigned("overlap", overlap);
  f->dump_bool("mirroring", mirroring);
}

void MigrationSpec::generate_test_instances(std::list<MigrationSpec*> &o) {
  o.push_back(new MigrationSpec());
  o.push_back(new MigrationSpec(MIGRATION_HEADER_TYPE_SRC, 1, "ns",
                                "image_name", "image_id", {{1, 2}}, 123, true,
                                true, MIGRATION_STATE_PREPARED, "description"));
}

std::ostream& operator<<(std::ostream& os,
                         const MigrationSpec& migration_spec) {
  os << "["
     << "header_type=" << migration_spec.header_type << ", "
     << "pool_id=" << migration_spec.pool_id << ", "
     << "pool_namespace=" << migration_spec.pool_namespace << ", "
     << "image_name=" << migration_spec.image_name << ", "
     << "image_id=" << migration_spec.image_id << ", "
     << "snap_seqs=" << migration_spec.snap_seqs << ", "
     << "overlap=" << migration_spec.overlap << ", "
     << "flatten=" << migration_spec.flatten << ", "
     << "mirroring=" << migration_spec.mirroring << ", "
     << "state=" << migration_spec.state << ", "
     << "state_description=" << migration_spec.state_description << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const AssertSnapcSeqState& state) {
  switch (state) {
  case ASSERT_SNAPC_SEQ_GT_SNAPSET_SEQ:
    os << "gt";
    break;
  case ASSERT_SNAPC_SEQ_LE_SNAPSET_SEQ:
    os << "le";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

void sanitize_entity_inst(entity_inst_t* entity_inst) {
  // make all addrs of type ANY because the type isn't what uniquely
  // identifies them and clients and on-disk formats can be encoded
  // with different backwards compatibility settings.
  entity_inst->addr.set_type(entity_addr_t::TYPE_ANY);
}

} // namespace rbd
} // namespace cls
