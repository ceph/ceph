// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_TYPES_H
#define CEPH_RBD_MIRROR_TYPES_H

#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

class Context;

namespace cls { namespace rbd { struct MirrorSnapshotNamespace; } }

namespace rbd {
namespace mirror {

template <typename> struct MirrorStatusUpdater;

// Performance counters
enum {
  l_rbd_mirror_journal_first = 27000,
  l_rbd_mirror_journal_entries,
  l_rbd_mirror_journal_replay_bytes,
  l_rbd_mirror_journal_replay_latency,
  l_rbd_mirror_journal_last,
  l_rbd_mirror_snapshot_first,
  l_rbd_mirror_snapshot_snapshots,
  l_rbd_mirror_snapshot_sync_time,
  l_rbd_mirror_snapshot_sync_bytes,
  // per-image only counters below
  l_rbd_mirror_snapshot_remote_timestamp,
  l_rbd_mirror_snapshot_local_timestamp,
  l_rbd_mirror_snapshot_last_sync_time,
  l_rbd_mirror_snapshot_last_sync_bytes,
  l_rbd_mirror_snapshot_last,
};

typedef std::shared_ptr<librados::Rados> RadosRef;
typedef std::shared_ptr<librados::IoCtx> IoCtxRef;
typedef std::shared_ptr<librbd::Image> ImageRef;

enum MirrorEntityType {
  MIRROR_ENTITY_TYPE_IMAGE = 0,
  MIRROR_ENTITY_TYPE_GROUP = 1,
};

std::ostream &operator<<(std::ostream &os, const MirrorEntityType &type);

struct MirrorEntity {
  MirrorEntityType type = MIRROR_ENTITY_TYPE_IMAGE;
  std::string global_id;
  size_t count = 1;

  MirrorEntity(MirrorEntityType type, const std::string &global_id, size_t count)
    : type(type), global_id(global_id), count(count) {
  }

  inline bool operator==(const MirrorEntity &rhs) const {
    return type == rhs.type && global_id == rhs.global_id && count == rhs.count;
  }
  inline bool operator<(const MirrorEntity &rhs) const {
    if (type != rhs.type) {
      return type < rhs.type;
    }
    return global_id < rhs.global_id;
  }
};

std::ostream &operator<<(std::ostream &, const MirrorEntity &entity);

typedef std::set<MirrorEntity> MirrorEntities;

struct LocalPoolMeta {
  LocalPoolMeta() {}
  LocalPoolMeta(const std::string& mirror_uuid)
    : mirror_uuid(mirror_uuid) {
  }

  std::string mirror_uuid;
};

std::ostream& operator<<(std::ostream& os,
                         const LocalPoolMeta& local_pool_meta);

struct RemotePoolMeta {
  RemotePoolMeta() {}
  RemotePoolMeta(const std::string& mirror_uuid,
                 const std::string& mirror_peer_uuid)
    : mirror_uuid(mirror_uuid),
      mirror_peer_uuid(mirror_peer_uuid) {
  }

  std::string mirror_uuid;
  std::string mirror_peer_uuid;
};

std::ostream& operator<<(std::ostream& os,
                         const RemotePoolMeta& remote_pool_meta);

template <typename I>
struct Peer {
  std::string uuid;
  mutable librados::IoCtx io_ctx;
  RemotePoolMeta remote_pool_meta;
  MirrorStatusUpdater<I>* mirror_status_updater = nullptr;

  Peer() {
  }
  Peer(const std::string& uuid,
       librados::IoCtx& io_ctx,
       const RemotePoolMeta& remote_pool_meta,
       MirrorStatusUpdater<I>* mirror_status_updater)
    : io_ctx(io_ctx),
      remote_pool_meta(remote_pool_meta),
      mirror_status_updater(mirror_status_updater) {
  }

  inline bool operator<(const Peer &rhs) const {
    return uuid < rhs.uuid;
  }
};

template <typename I>
std::ostream& operator<<(std::ostream& os, const Peer<I>& peer) {
  return os << peer.remote_pool_meta;
}

struct PeerSpec {
  PeerSpec() = default;
  PeerSpec(const std::string &uuid, const std::string &cluster_name,
	   const std::string &client_name)
    : uuid(uuid), cluster_name(cluster_name), client_name(client_name)
  {
  }
  PeerSpec(const librbd::mirror_peer_site_t &peer) :
    uuid(peer.uuid),
    cluster_name(peer.site_name),
    client_name(peer.client_name)
  {
  }

  std::string uuid;
  std::string cluster_name;
  std::string client_name;

  /// optional config properties
  std::string mon_host;
  std::string key;

  bool operator==(const PeerSpec& rhs) const {
    return (uuid == rhs.uuid &&
            cluster_name == rhs.cluster_name &&
            client_name == rhs.client_name &&
            mon_host == rhs.mon_host &&
            key == rhs.key);
  }
  bool operator<(const PeerSpec& rhs) const {
    if (uuid != rhs.uuid) {
      return uuid < rhs.uuid;
    } else if (cluster_name != rhs.cluster_name) {
      return cluster_name < rhs.cluster_name;
    } else if (client_name != rhs.client_name) {
      return client_name < rhs.client_name;
    } else if (mon_host < rhs.mon_host) {
      return mon_host < rhs.mon_host;
    } else {
      return key < rhs.key;
    }
  }
};

std::ostream& operator<<(std::ostream& os, const PeerSpec &peer);

struct GroupCtx {
  struct Listener {
    virtual ~Listener() {
    }

    virtual void stop() = 0;

    virtual void notify_group_snap_image_complete(
        int64_t local_pool_id,
        const std::string &local_image_id,
        const std::string &remote_group_snap_id,
        uint64_t local_snap_id) = 0;
  };

  std::string name;
  std::string group_id;
  std::string global_group_id;
  bool primary = false;
  mutable librados::IoCtx io_ctx;
  Listener *listener = nullptr;

  GroupCtx() {
  }

  GroupCtx(const std::string &name, const std::string &group_id,
           const std::string &global_group_id, bool primary,
           librados::IoCtx &io_ctx_)
    : name(name), group_id(group_id), global_group_id(global_group_id),
      primary(primary) {
    io_ctx.dup(io_ctx_);
  }
};

std::ostream& operator<<(std::ostream& lhs, const GroupCtx &group_ctx);

} // namespace mirror
} // namespace rbd


#endif // CEPH_RBD_MIRROR_TYPES_H
