// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_TYPES_H
#define CEPH_RBD_MIRROR_TYPES_H

#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

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

struct ImageId {
  std::string global_id;
  std::string id;

  explicit ImageId(const std::string &global_id) : global_id(global_id) {
  }
  ImageId(const std::string &global_id, const std::string &id)
    : global_id(global_id), id(id) {
  }

  inline bool operator==(const ImageId &rhs) const {
    return (global_id == rhs.global_id && id == rhs.id);
  }
  inline bool operator<(const ImageId &rhs) const {
    return global_id < rhs.global_id;
  }
};

std::ostream &operator<<(std::ostream &, const ImageId &image_id);

typedef std::set<ImageId> ImageIds;

struct LocalPoolMeta {
  LocalPoolMeta() {}
  LocalPoolMeta(const std::string& mirror_uuid)
    : mirror_uuid(mirror_uuid) {
  }

  std::string mirror_uuid;
};

std::ostream& operator<<(std::ostream& lhs,
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

std::ostream& operator<<(std::ostream& lhs,
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
std::ostream& operator<<(std::ostream& lhs, const Peer<I>& peer) {
  return lhs << peer.remote_pool_meta;
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

std::ostream& operator<<(std::ostream& lhs, const PeerSpec &peer);

} // namespace mirror
} // namespace rbd


#endif // CEPH_RBD_MIRROR_TYPES_H
