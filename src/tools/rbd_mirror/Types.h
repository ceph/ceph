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

// Performance counters
enum {
  l_rbd_mirror_first = 27000,
  l_rbd_mirror_replay,
  l_rbd_mirror_replay_bytes,
  l_rbd_mirror_replay_latency,
  l_rbd_mirror_last,
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

struct Peer {
  std::string peer_uuid;
  librados::IoCtx io_ctx;

  Peer() {
  }
  Peer(const std::string &peer_uuid) : peer_uuid(peer_uuid) {
  }
  Peer(const std::string &peer_uuid, librados::IoCtx& io_ctx)
    : peer_uuid(peer_uuid), io_ctx(io_ctx) {
  }

  inline bool operator<(const Peer &rhs) const {
    return peer_uuid < rhs.peer_uuid;
  }
};

typedef std::set<Peer> Peers;

struct PeerSpec {
  PeerSpec() = default;
  PeerSpec(const std::string &uuid, const std::string &cluster_name,
	   const std::string &client_name)
    : uuid(uuid), cluster_name(cluster_name), client_name(client_name)
  {
  }
  PeerSpec(const librbd::mirror_peer_t &peer) :
    uuid(peer.uuid),
    cluster_name(peer.cluster_name),
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
