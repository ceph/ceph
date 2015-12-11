// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_TYPES_H
#define CEPH_RBD_MIRROR_TYPES_H

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "include/rbd/librbd.hpp"

namespace rbd {
namespace mirror {

typedef shared_ptr<librados::Rados> RadosRef;
typedef shared_ptr<librados::IoCtx> IoCtxRef;
typedef shared_ptr<librbd::Image> ImageRef;

struct peer_t {
  peer_t() = default;
  peer_t(const std::string &uuid, const std::string &cluster_name,
	 const std::string &client_name)
    : cluster_uuid(uuid), cluster_name(cluster_name), client_name(client_name)
  {
  }
  peer_t(const librbd::mirror_peer_t &peer) :
    cluster_uuid(peer.cluster_uuid),
    cluster_name(peer.cluster_name),
    client_name(peer.client_name)
  {
  }
  std::string cluster_uuid;
  std::string cluster_name;
  std::string client_name;
  bool operator<(const peer_t &rhs) const {
    return this->cluster_uuid < rhs.cluster_uuid;
  }
  bool operator()(const peer_t &lhs, const peer_t &rhs) const {
    return lhs.cluster_uuid < rhs.cluster_uuid;
  }
  bool operator==(const peer_t &rhs) const {
    return cluster_uuid == rhs.cluster_uuid;
  }
};

} // namespace mirror
} // namespace rbd

std::ostream& operator<<(std::ostream& lhs, const rbd::mirror::peer_t &peer);

#endif // CEPH_RBD_MIRROR_TYPES_H
