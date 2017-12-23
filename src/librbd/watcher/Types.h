// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_TYPES_H
#define CEPH_LIBRBD_WATCHER_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"

namespace ceph { class Formatter; }

namespace librbd {

class Watcher;

namespace watcher {

struct ClientId {
  uint64_t gid;
  uint64_t handle;

  ClientId() : gid(0), handle(0) {}
  ClientId(uint64_t gid, uint64_t handle) : gid(gid), handle(handle) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  inline bool is_valid() const {
    return (*this != ClientId());
  }

  inline bool operator==(const ClientId &rhs) const {
    return (gid == rhs.gid && handle == rhs.handle);
  }
  inline bool operator!=(const ClientId &rhs) const {
    return !(*this == rhs);
  }
  inline bool operator<(const ClientId &rhs) const {
    if (gid != rhs.gid) {
      return gid < rhs.gid;
    } else {
      return handle < rhs.handle;
    }
  }
};

struct NotifyResponse {
  std::map<ClientId, bufferlist> acks;
  std::vector<ClientId> timeouts;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
};

template <typename ImageCtxT>
struct Traits {
  typedef librbd::Watcher Watcher;
};

std::ostream &operator<<(std::ostream &out,
                         const ClientId &client);

WRITE_CLASS_ENCODER(ClientId);
WRITE_CLASS_ENCODER(NotifyResponse);

} // namespace watcher
} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_TYPES_H
