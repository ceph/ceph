// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_WATCHER_H
#define CEPH_RBD_MIRROR_POOL_WATCHER_H

#include <map>
#include <memory>
#include <set>
#include <string>

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "include/rados/librados.hpp"
#include "types.h"

namespace rbd {
namespace mirror {

/**
 * Keeps track of images that have mirroring enabled within all
 * pools.
 */
class PoolWatcher {
public:
  struct ImageIds {
    std::string id;
    std::string global_id;

    ImageIds(const std::string &id, const std::string &global_id = "")
      : id(id), global_id(global_id) {
    }

    inline bool operator==(const ImageIds &rhs) const {
      return (id == rhs.id && global_id == rhs.global_id);
    }
    inline bool operator<(const ImageIds &rhs) const {
      return id < rhs.id;
    }
  };
  typedef std::map<int64_t, std::set<ImageIds> > PoolImageIds;

  PoolWatcher(RadosRef cluster, double interval_seconds,
	      Mutex &lock, Cond &cond);
  ~PoolWatcher();
  PoolWatcher(const PoolWatcher&) = delete;
  PoolWatcher& operator=(const PoolWatcher&) = delete;

  const PoolImageIds& get_images() const;
  void refresh_images(bool reschedule=true);

private:
  Mutex &m_lock;
  Cond &m_refresh_cond;
  bool m_stopping;

  RadosRef m_cluster;
  SafeTimer m_timer;
  double m_interval;

  PoolImageIds m_images;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_POOL_WATCHER_H
