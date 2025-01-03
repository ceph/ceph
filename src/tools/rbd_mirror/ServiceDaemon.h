// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_SERVICE_DAEMON_H
#define CEPH_RBD_MIRROR_SERVICE_DAEMON_H

#include "common/ceph_mutex.h"
#include "include/common_fwd.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/service_daemon/Types.h"
#include <map>
#include <string>

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class ServiceDaemon {
public:
  ServiceDaemon(CephContext *cct, RadosRef rados, Threads<ImageCtxT>* threads);
  ~ServiceDaemon();

  int init();

  void add_pool(int64_t pool_id, const std::string& pool_name);
  void remove_pool(int64_t pool_id);

  void add_namespace(int64_t pool_id, const std::string& namespace_name);
  void remove_namespace(int64_t pool_id, const std::string& namespace_name);

  uint64_t add_or_update_callout(int64_t pool_id, uint64_t callout_id,
                                 service_daemon::CalloutLevel callout_level,
                                 const std::string& text);
  void remove_callout(int64_t pool_id, uint64_t callout_id);

  void add_or_update_attribute(int64_t pool_id, const std::string& key,
                               const service_daemon::AttributeValue& value);
  void add_or_update_namespace_attribute(
      int64_t pool_id, const std::string& namespace_name,
      const std::string& key, const service_daemon::AttributeValue& value);
  void remove_attribute(int64_t pool_id, const std::string& key);

private:
  struct Callout {
    service_daemon::CalloutLevel level;
    std::string text;

    Callout() : level(service_daemon::CALLOUT_LEVEL_INFO) {
    }
    Callout(service_daemon::CalloutLevel level, const std::string& text)
      : level(level), text(text) {
    }
  };
  typedef std::map<uint64_t, Callout> Callouts;
  typedef std::map<std::string, service_daemon::AttributeValue> Attributes;
  typedef std::map<std::string, Attributes> NamespaceAttributes;

  struct Pool {
    std::string name;
    Callouts callouts;
    Attributes attributes;
    NamespaceAttributes ns_attributes;

    Pool(const std::string& name) : name(name) {
    }
  };

  typedef std::map<int64_t, Pool> Pools;

  CephContext *m_cct;
  RadosRef m_rados;
  Threads<ImageCtxT>* m_threads;

  ceph::mutex m_lock = ceph::make_mutex("rbd::mirror::ServiceDaemon");
  Pools m_pools;
  uint64_t m_callout_id = service_daemon::CALLOUT_ID_NONE;

  Context* m_timer_ctx = nullptr;

  void schedule_update_status();
  void update_status();
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ServiceDaemon<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_SERVICE_DAEMON_H
