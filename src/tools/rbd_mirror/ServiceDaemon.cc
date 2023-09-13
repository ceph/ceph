// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/ServiceDaemon.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Timer.h"
#include "tools/rbd_mirror/Threads.h"
#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ServiceDaemon: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {

namespace {

const std::string RBD_MIRROR_AUTH_ID_PREFIX("rbd-mirror.");

struct AttributeDumpVisitor : public boost::static_visitor<void> {
  ceph::Formatter *f;
  const std::string& name;

  AttributeDumpVisitor(ceph::Formatter *f, const std::string& name)
    : f(f), name(name) {
  }

  void operator()(bool val) const {
    f->dump_bool(name.c_str(), val);
  }
  void operator()(uint64_t val) const {
    f->dump_unsigned(name.c_str(), val);
  }
  void operator()(const std::string& val) const {
    f->dump_string(name.c_str(), val);
  }
};

} // anonymous namespace

using namespace service_daemon;

template <typename I>
ServiceDaemon<I>::ServiceDaemon(CephContext *cct, RadosRef rados,
                                Threads<I>* threads)
  : m_cct(cct), m_rados(rados), m_threads(threads) {
  dout(20) << dendl;
}

template <typename I>
ServiceDaemon<I>::~ServiceDaemon() {
  dout(20) << dendl;
  std::lock_guard timer_locker{m_threads->timer_lock};
  if (m_timer_ctx != nullptr) {
    m_threads->timer->cancel_event(m_timer_ctx);
    update_status();
  }
}

template <typename I>
int ServiceDaemon<I>::init() {
  dout(20) << dendl;

  std::string id = m_cct->_conf->name.get_id();
  if (id.find(RBD_MIRROR_AUTH_ID_PREFIX) == 0) {
    id = id.substr(RBD_MIRROR_AUTH_ID_PREFIX.size());
  }

  std::string instance_id = stringify(m_rados->get_instance_id());
  std::map<std::string, std::string> service_metadata = {
    {"id", id}, {"instance_id", instance_id}};
  int r = m_rados->service_daemon_register("rbd-mirror", instance_id,
                                           service_metadata);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
void ServiceDaemon<I>::add_pool(int64_t pool_id, const std::string& pool_name) {
  dout(20) << "pool_id=" << pool_id << ", pool_name=" << pool_name << dendl;

  {
    std::lock_guard locker{m_lock};
    m_pools.insert({pool_id, {pool_name}});
  }
  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::remove_pool(int64_t pool_id) {
  dout(20) << "pool_id=" << pool_id << dendl;
  {
    std::lock_guard locker{m_lock};
    m_pools.erase(pool_id);
  }
  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::add_namespace(int64_t pool_id,
                                     const std::string& namespace_name) {
  dout(20) << "pool_id=" << pool_id << ", namespace=" << namespace_name
           << dendl;

  std::lock_guard locker{m_lock};
  auto pool_it = m_pools.find(pool_id);
  if (pool_it == m_pools.end()) {
    return;
  }
  pool_it->second.ns_attributes[namespace_name];

  // don't schedule update status as the namespace attributes are empty yet
}

template <typename I>
void ServiceDaemon<I>::remove_namespace(int64_t pool_id,
                                        const std::string& namespace_name) {
  dout(20) << "pool_id=" << pool_id << ", namespace=" << namespace_name
           << dendl;
  {
    std::lock_guard locker{m_lock};
    auto pool_it = m_pools.find(pool_id);
    if (pool_it == m_pools.end()) {
      return;
    }
    pool_it->second.ns_attributes.erase(namespace_name);
  }
  schedule_update_status();
}

template <typename I>
uint64_t ServiceDaemon<I>::add_or_update_callout(int64_t pool_id,
                                                 uint64_t callout_id,
                                                 CalloutLevel callout_level,
                                                 const std::string& text) {
  dout(20) << "pool_id=" << pool_id << ", "
           << "callout_id=" << callout_id << ", "
           << "callout_level=" << callout_level << ", "
           << "text=" << text << dendl;

  {
    std::lock_guard locker{m_lock};
    auto pool_it = m_pools.find(pool_id);
    if (pool_it == m_pools.end()) {
      return CALLOUT_ID_NONE;
    }

    if (callout_id == CALLOUT_ID_NONE) {
      callout_id = ++m_callout_id;
    }
    pool_it->second.callouts[callout_id] = {callout_level, text};
  }

  schedule_update_status();
  return callout_id;
}

template <typename I>
void ServiceDaemon<I>::remove_callout(int64_t pool_id, uint64_t callout_id) {
  dout(20) << "pool_id=" << pool_id << ", "
           << "callout_id=" << callout_id << dendl;

  {
    std::lock_guard locker{m_lock};
    auto pool_it = m_pools.find(pool_id);
    if (pool_it == m_pools.end()) {
      return;
    }
    pool_it->second.callouts.erase(callout_id);
  }

  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::add_or_update_attribute(int64_t pool_id,
                                               const std::string& key,
                                               const AttributeValue& value) {
  dout(20) << "pool_id=" << pool_id << ", "
           << "key=" << key << ", "
           << "value=" << value << dendl;

  {
    std::lock_guard locker{m_lock};
    auto pool_it = m_pools.find(pool_id);
    if (pool_it == m_pools.end()) {
      return;
    }
    pool_it->second.attributes[key] = value;
  }

  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::add_or_update_namespace_attribute(
    int64_t pool_id, const std::string& namespace_name, const std::string& key,
    const AttributeValue& value) {
  if (namespace_name.empty()) {
    add_or_update_attribute(pool_id, key, value);
    return;
  }

  dout(20) << "pool_id=" << pool_id << ", "
           << "namespace=" << namespace_name << ", "
           << "key=" << key << ", "
           << "value=" << value << dendl;

  {
    std::lock_guard locker{m_lock};
    auto pool_it = m_pools.find(pool_id);
    if (pool_it == m_pools.end()) {
      return;
    }

    auto ns_it = pool_it->second.ns_attributes.find(namespace_name);
    if (ns_it == pool_it->second.ns_attributes.end()) {
      return;
    }

    ns_it->second[key] = value;
  }

  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::remove_attribute(int64_t pool_id,
                                        const std::string& key) {
  dout(20) << "pool_id=" << pool_id << ", "
           << "key=" << key << dendl;

  {
    std::lock_guard locker{m_lock};
    auto pool_it = m_pools.find(pool_id);
    if (pool_it == m_pools.end()) {
      return;
    }
    pool_it->second.attributes.erase(key);
  }

  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::schedule_update_status() {
  std::lock_guard timer_locker{m_threads->timer_lock};
  if (m_timer_ctx != nullptr) {
    return;
  }

  dout(20) << dendl;

  m_timer_ctx = new LambdaContext([this](int) {
      m_timer_ctx = nullptr;
      update_status();
    });
  m_threads->timer->add_event_after(1, m_timer_ctx);
}

template <typename I>
void ServiceDaemon<I>::update_status() {
  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));

  ceph::JSONFormatter f;
  {
    std::lock_guard locker{m_lock};
    f.open_object_section("pools");
    for (auto& pool_pair : m_pools) {
      f.open_object_section(stringify(pool_pair.first).c_str());
      f.dump_string("name", pool_pair.second.name);
      f.open_object_section("callouts");
      for (auto& callout : pool_pair.second.callouts) {
        f.open_object_section(stringify(callout.first).c_str());
        f.dump_string("level", stringify(callout.second.level).c_str());
        f.dump_string("text", callout.second.text.c_str());
        f.close_section();
      }
      f.close_section(); // callouts

      for (auto& attribute : pool_pair.second.attributes) {
        AttributeDumpVisitor attribute_dump_visitor(&f, attribute.first);
        boost::apply_visitor(attribute_dump_visitor, attribute.second);
      }

      if (!pool_pair.second.ns_attributes.empty()) {
        f.open_object_section("namespaces");
        for (auto& [ns, attributes] : pool_pair.second.ns_attributes) {
          f.open_object_section(ns.c_str());
          for (auto& [key, value] : attributes) {
            AttributeDumpVisitor attribute_dump_visitor(&f, key);
            boost::apply_visitor(attribute_dump_visitor, value);
          }
          f.close_section(); // namespace
        }
        f.close_section(); // namespaces
      }
      f.close_section(); // pool
    }
    f.close_section(); // pools
  }

  std::stringstream ss;
  f.flush(ss);

  dout(20) << ss.str() << dendl;

  int r = m_rados->service_daemon_update_status({{"json", ss.str()}});
  if (r < 0) {
    derr << "failed to update service daemon status: " << cpp_strerror(r)
         << dendl;
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ServiceDaemon<librbd::ImageCtx>;
