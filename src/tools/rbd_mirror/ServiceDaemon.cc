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
  : m_cct(cct), m_rados(rados), m_threads(threads),
    m_lock("rbd::mirror::ServiceDaemon") {
  dout(20) << dendl;
}

template <typename I>
ServiceDaemon<I>::~ServiceDaemon() {
  dout(20) << dendl;
  Mutex::Locker timer_locker(m_threads->timer_lock);
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
    Mutex::Locker locker(m_lock);
    m_pools.insert({pool_id, {pool_name}});
  }
  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::remove_pool(int64_t pool_id) {
  dout(20) << "pool_id=" << pool_id << dendl;
  {
    Mutex::Locker locker(m_lock);
    m_pools.erase(pool_id);
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
    Mutex::Locker locker(m_lock);
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
    Mutex::Locker locker(m_lock);
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
    Mutex::Locker locker(m_lock);
    auto pool_it = m_pools.find(pool_id);
    if (pool_it == m_pools.end()) {
      return;
    }
    pool_it->second.attributes[key] = value;
  }

  schedule_update_status();
}

template <typename I>
void ServiceDaemon<I>::remove_attribute(int64_t pool_id,
                                        const std::string& key) {
  dout(20) << "pool_id=" << pool_id << ", "
           << "key=" << key << dendl;

  {
    Mutex::Locker locker(m_lock);
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
  Mutex::Locker timer_locker(m_threads->timer_lock);
  if (m_timer_ctx != nullptr) {
    return;
  }

  m_timer_ctx = new FunctionContext([this](int) {
      m_timer_ctx = nullptr;
      update_status();
    });
  m_threads->timer->add_event_after(1, m_timer_ctx);
}

template <typename I>
void ServiceDaemon<I>::update_status() {
  dout(20) << dendl;
  assert(m_threads->timer_lock.is_locked());

  ceph::JSONFormatter f;
  {
    Mutex::Locker locker(m_lock);
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
      f.close_section(); // pool
    }
    f.close_section(); // pools
  }

  std::stringstream ss;
  f.flush(ss);

  int r = m_rados->service_daemon_update_status({{"json", ss.str()}});
  if (r < 0) {
    derr << "failed to update service daemon status: " << cpp_strerror(r)
         << dendl;
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ServiceDaemon<librbd::ImageCtx>;
