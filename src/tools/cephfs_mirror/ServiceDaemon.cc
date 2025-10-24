// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "ServiceDaemon.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/JSONFormatter.h"
#include "common/Timer.h"
#include "include/Context.h"
#include "include/stringify.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::ServiceDaemon: " << this << " " \
                           << __func__

namespace cephfs {
namespace mirror {

namespace {

struct AttributeDumpVisitor {
  ceph::Formatter *f;
  std::string name;

  AttributeDumpVisitor(ceph::Formatter *f, std::string_view name)
    : f(f), name(name) {
  }

  void operator()(bool val) const {
    f->dump_bool(name.c_str(), val);
  }
  void operator()(uint64_t val) const {
    f->dump_unsigned(name.c_str(), val);
  }
  void operator()(const std::string &val) const {
    f->dump_string(name.c_str(), val);
  }
};

} // anonymous namespace

ServiceDaemon::ServiceDaemon(CephContext *cct, RadosRef rados, Messenger* msgr, MonClient* monc)
  : m_cct(cct),
    m_rados(rados),
    m_timer(new SafeTimer(cct, m_timer_lock, true)),
    h_timer(new SafeTimer(cct, h_timer_lock, true)),
    mgrc(cct, msgr, &monc->monmap) {
  m_timer->init();
  h_timer->init();
}

ServiceDaemon::~ServiceDaemon() {
  dout(10) << dendl;
  {
    std::scoped_lock timer_lock(m_timer_lock);
    if (m_timer_ctx != nullptr) {
      dout(5) << ": canceling timer task=" << m_timer_ctx << dendl;
      m_timer->cancel_event(m_timer_ctx);
    }
    std::scoped_lock h_lock(h_timer_lock);
    if (h_timer_ctx != nullptr) {
      dout(5) << ": canceling health timer task=" << h_timer_ctx << dendl;
      h_timer->cancel_event(h_timer_ctx);
    }
    m_timer->shutdown();
    h_timer->shutdown();
  }
  delete m_timer;
  delete h_timer;
}

int ServiceDaemon::init() {
  dout(20) << dendl;

  std::string id = m_cct->_conf->name.get_id();
  if (id.find(CEPHFS_MIRROR_AUTH_ID_PREFIX) == 0) {
    id = id.substr(CEPHFS_MIRROR_AUTH_ID_PREFIX.size());
  }
  std::string instance_id = stringify(m_rados->get_instance_id());

  std::map<std::string, std::string> service_metadata = {{"id", id},
                                                         {"instance_id", instance_id}};
  int r = m_rados->service_daemon_register("cephfs-mirror", instance_id,
                                           service_metadata);
  if (r < 0) {
    return r;
  }
  mgrc.init();
  return 0;
}

void ServiceDaemon::add_filesystem(fs_cluster_id_t fscid, std::string_view fs_name) {
  dout(10) << ": fscid=" << fscid << ", fs_name=" << fs_name << dendl;

  {
    std::scoped_lock locker(m_lock);
    m_filesystems.emplace(fscid, Filesystem(fs_name));
  }
  schedule_update_status();
}

void ServiceDaemon::remove_filesystem(fs_cluster_id_t fscid) {
  dout(10) << ": fscid=" << fscid << dendl;

  {
    std::scoped_lock locker(m_lock);
    m_filesystems.erase(fscid);
  }
  schedule_update_status();
}

void ServiceDaemon::add_peer(fs_cluster_id_t fscid, const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }
    fs_it->second.peer_attributes.emplace(peer, Attributes{});
  }
  schedule_update_status();
}

void ServiceDaemon::remove_peer(fs_cluster_id_t fscid, const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }
    fs_it->second.peer_attributes.erase(peer);
  }
  schedule_update_status();
}

void ServiceDaemon::add_or_update_fs_attribute(fs_cluster_id_t fscid, std::string_view key,
                                               AttributeValue value) {
  dout(10) << ": fscid=" << fscid << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }

    fs_it->second.fs_attributes[std::string(key)] = value;
    if (key == SERVICE_DAEMON_MIRROR_ENABLE_FAILED_KEY) {
      m_health_metrics.emplace_back(daemon_metric::CEPHFS_MIRROR_FAILURE, 1, ceph_clock_now());
    }
  }
  schedule_update_status();
}

void ServiceDaemon::add_or_update_peer_attribute(fs_cluster_id_t fscid, const Peer &peer,
                                                 std::string_view key, AttributeValue value) {
  dout(10) << ": fscid=" << fscid << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto fs_it = m_filesystems.find(fscid);
    if (fs_it == m_filesystems.end()) {
      return;
    }

    auto peer_it = fs_it->second.peer_attributes.find(peer);
    if (peer_it == fs_it->second.peer_attributes.end()) {
      return;
    }

    peer_it->second[std::string(key)] = value;
    if (key == SERVICE_DAEMON_FAILED_DIR_COUNT_KEY) {
      m_health_metrics.emplace_back(daemon_metric::CEPHFS_MIRROR_SNAP_SYNC_FAILURE, 1, ceph_clock_now());
    }
  }
  schedule_update_status();
}

void ServiceDaemon::schedule_update_status() {
  dout(10) << dendl;

  std::scoped_lock timer_lock(m_timer_lock);
  if (m_timer_ctx != nullptr) {
    return;
  }

  m_timer_ctx = new LambdaContext([this] {
                                    m_timer_ctx = nullptr;
                                    update_status();
                                  });
  m_timer->add_event_after(1, m_timer_ctx);
}

void ServiceDaemon::update_status() {
  dout(20) << ": " << m_filesystems.size() << " filesystem(s)" << dendl;

  ceph::JSONFormatter f;
  {
    std::scoped_lock locker(m_lock);
    f.open_object_section("filesystems");
    for (auto &[fscid, filesystem] : m_filesystems) {
      f.open_object_section(stringify(fscid).c_str());
      f.dump_string("name", filesystem.fs_name);
      for (auto &[attr_name, attr_value] : filesystem.fs_attributes) {
            AttributeDumpVisitor visitor(&f, attr_name);
            std::visit(visitor, attr_value);
      }
      f.open_object_section("peers");
      for (auto &[peer, attributes] : filesystem.peer_attributes) {
        f.open_object_section(peer.uuid);
        f.dump_object("remote", peer.remote);
        f.open_object_section("stats");
        for (auto &[attr_name, attr_value] : attributes) {
            AttributeDumpVisitor visitor(&f, attr_name);
            std::visit(visitor, attr_value);
        }
        f.close_section(); // stats
        f.close_section(); // peer.uuid
      }
      f.close_section(); // peers
      f.close_section(); // fscid
    }
    f.close_section(); // filesystems
  }

  std::stringstream ss;
  f.flush(ss);

  int r = m_rados->service_daemon_update_status({{"status_json", ss.str()}});
  if (r < 0) {
    derr << ": failed to update service daemon status: " << cpp_strerror(r)
         << dendl;
  }
}

void ServiceDaemon::update_mirror_health(std::vector<DaemonHealthMetric>& health_metrics) {
  dout(20) << dendl;
  int r = m_rados->service_daemon_update_health(std::move(health_metrics));
  if (r < 0) {
    derr << ": failed to update mirror daemon health: " << cpp_strerror(r)
         << dendl;
  }
}

  /*std::vector<DaemonHealthMetric> ServiceDaemon::get_health_metrics() {

  return m_health_metrics;
  }*/

void ServiceDaemon::schedule_health_tick()
{
  if (h_timer_ctx != nullptr) {
    return;
  }
  h_timer_ctx = new LambdaContext([this](int) {
    h_timer_ctx = nullptr;
    health_tick();
  });
  h_timer->add_event_after(m_cct->_conf.get_val<std::chrono::seconds>("cephfs_mirror_health_tick_interval"),
			   h_timer_ctx);
}

void ServiceDaemon::health_tick()
{
  dout(20) << dendl;
  int r = m_rados->service_daemon_update_health(std::move(m_health_metrics));
  if (r < 0) {
    derr << ": failed to update mirror daemon health: " << cpp_strerror(r)
         << dendl;
  }

  schedule_health_tick();
}

} // namespace mirror
} // namespace cephfs
