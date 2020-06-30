// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "tools/rbd/MirrorDaemonServiceInfo.h"

#include <boost/scope_exit.hpp>
#include <iostream>

#include "json_spirit/json_spirit.h"

namespace rbd {

std::ostream& operator<<(std::ostream& os, MirrorHealth mirror_health) {
  switch (mirror_health) {
  case MIRROR_HEALTH_OK:
    os << "OK";
    break;
  case MIRROR_HEALTH_UNKNOWN:
    os << "UNKNOWN";
    break;
  case MIRROR_HEALTH_WARNING:
    os << "WARNING";
    break;
  case MIRROR_HEALTH_ERROR:
    os << "ERROR";
    break;
  }
  return os;
}

std::string MirrorService::get_image_description() const {
  std::string description = (!client_id.empty() ? client_id :
                                                  stringify(service_id));
  if (!hostname.empty()) {
    description += " on " + hostname;
  }
  return description;
}

void MirrorService::dump_image(
    argument_types::Format::Formatter formatter) const {
  formatter->open_object_section("daemon_service");
  formatter->dump_string("service_id", service_id);
  formatter->dump_string("instance_id", instance_id);
  formatter->dump_string("daemon_id", client_id);
  formatter->dump_string("hostname", hostname);
  formatter->close_section();
}

int MirrorDaemonServiceInfo::init() {
  int r = get_mirror_service_dump();
  if (r < 0) {
    return r;
  } else if (m_mirror_services.empty()) {
    return 0;
  }

  r = get_mirror_service_status();
  if (r < 0) {
    return r;
  }

  return 0;
}

const MirrorService* MirrorDaemonServiceInfo::get_by_service_id(
    const std::string& service_id) const {
  auto it = m_mirror_services.find(service_id);
  if (it == m_mirror_services.end()) {
    return nullptr;
  }

  return &it->second;
}

const MirrorService* MirrorDaemonServiceInfo::get_by_instance_id(
    const std::string& instance_id) const {
  auto it = m_instance_to_service_ids.find(instance_id);
  if (it == m_instance_to_service_ids.end()) {
    return nullptr;
  }

  return get_by_service_id(it->second);
}

MirrorServices MirrorDaemonServiceInfo::get_mirror_services() const {
  MirrorServices mirror_services;
  for (auto& it : m_mirror_services) {
    mirror_services.push_back(it.second);
  }
  return mirror_services;
}

int MirrorDaemonServiceInfo::get_mirror_service_dump() {
  librados::Rados rados(m_io_ctx);
  std::string cmd = R"({"prefix": "service dump", "format": "json"})";
  bufferlist in_bl;
  bufferlist out_bl;

  int r = rados.mon_command(cmd, in_bl, &out_bl, nullptr);
  if (r < 0) {
    std::cerr << "rbd: failed to query services: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  json_spirit::mValue json_root;
  if(!json_spirit::read(out_bl.to_str(), json_root)) {
    std::cerr << "rbd: invalid service dump JSON received" << std::endl;
    return -EBADMSG;
  }

  try {
    auto& services = json_root.get_obj()["services"];
    if (services.is_null()) {
      std::cerr << "rbd: missing services in service dump JSON" << std::endl;
      return -EBADMSG;
    }

    auto& service = services.get_obj()["rbd-mirror"];
    if (service.is_null()) {
      // no rbd-mirror daemons running
      return 0;
    }

    auto& daemons = service.get_obj()["daemons"];
    if (daemons.is_null()) {
      return 0;
    }

    for (auto& daemon_pair : daemons.get_obj()) {
        // rbd-mirror instances will always be integers but other objects
        // are included
      auto& service_id = daemon_pair.first;
      if (daemon_pair.second.type() != json_spirit::obj_type) {
        continue;
      }

      auto& daemon = daemon_pair.second.get_obj();
      auto& metadata_val = daemon["metadata"];
      if (metadata_val.is_null()) {
        continue;
      }
      auto& metadata = metadata_val.get_obj();

      MirrorService mirror_service{service_id};

      auto& client_id = metadata["id"];
      if (!client_id.is_null()) {
        mirror_service.client_id = client_id.get_str();
      }

      auto& ceph_version = metadata["ceph_version_short"];
      if (!ceph_version.is_null()) {
        mirror_service.ceph_version = ceph_version.get_str();
      }

      auto& hostname = metadata["hostname"];
      if (!hostname.is_null()) {
        mirror_service.hostname = hostname.get_str();
      }

      m_mirror_services[service_id] = mirror_service;
    }

  } catch (std::runtime_error&) {
    std::cerr << "rbd: unexpected service dump JSON received" << std::endl;
    return -EBADMSG;
  }

  return 0;
}

int MirrorDaemonServiceInfo::get_mirror_service_status() {
  librados::Rados rados(m_io_ctx);
  std::string cmd = R"({"prefix": "service status", "format": "json"})";
  bufferlist in_bl;
  bufferlist out_bl;

  int r = rados.mon_command(cmd, in_bl, &out_bl, nullptr);
  if (r < 0) {
    std::cerr << "rbd: failed to query service status: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  json_spirit::mValue json_root;
  if(!json_spirit::read(out_bl.to_str(), json_root)) {
    std::cerr << "rbd: invalid service status JSON received" << std::endl;
    return -EBADMSG;
  }

  bool found_leader = false;
  bool found_pool = false;

  try {
    auto& service = json_root.get_obj()["rbd-mirror"];
    if (service.is_null()) {
      return 0;
    }

    for (auto& daemon_pair : service.get_obj()) {
      std::string service_id = daemon_pair.first;
      auto it = m_mirror_services.find(service_id);
      if (it == m_mirror_services.end()) {
        continue;
      }

      auto& mirror_service = it->second;
      auto& daemon = daemon_pair.second.get_obj();
      auto& status = daemon["status"];
      if (status.is_null()) {
        mirror_service.callouts.push_back("not reporting status");
        mirror_service.health = MIRROR_HEALTH_WARNING;
        continue;
      }

      auto& json = status.get_obj()["json"];
      if (json.is_null()) {
        mirror_service.callouts.push_back("not reporting status");
        mirror_service.health = MIRROR_HEALTH_WARNING;
        continue;
      }

      json_spirit::mValue json_status;
      if(!json_spirit::read(json.get_str(), json_status)) {
        std::cerr << "rbd: invalid service status daemon status JSON received"
                  << std::endl;
        return -EBADMSG;
      }

      auto& pool_val = json_status.get_obj()[stringify(m_io_ctx.get_id())];
      if (pool_val.is_null()) {
        mirror_service.callouts.push_back("not reporting status for pool");
        mirror_service.health = MIRROR_HEALTH_WARNING;
        continue;
      }

      auto& pool = pool_val.get_obj();
      found_pool = true;

      auto& instance_id = pool["instance_id"];
      if (!instance_id.is_null()) {
        mirror_service.instance_id = instance_id.get_str();
        m_instance_to_service_ids[mirror_service.instance_id] = service_id;
      }

      auto& leader = pool["leader"];
      if (!leader.is_null() && leader.get_bool()) {
        mirror_service.leader = true;
        found_leader = true;
      }

      MirrorHealth mirror_service_health = MIRROR_HEALTH_OK;
      auto& callouts = pool["callouts"];
      if (!callouts.is_null()) {
        for (auto& callout_pair : callouts.get_obj()) {
          auto& callout = callout_pair.second.get_obj();
          auto& level = callout["level"];
          if (level.is_null()) {
            continue;
          }

          auto& level_str = level.get_str();
          if (mirror_service_health < MIRROR_HEALTH_ERROR &&
              level_str == "error") {
            mirror_service_health = MIRROR_HEALTH_ERROR;
          } else if (mirror_service_health < MIRROR_HEALTH_WARNING &&
                     level_str == "warning") {
            mirror_service_health = MIRROR_HEALTH_WARNING;
          }

          auto& text = callout["text"];
          if (!text.is_null()) {
            mirror_service.callouts.push_back(text.get_str());
          }
        }
      }
      mirror_service.health = mirror_service_health;
    }
  } catch (std::runtime_error&) {
    std::cerr << "rbd: unexpected service status JSON received" << std::endl;
    return -EBADMSG;
  }

  // compute overall daemon health
  m_daemon_health = MIRROR_HEALTH_OK;
  if (!found_pool) {
    // no daemons are reporting status for this pool
    m_daemon_health = MIRROR_HEALTH_ERROR;
  } else if (!found_leader) {
    // no daemons are reporting leader role for this pool
    m_daemon_health = MIRROR_HEALTH_WARNING;
  }

  for (auto& pair : m_mirror_services) {
    m_daemon_health = std::max(m_daemon_health, pair.second.health);
  }

  return 0;
}

} // namespace rbd

