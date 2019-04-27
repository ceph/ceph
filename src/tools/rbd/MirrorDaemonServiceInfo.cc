// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "tools/rbd/MirrorDaemonServiceInfo.h"

#include <boost/scope_exit.hpp>
#include <iostream>

namespace rbd {

int MirrorDaemonServiceInfo::init() {

  std::string cmd = "{\"prefix\": \"service dump\"}";

  bufferlist in_bl;
  bufferlist out_bl;
  int r = librados::Rados(m_io_ctx).mgr_command(cmd, in_bl, &out_bl, nullptr);
  if (r < 0) {
    std::cerr << "rbd: failed to get service dump: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  bool json_valid = false;
  json_spirit::mValue json_root;
  if (json_spirit::read(out_bl.to_str(), json_root)) {
    try {
      auto& json_obj = json_root.get_obj();
      if (json_obj.count("services")) {
        auto &services = json_obj["services"].get_obj();
        if (services.count("rbd-mirror")) {
          auto &mirror_service = services["rbd-mirror"].get_obj();
          if (mirror_service.count("daemons")) {
            for (auto &it : mirror_service["daemons"].get_obj()) {
              if (it.second.type() != json_spirit::obj_type ||
                  !it.second.get_obj().count("metadata")) {
                continue;
              }
              auto &service_id = it.first;
              auto &daemon_metadata = it.second.get_obj()["metadata"].get_obj();
              for (auto &iter : daemon_metadata) {
                if (iter.second.type() != json_spirit::str_type) {
                  continue;
                }
                m_daemons_metadata[service_id][iter.first] = iter.second.get_str();
              }
            }
          }
        }
      }
      json_valid = true;
    } catch (std::runtime_error&) {
    }
  }

  if (!json_valid) {
    std::cerr << "rbd: failed to parse service status" << std::endl;
    return -EBADMSG;
  }

  cmd = "{\"prefix\": \"service status\"}";

  out_bl.clear();
  r = librados::Rados(m_io_ctx).mgr_command(cmd, in_bl, &out_bl, nullptr);
  if (r < 0) {
    std::cerr << "rbd: failed to get service status: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  json_valid = false;
  if (json_spirit::read(out_bl.to_str(), json_root)) {
    try {
      auto& json_obj = json_root.get_obj();
      if (json_obj.count("rbd-mirror")) {
        auto &mirror_service = json_obj["rbd-mirror"].get_obj();
        for (auto &it : mirror_service) {
          auto &service_id = it.first;
          auto &daemon = it.second.get_obj();
          if (daemon.count("status") &&
              daemon["status"].get_obj().count("json")) {
            auto& status_json_str =
                daemon["status"].get_obj()["json"].get_str();
            json_spirit::mValue status_json_root;
            if (json_spirit::read(status_json_str, status_json_root)) {
              auto& status = status_json_root.get_obj();
              auto iter = status.find(stringify(m_io_ctx.get_id()));
              if (iter != status.end() && 
                  iter->second.get_obj().count("instance_id")) {
                auto &instance_id =
                  iter->second.get_obj()["instance_id"].get_str();
                m_instance_id_to_service_id[instance_id] = service_id;
              }
            }
          }
        }
      }
      json_valid = true;
    } catch (std::runtime_error&) {
    }
  }

  if (!json_valid) {
    std::cerr << "rbd: failed to parse service status" << std::endl;
    return -EBADMSG;
  }

  return 0;
}

std::string MirrorDaemonServiceInfo::get_description(
    const std::string &instance_id) const {
  if (!m_instance_id_to_service_id.count(instance_id)) {
    return {};
  }

  auto service_id = m_instance_id_to_service_id.find(instance_id)->second;

  auto it = m_daemons_metadata.find(service_id);
  if (it == m_daemons_metadata.end()) {
    return service_id;
  }

  auto &metadata = it->second;
  auto iter = metadata.find("id");
  std::string description = (iter != metadata.end()) ?
      iter->second : service_id;
  iter = metadata.find("hostname");
  if (iter != metadata.end()) {
    description += " on " + iter->second;
  }

  return description;
}

void MirrorDaemonServiceInfo::dump(
    const std::string &instance_id,
    argument_types::Format::Formatter formatter) const {
  formatter->open_object_section("daemon_service");
  BOOST_SCOPE_EXIT(formatter) {
    formatter->close_section();
  } BOOST_SCOPE_EXIT_END;

  if (instance_id.empty() ||
      !m_instance_id_to_service_id.count(instance_id)) {
    return;
  }

  auto service_id = m_instance_id_to_service_id.find(instance_id)->second;
  formatter->dump_string("service_id", service_id);
  formatter->dump_string("instance_id", instance_id);

  auto it = m_daemons_metadata.find(service_id);
  if (it == m_daemons_metadata.end()) {
    return;
  }

  auto &metadata = it->second;
  auto iter = metadata.find("id");
  if (iter != metadata.end()) {
    formatter->dump_string("daemon_id", iter->second);
  }
  iter = metadata.find("hostname");
  if (iter != metadata.end()) {
    formatter->dump_string("hostname", iter->second);
  }
}

} // namespace rbd

