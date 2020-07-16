// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>
#include <list>
#include <sstream>

#include "include/utime.h"
#include "include/buffer.h"
#include "msg/msg_types.h"

namespace ceph {
  class Formatter;
}

struct ServiceMap {
  struct Daemon {
    uint64_t gid = 0;
    entity_addr_t addr;
    epoch_t start_epoch = 0;   ///< epoch first registered
    utime_t start_stamp;       ///< timestamp daemon started/registered
    std::map<std::string,std::string> metadata;  ///< static metadata
    std::map<std::string,std::string> task_status; ///< running task status

    void encode(ceph::buffer::list& bl, uint64_t features) const;
    void decode(ceph::buffer::list::const_iterator& p);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<Daemon*>& ls);
  };

  struct Service {
    std::map<std::string,Daemon> daemons;
    std::string summary;   ///< summary status std::string for 'ceph -s'

    void encode(ceph::buffer::list& bl, uint64_t features) const;
    void decode(ceph::buffer::list::const_iterator& p);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<Service*>& ls);

    std::string get_summary() const;
    std::string get_task_summary(const std::string_view task_prefix) const;
    void count_metadata(const std::string& field,
			std::map<std::string,int> *out) const;
  };

  epoch_t epoch = 0;
  utime_t modified;
  std::map<std::string,Service> services;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ServiceMap*>& ls);

  std::pair<Daemon*,bool> get_daemon(const std::string& service,
				     const std::string& daemon) {
    auto& s = services[service];
    auto [d, added] = s.daemons.try_emplace(daemon);
    return {&d->second, added};
  }

  bool rm_daemon(const std::string& service,
		 const std::string& daemon) {
    auto p = services.find(service);
    if (p == services.end()) {
      return false;
    }
    auto q = p->second.daemons.find(daemon);
    if (q == p->second.daemons.end()) {
      return false;
    }
    p->second.daemons.erase(q);
    if (p->second.daemons.empty()) {
      services.erase(p);
    }
    return true;
  }

  static inline bool is_normal_ceph_entity(std::string_view type) {
    if (type == "osd" ||
        type == "client" ||
        type == "mon" ||
        type == "mds" ||
        type == "mgr") {
      return true;
    }

    return false;
  }
};
WRITE_CLASS_ENCODER_FEATURES(ServiceMap)
WRITE_CLASS_ENCODER_FEATURES(ServiceMap::Service)
WRITE_CLASS_ENCODER_FEATURES(ServiceMap::Daemon)
