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

    std::string get_summary() const {
      if (summary.size()) {
	return summary;
      }
      if (daemons.empty()) {
	return "no daemons active";
      }
      std::ostringstream ss;
      ss << daemons.size() << (daemons.size() > 1 ? " daemons" : " daemon")
	 << " active";

      if (!daemons.empty()) {
	ss << " (";
	for (auto p = daemons.begin(); p != daemons.end(); ++p) {
	  if (p != daemons.begin()) {
	    ss << ", ";
	  }
	  ss << p->first;
	}
	ss << ")";
      }

      return ss.str();
    }

    void count_metadata(const std::string& field,
			std::map<std::string,int> *out) const {
      for (auto& p : daemons) {
	auto q = p.second.metadata.find(field);
	if (q == p.second.metadata.end()) {
	  (*out)["unknown"]++;
	} else {
	  (*out)[q->second]++;
	}
      }
    }

  };

  epoch_t epoch = 0;
  utime_t modified;
  std::map<std::string,Service> services;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ServiceMap*>& ls);

  Daemon* get_daemon(const std::string& service,
		     const std::string& daemon) {
    return &services[service].daemons[daemon];
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
};
WRITE_CLASS_ENCODER_FEATURES(ServiceMap)
WRITE_CLASS_ENCODER_FEATURES(ServiceMap::Service)
WRITE_CLASS_ENCODER_FEATURES(ServiceMap::Daemon)
