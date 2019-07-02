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

#include <boost/optional.hpp>

namespace ceph {
  class Formatter;
}

struct ServiceMap {
  struct Daemon {
    boost::optional<uint64_t> gid;
    entity_addr_t addr;
    epoch_t start_epoch = 0;   ///< epoch first registered
    utime_t start_stamp;       ///< timestamp daemon started/registered
    std::map<std::string,std::string> metadata;  ///< static metadata
    std::map<std::string,std::string> task_status; ///< running task status

    void encode(bufferlist& bl, uint64_t features) const;
    void decode(bufferlist::const_iterator& p);
    void dump(Formatter *f) const;
    static void generate_test_instances(std::list<Daemon*>& ls);
  };

  struct Service {
    map<std::string,Daemon> daemons;
    std::string summary;   ///< summary status string for 'ceph -s'

    void encode(bufferlist& bl, uint64_t features) const;
    void decode(bufferlist::const_iterator& p);
    void dump(Formatter *f) const;
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

    std::string get_task_summary(const std::string_view task_prefix) const {
      // contruct a map similar to:
      //     {"service1 status" -> {"service1.0" -> "running"}}
      //     {"service2 status" -> {"service2.0" -> "idle"},
      //                           {"service2.1" -> "running"}}
      std::map<std::string, std::map<std::string, std::string>> by_task;
      for (const auto &p : daemons) {
        std::stringstream d;
        d << task_prefix << "." << p.first;
        for (const auto &q : p.second.task_status) {
          auto p1 = by_task.emplace(q.first, std::map<std::string, std::string>{}).first;
          auto p2 = p1->second.emplace(d.str(), std::string()).first;
          p2->second = q.second;
        }
      }

      std::stringstream ss;
      for (const auto &p : by_task) {
        ss << "\n    " << p.first << ":";
        for (auto q : p.second) {
          ss << "\n        " << q.first << ": " << q.second;
        }
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
  map<std::string,Service> services;

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
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
