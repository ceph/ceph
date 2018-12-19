// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SYNC_LOG_H
#define CEPH_RGW_SYNC_LOG_H

#include <atomic>

#include "common/Mutex.h"
#include "common/shunique_lock.h"
#include "common/admin_socket.h"

#include <set>
#include <ostream>
#include <string>
#include <shared_mutex>
#include <boost/circular_buffer.hpp>

#define SSTR(o) ({      \
  std::stringstream ss; \
  ss << o;              \
  ss.str();             \
})

#define RGW_SNS_FLAG_ACTIVE   1
#define RGW_SNS_FLAG_ERROR    2

class RGWRados;
class RGWSyncTraceManager;
class RGWSyncTraceNode;
class RGWSyncTraceServiceMapThread;

using RGWSyncTraceNodeRef = std::shared_ptr<RGWSyncTraceNode>;

class RGWSyncTraceNode final {
  friend class RGWSyncTraceManager;

  CephContext *cct;
  RGWSyncTraceNodeRef parent;

  uint16_t state{0};
  std::string status;

  Mutex lock{"RGWSyncTraceNode::lock"};

  std::string type;
  std::string id;

  std::string prefix;

  std::string resource_name;

  uint64_t handle;

  boost::circular_buffer<string> history;

  // private constructor, create with RGWSyncTraceManager::add_node()
  RGWSyncTraceNode(CephContext *_cct, uint64_t _handle,
                   const RGWSyncTraceNodeRef& _parent,
                   const std::string& _type, const std::string& _id);

 public:
  void set_resource_name(const string& s) {
    resource_name = s;
  }

  const string& get_resource_name() {
    return resource_name;
  }

  void set_flag(uint16_t s) {
    state |= s;
  }
  void unset_flag(uint16_t s) {
    state &= ~s;
  }
  bool test_flags(uint16_t f) {
    return (state & f) == f;
  }
  void log(int level, const std::string& s);

  std::string to_str() {
    return prefix + " " + status;
  }

  const string& get_prefix() {
    return prefix;
  }

  std::ostream& operator<<(std::ostream& os) { 
    os << to_str();
    return os;            
  }

  boost::circular_buffer<string>& get_history() {
    return history;
  }

  bool match(const string& search_term, bool search_history);
};

class RGWSyncTraceManager : public AdminSocketHook {
  friend class RGWSyncTraceNode;

  mutable std::shared_timed_mutex lock;
  using shunique_lock = ceph::shunique_lock<decltype(lock)>;

  CephContext *cct;
  RGWSyncTraceServiceMapThread *service_map_thread{nullptr};

  std::map<uint64_t, RGWSyncTraceNodeRef> nodes;
  boost::circular_buffer<RGWSyncTraceNodeRef> complete_nodes;

  std::atomic<uint64_t> count = { 0 };

  std::list<std::array<string, 3> > admin_commands;

  uint64_t alloc_handle() {
    return ++count;
  }
  void finish_node(RGWSyncTraceNode *node);

public:
  RGWSyncTraceManager(CephContext *_cct, int max_lru) : cct(_cct), complete_nodes(max_lru) {}
  ~RGWSyncTraceManager();

  void init(RGWRados *store);

  const RGWSyncTraceNodeRef root_node;

  RGWSyncTraceNodeRef add_node(const RGWSyncTraceNodeRef& parent,
                               const std::string& type,
                               const std::string& id = "");

  int hook_to_admin_command();
  bool call(std::string_view command, const cmdmap_t& cmdmap,
            std::string_view format, bufferlist& out) override;
  string get_active_names();
};


#endif
