#ifndef CEPH_RGW_SYNC_LOG_H
#define CEPH_RGW_SYNC_LOG_H

#include "include/atomic.h"

#include "common/Mutex.h"
#include "common/RWLock.h"

#include <set>
#include <ostream>
#include <string>
#include <boost/circular_buffer.hpp>

#define SSTR(o) ({      \
  std::stringstream ss; \
  ss << o;              \
  ss.str();             \
})

enum RGWSyncTraceNodeState {
  SNS_INACTIVE = 0,
  SNS_ACTIVE   = 1,
};

class RGWSyncTraceManager;
class RGWSyncTraceNode;

using RGWSyncTraceNodeRef = std::shared_ptr<RGWSyncTraceNode>;

class RGWSyncTraceNode {
  friend class RGWSyncTraceManager;

  CephContext *cct;

  RGWSyncTraceManager *manager{nullptr};
  RGWSyncTraceNodeRef parent;

  RGWSyncTraceNodeState state{SNS_INACTIVE};
  std::string status;

  Mutex lock{"RGWSyncTraceNode::lock"};

protected:
  std::string type;
  std::string trigger;
  std::string id;

  std::string prefix;

  uint64_t handle;

public:
  RGWSyncTraceNode(CephContext *_cct, RGWSyncTraceManager *_manager, const RGWSyncTraceNodeRef& _parent,
           const std::string& _type, const std::string& _trigger, const std::string& _id);

  void set_state(RGWSyncTraceNodeState s);
  void log(int level, const std::string& s);

  std::string to_str() {
    return prefix + " " + status;
  }

  const string& get_prefix() {
    return prefix;
  }

  void finish(int ret);

  std::ostream& operator<<(std::ostream& os) { 
    os << to_str();
    return os;            
  }
};


class RGWSyncTraceManager {
  friend class RGWSyncTraceNode;

  CephContext *cct;

  std::map<uint64_t, RGWSyncTraceNodeRef> nodes;
  boost::circular_buffer<RGWSyncTraceNodeRef> complete_nodes;

  RWLock lock{"RGWSyncTraceManager::lock"};

  atomic64_t count;

protected:
  uint64_t alloc_handle() {
    return count.inc();
  }

public:
  RGWSyncTraceManager(CephContext *_cct, int max_lru) : cct(_cct) {}

  const RGWSyncTraceNodeRef root_node;

  RGWSyncTraceNodeRef& add_node(RGWSyncTraceNode *node);
  void finish_node(RGWSyncTraceNode *node);

};


#endif
