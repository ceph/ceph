#ifndef CEPH_RGW_SYNC_LOG_H
#define CEPH_RGW_SYNC_LOG_H

#include "include/atomic.h"

#include "common/Mutex.h"
#include "common/RWLock.h"

#include <set>
#include <ostream>
#include <string>
#include <boost/circular_buffer.hpp>

enum RGWSyncTraceNodeState {
  SNS_INACTIVE = 0,
  SNS_ACTIVE   = 1,
};

class RGWSyncTraceManager;
class RGWSyncTraceNode;

using RGWSyncTraceNodeRef = std::shared_ptr<RGWSyncTraceNode>;

class RGWSyncTraceNode {
  friend class RGWSyncTraceManager;

  RGWSyncTraceManager *manager{nullptr};
  RGWSyncTraceNodeRef parent;

  RGWSyncTraceNodeState state{SNS_INACTIVE};
  std::string status;

  Mutex lock{"RGWSyncTraceNode::lock"};

protected:
  std::string type;
  std::string trigger;
  std::string id;

  uint64_t handle;

public:
  RGWSyncTraceNode(RGWSyncTraceManager *_manager, RGWSyncTraceNodeRef& _parent,
           const std::string& _type, const std::string& _trigger, const std::string& _id);

  void set_state(RGWSyncTraceNodeState s);
  void set(const std::string& s) {
    status = s;
  }

  std::string to_str() {
    return trigger + ":" + id + ": " + status;
  }

  void finish(int ret);

  std::ostream& operator<<(std::ostream& os) { 
    os << to_str();
    return os;            
  }
};


class RGWSyncTraceManager {
  friend class RGWSyncTraceNode;

  std::map<uint64_t, RGWSyncTraceNodeRef> nodes;
  boost::circular_buffer<RGWSyncTraceNodeRef> complete_nodes;

  RWLock lock{"RGWSyncTraceManager::lock"};

  atomic64_t count;

protected:
  uint64_t alloc_handle() {
    return count.inc();
  }

public:
  RGWSyncTraceManager(int max_lru) {}

  RGWSyncTraceNodeRef& add_node(RGWSyncTraceNode *node);
  void finish_node(RGWSyncTraceNode *node);
};


#endif
