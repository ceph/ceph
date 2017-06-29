#ifndef CEPH_RGW_SYNC_LOG_H
#define CEPH_RGW_SYNC_LOG_H

#include "include/atomic.h"

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/admin_socket.h"

#include <set>
#include <ostream>
#include <string>
#include <boost/circular_buffer.hpp>

#define SSTR(o) ({      \
  std::stringstream ss; \
  ss << o;              \
  ss.str();             \
})

#define RGW_SNS_FLAG_ACTIVE   1
#define RGW_SNS_FLAG_ERROR    2

class RGWSyncTraceManager;
class RGWSyncTraceNode;
class RGWSyncTraceNodeContainer;

using RGWSyncTraceNodeRef = std::shared_ptr<RGWSyncTraceNode>;
using RGWSTNCRef = std::shared_ptr<RGWSyncTraceNodeContainer>;

class RGWSyncTraceNode {
  friend class RGWSyncTraceManager;

  CephContext *cct;

  RGWSyncTraceManager *manager{nullptr};
  RGWSyncTraceNodeRef parent;

  uint16_t state{0};
  std::string status;

  Mutex lock{"RGWSyncTraceNode::lock"};

protected:
  std::string type;
  std::string id;

  std::string prefix;

  uint64_t handle;

  boost::circular_buffer<string> history;
public:
  RGWSyncTraceNode(CephContext *_cct, RGWSyncTraceManager *_manager, const RGWSyncTraceNodeRef& _parent,
           const std::string& _type, const std::string& _id);

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
  void finish();

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

/*
 * a container to RGWSyncTraceNodeRef, responsible to keep track
 * of live nodes, and when last ref is dropped, calls ->finish()
 * so that node moves to the retired list in the manager
 */
class RGWSyncTraceNodeContainer {
  RGWSyncTraceNodeRef tn;
public:
  RGWSyncTraceNodeContainer(RGWSyncTraceNodeRef& _tn) : tn(_tn) {}

  ~RGWSyncTraceNodeContainer();

  RGWSyncTraceNodeRef& operator*() {
    return tn;
  }

  RGWSyncTraceNodeRef& operator->() {
    return tn;
  }

  void set_flag(uint16_t flag) {
    tn->set_flag(flag);
  }
  void unset_flag(uint16_t flag) {
    tn->unset_flag(flag);
  }
  bool test_flags(uint16_t f) {
    return tn->test_flags(f);
  }
  void log(int level, const std::string& s) {
    return tn->log(level, s);
  }
  RGWSyncTraceNodeRef& ref() {
    return tn;
  }
};


class RGWSyncTraceManager : public AdminSocketHook {
  friend class RGWSyncTraceNode;

  CephContext *cct;

  std::map<uint64_t, RGWSyncTraceNodeRef> nodes;
  boost::circular_buffer<RGWSyncTraceNodeRef> complete_nodes;

  RWLock lock{"RGWSyncTraceManager::lock"};

  atomic64_t count;

  std::list<std::array<string, 3> > admin_commands;
protected:
  uint64_t alloc_handle() {
    return count.inc();
  }

public:
#warning complete_nodes size configurable
  RGWSyncTraceManager(CephContext *_cct, int max_lru) : cct(_cct), complete_nodes(512) {}
  ~RGWSyncTraceManager();

  const RGWSyncTraceNodeRef root_node;

  RGWSTNCRef add_node(RGWSyncTraceNode *node);
  void finish_node(RGWSyncTraceNode *node);

  int hook_to_admin_command();
  bool call(std::string command, cmdmap_t& cmdmap, std::string format, bufferlist& out);
};


#endif
