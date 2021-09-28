#ifndef _REACTOR_H_
#define _REACTOR_H_

#include "EventHandler.h"
#include "common/ceph_context.h"
#include <unordered_map>
#include <atomic>

namespace librbd::cache::pwl::rwl::replica {


class Reactor {
public:
  Reactor(CephContext *cct);
  ~Reactor();

  Reactor(const Reactor &) = delete;
  Reactor& operator=(const Reactor &) = delete;

  // Register an EventHandler of a particular EventType.
  int register_handler(EventHandlerPtr eh, EventType et);

  // Remove an EventHandler of a particular EventType.
  int remove_handler(EventHandlerPtr eh, EventType et);

  // Entry point into the reactive event loop.
  int handle_events();

  bool empty() { return _event_table.empty(); }

  void shutdown();

private:
  int _epoll;
  std::atomic<bool> _stop{false};
  CephContext *_cct;
  std::unordered_map<Handle, EventHandle> _event_table;
};

} // namespace ceph::librbd::cache::pwl::rwl::replica
#endif //_REACTOR_H_