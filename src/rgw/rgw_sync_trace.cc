#ifndef CEPH_RGW_SYNC_TRACE_H
#define CEPH_RGW_SYNC_TRACE_H

#include "rgw_sync_trace.h"

using namespace std;

RGWSyncTraceNode::RGWSyncTraceNode(RGWSyncTraceManager *_manager, RGWSyncTraceNodeRef& _parent,
                   const string& _type, const string& _trigger, const string& _id) : manager(_manager),
                                                                                     parent(_parent),
                                                                                     type(_type),
                                                                                     trigger(_trigger),
                                                                                     id(_id)
{
  handle = manager->alloc_handle();
}

void RGWSyncTraceNode::finish(int ret) {
  char buf[32];
  snprintf(buf, sizeof(buf), "status=%d", ret);

  status = buf;

  manager->finish_node(this);
}


RGWSyncTraceNodeRef& RGWSyncTraceManager::add_node(RGWSyncTraceNode *node)
{
  RWLock::WLocker wl(lock);
  RGWSyncTraceNodeRef& ref = nodes[node->handle];
  ref.reset(node);
  return ref;
}

void RGWSyncTraceManager::finish_node(RGWSyncTraceNode *node)
{
  RWLock::WLocker wl(lock);
  auto iter = nodes.find(node->handle);
  if (iter == nodes.end()) {
    /* not found, already finished */
    return;
  }

  complete_nodes.push_back(iter->second);
  nodes.erase(iter);
};


#endif

