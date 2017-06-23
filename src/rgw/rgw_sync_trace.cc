#ifndef CEPH_RGW_SYNC_TRACE_H
#define CEPH_RGW_SYNC_TRACE_H

#include "common/debug.h"

#include "rgw_sync_trace.h"

using namespace std;

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw_sync

RGWSyncTraceNode::RGWSyncTraceNode(CephContext *_cct, RGWSyncTraceManager *_manager, const RGWSyncTraceNodeRef& _parent,
                   const string& _type, const string& _trigger, const string& _id) : cct(_cct),
                                                                                     manager(_manager),
                                                                                     parent(_parent),
                                                                                     type(_type),
                                                                                     trigger(_trigger),
                                                                                     id(_id)
{
  if (parent.get()) {
    prefix = parent->get_prefix();
  }
  if (!trigger.empty()) {
    prefix += trigger + ":";
  }

  if (!type.empty()) {
    prefix += type;
    if (!id.empty()) {
      prefix += "[" + id + "]";
    }
    prefix += ":";
  }
  handle = manager->alloc_handle();
}

void RGWSyncTraceNode::log(int level, const string& s)
{
  status = s;
  ldout(cct, level) << "RGW-SYNC:" << to_str() << dendl;
}

void RGWSyncTraceNode::finish()
{
  manager->finish_node(this);
}


RGWSTNCRef RGWSyncTraceManager::add_node(RGWSyncTraceNode *node)
{
  RWLock::WLocker wl(lock);
  RGWSyncTraceNodeRef& ref = nodes[node->handle];
  ref.reset(node);
  return RGWSTNCRef(new RGWSyncTraceNodeContainer(ref));
}

RGWSyncTraceNodeContainer::~RGWSyncTraceNodeContainer()
{
  tn->finish();
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

