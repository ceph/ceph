#ifndef CEPH_RGW_SYNC_TRACE_H
#define CEPH_RGW_SYNC_TRACE_H

#include "common/debug.h"
#include "common/ceph_json.h"

#include "rgw_sync_trace.h"

using namespace std;

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw_sync

#warning history size configurable
RGWSyncTraceNode::RGWSyncTraceNode(CephContext *_cct, RGWSyncTraceManager *_manager,
                                   const RGWSyncTraceNodeRef& _parent,
                                   const string& _type, const string& _id) : cct(_cct),
                                                                             manager(_manager),
                                                                             parent(_parent),
                                                                             type(_type),
                                                                             id(_id), history(32)
{
  if (parent.get()) {
    prefix = parent->get_prefix();
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
  history.push_back(status);
  /* dump output on either rgw_sync, or rgw -- but only once */
  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw_sync, level)) {
    lsubdout(cct, rgw_sync, level) << "RGW-SYNC:" << to_str() << dendl;
  } else {
    lsubdout(cct, rgw, level) << "RGW-SYNC:" << to_str() << dendl;
  }
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

bool RGWSyncTraceNode::match(const string& search_term, bool search_history)
{
  if (prefix.find(search_term) != string::npos) {
    return true;
  }
  if (status.find(search_term) != string::npos) {
    return true;
  }
  if (!search_history) {
    return false;
  }

  for (auto h : history) {
    if (h.find(search_term) != string::npos) {
      return true;
    }
  }

  return false;
}

RGWSyncTraceManager::~RGWSyncTraceManager()
{
  AdminSocket *admin_socket = cct->get_admin_socket();
  for (auto cmd : admin_commands) {
    admin_socket->unregister_command(cmd[0]);
  }
}

int RGWSyncTraceManager::hook_to_admin_command()
{
  AdminSocket *admin_socket = cct->get_admin_socket();

  admin_commands = { { "sync trace show", "sync trace show name=search,type=CephString,req=false", "sync trace show [filter_str]: show current multisite tracing information" },
                     { "sync trace history", "sync trace history name=search,type=CephString,req=false", "sync trace history [filter_str]: show history of multisite tracing information" },
                     { "sync trace active", "sync trace active name=search,type=CephString,req=false", "show active multisite sync entities information" } };
  for (auto cmd : admin_commands) {
    int r = admin_socket->register_command(cmd[0], cmd[1], this,
                                           cmd[2]);
    if (r < 0) {
      lderr(cct) << "ERROR: fail to register admin socket command (r=" << r << ")" << dendl;
      return r;
    }
  }
  return 0;
}

static void dump_node(RGWSyncTraceNode *entry, bool show_history, JSONFormatter& f)
{
  f.open_object_section("entry");
  ::encode_json("status", entry->to_str(), &f);
  if (show_history) {
    f.open_array_section("history");
    for (auto h : entry->get_history()) {
      ::encode_json("entry", h, &f);
    }
    f.close_section();
  }
  f.close_section();
}

bool RGWSyncTraceManager::call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {

  bool show_history = (command == "sync trace history");
  bool show_active = (command == "sync trace active");

  string search;

  auto si = cmdmap.find("search");
  if (si != cmdmap.end()) {
    search = boost::get<string>(si->second);
  }

  RWLock::RLocker rl(lock);

  stringstream ss;
  JSONFormatter f(true);

  f.open_object_section("result");
  f.open_array_section("running");
  for (auto n : nodes) {
    auto& entry = n.second;

    if (!search.empty() && !entry->match(search, show_history)) {
      continue;
    }
    if (show_active && !entry->test_flags(RGW_SNS_FLAG_ACTIVE)) {
      continue;
    }
    dump_node(entry.get(), show_history, f);
    f.flush(ss);
  }
  f.close_section();

  f.open_array_section("complete");
  for (auto& entry : complete_nodes) {
    if (!search.empty() && !entry->match(search, show_history)) {
      continue;
    }
    if (show_active && !entry->test_flags(RGW_SNS_FLAG_ACTIVE)) {
      continue;
    }
    dump_node(entry.get(), show_history, f);
    f.flush(ss);
  }
  f.close_section();

  f.close_section();
  f.flush(ss);
  out.append(ss);

  return true;
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

