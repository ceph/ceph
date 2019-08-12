// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_SYNC_TRACE_H
#define CEPH_RGW_SYNC_TRACE_H

#include <regex>

#include "common/debug.h"
#include "common/ceph_json.h"

#include "rgw_sync_trace.h"
#include "rgw_rados.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw_sync

RGWSyncTraceNode::RGWSyncTraceNode(CephContext *_cct, uint64_t _handle,
                                   const RGWSyncTraceNodeRef& _parent,
                                   const string& _type, const string& _id) : cct(_cct),
                                                                             parent(_parent),
                                                                             type(_type),
                                                                             id(_id),
                                                                             handle(_handle),
                                                                             history(cct->_conf->rgw_sync_trace_per_node_log_size)
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
}

void RGWSyncTraceNode::log(int level, const string& s)
{
  status = s;
  history.push_back(status);
  /* dump output on either rgw_sync, or rgw -- but only once */
  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw_sync, level)) {
    lsubdout(cct, rgw_sync,
      ceph::dout::need_dynamic(level)) << "RGW-SYNC:" << to_str() << dendl;
  } else {
    lsubdout(cct, rgw,
      ceph::dout::need_dynamic(level)) << "RGW-SYNC:" << to_str() << dendl;
  }
}


class RGWSyncTraceServiceMapThread : public RGWRadosThread {
  RGWRados *store;
  RGWSyncTraceManager *manager;

  uint64_t interval_msec() override {
    return cct->_conf->rgw_sync_trace_servicemap_update_interval * 1000;
  }
public:
  RGWSyncTraceServiceMapThread(RGWRados *_store, RGWSyncTraceManager *_manager)
    : RGWRadosThread(_store, "sync-trace"), store(_store), manager(_manager) {}

  int process() override;
};

int RGWSyncTraceServiceMapThread::process()
{
  map<string, string> status;
  status["current_sync"] = manager->get_active_names();
  int ret = store->update_service_map(std::move(status));
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: update_service_map() returned ret=" << ret << dendl;
  }
  return 0;
}

RGWSyncTraceNodeRef RGWSyncTraceManager::add_node(const RGWSyncTraceNodeRef& parent,
                                                  const std::string& type,
                                                  const std::string& id)
{
  shunique_lock wl(lock, ceph::acquire_unique);
  auto handle = alloc_handle();
  RGWSyncTraceNodeRef& ref = nodes[handle];
  ref.reset(new RGWSyncTraceNode(cct, handle, parent, type, id));
  // return a separate shared_ptr that calls finish() on the node instead of
  // deleting it. the lambda capture holds a reference to the original 'ref'
  auto deleter = [ref, this] (RGWSyncTraceNode *node) { finish_node(node); };
  return {ref.get(), deleter};
}

bool RGWSyncTraceNode::match(const string& search_term, bool search_history)
{
  try {
    std::regex expr(search_term);
    std::smatch m;

    if (regex_search(prefix, m, expr)) {
      return true;
    }
    if (regex_search(status, m,expr)) {
      return true;
    }
    if (!search_history) {
      return false;
    }

    for (auto h : history) {
      if (regex_search(h, m, expr)) {
        return true;
      }
    }
  } catch (const std::regex_error& e) {
    ldout(cct, 5) << "NOTICE: sync trace: bad expression: bad regex search term" << dendl;
  }

  return false;
}

void RGWSyncTraceManager::init(RGWRados *store)
{
  service_map_thread = new RGWSyncTraceServiceMapThread(store, this);
  service_map_thread->start();
}

RGWSyncTraceManager::~RGWSyncTraceManager()
{
  cct->get_admin_socket()->unregister_commands(this);
  service_map_thread->stop();
  delete service_map_thread;

  nodes.clear();
}

int RGWSyncTraceManager::hook_to_admin_command()
{
  AdminSocket *admin_socket = cct->get_admin_socket();

  admin_commands = { { "sync trace show", "sync trace show name=search,type=CephString,req=false", "sync trace show [filter_str]: show current multisite tracing information" },
                     { "sync trace history", "sync trace history name=search,type=CephString,req=false", "sync trace history [filter_str]: show history of multisite tracing information" },
                     { "sync trace active", "sync trace active name=search,type=CephString,req=false", "show active multisite sync entities information" },
                     { "sync trace active_short", "sync trace active_short name=search,type=CephString,req=false", "show active multisite sync entities entries" } };
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

string RGWSyncTraceManager::get_active_names()
{
  shunique_lock rl(lock, ceph::acquire_shared);

  stringstream ss;
  JSONFormatter f;

  f.open_array_section("result");
  for (auto n : nodes) {
    auto& entry = n.second;

    if (!entry->test_flags(RGW_SNS_FLAG_ACTIVE)) {
      continue;
    }
    const string& name = entry->get_resource_name();
    if (!name.empty()) {
      ::encode_json("entry", name, &f);
    }
    f.flush(ss);
  }
  f.close_section();
  f.flush(ss);

  return ss.str();
}

bool RGWSyncTraceManager::call(std::string_view command, const cmdmap_t& cmdmap,
                               std::string_view format, bufferlist& out) {

  bool show_history = (command == "sync trace history");
  bool show_short = (command == "sync trace active_short");
  bool show_active = (command == "sync trace active") || show_short;

  string search;

  auto si = cmdmap.find("search");
  if (si != cmdmap.end()) {
    search = boost::get<string>(si->second);
  }

  shunique_lock rl(lock, ceph::acquire_shared);

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
    if (show_short) {
      const string& name = entry->get_resource_name();
      if (!name.empty()) {
        ::encode_json("entry", name, &f);
      }
    } else {
      dump_node(entry.get(), show_history, f);
    }
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
  RGWSyncTraceNodeRef old_node;

  {
    shunique_lock wl(lock, ceph::acquire_unique);
    if (!node) {
      return;
    }
    auto iter = nodes.find(node->handle);
    if (iter == nodes.end()) {
      /* not found, already finished */
      return;
    }

    if (complete_nodes.full()) {
      /* take a reference to the entry that is going to be evicted,
       * can't let it get evicted under lock held, otherwise
       * it's a deadlock as it will call finish_node()
       */
      old_node = complete_nodes.front();
    }

    complete_nodes.push_back(iter->second);
    nodes.erase(iter);
  }
};

#endif

