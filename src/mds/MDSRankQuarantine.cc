/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "MDSRank.h"
#include "MDCache.h"
#include "QuarantineManager.h"

#include "mon/MonClient.h"
#include "common/cmdparse.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/JSONFormatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "quarantine.mds." << whoami << " <" << __func__ << "> "

#undef dout
#define dout(lvl)                                                        \
  do {                                                                   \
    auto subsys = ceph_subsys_mds;                                       \
    if ((dout_context)->_conf->subsys.should_gather(dout_subsys, lvl)) { \
      subsys = dout_subsys;                                              \
    }                                                                    \
  dout_impl(dout_context, ceph::dout::need_dynamic(subsys), lvl) dout_prefix

#undef dendl
#define dendl \
  dendl_impl; \
  }           \
  while (0)

using TOPNSPC::common::cmd_getval;

// asok command handler
void MDSRank::command_quarantine_dir(const cmdmap_t& cmdmap, asok_finisher on_finish, const unsigned op)
{
  std::string path;
  cmd_getval(cmdmap, "path", path);

  MDRequestRef mdr = mdcache->request_start_internal(CEPH_MDS_OP_QUARANTINEDIR_AUTH);
  mdr->no_early_reply = true;
  mdr->set_filepath(filepath(path));
  mdr->qtine_op = op;
  mdr->qtine_mgr = std::make_shared<QuarantineTracker>();
  auto qtine_mgr = mdr->qtine_mgr;
  mdr->internal_op_finish = new LambdaContext([qtine_mgr](int r) {
    // Internal request forwarding/cancellation can complete the MDRequest
    // before the quarantine tracker runs. Surface those errors to asok.
    if (r < 0) {
      qtine_mgr->complete(r);
    }
  });
  mdr->qtine_mgr->register_callback(
      new LambdaContext([this, qtine_mgr, on_finish=on_finish, op=op](int r) {
	// Unregister tracker now that all work (including replica replies) is done
	auto ino = qtine_mgr->get_qtine_ino();
	if (ino) {
	  unregister_quarantine_mgr(ino);
	}
	std::string_view op_str{(op == QUARANTINE_ADD ? "enable" : "disable")};
	ceph::bufferlist bl;
	JSONFormatter f(true);
	f.open_object_section("result");
	f.dump_int("return_code", r);
	f.dump_string("operation", op_str);
	if (r) {
	  f.dump_string("status", "failed");
	  f.dump_string("error", cpp_strerror(r));
	} else {
	  f.dump_string("status", "successful");
	}
	f.close_section();
	std::ostringstream oss;
	f.flush(oss);
	bl.append(oss.str());
	on_finish(r, "", bl);
      }));
  std::lock_guard l(mds_lock);
  mdcache->dispatch_request(mdr);
}

bool MDSRank::register_quarantine_mgr(inodeno_t ino, QtineMgrRef qtine_op)
{
  std::lock_guard lck(qtine_mutex);
  auto it = qtine_ops.find(ino);
  if (it != qtine_ops.end()) {
    // Previous op's work finished but WORK request hasn't cleaned up yet.
    // Safe to replace since all async work is done.
    if (it->second->is_complete()) {
      it->second = qtine_op;
      return true;
    }
    return false;  // Active op still running
  }
  qtine_ops[ino] = qtine_op;
  return true;
}

void MDSRank::unregister_quarantine_mgr(inodeno_t ino)
{
  std::lock_guard lck(qtine_mutex);
  qtine_ops.erase(ino);
}

QtineMgrRef MDSRank::get_quarantine_mgr(inodeno_t ino)
{
  std::lock_guard lck(qtine_mutex);
  auto it = qtine_ops.find(ino);
  if (it != qtine_ops.end()) {
    return it->second;
  }
  return nullptr;
}

