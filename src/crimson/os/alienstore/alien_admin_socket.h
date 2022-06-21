// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef ALIEN_ADMIN_SOCKET_H
#define ALIEN_ADMIN_SOCKET_H

#include "common/admin_socket.h"
#include "crimson/admin/admin_socket.h"
#include "crimson/os/alienstore/thread_pool.h"

namespace seastar::alien {
  class instance;
}

class CnAdminSocketHook;

class CnAdminSocket : public AdminSocket {
  seastar::lw_shared_ptr<crimson::admin::AdminSocket> cn_asok;
  std::unique_ptr<crimson::os::ThreadPool>& tp;
  std::map<const ::AdminSocketHook*, std::vector<CnAdminSocketHook*> > hook_tracker;
  seastar::alien::instance& inst;
  unsigned shard;

public:
  CnAdminSocket(ceph::common::CephContext* cct,
		seastar::lw_shared_ptr<crimson::admin::AdminSocket> cn_asok,
		std::unique_ptr<crimson::os::ThreadPool>& tp,
		seastar::alien::instance& inst,
		unsigned shard);
  ~CnAdminSocket() override;
  int register_command(std::string_view cmddesc,
		       AdminSocketHook *hook,
		       std::string_view help) override;
  void unregister_commands(const AdminSocketHook *hook) override;
};

#endif
