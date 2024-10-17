// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef COMMAND_TABLE_H_
#define COMMAND_TABLE_H_

#include "messages/MCommand.h"
#include "messages/MMgrCommand.h"

class CommandOp
{
  public:
  ConnectionRef con;
  ceph_tid_t tid;
  // multi_target_id == 0 means single target command
  ceph_tid_t multi_target_id;

  std::vector<std::string> cmd;
  ceph::buffer::list    inbl;
  Context      *on_finish;
  ceph::buffer::list   *outbl;
  std::string  *outs;

  MessageRef get_message(const uuid_d &fsid,
			 bool mgr=false) const
  {
    if (mgr) {
      auto m = ceph::make_message<MMgrCommand>(fsid);
      m->cmd = cmd;
      m->set_data(inbl);
      m->set_tid(tid);
      return m;
    } else {
      auto m = ceph::make_message<MCommand>(fsid);
      m->cmd = cmd;
      m->set_data(inbl);
      m->set_tid(tid);
      return m;
    }
  }

  CommandOp(const ceph_tid_t t) : tid(t), multi_target_id(0), on_finish(nullptr),
                                  outbl(nullptr), outs(nullptr) {}
  CommandOp() : tid(0), on_finish(nullptr), outbl(nullptr), outs(nullptr) {}
  CommandOp(const ceph_tid_t t, const ceph_tid_t multi_id) : tid(t), multi_target_id(multi_id),
                                                             on_finish(nullptr), outbl(nullptr), outs(nullptr) {}
};

/**
 * Hold client-side state for a collection of in-flight commands
 * to a remote service.
 */
template<typename T>
class CommandTable
{
protected:
  ceph_tid_t last_tid;
  ceph_tid_t last_multi_target_id;

  std::map<ceph_tid_t, T> commands;
  std::map<ceph_tid_t, std::set<ceph_tid_t> > multi_targets;

public:

  CommandTable()
    : last_tid(0), last_multi_target_id(0)
  {}

  ~CommandTable()
  {
    ceph_assert(commands.empty());
    for (const auto& pair : multi_targets) {
      ceph_assert(pair.second.empty());
    }
  }

  ceph_tid_t get_new_multi_target_id()
  {
    return ++last_multi_target_id;
  }

  T& start_command(ceph_tid_t multi_id=0)
  {
    ceph_tid_t tid = last_tid++;
    commands.insert(std::make_pair(tid, T(tid, multi_id)) );

    if (multi_id != 0) {
      multi_targets[multi_id].insert(tid);
    }

    return commands.at(tid);
  }

  const std::map<ceph_tid_t, T> &get_commands() const
  {
    return commands;
  }

  bool exists(ceph_tid_t tid) const
  {
    return commands.count(tid) > 0;
  }

  std::size_t count_multi_commands(ceph_tid_t multi_id)
  {
    return multi_targets[multi_id].size();
  }

  T& get_command(ceph_tid_t tid)
  {
    return commands.at(tid);
  }

  void erase(ceph_tid_t tid)
  {
    ceph_tid_t multi_id = commands.at(tid).multi_target_id;
    commands.erase(tid);
    multi_targets[multi_id].erase(tid);

    if(count_multi_commands(multi_id) == 0) {
      multi_targets.erase(multi_id);
    }
  }

  void clear() {
    commands.clear();
    multi_targets.clear();
  }
};

#endif

