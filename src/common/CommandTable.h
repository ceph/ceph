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

class CommandOp
{
  public:
  ConnectionRef con;
  ceph_tid_t tid;

  std::vector<std::string> cmd;
  bufferlist    inbl;
  Context      *on_finish;
  bufferlist   *outbl;
  std::string  *outs;

  MCommand::ref get_message(const uuid_d &fsid) const
  {
    auto m = MCommand::create(fsid);
    m->cmd = cmd;
    m->set_data(inbl);
    m->set_tid(tid);

    return m;
  }

  CommandOp(const ceph_tid_t t) : tid(t), on_finish(nullptr),
                                  outbl(nullptr), outs(nullptr) {}
  CommandOp() : tid(0), on_finish(nullptr), outbl(nullptr), outs(nullptr) {}
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
  std::map<ceph_tid_t, T> commands;

public:

  CommandTable()
    : last_tid(0)
  {}

  ~CommandTable()
  {
    ceph_assert(commands.empty());
  }

  T& start_command()
  {
    ceph_tid_t tid = last_tid++;
    commands.insert(std::make_pair(tid, T(tid)) );

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

  T& get_command(ceph_tid_t tid)
  {
    return commands.at(tid);
  }

  void erase(ceph_tid_t tid)
  {
    commands.erase(tid);
  }

  void clear() {
    commands.clear();
  }
};

#endif

