// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
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

  MCommand *get_message(const uuid_d &fsid) const
  {
    MCommand *m = new MCommand(fsid);
    m->cmd = cmd;
    m->set_data(inbl);
    m->set_tid(tid);

    return m;
  }

  CommandOp(const ceph_tid_t t) : tid(t) {}
};

template<typename T>
class CommandTable
{
protected:
  ceph_tid_t last_tid;
  std::map<ceph_tid_t, T*> commands;

public:

  CommandTable()
    : last_tid(0)
  {}

  T* start_command()
  {
    ceph_tid_t tid = last_tid++;
    auto cmd = new T(tid);
    commands[tid] = cmd;
    
    return cmd;
  }

  const std::map<ceph_tid_t, T*> &get_commands() const
  {
    return commands;
  }

  T* get_command(ceph_tid_t tid)
  {
    auto result = commands.find(tid);
    if (result == commands.end()) {
      return nullptr;
    } else {
      return result->second;
    }
  }

  void erase(ceph_tid_t tid)
  {
    auto ptr = commands.at(tid);
    delete ptr;
    commands.erase(tid);
  }
};

#endif

