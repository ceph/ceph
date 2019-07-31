// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Ltd
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef FS_COMMANDS_H_
#define FS_COMMANDS_H_

#include "Monitor.h"
#include "CommandHandler.h"

#include "osd/OSDMap.h"
#include "mds/FSMap.h"

#include <string>
#include <sstream>

class FileSystemCommandHandler : protected CommandHandler
{
protected:
  std::string prefix;

  /**
   * Return 0 if the pool is suitable for use with CephFS, or
   * in case of errors return a negative error code, and populate
   * the passed stringstream with an explanation.
   *
   * @param metadata whether the pool will be for metadata (stricter checks)
   */
  int _check_pool(
      OSDMap &osd_map,
      const int64_t pool_id,
      bool metadata,
      bool force,
      std::stringstream *ss) const;

  virtual std::string const &get_prefix() {return prefix;}

public:
  FileSystemCommandHandler(const std::string &prefix_)
    : prefix(prefix_)
  {}

  virtual ~FileSystemCommandHandler()
  {}

  bool can_handle(std::string const &prefix_)
  {
    return get_prefix() == prefix_;
  }

  static std::list<std::shared_ptr<FileSystemCommandHandler> > load(Paxos *paxos);

  virtual bool batched_propose() {
    return false;
  }

  virtual int handle(
    Monitor *mon,
    FSMap &fsmap,
    MonOpRequestRef op,
    const cmdmap_t& cmdmap,
    std::stringstream &ss) = 0;
};

#endif
