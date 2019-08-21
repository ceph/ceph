// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LLC <contact@suse.com> 
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MONMAPMONITOR_CMDS_H
#define CEPH_MONMAPMONITOR_CMDS_H

#include "mon/Monitor.h"
#include "mon/MonmapMonitor.h"
#include "mon/MonOpRequest.h"

#include "mon/commands/Command.h"

#include "include/types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/dout.h"
#include "common/cmdparse.h"


/* MonMonReadCommand
 *
 * Defines a read-only command for the MonmapMonitor, thus simplifying further
 * command definitions for this service.
 */
struct MonMonReadCommand :
  public ReadCommand<MonmapMonitor, MonMap, MonMap>
{
  explicit MonMonReadCommand(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_ctx) :
    ReadCommand<MonmapMonitor, MonMap, MonMap>(_mon, _monmon, _ctx)
  { }

  virtual ~MonMonReadCommand() { }

 private:
  MonMonReadCommand() = delete;
};

/* MonMonWriteCommand
 *
 * Defines a write command for the MonmapMonitor, thus simplifying further
 * command definitions for this service. A write command will presume that
 * we may have both a 'preprocess' and a 'prepare' stage, and we should thus
 * allow for individual commands to decide what to do.
 */
struct MonMonWriteCommand :
  public WriteCommand<MonmapMonitor, MonMap, MonMap>
{
  explicit MonMonWriteCommand(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_ctx) :
    WriteCommand<MonmapMonitor, MonMap, MonMap>(_mon, _monmon, _ctx)
  { }

  virtual ~MonMonWriteCommand() { }

 private:
  MonMonWriteCommand() = delete;
};

typedef std::shared_ptr<MonMonReadCommand> MonMonReadCommandRef;
typedef std::shared_ptr<MonMonWriteCommand> MonMonWriteCommandRef;

// preprocess commands (RO)

/* MonMonStatCommand - handle 'mon stat' */
struct MonMonStatCommand : public MonMonReadCommand
{
  explicit MonMonStatCommand(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonReadCommand(_mon, _monmon, _cct)
  { }

  bool handles_command(const string &prefix) {
    return (prefix == "mon stat");
  }

  bool do_preprocess(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      const MonMap &monmap);
};

struct MonMonGetMap : public MonMonReadCommand
{
  explicit MonMonGetMap(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonReadCommand(_mon, _monmon, _cct)
  { }

  bool handles_command(const string &prefix) {
    return (prefix == "mon getmap" || prefix == "mon dump");
  }

  virtual bool do_preprocess(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      const MonMap &monmap) final;
};


struct MonMonFeatureLs : public MonMonReadCommand
{
  explicit MonMonFeatureLs(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonReadCommand(_mon, _monmon, _cct)
  { }

  bool handles_command(const string &prefix) {
    return (prefix == "mon feature ls");
  }

  bool do_preprocess(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      const MonMap &monmap);
};

#endif // CEPH_MONMAPMONITOR_CMDS_H
