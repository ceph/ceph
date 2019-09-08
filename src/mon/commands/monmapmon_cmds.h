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
  public ReadCommand<MonmapMonitor, const MonMap&>
{
  explicit MonMonReadCommand(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_ctx) :
    ReadCommand<MonmapMonitor, const MonMap&>(_mon, _monmon, _ctx)
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
  public WriteCommand<MonmapMonitor, MonMap&, const MonMap&>
{
  explicit MonMonWriteCommand(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_ctx) :
    WriteCommand<MonmapMonitor, MonMap&, const MonMap&>(_mon, _monmon, _ctx)
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

// prepare commands (RW)


  /* Please note:
   *
   * Adding or removing monitors may lead to loss of quorum.
   *
   * Because quorum may be lost, it's important to reply something
   * to the user, lest she end up waiting forever for a reply. And
   * no reply will ever be sent until quorum is formed again.
   *
   * On the other hand, this means we're leaking uncommitted state
   * to the user. As such, please be mindful of the reply message.
   *
   * e.g., 'adding monitor mon.foo' is okay ('adding' is an on-going
   * operation and conveys its not-yet-permanent nature); whereas
   * 'added monitor mon.foo' presumes the action has successfully
   * completed and state has been committed, which may not be true.
   *
   */

struct MonMonAdd : public MonMonWriteCommand
{
  explicit MonMonAdd(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonWriteCommand(_mon, _monmon, _cct)
  { }

  virtual ~MonMonAdd() { }

  bool handles_command(const string &prefix) { return prefix == "mon add"; }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      MonMap &pending,
      const MonMap &stable) final;
};

struct MonMonRemove : public MonMonWriteCommand
{
  explicit MonMonRemove(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonWriteCommand(_mon, _monmon, _cct)
  { }

  virtual ~MonMonRemove() { }

  bool handles_command(const string &prefix) {
    return (prefix == "mon rm" || prefix == "mon remove");
  }

  bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      MonMap &pending,
      const MonMap &stable);
};

struct MonMonFeatureSet : public MonMonWriteCommand
{
  explicit MonMonFeatureSet(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonWriteCommand(_mon, _monmon, _cct)
  { }

  virtual ~MonMonFeatureSet() { }

  bool handles_command(const string &prefix) {
    return (prefix == "mon feature set");
  }

  bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      MonMap &pending,
      const MonMap &stable);
};

struct MonMonSetRank : public MonMonWriteCommand
{
  explicit MonMonSetRank(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonWriteCommand(_mon, _monmon, _cct)
  { }

  virtual ~MonMonSetRank() { }

  bool handles_command(const string &prefix) {
    return (prefix == "mon set-rank");
  }

  bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      MonMap &pending,
      const MonMap &stable);
};

struct MonMonSetAddrs : public MonMonWriteCommand
{
  explicit MonMonSetAddrs(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonWriteCommand(_mon, _monmon, _cct)
  { }

  virtual ~MonMonSetAddrs() { }

  virtual bool handles_command(const string &prefix) override {
    return (prefix == "mon set-addrs");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      MonMap &pending,
      const MonMap &stable) final;
};

struct MonMonSetWeight : public MonMonWriteCommand
{
  explicit MonMonSetWeight(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonWriteCommand(_mon, _monmon, _cct)
  { }

  virtual ~MonMonSetWeight() { }

  virtual bool handles_command(const string &prefix) override {
    return (prefix == "mon set-weight");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      MonMap &pending,
      const MonMap &stable) final;
};

struct MonMonEnableMsgr2 : public MonMonWriteCommand
{
  explicit MonMonEnableMsgr2(
      Monitor *_mon,
      MonmapMonitor *_monmon,
      CephContext *_cct) :
    MonMonWriteCommand(_mon, _monmon, _cct)
  { }

  virtual ~MonMonEnableMsgr2() { }

  virtual bool handles_command(const string &prefix) override {
    return (prefix == "mon enable-msgr2");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      MonMap &pending,
      const MonMap &stable) final;
};

#endif // CEPH_MONMAPMONITOR_CMDS_H
