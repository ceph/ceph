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

#ifndef CEPH_OSDMONITOR_CMDS_H
#define CEPH_OSDMONITOR_CMDS_H

#include <sstream>

#include "mon/Monitor.h"
#include "mon/OSDMonitor.h"
#include "mon/MonOpRequest.h"

#include "mon/commands/Command.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"
#include "crush/CrushTreeDumper.h"

#include "osd/OSDMap.h"

#include "include/types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/dout.h"
#include "common/cmdparse.h"


// prepare / write commands

struct OSDMonWriteCommand :
  public WriteCommand<OSDMonitor, OSDMap, OSDMap::Incremental>
{
  explicit OSDMonWriteCommand(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    WriteCommand<OSDMonitor, OSDMap, OSDMap::Incremental>(_mon, _osdmon, _cct)
  { }

  virtual ~OSDMonWriteCommand() { }
};

struct OSDMonSetCrushmap : public OSDMonWriteCommand
{
  explicit OSDMonSetCrushmap(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    OSDMonWriteCommand(_mon, _osdmon, _cct)
  { }

  virtual ~OSDMonSetCrushmap() { }

  virtual bool handles_command(const string &prefix) final {
    return (prefix == "osd setcrushmap" || prefix == "osd crush set");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      OSDMap::Incremental &pending_inc,
      OSDMap &osdmap) final;
};

struct OSDMonCrushSetStrawBuckets : public OSDMonWriteCommand
{
  explicit OSDMonCrushSetStrawBuckets(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    OSDMonWriteCommand(_mon, _osdmon, _cct)
  { }

  virtual ~OSDMonCrushSetStrawBuckets() { }

  virtual bool handles_command(const string &prefix) final {
    return (prefix == "osd crush set-all-straw-buckets-to-straw2");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      OSDMap::Incremental &pending_inc,
      OSDMap &osdmap) final;
};

struct OSDMonCrushSetDeviceClass : public OSDMonWriteCommand
{
  explicit OSDMonCrushSetDeviceClass(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    OSDMonWriteCommand(_mon, _osdmon, _cct)
  { }

  virtual ~OSDMonCrushSetDeviceClass() { }

  virtual bool handles_command(const string &prefix) final {
    return (prefix == "osd crush set-device-class");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      OSDMap::Incremental &pending_inc,
      OSDMap &osdmap) final; 
};


struct OSDMonCrushRemoveDeviceClass : public OSDMonWriteCommand
{
  explicit OSDMonCrushRemoveDeviceClass(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    OSDMonWriteCommand(_mon, _osdmon, _cct)
  { }
  virtual ~OSDMonCrushRemoveDeviceClass() { }

  virtual bool handles_command(const string &prefix) {
    return (prefix == "osd crush rm-device-class");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      OSDMap::Incremental &pending_inc,
      OSDMap &osdmap) final;
};

struct OSDMonCrushClassCreate : public OSDMonWriteCommand
{
  explicit OSDMonCrushClassCreate(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    OSDMonWriteCommand(_mon, _osdmon, _cct)
  { }
  virtual ~OSDMonCrushClassCreate() { }

  virtual bool handles_command(const string &prefix) {
    return (prefix == "osd crush class create");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      OSDMap::Incremental &pending_inc,
      OSDMap &osdmap) final;
};


struct OSDMonCrushClassRemove : public OSDMonWriteCommand
{
  explicit OSDMonCrushClassRemove(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    OSDMonWriteCommand(_mon, _osdmon, _cct)
  { }
  virtual ~OSDMonCrushClassRemove() { }

  virtual bool handles_command(const string &prefix) {
    return (prefix == "osd crush class rm");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      OSDMap::Incremental &pending_inc,
      OSDMap &osdmap) final;
};


struct OSDMonCrushClassRename : public OSDMonWriteCommand
{
  explicit OSDMonCrushClassRename(
      Monitor *_mon,
      OSDMonitor *_osdmon,
      CephContext *_cct) :
    OSDMonWriteCommand(_mon, _osdmon, _cct)
  { }
  virtual ~OSDMonCrushClassRename() { }

  virtual bool handles_command(const string &prefix) {
    return (prefix == "osd crush class rename");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      OSDMap::Incremental &pending_inc,
      OSDMap &osdmap) final;
};





#endif // CEPH_OSDMONITOR_CMDS_H

