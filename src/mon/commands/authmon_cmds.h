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

#ifndef CEPH_AUTHMON_CMDS_H
#define CEPH_AUTHMON_CMDS_H

#include "mon/Monitor.h"
#include "mon/AuthMonitor.h"
#include "mon/MonOpRequest.h"

#include "mon/commands/Command.h"

#include "include/types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/dout.h"
#include "common/cmdparse.h"



struct AuthMonCommon
{
  static bool get_entities(
      const cmdmap_t &cmdmap,
      EntityName &entity,
      string &entity_name,
      stringstream &ss){

    cmd_getval(g_ceph_context, cmdmap, "entity", entity_name);
    if (!entity_name.empty() && !entity.from_str(entity_name)) {
      ss << "invalid entity_auth" << entity_name;
      return false;
    }
    return true;
  }
};


struct AuthMonReadCommand : public ReadCommand<AuthMonitor>
{
  explicit AuthMonReadCommand(
      Monitor *_mon,
      AuthMonitor *_authmon,
      CephContext *_cct) :
    ReadCommand<AuthMonitor>(_mon, _authmon, _cct) { }
  virtual ~AuthMonReadCommand() { }

  void export_keyring(KeyRing& keyring) {
    service->export_keyring(keyring);
  }
};


struct AuthMonWriteCommand : public WriteCommand<AuthMonitor>
{
  explicit AuthMonWriteCommand(
      Monitor *_mon,
      AuthMonitor *_authmon,
      CephContext *_cct) :
    WriteCommand<AuthMonitor>(_mon, _authmon, _cct) { }
  virtual ~AuthMonWriteCommand() { }

  int import_keyring(KeyRing &keyring) {
    return service->import_keyring(keyring);
  }
};


using AuthMonReadCommandRef = std::shared_ptr<AuthMonReadCommand>;
using AuthMonWriteCommandRef = std::shared_ptr<AuthMonWriteCommand>;


// read commands

struct AuthMonAuthExport :
  public AuthMonReadCommand
{
  explicit AuthMonAuthExport(
      Monitor *_mon,
      AuthMonitor *_authmon,
      CephContext *_cct) : AuthMonReadCommand(_mon, _authmon, _cct) { }

  virtual bool handles_command(const string &prefix) final {
    return (prefix == "auth export");
  }

  virtual bool do_preprocess(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f) final;
};

struct AuthMonAuthGet : public AuthMonReadCommand
{
  explicit AuthMonAuthGet(
      Monitor *_mon,
      AuthMonitor *_authmon,
      CephContext *_cct) : AuthMonReadCommand(_mon, _authmon, _cct) { }
  virtual ~AuthMonAuthGet() { }

  virtual bool handles_command(const string &prefix) final {
    return (prefix == "auth get");
  }

  virtual bool do_preprocess(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f) final;
};



// write commands

struct AuthMonImport : public AuthMonWriteCommand
{
  explicit AuthMonImport(
      Monitor *_mon,
      AuthMonitor *_authmon,
      CephContext *_cct) : AuthMonWriteCommand(_mon, _authmon, _cct) { }
  virtual ~AuthMonImport() { }

  virtual bool handles_command(const string &prefix) final {
    return (prefix == "auth import");
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f) final;
};



#endif // CEPH_AUTHMON_CMDS_H
