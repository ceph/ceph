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

#include "mon/Monitor.h"
#include "mon/AuthMonitor.h"
#include "mon/MonOpRequest.h"

#include "mon/commands/Command.h"
#include "mon/commands/authmon_cmds.h"

#include "messages/MMonCommand.h"

#include "include/types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/dout.h"
#include "common/cmdparse.h"

#define dout_subsys ceph_subsys_mon


// read commands
//

bool AuthMonAuthExport::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f)
{
  string entity_name;
  EntityName entity;
  bool ret = AuthMonCommon::get_entities(cmdmap, entity, entity_name, ss);
  if (!ret) {
    reply(op, -EINVAL, ss, get_last_committed());
    return true;
  }

  KeyRing keyring;
  export_keyring(keyring);
  if (!entity_name.empty()) {
    EntityAuth eauth;
    if (keyring.get_auth(entity, eauth)) {
      KeyRing kr;
      kr.add(entity, eauth);
      if (f)
	kr.encode_formatted("auth", f.get(), rdata);
      else
	kr.encode_plaintext(rdata);
      ss << "export " << eauth;
    } else {
      ss << "no key for " << eauth;
      reply(op, -ENOENT, ss, get_last_committed());
      return true;
    }
  } else {
    if (f)
      keyring.encode_formatted("auth", f.get(), rdata);
    else
      keyring.encode_plaintext(rdata);

    ss << "exported master keyring";
  }

  reply_with_data(op, 0, ss, rdata, get_last_committed());
  return true;
};

bool AuthMonAuthGet::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f)
{
  KeyRing keyring;
  EntityAuth entity_auth;

  string entity_name;
  EntityName entity;
  bool ret = AuthMonCommon::get_entities(cmdmap, entity, entity_name, ss);
  if (!ret) {
    reply(op, -EINVAL, ss, get_last_committed());
    return true;
  }

  if (entity_name.empty()) {
    ss << "invalid command";
    reply(op, -EINVAL, ss, get_last_committed());
    return true;
  }

  int r = 0;
  if(!mon->key_server.get_auth(entity, entity_auth)) {
    ss << "failed to find " << entity_name << " in keyring";
    r = -ENOENT;
  } else {
    keyring.add(entity, entity_auth);
    if (f)
      keyring.encode_formatted("auth", f.get(), rdata);
    else
      keyring.encode_plaintext(rdata);
    ss << "exported keyring for " << entity_name;
    r = 0;
  }

  reply_with_data(op, r, ss, rdata, get_last_committed());
  return true;
}

// write commands
//

bool AuthMonImport::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  bufferlist bl = m->get_data();
  if (bl.length() == 0) {
    ss << "auth import: no data supplied";
    reply(op, -EINVAL, ss, get_last_committed());
    return true;
  }
  auto iter = bl.cbegin();
  KeyRing keyring;
  try {
    decode(keyring, iter);
  } catch (const buffer::error &ex) {
    ss << "error decoding keyring" << " " << ex.what();
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  int err = import_keyring(keyring);
  if (err < 0) {
    ss << "auth import: no caps supplied";
    reply(op, -EINVAL, ss, get_last_committed());
    return true;
  }
  ss << "imported keyring";
  update(op, ss, get_last_committed() + 1); 
  return true;
}
