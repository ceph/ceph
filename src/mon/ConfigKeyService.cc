// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sstream>
#include <stdlib.h>
#include <limits.h>

#include <boost/intrusive_ptr.hpp>
#include "include/assert.h"

#include "mon/Monitor.h"
#include "mon/QuorumService.h"
#include "mon/ConfigKeyService.h"
#include "mon/MonitorDBStore.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon,
                        const ConfigKeyService *service) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ")." << service->get_name()
                << "(" << service->get_epoch() << ") ";
}

const string ConfigKeyService::STORE_PREFIX = "mon_config_key";

int ConfigKeyService::store_get(string key, bufferlist &bl)
{
  if (!store_exists(key))
    return -ENOENT;

  return mon->store->get(STORE_PREFIX, key, bl);
}

void ConfigKeyService::store_put(string key, bufferlist &bl, Context *cb)
{
  bufferlist proposal_bl;
  MonitorDBStore::Transaction t;
  t.put(STORE_PREFIX, key, bl);
  t.encode(proposal_bl);

  paxos->propose_new_value(proposal_bl, cb);
}

void ConfigKeyService::store_delete(string key, Context *cb)
{
  bufferlist proposal_bl;
  MonitorDBStore::Transaction t;
  t.erase(STORE_PREFIX, key);
  t.encode(proposal_bl);
  paxos->propose_new_value(proposal_bl, cb);
}

bool ConfigKeyService::store_exists(string key)
{
  return mon->store->exists(STORE_PREFIX, key);
}

void ConfigKeyService::store_list(stringstream &ss)
{
  KeyValueDB::Iterator iter =
    mon->store->get_iterator(STORE_PREFIX);

  JSONFormatter f(true);
  f.open_array_section("keys");

  while (iter->valid()) {
    string key(iter->key());
    f.dump_string("key", key);
    iter->next();
  }
  f.close_section();
  f.flush(ss);
}


bool ConfigKeyService::service_dispatch(Message *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  if (!in_quorum()) {
    dout(1) << __func__ << " not in quorum -- ignore message" << dendl;
    m->put();
    return false;
  }

  assert(m != NULL);
  assert(m->get_type() == MSG_MON_COMMAND);

  MMonCommand *cmd = static_cast<MMonCommand*>(m);

  assert(!cmd->cmd.empty());
  assert(cmd->cmd[0] == "config-key");

  int ret = 0;
  stringstream ss;
  bufferlist rdata;

  if (cmd->cmd.size() < 2) {
    ret = -EINVAL;
    ss << "usage: config-key <get|put|list|exists|delete> [<key>]";
    goto out;
  }

  if (cmd->cmd[1] == "get") {
    if (cmd->cmd.size() != 3) {
      ret = -EINVAL;
      ss << "usage: config-key get <key> -o <file>";
      goto out;
    }
    ret = store_get(cmd->cmd[2], rdata);
    if (ret < 0) {
      assert(!rdata.length());
      ss << "error obtaining '" << cmd->cmd[2] << "': "
         << cpp_strerror(ret);
      goto out;
    }
    ss << "obtained '" << cmd->cmd[2] << "'";
  } else if (cmd->cmd[1] == "put") {
    if (!mon->is_leader()) {
      mon->forward_request_leader(cmd);
      // we forward the message; so return now.
      return true;
    }

    bufferlist data;
    if (cmd->cmd.size() < 3 || cmd->cmd.size() > 4) {
      ret = -EINVAL;
      ss << "usage: store put <key> [-i <file>|<value>]";
      goto out;
    } else if (cmd->cmd.size() == 4) {
      // they specified a value in the command instead of a file
      data.append(cmd->cmd[3]);
    } else if (cmd->get_data_len() > 0) {
      // they specified '-i <file>'
      data = cmd->get_data();
    }
    if (data.length() > (size_t) g_conf->mon_config_key_max_entry_size) {
      ret = -EFBIG; // File too large
      ss << "error: entry size limited to "
         << g_conf->mon_config_key_max_entry_size << " bytes. "
         << "Use 'mon config key max entry size' to manually adjust";
      goto out;
    }
    // we'll reply to the message once the proposal has been handled
    store_put(cmd->cmd[2], data,
        new Monitor::C_Command(mon, cmd, 0, "value stored", 0));
    // return for now; we'll put the message once it's done.
    return true;
  } else if (cmd->cmd[1] == "delete") {
    if (!mon->is_leader()) {
      mon->forward_request_leader(cmd);
      return true;
    }

    if (cmd->cmd.size() != 3) {
      ret = -EINVAL;
      ss << "usage: config-key delete <key>";
      goto out;
    }
    if (!store_exists(cmd->cmd[2])) {
      ret = 0;
      ss << "no such key '" << cmd->cmd[2] << "'";
      goto out;
    }
    store_delete(cmd->cmd[2],
        new Monitor::C_Command(mon, cmd, 0, "key deleted", 0));
    // return for now; we'll put the message once it's done
    return true;
  } else if (cmd->cmd[1] == "exists") {
    if (cmd->cmd.size() != 3) {
      ret = -EINVAL;
      ss << "usage: config-key exists <key>";
      goto out;
    }
    bool exists = store_exists(cmd->cmd[2]);
    ss << "key '" << cmd->cmd[2] << "'";
    if (exists) {
      ss << " exists";
      ret = 0;
    } else {
      ss << " doesn't exist";
      ret = -ENOENT;
    }
  } else if (cmd->cmd[1] == "list") {
    if (cmd->cmd.size() > 2) {
      ret = -EINVAL;
      ss << "usage: config-key list";
      goto out;
    }
    stringstream tmp_ss;
    store_list(tmp_ss);
    rdata.append(tmp_ss);
    ret = 0;
  }

out:
  if (!cmd->get_source().is_mon()) {
    string rs = ss.str();
    mon->reply_command(cmd, ret, rs, rdata, 0);
  } else {
    cmd->put();
  }

  return (ret == 0);
}

