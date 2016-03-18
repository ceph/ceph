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

#include "mon/Monitor.h"
#include "mon/QuorumService.h"
#include "mon/ConfigKeyService.h"
#include "mon/MonitorDBStore.h"

#include "common/config.h"
#include "common/cmdparse.h"
#include "common/errno.h"

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
  return mon->store->get(STORE_PREFIX, key, bl);
}

void ConfigKeyService::get_store_prefixes(set<string>& s)
{
  s.insert(STORE_PREFIX);
}

void ConfigKeyService::store_put(string key, bufferlist &bl, Context *cb)
{
  bufferlist proposal_bl;
  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  t->put(STORE_PREFIX, key, bl);
  if (cb)
    paxos->queue_pending_finisher(cb);
  paxos->trigger_propose();
}

void ConfigKeyService::store_delete(string key, Context *cb)
{
  bufferlist proposal_bl;
  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  t->erase(STORE_PREFIX, key);
  if (cb)
    paxos->queue_pending_finisher(cb);
  paxos->trigger_propose();
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


bool ConfigKeyService::service_dispatch(MonOpRequestRef op)
{
  Message *m = op->get_req();
  assert(m != NULL);
  dout(10) << __func__ << " " << *m << dendl;

  if (!in_quorum()) {
    dout(1) << __func__ << " not in quorum -- ignore message" << dendl;
    return false;
  }

  assert(m->get_type() == MSG_MON_COMMAND);

  MMonCommand *cmd = static_cast<MMonCommand*>(m);

  assert(!cmd->cmd.empty());

  int ret = 0;
  stringstream ss;
  bufferlist rdata;

  string prefix;
  map<string, cmd_vartype> cmdmap;

  if (!cmdmap_from_json(cmd->cmd, &cmdmap, ss)) {
    return false;
  }

  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  string key;
  cmd_getval(g_ceph_context, cmdmap, "key", key);

  if (prefix == "config-key get") {
    ret = store_get(key, rdata);
    if (ret < 0) {
      assert(!rdata.length());
      ss << "error obtaining '" << key << "': " << cpp_strerror(ret);
      goto out;
    }
    ss << "obtained '" << key << "'";

  } else if (prefix == "config-key put") {
    if (!mon->is_leader()) {
      mon->forward_request_leader(op);
      // we forward the message; so return now.
      return true;
    }

    bufferlist data;
    string val;
    if (cmd_getval(g_ceph_context, cmdmap, "val", val)) {
      // they specified a value in the command instead of a file
      data.append(val);
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
    store_put(key, data,
        new Monitor::C_Command(mon, op, 0, "value stored", 0));
    // return for now; we'll put the message once it's done.
    return true;

  } else if (prefix == "config-key del" ||
             prefix == "config-key rm") {
    if (!mon->is_leader()) {
      mon->forward_request_leader(op);
      return true;
    }

    if (!store_exists(key)) {
      ret = 0;
      ss << "no such key '" << key << "'";
      goto out;
    }
    store_delete(key, new Monitor::C_Command(mon, op, 0, "key deleted", 0));
    // return for now; we'll put the message once it's done
    return true;

  } else if (prefix == "config-key exists") {
    bool exists = store_exists(key);
    ss << "key '" << key << "'";
    if (exists) {
      ss << " exists";
      ret = 0;
    } else {
      ss << " doesn't exist";
      ret = -ENOENT;
    }

  } else if (prefix == "config-key list") {
    stringstream tmp_ss;
    store_list(tmp_ss);
    rdata.append(tmp_ss);
    ret = 0;
  }

out:
  if (!cmd->get_source().is_mon()) {
    string rs = ss.str();
    mon->reply_command(op, ret, rs, rdata, 0);
  }

  return (ret == 0);
}

