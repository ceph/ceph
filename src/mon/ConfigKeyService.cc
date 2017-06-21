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
#include "mon/ConfigKeyService.h"
#include "mon/MonitorDBStore.h"
#include "mon/OSDMonitor.h"
#include "common/errno.h"
#include "include/stringify.h"

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
const string ConfigKeyService::META_PREFIX = "mon_config_key_meta";
const string ConfigKeyService::META_KEY = "meta";

void ConfigKeyService::refresh()
{
  version_t had_version = version;
  bufferlist bl;
  mon->store->get(META_PREFIX, META_KEY, bl);
  list<string> changed_keys;
  uint32_t total_keys = 0;
  if (bl.length()) {
    auto p = bl.begin();
    ::decode(version, p);
    while (!p.end()) {
      string k;
      ::decode(k, p);
      changed_keys.push_back(k);
      ::decode(total_keys, p);
    }
    dout(20) << __func__ << " meta shows version " << version << " and "
	     << changed_keys.size() << " changed keys" << dendl;
  } else {
    version = 0;
  }

  if (had_version && version == had_version + 1) {
    for (auto& k : changed_keys) {
      bufferlist bl;
      int r = mon->store->get(STORE_PREFIX, k, bl);
      if (r == -ENOENT) {
	config_keys.erase(k);
      } else if (r == 0) {
	config_keys[k] = bl;
      } else {
	derr << __func__ << " got " << cpp_strerror(r) << dendl;
	ceph_abort();
      }
    }
    dout(10) << __func__ << " refreshed v" << version << " with "
	     << changed_keys.size() << " changed keys" << dendl;
    if (total_keys == config_keys.size()) {
      goto out;
    }
    derr << " have " << config_keys.size() << " expected " << total_keys
	 << ", doing full reload" << dendl;
  } else if (had_version && had_version == version) {
    dout(20) << __func__ << " still v" << version << ", no change" << dendl;
    if (total_keys == config_keys.size()) {
      goto out;
    }
    derr << " have " << config_keys.size() << " but expected " << total_keys
	 << ", doing full reload" << dendl;
  }

  {
    config_keys.clear();
    KeyValueDB::Iterator iter =
      mon->store->get_iterator(STORE_PREFIX);
    while (iter->valid()) {
      config_keys[iter->key()] = iter->value();
      iter->next();
    }
  }
  dout(10) << __func__ << " full reload v" << version << " with "
	   << config_keys.size() << " keys" << dendl;

out:
  metabl.clear();
  ::encode(version + 1, metabl);
}

void ConfigKeyService::_store_put(
  MonitorDBStore::TransactionRef t,
  const string &key,
  bufferlist& bl)
{
  t->put(STORE_PREFIX, key, bl);
  config_keys[key] = bl;
  ::encode(key, metabl);
  ::encode((uint32_t)config_keys.size(), metabl);
}

void ConfigKeyService::_store_delete(
    MonitorDBStore::TransactionRef t,
    const string &key)
{
  t->erase(STORE_PREFIX, key);
  config_keys.erase(key);
  ::encode(key, metabl);
  ::encode((uint32_t)config_keys.size(), metabl);
}

void ConfigKeyService::_store_finish(
  MonitorDBStore::TransactionRef t,
  Context *cb)
{
  t->put(META_PREFIX, META_KEY, metabl);
  if (cb)
    paxos->queue_pending_finisher(cb);
  paxos->trigger_propose();
}

void ConfigKeyService::_store_delete_prefix(
    MonitorDBStore::TransactionRef t,
    const string &prefix)
{
  for (auto p = config_keys.lower_bound(prefix);
       p != config_keys.end();
       ++p) {
    size_t q = p->first.find(prefix);
    if (q == string::npos || q > 0) {
      break;
    }
    _store_delete(t, p->first);
  }
}

void ConfigKeyService::store_put(const string &key, bufferlist &bl, Context *cb)
{
  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  _store_put(t, key, bl);
  _store_finish(t, cb);
}

void ConfigKeyService::store_delete(const string &key, Context *cb)
{
  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  _store_delete(t, key);
  _store_finish(t, cb);
}

int ConfigKeyService::store_get(const string &key, bufferlist &bl)
{
  auto p = config_keys.find(key);
  if (p == config_keys.end()) {
    return -ENOENT;
  }
  bl = p->second;
  return 0;
}

bool ConfigKeyService::store_exists(const string &key)
{
  return config_keys.count(key);
}

void ConfigKeyService::store_list(stringstream &ss)
{
  JSONFormatter f(true);
  f.open_array_section("keys");
  for (auto& p : config_keys) {
    f.dump_string("key", p.first);
  }
  f.close_section();
  f.flush(ss);
}

bool ConfigKeyService::store_has_prefix(const string &prefix)
{
  auto p = config_keys.lower_bound(prefix);
  if (p == config_keys.end()) {
    return false;
  }
  size_t q = p->first.find(prefix);
  if (q != string::npos && q == 0) {
    return true;
  }
  return false;
}

void ConfigKeyService::store_dump(stringstream &ss)
{
  JSONFormatter f(true);
  f.open_object_section("config-key store");
  for (auto& p : config_keys) {
    f.dump_string(p.first.c_str(), p.second.to_str());
  }
  f.close_section();
  f.flush(ss);
}

void ConfigKeyService::get_store_prefixes(set<string>& s)
{
  s.insert(STORE_PREFIX);
}

bool ConfigKeyService::service_dispatch(MonOpRequestRef op)
{
  Message *m = op->get_req();
  assert(m != NULL);
  dout(10) << __func__ << " " << *m << dendl;

  if (!in_quorum()) {
    dout(1) << __func__ << " not in quorum -- waiting" << dendl;
    paxos->wait_for_readable(op, new Monitor::C_RetryMessage(mon, op));
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
    ss << "set " << key;
    store_put(key, data,
	      new Monitor::C_Command(mon, op, 0, ss.str(), 0));
    // return for now; we'll put the message once it's done.
    return true;

  } else if (prefix == "config-key mput" ||
	     prefix == "config-key mput-prefix") {
    if (!mon->is_leader()) {
      mon->forward_request_leader(op);
      // we forward the message; so return now.
      return true;
    }

    string key_prefix;
    if (prefix == "config-key mput-prefix") {
      if (!cmd_getval(g_ceph_context, cmdmap, "key_prefix", key_prefix)) {
	ret = -EINVAL;
	goto out;
      }
      if (key_prefix.size() == 0) {
	ss << "key_prefix must be non-empty";
	ret = -EINVAL;
	goto out;
      }
    }
    vector<string> kv;
    if (!cmd_getval(g_ceph_context, cmdmap, "keysandvalues", kv)) {
      ret = -EINVAL;
      goto out;
    }
    if (kv.empty() || kv.size() % 2) {
      ss << "must have same number of keys and values";
      ret = -EINVAL;
      goto out;
    }
    auto p = kv.begin();
    while (p != kv.end()) {
      const string& k = *p++;
      if (key_prefix.size() &&
	  k.find(key_prefix) != 0) {
	ss << "key " << k << " not under prefix " << key_prefix;
	ret = -EINVAL;
	goto out;
      }
      const string& v = *p++;
      if (v.size() > (size_t) g_conf->mon_config_key_max_entry_size) {
	ss << "key " << k << " value is " << v.size() << " > "
	   << g_conf->mon_config_key_max_entry_size;
	ret = -EFBIG; // File too large
	goto out;
      }
    }
    MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
    if (key_prefix.size()) {
      _store_delete_prefix(t, key_prefix);
    }
    p = kv.begin();
    while (p != kv.end()) {
      const string& k = *p++;
      bufferlist bl;
      bl.append(*p++);
      _store_put(t, k, bl);
    }
    _store_finish(t, new Monitor::C_Command(mon, op, 0, ss.str(), 0));
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

  } else if (prefix == "config-key rm-prefix") {
    if (!mon->is_leader()) {
      mon->forward_request_leader(op);
      return true;
    }

    string key_prefix;
    if (!cmd_getval(g_ceph_context, cmdmap, "key_prefix", key_prefix)) {
      ret = -EINVAL;
      goto out;
    }
    MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
    _store_delete_prefix(t, key_prefix);
    _store_finish(t, new Monitor::C_Command(mon, op, 0, ss.str(), 0));
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

  } else if (prefix == "config-key dump") {
    stringstream tmp_ss;
    store_dump(tmp_ss);
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

string _get_dmcrypt_prefix(const uuid_d& uuid, const string k)
{
  return "dm-crypt/osd/" + stringify(uuid) + "/" + k;
}

int ConfigKeyService::validate_osd_destroy(
    const int32_t id,
    const uuid_d& uuid)
{
  string dmcrypt_prefix = _get_dmcrypt_prefix(uuid, "");
  string daemon_prefix =
    "daemon-private/osd." + stringify(id) + "/";

  if (!store_has_prefix(dmcrypt_prefix) &&
      !store_has_prefix(daemon_prefix)) {
    return -ENOENT;
  }
  return 0;
}

void ConfigKeyService::do_osd_destroy(int32_t id, uuid_d& uuid)
{
  string dmcrypt_prefix = _get_dmcrypt_prefix(uuid, "");
  string daemon_prefix =
    "daemon-private/osd." + stringify(id) + "/";

  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  for (auto p : { dmcrypt_prefix, daemon_prefix }) {
    _store_delete_prefix(t, p);
  }
  _store_finish(t, nullptr);
}

int ConfigKeyService::validate_osd_new(
    const uuid_d& uuid,
    const string& dmcrypt_key,
    stringstream& ss)
{
  string dmcrypt_prefix = _get_dmcrypt_prefix(uuid, "luks");
  bufferlist value;
  value.append(dmcrypt_key);

  if (store_exists(dmcrypt_prefix)) {
    bufferlist existing_value;
    int err = store_get(dmcrypt_prefix, existing_value);
    if (err < 0) {
      dout(10) << __func__ << " unable to get dm-crypt key from store (r = "
               << err << ")" << dendl;
      return err;
    }
    if (existing_value.contents_equal(value)) {
      // both values match; this will be an idempotent op.
      return EEXIST;
    }
    ss << "dm-crypt key already exists and does not match";
    return -EEXIST;
  }
  return 0;
}

void ConfigKeyService::do_osd_new(
    const uuid_d& uuid,
    const string& dmcrypt_key)
{
  assert(paxos->is_plugged());

  string dmcrypt_key_prefix = _get_dmcrypt_prefix(uuid, "luks");
  bufferlist dmcrypt_key_value;
  dmcrypt_key_value.append(dmcrypt_key);
  // store_put() will call trigger_propose
  store_put(dmcrypt_key_prefix, dmcrypt_key_value, nullptr);
}
