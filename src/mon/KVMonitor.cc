// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mon/Monitor.h"
#include "mon/KVMonitor.h"
#include "include/stringify.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)

static ostream& _prefix(std::ostream *_dout, const Monitor &mon,
                        const KVMonitor *hmon) {
  return *_dout << "mon." << mon.name << "@" << mon.rank
		<< "(" << mon.get_state_name() << ").kv ";
}

const string KV_PREFIX = "mon_config_key";

const int MAX_HISTORY = 50;


static bool is_binary_string(const string& s)
{
  for (auto c : s) {
    // \n and \t are escaped in JSON; other control characters are not.
    if ((c < 0x20 && c != '\n' && c != '\t') || c >= 0x7f) {
      return true;
    }
  }
  return false;
}


KVMonitor::KVMonitor(Monitor &m, Paxos &p, const string& service_name)
  : PaxosService(m, p, service_name) {
}

void KVMonitor::init()
{
  dout(10) << __func__ << dendl;
}

void KVMonitor::create_initial()
{
  dout(10) << __func__ << dendl;
  version = 0;
  pending.clear();
}

void KVMonitor::update_from_paxos(bool *need_bootstrap)
{
  if (version == get_last_committed()) {
    return;
  }
  version = get_last_committed();
  dout(10) << __func__ << " " << version << dendl;
  //check_all_subs();
}

void KVMonitor::create_pending()
{
  dout(10) << " " << version << dendl;
  pending.clear();
}

void KVMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << " " << (version+1) << dendl;
  put_last_committed(t, version+1);

  // record the delta for this commit point
  bufferlist bl;
  encode(pending, bl);
  put_version(t, version+1, bl);
  
  // make actual changes
  for (auto& p : pending) {
    string key = p.first;
    if (p.second) {
      dout(20) << __func__ << " set " << key << dendl;
      t->put(KV_PREFIX, key, *p.second);
    } else {
      dout(20) << __func__ << " rm " << key << dendl;
      t->erase(KV_PREFIX, key);
    }
  }
}

version_t KVMonitor::get_trim_to() const
{
  // we don't need that many old states, but keep a few
  if (version > MAX_HISTORY) {
    return version - MAX_HISTORY;
  }
  return 0;
}

void KVMonitor::get_store_prefixes(set<string>& s) const
{
  s.insert(service_name);
  s.insert(KV_PREFIX);
}

void KVMonitor::tick()
{
  if (!is_active() || !mon.is_leader()) {
    return;
  }
  dout(10) << __func__ << dendl;
}

void KVMonitor::on_active()
{
}


bool KVMonitor::preprocess_query(MonOpRequestRef op)
{
  switch (op->get_req()->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return preprocess_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  }
  return false;
}

bool KVMonitor::preprocess_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  std::stringstream ss;
  int err = 0;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }
  string format;
  cmd_getval(cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);
  string key;
  cmd_getval(cmdmap, "key", key);

  bufferlist odata;

  if (prefix == "config-key get") {
    err = mon.store->get(KV_PREFIX, key, odata);
  }
  else if (prefix == "config-key exists") {
    bool exists = mon.store->exists(KV_PREFIX, key);
    ss << "key '" << key << "'";
    if (exists) {
      ss << " exists";
      err = 0;
    } else {
      ss << " doesn't exist";
      err = -ENOENT;
    }
  }
  else if (prefix == "config-key list" ||
	   prefix == "config-key ls") {
    if (!f) {
      f.reset(Formatter::create("json-pretty"));
    }
    KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);
    f->open_array_section("keys");
    while (iter->valid()) {
      string key(iter->key());
      f->dump_string("key", key);
      iter->next();
    }
    f->close_section();

    stringstream tmp_ss;
    f->flush(tmp_ss);
    odata.append(tmp_ss);
    err = 0;
  }
  else if (prefix == "config-key dump") {
    if (!f) {
      f.reset(Formatter::create("json-pretty"));
    }

    KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);
    if (key.size()) {
      iter->lower_bound(key);
    }
    f->open_object_section("config-key store");
    while (iter->valid()) {
      if (key.size() &&
	  iter->key().find(key) != 0) {
	break;
      }
      string s = iter->value().to_str();
      if (is_binary_string(s)) {
	ostringstream ss;
	ss << "<<< binary blob of length " << s.size() << " >>>";
	f->dump_string(iter->key().c_str(), ss.str());
      } else {
	f->dump_string(iter->key().c_str(), s);
      }
      iter->next();
    }
    f->close_section();
    
    stringstream tmp_ss;
    f->flush(tmp_ss);
    odata.append(tmp_ss);
    err = 0;
  }
  else {
    return false;
  }

  mon.reply_command(op, err, ss.str(), odata, get_last_committed());
  return true;
}

bool KVMonitor::prepare_update(MonOpRequestRef op)
{
  Message *m = op->get_req();
  dout(7) << "prepare_update " << *m
	  << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return prepare_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  }
  return false;
}


bool KVMonitor::prepare_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  std::stringstream ss;
  int err = 0;
  bufferlist odata;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);
  string key;
  if (!cmd_getval(cmdmap, "key", key)) {
    err = -EINVAL;
    ss << "must specify a key";
    goto reply;
  }


  if (prefix == "config-key set" ||
      prefix == "config-key put") {
    bufferlist data;
    string val;
    if (cmd_getval(cmdmap, "val", val)) {
      // they specified a value in the command instead of a file
      data.append(val);
    } else if (m->get_data_len() > 0) {
      // they specified '-i <file>'
      data = m->get_data();
    }
    if (data.length() > (size_t) g_conf()->mon_config_key_max_entry_size) {
      err = -EFBIG; // File too large
      ss << "error: entry size limited to "
         << g_conf()->mon_config_key_max_entry_size << " bytes. "
         << "Use 'mon config key max entry size' to manually adjust";
      goto reply;
    }

    ss << "set " << key;
    pending[key] = data;
    goto update;
  }
  else if (prefix == "config-key del" ||
	   prefix == "config-key rm") {
    ss << "key deleted";
    pending[key] = boost::none;
    goto update;
  }
  else {
    ss << "unknown command " << prefix;
    err = -EINVAL;
  }

reply:
  mon.reply_command(op, err, ss.str(), odata, get_last_committed());
  return false;

update:
  // see if there is an actual change
  if (pending.empty()) {
    err = 0;
    goto reply;
  }
  force_immediate_propose();  // faster response
  wait_for_finished_proposal(
    op,
    new Monitor::C_Command(
      mon, op, 0, ss.str(), odata,
      get_last_committed() + 1));
  return true;
}




static string _get_dmcrypt_prefix(const uuid_d& uuid, const string k)
{
  return "dm-crypt/osd/" + stringify(uuid) + "/" + k;
}

bool KVMonitor::_have_prefix(const string &prefix)
{
  KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);

  while (iter->valid()) {
    string key(iter->key());
    size_t p = key.find(prefix);
    if (p != string::npos && p == 0) {
      return true;
    }
    iter->next();
  }
  return false;
}

int KVMonitor::validate_osd_destroy(
  const int32_t id,
  const uuid_d& uuid)
{
  string dmcrypt_prefix = _get_dmcrypt_prefix(uuid, "");
  string daemon_prefix =
    "daemon-private/osd." + stringify(id) + "/";

  if (!_have_prefix(dmcrypt_prefix) &&
      !_have_prefix(daemon_prefix)) {
    return -ENOENT;
  }
  return 0;
}

void KVMonitor::do_osd_destroy(int32_t id, uuid_d& uuid)
{
  string dmcrypt_prefix = _get_dmcrypt_prefix(uuid, "");
  string daemon_prefix =
    "daemon-private/osd." + stringify(id) + "/";

  for (auto& prefix : { dmcrypt_prefix, daemon_prefix }) {
    KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);
    iter->lower_bound(prefix);
    if (iter->key().find(prefix) != 0) {
      break;
    }
    pending[iter->key()] = boost::none;
  }

  paxos.trigger_propose();
}

int KVMonitor::validate_osd_new(
  const uuid_d& uuid,
  const string& dmcrypt_key,
  stringstream& ss)
{
  string dmcrypt_prefix = _get_dmcrypt_prefix(uuid, "luks");
  bufferlist value;
  value.append(dmcrypt_key);
  
  if (mon.store->exists(KV_PREFIX, dmcrypt_prefix)) {
    bufferlist existing_value;
    int err = mon.store->get(KV_PREFIX, dmcrypt_prefix, existing_value);
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

void KVMonitor::do_osd_new(
  const uuid_d& uuid,
  const string& dmcrypt_key)
{
  ceph_assert(paxos.is_plugged());

  string dmcrypt_key_prefix = _get_dmcrypt_prefix(uuid, "luks");
  bufferlist dmcrypt_key_value;
  dmcrypt_key_value.append(dmcrypt_key);

  pending[dmcrypt_key_prefix] = dmcrypt_key_value;
}
