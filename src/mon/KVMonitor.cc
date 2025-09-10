// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mon/Monitor.h"
#include "mon/KVMonitor.h"
#include "include/stringify.h"
#include "messages/MKVData.h"
#include "messages/MMonCommand.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)

using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::stringstream;

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
  check_all_subs();
}

void KVMonitor::create_pending()
{
  dout(10) << " " << version << dendl;
  pending.clear();
  pending_range_deletes.clear();
}

void KVMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << " " << (version+1) << dendl;
  put_last_committed(t, version+1);

  // record the delta for this commit point
  bufferlist bl;

  // Feature-based encoding strategy
  // Check if all monitors in quorum support KV range operations
  bool can_use_range_ops = HAVE_FEATURE(mon.get_quorum_con_features(), KV_RANGE_OPS);

  if (can_use_range_ops) {
    ENCODE_START(1, 1, bl);
    encode(pending, bl);     // key operations (always present)
    encode(pending_range_deletes, bl);  // range operations (new in v2)
    ENCODE_FINISH(bl);

    dout(15) << __func__ << " using v2 format with range ops, "
             << "range_ops=" << pending_range_deletes.size()
             << " key_ops=" << pending.size() << dendl;
  } else {
    // Legacy encoding (no version header, compatible with all versions)
    encode(pending, bl);

    dout(15) << __func__ << " using legacy format, key_ops=" << pending.size() << dendl;
  }

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

  // handle range deletions
  for (auto& rd : pending_range_deletes) {
    if (rd.start.empty() && rd.end.empty()) {
      // delete all keys with prefix
      dout(10) << __func__ << " rm_prefix " << rd.prefix
               << " (deleting all keys with this prefix)" << dendl;
      t->erase_range(KV_PREFIX, rd.prefix, rd.prefix + "~");
    } else {
      string start_key = rd.prefix + (rd.start.empty() ? "" : "/" + rd.start);
      string end_key = rd.prefix + (rd.end.empty() ? "~" : "/" + rd.end);
      dout(10) << __func__ << " rm_range [" << start_key << ", " << end_key
               << ") prefix=" << rd.prefix << " start=" << rd.start
               << " end=" << rd.end << dendl;
      t->erase_range(KV_PREFIX, start_key, end_key);
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
  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
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
    pending[key].reset();
    goto update;
  }
  else if (prefix == "config-key rm-range") {
    // Check if range operations are supported
    bool range_ops_supported = HAVE_FEATURE(mon.get_quorum_con_features(), KV_RANGE_OPS);

    if (!range_ops_supported) {
      err = -EOPNOTSUPP;
      ss << "range operations not supported: cluster does not have 'kv-range-ops' feature enabled. "
         << "All monitors must be upgraded to support this feature.";
      goto reply;
    }

    // For range deletion, we need prefix parameter instead of key
    string prefix_key;
    if (!cmd_getval(cmdmap, "key", prefix_key)) {
      err = -EINVAL;
      ss << "must specify a prefix for range deletion";
      goto reply;
    }

    string start, end;
    cmd_getval(cmdmap, "start", start);
    cmd_getval(cmdmap, "end", end);

    // Use helper method for validation
    err = _validate_range_params(prefix_key, start, end, ss);
    if (err != 0) {
      goto reply;
    }

    dout(10) << __func__ << " rm-range key_prefix=" << prefix_key
             << " start=" << start << " end=" << end << dendl;

    // Add to pending range deletions
    pending_range_deletes.emplace_back(prefix_key, start, end);
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
  if (pending.empty() && pending_range_deletes.empty()) {
    err = 0;
    goto reply;
  }
  force_immediate_propose();  // faster response
  wait_for_commit(
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
  ceph_assert(is_writeable());

  string dmcrypt_prefix = _get_dmcrypt_prefix(uuid, "");
  string daemon_prefix =
    "daemon-private/osd." + stringify(id) + "/";

  for (auto& prefix : { dmcrypt_prefix, daemon_prefix }) {
    KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);
    iter->lower_bound(prefix);
    if (iter->key().find(prefix) != 0) {
      break;
    }
    pending[iter->key()].reset();
  }

  propose_pending();
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
  ceph_assert(is_writeable());

  string dmcrypt_key_prefix = _get_dmcrypt_prefix(uuid, "luks");
  bufferlist dmcrypt_key_value;
  dmcrypt_key_value.append(dmcrypt_key);

  pending[dmcrypt_key_prefix] = dmcrypt_key_value;

  propose_pending();
}


void KVMonitor::check_sub(MonSession *s)
{
  if (!s->authenticated) {
    dout(20) << __func__ << " not authenticated " << s->entity_name << dendl;
    return;
  }
  for (auto& p : s->sub_map) {
    if (p.first.find("kv:") == 0) {
      check_sub(p.second);
    }
  }
}

void KVMonitor::check_sub(Subscription *sub)
{
  dout(10) << __func__
	   << " next " << sub->next
	   << " have " << version << dendl;
  if (sub->next <= version) {
    maybe_send_update(sub);
    if (sub->onetime) {
      mon.with_session_map([sub](MonSessionMap& session_map) {
	  session_map.remove_sub(sub);
	});
    }
  }
}

void KVMonitor::check_all_subs()
{
  dout(10) << __func__ << dendl;
  int updated = 0, total = 0;
  for (auto& i : mon.session_map.subs) {
    if (i.first.find("kv:") == 0) {
      auto p = i.second->begin();
      while (!p.end()) {
	auto sub = *p;
	++p;
	++total;
	if (maybe_send_update(sub)) {
	  ++updated;
	}
      }
    }
  }
  dout(10) << __func__ << " updated " << updated << " / " << total << dendl;
}

bool KVMonitor::maybe_send_update(Subscription *sub)
{
  if (sub->next > version) {
    return false;
  }

  auto m = new MKVData;
  m->prefix = sub->type.substr(3);
  m->version = version;

  if (sub->next && sub->next > get_first_committed()) {
    // incremental
    m->incremental = true;

    for (version_t cur = sub->next; cur <= version; ++cur) {
      bufferlist bl;
      int err = get_version(cur, bl);
      ceph_assert(err == 0);

      auto p = bl.cbegin();

      std::map<std::string, std::optional<ceph::buffer::list>> key_ops;
      std::vector<RangeDeleteOp> range_ops;

      bool can_use_range_ops = HAVE_FEATURE(mon.get_quorum_con_features(), KV_RANGE_OPS);

      if (can_use_range_ops) {
        // New versioned format
        DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, p);
        decode(key_ops, p);
        decode(range_ops, p);
        DECODE_FINISH(p);
      } else {
        // Legacy format (no version header)
        decode(key_ops, p);
      }

      // Handle key operations
      for (auto& i : key_ops) {
        if (i.first.find(m->prefix) == 0) {
          m->data[i.first] = i.second;
        }
      }


      // iter all config prefix key in range_ops, convert range to all matched keys, and add to m->data
      for (auto& rd : range_ops) {
        // Check if this range delete affects the subscriber's prefix
        if (m->prefix.find(rd.prefix) == 0 || rd.prefix.find(m->prefix) == 0) {
          // Enumerate all keys that would be deleted by this range operation
          KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);

          // Calculate the actual key range that will be deleted
          string start_key, end_key;
          if (rd.start.empty() && rd.end.empty()) {
            // Delete all keys with prefix
            start_key = rd.prefix;
          } else {
            start_key = rd.prefix + (rd.start.empty() ? "" : "/" + rd.start);
            end_key = rd.prefix + (rd.end.empty() ? "~" : "/" + rd.end);
          }

          dout(20) << __func__ << " range delete: enumerating keys in range ["
                   << start_key << ", " << end_key << ") for subscriber " << m->prefix << dendl;

          // Iterate through all keys in the deletion range
          iter->lower_bound(start_key);
          while (iter->valid() && iter->key() < end_key) {
            string key = iter->key();
            // Only include keys that match the subscriber's prefix
            if (key.find(m->prefix) == 0) {
              // Mark this key as deleted (std::nullopt indicates deletion)
              m->data[key] = std::nullopt;
              dout(20) << __func__ << " marking key for deletion: " << key << dendl;
            }
            iter->next();
          }
        }
      }
    }

    dout(10) << __func__ << " incremental keys for " << m->prefix
	     << ", v " << sub->next << ".." << version
	     << ", " << m->data.size() << " keys"
	     << dendl;
  } else {
    m->incremental = false;

    KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);
    iter->lower_bound(m->prefix);
    while (iter->valid() &&
	   iter->key().find(m->prefix) == 0) {
      m->data[iter->key()] = iter->value();
      iter->next();
    }

    dout(10) << __func__ << " sending full dump of " << m->prefix
	     << ", " << m->data.size() << " keys"
	     << dendl;
  }
  sub->session->con->send_message(m);
  sub->next = version + 1;
  return true;
}

int KVMonitor::_validate_range_params(const std::string &prefix,
                                      const std::string &start,
                                      const std::string &end,
                                      std::ostream &ss) const {
  // Basic prefix validation
  if (prefix.empty()) {
    ss << "prefix cannot be empty";
    return -EINVAL;
  }

  // Check for invalid characters
  for (char c : prefix) {
    if (c < 0x20 || c > 0x7E) { // Non-printable ASCII
      ss << "prefix contains invalid characters";
      return -EINVAL;
    }
  }

  // Validate range if both start and end are specified
  if (!start.empty() && !end.empty()) {
    if (start >= end) {
      ss << "invalid range: start '" << start << "' >= end '" << end << "'";
      return -EINVAL;
    }

    // Check for very large ranges that might indicate typos
    if (start.length() == 1 && end.length() == 1 &&
        (end[0] - start[0]) > 26) { // More than alphabet
      ss << "suspiciously large single-character range";
      return -EINVAL;
    }
  }

  // Check for suspicious prefix patterns
  if (prefix == "/" || prefix == "." || prefix == "..") {
    ss << "potentially dangerous prefix pattern";
    return -EINVAL;
  }

  return 0;
}