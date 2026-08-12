// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mon/Monitor.h"
#include "mon/KVMonitor.h"
#include "mon/Paxos.h"
#include "common/debug.h"
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


/*
 * Marks a delta as carrying range removals. The original delta format
 * begins with the __u32 entry count of a map, so a count of 0xffffffff
 * cannot occur and is safe to use as a discriminator.
 */
static constexpr __u32 DELTA_RANGE_MARKER = 0xffffffff;

std::string KVMonitor::prefix_upper_bound(const string& p)
{
  string b = p;
  while (!b.empty()) {
    // keys are arbitrary byte strings, so compare unsigned
    auto c = static_cast<unsigned char>(b.back());
    if (c != 0xff) {
      b.back() = static_cast<char>(c + 1);
      return b;
    }
    b.pop_back();
  }
  return string();               // unbounded above
}

void KVMonitor::RangeDeleteOp::get_bounds(string *begin, string *end_bound) const
{
  if (start.empty() && end.empty()) {
    *begin = prefix;
    *end_bound = KVMonitor::prefix_upper_bound(prefix);
    return;
  }
  *begin = start.empty() ? prefix : prefix + "/" + start;
  *end_bound = end.empty() ? KVMonitor::prefix_upper_bound(prefix)
                           : prefix + "/" + end;
}

bool KVMonitor::RangeDeleteOp::overlaps_prefix(const string& p) const
{
  string begin, end_bound;
  get_bounds(&begin, &end_bound);
  string p_end = KVMonitor::prefix_upper_bound(p);
  // [begin, end_bound) vs [p, p_end), where an empty upper bound is +inf
  if (!end_bound.empty() && end_bound <= p) {
    return false;
  }
  if (!p_end.empty() && p_end <= begin) {
    return false;
  }
  return true;
}

void KVMonitor::encode_delta(
  const std::map<std::string,std::optional<ceph::buffer::list>>& key_ops,
  const std::vector<RangeDeleteOp>& range_ops,
  bufferlist *bl)
{
  if (range_ops.empty()) {
    // original format, readable by a monitor without range support
    encode(key_ops, *bl);
    return;
  }
  encode(DELTA_RANGE_MARKER, *bl);
  ENCODE_START(1, 1, *bl);
  encode(key_ops, *bl);
  encode(range_ops, *bl);
  ENCODE_FINISH(*bl);
}

void KVMonitor::decode_delta(
  const bufferlist& bl,
  std::map<std::string,std::optional<ceph::buffer::list>> *key_ops,
  std::vector<RangeDeleteOp> *range_ops)
{
  auto p = bl.cbegin();
  if (bl.length() >= sizeof(__u32)) {
    auto peek = p;
    __u32 marker;
    decode(marker, peek);
    if (marker == DELTA_RANGE_MARKER) {
      p = peek;
      DECODE_START(1, p);
      decode(*key_ops, p);
      decode(*range_ops, p);
      DECODE_FINISH(p);
      return;
    }
  }
  decode(*key_ops, p);
}

int KVMonitor::validate_range_params(const string& prefix,
                                     const string& start,
                                     const string& end,
                                     std::ostream& ss)
{
  if (prefix.empty()) {
    ss << "prefix cannot be empty";
    return -EINVAL;
  }
  // Arguments come from the CLI; restricting them to printable ASCII also
  // keeps the derived bounds well defined.
  for (auto arg : {&prefix, &start, &end}) {
    for (unsigned char c : *arg) {
      if (c < 0x20 || c > 0x7e) {
        ss << "arguments must be printable ASCII";
        return -EINVAL;
      }
    }
  }
  if (!start.empty() && !end.empty() && start >= end) {
    ss << "invalid range: start '" << start << "' >= end '" << end << "'";
    return -EINVAL;
  }
  return 0;
}

KVMonitor::KVMonitor(Monitor &m, Paxos &p, const string& service_name)
  : PaxosService(m, p, service_name) {
}

bool KVMonitor::_range_ops_supported() const
{
  return mon.get_quorum_mon_features().contains_all(
    ceph::features::mon::FEATURE_KV_RANGE_OPS);
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
  pending_range_deletes.clear();
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
  encode_delta(pending, pending_range_deletes, &bl);
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

  // Range removals go in as a single erase_range, which lets the store use
  // its own range delete rather than one operation per key. Applying these
  // after the puts above means a removal covering a key set in this same
  // proposal wins, which matches the order the delta is interpreted in.
  for (auto& rd : pending_range_deletes) {
    string begin, end_bound;
    rd.get_bounds(&begin, &end_bound);
    if (end_bound.empty()) {
      // Unbounded above: there is no key that sorts beyond an all-0xff
      // prefix, so fall back to iterating what is actually there.
      KeyValueDB::Iterator iter = mon.store->get_iterator(KV_PREFIX);
      for (iter->lower_bound(begin); iter->valid(); iter->next()) {
        dout(20) << __func__ << " rm " << iter->key() << dendl;
        t->erase(KV_PREFIX, iter->key());
      }
      continue;
    }
    dout(10) << __func__ << " rm_range [" << begin << ", " << end_bound
             << ") prefix=" << rd.prefix << " start=" << rd.start
             << " end=" << rd.end << dendl;
    t->erase_range(KV_PREFIX, begin, end_bound);
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
    // A delta recording a range removal cannot be read by a monitor without
    // range support, so require the whole quorum to have it first.
    if (!_range_ops_supported()) {
      err = -EOPNOTSUPP;
      ss << "range removal requires all monitors to support the "
         << "kv_range_ops feature";
      goto reply;
    }

    // for rm-range the "key" argument is a prefix, not a single key
    string start, end;
    cmd_getval(cmdmap, "start", start);
    cmd_getval(cmdmap, "end", end);

    err = validate_range_params(key, start, end, ss);
    if (err < 0) {
      goto reply;
    }

    dout(10) << __func__ << " rm-range prefix=" << key
             << " start=" << start << " end=" << end << dendl;

    ss << "keys deleted";
    pending_range_deletes.emplace_back(key, start, end);
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

  // A range removal cannot be expressed as an incremental update without
  // naming every key it covered, which is the cost the range removal exists
  // to avoid. Send a full dump instead: that is bounded by what remains
  // under the prefix, not by how much was deleted.
  // A range removal is forwarded as a range: keys are ordered, so a
  // subscriber holding them in an ordered container can apply the interval
  // itself. That keeps the update independent of how many keys were removed.
  // A subscriber too old to decode a range has to be resynced instead.
  bool sub_takes_ranges = HAVE_FEATURE(
    sub->session->con->get_features(), SERVER_UMBRELLA);
  bool needs_resync = false;

  if (sub->next && sub->next > get_first_committed() && !sub_takes_ranges) {
    for (version_t cur = sub->next; cur <= version && !needs_resync; ++cur) {
      bufferlist bl;
      int err = get_version(cur, bl);
      ceph_assert(err == 0);

      std::map<std::string,std::optional<ceph::buffer::list>> key_ops;
      std::vector<RangeDeleteOp> range_ops;
      decode_delta(bl, &key_ops, &range_ops);

      for (auto& rd : range_ops) {
        if (rd.overlaps_prefix(m->prefix)) {
          needs_resync = true;
          break;
        }
      }
    }
  }

  if (sub->next && sub->next > get_first_committed() && !needs_resync) {
    // incremental
    m->incremental = true;

    for (version_t cur = sub->next; cur <= version; ++cur) {
      bufferlist bl;
      int err = get_version(cur, bl);
      ceph_assert(err == 0);

      std::map<std::string,std::optional<ceph::buffer::list>> pending;
      std::vector<RangeDeleteOp> range_ops;
      decode_delta(bl, &pending, &range_ops);

      for (auto& i : pending) {
	if (i.first.find(m->prefix) == 0) {
	  m->data[i.first] = i.second;
	}
      }

      for (auto& rd : range_ops) {
        if (!rd.overlaps_prefix(m->prefix)) {
          continue;
        }
        std::string begin, end_bound;
        rd.get_bounds(&begin, &end_bound);
        // Clamp to what the subscriber actually watches, so it never has to
        // reason about keys outside its own prefix.
        std::string sub_end = prefix_upper_bound(m->prefix);
        if (begin < m->prefix) {
          begin = m->prefix;
        }
        if (!sub_end.empty() &&
            (end_bound.empty() || end_bound > sub_end)) {
          end_bound = sub_end;
        }
        m->range_deletes.emplace_back(begin, end_bound);
      }
    }

    dout(10) << __func__ << " incremental keys for " << m->prefix
	     << ", v " << sub->next << ".." << version
	     << ", " << m->data.size() << " keys"
	     << ", " << m->range_deletes.size() << " ranges"
	     << dendl;
  } else {
    if (needs_resync) {
      dout(10) << __func__ << " subscriber for " << m->prefix
               << " cannot decode range removals, sending full dump" << dendl;
    }
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
