// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/predicate.hpp>

#include "mon/Monitor.h"
#include "mon/ConfigMonitor.h"
#include "mon/KVMonitor.h"
#include "mon/MgrMonitor.h"
#include "mon/OSDMonitor.h"
#include "messages/MConfig.h"
#include "messages/MGetConfig.h"
#include "messages/MMonCommand.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/cmdparse.h"
#include "include/stringify.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)
using namespace TOPNSPC::common;

using namespace std::literals;

using std::cerr;
using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::setfill;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;
using std::unique_ptr;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;
static ostream& _prefix(std::ostream *_dout, const Monitor &mon,
                        const ConfigMonitor *hmon) {
  return *_dout << "mon." << mon.name << "@" << mon.rank
		<< "(" << mon.get_state_name() << ").config ";
}

const string KEY_PREFIX("config/");
const string HISTORY_PREFIX("config-history/");

ConfigMonitor::ConfigMonitor(Monitor &m, Paxos &p, const string& service_name)
  : PaxosService(m, p, service_name) {
}

void ConfigMonitor::init()
{
  dout(10) << __func__ << dendl;
}

void ConfigMonitor::create_initial()
{
  dout(10) << __func__ << dendl;
  version = 0;
  pending.clear();
}

void ConfigMonitor::update_from_paxos(bool *need_bootstrap)
{
  if (version == get_last_committed()) {
    return;
  }
  version = get_last_committed();
  dout(10) << __func__ << " " << version << dendl;
  load_config();
  check_all_subs();
}

void ConfigMonitor::create_pending()
{
  dout(10) << " " << version << dendl;
  pending.clear();
  pending_description.clear();
}

void ConfigMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << " " << (version+1) << dendl;
  put_last_committed(t, version+1);
  // NOTE: caller should have done encode_pending_to_kvmon() and
  // kvmon->propose_pending() to commit the actual config changes.
}

void ConfigMonitor::encode_pending_to_kvmon()
{
  // we need to pass our data through KVMonitor so that it is properly
  // versioned and shared with subscribers.
  for (auto& [key, value] : pending_cleanup) {
    if (pending.count(key) == 0) {
      derr << __func__ << " repair: adjusting config key '" << key << "'"
	   << dendl;
      pending[key] = value;
    }
  }
  pending_cleanup.clear();

  // TODO: record changed sections (osd, mds.foo, rack:bar, ...)

  string history = HISTORY_PREFIX + stringify(version+1) + "/";
  {
    bufferlist metabl;
    ::encode(ceph_clock_now(), metabl);
    ::encode(pending_description, metabl);
    mon.kvmon()->enqueue_set(history, metabl);
  }
  for (auto& p : pending) {
    string key = KEY_PREFIX + p.first;
    auto q = current.find(p.first);
    if (q != current.end()) {
      if (p.second && *p.second == q->second) {
	continue;
      }
      mon.kvmon()->enqueue_set(history + "-" + p.first, q->second);
    } else if (!p.second) {
      continue;
    }
    if (p.second) {
      dout(20) << __func__ << " set " << key << dendl;
      mon.kvmon()->enqueue_set(key, *p.second);
      mon.kvmon()->enqueue_set(history + "+" + p.first, *p.second);
   } else {
      dout(20) << __func__ << " rm " << key << dendl;
      mon.kvmon()->enqueue_rm(key);
    }
  }
}

version_t ConfigMonitor::get_trim_to() const
{
  // we don't actually need *any* old states, but keep a few.
  if (version > 5) {
    return version - 5;
  }
  return 0;
}

bool ConfigMonitor::preprocess_query(MonOpRequestRef op)
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

bool ConfigMonitor::preprocess_command(MonOpRequestRef op)
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

  bufferlist odata;
  if (prefix == "config help") {
    stringstream ss;
    string name;
    cmd_getval(cmdmap, "key", name);
    name = ConfFile::normalize_key_name(name);
    const Option *opt = g_conf().find_option(name);
    if (!opt) {
      opt = mon.mgrmon()->find_module_option(name);
    }
    if (opt) {
      if (f) {
	f->dump_object("option", *opt);
      } else {
	opt->print(&ss);
      }
    } else {
      ss << "configuration option '" << name << "' not recognized";
      err = -ENOENT;
      goto reply;
    }
    if (f) {
      f->flush(odata);
    } else {
      odata.append(ss.str());
    }
  } else if (prefix == "config ls") {
    ostringstream ss;
    if (f) {
      f->open_array_section("options");
    }
    for (auto& i : ceph_options) {
      if (f) {
	f->dump_string("option", i.name);
      } else {
	ss << i.name << "\n";
      }
    }
    for (auto& i : mon.mgrmon()->get_mgr_module_options()) {
      if (f) {
	f->dump_string("option", i.first);
      } else {
	ss << i.first << "\n";
      }
    }
    if (f) {
      f->close_section();
      f->flush(odata);
    } else {
      odata.append(ss.str());
    }
  } else if (prefix == "config dump") {
    list<pair<string,Section*>> sections = {
      make_pair("global", &config_map.global)
    };
    for (string type : { "mon", "mgr", "osd", "mds", "client" }) {
      auto i = config_map.by_type.find(type);
      if (i != config_map.by_type.end()) {
	sections.push_back(make_pair(i->first, &i->second));
      }
      auto j = config_map.by_id.lower_bound(type);
      while (j != config_map.by_id.end() &&
	     j->first.find(type) == 0) {
	sections.push_back(make_pair(j->first, &j->second));
	++j;
      }
    }
    TextTable tbl;
    if (!f) {
      tbl.define_column("WHO", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("MASK", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("LEVEL", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("OPTION", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("VALUE", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("RO", TextTable::LEFT, TextTable::LEFT);
    } else {
      f->open_array_section("config");
    }
    for (auto s : sections) {
      for (auto& i : s.second->options) {
	if (!f) {
	  tbl << s.first;
	  tbl << i.second.mask.to_str();
	  tbl << Option::level_to_str(i.second.opt->level);
          tbl << i.first;
	  tbl << i.second.raw_value;
	  tbl << (i.second.opt->can_update_at_runtime() ? "" : "*");
	  tbl << TextTable::endrow;
	} else {
	  f->open_object_section("option");
	  f->dump_string("section", s.first);
	  i.second.dump(f.get());
	  f->close_section();
	}
      }
    }
    if (!f) {
      odata.append(stringify(tbl));
    } else {
      f->close_section();
      f->flush(odata);
    }
  } else if (prefix == "config get") {
    string who, name;
    cmd_getval(cmdmap, "who", who);

    EntityName entity;
    if (!entity.from_str(who) &&
	!entity.from_str(who + ".")) {
      ss << "unrecognized entity '" << who << "'";
      err = -EINVAL;
      goto reply;
    }

    map<string,string> crush_location;
    string device_class;
    if (entity.is_osd()) {
      mon.osdmon()->osdmap.crush->get_full_location(who, &crush_location);
      int id = atoi(entity.get_id().c_str());
      const char *c = mon.osdmon()->osdmap.crush->get_item_class(id);
      if (c) {
	device_class = c;
      }
      dout(10) << __func__ << " crush_location " << crush_location
	       << " class " << device_class << dendl;
    }

    std::map<std::string,pair<std::string,const MaskedOption*>> src;
    auto config = config_map.generate_entity_map(
      entity,
      crush_location,
      mon.osdmon()->osdmap.crush.get(),
      device_class,
      &src);

    if (cmd_getval(cmdmap, "key", name)) {
      name = ConfFile::normalize_key_name(name);
      const Option *opt = g_conf().find_option(name);
      if (!opt) {
	opt = mon.mgrmon()->find_module_option(name);
      }
      if (!opt) {
        ss << "unrecognized key '" << name << "'";
	err = -ENOENT;
	goto reply;
      }
      if (opt->has_flag(Option::FLAG_NO_MON_UPDATE)) {
	// handle special options
	if (name == "fsid") {
	  odata.append(stringify(mon.monmap->get_fsid()));
	  odata.append("\n");
	  goto reply;
	}
	err = -EINVAL;
	ss << name << " is special and cannot be stored by the mon";
	goto reply;
      }
      // get a single value
      auto p = config.find(name);
      if (p != config.end()) {
	odata.append(p->second);
	odata.append("\n");
	goto reply;
      }
      if (!entity.is_client() &&
	  opt->daemon_value != Option::value_t{}) {
	odata.append(Option::to_str(opt->daemon_value));
      } else {
	odata.append(Option::to_str(opt->value));
      }
      odata.append("\n");
    } else {
      // dump all (non-default) values for this entity
      TextTable tbl;
      if (!f) {
	tbl.define_column("WHO", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("MASK", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("LEVEL", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("OPTION", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("VALUE", TextTable::LEFT, TextTable::LEFT);
	tbl.define_column("RO", TextTable::LEFT, TextTable::LEFT);
      } else {
	f->open_object_section("config");
      }
      auto p = config.begin();
      auto q = src.begin();
      for (; p != config.end(); ++p, ++q) {
	if (name.size() && p->first != name) {
	  continue;
	}
	if (!f) {
	  tbl << q->second.first;
	  tbl << q->second.second->mask.to_str();
	  tbl << Option::level_to_str(q->second.second->opt->level);
	  tbl << p->first;
	  tbl << p->second;
	  tbl << (q->second.second->opt->can_update_at_runtime() ? "" : "*");
	  tbl << TextTable::endrow;
	} else {
	  f->open_object_section(p->first.c_str());
	  f->dump_string("value", p->second);
	  f->dump_string("section", q->second.first);
	  f->dump_object("mask", q->second.second->mask);
	  f->dump_bool("can_update_at_runtime",
		       q->second.second->opt->can_update_at_runtime());
	  f->close_section();
	}
      }
      if (!f) {
	odata.append(stringify(tbl));
      } else {
	f->close_section();
	f->flush(odata);
      }
    }
  } else if (prefix == "config log") {
    int64_t num = 10;
    cmd_getval(cmdmap, "num", num);
    ostringstream ds;
    if (f) {
      f->open_array_section("changesets");
    }
    for (version_t v = version; v > version - std::min(version, (version_t)num); --v) {
      ConfigChangeSet ch;
      load_changeset(v, &ch);
      if (f) {
	f->dump_object("changeset", ch);
      } else {
	ch.print(ds);
      }
    }
    if (f) {
      f->close_section();
      f->flush(odata);
    } else {
      odata.append(ds.str());
    }
  } else if (prefix == "config generate-minimal-conf") {
    ostringstream conf;
    conf << "# minimal ceph.conf for " << mon.monmap->get_fsid() << "\n";

    // the basics
    conf << "[global]\n";
    conf << "\tfsid = " << mon.monmap->get_fsid() << "\n";
    conf << "\tmon_host = ";
    for (auto i = mon.monmap->mon_info.begin();
	 i != mon.monmap->mon_info.end();
	 ++i) {
      if (i != mon.monmap->mon_info.begin()) {
	conf << " ";
      }
      if (i->second.public_addrs.size() == 1 &&
	  i->second.public_addrs.front().is_legacy() &&
	  i->second.public_addrs.front().get_port() == CEPH_MON_PORT_LEGACY) {
	// if this is a legacy addr on the legacy default port, then
	// use the legacy-compatible formatting so that old clients
	// can use this config.  new code will see the :6789 and correctly
	// interpret this as a v1 address.
	conf << i->second.public_addrs.get_legacy_str();
      } else {
	conf << i->second.public_addrs;
      }
    }
    conf << "\n";
    conf << config_map.global.get_minimal_conf();
    for (auto m : { &config_map.by_type, &config_map.by_id }) {
      for (auto& i : *m) {
	auto s = i.second.get_minimal_conf();
	if (s.size()) {
	  conf << "\n[" << i.first << "]\n" << s;
	}
      }
    }
    odata.append(conf.str());
    err = 0;
  } else {
    return false;
  }

  reply:
  mon.reply_command(op, err, ss.str(), odata, get_last_committed());
  return true;
}

void ConfigMonitor::handle_get_config(MonOpRequestRef op)
{
  auto m = op->get_req<MGetConfig>();
  dout(10) << __func__ << " " << m->name << " host " << m->host << dendl;

  const OSDMap& osdmap = mon.osdmon()->osdmap;
  map<string,string> crush_location;
  osdmap.crush->get_full_location(m->host, &crush_location);
  auto out = config_map.generate_entity_map(
    m->name,
    crush_location,
    osdmap.crush.get(),
    m->device_class);
  dout(20) << " config is " << out << dendl;
  m->get_connection()->send_message(new MConfig{std::move(out)});
}

bool ConfigMonitor::prepare_update(MonOpRequestRef op)
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

bool ConfigMonitor::prepare_command(MonOpRequestRef op)
{
  auto m = op->get_req<MMonCommand>();
  std::stringstream ss;
  int err = -EINVAL;

  // make sure kv is writeable.
  if (!mon.kvmon()->is_writeable()) {
    dout(10) << __func__ << " waiting for kv mon to be writeable" << dendl;
    mon.kvmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
    return false;
  }

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);
  bufferlist odata;

  if (prefix == "config set" ||
      prefix == "config rm") {
    string who;
    string name, value;
    bool force = false;
    cmd_getval(cmdmap, "who", who);
    cmd_getval(cmdmap, "name", name);
    cmd_getval(cmdmap, "value", value);
    cmd_getval(cmdmap, "force", force);
    name = ConfFile::normalize_key_name(name);
    
    if (prefix == "config set" && !force) {
      const Option *opt = g_conf().find_option(name);
      if (!opt) {
	opt = mon.mgrmon()->find_module_option(name);
      }
      if (!opt) {
	ss << "unrecognized config option '" << name << "'";
	err = -EINVAL;
	goto reply;
      }

      Option::value_t real_value;
      string errstr;
      err = opt->parse_value(value, &real_value, &errstr, &value);
      if (err < 0) {
	ss << "error parsing value: " << errstr;
	goto reply;
      }

      if (opt->has_flag(Option::FLAG_NO_MON_UPDATE)) {
	err = -EINVAL;
	ss << name << " is special and cannot be stored by the mon";
	goto reply;
      }
    }

    string section;
    OptionMask mask;
    if (!ConfigMap::parse_mask(who, &section, &mask)) {
      ss << "unrecognized config target '" << who << "'";
      err = -EINVAL;
      goto reply;
    }

    string key;
    if (section.size()) {
      key += section + "/";
    } else {
      key += "global/";
    }
    string mask_str = mask.to_str();
    if (mask_str.size()) {
      key += mask_str + "/";
    }
    key += name;

    if (prefix == "config set") {
      bufferlist bl;
      bl.append(value);
      pending[key] = bl;
    } else {
      pending[key].reset();
    }
    goto update;
  } else if (prefix == "config reset") {
    int64_t revert_to = -1;
    cmd_getval(cmdmap, "num", revert_to);
    if (revert_to < 0 ||
        revert_to > (int64_t)version) {
      err = -EINVAL;
      ss << "must specify a valid historical version to revert to; "
         << "see 'ceph config log' for a list of avialable configuration "
         << "historical versions";
      goto reply;
    }
    if (revert_to == (int64_t)version) {
      err = 0;
      goto reply;
    }
    for (int64_t v = version; v > revert_to; --v) {
      ConfigChangeSet ch;
      load_changeset(v, &ch);
      for (auto& i : ch.diff) {
	if (i.second.first) {
	  bufferlist bl;
	  bl.append(*i.second.first);
	  pending[i.first] = bl;
	} else if (i.second.second) {
	  pending[i.first].reset();
	}
      }
    }
    pending_description = string("reset to ") + stringify(revert_to);
    goto update;
  } else if (prefix == "config assimilate-conf") {
    ConfFile cf;
    bufferlist bl = m->get_data();
    err = cf.parse_bufferlist(&bl, &ss);
    if (err < 0) {
      goto reply;
    }
    bool updated = false;
    ostringstream newconf;
    for (auto& [section, s] : cf) {
      dout(20) << __func__ << " [" << section << "]" << dendl;
      bool did_section = false;
      for (auto& [key, val] : s) {
	Option::value_t real_value;
	string value;
	string errstr;
	if (key.empty()) {
	  continue;
	}
	// a known and worthy option?
	const Option *o = g_conf().find_option(key);
	if (!o) {
	  o = mon.mgrmon()->find_module_option(key);
	}
	if (!o ||
	    (o->flags & Option::FLAG_NO_MON_UPDATE) ||
	    (o->flags & Option::FLAG_CLUSTER_CREATE)) {
	  goto skip;
	}
	// normalize
	err = o->parse_value(val, &real_value, &errstr, &value);
	if (err < 0) {
	  dout(20) << __func__ << " failed to parse " << key << " = '"
		   << val << "'" << dendl;
	  goto skip;
	}
	// does it conflict with an existing value?
	{
	  const Section *s = config_map.find_section(section);
	  if (s) {
	    auto k = s->options.find(key);
	    if (k != s->options.end()) {
	      if (value != k->second.raw_value) {
		dout(20) << __func__ << " have " << key
			 << " = " << k->second.raw_value
			 << " (not " << value << ")" << dendl;
		goto skip;
	      }
	      dout(20) << __func__ << " already have " << key
		       << " = " << k->second.raw_value << dendl;
	      continue;
	    }
	  }
	}
	dout(20) << __func__ << "  add " << key << " = " << value
		 << " (" << val << ")" << dendl;
	{
	  bufferlist bl;
	  bl.append(value);
	  pending[section + "/" + key] = bl;
	  updated = true;
	}
	continue;

       skip:
	dout(20) << __func__ << " skip " << key << " = " << value
		 << " (" << val << ")" << dendl;
	if (!did_section) {
	  newconf << "\n[" << section << "]\n";
	  did_section = true;
	}
	newconf << "\t" << key << " = " << val << "\n";
      }
    }
    odata.append(newconf.str());
    if (updated) {
      goto update;
    }
  } else {
    ss << "unknown command " << prefix;
    err = -EINVAL;
  }

reply:
  mon.reply_command(op, err, ss.str(), odata, get_last_committed());
  return false;

update:
  // see if there is an actual change
  auto p = pending.begin();
  while (p != pending.end()) {
    auto q = current.find(p->first);
    if (p->second && q != current.end() && *p->second == q->second) {
      // set to same value
      p = pending.erase(p);
    } else if (!p->second && q == current.end()) {
      // erasing non-existent value
      p = pending.erase(p);
    } else {
      ++p;
    }
  }
  if (pending.empty()) {
    err = 0;
    goto reply;
  }
  // immediately propose *with* KV mon
  encode_pending_to_kvmon();
  paxos.plug();
  mon.kvmon()->propose_pending();
  paxos.unplug();
  force_immediate_propose();
  wait_for_finished_proposal(
    op,
    new Monitor::C_Command(
      mon, op, 0, ss.str(), odata,
      get_last_committed() + 1));
  return true;
}

void ConfigMonitor::tick()
{
  if (!is_active() || !mon.is_leader()) {
    return;
  }
  dout(10) << __func__ << dendl;
  bool changed = false;
  if (!pending_cleanup.empty()) {
    changed = true;
  }
  if (changed && mon.kvmon()->is_writeable()) {
    paxos.plug();
    encode_pending_to_kvmon();
    mon.kvmon()->propose_pending();
    paxos.unplug();
    propose_pending();
  }
}

void ConfigMonitor::on_active()
{
}

void ConfigMonitor::load_config()
{
  std::map<std::string,std::string> renamed_pacific = {
    { "mon_osd_blacklist_default_expire", "mon_osd_blocklist_default_expire" },
    { "mon_mds_blacklist_interval", "mon_mds_blocklist_interval" },
    { "mon_mgr_blacklist_interval", "mon_mgr_blocklist_interval" },
    { "rbd_blacklist_on_break_lock", "rbd_blocklist_on_break_lock" },
    { "rbd_blacklist_expire_seconds", "rbd_blocklist_expire_seconds" },
    { "mds_session_blacklist_on_timeout", "mds_session_blocklist_on_timeout" },
    { "mds_session_blacklist_on_evict", "mds_session_blocklist_on_evict" },
  };

  unsigned num = 0;
  KeyValueDB::Iterator it = mon.store->get_iterator(KV_PREFIX);
  it->lower_bound(KEY_PREFIX);
  config_map.clear();
  current.clear();
  pending_cleanup.clear();
  while (it->valid() &&
	 it->key().compare(0, KEY_PREFIX.size(), KEY_PREFIX) == 0) {
    string key = it->key().substr(KEY_PREFIX.size());
    string value = it->value().to_str();

    current[key] = it->value();

    string name;
    string who;
    config_map.parse_key(key, &name, &who);

    // has this option been renamed?
    {
      auto p = renamed_pacific.find(name);
      if (p != renamed_pacific.end()) {
	if (mon.monmap->min_mon_release >= ceph_release_t::pacific) {
	  // schedule a cleanup
	  pending_cleanup[key].reset();
	  pending_cleanup[who + "/" + p->second] = it->value();
	}
	// continue loading under the new name
	name = p->second;
      }
    }

    const Option *opt = g_conf().find_option(name);
    if (!opt) {
      opt = mon.mgrmon()->find_module_option(name);
    }
    if (!opt) {
      dout(10) << __func__ << " unrecognized option '" << name << "'" << dendl;
      config_map.stray_options.push_back(
	std::unique_ptr<Option>(
	  new Option(name, Option::TYPE_STR, Option::LEVEL_UNKNOWN)));
      opt = config_map.stray_options.back().get();
    }

    string err;
    int r = opt->pre_validate(&value, &err);
    if (r < 0) {
      dout(10) << __func__ << " pre-validate failed on '" << name << "' = '"
	       << value << "' for " << name << dendl;
    }
    
    MaskedOption mopt(opt);
    mopt.raw_value = value;
    mopt.localized_name = name;
    string section_name;
    if (who.size() &&
	!ConfigMap::parse_mask(who, &section_name, &mopt.mask)) {
      derr << __func__ << " invalid mask for key " << key << dendl;
      pending_cleanup[key].reset();
    } else if (opt->has_flag(Option::FLAG_NO_MON_UPDATE)) {
      dout(10) << __func__ << " NO_MON_UPDATE option '"
	       << name << "' = '" << value << "' for " << name
	       << dendl;
      pending_cleanup[key].reset();
    } else {
      if (section_name.empty()) {
	// we prefer global/$option instead of just $option
	derr << __func__ << " adding global/ prefix to key '" << key << "'"
	     << dendl;
	pending_cleanup[key].reset();
	pending_cleanup["global/"s + key] = it->value();
      }
      Section *section = &config_map.global;;
      if (section_name.size() && section_name != "global") {
	if (section_name.find('.') != std::string::npos) {
	  section = &config_map.by_id[section_name];
	} else {
	  section = &config_map.by_type[section_name];
	}
      }
      section->options.insert(make_pair(name, std::move(mopt)));
      ++num;
    }
    it->next();
  }
  dout(10) << __func__ << " got " << num << " keys" << dendl;

  // refresh our own config
  {
    const OSDMap& osdmap = mon.osdmon()->osdmap;
    map<string,string> crush_location;
    osdmap.crush->get_full_location(g_conf()->host, &crush_location);
    auto out = config_map.generate_entity_map(
      g_conf()->name,
      crush_location,
      osdmap.crush.get(),
      string{}); // no device class
    g_conf().set_mon_vals(g_ceph_context, out, nullptr);
  }
}

void ConfigMonitor::load_changeset(version_t v, ConfigChangeSet *ch)
{
  ch->version = v;
  string prefix = HISTORY_PREFIX + stringify(v) + "/";
  KeyValueDB::Iterator it = mon.store->get_iterator(KV_PREFIX);
  it->lower_bound(prefix);
  while (it->valid() && it->key().find(prefix) == 0) {
    if (it->key() == prefix) {
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      try {
	decode(ch->stamp, p);
	decode(ch->name, p);
      }
      catch (ceph::buffer::error& e) {
	derr << __func__ << " failure decoding changeset " << v << dendl;
      }
    } else {
      char op = it->key()[prefix.length()];
      string key = it->key().substr(prefix.length() + 1);
      if (op == '-') {
	ch->diff[key].first = it->value().to_str();
      } else if (op == '+') {
	ch->diff[key].second = it->value().to_str();
      }
    }
    it->next();
  }
}

bool ConfigMonitor::refresh_config(MonSession *s)
{
  const OSDMap& osdmap = mon.osdmon()->osdmap;
  map<string,string> crush_location;
  if (s->remote_host.size()) {
    osdmap.crush->get_full_location(s->remote_host, &crush_location);
    dout(10) << __func__ << " crush_location for remote_host " << s->remote_host
	     << " is " << crush_location << dendl;
  }

  string device_class;
  if (s->name.is_osd()) {
    osdmap.crush->get_full_location(s->entity_name.to_str(), &crush_location);
    const char *c = osdmap.crush->get_item_class(s->name.num());
    if (c) {
      device_class = c;
      dout(10) << __func__ << " device_class " << device_class << dendl;
    }
  }

  dout(20) << __func__ << " " << s->entity_name << " crush " << crush_location
	   << " device_class " << device_class << dendl;
  auto out = config_map.generate_entity_map(
    s->entity_name,
    crush_location,
    osdmap.crush.get(),
    device_class);

  if (out == s->last_config && s->any_config) {
    dout(20) << __func__ << " no change, " << out << dendl;
    return false;
  }
  // removing this to hide sensitive data going into logs
  // leaving this for debugging purposes
 //  dout(20) << __func__ << " " << out << dendl;
  s->last_config = std::move(out);
  s->any_config = true;
  return true;
}

bool ConfigMonitor::maybe_send_config(MonSession *s)
{
  bool changed = refresh_config(s);
  dout(10) << __func__ << " to " << s->name << " "
	   << (changed ? "(changed)" : "(unchanged)")
	   << dendl;
  if (changed) {
    send_config(s);
  }
  return changed;
}

void ConfigMonitor::send_config(MonSession *s)
{
  dout(10) << __func__ << " to " << s->name << dendl;
  auto m = new MConfig(s->last_config);
  s->con->send_message(m);
}

void ConfigMonitor::check_sub(MonSession *s)
{
  if (!s->authenticated) {
    dout(20) << __func__ << " not authenticated " << s->entity_name << dendl;
    return;
  }
  auto p = s->sub_map.find("config");
  if (p != s->sub_map.end()) {
    check_sub(p->second);
  }
}

void ConfigMonitor::check_sub(Subscription *sub)
{
  dout(10) << __func__
	   << " next " << sub->next
	   << " have " << version << dendl;
  if (sub->next <= version) {
    maybe_send_config(sub->session);
    if (sub->onetime) {
      mon.with_session_map([sub](MonSessionMap& session_map) {
	  session_map.remove_sub(sub);
	});
    } else {
      sub->next = version + 1;
    }
  }
}

void ConfigMonitor::check_all_subs()
{
  dout(10) << __func__ << dendl;
  auto subs = mon.session_map.subs.find("config");
  if (subs == mon.session_map.subs.end()) {
    return;
  }
  int updated = 0, total = 0;
  auto p = subs->second->begin();
  while (!p.end()) {
    auto sub = *p;
    ++p;
    ++total;
    if (maybe_send_config(sub->session)) {
      ++updated;
    }
  }
  dout(10) << __func__ << " updated " << updated << " / " << total << dendl;
}
