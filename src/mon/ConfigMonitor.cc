// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mon/Monitor.h"
#include "mon/ConfigMonitor.h"
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
static ostream& _prefix(std::ostream *_dout, const Monitor *mon,
                        const ConfigMonitor *hmon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ").config ";
}

const string KEY_PREFIX("config/");

ConfigMonitor::ConfigMonitor(Monitor *m, Paxos *p, const string& service_name)
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
}

void ConfigMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << " " << (version+1) << dendl;
  put_last_committed(t, version+1);

  // TODO: record changed sections (osd, mds.foo, rack:bar, ...)

  for (auto& p : pending) {
    string key = KEY_PREFIX + p.first;
    if (p.second) {
      dout(20) << __func__ << " set " << key << dendl;
      t->put(CONFIG_PREFIX, key, *p.second);
    } else {
      dout(20) << __func__ << " rm " << key << dendl;
      t->erase(CONFIG_PREFIX, key);
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
    return preprocess_command(op);
  }
  return false;
}

static string indent_who(const string& who)
{
  if (who == "global") {
    return who;
  }
  if (who.find('.') == string::npos) {
    return "  " + who;
  }
  return "    " + who;
}

bool ConfigMonitor::preprocess_command(MonOpRequestRef op)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  std::stringstream ss;
  int err = 0;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }
  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  bufferlist odata;
  if (prefix == "config help") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "key", name);
    const Option *opt = g_conf->find_option(name);
    if (!opt) {
      ss << "configuration option '" << name << "' not recognized";
      err = -ENOENT;
      goto reply;
    }
    if (f) {
      f->dump_object("option", *opt);
      f->flush(odata);
    } else {
      stringstream ss;
      opt->print(&ss);
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
	  tbl << indent_who(s.first);
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
    cmd_getval(g_ceph_context, cmdmap, "who", who);

    EntityName entity;
    if (!entity.from_str(who)) {
      ss << "unrecognized entity '" << who << "'";
      err = -EINVAL;
      goto reply;
    }

    map<string,string> crush_location;
    string device_class;
    if (entity.is_osd()) {
      mon->osdmon()->osdmap.crush->get_full_location(who, &crush_location);
      int id = atoi(entity.get_id().c_str());
      const char *c = mon->osdmon()->osdmap.crush->get_item_class(id);
      if (c) {
	device_class = c;
      }
      dout(10) << __func__ << " crush_location " << crush_location
	       << " class " << device_class << dendl;
    }

    std::map<std::string,std::string> config;
    std::map<std::string,pair<std::string,const MaskedOption*>> src;
    config_map.generate_entity_map(
      entity,
      crush_location,
      mon->osdmon()->osdmap.crush.get(),
      device_class,
      &config, &src);

    if (cmd_getval(g_ceph_context, cmdmap, "key", name)) {
      // get a single value
      auto p = config.find(name);
      if (p != config.end()) {
	odata.append(p->second);
	odata.append("\n");
	goto reply;
      }
      const Option *opt = g_conf->find_option(name);
      if (!opt) {
	err = -ENOENT;
	goto reply;
      }
      if (!entity.is_client() &&
	  !boost::get<boost::blank>(&opt->daemon_value)) {
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
  } else {
    return false;
  }

  reply:
  mon->reply_command(op, err, ss.str(), odata, get_last_committed());
  return true;
}

void ConfigMonitor::handle_get_config(MonOpRequestRef op)
{
  MGetConfig *m = static_cast<MGetConfig*>(op->get_req());
  dout(10) << __func__ << " " << m->name << " host " << m->host << dendl;

  const OSDMap& osdmap = mon->osdmon()->osdmap;
  map<string,string> crush_location;
  osdmap.crush->get_full_location(m->host, &crush_location);
  map<string,string> out;
  config_map.generate_entity_map(
    m->name,
    crush_location,
    osdmap.crush.get(),
    m->device_class,
    &out);
  dout(20) << " config is " << out << dendl;
  m->get_connection()->send_message(new MConfig(out));
}

bool ConfigMonitor::prepare_update(MonOpRequestRef op)
{
  Message *m = op->get_req();
  dout(7) << "prepare_update " << *m
	  << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_MON_COMMAND:
    return prepare_command(op);
  }
  return false;
}

bool ConfigMonitor::prepare_command(MonOpRequestRef op)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  std::stringstream ss;
  int err = -EINVAL;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  bufferlist odata;

  if (prefix == "config set" ||
      prefix == "config rm") {
    string who;
    string name, value;
    cmd_getval(g_ceph_context, cmdmap, "who", who);
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    cmd_getval(g_ceph_context, cmdmap, "value", value);

    if (prefix == "config set") {
      const Option *opt = g_conf->find_option(name);
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
      pending[key] = boost::none;
    }
    goto update;
  } else if (prefix == "config assimilate-conf") {
    ConfFile cf;
    deque<string> errors;
    bufferlist bl = m->get_data();
    err = cf.parse_bufferlist(&bl, &errors, &ss);
    if (err < 0) {
      ss << "parse errors: " << errors;
      goto reply;
    }
    bool updated = false;
    ostringstream newconf;
    for (auto i = cf.sections_begin(); i != cf.sections_end(); ++i) {
      string section = i->first;
      const ConfSection& s = i->second;
      dout(20) << __func__ << " [" << section << "]" << dendl;
      bool did_section = false;
      for (auto& j : s.lines) {
	Option::value_t real_value;
	string value;
	string errstr;
	if (!j.key.size()) {
	  continue;
	}
	// a known and worthy option?
	const Option *o = g_conf->find_option(j.key);
	if (!o ||
	    o->flags & Option::FLAG_NO_MON_UPDATE) {
	  goto skip;
	}
	// normalize
	err = o->parse_value(j.val, &real_value, &errstr, &value);
	if (err < 0) {
	  dout(20) << __func__ << " failed to parse " << j.key << " = '"
		   << j.val << "'" << dendl;
	  goto skip;
	}
	// does it conflict with an existing value?
	{
	  const Section *s = config_map.find_section(section);
	  if (s) {
	    auto k = s->options.find(j.key);
	    if (k != s->options.end()) {
	      if (value != k->second.raw_value) {
		dout(20) << __func__ << " have " << j.key
			 << " = " << k->second.raw_value
			 << " (not " << value << ")" << dendl;
		goto skip;
	      }
	      dout(20) << __func__ << " already have " << j.key
		       << " = " << k->second.raw_value << dendl;
	      continue;
	    }
	  }
	}
	dout(20) << __func__ << "  add " << j.key << " = " << value
		 << " (" << j.val << ")" << dendl;
	{
	  string key = section + "/" + j.key;
	  bufferlist bl;
	  bl.append(value);
	  pending[key] = bl;
	  updated = true;
	}
	continue;

       skip:
	dout(20) << __func__ << " skip " << j.key << " = " << value
		 << " (" << j.val << ")" << dendl;
	if (!did_section) {
	  newconf << "\n[" << section << "]\n";
	  did_section = true;
	}
	newconf << "\t" << j.key << " = " << j.val << "\n";
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
  mon->reply_command(op, err, ss.str(), odata, get_last_committed());
  return false;

update:
  force_immediate_propose();  // faster response
  wait_for_finished_proposal(
    op,
    new Monitor::C_Command(
      mon, op, 0, ss.str(), odata,
      get_last_committed() + 1));
  return true;
}

void ConfigMonitor::tick()
{
  if (!is_active()) {
    return;
  }
  dout(10) << __func__ << dendl;
  bool changed = false;
  if (changed) {
    propose_pending();
  }
}

void ConfigMonitor::on_active()
{
}

void ConfigMonitor::load_config()
{
  unsigned num = 0;
  KeyValueDB::Iterator it = mon->store->get_iterator(CONFIG_PREFIX);
  it->lower_bound(KEY_PREFIX);
  config_map.clear();
  while (it->valid() &&
	 it->key().compare(0, KEY_PREFIX.size(), KEY_PREFIX) == 0) {
    string key = it->key().substr(KEY_PREFIX.size());
    string value = it->value().to_str();

    auto last_slash = key.rfind('/');
    string name;
    string who;
    if (last_slash == std::string::npos) {
      name = key;
    } else {
      name = key.substr(last_slash + 1);
      who = key.substr(0, last_slash);
    }

    const Option *opt = g_conf->find_option(name);
    if (!opt) {
      dout(10) << __func__ << " unrecognized option '" << name << "'" << dendl;
      opt = new Option(name, Option::TYPE_STR, Option::LEVEL_UNKNOWN);
    }
    string err;
    int r = opt->pre_validate(&value, &err);
    if (r < 0) {
      dout(10) << __func__ << " pre-validate failed on '" << name << "' = '"
	       << value << "' for " << name << dendl;
    }

    string section_name;
    MaskedOption mopt(opt);
    mopt.raw_value = value;
    if (who.size() &&
	!ConfigMap::parse_mask(who, &section_name, &mopt.mask)) {
      derr << __func__ << " ignoring key " << key << dendl;
    } else {
      Section *section = &config_map.global;;
      if (section_name.size()) {
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
  dout(20) << __func__ << " config map:\n";
  JSONFormatter jf(true);
  jf.open_object_section("config_map");
  config_map.dump(&jf);
  jf.close_section();
  jf.flush(*_dout);
  *_dout << dendl;

  // refresh our own config
  {
    const OSDMap& osdmap = mon->osdmon()->osdmap;
    map<string,string> crush_location;
    osdmap.crush->get_full_location(g_conf->host, &crush_location);
    map<string,string> out;
    config_map.generate_entity_map(
      g_conf->name,
      crush_location,
      osdmap.crush.get(),
      string(), // no device class
      &out);
    g_conf->set_mon_vals(g_ceph_context, out);
  }
}

bool ConfigMonitor::refresh_config(MonSession *s)
{
  const OSDMap& osdmap = mon->osdmon()->osdmap;

  map<string,string> crush_location;
  if (s->remote_host.size()) {
    osdmap.crush->get_full_location(s->remote_host, &crush_location);
    dout(10) << __func__ << " crush_location for remote_host " << s->remote_host
	     << " is " << crush_location << dendl;
  }

  string device_class;
  if (s->inst.name.is_osd()) {
    const char *c = osdmap.crush->get_item_class(s->inst.name.num());
    if (c) {
      device_class = c;
      dout(10) << __func__ << " device_class " << device_class << dendl;
    }
  }

  dout(20) << __func__ << " " << s->entity_name << " crush " << crush_location
	   << " device_class " << device_class << dendl;
  map<string,string> out;
  config_map.generate_entity_map(
    s->entity_name,
    crush_location,
    osdmap.crush.get(),
    device_class,
    &out);

  if (out == s->last_config && s->any_config) {
    dout(20) << __func__ << " no change, " << out << dendl;
    return false;
  }

  dout(20) << __func__ << " " << out << dendl;
  s->last_config = out;
  s->any_config = true;
  return true;
}

bool ConfigMonitor::maybe_send_config(MonSession *s)
{
  bool changed = refresh_config(s);
  dout(10) << __func__ << " to " << s->inst << " "
	   << (changed ? "(changed)" : "(unchanged)")
	   << dendl;
  if (changed) {
    send_config(s);
  }
  return changed;
}

void ConfigMonitor::send_config(MonSession *s)
{
  dout(10) << __func__ << " to " << s->inst << dendl;
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
      mon->with_session_map([this, sub](MonSessionMap& session_map) {
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
  auto subs = mon->session_map.subs.find("config");
  if (subs == mon->session_map.subs.end()) {
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
