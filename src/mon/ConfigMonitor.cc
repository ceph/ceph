// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/split.hpp>

#include "mon/Monitor.h"
#include "mon/ConfigMonitor.h"
#include "mon/OSDMonitor.h"
#include "messages/MConfig.h"
#include "common/Formatter.h"

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
  version = get_last_committed();
  dout(10) << __func__ << dendl;
  load_config();

#warning fixme: load changed sections to hint load_config()
}

void ConfigMonitor::create_pending()
{
  dout(10) << " " << version << dendl;
  pending.clear();
}

void ConfigMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  ++version;
  dout(10) << " " << version << dendl;
  put_last_committed(t, version);

#warning fixme: record changed sections (osd, mds.foo, rack:bar, ...)
  
  for (auto& p : pending) {
    string key = KEY_PREFIX + p.first;
    if (p.second) {
      t->put(CONFIG_PREFIX, key, *p.second);
    } else {
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
  return false;
}

bool ConfigMonitor::prepare_update(MonOpRequestRef op)
{
  Message *m = op->get_req();
  dout(7) << "prepare_update " << *m
	  << " from " << m->get_orig_source_inst() << dendl;
  return false;
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

void ConfigMonitor::load_config()
{
  unsigned num = 0;
  KeyValueDB::Iterator it = mon->store->get_iterator(CONFIG_PREFIX);
  it->lower_bound(KEY_PREFIX);
  while (it->valid() &&
	 it->key().compare(0, KEY_PREFIX.size(), KEY_PREFIX) == 0) {
    string key = it->key().substr(KEY_PREFIX.size());
    string value = it->value().to_str();
    vector<string> split;
    boost::split(split, key, [](char c){return c == '/';});
    string name = split.back();
    split.pop_back();
    Section *section = &config_map.global;

    Option fake_opt(name, Option::TYPE_STR, Option::LEVEL_DEV);
    const Option *opt = g_conf->find_option(name);
    if (!opt) {
      dout(10) << __func__ << " unrecognized option '" << name << "'" << dendl;
      opt = &fake_opt;
    }
    string err;
    int r = opt->pre_validate(&value, &err);
    if (r < 0) {
      dout(10) << __func__ << " pre-validate failed on '" << name << "' = '"
	       << value << "' for " << split << dendl;
    }

    MaskedOption mopt(*opt);
    mopt.raw_value = value;
    string device_class;
    string loc_type, loc_value;
    for (unsigned j = 0; j < split.size(); ++j) {
      auto& i = split[j];
      size_t delim = i.find(':');
      if (delim > 0) {
	string k = i.substr(0, delim);
	if (k == "class") {
	  mopt.device_class = i.substr(delim + 1);
	} else {
	  mopt.location_type = k;
	  mopt.location_value = i.substr(delim + 1);
	}
	continue;
      }
      if (split.front().find('.') > 0) {
	section = &config_map.by_id[i];
      } else {
	section = &config_map.by_type[i];
      }
    }
    section->options.insert(make_pair(name, mopt));
    ++num;
    it->next();
  }
  dout(10) << __func__ << " got " << num << " keys" << dendl;
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

  map<string,string> out;
  config_map.generate_entity_map(
    s->entity_name,
    crush_location,
    osdmap.crush.get(),
    device_class,
    &out);

  if (out == s->last_config) {
    return false;
  }

  s->last_config = out;
  return true;
}

void ConfigMonitor::send_config(MonSession *s)
{
  bool changed = refresh_config(s);
  dout(10) << __func__ << " to " << s->inst << " "
	   << (changed ? "(changed)" : "(unchanged)")
	   << dendl;
  auto m = new MConfig(s->last_config);
  s->con->send_message(m);
}

void ConfigMonitor::check_sub(MonSession *s)
{
  if (!s->is_capable(s->entity_name.get_type_str(), MON_CAP_R)) {
    dout(20) << __func__ << " not capable for " << s->entity_name << " with "
	     << s->caps << dendl;
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
    send_config(sub->session);
    if (sub->onetime) {
      mon->with_session_map([this, sub](MonSessionMap& session_map) {
	  session_map.remove_sub(sub);
	});
    } else {
      sub->next = version + 1;
    }
  }
}
