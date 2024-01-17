// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/split.hpp>

#include "ConfigMap.h"
#include "crush/CrushWrapper.h"
#include "common/entity_name.h"

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

int MaskedOption::get_precision(const CrushWrapper *crush)
{
  // 0 = most precise
  if (mask.location_type.size()) {
    int r = crush->get_type_id(mask.location_type);
    if (r >= 0) {
      return r;
    }
    // bad type name, ignore it
  }
  int num_types = crush->get_num_type_names();
  if (mask.device_class.size()) {
    return num_types;
  }
  return num_types + 1;
}

void OptionMask::dump(Formatter *f) const
{
  if (location_type.size()) {
    f->dump_string("location_type", location_type);
    f->dump_string("location_value", location_value);
  }
  if (device_class.size()) {
    f->dump_string("device_class", device_class);
  }
}

void MaskedOption::dump(Formatter *f) const
{
  f->dump_string("name", localized_name);
  f->dump_string("value", raw_value);
  f->dump_string("level", Option::level_to_str(opt->level));
  f->dump_bool("can_update_at_runtime", opt->can_update_at_runtime());
  f->dump_string("mask", mask.to_str());
  mask.dump(f);
}

ostream& operator<<(ostream& out, const MaskedOption& o)
{
  out << o.localized_name;
  if (o.mask.location_type.size()) {
    out << "@" << o.mask.location_type << '=' << o.mask.location_value;
  }
  if (o.mask.device_class.size()) {
    out << "@class=" << o.mask.device_class;
  }
  return out;
}

// ----------

void Section::dump(Formatter *f) const
{
  for (auto& i : options) {
    f->dump_object(i.first.c_str(), i.second);
  }
}

std::string Section::get_minimal_conf() const
{
  std::string r;
  for (auto& i : options) {
    if (i.second.opt->has_flag(Option::FLAG_NO_MON_UPDATE) ||
	i.second.opt->has_flag(Option::FLAG_MINIMAL_CONF)) {
      if (i.second.mask.empty()) {
	r += "\t"s + i.first + " = " + i.second.raw_value + "\n";
      } else {
	r += "\t# masked option excluded: " + i.first + " = " +
	  i.second.raw_value + "\n";
      }
    }
  }
  return r;
}


// ------------

void ConfigMap::dump(Formatter *f) const
{
  f->dump_object("global", global);
  f->open_object_section("by_type");
  for (auto& i : by_type) {
    f->dump_object(i.first.c_str(), i.second);
  }
  f->close_section();
  f->open_object_section("by_id");
  for (auto& i : by_id) {
    f->dump_object(i.first.c_str(), i.second);
  }
  f->close_section();
}

std::map<std::string,std::string,std::less<>>
ConfigMap::generate_entity_map(
  const EntityName& name,
  const map<std::string,std::string>& crush_location,
  const CrushWrapper *crush,
  const std::string& device_class,
  std::map<std::string,pair<std::string,const MaskedOption*>> *src)
{
  // global, then by type, then by name prefix component(s), then name.
  // name prefix components are .-separated,
  // e.g. client.a.b.c -> [global, client, client.a, client.a.b, client.a.b.c]
  vector<pair<string,Section*>> sections = { make_pair("global", &global) };
  auto p = by_type.find(name.get_type_name());
  if (p != by_type.end()) {
    sections.emplace_back(name.get_type_name(), &p->second);
  }
  vector<std::string> name_bits;
  boost::split(name_bits, name.to_str(), [](char c){ return c == '.'; });
  std::string tname;
  for (unsigned p = 0; p < name_bits.size(); ++p) {
    if (p) {
      tname += '.';
    }
    tname += name_bits[p];
    auto q = by_id.find(tname);
    if (q != by_id.end()) {
      sections.push_back(make_pair(tname, &q->second));
    }
  }
  std::map<std::string,std::string,std::less<>> out;
  MaskedOption *prev = nullptr;
  for (auto s : sections) {
    for (auto& i : s.second->options) {
      auto& o = i.second;
      // match against crush location, class
      if (o.mask.device_class.size() &&
	  o.mask.device_class != device_class) {
	continue;
      }
      if (o.mask.location_type.size()) {
	auto p = crush_location.find(o.mask.location_type);
	if (p == crush_location.end() ||
	    p->second != o.mask.location_value) {
	  continue;
	}
      }
      if (prev && prev->opt->name != i.first) {
	prev = nullptr;
      }
      if (prev &&
	  prev->get_precision(crush) < o.get_precision(crush)) {
	continue;
      }
      out[i.first] = o.raw_value;
      if (src) {
	(*src)[i.first] = make_pair(s.first, &o);
      }
      prev = &o;
    }
  }
  return out;
}

bool ConfigMap::parse_mask(
  const std::string& who,
  std::string *section,
  OptionMask *mask)
{
  vector<std::string> split;
  boost::split(split, who, [](char c){ return c == '/'; });
  for (unsigned j = 0; j < split.size(); ++j) {
    auto& i = split[j];
    if (i == "global") {
      *section = "global";
      continue;
    }
    size_t delim = i.find(':');
    if (delim != std::string::npos) {
      string k = i.substr(0, delim);
      if (k == "class") {
	mask->device_class = i.substr(delim + 1);
      } else {
	mask->location_type = k;
	mask->location_value = i.substr(delim + 1);
      }
      continue;
    }
    string type, id;
    auto dotpos = i.find('.');
    if (dotpos != std::string::npos) {
      type = i.substr(0, dotpos);
      id = i.substr(dotpos + 1);
    } else {
      type = i;
    }
    if (EntityName::str_to_ceph_entity_type(type) == CEPH_ENTITY_TYPE_ANY) {
      return false;
    }
    *section = i;
  }
  return true;
}

void ConfigMap::parse_key(
  const std::string& key,
  std::string *name,
  std::string *who)
{
  auto last_slash = key.rfind('/');
  if (last_slash == std::string::npos) {
    *name = key;
  } else if (auto mgrpos = key.find("/mgr/"); mgrpos != std::string::npos) {
    *name = key.substr(mgrpos + 1);
    *who = key.substr(0, mgrpos);
  } else {
    *name = key.substr(last_slash + 1);
    *who = key.substr(0, last_slash);
  }
}


// --------------

void ConfigChangeSet::dump(Formatter *f) const
{
  f->dump_int("version", version);
  f->dump_stream("timestamp") << stamp;
  f->dump_string("name", name);
  f->open_array_section("changes");
  for (auto& i : diff) {
    f->open_object_section("change");
    f->dump_string("name", i.first);
    if (i.second.first) {
      f->dump_string("previous_value", *i.second.first);
    }
    if (i.second.second) {
      f->dump_string("new_value", *i.second.second);
    }
    f->close_section();
  }
  f->close_section();
}

void ConfigChangeSet::print(ostream& out) const
{
  out << "--- " << version << " --- " << stamp;
  if (name.size()) {
    out << " --- " << name;
  }
  out << " ---\n";
  for (auto& i : diff) {
    if (i.second.first) {
      out << "- " << i.first << " = " << *i.second.first << "\n";
    }
    if (i.second.second) {
      out << "+ " << i.first << " = " << *i.second.second << "\n";
    }
  }
}
