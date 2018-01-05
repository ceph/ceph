// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/split.hpp>

#include "ConfigMap.h"
#include "crush/CrushWrapper.h"
#include "common/entity_name.h"

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
  f->dump_string("name", opt->name);
  f->dump_string("value", raw_value);
  mask.dump(f);
}

ostream& operator<<(ostream& out, const MaskedOption& o)
{
  out << o.opt->name;
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

void ConfigMap::generate_entity_map(
  const EntityName& name,
  const map<std::string,std::string>& crush_location,
  const CrushWrapper *crush,
  const std::string& device_class,
  std::map<std::string,std::string> *out,
  std::map<std::string,pair<std::string,const MaskedOption*>> *src)
{
  // global, then by type, then by full name.
  vector<pair<string,Section*>> sections = { make_pair("global", &global) };
  auto p = by_type.find(name.get_type_name());
  if (p != by_type.end()) {
    sections.push_back(make_pair(name.get_type_name(), &p->second));
  }
  auto q = by_id.find(name.to_str());
  if (q != by_id.end()) {
    sections.push_back(make_pair(name.to_str(), &q->second));
  }
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
      (*out)[i.first] = o.raw_value;
      if (src) {
	(*src)[i.first] = make_pair(s.first, &o);
      }
      prev = &o;
    }
  }
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
    if (str_to_ceph_entity_type(type.c_str()) == CEPH_ENTITY_TYPE_ANY) {
      return false;
    }
    *section = i;
  }
  return true;
}
