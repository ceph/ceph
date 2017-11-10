// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ConfigMap.h"
#include "crush/CrushWrapper.h"

int MaskedOption::get_precision(const CrushWrapper *crush)
{
  // 0 = most precise
  if (location_type.size()) {
    int r = crush->get_type_id(location_type);
    if (r >= 0) {
      return r;
    }
    // bad type name, ignore it
  }
  int num_types = crush->get_num_type_names();
  if (device_class.size()) {
    return num_types;
  }
  return num_types + 1;
}

ostream& operator<<(ostream& out, const MaskedOption& o)
{
  out << o.opt.name;
  if (o.location_type.size()) {
    out << "@" << o.location_type << '=' << o.location_value;
  }
  if (o.device_class.size()) {
    out << "@class=" << o.device_class;
  }
  return out;
}

void ConfigMap::generate_entity_map(
  const EntityName& name,
  const map<std::string,std::string>& crush_location,
  const CrushWrapper *crush,
  const std::string& device_class,
  std::map<std::string,std::string> *out)
{
  // global, then by type, then by full name.
  vector<Section*> sections = { &global };
  auto p = by_type.find(name.get_type_name());
  if (p != by_type.end()) {
    sections.push_back(&p->second);
  }
  auto q = by_id.find(name.to_str());
  if (q != by_id.end()) {
    sections.push_back(&q->second);
  }
  MaskedOption *prev = nullptr;
  for (auto s : sections) {
    for (auto& i : s->options) {
      auto& o = i.second;
      // match against crush location, class
      if (o.device_class.size() &&
	  o.device_class != device_class) {
	continue;
      }
      if (o.location_type.size()) {
	auto p = crush_location.find(o.location_type);
	if (p == crush_location.end() ||
	    p->second != o.location_value) {
	  continue;
	}
      }
      if (prev && prev->opt.name != i.first) {
	prev = nullptr;
      }
      if (prev &&
	  prev->get_precision(crush) < o.get_precision(crush)) {
	continue;
      }
      (*out)[i.first] = o.raw_value;
      prev = &o;
    }
  }
}

