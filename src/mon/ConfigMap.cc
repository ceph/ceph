// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/split.hpp>

#include "ConfigMap.h"
#include "crush/CrushWrapper.h"
#include "common/entity_name.h"
#include "json_spirit/json_spirit.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#include "common/dout.h"

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

bool MaskedOption::OptionMask::empty() const {
  return (location_type.size() == 0
          && location_value.size() == 0
          && device_class.size() == 0);
}

std::string MaskedOption::OptionMask::to_str() const {
  std::string r;
  if (location_type.size()) {
    r += location_type + ":" + location_value;
  }
  if (device_class.size()) {
    if (r.size()) {
      r += "/";
    }
    r += "class:" + device_class;
  }
  return r;
}

void MaskedOption::OptionMask::dump(Formatter *f) const
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

Profile::Profile(Option *o)
  : opt(o)
{
}

void Profile::dump(Formatter *f) const
{
  f->dump_string("level", Option::level_to_str(opt->level));
  f->dump_string("desc", opt->desc);
  f->dump_string("fmt_desc", opt->long_desc);
  f->open_object_section("values");
  for (auto& i : profile) {
    f->open_object_section(i.first.c_str());
    for (auto& j : i.second.options) {
      f->dump_string(j.first.c_str(), j.second.raw_value);
    }
    f->close_section();
  }
  f->close_section();
}

int Profile::parse(
  CephContext *cct,
  const std::string& input,
  std::function<const Option *(const std::string&)> get_opt)
{
  json_spirit::mValue json;
  try {
    // try json parsing first
    json_spirit::read_or_throw(input, json);
    if (json.type() != json_spirit::obj_type) {
      return -EINVAL;
    }
    for (auto& i : json.get_obj()) { // profile_value -> {}
      if (i.first == "level") {
	if (i.second.type() != json_spirit::str_type) {
	  return -EINVAL;
	}
	if (i.second.get_str() == "basic") {
	  opt->level = Option::LEVEL_BASIC;
	} else if (i.second.get_str() == "advanced") {
	  opt->level = Option::LEVEL_ADVANCED;
	} else if (i.second.get_str() == "dev") {
	  opt->level = Option::LEVEL_DEV;
	}
      }
      else if (i.first == "desc") {
	if (i.second.type() != json_spirit::str_type) {
	  return -EINVAL;
	}
	opt->desc = i.second.get_str();
      }
      else if (i.first == "fmt_desc") {
	if (i.second.type() != json_spirit::str_type) {
	  return -EINVAL;
	}
	opt->long_desc = i.second.get_str();
      }
      else if (i.first == "values") {
	if (i.second.type() != json_spirit::obj_type) {
	  return -EINVAL;
	}
	for (auto& j : i.second.get_obj()) { // profile_value -> {}
	  if (j.second.type() != json_spirit::obj_type) {
	    return -EINVAL;
	  }
	  ldout(cct, 10) << __func__ << " value " << j.first << dendl;
	  auto r = profile.insert(make_pair(j.first, Section()));
	  opt->enum_allowed.push_back(r.first->first.c_str());
	  for (auto& k : j.second.get_obj()) { // { key -> value }
	    if (k.second.type() != json_spirit::str_type) {
	      return -EINVAL;
	    }
	    std::string name = k.first;
	    ldout(cct, 10) << __func__ << "   option " << name << dendl;
	    const Option *opt = get_opt(name);
	    ceph_assert(opt);
	    MaskedOption o(opt);
	    o.raw_value = k.second.get_str();
	    profile[j.first].options.insert(make_pair(name, std::move(o)));
	  }
	}
      } else {
	return -EINVAL;
      }
    }
  } catch (json_spirit::Error_position &e) {
    return -EINVAL;
  }
  return 0;
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
  f->open_object_section("profiles");
  for (auto& i : profiles) {
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
  std::unordered_map<std::string, ValueSource> *src)
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
    // apply profile content first
    for (auto& i : s.second->options) {
      auto p = profiles.find(i.first);
      if (p == profiles.end()) {
	continue;
      }

      // match against crush location, class
      auto& o = i.second;
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

      // apply profile options
      auto q = p->second.profile.find(o.raw_value);
      if (q != p->second.profile.end()) {
	for (auto& [opt, v] : q->second.options) {
	  out[opt] = v.raw_value;
	  if (src) {
	    (*src)[opt] = ConfigMap::ValueSource(s.first, &v, i.first, o.raw_value);
	  }
	}
      }

      prev = &o;
    }

    // ...then apply (all) options
    prev = nullptr;
    for (auto& i : s.second->options) {
      // match against crush location, class
      auto& o = i.second;
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
	(*src).emplace(i.first, ConfigMap::ValueSource(s.first, &o));
      }

      prev = &o;
    }
  }
  return out;
}

bool ConfigMap::parse_mask(
  const std::string& who,
  std::string *section,
  MaskedOption::OptionMask *mask)
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

int ConfigMap::add_option(
  CephContext *cct,
  const std::string& name,
  const std::string& who,
  const std::string& orig_value,
  std::function<const Option *(const std::string&)> get_opt)
{
  const Option *opt = get_option(cct, name, get_opt);

  string err;
  string value = orig_value;
  int r = opt->pre_validate(&value, &err);
  if (r < 0) {
    ldout(cct, 10) << __func__ << " pre-validate failed on '" << name << "' = '"
		   << value << "' for " << name << dendl;
  }

  int ret = 0;
  MaskedOption mopt(opt);
  mopt.raw_value = value;
  mopt.localized_name = name;
  string section_name;
  if (who.size() &&
      !ConfigMap::parse_mask(who, &section_name, &mopt.mask)) {
    lderr(cct) << __func__ << " invalid mask for option " << name << " mask " << who
	       << dendl;
    ret = -EINVAL;
  } else if (opt->has_flag(Option::FLAG_NO_MON_UPDATE)) {
    ldout(cct, 10) << __func__ << " NO_MON_UPDATE option '"
		   << name << "' = '" << value << "' for " << name
		   << dendl;
    ret = -EINVAL;
  } else {
    Section *section = &global;;
    if (section_name.size() && section_name != "global") {
      if (section_name.find('.') != std::string::npos) {
	section = &by_id[section_name];
      } else {
	section = &by_type[section_name];
      }
    }
    section->options.insert(make_pair(name, std::move(mopt)));
  }
  return ret;
}

const Option *ConfigMap::get_option(
  CephContext *cct,
  const std::string& name,
  std::function<const Option *(const std::string&)> get_opt)
{
  const Option *opt = get_opt(name);
  if (!opt) {
    auto p = profiles.find(name);
    if (p != profiles.end()) {
      return p->second.opt;
    }
    ldout(cct, 10) << __func__ << " unrecognized option '" << name << "'" << dendl;
    stray_options.push_back(
      std::unique_ptr<Option>(
	new Option(name, Option::TYPE_STR, Option::LEVEL_UNKNOWN)));
    opt = stray_options.back().get();
  }
  return opt;
}

int ConfigMap::add_profile(
  CephContext *cct,
  const std::string& name,
  const std::string& def,
  std::function<const Option *(const std::string&)> get_opt)
{
  stray_options.push_back(
    std::unique_ptr<Option>(
      new Option(name, Option::TYPE_STR, Option::LEVEL_UNKNOWN)));
  auto it = profiles.emplace(name, Profile(stray_options.back().get()));
  Profile& new_profile = it.first->second;
  int r = new_profile.parse(
    cct,
    def,
    [&](const std::string& name) {
      return get_option(cct, name, get_opt);
    }
  );
  if (r < 0) {
    profiles.erase(name);
    return -1;
  }

  // mark the profile option as a runtime option, even though it is a string
  new_profile.opt->set_flag(Option::FLAG_RUNTIME);

  return 0;
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
