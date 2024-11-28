// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "options.h"
#include "common/Formatter.h"
#include "common/options/build_options.h"
#include "common/strtol.h" // for strict_si_cast()

// Helpers for validators
#include "include/stringify.h"
#include "include/common_fwd.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <regex>

// Definitions for enums
#include "common/perf_counters.h"

// rbd feature and io operation validation
#include "librbd/Features.h"
#include "librbd/io/IoOperations.h"

using std::ostream;
using std::ostringstream;

using ceph::Formatter;
using ceph::parse_timespan;

namespace {
class printer {
  ostream& out;
public:
  explicit printer(ostream& os)
    : out(os) {}
  template<typename T>
  void operator()(const T& v) const {
    out << v;
  }
  void operator()(std::monostate) const {
    return;
  }
  void operator()(bool v) const {
    out << (v ? "true" : "false");
  }
  void operator()(double v) const {
    out << std::fixed << v << std::defaultfloat;
  }
  void operator()(const Option::size_t& v) const {
    out << v.value;
  }
  void operator()(const std::chrono::seconds v) const {
    out << v.count();
  }
  void operator()(const std::chrono::milliseconds v) const {
    out << v.count();
  }
};
}

ostream& operator<<(ostream& os, const Option::value_t& v) {
  printer p{os};
  std::visit(p, v);
  return os;
}

void Option::dump_value(const char *field_name,
    const Option::value_t &v, Formatter *f) const
{
  if (v == value_t{}) {
    // This should be nil but Formatter doesn't allow it.
    f->dump_string(field_name, "");
    return;
  }
  switch (type) {
  case TYPE_INT:
    f->dump_int(field_name, std::get<int64_t>(v)); break;
  case TYPE_UINT:
    f->dump_unsigned(field_name, std::get<uint64_t>(v)); break;
  case TYPE_STR:
    f->dump_string(field_name, std::get<std::string>(v)); break;
  case TYPE_FLOAT:
    f->dump_float(field_name, std::get<double>(v)); break;
  case TYPE_BOOL:
    f->dump_bool(field_name, std::get<bool>(v)); break;
  default:
    f->dump_stream(field_name) << v; break;
  }
}

int Option::pre_validate(std::string *new_value, std::string *err) const
{
  if (validator) {
    return validator(new_value, err);
  } else {
    return 0;
  }
}

int Option::validate(const Option::value_t &new_value, std::string *err) const
{
  // Generic validation: min
  if (min != value_t{}) {
    if (new_value < min) {
      std::ostringstream oss;
      oss << "Value '" << new_value << "' is below minimum " << min;
      *err = oss.str();
      return -EINVAL;
    }
  }

  // Generic validation: max
  if (max != value_t{}) {
    if (new_value > max) {
      std::ostringstream oss;
      oss << "Value '" << new_value << "' exceeds maximum " << max;
      *err = oss.str();
      return -EINVAL;
    }
  }

  // Generic validation: enum
  if (!enum_allowed.empty() && type == Option::TYPE_STR) {
    auto found = std::find(enum_allowed.begin(), enum_allowed.end(),
                           std::get<std::string>(new_value));
    if (found == enum_allowed.end()) {
      std::ostringstream oss;
      oss << "'" << new_value << "' is not one of the permitted "
                 "values: " << joinify(enum_allowed.begin(),
                                       enum_allowed.end(),
                                       std::string(", "));
      *err = oss.str();
      return -EINVAL;
    }
  }

  return 0;
}

int Option::parse_value(
  const std::string& raw_val,
  value_t *out,
  std::string *error_message,
  std::string *normalized_value) const
{
  std::string val = raw_val;

  int r = pre_validate(&val, error_message);
  if (r != 0) {
    return r;
  }

  if (type == Option::TYPE_INT) {
    int64_t f = strict_si_cast<int64_t>(val, error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    }
    *out = f;
  } else if (type == Option::TYPE_UINT) {
    uint64_t f = strict_si_cast<uint64_t>(val, error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    }
    *out = f;
  } else if (type == Option::TYPE_STR) {
    *out = val;
  } else if (type == Option::TYPE_FLOAT) {
    double f = strict_strtod(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    } else {
      *out = f;
    }
  } else if (type == Option::TYPE_BOOL) {
    bool b = strict_strtob(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    } else {
      *out = b;
    }
  } else if (type == Option::TYPE_ADDR) {
    entity_addr_t addr;
    if (!addr.parse(val)){
      return -EINVAL;
    }
    *out = addr;
  } else if (type == Option::TYPE_ADDRVEC) {
    entity_addrvec_t addr;
    if (!addr.parse(val.c_str())){
      return -EINVAL;
    }
    *out = addr;
  } else if (type == Option::TYPE_UUID) {
    uuid_d uuid;
    if (!uuid.parse(val.c_str())) {
      return -EINVAL;
    }
    *out = uuid;
  } else if (type == Option::TYPE_SIZE) {
    Option::size_t sz{strict_iecstrtoll(val, error_message)};
    if (!error_message->empty()) {
      return -EINVAL;
    }
    *out = sz;
  } else if (type == Option::TYPE_SECS) {
    try {
      *out = parse_timespan(val);
    } catch (const std::invalid_argument& e) {
      *error_message = e.what();
      return -EINVAL;
    }
  } else if (type == Option::TYPE_MILLISECS) {
    try {
      *out = std::chrono::milliseconds(std::stoull(val));
    } catch (const std::logic_error& e) {
      *error_message = e.what();
      return -EINVAL;
    }
  } else {
    ceph_abort();
  }

  r = validate(*out, error_message);
  if (r != 0) {
    return r;
  }

  if (normalized_value) {
    *normalized_value = to_str(*out);
  }
  return 0;
}

void Option::dump(Formatter *f) const
{
  f->dump_string("name", name);

  f->dump_string("type", type_to_str(type));

  f->dump_string("level", level_to_str(level));

  f->dump_string("desc", desc);
  f->dump_string("long_desc", long_desc);

  dump_value("default", value, f);
  dump_value("daemon_default", daemon_value, f);

  f->open_array_section("tags");
  for (const auto t : tags) {
    f->dump_string("tag", t);
  }
  f->close_section();

  f->open_array_section("services");
  for (const auto s : services) {
    f->dump_string("service", s);
  }
  f->close_section();

  f->open_array_section("see_also");
  for (const auto sa : see_also) {
    f->dump_string("see_also", sa);
  }
  f->close_section();

  if (type == TYPE_STR) {
    f->open_array_section("enum_values");
    for (const auto &ea : enum_allowed) {
      f->dump_string("enum_value", ea);
    }
    f->close_section();
  }

  dump_value("min", min, f);
  dump_value("max", max, f);

  f->dump_bool("can_update_at_runtime", can_update_at_runtime());

  f->open_array_section("flags");
  if (has_flag(FLAG_RUNTIME)) {
    f->dump_string("option", "runtime");
  }
  if (has_flag(FLAG_NO_MON_UPDATE)) {
    f->dump_string("option", "no_mon_update");
  }
  if (has_flag(FLAG_STARTUP)) {
    f->dump_string("option", "startup");
  }
  if (has_flag(FLAG_CLUSTER_CREATE)) {
    f->dump_string("option", "cluster_create");
  }
  if (has_flag(FLAG_CREATE)) {
    f->dump_string("option", "create");
  }
  f->close_section();
}

std::string Option::to_str(const Option::value_t& v)
{
  return stringify(v);
}

void Option::print(ostream *out) const
{
  *out << name << " - " << desc << "\n";
  *out << "  (" << type_to_str(type) << ", " << level_to_str(level) << ")\n";
  if (daemon_value != value_t{}) {
    *out << "  Default (non-daemon): " << stringify(value) << "\n";
    *out << "  Default (daemon): " << stringify(daemon_value) << "\n";
  } else {
    *out << "  Default: " << stringify(value) << "\n";
  }
  if (!enum_allowed.empty()) {
    *out << "  Possible values: ";
    for (auto& i : enum_allowed) {
      *out << " " << stringify(i);
    }
    *out << "\n";
  }
  if (min != value_t{}) {
    *out << "  Minimum: " << stringify(min) << "\n"
	 << "  Maximum: " << stringify(max) << "\n";
  }
  *out << "  Can update at runtime: "
       << (can_update_at_runtime() ? "true" : "false") << "\n";
  if (!services.empty()) {
    *out << "  Services: " << services << "\n";
  }
  if (!tags.empty()) {
    *out << "  Tags: " << tags << "\n";
  }
  if (!see_also.empty()) {
    *out << "  See also: " << see_also << "\n";
  }

  if (long_desc.size()) {
    *out << "\n" << long_desc << "\n";
  }
}

const std::vector<Option> ceph_options = build_options();
