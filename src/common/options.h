// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <chrono>
#include <string>
#include <vector>
#include <boost/variant.hpp>
#include "include/str_list.h"
#include "msg/msg_types.h"
#include "include/uuid.h"

struct Option {
  enum type_t {
    TYPE_UINT = 0,
    TYPE_INT = 1,
    TYPE_STR = 2,
    TYPE_FLOAT = 3,
    TYPE_BOOL = 4,
    TYPE_ADDR = 5,
    TYPE_ADDRVEC = 6,
    TYPE_UUID = 7,
    TYPE_SIZE = 8,
    TYPE_SECS = 9,
  };

  static const char *type_to_c_type_str(type_t t) {
    switch (t) {
    case TYPE_UINT: return "uint64_t";
    case TYPE_INT: return "int64_t";
    case TYPE_STR: return "std::string";
    case TYPE_FLOAT: return "double";
    case TYPE_BOOL: return "bool";
    case TYPE_ADDR: return "entity_addr_t";
    case TYPE_ADDRVEC: return "entity_addrvec_t";
    case TYPE_UUID: return "uuid_d";
    case TYPE_SIZE: return "size_t";
    case TYPE_SECS: return "secs";
    default: return "unknown";
    }
  }
  static const char *type_to_str(type_t t) {
    switch (t) {
    case TYPE_UINT: return "uint";
    case TYPE_INT: return "int";
    case TYPE_STR: return "str";
    case TYPE_FLOAT: return "float";
    case TYPE_BOOL: return "bool";
    case TYPE_ADDR: return "addr";
    case TYPE_ADDRVEC: return "addrvec";
    case TYPE_UUID: return "uuid";
    case TYPE_SIZE: return "size";
    case TYPE_SECS: return "secs";
    default: return "unknown";
    }
  }
  static int str_to_type(const std::string& s) {
    if (s == "uint") {
      return TYPE_UINT;
    }
    if (s == "int") {
      return TYPE_INT;
    }
    if (s == "str") {
      return TYPE_STR;
    }
    if (s == "float") {
      return TYPE_FLOAT;
    }
    if (s == "bool") {
      return TYPE_BOOL;
    }
    if (s == "addr") {
      return TYPE_ADDR;
    }
    if (s == "addrvec") {
      return TYPE_ADDRVEC;
    }
    if (s == "uuid") {
      return TYPE_UUID;
    }
    if (s == "size") {
      return TYPE_SIZE;
    }
    if (s == "secs") {
      return TYPE_SECS;
    }
    return -1;
  }

  /**
   * Basic: for users, configures some externally visible functional aspect
   * Advanced: for users, configures some internal behaviour
   * Development: not for users.  May be dangerous, may not be documented.
   */
  enum level_t {
    LEVEL_BASIC = 0,
    LEVEL_ADVANCED = 1,
    LEVEL_DEV = 2,
    LEVEL_UNKNOWN = 3,
  };

  static const char *level_to_str(level_t l) {
    switch (l) {
      case LEVEL_BASIC: return "basic";
      case LEVEL_ADVANCED: return "advanced";
      case LEVEL_DEV: return "dev";
      default: return "unknown";
    }
  }

  enum flag_t {
    FLAG_RUNTIME = 0x1,         ///< option can be changed at runtime
    FLAG_NO_MON_UPDATE = 0x2,   ///< option cannot be changed via mon config
    FLAG_STARTUP = 0x4,         ///< option can only take effect at startup
    FLAG_CLUSTER_CREATE = 0x8,  ///< option only has effect at cluster creation
    FLAG_CREATE = 0x10,         ///< option only has effect at daemon creation
    FLAG_MGR = 0x20,            ///< option is a mgr module option
    FLAG_MINIMAL_CONF = 0x40,   ///< option should go in a minimal ceph.conf
  };

  struct size_t {
    std::size_t value;
    operator uint64_t() const {
      return static_cast<uint64_t>(value);
    }
    bool operator==(const size_t& rhs) const {
      return value == rhs.value;
    }
  };

  using value_t = boost::variant<
    boost::blank,
    std::string,
    uint64_t,
    int64_t,
    double,
    bool,
    entity_addr_t,
    entity_addrvec_t,
    std::chrono::seconds,
    size_t,
    uuid_d>;
  const std::string name;
  const type_t type;
  const level_t level;

  std::string desc;
  std::string long_desc;

  unsigned flags = 0;

  int subsys = -1; // if >= 0, we are a subsys debug level

  value_t value;
  value_t daemon_value;

  static std::string to_str(const value_t& v);

  // Items like mon, osd, rgw, rbd, ceph-fuse.  This is advisory metadata
  // for presentation layers (like web dashboards, or generated docs), so that
  // they know which options to display where.
  // Additionally: "common" for settings that exist in any Ceph code.  Do
  // not use common for settings that are just shared some places: for those
  // places, list them.
  std::vector<const char*> services;

  // Topics like:
  // "service": a catchall for the boring stuff like log/asok paths.
  // "network"
  // "performance": a setting that may need adjustment depending on
  //                environment/workload to get best performance.
  std::vector<const char*> tags;

  std::vector<const char*> see_also;

  value_t min, max;
  std::vector<const char*> enum_allowed;

  /**
   * Return nonzero and set second argument to error string if the
   * value is invalid.
   *
   * These callbacks are more than just validators, as they can also
   * modify the value as it passes through.
   */
  typedef std::function<int(std::string *, std::string *)> validator_fn_t;
  validator_fn_t validator;

  Option(std::string const &name, type_t t, level_t l)
    : name(name), type(t), level(l)
  {
    // While value_t is nullable (via boost::blank), we don't ever
    // want it set that way in an Option instance: within an instance,
    // the type of ::value should always match the declared type.
    switch (type) {
    case TYPE_INT:
      value = int64_t(0); break;
    case TYPE_UINT:
      value = uint64_t(0); break;
    case TYPE_STR:
      value = std::string(""); break;
    case TYPE_FLOAT:
      value = 0.0; break;
    case TYPE_BOOL:
      value = false; break;
    case TYPE_ADDR:
      value = entity_addr_t(); break;
    case TYPE_ADDRVEC:
      value = entity_addrvec_t(); break;
    case TYPE_UUID:
      value = uuid_d(); break;
    case TYPE_SIZE:
      value = size_t{0}; break;
    case TYPE_SECS:
      value = std::chrono::seconds{0}; break;
    default:
      ceph_abort();
    }
  }

  void dump_value(const char *field_name, const value_t &v, ceph::Formatter *f) const;

  // Validate and potentially modify incoming string value
  int pre_validate(std::string *new_value, std::string *err) const;

  // Validate properly typed value against bounds
  int validate(const Option::value_t &new_value, std::string *err) const;

  // const char * must be explicit to avoid it being treated as an int
  Option& set_value(value_t& v, const char *new_value) {
    v = std::string(new_value);
    return *this;
  }

  // bool is an integer, but we don't think so. teach it the hard way.
  template<typename T>
  using is_not_integer = std::enable_if<!std::is_integral<T>::value ||
					std::is_same<T, bool>::value, int>;
  template<typename T>
  using is_integer = std::enable_if<std::is_integral<T>::value &&
				    !std::is_same<T, bool>::value, int>;
  template<typename T, typename is_not_integer<T>::type = 0>
  Option& set_value(value_t& v, const T& new_value) {
    v = new_value;
    return *this;
  }

  // For potentially ambiguous types, inspect Option::type and
  // do some casting.  This is necessary to make sure that setting
  // a float option to "0" actually sets the double part of variant.
  template<typename T, typename is_integer<T>::type = 0>
  Option& set_value(value_t& v, T new_value) {
    switch (type) {
    case TYPE_INT:
      v = int64_t(new_value); break;
    case TYPE_UINT:
      v = uint64_t(new_value); break;
    case TYPE_FLOAT:
      v = double(new_value); break;
    case TYPE_BOOL:
      v = bool(new_value); break;
    case TYPE_SIZE:
      v = size_t{static_cast<std::size_t>(new_value)}; break;
    case TYPE_SECS:
      v = std::chrono::seconds{new_value}; break;
    default:
      std::cerr << "Bad type in set_value: " << name << ": "
                << typeid(T).name() << std::endl;
      ceph_abort();
    }
    return *this;
  }

  /// parse and validate a string input
  int parse_value(
    const std::string& raw_val,
    value_t *out,
    std::string *error_message,
    std::string *normalized_value=nullptr) const;

  template<typename T>
  Option& set_default(const T& v) {
    return set_value(value, v);
  }

  template<typename T>
  Option& set_daemon_default(const T& v) {
    return set_value(daemon_value, v);
  }
  Option& add_tag(const char* tag) {
    tags.push_back(tag);
    return *this;
  }
  Option& add_tag(const std::initializer_list<const char*>& ts) {
    tags.insert(tags.end(), ts);
    return *this;
  }
  Option& add_service(const char* service) {
    services.push_back(service);
    return *this;
  }
  Option& add_service(const std::initializer_list<const char*>& ss) {
    services.insert(services.end(), ss);
    return *this;
  }
  Option& add_see_also(const char* t) {
    see_also.push_back(t);
    return *this;
  }
  Option& add_see_also(const std::initializer_list<const char*>& ts) {
    see_also.insert(see_also.end(), ts);
    return *this;
  }
  Option& set_description(const char* new_desc) {
    desc = new_desc;
    return *this;
  }
  Option& set_long_description(const char* new_desc) {
    long_desc = new_desc;
    return *this;
  }

  template<typename T>
  Option& set_min(const T& mi) {
    set_value(min, mi);
    return *this;
  }

  template<typename T>
  Option& set_min_max(const T& mi, const T& ma) {
    set_value(min, mi);
    set_value(max, ma);
    return *this;
  }

  Option& set_enum_allowed(const std::vector<const char*>& allowed)
  {
    enum_allowed = allowed;
    return *this;
  }

  Option &set_flag(flag_t f) {
    flags |= f;
    return *this;
  }
  Option &set_flags(flag_t f) {
    flags |= f;
    return *this;
  }

  Option &set_validator(const validator_fn_t  &validator_)
  {
    validator = validator_;
    return *this;
  }

  Option &set_subsys(int s) {
    subsys = s;
    return *this;
  }

  void dump(ceph::Formatter *f) const;
  void print(std::ostream *out) const;

  bool has_flag(flag_t f) const {
    return flags & f;
  }

  /**
   * A crude indicator of whether the value may be
   * modified safely at runtime -- should be replaced
   * with proper locking!
   */
  bool can_update_at_runtime() const
  {
    return
      (has_flag(FLAG_RUNTIME)
       || (!has_flag(FLAG_MGR)
	   && (type == TYPE_BOOL || type == TYPE_INT
	       || type == TYPE_UINT || type == TYPE_FLOAT
	       || type == TYPE_SIZE || type == TYPE_SECS)))
      && !has_flag(FLAG_STARTUP)
      && !has_flag(FLAG_CLUSTER_CREATE)
      && !has_flag(FLAG_CREATE);
  }
};

extern const std::vector<Option> ceph_options;

