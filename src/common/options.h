// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <list>
#include <boost/variant.hpp>
#include "include/str_list.h"
#include "msg/msg_types.h"
#include "include/uuid.h"

struct Option {
  enum type_t {
    TYPE_UINT,
    TYPE_INT,
    TYPE_STR,
    TYPE_FLOAT,
    TYPE_BOOL,
    TYPE_ADDR,
    TYPE_UUID,
  };

  const char *type_to_str(type_t t) const {
    switch (t) {
    case TYPE_UINT: return "uint64_t";
    case TYPE_INT: return "int64_t";
    case TYPE_STR: return "std::string";
    case TYPE_FLOAT: return "double";
    case TYPE_BOOL: return "bool";
    case TYPE_ADDR: return "entity_addr_t";
    case TYPE_UUID: return "uuid_d";
    default: return "unknown";
    }
  }

  /**
   * Basic: for users, configures some externally visible functional aspect
   * Advanced: for users, configures some internal behaviour
   * Development: not for users.  May be dangerous, may not be documented.
   */
  enum level_t {
    LEVEL_BASIC,
    LEVEL_ADVANCED,
    LEVEL_DEV,
  };

  const char *level_to_str(level_t l) const {
    switch(l) {
      case LEVEL_BASIC: return "basic";
      case LEVEL_ADVANCED: return "advanced";
      case LEVEL_DEV: return "developer";
      default: return "unknown";
    }
  }

  using value_t = boost::variant<
    boost::blank,
    std::string,
    uint64_t,
    int64_t,
    double,
    bool,
    entity_addr_t,
    uuid_d>;
  const std::string name;
  const type_t type;
  const level_t level;

  std::string desc;
  std::string long_desc;

  value_t value;
  value_t daemon_value;

  // Items like mon, osd, rgw, rbd, ceph-fuse.  This is advisory metadata
  // for presentation layers (like web dashboards, or generated docs), so that
  // they know which options to display where.
  // Additionally: "common" for settings that exist in any Ceph code.  Do
  // not use common for settings that are just shared some places: for those
  // places, list them.
  std::list<const char*> services;

  // Topics like:
  // "service": a catchall for the boring stuff like log/asok paths.
  // "network"
  // "performance": a setting that may need adjustment depending on
  //                environment/workload to get best performance.
  std::list<const char*> tags;

  std::list<const char*> see_also;

  value_t min, max;
  std::list<std::string> enum_allowed;

  bool safe;

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
    : name(name), type(t), level(l), safe(false)
  {
    // While value_t is nullable (via boost::blank), we don't ever
    // want it set that way in an Option instance: within an instance,
    // the type of ::value should always match the declared type.
    if (type == TYPE_INT) {
      value = int64_t(0);
    } else if (type == TYPE_UINT) {
      value = uint64_t(0);
    } else if (type == TYPE_STR) {
      value = std::string("");
    } else if (type == TYPE_FLOAT) {
      value = 0.0;
    } else if (type == TYPE_BOOL) {
      value = false;
    } else if (type == TYPE_ADDR) {
      value = entity_addr_t();
    } else if (type == TYPE_UUID) {
      value = uuid_d();
    } else {
      ceph_abort();
    }
  }

  void dump_value(const char *field_name, const value_t &v, Formatter *f) const;

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
    if (type == TYPE_INT) {
      v = int64_t(new_value);
    } else if (type == TYPE_UINT) {
      v = uint64_t(new_value);
    } else if (type == TYPE_FLOAT) {
      v = double(new_value);
    } else if (type == TYPE_BOOL) {
      v = bool(new_value);
    } else {
      std::cerr << "Bad type in set_value: " << name << ": "
                << typeid(T).name() << std::endl;
      ceph_abort();
    }
    return *this;
  }

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
  Option& add_tag(std::initializer_list<const char*> ts) {
    tags.insert(tags.end(), ts);
    return *this;
  }
  Option& add_service(const char* service) {
    services.push_back(service);
    return *this;
  }
  Option& add_service(std::initializer_list<const char*> ss) {
    services.insert(services.end(), ss);
    return *this;
  }
  Option& add_see_also(const char* t) {
    see_also.push_back(t);
    return *this;
  }
  Option& add_see_also(std::initializer_list<const char*> ts) {
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

  Option& set_enum_allowed(const std::list<std::string> allowed)
  {
    enum_allowed = allowed;
    return *this;
  }

  Option &set_safe() {
    safe = true;
    return *this;
  }

  Option &set_validator(const validator_fn_t  &validator_)
  {
    validator = validator_;
    return *this;
  }

  void dump(Formatter *f) const;

  /**
   * A crude indicator of whether the value may be
   * modified safely at runtime -- should be replaced
   * with proper locking!
   */
  bool is_safe() const
  {
    return safe || type == TYPE_BOOL || type == TYPE_INT
                || type == TYPE_UINT || type == TYPE_FLOAT;
  }
};

extern const std::vector<Option> ceph_options;

