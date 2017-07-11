// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <list>
#include <boost/variant.hpp>
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

  const char *type_to_str(type_t t) {
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

  enum level_t {
    LEVEL_BASIC,
    LEVEL_ADVANCED,
    LEVEL_DEV,
  };

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
  value_t nondaemon_value;

  std::list<std::string> tags;
  std::list<std::string> see_also;

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
  template<typename T>
  Option& set_nondaemon_default(const T& v) {
    return set_value(nondaemon_value, v);
  }
  Option& add_tag(const char* t) {
    tags.push_back(t);
    return *this;
  }
  Option& add_see_also(const char* t) {
    see_also.push_back(t);
    return *this;
  }
  Option& set_description(const char* new_desc) {
    desc = new_desc;
    return *this;
  }
  Option& set_long_description(const char* new_desc) {
    desc = new_desc;
    return *this;
  }
  template<typename T>
  Option& set_min_max(const T& mi, const T& ma) {
    min = mi;
    max = ma;
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

  /**
   * A crude indicator of whether the value may be
   * modified safely at runtime -- should be replaced
   * with proper locking!
   */
  bool is_safe() const
  {
    return type == TYPE_INT || type == TYPE_FLOAT;
  }
};

extern const std::vector<Option> ceph_options;

