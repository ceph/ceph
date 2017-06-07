// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <list>
#include <boost/variant.hpp>

//#include "include/uuid.h"
//#include "msg/msg_types.h"

struct Option {
  enum type_t {
    TYPE_INT,
    TYPE_STR,
    TYPE_FLOAT,
    TYPE_BOOL,
    TYPE_ADDR,
    TYPE_UUID,
  };

  const char *type_to_str(type_t t) {
    switch (t) {
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
    std::string,
    int64_t,
    double,
    bool//,
    //entity_addr_t,
    /*uuid_d*/>;
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

  Option(const char* name, type_t t, level_t l)
    : name(name), type(t), level(l)
  {}

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
  template<typename T, typename is_integer<T>::type = 0>
  Option& set_value(value_t& v, T new_value) {
    v = int64_t(new_value);
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
};

// array of ceph options.  the last one will have a blank name.
extern struct Option *ceph_options;
