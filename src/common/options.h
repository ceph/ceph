// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <list>
#include <boost/variant.hpp>

//#include "include/uuid.h"
//#include "msg/msg_types.h"

struct Option {
  typedef enum {
    TYPE_INT,
    TYPE_STR,
    TYPE_FLOAT,
    TYPE_BOOL,
    TYPE_ADDR,
    TYPE_UUID,
  } type_t;

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

  typedef enum {
    LEVEL_BASIC,
    LEVEL_ADVANCED,
    LEVEL_DEV,
  } level_t;

  typedef boost::variant<
    std::string,
    int64_t,
    double,
    bool//,
    //entity_addr_t,
    /*uuid_d*/> value_t;
  std::string name;
  type_t type;
  level_t level;

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

  template<typename T>
  Option& set_default(const T& v) {
    value = v;
    return *this;
  }
  template<typename T>
  Option& set_daemon_default(const T& v) {
    daemon_value = v;
    return *this;
  }
  template<typename T>
  Option& set_nondaemon_default(const T& v) {
    daemon_value = v;
    return *this;
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
