// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include "include/uuid.h"
#include "msg/msg_types.h"

struct Option {
  typedef enum {
    TYPE_INT,
    TYPE_STR,
    TYPE_DOUBLE,
    TYPE_BOOL,
    TYPE_ADDR,
    TYPE_UUID,
  } type_t;

  typedef enum {
    LEVEL_BASIC,
    LEVEL_ADVANCED,
    LEVEL_DEV,
  };

  typedef boost::variant<std::string, int64_t, double, bool,
			 entity_addr_t, uuid_d> value_t;

  value_t value;
  value_t daemon_value;
  value_t nondaemon_value;

  string name;
  string desc;
  string long_desc;

  list<std::string> tags;

  value_t min, max;
  list<std::string> enum_allowed;

  Option(const char* name)
    : name(name)
  {}

  template<typename T>
  Option& set_default(const T& v) {
    value = v;
    return *this;
  }
  Option& set_daemon_default(const T& v) {
    daemon_value = v;
    return *this;
  }
  Option& set_nondaemon_default(const T& v) {
    daemon_value = v;
    return *this;
  }
  Option& add_tag(const char* t) {
    tags.push_back(t);
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
  Option& set_min(const char* v) {
    min = v;
    return *this;
  }
  template<typename T>
  Option& set_max(const char* v) {
    min = v;
    return *this;
  }
};

// array of ceph options.  the last one will have a blank name.
extern struct Option *ceph_options;
