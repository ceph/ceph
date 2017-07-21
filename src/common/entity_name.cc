// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/entity_name.h"

#include <sstream>

using std::string;

extern const char *ceph_entity_type_name(int type);

struct str_to_entity_type_t {
  uint32_t type;
  const char *str;
};

static const str_to_entity_type_t STR_TO_ENTITY_TYPE[] = {
  { CEPH_ENTITY_TYPE_AUTH, "auth" },
  { CEPH_ENTITY_TYPE_MON, "mon" },
  { CEPH_ENTITY_TYPE_OSD, "osd" },
  { CEPH_ENTITY_TYPE_MDS, "mds" },
  { CEPH_ENTITY_TYPE_MGR, "mgr" },
  { CEPH_ENTITY_TYPE_CLIENT, "client" },
};

EntityName::
EntityName()
  : type(0)
{
}

const std::string& EntityName::
to_str() const
{
  return type_id;
}

const char* EntityName::
to_cstr() const
{
  return type_id.c_str();
}

bool EntityName::
from_str(const string& s)
{
  size_t pos = s.find('.');

  if (pos == string::npos)
    return false;
 
  string type_ = s.substr(0, pos);
  string id_ = s.substr(pos + 1);
  if (set(type_, id_))
    return false;
  return true;
}

void EntityName::
set(uint32_t type_, const std::string &id_)
{
  type = type_;
  id = id_;

  if (type) {
    std::ostringstream oss;
    oss << ceph_entity_type_name(type_) << "." << id_;
    type_id = oss.str();
  } else {
    type_id.clear();
  }
}

int EntityName::
set(const std::string &type_, const std::string &id_)
{
  uint32_t t = str_to_ceph_entity_type(type_.c_str());
  if (t == CEPH_ENTITY_TYPE_ANY)
    return -EINVAL;
  set(t, id_);
  return 0;
}

void EntityName::
set_type(uint32_t type_)
{
  set(type_, id);
}

int EntityName::
set_type(const char *type_)
{
  return set(type_, id);
}

void EntityName::
set_id(const std::string &id_)
{
  set(type, id_);
}

void EntityName::set_name(entity_name_t n)
{
  char s[40];
  sprintf(s, "%lld", (long long)n.num());
  set(n.type(), s);
}

const char* EntityName::
get_type_str() const
{
  return ceph_entity_type_name(type);
}

const char *EntityName::
get_type_name() const
{
  return ceph_entity_type_name(type);
}

const std::string &EntityName::
get_id() const
{
  return id;
}

bool EntityName::
has_default_id() const
{
  return (id == "admin");
}

std::string EntityName::
get_valid_types_as_str()
{
  std::string out;
  size_t i;
  std::string sep("");
  for (i = 0; i < sizeof(STR_TO_ENTITY_TYPE)/sizeof(STR_TO_ENTITY_TYPE[0]); ++i) {
    out += sep;
    out += STR_TO_ENTITY_TYPE[i].str;
    sep = ", ";
  }
  return out;
}

bool operator<(const EntityName& a, const EntityName& b)
{
  return (a.type < b.type) || (a.type == b.type && a.id < b.id);
}

std::ostream& operator<<(std::ostream& out, const EntityName& n)
{
  return out << n.to_str();
}

uint32_t str_to_ceph_entity_type(const char * str)
{
  size_t i;
  for (i = 0; i < sizeof(STR_TO_ENTITY_TYPE)/sizeof(STR_TO_ENTITY_TYPE[0]); ++i) {
    if (strcmp(str, STR_TO_ENTITY_TYPE[i].str) == 0)
      return STR_TO_ENTITY_TYPE[i].type;
  }
  return CEPH_ENTITY_TYPE_ANY;
}
