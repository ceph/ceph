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
#include "include/msgr.h"

#include <sstream>
#include <string>

using std::string;

extern const char *ceph_entity_type_name(int type);

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
  int pos = s.find('.');

  if (pos < 0)
    return false;
 
  string type_ = s.substr(0, pos);
  string id_ = s.substr(pos + 1);
  set(type_, id_);
  return true;
}

void EntityName::
set(uint32_t type_, const std::string &id_)
{
  type = type_;
  id = id_;

  std::ostringstream oss;
  oss << ceph_entity_type_name(type_) << "." << id_;
  type_id = oss.str();
}

void EntityName::
set(const std::string &type_, const std::string &id_)
{
  set(str_to_ceph_entity_type(type_.c_str()), id_);
}

void EntityName::
set_type(uint32_t type_)
{
  set(type_, id);
}

void EntityName::
set_type(const char *type_)
{
  set(type_, id);
}

void EntityName::
set_id(const std::string &id_)
{
  set(type, id_);
}

const char* EntityName::
get_type_str() const
{
  return ceph_entity_type_name(type);
}

bool EntityName::
is_admin() const
{
  return (id.compare("admin") == 0);
}

uint32_t EntityName::
get_type() const
{
  return type;
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
  if (strcmp(str, "auth") == 0) {
    return CEPH_ENTITY_TYPE_AUTH;
  } else if (strcmp(str, "mon") == 0) {
    return CEPH_ENTITY_TYPE_MON;
  } else if (strcmp(str, "osd") == 0) {
    return CEPH_ENTITY_TYPE_OSD;
  } else if (strcmp(str, "mds") == 0) {
    return CEPH_ENTITY_TYPE_MDS;
  } else {
    return CEPH_ENTITY_TYPE_CLIENT;
  }
}
