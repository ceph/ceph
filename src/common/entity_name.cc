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
#include "common/ceph_strings.h"

#include <sstream>

using std::string;


const std::array<EntityName::str_to_entity_type_t, 6> EntityName::STR_TO_ENTITY_TYPE = {{
  { CEPH_ENTITY_TYPE_AUTH, "auth" },
  { CEPH_ENTITY_TYPE_MON, "mon" },
  { CEPH_ENTITY_TYPE_OSD, "osd" },
  { CEPH_ENTITY_TYPE_MDS, "mds" },
  { CEPH_ENTITY_TYPE_MGR, "mgr" },
  { CEPH_ENTITY_TYPE_CLIENT, "client" },
}};

void EntityName::dump(ceph::Formatter *f) const {
  f->dump_int("type", type);
  f->dump_string("id", id);
}

void EntityName::generate_test_instances(std::list<EntityName*>& ls) {
  ls.push_back(new EntityName);
  ls.push_back(new EntityName);
  ls.back()->set_type(CEPH_ENTITY_TYPE_OSD);
  ls.back()->set_id("0");
  ls.push_back(new EntityName);
  ls.back()->set_type(CEPH_ENTITY_TYPE_MDS);
  ls.back()->set_id("a");
}

const std::string& EntityName::to_str() const {
  return type_id;
}

const char* EntityName::to_cstr() const {
  return type_id.c_str();
}

bool EntityName::from_str(std::string_view s) {
  size_t pos = s.find('.');

  if (pos == string::npos)
    return false;

  auto type_ = s.substr(0, pos);
  auto id_ = s.substr(pos + 1);
  if (set(type_, id_))
    return false;
  return true;
}

void EntityName::set(uint32_t type_, std::string_view id_) {
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

int EntityName::set(std::string_view type_, std::string_view id_) {
  uint32_t t = str_to_ceph_entity_type(type_);
  if (t == CEPH_ENTITY_TYPE_ANY)
    return -EINVAL;
  set(t, id_);
  return 0;
}

void EntityName::set_type(uint32_t type_) {
  set(type_, id);
}

int EntityName::
set_type(std::string_view type_)
{
  return set(type_, id);
}

void EntityName::set_id(std::string_view id_) {
  set(type, id_);
}

void EntityName::set_name(entity_name_t n)
{
  char s[40];
  sprintf(s, "%lld", (long long)n.num());
  set(n.type(), s);
}

const char* EntityName::get_type_str() const {
  return ceph_entity_type_name(type);
}

std::string_view EntityName::get_type_name() const {
  return ceph_entity_type_name(type);
}

const std::string &EntityName::get_id() const {
  return id;
}

bool EntityName::has_default_id() const {
  return (id == "admin");
}

std::string EntityName::get_valid_types_as_str() {
  std::ostringstream out;
  size_t i;
  for (i = 0; i < STR_TO_ENTITY_TYPE.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << STR_TO_ENTITY_TYPE[i].str;
  }
  return out.str();
}

uint32_t EntityName::str_to_ceph_entity_type(std::string_view s)
{
  size_t i;
  for (i = 0; i < STR_TO_ENTITY_TYPE.size(); ++i) {
    if (s == STR_TO_ENTITY_TYPE[i].str)
      return STR_TO_ENTITY_TYPE[i].type;
  }
  return CEPH_ENTITY_TYPE_ANY;
}

bool operator<(const EntityName& a, const EntityName& b)
{
  return (a.type < b.type) || (a.type == b.type && a.id < b.id);
}

std::ostream& operator<<(std::ostream& out, const EntityName& n)
{
  return out << n.to_str();
}
