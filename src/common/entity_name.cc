// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "common/Formatter.h"

#include <sstream>
#include <string>
#include <string_view>
#include <vector>

using std::string;
using namespace std::literals::string_view_literals;

void EntityName::encode(ceph::buffer::list& bl) const
{
  using ceph::encode;
  {
    uint32_t _type = type;
    encode(_type, bl);
  }
  encode(id, bl);
}

void EntityName::decode(ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  std::string id_;
  uint32_t _type;
  decode(_type, bl);
  decode(id_, bl);
  set(static_cast<entity_type_t>(_type), id_);
}

using namespace std::string_literals;
static const std::vector<std::pair<entity_type_t, std::string>> STR_TO_ENTITY_TYPE = {
  { CEPH_ENTITY_TYPE_AUTH, "auth"s },
  { CEPH_ENTITY_TYPE_MON, "mon"s },
  { CEPH_ENTITY_TYPE_OSD, "osd"s },
  { CEPH_ENTITY_TYPE_MDS, "mds"s },
  { CEPH_ENTITY_TYPE_MGR, "mgr"s },
  { CEPH_ENTITY_TYPE_CLIENT, "client"s },
};

void EntityName::dump(ceph::Formatter *f) const {
  f->dump_int("type", type);
  f->dump_string("type_str", get_type_name());
  f->dump_string("id", id);
}

std::list<EntityName> EntityName::generate_test_instances() {
  std::list<EntityName> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().set_type(CEPH_ENTITY_TYPE_OSD);
  ls.back().set_id("0");
  ls.emplace_back();
  ls.back().set_type(CEPH_ENTITY_TYPE_MDS);
  ls.back().set_id("a");
  return ls;
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

void EntityName::set(entity_type_t type_, std::string_view id_) {
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
  auto t = str_to_ceph_entity_type(type_);
  if (t == CEPH_ENTITY_TYPE_ANY)
    return -EINVAL;
  set(t, id_);
  return 0;
}

void EntityName::set_type(entity_type_t type_) {
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
    out << STR_TO_ENTITY_TYPE[i].first;
  }
  return out.str();
}

entity_type_t EntityName::str_to_ceph_entity_type(std::string_view s)
{
  auto l = [s](const auto& p) {
    return s == p.second;
  };
  auto it = std::find_if(STR_TO_ENTITY_TYPE.begin(), STR_TO_ENTITY_TYPE.end(), std::move(l));
  if (it == STR_TO_ENTITY_TYPE.end()) {
    return CEPH_ENTITY_TYPE_ANY;
  } else {
    return it->first;
  }
}

std::string_view EntityName::ceph_entity_type_to_str(entity_type_t type)
{
  auto l = [type](const auto& p) {
    return type == p.first;
  };
  auto it = std::find_if(STR_TO_ENTITY_TYPE.begin(), STR_TO_ENTITY_TYPE.end(), std::move(l));
  if (it == STR_TO_ENTITY_TYPE.end()) {
    return "???"sv;
  } else {
    return it->second;
  }
}

bool operator<(const EntityName& a, const EntityName& b)
{
  return (a.type < b.type) || (a.type == b.type && a.id < b.id);
}

std::ostream& operator<<(std::ostream& out, const EntityName& n)
{
  return out << n.to_str();
}
