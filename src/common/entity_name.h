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

#ifndef CEPH_COMMON_ENTITY_NAME_H
#define CEPH_COMMON_ENTITY_NAME_H

#include <string_view>

#include <ifaddrs.h>

#include "msg/msg_types.h"

/* Represents a Ceph entity name.
 *
 * For example, mds.0 is the name of the first metadata server.
 * client
 */
struct EntityName
{
  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(type, bl);
    encode(id, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    uint32_t type_;
    std::string id_;
    decode(type_, bl);
    decode(id_, bl);
    set(type_, id_);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<EntityName*>& ls);
  const std::string& to_str() const;
  const char *to_cstr() const;
  bool from_str(std::string_view s);
  void set(uint32_t type_, std::string_view id_);
  int set(std::string_view type_, std::string_view id_);
  void set_type(uint32_t type_);
  int set_type(std::string_view type);
  void set_id(std::string_view id_);
  void set_name(entity_name_t n);

  const char* get_type_str() const;

  uint32_t get_type() const { return type; }
  bool is_osd() const { return get_type() == CEPH_ENTITY_TYPE_OSD; }
  bool is_mgr() const { return get_type() == CEPH_ENTITY_TYPE_MGR; }
  bool is_mds() const { return get_type() == CEPH_ENTITY_TYPE_MDS; }
  bool is_client() const { return get_type() == CEPH_ENTITY_TYPE_CLIENT; }
  bool is_mon() const { return get_type() == CEPH_ENTITY_TYPE_MON; }

  std::string_view get_type_name() const;
  const std::string &get_id() const;
  bool has_default_id() const;

  static std::string get_valid_types_as_str();
  static uint32_t str_to_ceph_entity_type(std::string_view);

  friend bool operator<(const EntityName& a, const EntityName& b);
  friend std::ostream& operator<<(std::ostream& out, const EntityName& n);

  bool operator==(const EntityName& rhs) const noexcept {
    return type == rhs.type && id == rhs.id;
  }

private:
  struct str_to_entity_type_t {
    uint32_t type;
    const char *str;
  };
  static const std::array<str_to_entity_type_t, 6> STR_TO_ENTITY_TYPE;

  uint32_t type = 0;
  std::string id;
  std::string type_id;
};

WRITE_CLASS_ENCODER(EntityName)

#endif
