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

#include <iosfwd>
#include <stdint.h>
#include <string>

#include "include/encoding.h"
#include "include/buffer.h"

/* Represents a Ceph entity name.
 *
 * For example, mds.0 is the name of the first metadata server.
 * client
 */
struct EntityName
{
  EntityName();

  void encode(bufferlist& bl) const {
    ::encode(type, bl);
    ::encode(id, bl);
  }
  void decode(bufferlist::iterator& bl) {
    uint32_t type_;
    std::string id_;
    ::decode(type_, bl);
    ::decode(id_, bl);
    set(type_, id_);
  }

  const std::string& to_str() const;
  const char *to_cstr() const;
  bool from_str(const std::string& s);
  void set(uint32_t type_, const std::string &id_);
  void set(const std::string &type_, const std::string &id_);
  void set_type(uint32_t type_);
  void set_type(const char *type);

  const char* get_type_str() const;
  bool is_admin() const;
  uint32_t get_type() const;
  const std::string &get_id() const;

  friend bool operator<(const EntityName& a, const EntityName& b);
  friend std::ostream& operator<<(std::ostream& out, const EntityName& n);

private:
  uint32_t type;
  std::string id;
  std::string type_id;
};

WRITE_CLASS_ENCODER(EntityName);

#endif
