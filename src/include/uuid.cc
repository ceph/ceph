// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "uuid.h"
#include "random.h"
#include "common/Formatter.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <random>

void uuid_d::generate_random() {
  random_device_t rng;
  boost::uuids::basic_random_generator gen(rng);
  uuid = gen();
}

bool uuid_d::parse(const char *s) {
  try {
    boost::uuids::string_generator gen;
    uuid = gen(s);
    return true;
  } catch (std::runtime_error& e) {
    return false;
  }
}

void uuid_d::print(char *s) const {
  memcpy(s, boost::uuids::to_string(uuid).c_str(), 37);
}

std::string uuid_d::to_string() const {
  return boost::uuids::to_string(uuid);
}

void uuid_d::dump(ceph::Formatter *f) const
{
  f->dump_stream("uuid") << to_string();
}

void uuid_d::generate_test_instances(std::list<uuid_d*>& o)
{
  // these are sourced from examples at
  // https://www.boost.org/doc/libs/1_62_0/libs/uuid/uuid.html#Synopsis_generators
  boost::uuids::string_generator gen;
  o.push_back(new uuid_d());
  o.back()->uuid = gen("{01234567-89ab-cdef-0123-456789abcdef}");
  o.push_back(new uuid_d());
  o.back()->uuid = gen(L"01234567-89ab-cdef-0123-456789abcdef");
  o.push_back(new uuid_d());
  o.back()->uuid = gen(std::string("0123456789abcdef0123456789abcdef"));
  o.push_back(new uuid_d());
  o.back()->uuid = gen(std::wstring(L"01234567-89ab-cdef-0123-456789abcdef"));
}
