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
#include "common/Formatter.h"

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
