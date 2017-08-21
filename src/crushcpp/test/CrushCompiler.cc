// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2017 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <iostream>
#include <fstream>
#include <gtest/gtest.h>

#include "include/stringify.h"
#include "common/Formatter.h"
#include "crush/CrushCompiler.h"
#include "crush/CrushWrapper.h"

TEST(CrushCompiler, compile) {
  string srcfn = "map.txt";
  ifstream in(srcfn.c_str());
  EXPECT_NE(0, in.is_open());
  CrushWrapper crush;
  CrushCompiler cc(crush, cerr, 1);
  cc.compile(in, srcfn.c_str());
  boost::scoped_ptr<ceph::Formatter> f(ceph::Formatter::create("json-pretty", "json-pretty", "json-pretty"));
  f->open_object_section("crush_map");
  crush.dump(f.get());
  f->close_section();
  f->flush(cout);
  cout << "\n";
}
