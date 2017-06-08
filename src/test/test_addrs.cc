// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"
#include "msg/msg_types.h"
#include "gtest/gtest.h"

#include <sstream>

// input, parsed+printed addr output, leftover
// if the parse fails, output + leftover should both be blank.
const char *addr_checks[][3] = {
  { "127.0.0.1", "127.0.0.1:0/0", "" },
  { "127.0.0.1 foo", "127.0.0.1:0/0", " foo" },
  { "127.0.0.1:1234 foo", "127.0.0.1:1234/0", " foo" },
  { "127.0.0.1:1234/5678 foo", "127.0.0.1:1234/5678", " foo" },
  { "1.2.3:4 a", "", "" },
  { "2607:f298:4:2243::5522", "[2607:f298:4:2243::5522]:0/0", "" },
  { "[2607:f298:4:2243::5522]", "[2607:f298:4:2243::5522]:0/0", "" },
  { "2607:f298:4:2243::5522a", "", "" },
  { "[2607:f298:4:2243::5522]a", "[2607:f298:4:2243::5522]:0/0", "a" },
  { "[2607:f298:4:2243::5522]:1234a", "[2607:f298:4:2243::5522]:1234/0", "a" },
  { "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "[2001:db8:85a3::8a2e:370:7334]:0/0", "" },
  { "2001:2db8:85a3:4334:4324:8a2e:1370:7334", "[2001:2db8:85a3:4334:4324:8a2e:1370:7334]:0/0", "" },
  { "::", "[::]:0/0", "" },
  { "::zz", "[::]:0/0", "zz" },
  { ":: 12:34", "[::]:0/0", " 12:34" },
  { NULL, NULL, NULL },
};


TEST(Msgr, TestAddrParsing)
{

  for (unsigned i = 0; addr_checks[i][0]; ++i) {
    entity_addr_t a;
    const char *end = "";
    bool ok = a.parse(addr_checks[i][0], &end);
    string in = addr_checks[i][0];
    string out;
    if (ok) {
      stringstream ss;
      ss << a;
      getline(ss, out);
    }
    string left = end;
      
    cout << "'" << addr_checks[i][0] << "' -> '" << out << "' + '" << left << "'" << std::endl;

    ASSERT_EQ(out, addr_checks[i][1]);
    ASSERT_EQ(left, addr_checks[i][2]);
  }
}
