// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>

#include "include/stringify.h"
#include "mon/MonCap.h"

#include "gtest/gtest.h"

using namespace std;

const char *parse_good[] = {

  // MonCapMatch
  "allow *",
  "allow r",
  "allow rwx",
  "allow  r",
  " allow rwx",
  "allow rwx ",
  " allow rwx ",
  " allow\t   rwx ",
  "\tallow\nrwx\t",
  "allow service=foo x",
  "allow service=\"froo\" x",
  "allow profile osd",
  "allow profile osd-bootstrap",
  "allow profile \"mds-bootstrap\", allow *",
  "allow command \"a b c\"",
  "allow command abc",
  "allow command abc with arg=foo",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2 prefix bar arg3 prefix baz",
  "allow command abc with arg=foo arg2 prefix \"bar bingo\" arg3 prefix baz",
  "allow command abc with arg regex \"^[0-9a-z.]*$\"",
  "allow command abc with arg regex \"\(invaluid regex\"",
  "allow service foo x",
  "allow service foo x; allow service bar x",
  "allow service foo w ;allow service bar x",
  "allow service foo  w , allow service bar x",
  "allow service foo r , allow service bar x",
  "allow service foo_foo r, allow service bar r",
  "allow service foo-foo r, allow service bar r",
  "allow service \" foo \" w, allow service bar r",
  "allow command abc with arg=foo arg2=bar, allow service foo r",
  "allow command abc.def with arg=foo arg2=bar, allow service foo r",
  "allow command \"foo bar\" with arg=\"baz\"",
  "allow command \"foo bar\" with arg=\"baz.xx\"",
  "profile osd",
  "profile \"mds-bootstrap\", profile foo",
  "allow * network 1.2.3.4/24",
  "allow * network ::1/128",
  "allow * network [aa:bb::1]/128",
  "allow service=foo x network 1.2.3.4/16",
  "allow command abc network 1.2.3.4/8",
  "profile osd network 1.2.3.4/32",
  "allow profile mon network 1.2.3.4/32",
  0
};

TEST(MonCap, ParseGood) {
  for (int i=0; parse_good[i]; ++i) {
    string str = parse_good[i];
    MonCap cap;
    std::cout << "Testing good input: '" << str << "'" << std::endl;
    ASSERT_TRUE(cap.parse(str, &cout));
    std::cout << "                                         -> " << cap << std::endl;
  }
}

// these should stringify to the input value
const char *parse_identity[] = {
  "allow *",
  "allow r",
  "allow rwx",
  "allow service foo x",
  "allow profile osd",
  "allow profile osd-bootstrap",
  "allow profile mds-bootstrap, allow *",
  "allow profile \"foo bar\", allow *",
  "allow command abc",
  "allow command \"a b c\"",
  "allow command abc with arg=foo",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2=bar",
  "allow command abc with arg=foo arg2 prefix bar arg3 prefix baz",
  "allow command abc with arg=foo arg2 prefix \"bar bingo\" arg3 prefix baz",
  "allow service foo x",
  "allow service foo x, allow service bar x",
  "allow service foo w, allow service bar x",
  "allow service foo r, allow service bar x",
  "allow service foo_foo r, allow service bar r",
  "allow service foo-foo r, allow service bar r",
  "allow service \" foo \" w, allow service bar r",
  "allow command abc with arg=foo arg2=bar, allow service foo r",
  0
};

TEST(MonCap, ParseIdentity)
{
  for (int i=0; parse_identity[i]; ++i) {
    string str = parse_identity[i];
    MonCap cap;
    std::cout << "Testing good input: '" << str << "'" << std::endl;
    ASSERT_TRUE(cap.parse(str, &cout));
    string out = stringify(cap);
    ASSERT_EQ(out, str);
  }
}

const char *parse_bad[] = {
  "allow r foo",
  "allow*",
  "foo allow *",
  "allow profile foo rwx",
  "allow profile",
  "allow profile foo bar rwx",
  "allow service bar",
  "allow command baz x",
  "allow r w",
  "ALLOW r",
  "allow rwx,",
  "allow rwx x",
  "allow r pool foo r",
  "allow wwx pool taco",
  "allow wwx pool taco^funny&chars",
  "allow rwx pool 'weird name''",
  "allow rwx object_prefix \"beforepool\" pool weird",
  "allow rwx auid 123 pool asdf",
  "allow command foo a prefix b",
  "allow command foo with a prefixb",
  "allow command foo with a = prefix b",
  "allow command foo with a prefix b c",
  0
};

TEST(MonCap, ParseBad) {
  for (int i=0; parse_bad[i]; ++i) {
    string str = parse_bad[i];
    MonCap cap;
    std::cout << "Testing bad input: '" << str << "'" << std::endl;
    ASSERT_FALSE(cap.parse(str, &cout));
  }
}

TEST(MonCap, AllowAll) {
  MonCap cap;
  ASSERT_FALSE(cap.is_allow_all());

  ASSERT_TRUE(cap.parse("allow r", NULL));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow w", NULL));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow x", NULL));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rwx", NULL));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rw", NULL));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow rx", NULL));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow wx", NULL));
  ASSERT_FALSE(cap.is_allow_all());
  cap.grants.clear();

  ASSERT_TRUE(cap.parse("allow *", NULL));
  ASSERT_TRUE(cap.is_allow_all());
  ASSERT_TRUE(cap.is_capable(NULL, {}, "foo", "asdf", {}, true, true, true,
                             {}));

  MonCap cap2;
  ASSERT_FALSE(cap2.is_allow_all());
  cap2.set_allow_all();
  ASSERT_TRUE(cap2.is_allow_all());
}

TEST(MonCap, Network) {
  MonCap cap;
  bool r = cap.parse("allow * network 192.168.0.0/16, allow * network 10.0.0.0/8", NULL);
  ASSERT_TRUE(r);

  entity_addr_t a, b, c;
  a.parse("10.1.2.3");
  b.parse("192.168.2.3");
  c.parse("192.167.2.3");

  ASSERT_TRUE(cap.is_capable(NULL, {}, "foo", "asdf", {}, true, true, true,
			     a));
  ASSERT_TRUE(cap.is_capable(NULL, {}, "foo", "asdf", {}, true, true, true,
			     b));
  ASSERT_FALSE(cap.is_capable(NULL, {}, "foo", "asdf", {}, true, true, true,
			     c));
}

TEST(MonCap, ProfileOSD) {
  MonCap cap;
  bool r = cap.parse("allow profile osd", NULL);
  ASSERT_TRUE(r);

  EntityName name;
  name.from_str("osd.123");
  map<string,string> ca;

  ASSERT_TRUE(cap.is_capable(NULL, name, "osd", "", ca, true, false, false,
			     {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "osd", "", ca, true, true, false, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "osd", "", ca, true, true, true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "osd", "", ca, true, true, true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "mon", "", ca, true, false, false,
                             {}));

  ASSERT_FALSE(cap.is_capable(NULL, name, "mds", "", ca, true, true, true, {}));
  ASSERT_FALSE(cap.is_capable(NULL, name, "mon", "", ca, true, true, true, {}));

  ca.clear();
  ASSERT_FALSE(cap.is_capable(NULL, name, "", "config-key get", ca, true, true,
                              true, {}));
  ca["key"] = "daemon-private/osd.123";
  ASSERT_FALSE(cap.is_capable(NULL, name, "", "config-key get", ca, true, true,
                              true, {}));
  ca["key"] = "daemon-private/osd.12/asdf";
  ASSERT_FALSE(cap.is_capable(NULL, name, "", "config-key get", ca, true, true,
                              true, {}));
  ca["key"] = "daemon-private/osd.123/";
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key get", ca, true, true,
                             true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key get", ca, true, true,
                             true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key get", ca, true, true,
                             true, {}));
  ca["key"] = "daemon-private/osd.123/foo";
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key get", ca, true, true,
                             true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key put", ca, true, true,
                             true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key set", ca, true, true,
                             true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key exists", ca, true,
                             true, true, {}));
  ASSERT_TRUE(cap.is_capable(NULL, name, "", "config-key delete", ca, true,
                             true, true, {}));
}

TEST(MonCap, CommandRegEx) {
  MonCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("allow command abc with arg regex \"^[0-9a-z.]*$\"",
                        NULL));

  EntityName name;
  name.from_str("osd.123");
  ASSERT_TRUE(cap.is_capable(nullptr, name, "", "abc", {{"arg", "12345abcde"}},
                             true, true, true, {}));
  ASSERT_FALSE(cap.is_capable(nullptr, name, "", "abc", {{"arg", "~!@#$"}},
                              true, true, true, {}));

  ASSERT_TRUE(cap.parse("allow command abc with arg regex \"[*\"", NULL));
  ASSERT_FALSE(cap.is_capable(nullptr, name, "", "abc", {{"arg", ""}}, true,
                              true, true, {}));
}

TEST(MonCap, ProfileBootstrapRBD) {
  MonCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("profile bootstrap-rbd", NULL));

  EntityName name;
  name.from_str("mon.a");
  ASSERT_TRUE(cap.is_capable(nullptr,  name, "",
                             "auth get-or-create", {
                               {"entity", "client.rbd"},
                               {"caps_mon", "profile rbd"},
                               {"caps_osd", "profile rbd pool=foo, profile rbd-read-only"},
                             }, true, true, true,
			     {}));
  ASSERT_FALSE(cap.is_capable(nullptr,  name, "",
                              "auth get-or-create", {
                                {"entity", "client.rbd"},
                                {"caps_mon", "allow *"},
                                {"caps_osd", "profile rbd"},
                              }, true, true, true,
			      {}));
  ASSERT_FALSE(cap.is_capable(nullptr,  name, "",
                              "auth get-or-create", {
                                {"entity", "client.rbd"},
                                {"caps_mon", "profile rbd"},
                                {"caps_osd", "profile rbd pool=foo, allow *, profile rbd-read-only"},
                              }, true, true, true,
			      {}));
}

TEST(MonCap, ProfileBootstrapRBDMirror) {
  MonCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("profile bootstrap-rbd-mirror", NULL));

  EntityName name;
  name.from_str("mon.a");
  ASSERT_TRUE(cap.is_capable(nullptr,  name, "",
                             "auth get-or-create", {
                               {"entity", "client.rbd"},
                               {"caps_mon", "profile rbd-mirror"},
                               {"caps_osd", "profile rbd pool=foo, profile rbd-read-only"},
                             }, true, true, true,
			     {}));
  ASSERT_FALSE(cap.is_capable(nullptr,  name, "",
                              "auth get-or-create", {
                                {"entity", "client.rbd"},
                                {"caps_mon", "profile rbd"},
                                {"caps_osd", "profile rbd pool=foo, profile rbd-read-only"},
                              }, true, true, true,
			      {}));
  ASSERT_FALSE(cap.is_capable(nullptr,  name, "",
                              "auth get-or-create", {
                                {"entity", "client.rbd"},
                                {"caps_mon", "allow *"},
                                {"caps_osd", "profile rbd"},
                              }, true, true, true,
			      {}));
  ASSERT_FALSE(cap.is_capable(nullptr,  name, "",
                              "auth get-or-create", {
                                {"entity", "client.rbd"},
                                {"caps_mon", "profile rbd-mirror"},
                                {"caps_osd", "profile rbd pool=foo, allow *, profile rbd-read-only"},
                              }, true, true, true,
			      {}));
}

TEST(MonCap, ProfileRBD) {
  MonCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("profile rbd", NULL));

  EntityName name;
  name.from_str("mon.a");
  ASSERT_FALSE(cap.is_capable(nullptr,  name, "config-key",
                              "config-key get", {
                                {"key", "rbd/mirror/peer/1/1234"},
                              }, true, false, false, {}));
}

TEST(MonCap, ProfileRBDMirror) {
  MonCap cap;
  ASSERT_FALSE(cap.is_allow_all());
  ASSERT_TRUE(cap.parse("profile rbd-mirror", NULL));

  EntityName name;
  name.from_str("mon.a");
  ASSERT_TRUE(cap.is_capable(nullptr, name, "config-key",
                             "config-key get", {
                               {"key", "rbd/mirror/peer/1/1234"},
                             }, true, false, false, {}));
}
