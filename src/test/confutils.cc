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
#include "common/ConfUtils.h"
#include "common/errno.h"
#include "gtest/gtest.h"

#include <errno.h>
#include <iostream>
#include <stdlib.h>
#include <sstream>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>

using std::cerr;
using std::ostringstream;

#define MAX_FILES_TO_DELETE 1000UL

static size_t config_idx = 0;
static size_t unlink_idx = 0;
static char *to_unlink[MAX_FILES_TO_DELETE];

static std::string get_temp_dir()
{
  static std::string temp_dir;

  if (temp_dir.empty()) {
    const char *tmpdir = getenv("TMPDIR");
    if (!tmpdir)
      tmpdir = "/tmp";
    srand(time(NULL));
    ostringstream oss;
    oss << tmpdir << "/confutils_test_dir." << rand() << "." << getpid();
    umask(022);
    int res = mkdir(oss.str().c_str(), 01777);
    if (res) {
      cerr << "failed to create temp directory '" << temp_dir << "'" << std::endl;
      return "";
    }
    temp_dir = oss.str();
  }
  return temp_dir;
}

static void unlink_all(void)
{
  for (size_t i = 0; i < unlink_idx; ++i) {
    unlink(to_unlink[i]);
  }
  for (size_t i = 0; i < unlink_idx; ++i) {
    free(to_unlink[i]);
  }
  rmdir(get_temp_dir().c_str());
}

static int create_tempfile(const std::string &fname, const char *text)
{
  FILE *fp = fopen(fname.c_str(), "w");
  if (!fp) {
    int err = errno;
    cerr << "Failed to write file '" << fname << "' to temp directory '"
	 << get_temp_dir() << "'. " << cpp_strerror(err) << std::endl;
    return err;
  }
  if (unlink_idx >= MAX_FILES_TO_DELETE)
    return -ENOBUFS;
  if (unlink_idx == 0) {
    memset(to_unlink, 0, sizeof(to_unlink));
    atexit(unlink_all);
  }
  to_unlink[unlink_idx++] = strdup(fname.c_str());
  size_t strlen_text = strlen(text);
  size_t res = fwrite(text, 1, strlen_text, fp);
  if (res != strlen_text) {
    int err = errno;
    cerr << "fwrite error while writing to " << fname 
	 << ": " << cpp_strerror(err) << std::endl;
    fclose(fp);
    return err;
  }
  fclose(fp);
  return 0;
}

static std::string next_tempfile(const char *text)
{
  ostringstream oss;
  std::string temp_dir(get_temp_dir());
  if (temp_dir.empty())
    return "";
  oss << temp_dir << "/test_config." << config_idx++ << ".config";
  int ret = create_tempfile(oss.str(), text);
  if (ret)
    return "";
  return oss.str();
}

const char * const simple_conf_1 = "\
; here's a comment\n\
[global]\n\
        keyring = .my_ceph_keyring\n\
\n\
[mds]\n\
	log dir = out\n\
	log per instance = true\n\
	log sym history = 100\n\
        profiling logger = true\n\
	profiling logger dir = wowsers\n\
	chdir = ""\n\
	pid file = out/$name.pid\n\
\n\
        mds debug frag = true\n\
[osd]\n\
	pid file = out/$name.pid\n\
        osd scrub load threshold = 5.0\n\
\n\
        lockdep = 1\n\
[osd0]\n\
        osd data = dev/osd0\n\
        osd journal size = 100\n\
[mds.a]\n\
[mds.b]\n\
[mds.c]\n\
";

// we can add whitespace at odd locations and it will get stripped out.
const char * const simple_conf_2 = "\
[mds.a]\n\
	log dir = special_mds_a\n\
[mds]\n\
	log sym history = 100\n\
	log dir = out # after a comment, anything # can ### happen ;;; right?\n\
	log per instance = true\n\
        profiling logger = true\n\
	profiling                 logger dir = log\n\
	chdir = ""\n\
        pid file\t=\tfoo2\n\
[osd0]\n\
        keyring   =       osd_keyring          ; osd's keyring\n\
\n\
[global]\n\
	# I like pound signs as comment markers.\n\
	; Do you like pound signs as comment markers?\n\
        keyring = shenanigans          ; The keyring of a leprechaun\n\
\n\
	# Let's just have a line with a lot of whitespace and nothing else.\n\
                         \n\
        lockdep = 1\n\
";

// test line-combining
const char * const conf3 = "\
[global]\n\
	log file = /quite/a/long/path\\\n\
/for/a/log/file\n\
	pid file = \\\n\
                           spork\\\n\
\n\
[mon] #nothing here \n\
";

// illegal because it contains an invalid utf8 sequence.
const char illegal_conf1[] = "\
[global]\n\
	log file = foo\n\
	pid file = invalid-utf-\xe2\x28\xa1\n\
[osd0]\n\
        keyring = osd_keyring          ; osd's keyring\n\
";

// illegal because it contains a malformed section header.
const char illegal_conf2[] = "\
[global\n\
	log file = foo\n\
[osd0]\n\
        keyring = osd_keyring          ; osd's keyring\n\
";

// illegal because it contains a line that doesn't parse
const char illegal_conf3[] = "\
[global]\n\
        who_what_where\n\
[osd0]\n\
        keyring = osd_keyring          ; osd's keyring\n\
";

// unicode config file
const char unicode_config_1[] = "\
[global]\n\
        log file =           \x66\xd1\x86\xd1\x9d\xd3\xad\xd3\xae     \n\
        pid file =           foo-bar\n\
[osd0]\n\
";

TEST(ParseFiles1, ConfUtils) {
  std::string simple_conf_1_f(next_tempfile(simple_conf_1));
  ConfFile cf1(simple_conf_1_f.c_str());
  ASSERT_EQ(cf1.parse(), 0);

  std::string simple_conf_2_f(next_tempfile(simple_conf_1));
  ConfFile cf2(simple_conf_2_f.c_str());
  ASSERT_EQ(cf2.parse(), 0);

  bufferlist bl3;
  bl3.append(simple_conf_1, strlen(simple_conf_1));
  ConfFile cf3(&bl3);
  ASSERT_EQ(cf3.parse(), 0);

  bufferlist bl4;
  bl4.append(simple_conf_2, strlen(simple_conf_2));
  ConfFile cf4(&bl4);
  ASSERT_EQ(cf4.parse(), 0);
}

TEST(ReadFiles1, ConfUtils) {
  std::string simple_conf_1_f(next_tempfile(simple_conf_1));
  ConfFile cf1(simple_conf_1_f.c_str());
  ASSERT_EQ(cf1.parse(), 0);

  std::string val;
  ASSERT_EQ(cf1.read("global", "keyring", val), 0);
  ASSERT_EQ(val, ".my_ceph_keyring");

  ASSERT_EQ(cf1.read("mds", "profiling logger dir", val), 0);
  ASSERT_EQ(val, "wowsers");

  ASSERT_EQ(cf1.read("mds", "something that does not exist", val), -ENOENT);

  // exists in mds section, but not in global
  ASSERT_EQ(cf1.read("global", "profiling logger dir", val), -ENOENT);

  bufferlist bl2;
  bl2.append(simple_conf_2, strlen(simple_conf_2));
  ConfFile cf2(&bl2);
  ASSERT_EQ(cf2.parse(), 0);
  ASSERT_EQ(cf2.read("osd0", "keyring", val), 0);
  ASSERT_EQ(val, "osd_keyring");

  ASSERT_EQ(cf2.read("mds", "pid file", val), 0);
  ASSERT_EQ(val, "foo2");
  ASSERT_EQ(cf2.read("nonesuch", "keyring", val), -ENOENT);
}

TEST(ReadFiles2, ConfUtils) {
  std::string conf3_f(next_tempfile(conf3));
  ConfFile cf1(conf3_f.c_str());
  std::string val;
  ASSERT_EQ(cf1.parse(), 0);
  ASSERT_EQ(cf1.read("global", "log file", val), 0);
  ASSERT_EQ(val, "/quite/a/long/path/for/a/log/file");
  ASSERT_EQ(cf1.read("global", "pid file", val), 0);
  ASSERT_EQ(val, "spork");

  std::string unicode_config_1f(next_tempfile(unicode_config_1));
  ConfFile cf2(unicode_config_1f.c_str());
  ASSERT_EQ(cf2.parse(), 0);
  ASSERT_EQ(cf2.read("global", "log file", val), 0);
  ASSERT_EQ(val, "\x66\xd1\x86\xd1\x9d\xd3\xad\xd3\xae");
}

// FIXME: illegal configuration files don't return a parse error currently.
TEST(IllegalFiles, ConfUtils) {
  std::string illegal_conf1_f(next_tempfile(illegal_conf1));
  ConfFile cf1(illegal_conf1_f.c_str());
  std::string val;
  ASSERT_EQ(cf1.parse(), 0);

  bufferlist bl2;
  bl2.append(illegal_conf2, strlen(illegal_conf2));
  ConfFile cf2(&bl2);
  ASSERT_EQ(cf2.parse(), 0);

  std::string illegal_conf3_f(next_tempfile(illegal_conf3));
  ConfFile cf3(illegal_conf3_f.c_str());
  ASSERT_EQ(cf3.parse(), 0);
}
