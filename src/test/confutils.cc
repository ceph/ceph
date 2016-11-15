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
#include "common/config.h"
#include "common/errno.h"
#include "gtest/gtest.h"
#include "include/buffer.h"

#include <errno.h>
#include <iostream>
#include <stdlib.h>
#include <sstream>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "include/memory.h"

using ceph::bufferlist;
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
  ceph::shared_ptr<FILE> fpp(fp, fclose);
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
    return err;
  }
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

const char * const trivial_conf_1 = "";

const char * const trivial_conf_2 = "log dir = foobar";

const char * const trivial_conf_3 = "log dir = barfoo\n";

const char * const trivial_conf_4 = "log dir = \"barbaz\"\n";

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

const char * const escaping_conf_1 = "\
[global]\n\
	log file = the \"scare quotes\"\n\
	pid file = a \\\n\
pid file\n\
[mon]\n\
	keyring = \"nested \\\"quotes\\\"\"\n\
";

const char * const escaping_conf_2 = "\
[apple \\]\\[]\n\
	log file = floppy disk\n\
[mon]\n\
	keyring = \"backslash\\\\\"\n\
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

// illegal because it has unterminated quotes
const char illegal_conf4[] = "\
[global]\n\
        keyring = \"unterminated quoted string\n\
[osd0]\n\
        keyring = osd_keyring          ; osd's keyring\n\
";

// illegal because it has a backslash at the very end
const char illegal_conf5[] = "\
[global]\n\
        keyring = something awful\\\\\n\
";

// unicode config file
const char unicode_config_1[] = "\
[global]\n\
        log file =           \x66\xd1\x86\xd1\x9d\xd3\xad\xd3\xae     \n\
        pid file =           foo-bar\n\
[osd0]\n\
";

const char override_config_1[] = "\
[global]\n\
        log file =           global_log\n\
[mds]\n\
        log file =           mds_log\n\
[osd]\n\
        log file =           osd_log\n\
[osd.0]\n\
        log file =           osd0_log\n\
";

const char dup_key_config_1[] = "\
[mds.a]\n\
        log_file = 1\n\
        log_file = 3\n\
";

TEST(ConfUtils, Whitespace) {
  std::string test0("");
  ConfFile::trim_whitespace(test0, false);
  ASSERT_EQ(test0, "");

  std::string test0a("");
  ConfFile::trim_whitespace(test0a, true);
  ASSERT_EQ(test0a, "");

  std::string test0b("          ");
  ConfFile::trim_whitespace(test0b, false);
  ASSERT_EQ(test0b, "");

  std::string test0c("          ");
  ConfFile::trim_whitespace(test0c, true);
  ASSERT_EQ(test0c, "");

  std::string test1(" abc             ");
  ConfFile::trim_whitespace(test1, false);
  ASSERT_EQ(test1, "abc");

  std::string test2(" abc        d     ");
  ConfFile::trim_whitespace(test2, true);
  ASSERT_EQ(test2, "abc d");

  std::string test3(" abc        d     ");
  ConfFile::trim_whitespace(test3, false);
  ASSERT_EQ(test3, "abc        d");

  std::string test4("abcd");
  ConfFile::trim_whitespace(test4, false);
  ASSERT_EQ(test4, "abcd");

  std::string test5("abcd");
  ConfFile::trim_whitespace(test5, true);
  ASSERT_EQ(test5, "abcd");
}

TEST(ConfUtils, ParseFiles0) {
  std::deque<std::string> err;
  std::string val;
  std::ostringstream warn;

  std::string trivial_conf_1_f(next_tempfile(trivial_conf_1));
  ConfFile cf1;
  ASSERT_EQ(cf1.parse_file(trivial_conf_1_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);

  std::string trivial_conf_2_f(next_tempfile(trivial_conf_2));
  ConfFile cf2;
  ASSERT_EQ(cf2.parse_file(trivial_conf_2_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 1U);

  bufferlist bl3;
  bl3.append(trivial_conf_3, strlen(trivial_conf_3));
  ConfFile cf3;
  ASSERT_EQ(cf3.parse_bufferlist(&bl3, &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);
  ASSERT_EQ(cf3.read("global", "log dir", val), 0);
  ASSERT_EQ(val, "barfoo");

  std::string trivial_conf_4_f(next_tempfile(trivial_conf_4));
  ConfFile cf4;
  ASSERT_EQ(cf4.parse_file(trivial_conf_4_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);
  ASSERT_EQ(cf4.read("global", "log dir", val), 0);
  ASSERT_EQ(val, "barbaz");
}

TEST(ConfUtils, ParseFiles1) {
  std::deque<std::string> err;
  std::ostringstream warn;
  std::string simple_conf_1_f(next_tempfile(simple_conf_1));
  ConfFile cf1;
  ASSERT_EQ(cf1.parse_file(simple_conf_1_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);

  std::string simple_conf_2_f(next_tempfile(simple_conf_1));
  ConfFile cf2;
  ASSERT_EQ(cf2.parse_file(simple_conf_2_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);

  bufferlist bl3;
  bl3.append(simple_conf_1, strlen(simple_conf_1));
  ConfFile cf3;
  ASSERT_EQ(cf3.parse_bufferlist(&bl3, &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);

  bufferlist bl4;
  bl4.append(simple_conf_2, strlen(simple_conf_2));
  ConfFile cf4;
  ASSERT_EQ(cf4.parse_bufferlist(&bl4, &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);
}

TEST(ConfUtils, ReadFiles1) {
  std::deque<std::string> err;
  std::ostringstream warn;
  std::string simple_conf_1_f(next_tempfile(simple_conf_1));
  ConfFile cf1;
  ASSERT_EQ(cf1.parse_file(simple_conf_1_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);

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
  ConfFile cf2;
  ASSERT_EQ(cf2.parse_bufferlist(&bl2, &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);
  ASSERT_EQ(cf2.read("osd0", "keyring", val), 0);
  ASSERT_EQ(val, "osd_keyring");

  ASSERT_EQ(cf2.read("mds", "pid file", val), 0);
  ASSERT_EQ(val, "foo2");
  ASSERT_EQ(cf2.read("nonesuch", "keyring", val), -ENOENT);
}

TEST(ConfUtils, ReadFiles2) {
  std::deque<std::string> err;
  std::ostringstream warn;
  std::string conf3_f(next_tempfile(conf3));
  ConfFile cf1;
  std::string val;
  ASSERT_EQ(cf1.parse_file(conf3_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);
  ASSERT_EQ(cf1.read("global", "log file", val), 0);
  ASSERT_EQ(val, "/quite/a/long/path/for/a/log/file");
  ASSERT_EQ(cf1.read("global", "pid file", val), 0);
  ASSERT_EQ(val, "spork");

  std::string unicode_config_1f(next_tempfile(unicode_config_1));
  ConfFile cf2;
  ASSERT_EQ(cf2.parse_file(unicode_config_1f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);
  ASSERT_EQ(cf2.read("global", "log file", val), 0);
  ASSERT_EQ(val, "\x66\xd1\x86\xd1\x9d\xd3\xad\xd3\xae");
}

TEST(ConfUtils, IllegalFiles) {
  std::deque<std::string> err;
  std::ostringstream warn;
  std::string illegal_conf1_f(next_tempfile(illegal_conf1));
  ConfFile cf1;
  ASSERT_EQ(cf1.parse_file(illegal_conf1_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 1U);

  bufferlist bl2;
  bl2.append(illegal_conf2, strlen(illegal_conf2));
  ConfFile cf2;
  ASSERT_EQ(cf2.parse_bufferlist(&bl2, &err, &warn), 0);
  ASSERT_EQ(err.size(), 1U);

  std::string illegal_conf3_f(next_tempfile(illegal_conf3));
  ConfFile cf3;
  ASSERT_EQ(cf3.parse_file(illegal_conf3_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 1U);

  std::string illegal_conf4_f(next_tempfile(illegal_conf4));
  ConfFile cf4;
  ASSERT_EQ(cf4.parse_file(illegal_conf4_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 1U);

  std::string illegal_conf5_f(next_tempfile(illegal_conf5));
  ConfFile cf5;
  ASSERT_EQ(cf5.parse_file(illegal_conf5_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 1U);
}

TEST(ConfUtils, EscapingFiles) {
  std::deque<std::string> err;
  std::ostringstream warn;
  std::string escaping_conf_1_f(next_tempfile(escaping_conf_1));
  ConfFile cf1;
  std::string val;
  ASSERT_EQ(cf1.parse_file(escaping_conf_1_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);

  ASSERT_EQ(cf1.read("global", "log file", val), 0);
  ASSERT_EQ(val, "the \"scare quotes\"");
  ASSERT_EQ(cf1.read("global", "pid file", val), 0);
  ASSERT_EQ(val, "a pid file");
  ASSERT_EQ(cf1.read("mon", "keyring", val), 0);
  ASSERT_EQ(val, "nested \"quotes\"");

  std::string escaping_conf_2_f(next_tempfile(escaping_conf_2));
  ConfFile cf2;
  ASSERT_EQ(cf2.parse_file(escaping_conf_2_f.c_str(), &err, &warn), 0);
  ASSERT_EQ(err.size(), 0U);

  ASSERT_EQ(cf2.read("apple ][", "log file", val), 0);
  ASSERT_EQ(val, "floppy disk");
  ASSERT_EQ(cf2.read("mon", "keyring", val), 0);
  ASSERT_EQ(val, "backslash\\");
}

TEST(ConfUtils, Overrides) {
  md_config_t conf;
  std::ostringstream warn;
  std::string override_conf_1_f(next_tempfile(override_config_1));

  conf.name.set(CEPH_ENTITY_TYPE_MON, "0");
  conf.parse_config_files(override_conf_1_f.c_str(), &warn, 0);
  ASSERT_EQ(conf.parse_errors.size(), 0U);
  ASSERT_EQ(conf.log_file, "global_log");

  conf.name.set(CEPH_ENTITY_TYPE_MDS, "a");
  conf.parse_config_files(override_conf_1_f.c_str(), &warn, 0);
  ASSERT_EQ(conf.parse_errors.size(), 0U);
  ASSERT_EQ(conf.log_file, "mds_log");

  conf.name.set(CEPH_ENTITY_TYPE_OSD, "0");
  conf.parse_config_files(override_conf_1_f.c_str(), &warn, 0);
  ASSERT_EQ(conf.parse_errors.size(), 0U);
  ASSERT_EQ(conf.log_file, "osd0_log");
}

TEST(ConfUtils, DupKey) {
  md_config_t conf;
  std::ostringstream warn;
  std::string dup_key_config_f(next_tempfile(dup_key_config_1));

  conf.name.set(CEPH_ENTITY_TYPE_MDS, "a");
  conf.parse_config_files(dup_key_config_f.c_str(), &warn, 0);
  ASSERT_EQ(conf.parse_errors.size(), 0U);
  ASSERT_EQ(conf.log_file, string("3"));
}


