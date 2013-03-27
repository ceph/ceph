// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
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

#include <stdio.h>
#include <signal.h>
#include "os/LFNIndex.h"
#include "os/chain_xattr.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

class TestIndex : public LFNIndex {
public:
  TestIndex(coll_t collection,
	    const char *base_path,
	    uint32_t index_version) : LFNIndex(collection, base_path, index_version) {}

  virtual uint32_t collection_version() {
    return index_version;
  }

  int cleanup() { return 0; }

  virtual int _split(
		     uint32_t match,                           
		     uint32_t bits,                            
		     std::tr1::shared_ptr<CollectionIndex> dest
		     ) { return 0; }

protected:
  virtual int _init() { return 0; }

  virtual int _created(
		       const vector<string> &path,
		       const hobject_t &hoid,     
		       const string &mangled_name 
		       ) { return 0; }

  virtual int _remove(
		      const vector<string> &path,
		      const hobject_t &hoid,    
		      const string &mangled_name
		      ) { return 0; }

  virtual int _lookup(
		      const hobject_t &hoid,
		      vector<string> *path,
		      string *mangled_name,
		      int *exists		 
		      ) { return 0; }

  virtual int _collection_list(
			       vector<hobject_t> *ls
			       ) { return 0; }

  virtual int _collection_list_partial(
				       const hobject_t &start,
				       int min_count,
				       int max_count,
				       snapid_t seq,
				       vector<hobject_t> *ls,
				       hobject_t *next
				       ) { return 0; }
};

class TestLFNIndex : public TestIndex, public ::testing::Test {
public:
  TestLFNIndex() : TestIndex(coll_t("ABC"), "PATH", 1) {
  }

  virtual void SetUp() {
    ::chmod("PATH", 0700);
    ::system("rm -fr PATH");
    ::mkdir("PATH", 0700);
  }

  virtual void TearDown() {
    ::system("rm -fr PATH");
  }
};

TEST_F(TestLFNIndex, get_mangled_name) {
  const vector<string> path;

  //
  // object name escape logic
  //
  {
    std::string mangled_name;
    int exists = 666;

    hobject_t hoid(sobject_t(".A/B_\\C.D", CEPH_NOSNAP));

    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_NE(std::string::npos, mangled_name.find("\\.A\\sB_\\\\C.D_head"));
    EXPECT_EQ(0, exists);
  }
  {
    std::string mangled_name;
    int exists = 666;

    hobject_t hoid(sobject_t("DIR_A", CEPH_NOSNAP));

    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_NE(std::string::npos, mangled_name.find("\\dA_head"));
    EXPECT_EQ(0, exists);
  }
  //
  // small object name
  //
  {
    std::string mangled_name;
    int exists = 666;
    hobject_t hoid(sobject_t("ABC", CEPH_NOSNAP));

    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_NE(std::string::npos, mangled_name.find("ABC_head"));
    EXPECT_EQ(std::string::npos, mangled_name.find("0_long"));
    EXPECT_EQ(0, exists);
    const std::string pathname("PATH/" + mangled_name);
    EXPECT_EQ(0, ::close(::creat(pathname.c_str(), 0600)));
    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_NE(std::string::npos, mangled_name.find("ABC_head"));
    EXPECT_EQ(1, exists);
    EXPECT_EQ(0, ::unlink(pathname.c_str()));
  }
  //
  // long object name
  //
  {
    std::string mangled_name;
    int exists;
    const std::string object_name(1024, 'A');
    hobject_t hoid(sobject_t(object_name, CEPH_NOSNAP));

    //
    // long version of the mangled name and no matching
    // file exists 
    //
    mangled_name.clear();
    exists = 666;
    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_NE(std::string::npos, mangled_name.find("0_long"));
    EXPECT_EQ(0, exists);

    const std::string pathname("PATH/" + mangled_name);

    //
    // if a file by the same name exists but does not have the
    // expected extended attribute, it is silently removed 
    //
    mangled_name.clear();
    exists = 666;
    EXPECT_EQ(0, ::close(::creat(pathname.c_str(), 0600)));
    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_NE(std::string::npos, mangled_name.find("0_long"));
    EXPECT_EQ(0, exists);
    EXPECT_EQ(-1, ::access(pathname.c_str(), 0));
    EXPECT_EQ(ENOENT, errno);

    //
    // if a file by the same name exists but does not have the
    // expected extended attribute, and cannot be removed, 
    // return on error
    //
    mangled_name.clear();
    exists = 666;
    EXPECT_EQ(0, ::close(::creat(pathname.c_str(), 0600)));
    EXPECT_EQ(0, ::chmod("PATH", 0500));
    EXPECT_EQ(-EACCES, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_EQ("", mangled_name);
    EXPECT_EQ(666, exists);
    EXPECT_EQ(0, ::chmod("PATH", 0700));
    EXPECT_EQ(0, ::unlink(pathname.c_str()));

    //
    // long version of the mangled name and a file
    // exists by that name and contains the long object name
    //
    mangled_name.clear();
    exists = 666;
    EXPECT_EQ(0, ::close(::creat(pathname.c_str(), 0600)));
    EXPECT_EQ(0, created(hoid, pathname.c_str()));
    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name, &exists));
    EXPECT_NE(std::string::npos, mangled_name.find("0_long"));
    EXPECT_EQ(1, exists);
    EXPECT_EQ(0, ::access(pathname.c_str(), 0));

    //
    // long version of the mangled name and a file exists by that name
    // and contains a long object name with the same prefix but they
    // are not identical and it so happens that their SHA1 is
    // identical : a collision number is used to differentiate them
    //
    const string LFN_ATTR = "user.cephos.lfn";
    const std::string object_name_same_prefix = object_name + "SUFFIX";
    EXPECT_EQ(object_name_same_prefix.size(), (unsigned)chain_setxattr(pathname.c_str(), LFN_ATTR.c_str(), object_name_same_prefix.c_str(), object_name_same_prefix.size()));
    std::string mangled_name_same_prefix;
    exists = 666;
    EXPECT_EQ(0, get_mangled_name(path, hoid, &mangled_name_same_prefix, &exists));
    EXPECT_NE(std::string::npos, mangled_name_same_prefix.find("1_long"));
    EXPECT_EQ(0, exists);
    
    EXPECT_EQ(0, ::unlink(pathname.c_str()));
  }
}

int main(int argc, char **argv) {
  int fd = ::creat("detect", 0600);
  int ret = chain_fsetxattr(fd, "user.test", "A", 1);
  ::close(fd);
  ::unlink("detect");
  if (ret < 0) {
    cerr << "SKIP LFNIndex because unable to test for xattr" << std::endl;
  } else {
    vector<const char*> args;
    argv_to_vec(argc, (const char **)argv, args);

    global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
    common_init_finish(g_ceph_context);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
  }
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_lfnindex ; ./unittest_lfnindex # --gtest_filter=LFNIndexTest.LFNIndex --log-to-stderr=true --debug-filestore=20"
// End:
