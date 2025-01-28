// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "rgw_sal_posix.h"
#include <gtest/gtest.h>
#include <iostream>
#include <filesystem>
#include "common/common_init.h"
#include "common/errno.h"
#include "global/global_init.h"

using namespace rgw::sal;

const std::string ATTR1{"attr1"};
const std::string ATTR2{"attr2"};
const std::string ATTR3{"attr3"};
const std::string ATTR_OBJECT_TYPE{"POSIX-Object-Type"};

namespace sf = std::filesystem;
class Environment* env;
sf::path base_path{"posixtest"};
std::unique_ptr<Directory> root;
std::vector<const char*> args;

class Environment : public ::testing::Environment {
public:
  boost::intrusive_ptr<CephContext> cct;
  DoutPrefixProvider* dpp{nullptr};

  Environment() {}

  virtual ~Environment() {}

  void SetUp() override {
    sf::remove_all(base_path);
    sf::create_directory(base_path);

    args.push_back("--rgw_multipart_min_part_size=32");
    args.push_back("--debug-rgw=20");
    args.push_back("--debug-ms=1");

    /* Proceed with environment setup */
    cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                      CODE_ENVIRONMENT_UTILITY,
                      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

    dpp = nullptr;
    //dpp = new NoDoutPrefix(cct.get(), 1);

    root = std::make_unique<Directory>(base_path, nullptr, cct.get());
    ASSERT_EQ(root->open(dpp), 0);
  }

  void TearDown() override {
    sf::remove_all(base_path);
  }
};


static inline void add_attr(Attrs& attrs, const std::string& name, const std::string& value)
{
  bufferlist bl;
  encode(value, bl);

  attrs[name] = bl;
}

static inline bool get_attr(Attrs& attrs, const char* name, bufferlist& bl)
{
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return false;
  }

  bl = iter->second;
  return true;
}

template <typename F>
static bool decode_attr(Attrs &attrs, const char *name, F &f) {
  bufferlist bl;
  if (!get_attr(attrs, name, bl)) {
    return false;
  }
  F tmpf;
  try {
    auto bufit = bl.cbegin();
    decode(tmpf, bufit);
  } catch (buffer::error &err) {
    return false;
  }

  f = tmpf;
  return true;
}

class TestDirectory : public Directory {
public:
  TestDirectory(std::string _name, Directory* _parent, CephContext* _ctx) : Directory(_name, _parent, _ctx)
    {}
  TestDirectory(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : Directory(_name, _parent, _stx, _ctx)
    {}
  virtual ~TestDirectory() { close(); }

  bool get_stat_done() { return stat_done; }
};

class TestFile : public File {
public:
  TestFile(std::string _name, Directory* _parent, CephContext* _ctx) : File(_name, _parent, _ctx)
    {}
  TestFile(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : File(_name, _parent, _stx, _ctx)
    {}
  virtual ~TestFile() { close(); }

  bool get_stat_done() { return stat_done; }
};

std::string get_test_name()
{
  std::string suitename =
      testing::UnitTest::GetInstance()->current_test_info()->test_suite_name();
  std::string testname =
      testing::UnitTest::GetInstance()->current_test_info()->name();

  return suitename + testname;
}


// Directory

TEST(FSEnt, DirCreate)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<Directory> testdir = std::make_unique<Directory>(dirname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testdir->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

TEST(FSEnt, DirBase)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<TestDirectory> testdir = std::make_unique<TestDirectory>(dirname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testdir->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  EXPECT_EQ(testdir->get_fd(), -1);
  EXPECT_EQ(testdir->get_name(), dirname);
  EXPECT_EQ(testdir->get_parent(), root.get());
  EXPECT_FALSE(testdir->exists());
  EXPECT_EQ(testdir->get_type(), ObjectType::DIRECTORY);
  EXPECT_FALSE(testdir->get_stat_done());

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(testdir->get_fd(), 0);

  ret = testdir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(testdir->get_stat_done());
  EXPECT_TRUE(S_ISDIR(testdir->get_stx().stx_mode));

  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  add_attr(attrs, ATTR2, ATTR2);
  Attrs extra_attrs;
  add_attr(extra_attrs, ATTR3, ATTR3);

  ret = testdir->write_attrs(env->dpp, null_yield, attrs, &extra_attrs);
  EXPECT_EQ(ret, 0);

  attrs.clear();
  ret = testdir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  std::string val;
  bool success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  ObjectType type;
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, ObjectType::DIRECTORY);

  ret = testdir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(testdir->get_fd(), -1);

  bufferlist bl;
  ret = testdir->write(0, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, -EINVAL);

  ret = testdir->read(0, 50, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, -EINVAL);

  ret = testdir->link_temp_file(env->dpp, null_yield, dirname);
  EXPECT_EQ(ret, -EINVAL);

  std::string copyname{dirname + "-copy"};
  sf::path cp{base_path / copyname};
  sf::remove_all(cp);
  EXPECT_FALSE(sf::exists(cp));
  ret = testdir->copy(env->dpp, null_yield, root.get(), copyname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(cp));
  EXPECT_TRUE(sf::is_directory(tp));

  std::unique_ptr<TestDirectory> copydir = std::make_unique<TestDirectory>(copyname, root.get(), env->cct.get());
  ret = copydir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(copydir->get_fd(), 0);

  ret = copydir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(copydir->get_stat_done());
  EXPECT_TRUE(S_ISDIR(copydir->get_stx().stx_mode));

  attrs.clear();
  ret = copydir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);

  ret = copydir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(copydir->get_fd(), -1);

  std::unique_ptr<FSEnt> ent;
  ret = root->get_ent(env->dpp, null_yield, dirname, std::string(), ent);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ent->get_type(), ObjectType::DIRECTORY);

  ret = testdir->remove(env->dpp, null_yield, false);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(sf::exists(tp));
}

TEST(FSEnt, DirAddDir)
{
  bool existed;
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<Directory> testdir = std::make_unique<Directory>(dirname, root.get(), env->cct.get());
  int ret = testdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  std::string subdirname{"SubDir"};
  sf::path sp{base_path / dirname / subdirname};
  std::unique_ptr<Directory> subdir = std::make_unique<Directory>(subdirname, testdir.get(), env->cct.get());
  ret = subdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::is_directory(sp));

  ret = subdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  std::string subsubdirname{"SubSubDir"};
  sf::path ssp{base_path / dirname / subdirname / subsubdirname };
  std::unique_ptr<Directory> subsubdir = std::make_unique<Directory>(subsubdirname, subdir.get(), env->cct.get());
  ret = subsubdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(ssp));
  EXPECT_TRUE(sf::is_directory(ssp));
}

TEST(FSEnt, DirRename)
{
  bool existed;
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<Directory> testdir = std::make_unique<Directory>(dirname, root.get(), env->cct.get());
  int ret = testdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  std::string subdirname{"SubDir"};
  sf::path sp{base_path / dirname / subdirname};
  std::unique_ptr<Directory> subdir = std::make_unique<Directory>(subdirname, testdir.get(), env->cct.get());
  ret = subdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::is_directory(sp));

  ret = subdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  std::string newsubdirname{"SubDir2"};
  sf::path nsp{base_path / dirname / newsubdirname};
  ret = subdir->rename(env->dpp, null_yield, testdir.get(), newsubdirname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(nsp));
  EXPECT_TRUE(sf::is_directory(nsp));
  EXPECT_FALSE(sf::exists(sp));
  EXPECT_FALSE(sf::is_directory(sp));
}


// File

TEST(FSEnt, FileCreateReal)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  TestFile testfile{fname, root.get(), env->cct.get()};

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testfile.create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
}

TEST(FSEnt, FileCreateTemp)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  TestFile testfile{fname, root.get(), env->cct.get()};

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testfile.create(env->dpp, &existed, true);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_FALSE(sf::exists(tp));

  std::string temp_fname{fname + "-blargh"};
  ret = testfile.link_temp_file(env->dpp, null_yield, temp_fname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
}

TEST(FSEnt, FileBase)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  std::unique_ptr<TestFile> testfile = std::make_unique<TestFile>(fname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));
  EXPECT_EQ(testfile->get_fd(), -1);

  bool existed;
  int ret = testfile->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
  // create() opens
  EXPECT_GT(testfile->get_fd(), 0);

  EXPECT_EQ(testfile->get_name(), fname);
  EXPECT_EQ(testfile->get_parent(), root.get());
  EXPECT_FALSE(testfile->exists());
  EXPECT_EQ(testfile->get_type(), ObjectType::FILE);
  EXPECT_FALSE(testfile->get_stat_done());

  ret = testfile->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(testfile->get_fd(), 0);

  ret = testfile->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(testfile->get_stat_done());
  EXPECT_TRUE(S_ISREG(testfile->get_stx().stx_mode));

  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  add_attr(attrs, ATTR2, ATTR2);
  Attrs extra_attrs;
  add_attr(extra_attrs, ATTR3, ATTR3);

  ret = testfile->write_attrs(env->dpp, null_yield, attrs, &extra_attrs);
  EXPECT_EQ(ret, 0);

  attrs.clear();
  ret = testfile->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  std::string val;
  bool success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  ObjectType type;
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, ObjectType::FILE);

  ret = testfile->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(testfile->get_fd(), -1);

  std::string copyname{fname + "-copy"};
  sf::path cp{base_path / copyname};
  EXPECT_FALSE(sf::exists(cp));
  ret = testfile->copy(env->dpp, null_yield, root.get(), copyname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(cp));
  EXPECT_TRUE(sf::is_regular_file(tp));

  std::unique_ptr<TestFile> copyfile = std::make_unique<TestFile>(copyname, root.get(), env->cct.get());
  ret = copyfile->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(copyfile->get_fd(), 0);

  ret = copyfile->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(copyfile->get_stat_done());
  EXPECT_TRUE(S_ISREG(copyfile->get_stx().stx_mode));

  attrs.clear();
  ret = copyfile->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 0);

  ret = copyfile->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(copyfile->get_fd(), -1);

  std::unique_ptr<FSEnt> ent;
  ret = root->get_ent(env->dpp, null_yield, fname, std::string(), ent);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ent->get_type(), ObjectType::FILE);

  ret = testfile->remove(env->dpp, null_yield, false);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(sf::exists(tp));
}

TEST(FSEnt, FileReadWrite)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  std::unique_ptr<File> testfile{std::make_unique<File>(fname, root.get(), env->cct.get())};

  int ret = testfile->create(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));

  bufferlist bl;
  encode(fname, bl);
  int len = bl.length();
  ret = testfile->write(0, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(sf::file_size(tp), len);

  bl.clear();
  ret = testfile->read(0, 50, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, len);

  std::string result;
  EXPECT_NO_THROW({
    auto bufit = bl.cbegin();
    decode(result, bufit);
  });

  EXPECT_EQ(result, fname);
}

TEST(FSEnt, SymlinkBase)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  std::string target{"symlinktarget"};
  std::unique_ptr<Symlink> testlink = std::make_unique<Symlink>(fname, root.get(), target, env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testlink->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::is_symlink(tp));
  EXPECT_EQ(sf::read_symlink(tp), target);

  EXPECT_EQ(testlink->get_name(), fname);
  EXPECT_EQ(testlink->get_parent(), root.get());
  EXPECT_FALSE(testlink->exists());
  EXPECT_EQ(testlink->get_type(), ObjectType::SYMLINK);

  std::unique_ptr<FSEnt> ent;
  ret = root->get_ent(env->dpp, null_yield, fname, std::string(), ent);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ent->get_type(), ObjectType::SYMLINK);


  ret = testlink->remove(env->dpp, null_yield, false);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(sf::exists(tp));
}

TEST(FSEnt, MPDirBase)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<MPDirectory> testdir = std::make_unique<MPDirectory>(dirname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testdir->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  EXPECT_EQ(testdir->get_fd(), -1);
  EXPECT_EQ(testdir->get_name(), dirname);
  EXPECT_EQ(testdir->get_parent(), root.get());
  EXPECT_FALSE(testdir->exists());
  EXPECT_EQ(testdir->get_type(), ObjectType::MULTIPART);

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(testdir->get_fd(), 0);

  ret = testdir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(S_ISDIR(testdir->get_stx().stx_mode));

  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  add_attr(attrs, ATTR2, ATTR2);
  Attrs extra_attrs;
  add_attr(extra_attrs, ATTR3, ATTR3);

  ret = testdir->write_attrs(env->dpp, null_yield, attrs, &extra_attrs);
  EXPECT_EQ(ret, 0);

  attrs.clear();
  ret = testdir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  std::string val;
  bool success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  ObjectType type;
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, ObjectType::MULTIPART);

  ret = testdir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(testdir->get_fd(), -1);

  bufferlist bl;
  ret = testdir->write(0, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, -EINVAL);

  ret = testdir->read(0, 50, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, 0);

  ret = testdir->link_temp_file(env->dpp, null_yield, dirname);
  EXPECT_EQ(ret, 0);

  std::string copyname{dirname + "-copy"};
  sf::path cp{base_path / copyname};
  sf::remove_all(cp);
  EXPECT_FALSE(sf::exists(cp));
  ret = testdir->copy(env->dpp, null_yield, root.get(), copyname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(cp));
  EXPECT_TRUE(sf::is_directory(tp));

  std::unique_ptr<MPDirectory> copydir = std::make_unique<MPDirectory>(copyname, root.get(), env->cct.get());
  ret = copydir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(copydir->get_fd(), 0);

  ret = copydir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(S_ISDIR(copydir->get_stx().stx_mode));

  attrs.clear();
  ret = copydir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);

  ret = copydir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(copydir->get_fd(), -1);

  std::unique_ptr<FSEnt> ent;
  ret = root->get_ent(env->dpp, null_yield, dirname, std::string(), ent);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ent->get_type(), ObjectType::MULTIPART);

  ret = testdir->remove(env->dpp, null_yield, false);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(sf::exists(tp));
}

TEST(FSEnt, MPDirTemp)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<MPDirectory> testdir = std::make_unique<MPDirectory>(dirname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testdir->create(env->dpp, &existed, true);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_FALSE(sf::exists(tp));

  std::string temp_fname; // unused
  ret = testdir->link_temp_file(env->dpp, null_yield, temp_fname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  EXPECT_EQ(testdir->get_name(), dirname);
  EXPECT_EQ(testdir->get_parent(), root.get());
  EXPECT_EQ(testdir->get_type(), ObjectType::MULTIPART);
}

TEST(FSEnt, MPDirReadWrite)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<MPDirectory> testdir = std::make_unique<MPDirectory>(dirname, root.get(), env->cct.get());
  int ret = testdir->create(env->dpp, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  bufferlist write_bl;
  int total_len{0};
  for (int part_num = 0; part_num < 4; ++part_num) {
    std::unique_ptr<File> testfile = testdir->get_part_file(part_num);
    sf::path pp{tp / testfile->get_name()};

    ret = testfile->create(env->dpp, /*existed=*/nullptr, /*temp_file=*/false);
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(sf::exists(pp));
    EXPECT_TRUE(sf::is_regular_file(pp));

    bufferlist bl;
    encode(dirname, bl);
    int len = bl.length();
    total_len += len;
    ret = testfile->write(0, bl, env->dpp, null_yield);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(sf::file_size(pp), len);
    write_bl.claim_append(bl);
  }

  ret = testdir->stat(env->dpp, true);
  EXPECT_EQ(ret, 0);

  bufferlist total_bl;
  std::string read_data;
  int left = total_len;
  int ofs{0};
  while (left > 0) {
    bufferlist bl;
    ret = testdir->read(ofs, left, bl, env->dpp, null_yield);
    EXPECT_GE(ret, 0);
    if (ret == 0)
      break;

    std::string result;
    EXPECT_NO_THROW({
      auto bufit = bl.cbegin();
      decode(result, bufit);
    });
    read_data += result;
    total_bl.claim_append(bl);
    left -= ret;
    ofs += ret;
  }

  EXPECT_EQ(total_bl.length(), total_len);
  EXPECT_EQ(total_bl, write_bl);

  EXPECT_EQ(read_data, dirname + dirname + dirname + dirname);
}

TEST(FSEnt, VerDirBase)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<VersionedDirectory> testdir = std::make_unique<VersionedDirectory>(dirname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed;
  int ret = testdir->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  /* Create opens */
  EXPECT_NE(testdir->get_fd(), -1);
  EXPECT_EQ(testdir->get_name(), dirname);
  EXPECT_EQ(testdir->get_parent(), root.get());
  EXPECT_FALSE(testdir->exists());
  EXPECT_EQ(testdir->get_type(), ObjectType::VERSIONED);

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(testdir->get_fd(), 0);

  ret = testdir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(S_ISDIR(testdir->get_stx().stx_mode));

  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  add_attr(attrs, ATTR2, ATTR2);
  Attrs extra_attrs;
  add_attr(extra_attrs, ATTR3, ATTR3);

  ret = testdir->write_attrs(env->dpp, null_yield, attrs, &extra_attrs);
  EXPECT_EQ(ret, 0);

  attrs.clear();
  ret = testdir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  std::string val;
  bool success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  ObjectType type;
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, ObjectType::VERSIONED);

  ret = testdir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(testdir->get_fd(), -1);

  bufferlist bl;
  ret = testdir->write(0, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, 0);

  ret = testdir->read(0, 50, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, 0);

  ret = testdir->link_temp_file(env->dpp, null_yield, dirname);
  EXPECT_EQ(ret, -EINVAL);

  std::string copyname{dirname + "-copy"};
  sf::path cp{base_path / copyname};
  sf::remove_all(cp);
  EXPECT_FALSE(sf::exists(cp));
  ret = testdir->copy(env->dpp, null_yield, root.get(), copyname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(cp));
  EXPECT_TRUE(sf::is_directory(tp));

  std::unique_ptr<VersionedDirectory> copydir = std::make_unique<VersionedDirectory>(copyname, root.get(), env->cct.get());
  ret = copydir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(copydir->get_fd(), 0);

  ret = copydir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(S_ISDIR(copydir->get_stx().stx_mode));

  attrs.clear();
  ret = copydir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);

  ret = copydir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(copydir->get_fd(), -1);

  std::unique_ptr<FSEnt> ent;
  ret = root->get_ent(env->dpp, null_yield, dirname, std::string(), ent);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ent->get_type(), ObjectType::VERSIONED);

  ret = testdir->remove(env->dpp, null_yield, false);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(sf::exists(tp));
}

TEST(FSEnt, VerDirReadWrite)
{
  std::string fname = get_test_name();
  std::unique_ptr<VersionedDirectory> verdir{
      std::make_unique<VersionedDirectory>(fname, root.get(), env->cct.get())};
  std::string instance_id{verdir->get_new_instance()};
  std::string vfname{"_%3A" + instance_id + "_" + fname};
  sf::path tp{base_path / fname};
  sf::path fp{tp / vfname};
  sf::path lp{tp / fname};

  int ret = verdir->create(env->dpp, /*existed=*/nullptr, /*temp_file=*/false);
  EXPECT_EQ(ret, 0);

  std::unique_ptr<File> testfile{std::make_unique<File>(vfname, verdir.get(), env->cct.get())};
  ret = verdir->add_file(env->dpp, std::move(testfile), /*existed=*/nullptr, /*temp_file=*/true);
  EXPECT_EQ(ret, 0);

  std::string temp_fname{fname + "-blargh"};
  ret = verdir->link_temp_file(env->dpp, null_yield, temp_fname);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
  EXPECT_TRUE(sf::exists(fp));
  EXPECT_TRUE(sf::is_regular_file(fp));
  EXPECT_TRUE(sf::exists(lp));
  EXPECT_TRUE(sf::is_symlink(lp));
  EXPECT_EQ(sf::read_symlink(lp), vfname);

  bufferlist bl;
  encode(fname, bl);
  int len = bl.length();
  ret = verdir->write(0, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(sf::file_size(fp), len);

  bl.clear();
  ret = verdir->read(0, 50, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, len);

  std::string result;
  EXPECT_NO_THROW({
    auto bufit = bl.cbegin();
    decode(result, bufit);
  });

  EXPECT_EQ(result, fname);

  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  add_attr(attrs, ATTR2, ATTR2);
  Attrs extra_attrs;
  add_attr(extra_attrs, ATTR3, ATTR3);

  ret = verdir->write_attrs(env->dpp, null_yield, attrs, &extra_attrs);
  EXPECT_EQ(ret, 0);

  attrs.clear();
  ret = verdir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  std::string val;
  bool success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  ObjectType type;
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, ObjectType::VERSIONED);

  FSEnt* ent = verdir->get_cur_version_ent();
  attrs.clear();
  ret = ent->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, ObjectType::FILE);
}

TEST(FSEnt, MPVerDirReadWrite)
{
  std::string testname = get_test_name();
  std::unique_ptr<VersionedDirectory> verdir{
      std::make_unique<VersionedDirectory>(testname, root.get(), env->cct.get())};
  std::string instance_id{verdir->get_new_instance()};
  std::string vfname{"_%3A" + instance_id + "_" + testname};
  sf::path vp{base_path / testname};
  sf::path mp{vp / vfname};
  sf::path lp{vp / testname};

  int ret = verdir->create(env->dpp, /*existed=*/nullptr, /*temp_file=*/false);
  EXPECT_EQ(ret, 0);

  std::unique_ptr<MPDirectory> mpdir{std::make_unique<MPDirectory>(vfname, verdir.get(), env->cct.get())};
  ret = verdir->add_file(env->dpp, std::move(mpdir), /*existed=*/nullptr, /*temp_file=*/true);
  EXPECT_EQ(ret, 0);

  std::string temp_fname{testname + "-blargh"};
  ret = verdir->link_temp_file(env->dpp, null_yield, temp_fname);
  EXPECT_TRUE(sf::exists(vp));
  EXPECT_TRUE(sf::is_directory(vp));
  EXPECT_TRUE(sf::exists(mp));
  EXPECT_TRUE(sf::is_directory(mp));
  EXPECT_TRUE(sf::exists(lp));
  EXPECT_TRUE(sf::is_symlink(lp));
  EXPECT_EQ(sf::read_symlink(lp), vfname);

  ret = verdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  MPDirectory* mpp = static_cast<MPDirectory*>(verdir->get_cur_version_ent());
  EXPECT_NE(mpp, nullptr);

  ret = mpp->open(env->dpp);
  EXPECT_EQ(ret, 0);

  bufferlist write_bl;
  int total_len{0};
  for (int part_num = 0; part_num < 4; ++part_num) {
    std::unique_ptr<File> testfile = mpp->get_part_file(part_num);
    sf::path pp{mp / testfile->get_name()};

    ret = testfile->create(env->dpp, /*existed=*/nullptr, /*temp_file=*/false);
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(sf::exists(pp));
    EXPECT_TRUE(sf::is_regular_file(pp));

    bufferlist bl;
    encode(testname, bl);
    int len = bl.length();
    total_len += len;
    ret = testfile->write(0, bl, env->dpp, null_yield);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(sf::file_size(pp), len);
    write_bl.claim_append(bl);
  }

  ret = verdir->stat(env->dpp, true);
  EXPECT_EQ(ret, 0);

  bufferlist total_bl;
  std::string read_data;
  int left = total_len;
  int ofs{0};
  while (left > 0) {
    bufferlist bl;
    ret = verdir->read(ofs, left, bl, env->dpp, null_yield);
    EXPECT_GE(ret, 0);
    if (ret == 0)
      break;

    std::string result;
    EXPECT_NO_THROW({
      auto bufit = bl.cbegin();
      decode(result, bufit);
    });
    read_data += result;
    total_bl.claim_append(bl);
    left -= ret;
    ofs += ret;
  }

  EXPECT_EQ(total_bl.length(), total_len);
  EXPECT_EQ(total_bl, write_bl);

  EXPECT_EQ(read_data, testname + testname + testname + testname);
}

class TestUser;
class TestDriver : public POSIXDriver
{
public:
  std::string driver_base;

  TestDriver(std::string _base_path) : POSIXDriver(nullptr), driver_base(_base_path)
  { }
  virtual ~TestDriver() = default;

  int init(const DoutPrefixProvider* dpp)
  {
    std::string cache_base = driver_base + "/cache";
    base_path = driver_base + "/root";

    root_dir = std::make_unique<Directory>(base_path, nullptr, env->cct.get());
    int ret = root_dir->open(env->dpp);
    if (ret < 0) {
      if (ret == -ENOTDIR) {
        ldpp_dout(env->dpp, 0) << " ERROR: base path (" << base_path
                          << "): was not a directory." << dendl;
        return ret;
      } else if (ret == -ENOENT) {
        ret = root_dir->create(env->dpp);
        if (ret < 0) {
          ldpp_dout(env->dpp, 0)
              << " ERROR: could not create base path (" << base_path
              << "): " << cpp_strerror(-ret) << dendl;
          return ret;
        }
      }
    }

    /* ordered listing cache */
    bucket_cache.reset(new BucketCache(
        this, base_path, cache_base, 100, 3, 3, 3));

    ldpp_dout(env->dpp, 20) << "SUCCESS" << dendl;
    return 0;
  }
  virtual CephContext* ctx(void) override {
    return get_pointer(env->cct);
  }

  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
};

class TestUser : public StoreUser {
  Attrs attrs;

public:
  TestUser(TestDriver *_dr, const rgw_user& _u) : StoreUser(_u) { }
  TestUser(TestDriver *_dr, const RGWUserInfo& _i) : StoreUser(_i) { }
  TestUser(TestDriver *_dr)  { }
  TestUser(TestUser& _o) = default;
  virtual ~TestUser() = default;

  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new TestUser(*this));
  }
  virtual Attrs& get_attrs() override { return attrs; }
  virtual void set_attrs(Attrs &_attrs) override { attrs = _attrs; }
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs&
				    new_attrs, optional_yield y) override { return 0; }
  virtual int read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
             uint64_t end_epoch, uint32_t max_entries, bool* is_truncated,
             RGWUsageIter &usage_iter,
             std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) override { return 0; }
  virtual int trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch, optional_yield y) override { return 0; }
  virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool
			 exclusive, RGWUserInfo* old_info = nullptr) override { return 0; }
  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  virtual int verify_mfa(const std::string &mfa_str, bool *verified,
                         const DoutPrefixProvider* dpp,
                         optional_yield y) override { return 0; }
  virtual int list_groups(const DoutPrefixProvider *dpp, optional_yield y,
                          std::string_view marker, uint32_t max_items,
                          GroupList &listing) override { return -ENOTSUP; }
};

std::unique_ptr<User> TestDriver::get_user(const rgw_user &u)
{
  return std::make_unique<TestUser>(this, u);
}

TEST(POSIXDriver, CreateDriver)
{
  std::string name = get_test_name();
  sf::path bp{sf::absolute(sf::path{base_path / name})};
  sf::create_directory(bp);
  sf::create_directory(bp / "cache");
  sf::create_directory(bp / "root");
  TestDriver driver{bp};

  sf::path tp{bp / "root"};

  int ret = driver.init(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

class POSIXDriverTest : public ::testing::Test {
  protected:
    std::unique_ptr<TestDriver> driver;
    rgw_owner owner;
    ACLOwner acl_owner;
    sf::path bp;
    std::string testname;

  public:
    POSIXDriverTest() {}

    void SetUp() {
      testname = get_test_name();
      bp = sf::path{sf::absolute(sf::path{base_path / testname})};
      sf::create_directory(bp);
      sf::create_directory(bp / "cache");
      sf::create_directory(bp / "root");
      driver = std::make_unique<TestDriver>(bp);
      int ret = driver->init(env->dpp);
      EXPECT_EQ(ret, 0);

      rgw_user uid{"tenant", testname};
      owner = uid;
      acl_owner.id = owner;
      EXPECT_EQ(ret, 0);
    }

    void TearDown() { sf::remove_all(bp); }
};

TEST_F(POSIXDriverTest, Bucket)
{
  RGWBucketInfo info;
  info.bucket.name = testname;
  info.owner = owner;
  info.creation_time = ceph::real_clock::now();

  std::unique_ptr<rgw::sal::Bucket> bucket = driver->get_bucket(info);
  EXPECT_NE(bucket.get(), nullptr);
  EXPECT_EQ(bucket->get_name(), testname);
  EXPECT_EQ(bucket->get_key().name, testname);
  EXPECT_EQ(bucket->get_key().tenant, "");
  EXPECT_EQ(bucket->get_key().bucket_id, "");
  EXPECT_FALSE(bucket->versioned());
  EXPECT_FALSE(bucket->versioning_enabled());

}

TEST_F(POSIXDriverTest, BucketCreate)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  bool bucket_exists;
  rgw::sal::Bucket::CreateParams createparams;

  RGWBucketInfo info;
  info.bucket.name = testname;
  info.owner = owner;
  info.creation_time = ceph::real_clock::now();
  bucket = driver->get_bucket(info);
  EXPECT_NE(bucket.get(), nullptr);

  createparams.owner = owner;

  int ret = bucket->create(env->dpp, createparams, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(bucket->get_name(), testname);
  EXPECT_EQ(bucket->get_key().name, testname);
  EXPECT_EQ(bucket->get_key().tenant, "");
  EXPECT_EQ(bucket->get_key().bucket_id, "");
  EXPECT_FALSE(bucket_exists);

  sf::path tp{bp / "root" / testname};
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

class POSIXBucketTest : public POSIXDriverTest {
protected:
  std::unique_ptr<rgw::sal::Bucket> bucket;

public:
  POSIXBucketTest() {}

  void SetUp() {
    POSIXDriverTest::SetUp();

    RGWBucketInfo info;
    info.bucket.name = testname;
    info.owner = owner;
    info.creation_time = ceph::real_clock::now();

    bucket = driver->get_bucket(info);
    EXPECT_NE(bucket.get(), nullptr);

    rgw::sal::Bucket::CreateParams createparams;
    createparams.owner = owner;
    int ret = bucket->create(env->dpp, createparams, null_yield);
    EXPECT_EQ(ret, 0);
  }

  void TearDown() {
    POSIXDriverTest::TearDown();
  }
};

TEST_F(POSIXBucketTest, Object)
{
  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(rgw_obj_key(testname, "instance", "namespace"));
  EXPECT_NE(object.get(), nullptr);
  EXPECT_EQ(object->get_name(), testname);
  EXPECT_EQ(object->get_key().name, testname);
  EXPECT_EQ(object->get_bucket(), bucket.get());
  EXPECT_EQ(object->get_oid(), "_namespace:instance_" + testname);
  EXPECT_EQ(object->get_instance(), "instance");

}

TEST_F(POSIXBucketTest, ObjectWrite)
{
  sf::path tp{bp / "root" / testname / testname};
  EXPECT_FALSE(sf::exists(tp));

  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(rgw_obj_key(testname));
  EXPECT_NE(object.get(), nullptr);

  std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
      env->dpp, null_yield, object.get(), acl_owner, nullptr, 0, testname);
  EXPECT_NE(writer.get(), nullptr);

  int ret = writer->prepare(null_yield);
  EXPECT_EQ(ret, 0);

  int ofs{0};
  std::string etag;
  for (int i = 0; i < 4; ++i) {
    bufferlist bl;
    encode(testname, bl);
    int len = bl.length();

    ret = writer->process(std::move(bl), ofs);
    EXPECT_EQ(ret, 0);

    ofs += len;
  }

  ret = writer->process({}, ofs);
  EXPECT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs attrs;
  bufferlist bl;
  encode(ATTR1, bl);
  attrs[ATTR1] = bl;
  req_context rctx{env->dpp, null_yield, nullptr};

  ret = writer->complete(ofs, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(object->get_size(), ofs);

  bufferlist getbl = object->get_attrs()[ATTR1];
  EXPECT_EQ(bl, getbl);

  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
}

class POSIXObjectTest : public POSIXBucketTest {
protected:
  std::unique_ptr<rgw::sal::Object> object;
  uint64_t write_size{0};
  bufferlist write_data;

public:
  POSIXObjectTest() {}

  void SetUp() {
    POSIXBucketTest::SetUp();
    object = write_object(testname);
  }

  std::unique_ptr<rgw::sal::Object> write_object(std::string objname) {
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(objname));
    EXPECT_NE(obj.get(), nullptr);

    std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
    EXPECT_NE(writer.get(), nullptr);

    int ret = writer->prepare(null_yield);
    EXPECT_EQ(ret, 0);

    std::string etag;
    for (int i = 0; i < 4; ++i) {
      bufferlist bl;
      encode(objname, bl);
      int len = bl.length();

      write_data.append(bl);

      ret = writer->process(std::move(bl), write_size);
      EXPECT_EQ(ret, 0);

      write_size += len;
    }

    ret = writer->process({}, write_size);
    EXPECT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    add_attr(attrs, ATTR1, ATTR1);
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(write_size, etag, &mtime, real_time(), attrs,
                           std::nullopt, real_time(), nullptr, nullptr, nullptr,
                           nullptr, nullptr, rctx, 0);
    EXPECT_EQ(ret, 0);

    return obj;
  }

  void TearDown() { POSIXBucketTest::TearDown(); }
};

class Read_CB : public RGWGetDataCB
{
public:
  bufferlist *save_bl;
  explicit Read_CB(bufferlist *_bl) : save_bl(_bl) {}
  ~Read_CB() override {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    save_bl->append(bl);
    return 0;
  }
};

TEST_F(POSIXObjectTest, BucketList)
{
  std::unique_ptr<rgw::sal::Object> obj1 = write_object(testname + "-1");
  EXPECT_NE(obj1.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj2 = write_object(testname + "-2");
  EXPECT_NE(obj2.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj3 = write_object(testname + "-3");
  EXPECT_NE(obj3.get(), nullptr);

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(results.is_truncated, false);

  EXPECT_EQ(results.objs.size(), 4);

  rgw_obj_key key(results.objs[0].key);
  EXPECT_EQ(key, object->get_key());
  rgw_obj_key key1(results.objs[1].key);
  EXPECT_EQ(key1, obj1->get_key());
  rgw_obj_key key2(results.objs[2].key);
  EXPECT_EQ(key2, obj2->get_key());
  rgw_obj_key key3(results.objs[3].key);
  EXPECT_EQ(key3, obj3->get_key());
}

TEST_F(POSIXObjectTest, BucketListV)
{
  std::unique_ptr<rgw::sal::Object> obj1 = write_object(testname + "-1");
  EXPECT_NE(obj1.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj2 = write_object(testname + "-2");
  EXPECT_NE(obj2.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj3 = write_object(testname + "-3");
  EXPECT_NE(obj3.get(), nullptr);

  rgw::sal::Bucket::ListParams params;
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results;

  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(results.is_truncated, false);

  EXPECT_EQ(results.objs.size(), 4);

  rgw_obj_key key(results.objs[0].key);
  EXPECT_EQ(key, object->get_key());
  rgw_obj_key key1(results.objs[1].key);
  EXPECT_EQ(key1, obj1->get_key());
  rgw_obj_key key2(results.objs[2].key);
  EXPECT_EQ(key2, obj2->get_key());
  rgw_obj_key key3(results.objs[3].key);
  EXPECT_EQ(key3, obj3->get_key());
}

TEST_F(POSIXObjectTest, ObjectRead)
{
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(object->get_read_op());

  int ret = read_op->prepare(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(object->get_size(), write_size);

  bufferlist bl;
  Read_CB cb(&bl);
  ret = read_op->iterate(env->dpp, 0, write_size, &cb, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(write_data, bl);
}

TEST_F(POSIXObjectTest, ObjectDelete)
{
  sf::path tp{bp / "root" / testname / testname};
  EXPECT_TRUE(sf::exists(tp));

  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = object->get_delete_op();
  int ret = del_op->delete_obj(env->dpp, null_yield, 0);
  EXPECT_EQ(ret, 0);

  EXPECT_FALSE(sf::exists(tp));
}

TEST_F(POSIXObjectTest, ObjectCopy)
{
  sf::path sp{bp / "root" / testname / testname};
  EXPECT_TRUE(sf::exists(sp));

  std::string dstname{testname + "-dst"};
  sf::path dp{bp / "root" / testname / dstname};

  std::unique_ptr<rgw::sal::Object> dstobj = bucket->get_object(rgw_obj_key(dstname));
  EXPECT_NE(dstobj.get(), nullptr);
  EXPECT_FALSE(sf::exists(dp));

  RGWEnv rgw_env;
  req_info info(env->cct.get(), &rgw_env);
  rgw_zone_id zone;
  rgw_placement_rule placement;
  ceph::real_time mtime;
  Attrs attrs;
  std::string tag;

  int ret = object->copy_object(acl_owner,
	   std::get<rgw_user>(owner),
	   &info,
	   zone,
	   dstobj.get(),
	   bucket.get(),
	   bucket.get(),
	   placement,
	   &mtime,
	   &mtime,
	   &mtime,
	   &mtime,
	   false,
	   nullptr,
	   nullptr,
	   ATTRSMOD_NONE,
	   false,
	   attrs,
	   RGWObjCategory::Main,
	   0,
	   boost::none,
	   nullptr,
	   &tag, /* use req_id as tag */
	   &tag,
	   nullptr,
	   nullptr,
	   env->dpp,
	   null_yield);
  EXPECT_EQ(ret, 0);

  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::exists(dp));
}

TEST_F(POSIXObjectTest, ObjectAttrs)
{
  int ret = object->get_obj_attrs(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  bufferlist origbl;
  encode(ATTR1, origbl);

  // POSIXDriver adds attributes ("POSIX-Owner", and "POSIX-Object-Type")
  EXPECT_EQ(object->get_attrs().size(), 3);
  EXPECT_EQ(object->get_attrs()[ATTR1], origbl);
  EXPECT_TRUE(object->get_attrs().contains("POSIX-Owner"));
  EXPECT_TRUE(object->get_attrs().contains(ATTR_OBJECT_TYPE));

  std::string addattr{"AddAttrO"};
  bufferlist addbl;
  encode(addattr, addbl);

  ret = object->modify_obj_attrs(addattr.c_str(), addbl, null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(object->get_attrs().size(), 4);
  EXPECT_EQ(object->get_attrs()[ATTR1], origbl);
  EXPECT_EQ(object->get_attrs()[addattr], addbl);
  EXPECT_TRUE(object->get_attrs().contains("POSIX-Owner"));
  EXPECT_TRUE(object->get_attrs().contains(ATTR_OBJECT_TYPE));

  ret = object->delete_obj_attrs(env->dpp, ATTR1.c_str(), null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(object->get_attrs().size(), 3);
  EXPECT_EQ(object->get_attrs()[addattr], addbl);
  EXPECT_TRUE(object->get_attrs().contains("POSIX-Owner"));
  EXPECT_TRUE(object->get_attrs().contains(ATTR_OBJECT_TYPE));
}

TEST_F(POSIXBucketTest, MultipartUpload)
{
  std::string upload_id = "c0ffee";
  std::unique_ptr<rgw::sal::MultipartUpload> upload = bucket->get_multipart_upload(testname, upload_id);

  EXPECT_NE(upload.get(), nullptr);
  EXPECT_EQ(upload->get_meta(), testname + "." + upload_id);
  EXPECT_EQ(upload->get_key(), testname);
  EXPECT_EQ(upload->get_upload_id(), upload_id);
}

TEST_F(POSIXBucketTest, MPUploadCreate)
{
  std::string upload_id = "c0ffee";
  std::string mpname{".multipart_" + testname + "." + upload_id};
  sf::path tp{bp / "root" / testname / mpname};
  EXPECT_FALSE(sf::exists(tp));

  std::unique_ptr<rgw::sal::MultipartUpload> upload = bucket->get_multipart_upload(testname, upload_id);
  EXPECT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  int ret = upload->init(env->dpp, null_yield, acl_owner, placement, attrs);
  EXPECT_EQ(ret, 0);

  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

class POSIXMPObjectTest : public POSIXBucketTest {
protected:
  std::unique_ptr<rgw::sal::MultipartUpload> def_upload;
  std::string mpname;
  bufferlist write_data;

public:
  POSIXMPObjectTest() {}

  void SetUp() {
    def_upload = get_upload("c0ffee");
  }

  std::unique_ptr<rgw::sal::MultipartUpload> get_upload(std::string upload_id) {
    POSIXBucketTest::SetUp();
    mpname = ".multipart_" + testname + "." + upload_id;

    std::unique_ptr<rgw::sal::MultipartUpload> upload =
      bucket->get_multipart_upload(testname, upload_id);
    EXPECT_NE(upload.get(), nullptr);

    rgw_placement_rule placement;
    Attrs attrs;
    add_attr(attrs, ATTR1, ATTR1);
    int ret = upload->init(env->dpp, null_yield, acl_owner, placement, attrs);
    EXPECT_EQ(ret, 0);

    return upload;
  }

  void TearDown() {
    POSIXBucketTest::TearDown();
  }

  int write_part(rgw::sal::MultipartUpload* upload, int part_num) {
    std::unique_ptr<rgw::sal::Writer> writer;
    rgw_placement_rule placement;
    std::string part_name = "part-" + fmt::format("{:0>5}", part_num);
    ACLOwner owner{bucket->get_owner()};

    writer = upload->get_writer(env->dpp, null_yield, nullptr, owner,
                                &placement, part_num, part_name);
    EXPECT_NE(writer.get(), nullptr);

    int ret = writer->prepare(null_yield);
    EXPECT_EQ(ret, 0);

    int ofs{0};
    for (int i = 0; i < 4; ++i) {
      bufferlist bl;
      encode(testname + part_name, bl);
      int len = bl.length();

      write_data.append(bl);

      ret = writer->process(std::move(bl), ofs);
      EXPECT_EQ(ret, 0);

      ofs += len;
    }

    ret = writer->process({}, ofs);
    EXPECT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    bufferlist bl;
    encode(ATTR1, bl);
    attrs[ATTR1] = bl;
    req_context rctx{env->dpp, null_yield, nullptr};

    ret = writer->complete(ofs, part_name, &mtime, real_time(), attrs,
                           std::nullopt, real_time(), nullptr, nullptr, nullptr,
                           nullptr, nullptr, rctx, 0);
    EXPECT_EQ(ret, 0);

    return ofs;
  }
  uint64_t create_MPObj(rgw::sal::MultipartUpload* upload, std::string name) {
    std::map<int, std::string> parts;
    int part_count{4};
    uint64_t write_size{0};

    for (int i = 1; i <= part_count; ++i) {
      write_size += write_part(upload, i);
      parts[i] = "part-" + fmt::format("{:0>5}", i);
    }

    std::list<rgw_obj_index_key> remove_objs;
    bool compressed = false;
    RGWCompressionInfo cs_info;
    std::unique_ptr<Object> mp_obj = bucket->get_object(rgw_obj_key(name));
    off_t ofs{0};
    uint64_t accounted_size{0};
    std::string tag;
    rgw::sal::MultipartUpload::prefix_map_t processed_prefixes;
    ACLOwner owner;
    owner.id = bucket->get_owner();

    int ret = upload->complete(env->dpp, null_yield, get_pointer(env->cct), parts,
                               remove_objs, accounted_size, compressed, cs_info,
                               ofs, tag, owner, 0, mp_obj.get(), processed_prefixes);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(write_size, ofs);
    EXPECT_EQ(write_size, accounted_size);

    for (int i = 1; i <= part_count; ++i) {
      std::string part_name = "part-" + fmt::format("{:0>5}", i);
      sf::path tp{bp / "root" / testname / name / part_name};
      EXPECT_TRUE(sf::exists(tp));
      EXPECT_TRUE(sf::is_regular_file(tp));
    }

    return write_size;
  }
};

TEST_F(POSIXMPObjectTest, MPUploadWrite)
{
  std::unique_ptr<rgw::sal::Writer> writer;
  rgw_placement_rule placement;

  writer = def_upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
                              &placement, 1, "00001");
  EXPECT_NE(writer.get(), nullptr);

  int ret = writer->prepare(null_yield);
  EXPECT_EQ(ret, 0);

  int ofs{0};
  std::string etag{"part-00001"};
  for (int i = 0; i < 4; ++i) {
    bufferlist bl;
    encode(testname, bl);
    int len = bl.length();

    ret = writer->process(std::move(bl), ofs);
    EXPECT_EQ(ret, 0);

    ofs += len;
  }

  ret = writer->process({}, ofs);
  EXPECT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs attrs;
  bufferlist bl;
  encode(ATTR1, bl);
  attrs[ATTR1] = bl;
  req_context rctx{env->dpp, null_yield, nullptr};

  ret = writer->complete(ofs, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  EXPECT_EQ(ret, 0);

  sf::path tp{bp / "root" / testname / mpname / "part-00001" };
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
}

TEST_F(POSIXMPObjectTest, MPUploadComplete)
{
  create_MPObj(def_upload.get(), testname);
}

TEST_F(POSIXMPObjectTest, MPUploadRead)
{
  uint64_t write_size = create_MPObj(def_upload.get(), testname);

  std::unique_ptr<Object> mp_obj = bucket->get_object(rgw_obj_key(testname));
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(mp_obj->get_read_op());

  int ret = read_op->prepare(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(mp_obj->get_size(), write_size);

  bufferlist bl;
  Read_CB cb(&bl);
  ret = read_op->iterate(env->dpp, 0, write_size, &cb, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(write_data, bl);
}

TEST_F(POSIXMPObjectTest, MPUploadCopy)
{
  create_MPObj(def_upload.get(), testname);

  sf::path sp{bp / "root" / testname / testname};
  std::string dstname{testname + "-dst"};
  sf::path dp{bp / "root" / testname / dstname};

  std::unique_ptr<Object> object = bucket->get_object(rgw_obj_key(testname));
  EXPECT_NE(object.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> dstobj = bucket->get_object(rgw_obj_key(dstname));
  EXPECT_NE(dstobj.get(), nullptr);
  EXPECT_FALSE(sf::exists(dp));

  RGWEnv rgw_env;
  req_info info(env->cct.get(), &rgw_env);
  rgw_zone_id zone;
  rgw_placement_rule placement;
  ceph::real_time mtime;
  Attrs attrs;
  std::string tag;

  int ret = object->copy_object(acl_owner,
	   std::get<rgw_user>(owner),
	   &info,
	   zone,
	   dstobj.get(),
	   bucket.get(),
	   bucket.get(),
	   placement,
	   &mtime,
	   &mtime,
	   &mtime,
	   &mtime,
	   false,
	   nullptr,
	   nullptr,
	   ATTRSMOD_NONE,
	   false,
	   attrs,
	   RGWObjCategory::Main,
	   0,
	   boost::none,
	   nullptr,
	   &tag, /* use req_id as tag */
	   &tag,
	   nullptr,
	   nullptr,
	   env->dpp,
	   null_yield);
  EXPECT_EQ(ret, 0);

  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::exists(dp));
}

TEST_F(POSIXMPObjectTest, BucketList)
{
  std::unique_ptr<rgw::sal::MultipartUpload> up1 = get_upload("c0ffee-1");
  create_MPObj(up1.get(), testname + "-1");
  std::unique_ptr<Object> obj1 = bucket->get_object(rgw_obj_key(testname + "-1"));
  EXPECT_NE(obj1.get(), nullptr);
  std::unique_ptr<rgw::sal::MultipartUpload> up2 = get_upload("c0ffee-2");
  create_MPObj(up2.get(), testname + "-2");
  std::unique_ptr<Object> obj2 = bucket->get_object(rgw_obj_key(testname + "-2"));
  EXPECT_NE(obj2.get(), nullptr);
  std::unique_ptr<rgw::sal::MultipartUpload> up3 = get_upload("c0ffee-3");
  create_MPObj(up3.get(), testname + "-3");
  std::unique_ptr<Object> obj3 = bucket->get_object(rgw_obj_key(testname + "-3"));
  EXPECT_NE(obj3.get(), nullptr);

  rgw::sal::Bucket::ListParams params;
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results;

  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(results.is_truncated, false);

  EXPECT_EQ(results.objs.size(), 3);

  for (auto ent : results.objs) {
    printf("%s\n", ent.key.to_string().c_str());
  }

  rgw_obj_key key1(results.objs[0].key);
  EXPECT_EQ(key1, obj1->get_key());
  rgw_obj_key key2(results.objs[1].key);
  EXPECT_EQ(key2, obj2->get_key());
  rgw_obj_key key3(results.objs[2].key);
  EXPECT_EQ(key3, obj3->get_key());
}

TEST_F(POSIXBucketTest, VersionedObjectWrite)
{
  bucket->get_info().flags |= BUCKET_VERSIONED;
  sf::path tp{bp / "root" / testname / testname};
  EXPECT_FALSE(sf::exists(tp));

  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(rgw_obj_key(testname));
  EXPECT_NE(object.get(), nullptr);

  object->gen_rand_obj_instance_name();
  std::string inst_id = object->get_instance();

  std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
      env->dpp, null_yield, object.get(), acl_owner, nullptr, 0, testname);
  EXPECT_NE(writer.get(), nullptr);

  int ret = writer->prepare(null_yield);
  EXPECT_EQ(ret, 0);

  int ofs{0};
  std::string etag;
  for (int i = 0; i < 4; ++i) {
    bufferlist bl;
    encode(testname, bl);
    int len = bl.length();

    ret = writer->process(std::move(bl), ofs);
    EXPECT_EQ(ret, 0);

    ofs += len;
  }

  ret = writer->process({}, ofs);
  EXPECT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs attrs;
  bufferlist bl;
  encode(ATTR1, bl);
  attrs[ATTR1] = bl;
  req_context rctx{env->dpp, null_yield, nullptr};

  ret = writer->complete(ofs, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(object->get_size(), ofs);

  bufferlist getbl = object->get_attrs()[ATTR1];
  EXPECT_EQ(bl, getbl);

  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  std::string vfname{"_%3A" + inst_id + "_" + testname};
  sf::path op{tp / vfname};
  EXPECT_TRUE(sf::exists(op));
  EXPECT_TRUE(sf::is_regular_file(op));

  sf::path curver{tp / testname};
  EXPECT_TRUE(sf::exists(curver));
  EXPECT_TRUE(sf::is_symlink(curver));
  EXPECT_EQ(sf::read_symlink(curver), vfname);

  std::unique_ptr<rgw::sal::Object> obj2 = bucket->get_object(rgw_obj_key(testname));
  EXPECT_NE(obj2.get(), nullptr);

  obj2->gen_rand_obj_instance_name();
  inst_id = obj2->get_instance();

  writer.reset();
  writer = driver->get_atomic_writer(env->dpp, null_yield, obj2.get(), acl_owner, nullptr,
				     0, testname);
  EXPECT_NE(writer.get(), nullptr);

  ret = writer->prepare(null_yield);
  EXPECT_EQ(ret, 0);

  std::string obj2_content{testname + "ver2"};
  ofs = 0;
  for (int i = 0; i < 4; ++i) {
    bufferlist bl;
    encode(obj2_content, bl);
    int len = bl.length();

    ret = writer->process(std::move(bl), ofs);
    EXPECT_EQ(ret, 0);

    ofs += len;
  }

  ret = writer->process({}, ofs);
  EXPECT_EQ(ret, 0);

  bl.clear();
  encode(ATTR1, bl);
  attrs[ATTR1] = bl;

  ret = writer->complete(ofs, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(obj2->get_size(), ofs);

  getbl = obj2->get_attrs()[ATTR1];
  EXPECT_EQ(bl, getbl);

  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  vfname = "_%3A" + inst_id + "_" + testname;
  sf::path o2p{tp / vfname};
  EXPECT_TRUE(sf::exists(o2p));
  EXPECT_TRUE(sf::is_regular_file(o2p));

  EXPECT_TRUE(sf::exists(curver));
  EXPECT_TRUE(sf::is_symlink(curver));
  EXPECT_EQ(sf::read_symlink(curver), vfname);

  std::unique_ptr<rgw::sal::Object> robj = bucket->get_object(rgw_obj_key(testname));
  EXPECT_NE(robj.get(), nullptr);

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(robj->get_read_op());
  ret = read_op->prepare(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(robj->get_key().instance, inst_id);
}

class POSIXVerObjectTest : public POSIXBucketTest {
protected:
  uint64_t write_size{0};
  bufferlist write_data;

public:
  POSIXVerObjectTest() {}

  void SetUp() {
    POSIXBucketTest::SetUp();

    bucket->get_info().flags |= BUCKET_VERSIONED;
  }

  std::unique_ptr<rgw::sal::Object>  write_version(std::string objname) {
    std::unique_ptr<rgw::sal::Object> object = bucket->get_object(rgw_obj_key(objname));
    EXPECT_NE(object.get(), nullptr);
    object->gen_rand_obj_instance_name();

    std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
        env->dpp, null_yield, object.get(), acl_owner, nullptr, 0, testname);
    EXPECT_NE(writer.get(), nullptr);

    int ret = writer->prepare(null_yield);
    EXPECT_EQ(ret, 0);

    std::string etag;
    for (int i = 0; i < 4; ++i) {
      bufferlist bl;
      encode(objname, bl);
      int len = bl.length();

      write_data.claim_append(bl);

      ret = writer->process(std::move(bl), write_size);
      EXPECT_EQ(ret, 0);

      write_size += len;
    }

    ret = writer->process({}, write_size);
    EXPECT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    add_attr(attrs, ATTR1, ATTR1);
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(write_size, etag, &mtime, real_time(), attrs,
                           std::nullopt, real_time(), nullptr, nullptr, nullptr,
                           nullptr, nullptr, rctx, 0);
    EXPECT_EQ(ret, 0);

    return object;
  }

  void TearDown() { POSIXBucketTest::TearDown(); }
};

TEST_F(POSIXVerObjectTest, url_encode)
{
  std::string objname = testname + "&foo";
  std::string encname = url_encode(objname, true);
  sf::path sp{bp / "root" / testname / encname};
  std::unique_ptr<rgw::sal::Object> obj1v1 = write_version(objname);
  EXPECT_NE(obj1v1.get(), nullptr);
  std::string obj1v1_inst = obj1v1->get_instance();

  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::is_directory(sp));

  std::string vfname1{"_%3A" + obj1v1_inst + "_" + encname};
  sf::path op1{sp / vfname1};
  EXPECT_TRUE(sf::exists(op1));
  EXPECT_TRUE(sf::is_regular_file(op1));
}


TEST_F(POSIXVerObjectTest, BucketListV)
{
  std::unique_ptr<rgw::sal::Object> obj1v1 = write_version(testname + "-1");
  EXPECT_NE(obj1v1.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj1v2 = write_version(testname + "-1");
  EXPECT_NE(obj1v2.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj2v1 = write_version(testname + "-2");
  EXPECT_NE(obj2v1.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj2v2 = write_version(testname + "-2");
  EXPECT_NE(obj2v2.get(), nullptr);

  rgw::sal::Bucket::ListParams params;
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results;

  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(results.is_truncated, false);

  EXPECT_EQ(results.objs.size(), 4);

  for (auto ent : results.objs) {
    printf("%s\n", ent.key.to_string().c_str());
  }

  rgw_obj_key key1(results.objs[0].key);
  EXPECT_EQ(key1, obj1v2->get_key());
  rgw_obj_key key2(results.objs[1].key);
  EXPECT_EQ(key2, obj1v1->get_key());

  rgw_obj_key key4(results.objs[2].key);
  EXPECT_EQ(key4, obj2v2->get_key());
  rgw_obj_key key5(results.objs[3].key);
  EXPECT_EQ(key5, obj2v1->get_key());
}


TEST_F(POSIXVerObjectTest, DeleteCurVersion)
{
  std::string srcname{testname + "-1"};
  std::unique_ptr<rgw::sal::Object> obj1v1 = write_version(srcname);
  EXPECT_NE(obj1v1.get(), nullptr);
  std::string obj1v1_inst = obj1v1->get_instance();
  std::unique_ptr<rgw::sal::Object> obj1v2 = write_version(srcname);
  EXPECT_NE(obj1v2.get(), nullptr);
  std::string obj1v2_inst = obj1v2->get_instance();
  std::unique_ptr<rgw::sal::Object> obj1v3 = write_version(srcname);
  EXPECT_NE(obj1v3.get(), nullptr);
  std::string obj1v3_inst = obj1v3->get_instance();
  sf::path sp{bp / "root" / testname / srcname};
  std::unique_ptr<rgw::sal::Object> delobj = bucket->get_object(rgw_obj_key(srcname));
  EXPECT_NE(delobj.get(), nullptr);
  EXPECT_TRUE(sf::exists(sp));
  sf::path ops{sp / srcname};

  std::string vfname1{"_%3A" + obj1v1_inst + "_" + srcname};
  sf::path op1{sp / vfname1};
  EXPECT_TRUE(sf::exists(op1));
  EXPECT_TRUE(sf::is_regular_file(op1));
  std::string vfname2{"_%3A" + obj1v2_inst + "_" + srcname};
  sf::path op2{sp / vfname2};
  EXPECT_TRUE(sf::exists(op2));
  EXPECT_TRUE(sf::is_regular_file(op2));
  std::string vfname3{"_%3A" + obj1v3_inst + "_" + srcname};
  sf::path op3{sp / vfname3};
  EXPECT_TRUE(sf::exists(op3));
  EXPECT_TRUE(sf::is_regular_file(op3));
  EXPECT_TRUE(sf::exists(ops));
  EXPECT_TRUE(sf::is_symlink(ops));
  EXPECT_EQ(sf::read_symlink(ops), vfname3);


  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = delobj->get_delete_op();
  int ret = del_op->delete_obj(env->dpp, null_yield, 0);
  EXPECT_EQ(ret, 0);
  std::string delobj_inst = delobj->get_instance();
  std::string dfname{"_%3A" + delobj_inst + "_" + srcname};

  EXPECT_TRUE(sf::exists(op1));
  EXPECT_TRUE(sf::is_regular_file(op1));
  EXPECT_TRUE(sf::exists(op2));
  EXPECT_TRUE(sf::is_regular_file(op2));
  EXPECT_TRUE(sf::exists(op3));
  EXPECT_TRUE(sf::is_regular_file(op3));
  EXPECT_FALSE(sf::exists(ops));
  EXPECT_TRUE(sf::is_symlink(ops));
  /* Need to find a way to get the correct version */
  //EXPECT_EQ(sf::read_symlink(ops), dfname);

}

TEST_F(POSIXVerObjectTest, DeleteOldVersion)
{
  std::string srcname{testname + "-1"};
  std::unique_ptr<rgw::sal::Object> obj1v1 = write_version(srcname);
  EXPECT_NE(obj1v1.get(), nullptr);
  std::string obj1v1_inst = obj1v1->get_instance();
  std::unique_ptr<rgw::sal::Object> obj1v2 = write_version(srcname);
  EXPECT_NE(obj1v2.get(), nullptr);
  std::string obj1v2_inst = obj1v2->get_instance();
  std::unique_ptr<rgw::sal::Object> obj1v3 = write_version(srcname);
  EXPECT_NE(obj1v3.get(), nullptr);
  std::string obj1v3_inst = obj1v3->get_instance();
  sf::path sp{bp / "root" / testname / srcname};
  std::unique_ptr<rgw::sal::Object> delobj = bucket->get_object(rgw_obj_key(srcname, obj1v2_inst));
  EXPECT_NE(delobj.get(), nullptr);
  EXPECT_TRUE(sf::exists(sp));
  sf::path ops{sp / srcname};

  std::string vfname1{"_%3A" + obj1v1_inst + "_" + srcname};
  sf::path op1{sp / vfname1};
  EXPECT_TRUE(sf::exists(op1));
  EXPECT_TRUE(sf::is_regular_file(op1));
  std::string vfname2{"_%3A" + obj1v2_inst + "_" + srcname};
  sf::path op2{sp / vfname2};
  EXPECT_TRUE(sf::exists(op2));
  EXPECT_TRUE(sf::is_regular_file(op2));
  std::string vfname3{"_%3A" + obj1v3_inst + "_" + srcname};
  sf::path op3{sp / vfname3};
  EXPECT_TRUE(sf::exists(op3));
  EXPECT_TRUE(sf::is_regular_file(op3));
  EXPECT_TRUE(sf::exists(ops));
  EXPECT_TRUE(sf::is_symlink(ops));
  EXPECT_EQ(sf::read_symlink(ops), vfname3);


  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = delobj->get_delete_op();
  int ret = del_op->delete_obj(env->dpp, null_yield, 0);
  EXPECT_EQ(ret, 0);

  EXPECT_TRUE(sf::exists(op1));
  EXPECT_TRUE(sf::is_regular_file(op1));
  EXPECT_FALSE(sf::exists(op2));
  EXPECT_TRUE(sf::exists(op3));
  EXPECT_TRUE(sf::is_regular_file(op3));
  EXPECT_TRUE(sf::exists(ops));
  EXPECT_TRUE(sf::is_symlink(ops));
  EXPECT_EQ(sf::read_symlink(ops), vfname3);

}

TEST_F(POSIXVerObjectTest, ObjectCopy)
{
  std::string srcname{testname + "-1"};
  std::unique_ptr<rgw::sal::Object> obj1v1 = write_version(srcname);
  EXPECT_NE(obj1v1.get(), nullptr);
  std::string obj1v1_inst = obj1v1->get_instance();
  std::unique_ptr<rgw::sal::Object> obj1v2 = write_version(srcname);
  EXPECT_NE(obj1v2.get(), nullptr);
  std::string obj1v2_inst = obj1v2->get_instance();
  sf::path sp{bp / "root" / testname / srcname};
  std::unique_ptr<rgw::sal::Object> srcobj = bucket->get_object(rgw_obj_key(srcname));
  EXPECT_NE(srcobj.get(), nullptr);
  EXPECT_TRUE(sf::exists(sp));


  std::string dstname{testname + "-dst"};
  sf::path dp{bp / "root" / testname / dstname};

  std::unique_ptr<rgw::sal::Object> dstobj = bucket->get_object(rgw_obj_key(dstname));
  EXPECT_NE(dstobj.get(), nullptr);
  EXPECT_FALSE(sf::exists(dp));

  RGWEnv rgw_env;
  req_info info(env->cct.get(), &rgw_env);
  rgw_zone_id zone;
  rgw_placement_rule placement;
  ceph::real_time mtime;
  Attrs attrs;
  std::string tag;

  int ret = srcobj->copy_object(acl_owner,
	   std::get<rgw_user>(owner),
	   &info,
	   zone,
	   dstobj.get(),
	   bucket.get(),
	   bucket.get(),
	   placement,
	   &mtime,
	   &mtime,
	   &mtime,
	   &mtime,
	   false,
	   nullptr,
	   nullptr,
	   ATTRSMOD_NONE,
	   false,
	   attrs,
	   RGWObjCategory::Main,
	   0,
	   boost::none,
	   nullptr,
	   &tag, /* use req_id as tag */
	   &tag,
	   nullptr,
	   nullptr,
	   env->dpp,
	   null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::exists(dp));

  std::string vfname1{"_%3A" + obj1v1_inst + "_" + dstname};
  sf::path op1{dp / vfname1};
  EXPECT_TRUE(sf::exists(op1));
  EXPECT_TRUE(sf::is_regular_file(op1));
  std::string vfname2{"_%3A" + obj1v2_inst + "_" + dstname};
  sf::path op2{dp / vfname2};
  EXPECT_TRUE(sf::exists(op2));
  EXPECT_TRUE(sf::is_regular_file(op2));
  sf::path ops{dp / dstname};
  EXPECT_TRUE(sf::exists(ops));
  EXPECT_TRUE(sf::is_symlink(ops));
  EXPECT_EQ(sf::read_symlink(ops), vfname2);
}

TEST_F(POSIXVerObjectTest, CopyVersion)
{
  std::string srcname{testname + "-1"};
  std::unique_ptr<rgw::sal::Object> obj1v1 = write_version(srcname);
  EXPECT_NE(obj1v1.get(), nullptr);
  std::string obj1v1_inst = obj1v1->get_instance();
  std::unique_ptr<rgw::sal::Object> obj1v2 = write_version(srcname);
  EXPECT_NE(obj1v2.get(), nullptr);
  std::string obj1v2_inst = obj1v2->get_instance();
  std::string vsrcname{"_%3A" + obj1v1_inst + "_" + srcname};
  sf::path sp{bp / "root" / testname / srcname / vsrcname};
  std::unique_ptr<rgw::sal::Object> srcobj = bucket->get_object(rgw_obj_key(srcname, obj1v1_inst));
  EXPECT_NE(srcobj.get(), nullptr);
  EXPECT_TRUE(sf::exists(sp));


  std::string dstname{testname + "-dst"};
  sf::path dp{bp / "root" / testname / dstname};

  std::unique_ptr<rgw::sal::Object> dstobj = bucket->get_object(rgw_obj_key(dstname));
  EXPECT_NE(dstobj.get(), nullptr);
  EXPECT_FALSE(sf::exists(dp));

  RGWEnv rgw_env;
  req_info info(env->cct.get(), &rgw_env);
  rgw_zone_id zone;
  rgw_placement_rule placement;
  ceph::real_time mtime;
  Attrs attrs;
  std::string tag;

  int ret = srcobj->copy_object(acl_owner,
	   std::get<rgw_user>(owner),
	   &info,
	   zone,
	   dstobj.get(),
	   bucket.get(),
	   bucket.get(),
	   placement,
	   &mtime,
	   &mtime,
	   &mtime,
	   &mtime,
	   false,
	   nullptr,
	   nullptr,
	   ATTRSMOD_NONE,
	   false,
	   attrs,
	   RGWObjCategory::Main,
	   0,
	   boost::none,
	   nullptr,
	   &tag, /* use req_id as tag */
	   &tag,
	   nullptr,
	   nullptr,
	   env->dpp,
	   null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::exists(dp));

  std::string vfname1{"_%3A" + obj1v1_inst + "_" + dstname};
  sf::path op1{dp / vfname1};
  EXPECT_TRUE(sf::exists(op1));
  EXPECT_TRUE(sf::is_regular_file(op1));
  std::string vfname2{"_%3A" + obj1v2_inst + "_" + dstname};
  sf::path op2{dp / vfname2};
  EXPECT_FALSE(sf::exists(op2));
  sf::path ops{dp / dstname};
  EXPECT_TRUE(sf::exists(ops));
  EXPECT_TRUE(sf::is_symlink(ops));
  EXPECT_EQ(sf::read_symlink(ops), vfname1);
}

class POSIXVerMPObjectTest : public POSIXVerObjectTest {
protected:
  std::unique_ptr<rgw::sal::MultipartUpload> upload;
  std::string upload_id = "c0ffee";
  std::string mpname;
  uint64_t write_size{0};
  bufferlist write_data;

public:
  POSIXVerMPObjectTest() {}

  void SetUp() {
    POSIXVerObjectTest::SetUp();
    mpname = ".multipart_" + testname + "." + upload_id;

    upload = bucket->get_multipart_upload(testname, upload_id);
    EXPECT_NE(upload.get(), nullptr);

    rgw_placement_rule placement;
    Attrs attrs;
    add_attr(attrs, ATTR1, ATTR1);
    int ret = upload->init(env->dpp, null_yield, acl_owner, placement, attrs);
    EXPECT_EQ(ret, 0);
  }

  void TearDown() {
    POSIXVerObjectTest::TearDown();
  }

  int write_part(int part_num) {
    std::unique_ptr<rgw::sal::Writer> writer;
    rgw_placement_rule placement;
    std::string part_name = "part-" + fmt::format("{:0>5}", part_num);

    writer = upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
                                &placement, part_num, part_name);
    EXPECT_NE(writer.get(), nullptr);

    int ret = writer->prepare(null_yield);
    EXPECT_EQ(ret, 0);

    int ofs{0};
    for (int i = 0; i < 4; ++i) {
      bufferlist bl;
      encode(testname + part_name, bl);
      int len = bl.length();

      write_data.append(bl);

      ret = writer->process(std::move(bl), ofs);
      EXPECT_EQ(ret, 0);

      ofs += len;
    }

    ret = writer->process({}, ofs);
    EXPECT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    bufferlist bl;
    encode(ATTR1, bl);
    attrs[ATTR1] = bl;
    req_context rctx{env->dpp, null_yield, nullptr};

    ret = writer->complete(ofs, part_name, &mtime, real_time(), attrs,
                           std::nullopt, real_time(), nullptr, nullptr, nullptr,
                           nullptr, nullptr, rctx, 0);
    EXPECT_EQ(ret, 0);

    return ofs;
  }
  void create_MPObj(std::string objname) {
    std::map<int, std::string> parts;
    int part_count{4};

    for (int i = 1; i <= part_count; ++i) {
      write_size += write_part(i);
      parts[i] = "part-" + fmt::format("{:0>5}", i);
    }

    std::list<rgw_obj_index_key> remove_objs;
    bool compressed = false;
    RGWCompressionInfo cs_info;
    std::unique_ptr<Object> mp_obj = bucket->get_object(rgw_obj_key(objname));
    off_t ofs{0};
    uint64_t accounted_size{0};
    std::string tag;
    rgw::sal::MultipartUpload::prefix_map_t processed_prefixes;
    ACLOwner owner;
    owner.id = bucket->get_owner();
    mp_obj->gen_rand_obj_instance_name();
    std::string inst_id = mp_obj->get_instance();
    std::string vfname{"_%3A" + inst_id + "_" + objname};
    sf::path op{bp / "root" / testname / objname / vfname };

    int ret = upload->complete(env->dpp, null_yield, get_pointer(env->cct), parts,
                               remove_objs, accounted_size, compressed, cs_info,
                               ofs, tag, owner, 0, mp_obj.get(), processed_prefixes);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(write_size, ofs);
    EXPECT_EQ(write_size, accounted_size);
    EXPECT_TRUE(sf::exists(op));
    EXPECT_TRUE(sf::is_directory(op));

    for (int i = 1; i <= part_count; ++i) {
      std::string part_name = "part-" + fmt::format("{:0>5}", i);
      sf::path pp{bp / "root" / testname / objname / vfname / part_name};
      EXPECT_TRUE(sf::exists(pp));
      EXPECT_TRUE(sf::is_regular_file(pp));
    }

    std::unique_ptr<Object> object = bucket->get_object(rgw_obj_key(objname));
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(object->get_read_op());

    ret = read_op->prepare(null_yield, env->dpp);
    EXPECT_EQ(ret, 0);

    std::string getver = object->get_instance();
    EXPECT_EQ(inst_id, getver);

    ObjectType type{ObjectType::VERSIONED};
    ret = decode_attr(object->get_attrs(), ATTR_OBJECT_TYPE.c_str(), type);
    EXPECT_EQ(type.type, ObjectType::VERSIONED);

    std::unique_ptr<Object> vobj = bucket->get_object(rgw_obj_key(objname, inst_id));
    std::unique_ptr<rgw::sal::Object::ReadOp> vread_op(vobj->get_read_op());

    ret = vread_op->prepare(null_yield, env->dpp);
    EXPECT_EQ(ret, 0);

    ret = decode_attr(vobj->get_attrs(), ATTR_OBJECT_TYPE.c_str(), type);
    EXPECT_EQ(type.type, ObjectType::VERSIONED);

    sf::path ops{bp / "root" / testname / objname / objname};
    EXPECT_TRUE(sf::exists(ops));
    EXPECT_TRUE(sf::is_symlink(ops));
    EXPECT_EQ(sf::read_symlink(ops), vfname);
  }
};

TEST_F(POSIXVerMPObjectTest, MPUploadComplete)
{
  create_MPObj(testname + "MPVER");
}


int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
