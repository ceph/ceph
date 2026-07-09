// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include <gtest/gtest.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/xattr.h>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>

#include "rgw/driver/nsfs/gpfs/gpfs_fcntl.h"

namespace sf = std::filesystem;

namespace {
  std::string gpfs_lib_path = "/usr/lpp/mmfs/lib/libgpfs.so";
  std::string gpfs_test_dir = "/mnt/rgw/nsfs";
  bool verbose = false;
}

using gpfs_fcntl_t = int(*)(int fd, void* arg);

static size_t align8(size_t n) { return (n + 7) & ~7; }

class GPFSXattrTest : public ::testing::Test {
protected:
  static void* dl_handle;
  static gpfs_fcntl_t fn_fcntl;

  static void SetUpTestSuite() {
    dl_handle = dlopen(gpfs_lib_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!dl_handle) {
      std::cerr << "dlopen " << gpfs_lib_path << ": " << dlerror() << std::endl;
      GTEST_SKIP() << "libgpfs.so not available";
    }
    fn_fcntl = reinterpret_cast<gpfs_fcntl_t>(dlsym(dl_handle, "gpfs_fcntl"));
    ASSERT_NE(fn_fcntl, nullptr) << "gpfs_fcntl symbol not found";
  }

  static void TearDownTestSuite() {
    if (dl_handle) {
      dlclose(dl_handle);
      dl_handle = nullptr;
    }
  }

  void SetUp() override {
    if (!fn_fcntl) {
      GTEST_SKIP() << "gpfs_fcntl not available";
    }

    if (!sf::exists(gpfs_test_dir)) {
      GTEST_SKIP() << "GPFS test directory " << gpfs_test_dir << " not found";
    }

    test_file = gpfs_test_dir + "/xattr_test_file";
    fd = open(test_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0) << "open " << test_file << ": " << strerror(errno);

    const char* data = "gpfs xattr test";
    ASSERT_GT(write(fd, data, strlen(data)), 0);
  }

  void TearDown() override {
    if (fd >= 0) {
      close(fd);
      fd = -1;
    }
    if (!test_file.empty()) {
      unlink(test_file.c_str());
    }
  }

  std::string test_file;
  int fd = -1;
};

void* GPFSXattrTest::dl_handle = nullptr;
gpfs_fcntl_t GPFSXattrTest::fn_fcntl = nullptr;

TEST_F(GPFSXattrTest, SetSingle)
{
  const std::string name = "user.test_set_single";
  const std::string value = "hello_gpfs";

  size_t namelen = name.size() + 1;
  size_t padded_name = align8(namelen);
  size_t padded_value = align8(value.size());
  size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;

  size_t total = sizeof(gpfsFcntlHeader_t) + entry_size;
  std::vector<char> buf(total, 0);

  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf.data());
  hdr->totalLength = total;
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf.data() + sizeof(gpfsFcntlHeader_t));
  ea->structLen = entry_size;
  ea->structType = GPFS_FCNTL_SET_XATTR;
  ea->nameLen = namelen;
  ea->bufferLen = value.size();
  ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
  memcpy(ea->buffer, name.c_str(), namelen);
  memcpy(ea->buffer + padded_name, value.data(), value.size());

  int ret = fn_fcntl(fd, buf.data());
  if (verbose) {
    std::cerr << "SET_XATTR ret=" << ret << " errno=" << errno
              << " errReasonCode=" << ea->errReasonCode << std::endl;
  }
  ASSERT_EQ(ret, 0) << "SET_XATTR failed: " << strerror(errno)
                     << " errReasonCode=" << ea->errReasonCode;

  char verify[256];
  ssize_t sz = fgetxattr(fd, name.c_str(), verify, sizeof(verify));
  ASSERT_EQ(sz, (ssize_t)value.size()) << "fgetxattr: " << strerror(errno);
  EXPECT_EQ(std::string(verify, sz), value);
}

TEST_F(GPFSXattrTest, GetSingle)
{
  const std::string name = "user.test_get_single";
  const std::string value = "world_gpfs";

  int ret = fsetxattr(fd, name.c_str(), value.data(), value.size(), 0);
  ASSERT_EQ(ret, 0) << "fsetxattr: " << strerror(errno);

  size_t namelen = name.size() + 1;
  size_t padded_name = align8(namelen);
  size_t val_space = align8(GPFS_FCNTL_XATTR_MAX_VALUELEN);
  size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + val_space;

  size_t total = sizeof(gpfsFcntlHeader_t) + entry_size;
  std::vector<char> buf(total, 0);

  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf.data());
  hdr->totalLength = total;
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf.data() + sizeof(gpfsFcntlHeader_t));
  ea->structLen = entry_size;
  ea->structType = GPFS_FCNTL_GET_XATTR;
  ea->nameLen = namelen;
  ea->bufferLen = padded_name + val_space;
  ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
  memcpy(ea->buffer, name.c_str(), namelen);

  ret = fn_fcntl(fd, buf.data());
  if (verbose) {
    std::cerr << "GET_XATTR ret=" << ret << " errno=" << errno
              << " errReasonCode=" << ea->errReasonCode
              << " bufferLen=" << ea->bufferLen << std::endl;
  }
  ASSERT_EQ(ret, 0) << "GET_XATTR failed: " << strerror(errno)
                     << " errReasonCode=" << ea->errReasonCode;

  char* valp = ea->buffer + padded_name;
  std::string got(valp, ea->bufferLen);
  EXPECT_EQ(got, value);
}

TEST_F(GPFSXattrTest, ListNames)
{
  const std::string n1 = "user.test_list_1";
  const std::string n2 = "user.test_list_2";
  ASSERT_EQ(fsetxattr(fd, n1.c_str(), "a", 1, 0), 0);
  ASSERT_EQ(fsetxattr(fd, n2.c_str(), "b", 1, 0), 0);

  struct {
    gpfsFcntlHeader_t hdr;
    gpfsListXAttr_t list;
    char buf[4096];
  } arg;
  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  int ret = fn_fcntl(fd, &arg);
  if (verbose) {
    std::cerr << "LIST_XATTR ret=" << ret << " errno=" << errno
              << " errReasonCode=" << arg.list.errReasonCode
              << " bufferLen=" << arg.list.bufferLen << std::endl;
  }
  ASSERT_EQ(ret, 0) << "LIST_XATTR failed: " << strerror(errno)
                     << " errReasonCode=" << arg.list.errReasonCode;

  std::vector<std::string> names;
  char* p = arg.list.buffer;
  int remaining = arg.list.bufferLen;
  while (remaining > 0 && *p != '\0') {
    uint8_t namelen = static_cast<uint8_t>(*p);
    p++; remaining--;
    if (namelen > remaining) break;
    names.emplace_back(p, namelen);
    p += namelen; remaining -= namelen;
  }

  if (verbose) {
    for (auto& n : names) {
      std::cerr << "  listed: " << n << std::endl;
    }
  }

  bool found1 = false, found2 = false;
  for (auto& n : names) {
    if (n == n1) found1 = true;
    if (n == n2) found2 = true;
  }
  EXPECT_TRUE(found1) << n1 << " not found in list";
  EXPECT_TRUE(found2) << n2 << " not found in list";
}

TEST_F(GPFSXattrTest, SetBatch)
{
  struct xattr_entry {
    std::string name;
    std::string value;
  };
  std::vector<xattr_entry> entries = {
    {"user.batch_1", "value_one"},
    {"user.batch_2", "value_two"},
    {"user.batch_3", "value_three"},
  };

  char buf[GPFS_MAX_FCNTL_LENGTH];
  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
  memset(hdr, 0, sizeof(*hdr));
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  size_t off = sizeof(gpfsFcntlHeader_t);
  for (auto& e : entries) {
    size_t namelen = e.name.size() + 1;
    size_t padded_name = align8(namelen);
    size_t padded_value = align8(e.value.size());
    size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;

    ASSERT_LE(off + entry_size, sizeof(buf));

    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
    memset(ea, 0, entry_size);
    ea->structLen = entry_size;
    ea->structType = GPFS_FCNTL_SET_XATTR;
    ea->nameLen = namelen;
    ea->bufferLen = e.value.size();
    ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
    memcpy(ea->buffer, e.name.c_str(), namelen);
    memcpy(ea->buffer + padded_name, e.value.data(), e.value.size());

    off += entry_size;
  }

  hdr->totalLength = off;
  int ret = fn_fcntl(fd, buf);
  ASSERT_EQ(ret, 0) << "batch SET_XATTR failed: " << strerror(errno);

  for (auto& e : entries) {
    char verify[256];
    ssize_t sz = fgetxattr(fd, e.name.c_str(), verify, sizeof(verify));
    ASSERT_EQ(sz, (ssize_t)e.value.size())
        << e.name << " fgetxattr: " << strerror(errno);
    EXPECT_EQ(std::string(verify, sz), e.value);
  }
}

TEST_F(GPFSXattrTest, GetBatch)
{
  struct xattr_entry {
    std::string name;
    std::string value;
  };
  std::vector<xattr_entry> entries = {
    {"user.bget_1", "val_alpha"},
    {"user.bget_2", "val_beta"},
  };

  for (auto& e : entries) {
    ASSERT_EQ(fsetxattr(fd, e.name.c_str(), e.value.data(), e.value.size(), 0), 0);
  }

  char buf[GPFS_MAX_FCNTL_LENGTH];
  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
  memset(hdr, 0, sizeof(*hdr));
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  size_t off = sizeof(gpfsFcntlHeader_t);
  struct slot_t { size_t off; std::string name; };
  std::vector<slot_t> slots;

  for (auto& e : entries) {
    size_t namelen = e.name.size() + 1;
    size_t padded_name = align8(namelen);
    size_t val_space = align8(GPFS_FCNTL_XATTR_MAX_VALUELEN);
    size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + val_space;

    ASSERT_LE(off + entry_size, sizeof(buf));

    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
    memset(ea, 0, entry_size);
    ea->structLen = entry_size;
    ea->structType = GPFS_FCNTL_GET_XATTR;
    ea->nameLen = namelen;
    ea->bufferLen = padded_name + val_space;
    ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
    memcpy(ea->buffer, e.name.c_str(), namelen);

    slots.push_back({off, e.name});
    off += entry_size;
  }

  hdr->totalLength = off;
  int ret = fn_fcntl(fd, buf);
  ASSERT_EQ(ret, 0) << "batch GET_XATTR failed: " << strerror(errno);

  for (size_t i = 0; i < slots.size(); ++i) {
    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + slots[i].off);
    ASSERT_EQ(ea->errReasonCode, 0) << entries[i].name << " error";
    size_t padded_name = align8(ea->nameLen);
    char* valp = ea->buffer + padded_name;
    std::string got(valp, ea->bufferLen);
    EXPECT_EQ(got, entries[i].value)
        << "mismatch on " << entries[i].name;
  }
}

/* SET then LIST on same fd — reproduces the RGW write_attrs pattern */
TEST_F(GPFSXattrTest, SetThenListSameFd)
{
  const std::string n1 = "user.stl_1";
  const std::string n2 = "user.stl_2";
  const std::string n3 = "user.stl_3";

  /* batch SET 3 xattrs */
  {
    char buf[GPFS_MAX_FCNTL_LENGTH];
    auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
    memset(hdr, 0, sizeof(*hdr));
    hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

    size_t off = sizeof(gpfsFcntlHeader_t);
    for (auto& name : {n1, n2, n3}) {
      std::string value = "val_" + name;
      size_t namelen = name.size() + 1;
      size_t padded_name = align8(namelen);
      size_t padded_value = align8(value.size());
      size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;

      auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
      memset(ea, 0, entry_size);
      ea->structLen = entry_size;
      ea->structType = GPFS_FCNTL_SET_XATTR;
      ea->nameLen = namelen;
      ea->bufferLen = value.size();
      ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
      memcpy(ea->buffer, name.c_str(), namelen);
      memcpy(ea->buffer + padded_name, value.data(), value.size());
      off += entry_size;
    }
    hdr->totalLength = off;
    int ret = fn_fcntl(fd, buf);
    ASSERT_EQ(ret, 0) << "batch SET failed: " << strerror(errno);
  }

  /* immediately LIST on the same fd — no close/reopen */
  struct {
    gpfsFcntlHeader_t hdr;
    gpfsListXAttr_t list;
    char buf[4096];
  } arg;
  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  int ret = fn_fcntl(fd, &arg);
  ASSERT_EQ(ret, 0) << "LIST_XATTR failed: " << strerror(errno);

  std::vector<std::string> names;
  char* p = arg.list.buffer;
  int remaining = arg.list.bufferLen;
  while (remaining > 0 && *p != '\0') {
    uint8_t namelen = static_cast<uint8_t>(*p);
    p++; remaining--;
    if (namelen > remaining) break;
    names.emplace_back(p, namelen);
    p += namelen; remaining -= namelen;
  }

  if (verbose) {
    for (auto& n : names) {
      std::cerr << "  SetThenList listed: " << n << std::endl;
    }
  }

  bool f1 = false, f2 = false, f3 = false;
  for (auto& n : names) {
    if (n == n1) f1 = true;
    if (n == n2) f2 = true;
    if (n == n3) f3 = true;
  }
  EXPECT_TRUE(f1) << n1 << " not found after batch SET + LIST on same fd";
  EXPECT_TRUE(f2) << n2 << " not found after batch SET + LIST on same fd";
  EXPECT_TRUE(f3) << n3 << " not found after batch SET + LIST on same fd";
}

/* Reproduce RGW's exact xattr pattern on a directory fd */
TEST_F(GPFSXattrTest, DirectoryMultiXattr)
{
  std::string dir_path = gpfs_test_dir + "/xattr_dir_multi";
  sf::create_directories(dir_path);

  int dir_fd = open(dir_path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  ASSERT_GE(dir_fd, 0) << "open dir: " << strerror(errno);

  /* these match RGW's create_bucket xattr names */
  struct xattr_entry {
    std::string name;
    std::string value;
  };
  std::vector<xattr_entry> entries = {
    {"user.nsfs.bucket_info", std::string(291, 'I')},
    {"user.nsfs.object_type", std::string(10, 'T')},
    {"user.nsfs.rgw.acl", std::string(147, 'A')},
  };

  /* SET via POSIX first */
  for (auto& e : entries) {
    ASSERT_EQ(fsetxattr(dir_fd, e.name.c_str(), e.value.data(), e.value.size(), 0), 0)
        << e.name << ": " << strerror(errno);
  }

  /* LIST via gpfs_fcntl */
  struct {
    gpfsFcntlHeader_t hdr;
    gpfsListXAttr_t list;
    char buf[4096];
  } arg;
  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  int ret = fn_fcntl(dir_fd, &arg);
  ASSERT_EQ(ret, 0) << "LIST_XATTR failed: " << strerror(errno);

  std::vector<std::string> names;
  char* p = arg.list.buffer;
  int remaining = arg.list.bufferLen;
  while (remaining > 0 && *p != '\0') {
    uint8_t namelen = static_cast<uint8_t>(*p);
    p++; remaining--;
    if (namelen > remaining) break;
    names.emplace_back(p, namelen);
    p += namelen; remaining -= namelen;
  }

  if (verbose) {
    std::cerr << "DirectoryMultiXattr: bufferLen=" << arg.list.bufferLen
              << " found " << names.size() << " xattrs" << std::endl;
    for (auto& n : names) {
      std::cerr << "  listed: " << n << std::endl;
    }
  }

  for (auto& e : entries) {
    bool found = false;
    for (auto& n : names) {
      if (n == e.name) { found = true; break; }
    }
    EXPECT_TRUE(found) << e.name << " not found in LIST_XATTR on directory";
  }

  /* now add a 4th xattr (like put_cors) and re-list */
  std::string cors_name = "user.nsfs.rgw.cors";
  std::string cors_val(55, 'C');
  ASSERT_EQ(fsetxattr(dir_fd, cors_name.c_str(), cors_val.data(), cors_val.size(), 0), 0);

  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  ret = fn_fcntl(dir_fd, &arg);
  ASSERT_EQ(ret, 0) << "LIST_XATTR after add failed: " << strerror(errno);

  names.clear();
  p = arg.list.buffer;
  remaining = arg.list.bufferLen;
  while (remaining > 0 && *p != '\0') {
    uint8_t namelen = static_cast<uint8_t>(*p);
    p++; remaining--;
    if (namelen > remaining) break;
    names.emplace_back(p, namelen);
    p += namelen; remaining -= namelen;
  }

  if (verbose) {
    std::cerr << "DirectoryMultiXattr after add: bufferLen=" << arg.list.bufferLen
              << " found " << names.size() << " xattrs" << std::endl;
    for (auto& n : names) {
      std::cerr << "  listed: " << n << std::endl;
    }
  }

  bool found_cors = false;
  for (auto& n : names) {
    if (n == cors_name) { found_cors = true; break; }
  }
  EXPECT_TRUE(found_cors) << cors_name << " not found after add + re-list";

  /* also try: SET via batch, then LIST via batch on same dir fd */
  std::string batch_name = "user.nsfs.rgw.tagging";
  std::string batch_val(30, 'G');

  {
    size_t namelen = batch_name.size() + 1;
    size_t padded_name = align8(namelen);
    size_t padded_value = align8(batch_val.size());
    size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;
    size_t total = sizeof(gpfsFcntlHeader_t) + entry_size;
    std::vector<char> sbuf(total, 0);

    auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(sbuf.data());
    hdr->totalLength = total;
    hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(sbuf.data() + sizeof(gpfsFcntlHeader_t));
    ea->structLen = entry_size;
    ea->structType = GPFS_FCNTL_SET_XATTR;
    ea->nameLen = namelen;
    ea->bufferLen = batch_val.size();
    ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
    memcpy(ea->buffer, batch_name.c_str(), namelen);
    memcpy(ea->buffer + padded_name, batch_val.data(), batch_val.size());

    ret = fn_fcntl(dir_fd, sbuf.data());
    ASSERT_EQ(ret, 0) << "batch SET on dir failed: " << strerror(errno);
  }

  /* LIST again */
  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  ret = fn_fcntl(dir_fd, &arg);
  ASSERT_EQ(ret, 0) << "LIST_XATTR after batch SET failed: " << strerror(errno);

  names.clear();
  p = arg.list.buffer;
  remaining = arg.list.bufferLen;
  while (remaining > 0 && *p != '\0') {
    uint8_t namelen = static_cast<uint8_t>(*p);
    p++; remaining--;
    if (namelen > remaining) break;
    names.emplace_back(p, namelen);
    p += namelen; remaining -= namelen;
  }

  if (verbose) {
    std::cerr << "DirectoryMultiXattr after batch SET: bufferLen=" << arg.list.bufferLen
              << " found " << names.size() << " xattrs" << std::endl;
    for (auto& n : names) {
      std::cerr << "  listed: " << n << std::endl;
    }
  }

  bool found_tag = false;
  for (auto& n : names) {
    if (n == batch_name) { found_tag = true; break; }
  }
  EXPECT_TRUE(found_tag) << batch_name << " not found after batch SET + LIST on dir";

  close(dir_fd);
  sf::remove_all(dir_path);
}

/* Two fds: write on fd1, list on fd2 — reproduces RGW cross-request pattern */
TEST_F(GPFSXattrTest, TwoFdDirectoryList)
{
  std::string dir_path = gpfs_test_dir + "/xattr_dir_twofd";
  sf::create_directories(dir_path);

  int fd1 = open(dir_path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  ASSERT_GE(fd1, 0) << "open fd1: " << strerror(errno);

  /* SET via batch on fd1 */
  std::vector<std::pair<std::string, std::string>> entries = {
    {"user.nsfs.bucket_info", std::string(291, 'I')},
    {"user.nsfs.object_type", std::string(10, 'T')},
    {"user.nsfs.rgw.acl", std::string(147, 'A')},
  };

  {
    char buf[GPFS_MAX_FCNTL_LENGTH];
    auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
    memset(hdr, 0, sizeof(*hdr));
    hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

    size_t off = sizeof(gpfsFcntlHeader_t);
    for (auto& [name, value] : entries) {
      size_t namelen = name.size() + 1;
      size_t padded_name = align8(namelen);
      size_t padded_value = align8(value.size());
      size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;

      auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
      memset(ea, 0, entry_size);
      ea->structLen = entry_size;
      ea->structType = GPFS_FCNTL_SET_XATTR;
      ea->nameLen = namelen;
      ea->bufferLen = value.size();
      ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
      memcpy(ea->buffer, name.c_str(), namelen);
      memcpy(ea->buffer + padded_name, value.data(), value.size());
      off += entry_size;
    }
    hdr->totalLength = off;
    int ret = fn_fcntl(fd1, buf);
    ASSERT_EQ(ret, 0) << "batch SET on fd1 failed: " << strerror(errno);
  }

  /* verify: POSIX fgetxattr on fd1 sees them */
  for (auto& [name, value] : entries) {
    char vbuf[1];
    ssize_t sz = fgetxattr(fd1, name.c_str(), vbuf, 0);
    ASSERT_GE(sz, 0) << name << " not found via POSIX on fd1: " << strerror(errno);
  }

  /* open a SECOND fd on the same directory */
  int fd2 = open(dir_path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  ASSERT_GE(fd2, 0) << "open fd2: " << strerror(errno);
  ASSERT_NE(fd1, fd2);

  /* LIST via gpfs_fcntl on fd2 */
  struct {
    gpfsFcntlHeader_t hdr;
    gpfsListXAttr_t list;
    char buf[4096];
  } arg;
  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  int ret = fn_fcntl(fd2, &arg);
  ASSERT_EQ(ret, 0) << "LIST_XATTR on fd2 failed: " << strerror(errno);

  std::vector<std::string> names;
  char* p = arg.list.buffer;
  int remaining = arg.list.bufferLen;
  while (remaining > 0 && *p != '\0') {
    uint8_t namelen = static_cast<uint8_t>(*p);
    p++; remaining--;
    if (namelen > remaining) break;
    names.emplace_back(p, namelen);
    p += namelen; remaining -= namelen;
  }

  if (verbose) {
    std::cerr << "TwoFd: fd1=" << fd1 << " fd2=" << fd2
              << " listed " << names.size() << " xattrs on fd2" << std::endl;
    for (auto& n : names) {
      std::cerr << "  listed: " << n << std::endl;
    }
  }

  for (auto& [name, _] : entries) {
    bool found = false;
    for (auto& n : names) {
      if (n == name) { found = true; break; }
    }
    EXPECT_TRUE(found) << name << " not found via LIST on fd2 (written on fd1)";
  }

  /* also try POSIX flistxattr on fd2 for comparison */
  {
    char lbuf[4096];
    ssize_t sz = flistxattr(fd2, lbuf, sizeof(lbuf));
    ASSERT_GE(sz, 0) << "flistxattr on fd2: " << strerror(errno);
    std::vector<std::string> posix_names;
    char* pp = lbuf;
    while (pp < lbuf + sz) {
      posix_names.emplace_back(pp);
      pp += strlen(pp) + 1;
    }
    if (verbose) {
      std::cerr << "TwoFd POSIX: listed " << posix_names.size() << " on fd2" << std::endl;
      for (auto& n : posix_names) {
        std::cerr << "  posix: " << n << std::endl;
      }
    }
  }

  close(fd1);
  close(fd2);
  sf::remove_all(dir_path);
}

/* LIST with max-size buffer — reproduces RGW's get_xattrs layout */
TEST_F(GPFSXattrTest, ListMaxBuffer)
{
  ASSERT_EQ(fsetxattr(fd, "user.maxbuf", "x", 1, 0), 0);

  struct {
    gpfsFcntlHeader_t hdr;
    gpfsListXAttr_t list;
    char buf[GPFS_MAX_FCNTL_LENGTH - sizeof(gpfsFcntlHeader_t)
             - sizeof(gpfsListXAttr_t)];
  } arg;
  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  if (verbose) {
    std::cerr << "ListMaxBuffer: totalLength=" << arg.hdr.totalLength
              << " structLen=" << arg.list.structLen
              << " bufferLen=" << arg.list.bufferLen
              << " sizeof(arg)=" << sizeof(arg) << std::endl;
  }

  int ret = fn_fcntl(fd, &arg);
  if (verbose) {
    std::cerr << "ListMaxBuffer: ret=" << ret << " errno=" << errno
              << " errReasonCode=" << arg.list.errReasonCode << std::endl;
  }
  EXPECT_EQ(ret, 0) << "LIST_XATTR max buffer failed: " << strerror(errno)
                     << " errReasonCode=" << arg.list.errReasonCode;
}

/* directory fd — reproduces the create_bucket path */
TEST_F(GPFSXattrTest, SetOnDirectory)
{
  std::string dir_path = gpfs_test_dir + "/xattr_test_dir";
  sf::create_directories(dir_path);

  int dir_fd = open(dir_path.c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(dir_fd, 0) << "open dir: " << strerror(errno);

  const std::string name = "user.dir_xattr";
  const std::string value = "dir_value";

  size_t namelen = name.size() + 1;
  size_t padded_name = align8(namelen);
  size_t padded_value = align8(value.size());
  size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;

  size_t total = sizeof(gpfsFcntlHeader_t) + entry_size;
  std::vector<char> buf(total, 0);

  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf.data());
  hdr->totalLength = total;
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf.data() + sizeof(gpfsFcntlHeader_t));
  ea->structLen = entry_size;
  ea->structType = GPFS_FCNTL_SET_XATTR;
  ea->nameLen = namelen;
  ea->bufferLen = value.size();
  ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
  memcpy(ea->buffer, name.c_str(), namelen);
  memcpy(ea->buffer + padded_name, value.data(), value.size());

  int ret = fn_fcntl(dir_fd, buf.data());
  if (verbose) {
    std::cerr << "SET_XATTR(dir) ret=" << ret << " errno=" << errno
              << " errReasonCode=" << ea->errReasonCode << std::endl;
  }
  EXPECT_EQ(ret, 0) << "SET_XATTR on directory failed: " << strerror(errno)
                     << " errReasonCode=" << ea->errReasonCode;

  close(dir_fd);
  sf::remove_all(dir_path);
}

TEST_F(GPFSXattrTest, ListOnDirectory)
{
  std::string dir_path = gpfs_test_dir + "/xattr_test_dir2";
  sf::create_directories(dir_path);

  int dir_fd = open(dir_path.c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(dir_fd, 0) << "open dir: " << strerror(errno);

  ASSERT_EQ(fsetxattr(dir_fd, "user.dlist", "v", 1, 0), 0);

  struct {
    gpfsFcntlHeader_t hdr;
    gpfsListXAttr_t list;
    char buf[4096];
  } arg;
  memset(&arg, 0, sizeof(arg));
  arg.hdr.totalLength = sizeof(arg);
  arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  arg.list.structLen = sizeof(arg.list) + sizeof(arg.buf);
  arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  arg.list.bufferLen = sizeof(arg.buf);

  int ret = fn_fcntl(dir_fd, &arg);
  if (verbose) {
    std::cerr << "LIST_XATTR(dir) ret=" << ret << " errno=" << errno
              << " errReasonCode=" << arg.list.errReasonCode
              << " bufferLen=" << arg.list.bufferLen << std::endl;
  }
  EXPECT_EQ(ret, 0) << "LIST_XATTR on directory failed: " << strerror(errno)
                     << " errReasonCode=" << arg.list.errReasonCode;

  close(dir_fd);
  sf::remove_all(dir_path);
}

/*
 * Benchmarks — batch gpfs_fcntl vs per-attr POSIX syscalls.
 *
 * Each benchmark creates a fresh file, writes N xattrs via both methods
 * across I iterations, and reports wall-clock ns/op.  The file is
 * re-truncated between methods to equalize cache state.
 */

static constexpr int BENCH_ITERS = 2000;

struct BenchParams {
  int num_attrs;
  int val_size;
};

class GPFSXattrBench : public GPFSXattrTest,
                       public ::testing::WithParamInterface<BenchParams> {};

static std::vector<std::pair<std::string, std::string>>
make_attrs(int n, int val_size) {
  std::vector<std::pair<std::string, std::string>> out;
  out.reserve(n);
  std::string val(val_size, 'V');
  for (int i = 0; i < n; i++) {
    out.emplace_back("user.bench_" + std::to_string(i), val);
  }
  return out;
}

static void write_batch(gpfs_fcntl_t fn, int fd,
                        const std::vector<std::pair<std::string,std::string>>& attrs)
{
  char buf[GPFS_MAX_FCNTL_LENGTH];
  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
  memset(hdr, 0, sizeof(*hdr));
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  size_t off = sizeof(gpfsFcntlHeader_t);
  for (auto& [name, value] : attrs) {
    size_t namelen = name.size() + 1;
    size_t padded_name = align8(namelen);
    size_t padded_value = align8(value.size());
    size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;

    if (off + entry_size > sizeof(buf)) break;

    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
    memset(ea, 0, entry_size);
    ea->structLen = entry_size;
    ea->structType = GPFS_FCNTL_SET_XATTR;
    ea->nameLen = namelen;
    ea->bufferLen = value.size();
    ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
    memcpy(ea->buffer, name.c_str(), namelen);
    memcpy(ea->buffer + padded_name, value.data(), value.size());
    off += entry_size;
  }
  hdr->totalLength = off;
  fn(fd, buf);
}

static void write_posix(int fd,
                        const std::vector<std::pair<std::string,std::string>>& attrs)
{
  for (auto& [name, value] : attrs) {
    fsetxattr(fd, name.c_str(), value.data(), value.size(), 0);
  }
}

static void read_batch(gpfs_fcntl_t fn, int fd,
                       const std::vector<std::pair<std::string,std::string>>& attrs)
{
  char buf[GPFS_MAX_FCNTL_LENGTH];
  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
  memset(hdr, 0, sizeof(*hdr));
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  size_t off = sizeof(gpfsFcntlHeader_t);
  for (auto& [name, value] : attrs) {
    size_t namelen = name.size() + 1;
    size_t padded_name = align8(namelen);
    /* use known value size + headroom, not XATTR_MAX_VALUELEN */
    size_t val_space = align8(value.size() + 256);
    size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + val_space;

    if (off + entry_size > sizeof(buf)) break;

    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
    memset(ea, 0, entry_size);
    ea->structLen = entry_size;
    ea->structType = GPFS_FCNTL_GET_XATTR;
    ea->nameLen = namelen;
    ea->bufferLen = padded_name + val_space;
    ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
    memcpy(ea->buffer, name.c_str(), namelen);
    off += entry_size;
  }
  hdr->totalLength = off;
  fn(fd, buf);
}

static void read_posix(int fd,
                       const std::vector<std::pair<std::string,std::string>>& attrs)
{
  char val[GPFS_FCNTL_XATTR_MAX_VALUELEN];
  for (auto& [name, _] : attrs) {
    fgetxattr(fd, name.c_str(), val, sizeof(val));
  }
}

TEST_P(GPFSXattrBench, WritePerf)
{
  auto [num_attrs, val_size] = GetParam();
  auto attrs = make_attrs(num_attrs, val_size);

  /* seed xattrs so both paths do overwrites (not creates) */
  write_posix(fd, attrs);

  /* warm up */
  for (int i = 0; i < 50; i++) {
    write_batch(fn_fcntl, fd, attrs);
  }

  /* benchmark: batch */
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < BENCH_ITERS; i++) {
    write_batch(fn_fcntl, fd, attrs);
  }
  auto t1 = std::chrono::steady_clock::now();

  /* benchmark: posix */
  auto t2 = std::chrono::steady_clock::now();
  for (int i = 0; i < BENCH_ITERS; i++) {
    write_posix(fd, attrs);
  }
  auto t3 = std::chrono::steady_clock::now();

  auto batch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  auto posix_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count();

  std::cout << "WRITE " << num_attrs << " attrs x " << val_size << "B"
            << "  iters=" << BENCH_ITERS
            << "  batch=" << batch_ns / BENCH_ITERS << " ns/op"
            << "  posix=" << posix_ns / BENCH_ITERS << " ns/op"
            << "  speedup=" << std::fixed << std::setprecision(2)
            << (double)posix_ns / batch_ns << "x"
            << std::endl;
}

TEST_P(GPFSXattrBench, ReadPerf)
{
  auto [num_attrs, val_size] = GetParam();
  auto attrs = make_attrs(num_attrs, val_size);

  /* seed xattrs */
  write_posix(fd, attrs);

  /* warm up */
  for (int i = 0; i < 50; i++) {
    read_batch(fn_fcntl, fd, attrs);
  }

  /* benchmark: batch */
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < BENCH_ITERS; i++) {
    read_batch(fn_fcntl, fd, attrs);
  }
  auto t1 = std::chrono::steady_clock::now();

  /* benchmark: posix */
  auto t2 = std::chrono::steady_clock::now();
  for (int i = 0; i < BENCH_ITERS; i++) {
    read_posix(fd, attrs);
  }
  auto t3 = std::chrono::steady_clock::now();

  auto batch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  auto posix_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count();

  std::cout << "READ  " << num_attrs << " attrs x " << val_size << "B"
            << "  iters=" << BENCH_ITERS
            << "  batch=" << batch_ns / BENCH_ITERS << " ns/op"
            << "  posix=" << posix_ns / BENCH_ITERS << " ns/op"
            << "  speedup=" << std::fixed << std::setprecision(2)
            << (double)posix_ns / batch_ns << "x"
            << std::endl;
}

INSTANTIATE_TEST_SUITE_P(XattrScale, GPFSXattrBench,
  ::testing::Values(
    BenchParams{3, 64},     /* bucket metadata: ~3 attrs */
    BenchParams{9, 128},    /* object PUT: ~9 attrs, typical value size */
    BenchParams{9, 1024},   /* object PUT: larger metadata values */
    BenchParams{20, 128},   /* heavy metadata: 20 attrs */
    BenchParams{20, 512}    /* stress: 20 attrs x 512B values */
  ),
  [](const ::testing::TestParamInfo<BenchParams>& info) {
    return std::to_string(info.param.num_attrs) + "x" +
           std::to_string(info.param.val_size) + "B";
  }
);

int main(int argc, char* argv[]) {
  for (int i = 1; i < argc; i++) {
    if (std::string(argv[i]) == "--verbose") {
      verbose = true;
    } else if (std::string(argv[i]) == "--gpfs-lib" && i + 1 < argc) {
      gpfs_lib_path = argv[++i];
    } else if (std::string(argv[i]) == "--gpfs-dir" && i + 1 < argc) {
      gpfs_test_dir = argv[++i];
    }
  }

  std::cout << "gpfs_lib=" << gpfs_lib_path
            << " gpfs_dir=" << gpfs_test_dir
            << " verbose=" << verbose << std::endl;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
