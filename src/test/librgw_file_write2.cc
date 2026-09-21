// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <fcntl.h>
#include <stdint.h>
#include <cstdint>
#include <memory>
#include <ranges>
#include <tuple>
#include <iostream>
#include <vector>
#include <map>
#include <random>
#include "xxhash.h"

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {
  librgw_t rgw = nullptr;
  string userid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;

  uint32_t owner_uid = 867;
  uint32_t owner_gid = 5309;
  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

  bool do_create = false;
  bool do_delete = false;
  bool do_large = false;
  bool do_verify = false;
  bool do_hexdump = false;

  string bucket_name = "sorrydave";
  string object_name = "jocaml";

  std::string lorem =
    "Lorem ipsum dolor sit amet";

  std::string dolor =
    R"(Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.)";

  std::string lacrimae = dolor + dolor;
  std::string dolorem = dolor + lorem;

  struct rgw_file_handle* bucket_fh = nullptr;
  struct rgw_file_handle* object_fh = nullptr;

  class Open2Helper {
  public:
    struct rgw_fs* fs{nullptr};
    struct rgw_file_handle* bucket_fh{nullptr};
    struct rgw_file_handle* object_fh{nullptr};

    Open2Helper(rgw_fs* _fs, rgw_file_handle* _bucket) :
      fs(_fs), bucket_fh(_bucket)
    {}

    using LookupResult = std::tuple<int, rgw_file_handle*>;
    LookupResult lookup(std::string name)
    {
      /* look up handle only--no linkage to object/file yet */
      int rc{0};
      rc = rgw_lookup(fs, bucket_fh, name.c_str(), &object_fh, nullptr,
                      0, RGW_LOOKUP_FLAG_CREATE);
      return LookupResult{rc, object_fh};
    }      
    
    using OpenResult = std::tuple<int, rgw_open_fd>;
    OpenResult open(uint32_t openflags, uint32_t flags)
    {
      OpenResult ofr;
      std::get<0>(ofr) = rgw_open2(fs, object_fh,
                                   &(std::get<1>(ofr)), openflags, flags);
      return ofr;
    }

    using StatResult = std::tuple<int, struct stat>;
    StatResult stat()
    {
      StatResult sr;
      std::get<0>(sr) = rgw_getattr(fs, object_fh, &(std::get<1>(sr)),
                                    RGW_GETATTR_FLAG_NONE);
      return sr;
    }

    int unlink()
    {
      return rgw_unlink(fs, bucket_fh, object_name.c_str(),
                        RGW_UNLINK_FLAG_NONE);
    }

    using ReadResult = std::tuple<int, std::string>;
    ReadResult read(rgw_open_fd fd, uint64_t off, uint64_t len)
    {
      char buf[1024];
      struct iovec iov[1];
      uint64_t nb_read{0};
      std::string str;
      int ret{0};

      while (len > 0) {

        iov->iov_base = buf;
        iov->iov_len = std::min(len, uint64_t(1024));

        ret = rgw_readv(fd, iov, 1, off, &nb_read, RGW_READ_FLAG_NONE);
        if (ret < 0) {
          return ReadResult(ret, str);
        }
        if (nb_read == 0) {
          break; /* EOF */
        }
        str.append((char*)iov->iov_base, nb_read);
        len -= nb_read;
        off += nb_read;
      }
      return ReadResult(ret, str);
    }

    using WriteResult = std::tuple<int, uint64_t>;
    WriteResult write(rgw_open_fd fd, const std::string& str, uint64_t off,
               uint64_t _len)
    {
      struct iovec iov[1];
      uint64_t nb_written{0}, nb_total{0},  pos{0};
      uint64_t len{std::min(_len, str.length())};

      while (len > 0) {
        std::string sub = str.substr(pos);
        iov->iov_base = (void*) sub.c_str();
        iov->iov_len = len;

        int ret = rgw_writev(fd, iov, 1, off, &nb_written, RGW_WRITE_FLAG_NONE);
        if (ret < 0) {
          return WriteResult(ret, nb_written);
        }
        if (unlikely(nb_written == 0)) {
          break; /* should not happen */
        }
        len -= nb_written;
        off += nb_written;
        pos += nb_written;
        nb_total += nb_written;
      }
      return WriteResult(0, nb_total);
    }

    int close(rgw_open_fd fd) { return rgw_close2(fd, RGW_CLOSE_FLAG_NONE); }

    int setattr(struct stat* st, uint32_t mask)
    {
      return rgw_setattr(fs, object_fh, st, mask, RGW_SETATTR_FLAG_NONE);
    }

    int setxattr(const std::string& key, const std::string& val)
    {
      rgw_xattrstr k = { const_cast<char*>(key.c_str()),
			  uint32_t(key.length()) };
      rgw_xattrstr v = { const_cast<char*>(val.c_str()),
			  uint32_t(val.length()) };
      rgw_xattr xa = { k, v };
      rgw_xattrlist xlist = { &xa, 1 };
      return rgw_setxattrs(fs, object_fh, &xlist, RGW_SETXATTR_FLAG_NONE);
    }

    using GetXattrResult = std::tuple<int, std::string>;
    GetXattrResult getxattr(const std::string& key)
    {
      std::string result;
      rgw_xattrstr k = { const_cast<char*>(key.c_str()),
			  uint32_t(key.length()) };
      rgw_xattrstr v = { nullptr, 0 };
      rgw_xattr xa = { k, v };
      rgw_xattrlist xlist = { &xa, 1 };

      auto cb = [](rgw_xattrlist* attrs, void* arg, uint32_t flags) -> int {
	auto* out = static_cast<std::string*>(arg);
	if (attrs->xattr_cnt > 0 && attrs->xattrs[0].val.val) {
	  out->assign(attrs->xattrs[0].val.val, attrs->xattrs[0].val.len);
	}
	return 0;
      };

      int rc = rgw_getxattrs(fs, object_fh, &xlist, cb, &result,
			     RGW_GETXATTR_FLAG_NONE);
      return GetXattrResult{rc, result};
    }

    using LsXattrResult = std::tuple<int, std::vector<std::string>>;
    LsXattrResult lsxattrs()
    {
      std::vector<std::string> keys;

      auto cb = [](rgw_xattrlist* attrs, void* arg, uint32_t flags) -> int {
	auto* out = static_cast<std::vector<std::string>*>(arg);
	for (uint32_t i = 0; i < attrs->xattr_cnt; ++i) {
	  out->emplace_back(attrs->xattrs[i].key.val,
			    attrs->xattrs[i].key.len);
	}
	return 0;
      };

      int rc = rgw_lsxattrs(fs, object_fh, nullptr, cb, &keys,
			    RGW_LSXATTR_FLAG_NONE);
      return LsXattrResult{rc, keys};
    }

    int rmxattr(const std::string& key)
    {
      rgw_xattrstr k = { const_cast<char*>(key.c_str()),
			  uint32_t(key.length()) };
      rgw_xattrstr v = { nullptr, 0 };
      rgw_xattr xa = { k, v };
      rgw_xattrlist xlist = { &xa, 1 };
      return rgw_rmxattrs(fs, object_fh, &xlist, RGW_RMXATTR_FLAG_NONE);
    }

    ~Open2Helper()
    {
      (void) rgw_fh_rele(fs, object_fh, RGW_FH_RELE_FLAG_NONE);
    }

  }; /* Open2helper */

  typedef std::tuple<string,uint64_t, struct rgw_file_handle*> fid_type;
  std::vector<fid_type> fids;

  std::uniform_int_distribution<uint8_t> uint_dist;
  std::mt19937 rng;

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(OPEN2, INIT) {
  int ret = librgw_create(&rgw, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw, nullptr);
}

TEST(OPEN2, MOUNT) {
  int ret = rgw_mount2(rgw, userid.c_str(), access_key.c_str(),
                       secret_key.c_str(), "/", &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(OPEN2, CREATE_BUCKET) {
  if (do_create) {
    struct stat st;
    struct rgw_file_handle *fh;

    st.st_uid = owner_uid;
    st.st_gid = owner_gid;
    st.st_mode = 755;

    int ret = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), &st, create_mask,
			&fh, RGW_MKDIR_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(OPEN2, LOOKUP_BUCKET) {
  int ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, LOOKUP_OBJECT) {
  int ret = rgw_lookup(fs, bucket_fh, object_name.c_str(), &object_fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_CREATE);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, OPEN1) {
  int ret = rgw_open(fs, object_fh, 0 /* posix flags */, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, PUT_OBJECT1) {
  size_t nbytes;
  string data = "hi mom"; // fix this
  int ret = rgw_write(fs, object_fh, 0, data.length(), &nbytes,
                      (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(nbytes, data.length());
  /* commit write transaction */
  ret = rgw_close(fs, object_fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, GET_OBJECT1)
{
  char sbuf[128];
  memset(sbuf, 0, 128);
  size_t nread;
  int ret = rgw_read(fs, object_fh, 0 /* off */, 6 /* len */, &nread, sbuf,
                     RGW_READ_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  std::string str{sbuf, 6};
  ASSERT_EQ(str, "hi mom");
}

TEST(OPEN2, CLOSE1) {
  int ret = rgw_close(fs, object_fh, RGW_CLOSE_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  /* manual handle release */
  ret = rgw_fh_rele(fs, object_fh, RGW_FH_RELE_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, STAT_OBJECT) {
  struct stat st;
  int ret = rgw_getattr(fs, object_fh, &st, RGW_GETATTR_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  dout(15) << "rgw_getattr on " << object_name << " size = "
	   << st.st_size << dendl;
}

TEST(OPEN2, DELETE_OBJECT) {
  if (do_delete) {
    int ret = rgw_unlink(fs, bucket_fh, object_name.c_str(),
			 RGW_UNLINK_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(OPEN2, OPEN2_READAFTERWRITE1)
{
  /* write and read-after-write, same handle */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("netbird2");
  ASSERT_EQ(get<0>(lfr), 0);
  ASSERT_NE(get<1>(lfr), nullptr);

  auto ofr = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofr), 0);
  ASSERT_NE(get<1>(ofr), nullptr);

  auto open1 = get<1>(ofr);

  auto nbw = o2h->write(open1, dolor, 0, dolor.length());
  ASSERT_EQ(std::get<0>(nbw), 0);  
  ASSERT_EQ(std::get<1>(nbw), dolor.length());

  nbw = o2h->write(open1, dolor, dolor.length(), dolor.length());
  ASSERT_EQ(std::get<0>(nbw), 0);
  ASSERT_EQ(std::get<1>(nbw), dolor.length());

  /* read after write, same handle */
  auto rdr1 = o2h->read(open1, 0, 2 * dolor.length());
  ASSERT_EQ(std::get<0>(rdr1), 0);
  ASSERT_EQ(std::get<1>(rdr1), dolor+dolor);

  /* read a subrange, same handle */
  std::string sit = "sit amet";
  auto rdr2 = o2h->read(open1, 18, 8);
  ASSERT_EQ(std::get<0>(rdr2), 0);
  ASSERT_EQ(sit, std::get<1>(rdr2));

  /* commit write transaction */
  int ret = o2h->close(open1); // not returning file handle!
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, OPEN2_READAFTERWRITE2)
{
  /* write and read-after-write, write and read handles */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("tray1");
  ASSERT_EQ(get<0>(lfr), 0);
  ASSERT_NE(get<1>(lfr), nullptr);
  
  auto ofr1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  auto open1 = std::get<1>(ofr1);
  ASSERT_NE(open1, nullptr);

  auto ofr2 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);  
  auto open2 = get<1>(ofr2);
  ASSERT_NE(open2, nullptr);

  std::string str1{"netbird lives in your"};
  auto nbw = o2h->write(open1, str1, 0, str1.length());
  ASSERT_EQ(std::get<0>(nbw), 0);
  ASSERT_EQ(std::get<1>(nbw), str1.length());

  auto rdr = o2h->read(open2, 0, str1.length());
  ASSERT_EQ(std::get<0>(rdr), 0);
  ASSERT_EQ(str1, std::get<1>(rdr));

  o2h->close(open1);
  o2h->close(open2);
}

TEST(OPEN2, ACC_MODES)
{
  /* write and read-after-write, write and read handles */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("accthis1");
  ASSERT_EQ(get<0>(lfr), 0);
  ASSERT_NE(get<1>(lfr), nullptr);
  
  auto ofr2 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_CREATE);
  auto ofr3 = o2h->open(O_WRONLY, RGW_OPEN_FLAG_NONE);

  auto open2 = std::get<1>(ofr2);
  ASSERT_NE(open2, nullptr);

  auto open3 = get<1>(ofr3);
  ASSERT_NE(open2, nullptr);
  
  std::string danger{"beware of darkness"};

  auto wr1 = o2h->write(open2, danger, 0, danger.length());
  ASSERT_NE(std::get<0>(wr1), 0);

  auto rdr1 = o2h->read(open3, 0, danger.length());
  ASSERT_NE(std::get<0>(rdr1), 0);

  o2h->close(open2);
  o2h->close(open3);
}

TEST(OPEN2, CREATE_FLAG)
{
  /* open non-existing + FLAG_NONE fails (correctly) */
  /* open non-existing + FLAG_CREATE succeeds */
}

TEST(OPEN2, SETATTR1)
{
  /* set attrs during open session, verify, close, reopen, verify */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("attrtest1");
  ASSERT_EQ(get<0>(lfr), 0);

  auto ofr = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  /* write some data */
  std::string data{"attr test content"};
  auto nbw = o2h->write(open1, data, 0, data.length());
  ASSERT_EQ(std::get<0>(nbw), 0);

  /* set uid/gid/mode */
  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_uid = 1234;
  st.st_gid = 5678;
  st.st_mode = 0644;
  int rc = o2h->setattr(&st, RGW_SETATTR_UID | RGW_SETATTR_GID |
			      RGW_SETATTR_MODE);
  ASSERT_EQ(rc, 0);

  /* verify attrs with getattr during open session */
  auto sr1 = o2h->stat();
  ASSERT_EQ(std::get<0>(sr1), 0);
  auto& st1 = std::get<1>(sr1);
  ASSERT_EQ(st1.st_uid, 1234u);
  ASSERT_EQ(st1.st_gid, 5678u);
  ASSERT_EQ(st1.st_size, (off_t)data.length());

  /* close (publish) */
  o2h->close(open1);

  /* verify attrs survived publish */
  auto sr2 = o2h->stat();
  ASSERT_EQ(std::get<0>(sr2), 0);
  auto& st2 = std::get<1>(sr2);
  ASSERT_EQ(st2.st_uid, 1234u);
  ASSERT_EQ(st2.st_gid, 5678u);
  ASSERT_EQ(st2.st_size, (off_t)data.length());
}

TEST(OPEN2, SETATTR_REOPEN)
{
  /* reopen a published object and verify attrs */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("attrtest1");
  ASSERT_EQ(get<0>(lfr), 0);

  /* reopen for read — object was published by SETATTR1 */
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  /* read back data */
  auto rdr = o2h->read(open1, 0, 17);
  ASSERT_EQ(std::get<0>(rdr), 0);
  ASSERT_EQ(std::get<1>(rdr), "attr test content");

  /* verify attrs survived reopen */
  auto sr = o2h->stat();
  ASSERT_EQ(std::get<0>(sr), 0);
  auto& st = std::get<1>(sr);
  ASSERT_EQ(st.st_uid, 1234u);
  ASSERT_EQ(st.st_gid, 5678u);
  ASSERT_EQ(st.st_size, 17);

  o2h->close(open1);
}

TEST(OPEN2, XATTR_SET_GET)
{
  /* set user metadata xattr during open session, read it back */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("xattrtest1");
  ASSERT_EQ(get<0>(lfr), 0);

  auto ofr = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  std::string data{"xattr test"};
  auto nbw = o2h->write(open1, data, 0, data.length());
  ASSERT_EQ(std::get<0>(nbw), 0);

  /* set a user metadata xattr */
  int rc = o2h->setxattr("color", "blue");
  ASSERT_EQ(rc, 0);

  rc = o2h->setxattr("shape", "round");
  ASSERT_EQ(rc, 0);

  /* read them back during open session */
  auto gr1 = o2h->getxattr("color");
  ASSERT_EQ(std::get<0>(gr1), 0);
  ASSERT_EQ(std::get<1>(gr1), "blue");

  auto gr2 = o2h->getxattr("shape");
  ASSERT_EQ(std::get<0>(gr2), 0);
  ASSERT_EQ(std::get<1>(gr2), "round");

  o2h->close(open1);
}

TEST(OPEN2, XATTR_PERSIST)
{
  /* verify xattrs survive publish + reopen */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("xattrtest1");
  ASSERT_EQ(get<0>(lfr), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  /* xattrs should have survived publish */
  auto gr1 = o2h->getxattr("color");
  ASSERT_EQ(std::get<0>(gr1), 0);
  ASSERT_EQ(std::get<1>(gr1), "blue");

  auto gr2 = o2h->getxattr("shape");
  ASSERT_EQ(std::get<0>(gr2), 0);
  ASSERT_EQ(std::get<1>(gr2), "round");

  /* data should survive too */
  auto rdr = o2h->read(open1, 0, 10);
  ASSERT_EQ(std::get<0>(rdr), 0);
  ASSERT_EQ(std::get<1>(rdr), "xattr test");

  o2h->close(open1);
}

TEST(OPEN2, XATTR_LIST)
{
  /* list xattrs on an open handle */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("xattrtest1");
  ASSERT_EQ(get<0>(lfr), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  auto lsr = o2h->lsxattrs();
  ASSERT_EQ(std::get<0>(lsr), 0);

  auto& keys = std::get<1>(lsr);
  /* should contain at least our two user metadata keys */
  bool found_color = false, found_shape = false;
  for (const auto& k : keys) {
    if (k == "color") { found_color = true; }
    if (k == "shape") { found_shape = true; }
  }
  ASSERT_TRUE(found_color);
  ASSERT_TRUE(found_shape);

  o2h->close(open1);
}

TEST(OPEN2, XATTR_REMOVE)
{
  /* remove an xattr, verify it's gone */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("xattrtest1");
  ASSERT_EQ(get<0>(lfr), 0);

  auto ofr = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  /* color should still exist */
  auto gr1 = o2h->getxattr("color");
  ASSERT_EQ(std::get<0>(gr1), 0);
  ASSERT_EQ(std::get<1>(gr1), "blue");

  /* remove it */
  int rc = o2h->rmxattr("color");
  ASSERT_EQ(rc, 0);

  /* verify it's gone */
  auto gr2 = o2h->getxattr("color");
  /* should return empty or error */
  ASSERT_TRUE(std::get<1>(gr2).empty());

  /* shape should still exist */
  auto gr3 = o2h->getxattr("shape");
  ASSERT_EQ(std::get<0>(gr3), 0);
  ASSERT_EQ(std::get<1>(gr3), "round");

  o2h->close(open1);
}

TEST(OPEN2, ACL_AFTER_PUBLISH)
{
  /* verify ACL survives publish: create, write, close, re-lookup */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  auto lfr = o2h->lookup("acltest1");
  ASSERT_EQ(get<0>(lfr), 0);

  auto ofr = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  std::string data{"acl test data"};
  auto nbw = o2h->write(open1, data, 0, data.length());
  ASSERT_EQ(std::get<0>(nbw), 0);

  o2h->close(open1);
  /* open1 published the shadow — object now exists on disk */
  o2h.reset();

  /* fresh lookup — exercises stat_leaf which reads ACL xattr;
   * would crash if ACL is missing (the original bug) */
  struct rgw_file_handle* relookup_fh{nullptr};
  int rc = rgw_lookup(fs, bucket_fh, "acltest1", &relookup_fh,
		      nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(rc, 0);
  ASSERT_NE(relookup_fh, nullptr);

  /* getattr should succeed and return valid size */
  struct stat st;
  rc = rgw_getattr(fs, relookup_fh, &st, RGW_GETATTR_FLAG_NONE);
  ASSERT_EQ(rc, 0);
  ASSERT_EQ(st.st_size, (off_t)data.length());

  rgw_fh_rele(fs, relookup_fh, RGW_FH_RELE_FLAG_NONE);
}

TEST(OPEN2, RENDEZVOUS1)
{
  /* write open rendezvous with active stream (published) */
  /* write open rendezvous with active stream (unpublished/FLAG_CREATE) */
}

TEST(OPEN2, TRUNC1)
{
  /* open handles follow trunc */
}

TEST(OPEN2, EXCL)
{
  /* open O_EXCL cases */
}

TEST(OPEN2, UNLINK1)
{
  /* read and write opens after unlink fail */
  /* read-open or write-open active streams survive unlink, but */
  /* write-close after unlink discards changes */
}


/* END ALL TESTS */

TEST(OPEN2, DELETE_BUCKET) {
  if (do_delete) {
    int ret = rgw_unlink(fs, fs->root_fh, bucket_name.c_str(),
			 RGW_UNLINK_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(OPEN2, CLEANUP) {
  int ret;
  if (object_fh) {
    ret = rgw_fh_rele(fs, object_fh, RGW_FH_RELE_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
  ret = rgw_fh_rele(fs, bucket_fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs, RGW_UMOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, SHUTDOWN) {
  librgw_shutdown(rgw);
}

int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  char* v = getenv("AWS_ACCESS_KEY_ID");
  if (v) {
    access_key = v;
  }

  v = getenv("AWS_SECRET_ACCESS_KEY");
  if (v) {
    secret_key = v;
  }

  string val;

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_witharg(args, arg_iter, &val, "--access",
			      (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--secret",
				     (char*) nullptr)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--userid",
				     (char*) nullptr)) {
      userid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      owner_uid = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--gid",
				     (char*) nullptr)) {
      owner_gid = std::stoi(val);
    } else if (ceph_argparse_flag(args, arg_iter, "--verify",
					    (char*) nullptr)) {
      do_verify = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
					    (char*) nullptr)) {
      do_create = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete",
					    (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--large",
					    (char*) nullptr)) {
      do_large = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--hexdump",
					    (char*) nullptr)) {
      do_hexdump = true;
    } else {
      ++arg_iter;
    }
  }

  /* don't accidentally run as anonymous */
  if ((access_key == "") ||
      (secret_key == "")) {
    std::cout << argv[0] << " no AWS credentials, exiting" << std::endl;
    return EPERM;
  }

  saved_args.argc = argc;
  saved_args.argv = argv;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
