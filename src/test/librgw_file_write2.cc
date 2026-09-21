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
#include <filesystem>
#include <sys/xattr.h>
#include <thread>
#include <chrono>
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
#include "rgw_lib.h" /* driver hints */
#include "rgw/rgw_file_int.h" /* the private view: refcounts, handles */

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {
  librgw_t rgw_h = nullptr;
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

  namespace sf = std::filesystem;

  /* the nsfs/posix drivers lay the namespace out on a filesystem, so
   * shadow state can be asserted directly rather than inferred.  the
   * root is the one the driver itself used (rgw_sal_nsfs.cc reads the
   * same key), so no path has to be passed in */
  sf::path nsfs_base() {
    return sf::path{g_conf().get_val<std::string>("rgw_nsfs_base_path")};
  }

  sf::path published_path(const std::string& obj) {
    return nsfs_base() / bucket_name / obj;
  }

  sf::path shadow_path(const std::string& obj) {
    /* the shadow lives beside its object, so a hierarchical key puts it
     * in <dirs>/.shadow/<leaf> rather than at the bucket root */
    auto pos = obj.rfind('/');
    if (pos == std::string::npos) {
      return nsfs_base() / bucket_name / ".shadow" / obj;
    }
    return nsfs_base() / bucket_name / obj.substr(0, pos)
	 / ".shadow" / obj.substr(pos + 1);
  }

  bool have_fs_layout() {
    std::error_code ec;
    return sf::is_directory(nsfs_base() / bucket_name, ec);
  }

  /* Tests own their objects, and clean at the start rather than the
   * end:  a failing run leaves its state on disk to be looked at, and
   * the next run is still repeatable. */
  void reset_object_at(struct rgw_file_handle* parent_fh,
		       const std::string& leaf,
		       const std::string& rel_path) {
    (void) rgw_unlink(fs, parent_fh, leaf.c_str(), RGW_UNLINK_FLAG_NONE);

    if (! have_fs_layout()) {
      return;
    }

    std::error_code ec;
    if (sf::exists(shadow_path(rel_path), ec)) {
      std::cerr << "WARNING: stale shadow for " << rel_path
		<< ", removed by reset_object" << std::endl;
      sf::remove(shadow_path(rel_path), ec);
    }
  }

  void reset_object(const std::string& name) {
    (void) rgw_unlink(fs, bucket_fh, name.c_str(), RGW_UNLINK_FLAG_NONE);

    if (! have_fs_layout()) {
      return;
    }

    /* a shadow which survives the unlink was leaked by an earlier run.
     * clear it so the suite stays repeatable, but say so--this is how
     * that class of bug otherwise stays invisible */
    std::error_code ec;
    if (sf::exists(shadow_path(name), ec)) {
      std::cerr << "WARNING: stale shadow for " << name
		<< ", removed by reset_object" << std::endl;
      sf::remove(shadow_path(name), ec);
    }
  }

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

    rgw::RGWFileHandle* rgw_fh_of() { return rgw::get_rgwfh(object_fh); }

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
  int ret = librgw_create(&rgw_h, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw_h, nullptr);
}

TEST(OPEN2, MOUNT) {
  int ret = rgw_mount2(rgw_h, userid.c_str(), access_key.c_str(),
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
    /* --create is meant to be safe on every run, not only the first */
    ASSERT_TRUE((ret == 0) || (ret == -EEXIST)) << "ret=" << ret;
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
  /* stateless (NFSv3) open of a not-yet-existing object:  a mode and
   * RGW_OPEN_FLAG_CREATE are required--opening O_RDONLY without
   * CREATE has nothing to bind to and returns -ENOENT */
  int ret = rgw_open(fs, object_fh, O_RDWR,
		     RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE);
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

  reset_object("netbird2");
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

  reset_object("tray1");
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

  reset_object("accthis1");
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

  reset_object("attrtest1");
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

  reset_object("xattrtest1");
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

  reset_object("acltest1");
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

TEST(OPEN2, ETAG_AFTER_PUBLISH)
{
  /* verify etag is computed at publish time */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("etagtest1");
  auto lfr = o2h->lookup("etagtest1");
  ASSERT_EQ(get<0>(lfr), 0);

  auto ofr = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  auto open1 = std::get<1>(ofr);
  ASSERT_NE(open1, nullptr);

  std::string data{"etag test content"};
  auto nbw = o2h->write(open1, data, 0, data.length());
  ASSERT_EQ(std::get<0>(nbw), 0);

  /* etag should not exist before publish */
  auto gr0 = o2h->getxattr("user.rgw.etag" /* RGW_ATTR_ETAG */);

  o2h->close(open1);
  /* published — etag should now be stamped */

  /* reopen to read the etag */
  auto ofr2 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  auto open2 = std::get<1>(ofr2);
  ASSERT_NE(open2, nullptr);

  auto gr1 = o2h->getxattr("user.rgw.etag" /* RGW_ATTR_ETAG */);
  ASSERT_EQ(std::get<0>(gr1), 0);

  auto etag = std::get<1>(gr1);
  /* trim trailing null if present (xattr convention) */
  if (!etag.empty() && etag.back() == '\0') {
    etag.pop_back();
  }
  /* etag should be a 32-char hex MD5 digest */
  ASSERT_EQ(etag.length(), 32u);
  for (char c : etag) {
    ASSERT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
  }

  o2h->close(open2);
}

TEST(OPEN2, RENDEZVOUS1)
{
  /* several write opens share one shadow, and publish happens once,
   * at last-writer close--both against an object being created and
   * against one which is already published */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("rendez1");
  auto lfr = o2h->lookup("rendez1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};
  std::string d4{"DDDD"};

  /* unpublished:  the object is being created */
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);

  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);

  ASSERT_EQ(get<0>(o2h->write(w0, a4, 0, a4.length())), 0);
  ASSERT_EQ(get<0>(o2h->write(w1, b4, a4.length(), b4.length())), 0);

  /* one shadow:  each writer sees the other's bytes */
  auto rdr = o2h->read(w0, 0, a4.length() + b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4 + b4);

  /* returning one of two write opens does not publish */
  ASSERT_EQ(o2h->close(w0), 0);
  /* the etag is stamped at publish, so it must not have moved yet */
  auto etag_mid = get<1>(o2h->getxattr("user.rgw.etag" /* RGW_ATTR_ETAG */));

  /* last writer close publishes */
  ASSERT_EQ(o2h->close(w1), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);

  auto gr1 = o2h->getxattr("user.rgw.etag");
  ASSERT_EQ(get<0>(gr1), 0);
  ASSERT_FALSE(get<1>(gr1).empty());
  ASSERT_NE(get<1>(gr1), etag_mid);

  rdr = o2h->read(r1, 0, a4.length() + b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4 + b4);
  ASSERT_EQ(o2h->close(r1), 0);

  /* published:  the first write open re-forks the shadow, the second
   * rendezvouses with it */
  auto ofw2 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw2), 0);
  auto w2 = get<1>(ofw2);

  auto ofw3 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw3), 0);
  auto w3 = get<1>(ofw3);

  ASSERT_EQ(get<0>(o2h->write(w3, d4, 0, d4.length())), 0);

  rdr = o2h->read(w2, 0, a4.length() + b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), d4 + b4);

  ASSERT_EQ(o2h->close(w2), 0);
  ASSERT_EQ(o2h->close(w3), 0);
}

TEST(OPEN2, TRUNC1)
{
  /* O_TRUNC truncates the shadow in place, so clients already
   * rendezvoused on it follow the truncation rather than being
   * stranded on an orphaned view */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("trunc1");
  auto lfr = o2h->lookup("trunc1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a8{"AAAABBBB"};
  std::string c2{"CC"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a8, 0, a8.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  /* reader attaches to the published object */
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);

  auto rdr = o2h->read(r1, 0, a8.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a8);

  /* a writer forks the shadow and truncates it */
  auto ofw1 = o2h->open(O_RDWR|O_TRUNC, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);

  /* the pre-existing reader follows the truncation */
  rdr = o2h->read(r1, 0, a8.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_TRUE(get<1>(rdr).empty());

  ASSERT_EQ(get<0>(o2h->write(w1, c2, 0, c2.length())), 0);

  rdr = o2h->read(r1, 0, c2.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), c2);

  ASSERT_EQ(o2h->close(w1), 0);
  ASSERT_EQ(o2h->close(r1), 0);

  /* the published object is the truncated one */
  auto ofr2 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr2), 0);
  auto r2 = get<1>(ofr2);
  rdr = o2h->read(r2, 0, a8.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), c2);
  ASSERT_EQ(o2h->close(r2), 0);
}

TEST(OPEN2, EXCL)
{
  /* open O_EXCL cases */
}

TEST(OPEN2, UNLINK1)
{
  /* posix unlink:  open descriptors keep operating on the doomed
   * shadow, which is never published, and whose storage is reclaimed
   * when the last open is returned */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("unlink1");
  auto lfr = o2h->lookup("unlink1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};
  std::string c4{"CCCC"};

  /* create and publish "AAAA" */
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a4, 0, a4.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  /* reopen (forks a shadow) and extend */
  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);
  ASSERT_EQ(get<0>(o2h->write(w1, b4, a4.length(), b4.length())), 0);

  int ret = rgw_unlink(fs, bucket_fh, "unlink1", RGW_UNLINK_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  /* the open survives the unlink and continues to work */
  auto nbw = o2h->write(w1, c4, a4.length() + b4.length(), c4.length());
  ASSERT_EQ(get<0>(nbw), 0);
  ASSERT_EQ(get<1>(nbw), c4.length());

  auto rdr = o2h->read(w1, 0, 12);
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4 + b4 + c4);

  /* last close discards;  nothing is published */
  ASSERT_EQ(o2h->close(w1), 0);

  struct rgw_file_handle* fh{nullptr};
  ret = rgw_lookup(fs, bucket_fh, "unlink1", &fh, nullptr, 0,
		   RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, -ENOENT);
}


TEST(OPEN2, READER_FOLLOWS_WRITER)
{
  /* a reader binds to the published object;  when a writer forks the
   * shadow, the reader follows it on its existing open, without
   * reopening--no autonomous history for whoever got there first */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("follow1");
  auto lfr = o2h->lookup("follow1");
  ASSERT_EQ(get<0>(lfr), 0);
  ASSERT_NE(get<1>(lfr), nullptr);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};

  /* create and publish "AAAA" */
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  auto nbw = o2h->write(w0, a4, 0, a4.length());
  ASSERT_EQ(get<0>(nbw), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  /* reader attaches;  no shadow exists, so it binds the published
   * object and must not construct one */
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);

  auto rdr = o2h->read(r1, 0, a4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4);

  /* a writer arrives on the same handle and forks the shadow */
  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);

  nbw = o2h->write(w1, b4, 0, b4.length());
  ASSERT_EQ(get<0>(nbw), 0);
  ASSERT_EQ(get<1>(nbw), b4.length());

  /* the pre-existing reader sees the write */
  rdr = o2h->read(r1, 0, b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), b4);

  ASSERT_EQ(o2h->close(w1), 0);
  ASSERT_EQ(o2h->close(r1), 0);
}

TEST(OPEN2, READER_ACROSS_PUBLISH)
{
  /* a reader holds one open across publish and a subsequent re-fork */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("acrosspub1");
  auto lfr = o2h->lookup("acrosspub1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};
  std::string c4{"CCCC"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a4, 0, a4.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);

  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);
  ASSERT_EQ(get<0>(o2h->write(w1, b4, 0, b4.length())), 0);

  /* last writer close publishes;  the reader stays open */
  ASSERT_EQ(o2h->close(w1), 0);

  auto rdr = o2h->read(r1, 0, b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), b4);

  /* a new writer re-forks from the published object */
  auto ofw2 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw2), 0);
  auto w2 = get<1>(ofw2);
  ASSERT_EQ(get<0>(o2h->write(w2, c4, 0, c4.length())), 0);

  rdr = o2h->read(r1, 0, c4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), c4);

  ASSERT_EQ(o2h->close(w2), 0);
  ASSERT_EQ(o2h->close(r1), 0);
}

TEST(OPEN2, V3_POSITIONAL)
{
  /* stateless (NFSv3) open:  no open token is returned to the caller,
   * and writes are positional--the legacy write cycle rejected any
   * non-contiguous write position */
  reset_object("v3pos1");

  struct rgw_file_handle* fh{nullptr};
  int ret = rgw_lookup(fs, bucket_fh, "v3pos1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_CREATE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fh, nullptr);

  ret = rgw_open(fs, fh, O_RDWR,
		 RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(ret, 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};
  size_t nbytes{0};

  ret = rgw_write(fs, fh, 0, a4.length(), &nbytes, (void*) a4.c_str(),
		  RGW_OPEN_FLAG_V3);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(nbytes, a4.length());

  ret = rgw_write(fs, fh, 100, b4.length(), &nbytes, (void*) b4.c_str(),
		  RGW_OPEN_FLAG_V3);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(nbytes, b4.length());

  char buf[8];
  size_t nread{0};
  memset(buf, 0, sizeof(buf));
  ret = rgw_read(fs, fh, 100, b4.length(), &nread, buf,
		 RGW_READ_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(nread, b4.length());
  ASSERT_EQ(std::string(buf, b4.length()), b4);

  /* size is taken from the shadow while the open is live */
  struct stat st;
  ret = rgw_getattr(fs, fh, &st, RGW_GETATTR_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(st.st_size, 104);

  ret = rgw_close(fs, fh, RGW_CLOSE_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);
}

TEST(OPEN2, V3_UPGRADE)
{
  /* a stateless read open is upgraded in place when a write arrives */
  reset_object("v3up1");

  struct rgw_file_handle* fh{nullptr};
  int ret = rgw_lookup(fs, bucket_fh, "v3up1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_CREATE);
  ASSERT_EQ(ret, 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};
  size_t nbytes{0};

  /* create and publish "AAAA" */
  ret = rgw_open(fs, fh, O_RDWR,
		 RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(ret, 0);
  ret = rgw_write(fs, fh, 0, a4.length(), &nbytes, (void*) a4.c_str(),
		  RGW_OPEN_FLAG_V3);
  ASSERT_EQ(ret, 0);
  ret = rgw_close(fs, fh, RGW_CLOSE_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  /* read-only stateless open */
  ret = rgw_open(fs, fh, O_RDONLY, RGW_OPEN_FLAG_V3);
  ASSERT_EQ(ret, 0);

  char buf[8];
  size_t nread{0};
  memset(buf, 0, sizeof(buf));
  ret = rgw_read(fs, fh, 0, a4.length(), &nread, buf, RGW_READ_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(std::string(buf, a4.length()), a4);

  /* write upgrades the open in place */
  ret = rgw_write(fs, fh, 0, b4.length(), &nbytes, (void*) b4.c_str(),
		  RGW_OPEN_FLAG_V3);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(nbytes, b4.length());

  memset(buf, 0, sizeof(buf));
  ret = rgw_read(fs, fh, 0, b4.length(), &nread, buf, RGW_READ_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(std::string(buf, b4.length()), b4);

  ret = rgw_close(fs, fh, RGW_CLOSE_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);
}

TEST(OPEN2, SETATTR_SIZE)
{
  /* NFS SETATTR with a size is a data operation:  it truncates (or
   * extends) the object, and it is complete when it returns--there is
   * no open bracketing it */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("size1");
  auto lfr = o2h->lookup("size1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a8{"AAAABBBB"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a8, 0, a8.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  auto ofr0 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr0), 0);
  auto r0 = get<1>(ofr0);
  auto etag0 = get<1>(o2h->getxattr("user.rgw.etag"));
  ASSERT_FALSE(etag0.empty());
  ASSERT_EQ(o2h->close(r0), 0);

  /* truncate with no open held */
  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_size = 4;
  ASSERT_EQ(o2h->setattr(&st, RGW_SETATTR_SIZE), 0);

  auto sr = o2h->stat();
  ASSERT_EQ(get<0>(sr), 0);
  ASSERT_EQ(get<1>(sr).st_size, 4);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);
  auto rdr = o2h->read(r1, 0, a8.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), std::string("AAAA"));

  /* the etag tracks the new content, and was not clobbered with the
   * handle's cached (pre-truncate) value */
  auto etag1 = get<1>(o2h->getxattr("user.rgw.etag"));
  ASSERT_FALSE(etag1.empty());
  ASSERT_NE(etag1, etag0);
  ASSERT_EQ(o2h->close(r1), 0);

  /* extend */
  memset(&st, 0, sizeof(st));
  st.st_size = 12;
  ASSERT_EQ(o2h->setattr(&st, RGW_SETATTR_SIZE), 0);

  auto ofr2 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr2), 0);
  auto r2 = get<1>(ofr2);
  rdr = o2h->read(r2, 0, 12);
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr).length(), 12u);
  ASSERT_EQ(get<1>(rdr).substr(0, 4), std::string("AAAA"));
  ASSERT_EQ(o2h->close(r2), 0);

  /* truncate while a writer holds the shadow:  it applies to the
   * shared view, and the writer's close publishes it */
  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);

  memset(&st, 0, sizeof(st));
  st.st_size = 2;
  ASSERT_EQ(o2h->setattr(&st, RGW_SETATTR_SIZE), 0);

  rdr = o2h->read(w1, 0, 12);
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), std::string("AA"));

  ASSERT_EQ(o2h->close(w1), 0);

  auto ofr3 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr3), 0);
  auto r3 = get<1>(ofr3);
  rdr = o2h->read(r3, 0, 12);
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), std::string("AA"));
  ASSERT_EQ(o2h->close(r3), 0);
}

TEST(OPEN2, COMMIT1)
{
  /* NFS COMMIT is fsync, not publish:  it makes written data durable
   * without finalizing the shadow into the S3 namespace, and it must
   * succeed however many times a client sends it */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_NE(o2h.get(), nullptr);

  reset_object("commit1");
  auto lfr = o2h->lookup("commit1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a4, 0, a4.length())), 0);

  int ret = rgw_commit(fs, o2h->object_fh, 0, a4.length(),
		       RGW_FSYNC_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  /* the data survives the commit, and is still not published */
  auto rdr = o2h->read(w0, 0, a4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4);

  /* the etag is stamped at publish, so it must not have moved yet */
  auto etag_mid = get<1>(o2h->getxattr("user.rgw.etag" /* RGW_ATTR_ETAG */));

  /* repeated COMMITs succeed */
  ASSERT_EQ(get<0>(o2h->write(w0, b4, a4.length(), b4.length())), 0);
  ret = rgw_commit(fs, o2h->object_fh, 0, 0, RGW_FSYNC_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ret = rgw_commit(fs, o2h->object_fh, 0, 0, RGW_FSYNC_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  /* fsync takes the same path */
  ret = rgw_fsync(fs, o2h->object_fh, RGW_FSYNC_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  ASSERT_EQ(o2h->close(w0), 0);

  /* close published */
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);

  auto gr1 = o2h->getxattr("user.rgw.etag");
  ASSERT_EQ(get<0>(gr1), 0);
  ASSERT_FALSE(get<1>(gr1).empty());
  ASSERT_NE(get<1>(gr1), etag_mid);

  rdr = o2h->read(r1, 0, a4.length() + b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4 + b4);
  ASSERT_EQ(o2h->close(r1), 0);

  /* a COMMIT with no open attached still succeeds */
  ret = rgw_commit(fs, o2h->object_fh, 0, 0, RGW_FSYNC_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  /* as does one on a directory */
  ret = rgw_commit(fs, bucket_fh, 0, 0, RGW_FSYNC_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(OPEN2, SHADOW_NOT_CREATED_BY_READER)
{
  /* direct evidence for the invariant the other tests only imply:  a
   * read open binds the published object and leaves no shadow behind */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  reset_object("noshadow1");
  auto lfr = o2h->lookup("noshadow1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a4{"AAAA"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  /* while the write open is held the object lives in the shadow */
  ASSERT_TRUE(sf::exists(shadow_path("noshadow1")));
  ASSERT_EQ(get<0>(o2h->write(w0, a4, 0, a4.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  /* published:  shadow gone, object present */
  ASSERT_FALSE(sf::exists(shadow_path("noshadow1")));
  ASSERT_TRUE(sf::exists(published_path("noshadow1")));

  /* a reader must not fork one */
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);
  auto rdr = o2h->read(r1, 0, a4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4);
  ASSERT_FALSE(sf::exists(shadow_path("noshadow1")));
  ASSERT_EQ(o2h->close(r1), 0);
  ASSERT_FALSE(sf::exists(shadow_path("noshadow1")));
}

TEST(OPEN2, RESUME_EXISTING_SHADOW)
{
  /* the rendezvous arm of get_fsio_handle:  a shadow which exists with
   * no handle attached is joined, not cloned over.  reachable in
   * production only across instances or after a crash, which is also
   * where the exclusive-fork EEXIST path lands */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  reset_object("resume1");
  auto lfr = o2h->lookup("resume1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string published{"PPPP"};
  std::string staged{"SSSSSSSS"};

  /* publish "PPPP" and drop all opens */
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, published, 0, published.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);
  ASSERT_FALSE(sf::exists(shadow_path("resume1")));

  /* stage a shadow out of band, as another instance would have */
  {
    std::error_code ec;
    sf::create_directories(shadow_path("resume1").parent_path(), ec);
    int sfd = ::open(shadow_path("resume1").c_str(),
		     O_RDWR | O_CREAT | O_EXCL, 0644);
    ASSERT_GE(sfd, 0);
    ASSERT_EQ(::write(sfd, staged.c_str(), staged.length()),
	      (ssize_t) staged.length());
    ::close(sfd);
  }

  /* opening must join that shadow, not clone the published object */
  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);

  auto rdr = o2h->read(w1, 0, staged.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), staged);

  /* and closing publishes what was staged */
  ASSERT_EQ(o2h->close(w1), 0);
  ASSERT_FALSE(sf::exists(shadow_path("resume1")));

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);
  rdr = o2h->read(r1, 0, staged.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), staged);
  ASSERT_EQ(o2h->close(r1), 0);
}

TEST(OPEN2, UNLINK_LEAVES_NO_SHADOW)
{
  /* direct evidence for UNLINK1:  the doomed shadow loses its name at
   * unlink and its storage at last close */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  reset_object("unlink2");
  auto lfr = o2h->lookup("unlink2");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a4{"AAAA"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a4, 0, a4.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);
  ASSERT_TRUE(sf::exists(shadow_path("unlink2")));

  ASSERT_EQ(rgw_unlink(fs, bucket_fh, "unlink2", RGW_UNLINK_FLAG_NONE), 0);

  /* name is gone immediately, both in the namespace and in .shadow */
  ASSERT_FALSE(sf::exists(published_path("unlink2")));
  ASSERT_FALSE(sf::exists(shadow_path("unlink2")));

  /* but the open still works on it */
  auto rdr = o2h->read(w1, 0, a4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4);

  ASSERT_EQ(o2h->close(w1), 0);
  ASSERT_FALSE(sf::exists(published_path("unlink2")));
  ASSERT_FALSE(sf::exists(shadow_path("unlink2")));
}

TEST(OPEN2, UNLINK_WITH_READER_ONLY)
{
  /* unlink while only a read open is held:  the handle is bound to the
   * published object, not to a shadow, so discard() has no shadow to
   * drop--but the open must keep working and nothing may be published */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  reset_object("rdonly1");
  auto lfr = o2h->lookup("rdonly1");
  ASSERT_EQ(get<0>(lfr), 0);

  std::string a4{"AAAA"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a4, 0, a4.length())), 0);
  ASSERT_EQ(o2h->close(w0), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);
  ASSERT_FALSE(sf::exists(shadow_path("rdonly1")));

  ASSERT_EQ(rgw_unlink(fs, bucket_fh, "rdonly1", RGW_UNLINK_FLAG_NONE), 0);
  ASSERT_FALSE(sf::exists(published_path("rdonly1")));

  /* the reader's fd holds the unlinked inode */
  auto rdr = o2h->read(r1, 0, a4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a4);

  ASSERT_EQ(o2h->close(r1), 0);
  ASSERT_FALSE(sf::exists(published_path("rdonly1")));
  ASSERT_FALSE(sf::exists(shadow_path("rdonly1")));
}

TEST(OPEN2, STATELESS_IDLE_FINALIZE)
{
  /* the idle finalizer publishes an unclosed stateless writer without
   * closing it, and a write which follows must re-fork the shadow
   * rather than mutate the object it just published */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  g_conf().set_val("rgw_nfs_stateless_finalize_secs", "1");
  g_conf().apply_changes(nullptr);

  reset_object("idle1");

  struct rgw_file_handle* fh{nullptr};
  int ret = rgw_lookup(fs, bucket_fh, "idle1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_CREATE);
  ASSERT_EQ(ret, 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};
  size_t nbytes{0};

  ret = rgw_open(fs, fh, O_RDWR, RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(ret, 0);
  ret = rgw_write(fs, fh, 0, a4.length(), &nbytes, (void*) a4.c_str(),
		  RGW_OPEN_FLAG_V3);
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(sf::exists(shadow_path("idle1")));
  ASSERT_FALSE(sf::exists(published_path("idle1")));

  /* no close;  the idle timer finalizes */
  std::this_thread::sleep_for(std::chrono::seconds(4));

  ASSERT_TRUE(sf::exists(published_path("idle1")));
  ASSERT_FALSE(sf::exists(shadow_path("idle1")));
  ASSERT_EQ(sf::file_size(published_path("idle1")), a4.length());

  /* the open is still live:  a further write must re-fork, not mutate
   * the published object */
  ret = rgw_write(fs, fh, a4.length(), b4.length(), &nbytes,
		  (void*) b4.c_str(), RGW_OPEN_FLAG_V3);
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(sf::exists(shadow_path("idle1")));
  ASSERT_EQ(sf::file_size(published_path("idle1")), a4.length());

  ret = rgw_close(fs, fh, RGW_CLOSE_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  ASSERT_FALSE(sf::exists(shadow_path("idle1")));
  ASSERT_EQ(sf::file_size(published_path("idle1")),
	    a4.length() + b4.length());

  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);

  g_conf().set_val("rgw_nfs_stateless_finalize_secs", "300");
  g_conf().apply_changes(nullptr);
}

TEST(OPEN2, FORK_RACE_JOINS_WINNER)
{
  /* two instances forking a shadow for the same object:  the fork is
   * exclusive, so the loser gets EEXIST and joins the winner's shadow
   * rather than cloning over it.  the race is injected through a driver
   * hint, so it is deterministic from a single process */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  auto* driver = rgw::g_rgwlib->get_driver();
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");
  std::map<std::string, std::string> out;

  int ret = driver->driver_hint(&dp, "inject-fork-race",
				{{"enable", "true"}}, &out);
  if (ret == -ENOTSUP) {
    GTEST_SKIP() << "driver does not implement inject-fork-race";
  }
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(out["enabled"], "true");

  std::string published{"PPPP"};
  std::string won{"RACEWON"};

  /* (a) creating an object:  the racers take the create arm */
  {
    std::unique_ptr<Open2Helper> o2h =
	std::make_unique<Open2Helper>(fs, bucket_fh);
    reset_object("racenew1");
    auto lfr = o2h->lookup("racenew1");
    ASSERT_EQ(get<0>(lfr), 0);

    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    auto w0 = get<1>(ofw);

    auto rdr = o2h->read(w0, 0, won.length());
    ASSERT_EQ(get<0>(rdr), 0);
    ASSERT_EQ(get<1>(rdr), won);

    ASSERT_TRUE(sf::exists(shadow_path("racenew1")));
    ASSERT_EQ(o2h->close(w0), 0);
    ASSERT_FALSE(sf::exists(shadow_path("racenew1")));
    ASSERT_EQ(sf::file_size(published_path("racenew1")), won.length());
  }

  /* (b) forking an existing object:  the racers take the COW clone arm,
   * which is the one clone_file's exclusive create guards */
  {
    ASSERT_EQ(driver->driver_hint(&dp, "inject-fork-race",
				  {{"enable", "false"}}), 0);

    std::unique_ptr<Open2Helper> o2h =
	std::make_unique<Open2Helper>(fs, bucket_fh);
    reset_object("race1");
    auto lfr = o2h->lookup("race1");
    ASSERT_EQ(get<0>(lfr), 0);

    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    auto w0 = get<1>(ofw);
    ASSERT_EQ(get<0>(o2h->write(w0, published, 0, published.length())), 0);
    ASSERT_EQ(o2h->close(w0), 0);
    ASSERT_EQ(sf::file_size(published_path("race1")), published.length());

    ASSERT_EQ(driver->driver_hint(&dp, "inject-fork-race",
				  {{"enable", "true"}}), 0);

    /* we lose the fork:  the content is the winner's shadow, not a
     * clone of the published object */
    auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
    ASSERT_EQ(get<0>(ofw1), 0);
    auto w1 = get<1>(ofw1);

    auto rdr = o2h->read(w1, 0, won.length());
    ASSERT_EQ(get<0>(rdr), 0);
    ASSERT_EQ(get<1>(rdr), won);

    ASSERT_TRUE(sf::exists(shadow_path("race1")));
    ASSERT_EQ(o2h->close(w1), 0);
    ASSERT_FALSE(sf::exists(shadow_path("race1")));
    ASSERT_EQ(sf::file_size(published_path("race1")), won.length());
  }

  ASSERT_EQ(driver->driver_hint(&dp, "inject-fork-race",
				{{"enable", "false"}}), 0);
}

TEST(OPEN2, GUARDS_ON_PUBLISHED_BINDING)
{
  /* the FSIO guards which refuse to mutate or publish a handle bound to
   * the published object.  every caller reclones first, so they are
   * unreachable through the API;  inject-skip-reclone makes reclone() a
   * no-op so the binding stays PUBLISHED and the guards are reached */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  auto* driver = rgw::g_rgwlib->get_driver();
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  int ret = driver->driver_hint(&dp, "inject-skip-reclone",
				{{"enable", "false"}});
  if (ret == -ENOTSUP) {
    GTEST_SKIP() << "driver does not implement inject-skip-reclone";
  }
  ASSERT_EQ(ret, 0);

  std::string a4{"AAAA"};

  /* ftruncate() refuses:  only a shadow is mutable */
  {
    std::unique_ptr<Open2Helper> o2h =
	std::make_unique<Open2Helper>(fs, bucket_fh);
    reset_object("guard1");
    ASSERT_EQ(get<0>(o2h->lookup("guard1")), 0);

    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);
    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

    /* a read open binds the published object and holds the handle, so
     * the write open below has something to (not) reclone */
    auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
    ASSERT_EQ(get<0>(ofr), 0);

    ASSERT_EQ(driver->driver_hint(&dp, "inject-skip-reclone",
				  {{"enable", "true"}}), 0);

    auto ofw1 = o2h->open(O_RDWR|O_TRUNC, RGW_OPEN_FLAG_NONE);
    ASSERT_EQ(get<0>(ofw1), -EPERM);

    ASSERT_EQ(driver->driver_hint(&dp, "inject-skip-reclone",
				  {{"enable", "false"}}), 0);
    ASSERT_EQ(o2h->close(get<1>(ofr)), 0);

    /* the published object was not truncated */
    ASSERT_EQ(sf::file_size(published_path("guard1")), a4.length());
  }

  /* publish() refuses, and close2 reports it rather than swallowing it
   * behind the release result */
  {
    std::unique_ptr<Open2Helper> o2h =
	std::make_unique<Open2Helper>(fs, bucket_fh);
    reset_object("guard2");
    ASSERT_EQ(get<0>(o2h->lookup("guard2")), 0);

    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);
    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

    auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
    ASSERT_EQ(get<0>(ofr), 0);

    ASSERT_EQ(driver->driver_hint(&dp, "inject-skip-reclone",
				  {{"enable", "true"}}), 0);

    auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
    ASSERT_EQ(get<0>(ofw1), 0);
    /* last writer close tries to publish a non-shadow binding */
    ASSERT_EQ(o2h->close(get<1>(ofw1)), -EINVAL);

    ASSERT_EQ(driver->driver_hint(&dp, "inject-skip-reclone",
				  {{"enable", "false"}}), 0);
    ASSERT_EQ(o2h->close(get<1>(ofr)), 0);

    ASSERT_EQ(sf::file_size(published_path("guard2")), a4.length());
    ASSERT_FALSE(sf::exists(shadow_path("guard2")));
  }
}

TEST(OPEN2, TRUNCATE_API)
{
  /* rgw_truncate is the path ganesha takes for a size change:  setattr2
   * calls it directly and then rgw_setattr for the remaining attrs, so
   * the RGW_SETATTR_SIZE route which SETATTR_SIZE covers is never used
   * by the FSAL */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  reset_object("trunc2");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("trunc2")), 0);

  std::string a8{"AAAABBBB"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a8, 0, a8.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  ASSERT_EQ(sf::file_size(published_path("trunc2")), a8.length());

  /* Look the object up again now that it exists.  stat_leaf stamps its
   * etag onto the handle, and that cached value is precisely what a
   * later setattr would write back over the truncated object's etag.
   * Without this step the handle holds no etag--reset_object unlinked
   * the object before the first lookup--and the assertion below could
   * not observe the regression it exists for. */
  ASSERT_EQ(get<0>(o2h->lookup("trunc2")), 0);

  auto ofr0 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr0), 0);
  auto etag_stale = get<1>(o2h->getxattr("user.rgw.etag"));
  ASSERT_FALSE(etag_stale.empty());
  ASSERT_EQ(o2h->close(get<1>(ofr0)), 0);

  /* shrink, with no open held */
  ASSERT_EQ(rgw_truncate(fs, o2h->object_fh, 4, RGW_TRUNCATE_FLAG_NONE), 0);
  ASSERT_EQ(sf::file_size(published_path("trunc2")), 4u);

  auto sr = o2h->stat();
  ASSERT_EQ(get<0>(sr), 0);
  ASSERT_EQ(get<1>(sr).st_size, 4);

  /* the setattr ganesha issues next, for the attrs which are not size */
  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_mode = 0644;
  ASSERT_EQ(o2h->setattr(&st, RGW_SETATTR_MODE), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);

  /* the object's etag is the truncated content's, not the one cached
   * before it */
  auto etag_now = get<1>(o2h->getxattr("user.rgw.etag"));
  ASSERT_FALSE(etag_now.empty());
  ASSERT_NE(etag_now, etag_stale);

  auto rdr = o2h->read(get<1>(ofr), 0, a8.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), std::string("AAAA"));
  ASSERT_EQ(o2h->close(get<1>(ofr)), 0);

  /* extend */
  ASSERT_EQ(rgw_truncate(fs, o2h->object_fh, 12, RGW_TRUNCATE_FLAG_NONE), 0);
  ASSERT_EQ(sf::file_size(published_path("trunc2")), 12u);

  /* while a writer holds the shadow it applies there, and the writer's
   * close publishes it */
  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  ASSERT_EQ(rgw_truncate(fs, o2h->object_fh, 2, RGW_TRUNCATE_FLAG_NONE), 0);
  ASSERT_EQ(sf::file_size(published_path("trunc2")), 12u);
  ASSERT_EQ(o2h->close(get<1>(ofw1)), 0);
  ASSERT_EQ(sf::file_size(published_path("trunc2")), 2u);

  /* a directory is not truncatable */
  ASSERT_EQ(rgw_truncate(fs, bucket_fh, 0, RGW_TRUNCATE_FLAG_NONE), -EISDIR);
}

TEST(OPEN2, LOOKUP_FINDS_UNPUBLISHED)
{
  /* An object which exists only as a shadow is part of the NFS view and
   * must be findable.  Resolving through a synthesized S3 GET could not
   * see it -- the shadow is by definition not in the S3 namespace -- so
   * this is also the assertion which proves the probe is being taken
   * rather than the fallback. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  reset_object("unpub1");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("unpub1")), 0);

  std::string a4{"AAAA"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);

  /* deliberately not closed: the object exists only in .shadow/ */
  ASSERT_TRUE(sf::exists(shadow_path("unpub1")));
  ASSERT_FALSE(sf::exists(published_path("unpub1")));

  /* a lookup without CREATE has to find it anyway */
  struct rgw_file_handle* fh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, bucket_fh, "unpub1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_NONE), 0);
  ASSERT_NE(fh, nullptr);

  struct stat st;
  ASSERT_EQ(rgw_getattr(fs, fh, &st, RGW_GETATTR_FLAG_NONE), 0);
  ASSERT_EQ(st.st_size, (off_t) a4.length());

  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

  /* and once published it is still found, by the same path */
  ASSERT_TRUE(sf::exists(published_path("unpub1")));
  ASSERT_EQ(rgw_lookup(fs, bucket_fh, "unpub1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_NONE), 0);
  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);
}

TEST(OPEN2, NESTED_OBJECT)
{
  /* Everything else in this suite uses objects at the root of a bucket,
   * where an object's parent is its bucket.  That is the only case in
   * which the parent's name and the bucket's name agree, and open2
   * relied on them agreeing -- a nested object looked its bucket up by
   * the name of its containing directory. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_uid = owner_uid;
  st.st_gid = owner_gid;
  st.st_mode = 755;

  struct rgw_file_handle* d1{nullptr};
  int ret = rgw_mkdir(fs, bucket_fh, "ndir1", &st, create_mask, &d1,
		      RGW_MKDIR_FLAG_NONE);
  ASSERT_TRUE((ret == 0) || (ret == -EEXIST)) << "ret=" << ret;
  if (! d1) {
    ASSERT_EQ(rgw_lookup(fs, bucket_fh, "ndir1", &d1, nullptr, 0,
			 RGW_LOOKUP_FLAG_NONE), 0);
  }

  struct rgw_file_handle* d2{nullptr};
  ret = rgw_mkdir(fs, d1, "ndir2", &st, create_mask, &d2,
		  RGW_MKDIR_FLAG_NONE);
  ASSERT_TRUE((ret == 0) || (ret == -EEXIST)) << "ret=" << ret;
  if (! d2) {
    ASSERT_EQ(rgw_lookup(fs, d1, "ndir2", &d2, nullptr, 0,
			 RGW_LOOKUP_FLAG_NONE), 0);
  }

  const std::string rel{"ndir1/ndir2/nested1"};
  reset_object_at(d2, "nested1", rel);

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, d2);
  ASSERT_EQ(get<0>(o2h->lookup("nested1")), 0);

  std::string a8{"NESTEDOK"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0) << "open of a nested object";
  auto w0 = get<1>(ofw);
  ASSERT_EQ(get<0>(o2h->write(w0, a8, 0, a8.length())), 0);

  /* the shadow sits beside its object, not at the bucket root */
  ASSERT_TRUE(sf::exists(shadow_path(rel)));
  ASSERT_FALSE(sf::exists(published_path(rel)));

  /* and a lookup resolves the hierarchical key while unpublished */
  struct rgw_file_handle* fh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, d2, "nested1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_NONE), 0);
  ASSERT_NE(fh, nullptr);
  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);

  auto rdr = o2h->read(w0, 0, a8.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a8);

  ASSERT_EQ(o2h->close(w0), 0);
  ASSERT_FALSE(sf::exists(shadow_path(rel)));
  ASSERT_TRUE(sf::exists(published_path(rel)));
  ASSERT_EQ(sf::file_size(published_path(rel)), a8.length());

  /* reopen the published nested object, and truncate it through the
   * path ganesha uses */
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  rdr = o2h->read(get<1>(ofr), 0, a8.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), a8);
  ASSERT_EQ(o2h->close(get<1>(ofr)), 0);

  ASSERT_EQ(rgw_truncate(fs, o2h->object_fh, 6, RGW_TRUNCATE_FLAG_NONE), 0);
  ASSERT_EQ(sf::file_size(published_path(rel)), 6u);

  (void) rgw_fh_rele(fs, d2, RGW_FH_RELE_FLAG_NONE);
  (void) rgw_fh_rele(fs, d1, RGW_FH_RELE_FLAG_NONE);
}

TEST(OPEN2, STATELESS_READ_RECLAIMED)
{
  /* rgw_read() opens the file handle's stateless open on demand, and a
   * v3 client never closes.  Nothing but the idle reclaimer returns
   * that open -- and because the open holds a reference on the handle,
   * a leak here is self-pinning: the handle can never be evicted, so it
   * can never be reclaimed either.
   *
   * Assert the reference is *returned*, rather than sampling the count
   * at a moment of our choosing.  A snapshot only says nothing is
   * outstanding right now, which stops being true as soon as
   * reclamation is deferred by design;  what matters is that it comes
   * back. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  g_conf().set_val("rgw_nfs_stateless_finalize_secs", "1");
  g_conf().apply_changes(nullptr);

  reset_object("rdidle1");

  std::string a4{"AAAA"};

  {
    std::unique_ptr<Open2Helper> o2h =
	std::make_unique<Open2Helper>(fs, bucket_fh);
    ASSERT_EQ(get<0>(o2h->lookup("rdidle1")), 0);
    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);
    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  }

  struct rgw_file_handle* fh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, bucket_fh, "rdidle1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_NONE), 0);
  auto* rgw_fh = rgw::get_rgwfh(fh);
  const uint32_t baseline = rgw_fh->get_refcnt();

  /* a read, with no close, as a v3 client would issue it */
  char buf[8];
  size_t nread{0};
  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(rgw_read(fs, fh, 0, a4.length(), &nread, buf,
		     RGW_READ_FLAG_NONE), 0);
  ASSERT_EQ(std::string(buf, a4.length()), a4);

  /* the open exists, so the wait below is not vacuous */
  ASSERT_GT(rgw_fh->get_refcnt(), baseline);
  ASSERT_NE(rgw_fh->get_global_open(), nullptr);

  /* wait for reclamation rather than assuming it is synchronous */
  bool reclaimed = false;
  for (int i = 0; i < 60; ++i) {
    if ((rgw_fh->get_refcnt() == baseline) &&
	(rgw_fh->get_global_open() == nullptr)) {
      reclaimed = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  ASSERT_TRUE(reclaimed)
      << "refcnt " << rgw_fh->get_refcnt() << " never returned to "
      << baseline;

  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);

  g_conf().set_val("rgw_nfs_stateless_finalize_secs", "300");
  g_conf().apply_changes(nullptr);
}

TEST(OPEN2, REOPEN2)
{
  /* NFSv4 reopen: change the access mode of an open the caller already
   * holds, rather than taking another.  The FSAL reaches this on its
   * first v4 open which changes share mode. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  reset_object("reopen1");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("reopen1")), 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

  /* a read open, bound to the published object */
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto r1 = get<1>(ofr);
  ASSERT_FALSE(sf::exists(shadow_path("reopen1")));

  /* writing through it is refused while it is read-only */
  auto nbw = o2h->write(r1, b4, 0, b4.length());
  ASSERT_EQ(get<0>(nbw), -EBADF);

  /* upgrade in place:  same open, now a writer, and the shadow exists */
  ASSERT_EQ(rgw_reopen2(r1, O_RDWR, RGW_OPEN_FLAG_NONE), 0);
  ASSERT_TRUE(sf::exists(shadow_path("reopen1")));

  ASSERT_EQ(get<0>(o2h->write(r1, b4, 0, b4.length())), 0);
  auto rdr = o2h->read(r1, 0, b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), b4);

  /* downgrade:  writes are refused again, and because this returned the
   * last write access it publishes, exactly as closing it would.  the
   * alternative -- publish only on close -- loses the write, since
   * close2 keys off the closing open's mode and this open is now a
   * reader */
  ASSERT_EQ(rgw_reopen2(r1, O_RDONLY, RGW_OPEN_FLAG_NONE), 0);
  ASSERT_EQ(get<0>(o2h->write(r1, a4, 0, a4.length())), -EBADF);
  ASSERT_FALSE(sf::exists(shadow_path("reopen1")));

  /* the reader follows the publish, and sees what was written */
  rdr = o2h->read(r1, 0, b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), b4);

  ASSERT_EQ(o2h->close(r1), 0);
  ASSERT_FALSE(sf::exists(shadow_path("reopen1")));

  auto ofr2 = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr2), 0);
  rdr = o2h->read(get<1>(ofr2), 0, b4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), b4);
  ASSERT_EQ(o2h->close(get<1>(ofr2)), 0);
}

TEST(OPEN2, REOPEN2_MULTI_WRITER)
{
  /* A downgrade publishes only when it returns the last write open.
   * REOPEN2 cannot show that: with a single writer every downgrade
   * returns the last one, so an implementation which published on every
   * downgrade passes it identically.  Two writers discriminates, and
   * it also covers the thing a share-mode change is for -- the other
   * writer keeps working. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  reset_object("reopen2");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("reopen2")), 0);

  std::string a4{"AAAA"};
  std::string b4{"BBBB"};
  std::string c4{"CCCC"};

  /* publish "AAAA" so there is a distinct prior content to compare */
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  ASSERT_EQ(sf::file_size(published_path("reopen2")), a4.length());

  /* two writers on the one shadow */
  auto ofw0 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw0), 0);
  auto w0 = get<1>(ofw0);
  auto ofw1 = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofw1), 0);
  auto w1 = get<1>(ofw1);

  ASSERT_EQ(get<0>(o2h->write(w0, b4, 0, b4.length())), 0);
  ASSERT_TRUE(sf::exists(shadow_path("reopen2")));

  /* downgrade w0:  a writer remains, so nothing is published */
  ASSERT_EQ(rgw_reopen2(w0, O_RDONLY, RGW_OPEN_FLAG_NONE), 0);
  ASSERT_TRUE(sf::exists(shadow_path("reopen2")));
  ASSERT_EQ(sf::file_size(published_path("reopen2")), a4.length());

  /* w0 may no longer write, w1 still may */
  ASSERT_EQ(get<0>(o2h->write(w0, c4, 0, c4.length())), -EBADF);
  ASSERT_EQ(get<0>(o2h->write(w1, c4, b4.length(), c4.length())), 0);

  /* and w0 reads what w1 writes, through the one shadow */
  auto rdr = o2h->read(w0, 0, b4.length() + c4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), b4 + c4);

  /* closing the remaining writer returns the last write open:  now it
   * publishes */
  ASSERT_EQ(o2h->close(w1), 0);
  ASSERT_FALSE(sf::exists(shadow_path("reopen2")));
  ASSERT_EQ(sf::file_size(published_path("reopen2")),
	    b4.length() + c4.length());

  ASSERT_EQ(o2h->close(w0), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  rdr = o2h->read(get<1>(ofr), 0, b4.length() + c4.length());
  ASSERT_EQ(get<0>(rdr), 0);
  ASSERT_EQ(get<1>(rdr), b4 + c4);
  ASSERT_EQ(o2h->close(get<1>(ofr)), 0);
}

TEST(OPEN2, UNIX_ATTRS_PERSIST)
{
  /* A file created through open2/write/close must carry its owner,
   * group and mode.  The legacy write path stamped these in
   * RGWWriteRequest::exec_finish(); the FSIO path had no equivalent, so
   * such a file had uid 0, gid 0 and no mode.
   *
   * The lookup on a fresh handle is the part that matters: a handle
   * which is still live answers getattr from memory and would pass
   * whether or not anything reached the object. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  reset_object("uxattr1");

  const uint32_t uid = 4242;
  const uint32_t gid = 4243;

  {
    std::unique_ptr<Open2Helper> o2h =
	std::make_unique<Open2Helper>(fs, bucket_fh);
    ASSERT_EQ(get<0>(o2h->lookup("uxattr1")), 0);

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = uid;
    st.st_gid = gid;
    st.st_mode = 0640;
    o2h->rgw_fh_of()->create_stat(&st, create_mask);

    std::string a4{"AAAA"};
    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);
    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  }

  /* On the object, not merely in a handle.  Without this the test
   * passes vacuously: rgw_lookup() may hand back the cached handle,
   * whose state still holds what create_stat() set, and decode_attrs()
   * only overwrites it when the attrs are actually present. */
  ASSERT_TRUE(sf::exists(published_path("uxattr1")));
  ASSERT_GE(::getxattr(published_path("uxattr1").c_str(),
		       "user.nsfs.rgw.unix1", nullptr, 0), 0)
      << "unix attrs were not written to the object";

  /* and come back on a handle which never saw them set */
  struct rgw_file_handle* fh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, bucket_fh, "uxattr1", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_NONE), 0);
  struct stat st2;
  ASSERT_EQ(rgw_getattr(fs, fh, &st2, RGW_GETATTR_FLAG_NONE), 0);
  ASSERT_EQ(st2.st_uid, uid);
  ASSERT_EQ(st2.st_gid, gid);
  ASSERT_EQ(st2.st_size, 4);
  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);
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
  librgw_shutdown(rgw_h);
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
