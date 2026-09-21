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
#include <sys/statvfs.h>
#include <thread>
#include <chrono>
#include <cstdint>
#include <memory>
#include <ranges>
#include <algorithm>
#include <tuple>
#include <iostream>
#include <vector>
#include <cstring>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <map>
#include <random>
#include "xxhash.h"

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw_lib.h" /* driver hints */
#include "rgw/rgw_file_int.h" /* the private view: refcounts, handles */
#include "librgw_sal_fixture.h" /* SAL-level bucket state the C API cannot set */

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

extern "C" {
  static int collect_names_cb(const char* name, void* arg, uint64_t offset,
			      struct stat* st, uint32_t st_mask,
			      uint32_t flags) {
    auto* names = static_cast<std::vector<std::string>*>(arg);
    names->push_back(std::string(name));
    return true;
  }
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

/* Assert the shape of a published etag.
 *
 * rgw_non_md5_etag selects between two forms, and the test must hold the
 * driver to whichever one is configured rather than accepting either --
 * the point of checking at all is that the wrong form reaches an S3
 * client verbatim.
 *
 * Both forms are stored bare, with no trailing NUL:  dump_etag() emits
 * the attribute into the quoted ETag header as-is, and a stored NUL makes
 * If-Match compare unequal because rgw_string_unquote() yields one fewer
 * byte than the attribute holds. */
static void expect_published_etag_shape(const std::string& etag)
{
  ASSERT_EQ(etag.find('\0'), std::string::npos)
      << "etag carries an embedded NUL: " << ::testing::PrintToString(etag);

  if (g_ceph_context->_conf->rgw_non_md5_etag) {
    /* mtime-<base36>-ino-<base36>: the same string nsfs gives version
     * ids.  The dashes are load-bearing -- they are what make an SDK
     * treat the value as a multipart etag and skip MD5 validation. */
    ASSERT_EQ(etag.compare(0, 6, "mtime-"), 0)
        << "not the non-md5 form: " << ::testing::PrintToString(etag);
    auto ino = etag.find("-ino-");
    ASSERT_NE(ino, std::string::npos)
        << "not the non-md5 form: " << ::testing::PrintToString(etag);
    ASSERT_GT(ino, 6u) << "empty mtime field: "
                       << ::testing::PrintToString(etag);
    ASSERT_GT(etag.length(), ino + 5) << "empty ino field: "
                                      << ::testing::PrintToString(etag);
    for (char c : etag) {
      ASSERT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') ||
                  c == '-')
          << "non-base36 char in " << ::testing::PrintToString(etag);
    }
  } else {
    ASSERT_EQ(etag.length(), 32u)
        << "etag is " << etag.length() << " bytes, not bare hex: "
        << ::testing::PrintToString(etag);
    for (char c : etag) {
      ASSERT_TRUE(std::isxdigit(static_cast<unsigned char>(c)))
          << "etag is not hex: " << ::testing::PrintToString(etag);
    }
  }
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
  expect_published_etag_shape(etag);

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

  /* the name is gone immediately;  on the nsfs/posix drivers that is
   * observable in both the namespace and .shadow */
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
   * Note this does not by itself prove the probe is being taken:  the
   * helper's lookup uses RGW_LOOKUP_FLAG_CREATE, so a handle is already
   * cached and the lookup below can be answered from it.
   * SAL_RESOLVES_UNPUBLISHED_SHADOW is the discriminating assertion. */
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

  /* deliberately not closed, so it stays unpublished;  on the
   * nsfs/posix drivers that means it exists only in .shadow/ */
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

TEST(OPEN2, SAL_RESOLVES_UNPUBLISHED_SHADOW)
{
  /* LOOKUP_FINDS_UNPUBLISHED asserts the same property through
   * rgw_lookup(), but it cannot distinguish resolution from a cache hit:
   * Open2Helper::lookup() looks the name up with RGW_LOOKUP_FLAG_CREATE
   * before the object exists, so a handle for it is already cached, and
   * the later RGW_LOOKUP_FLAG_NONE lookup can be answered from the cache
   * without anything being read back.
   *
   * Ask the SAL directly instead.  A sal::Object obtained from the
   * bucket carries no RGWFileHandle state, so a hit here can only come
   * from stat_fsio_view() resolving .shadow/ -- which is what decides
   * whether the unpublished-shadow gap is actually closed. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  reset_object("unpub2");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("unpub2")), 0);

  std::string a4{"AAAA"};

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), a4, 0, a4.length())), 0);

  /* deliberately not closed:  unpublished, which on the nsfs/posix
   * drivers means it exists only in .shadow/ */
  ASSERT_TRUE(sf::exists(shadow_path("unpub2")));
  ASSERT_FALSE(sf::exists(published_path("unpub2")));

  auto* driver = rgw::g_rgwlib->get_driver();
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<rgw::sal::Bucket> sal_bucket;
  ASSERT_EQ(driver->load_bucket(&dp, rgw_bucket("", bucket_name),
			        &sal_bucket, null_yield), 0);
  auto sal_object = sal_bucket->get_object(rgw_obj_key("unpub2"));

  struct stat st;
  rgw::sal::Attrs attrs;
  memset(&st, 0, sizeof(st));

  int rc = sal_object->stat_fsio_view(&dp, &st, &attrs, 0);
  if (rc == -ENOTSUP) {
    GTEST_SKIP() << "driver has no positional view";
  }
  ASSERT_EQ(rc, 0) << "unpublished shadow did not resolve";

  /* the published object does not exist, so a size which matches what
   * was written can only have come from the shadow */
  ASSERT_EQ(st.st_size, (off_t) a4.length());

  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  ASSERT_TRUE(sf::exists(published_path("unpub2")));
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

TEST(OPEN2, STATFS)
{
  /* ganesha calls this for get_fs_dynamic_info on every statfs, and a
   * client uses it to decide whether a write can fit.  cluster_stat()
   * used to return 0 without touching the struct, which rgw_statfs()
   * never initialized, so the answer was stack garbage. */
  struct rgw_statvfs vfs;
  memset(&vfs, 0xa5, sizeof(vfs));

  ASSERT_EQ(rgw_statfs(fs, fs->root_fh, &vfs, RGW_STATFS_FLAG_NONE), 0);

  /* a filesystem with no blocks at all is not a plausible answer, and
   * is what an unfilled struct would leave behind */
  ASSERT_GT(vfs.f_blocks, 0u);
  ASSERT_GT(vfs.f_bsize, 0u);
  ASSERT_LE(vfs.f_bavail, vfs.f_blocks);
  ASSERT_NE(vfs.f_blocks, 0xa5a5a5a5a5a5a5a5ULL);

  if (have_fs_layout()) {
    /* and it describes the filesystem the namespace is on */
    struct statvfs sys;
    ASSERT_EQ(::statvfs(nsfs_base().c_str(), &sys), 0);
    uint64_t frsize = sys.f_frsize ? sys.f_frsize : sys.f_bsize;
    uint64_t sys_kb = (sys.f_blocks * frsize) >> 10;
    uint64_t vfs_kb = (vfs.f_blocks * vfs.f_bsize) >> 10;
    /* same order of magnitude:  rgw_statfs reports in 1M blocks, and
     * the two calls are not simultaneous */
    ASSERT_GT(vfs_kb, sys_kb / 2);
    ASSERT_LT(vfs_kb, sys_kb * 2);
  }
}

TEST(OPEN2, DIR_ATTRS_PERSIST)
{
  /* On the nsfs/posix drivers a directory's attributes live on its
   * .folder sentinel, not on the directory inode.  Nothing in this
   * suite asserted on directory attributes, which is why a lookup path
   * that read them from the inode went unnoticed here and only showed
   * up in nfsns, and there only on a second run against the same root.
   *
   * The eviction is what makes this test able to fail: rgw_lookup()
   * otherwise returns the cached handle, whose state still holds what
   * rgw_mkdir() put there, and the assertion passes without any of it
   * having been read back. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  const uint32_t uid = 5150;
  const uint32_t gid = 5151;

  (void) rgw_unlink(fs, bucket_fh, "attrdir1", RGW_UNLINK_FLAG_NONE);

  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_uid = uid;
  st.st_gid = gid;
  st.st_mode = 0750;

  struct rgw_file_handle* dfh{nullptr};
  int ret = rgw_mkdir(fs, bucket_fh, "attrdir1", &st, create_mask, &dfh,
		      RGW_MKDIR_FLAG_NONE);
  ASSERT_TRUE((ret == 0) || (ret == -EEXIST)) << "ret=" << ret;
  if (! dfh) {
    ASSERT_EQ(rgw_lookup(fs, bucket_fh, "attrdir1", &dfh, nullptr, 0,
			 RGW_LOOKUP_FLAG_NONE), 0);
  }

  /* Ask the resolver directly rather than going through rgw_lookup().
   * A lookup hands back the cached handle, whose state still holds what
   * rgw_mkdir() put there, so the assertion would pass without any of
   * it being read back -- which is why this went unnoticed until nfsns
   * hit it across two runs, with a cold cache. */
  auto* driver = rgw::g_rgwlib->get_driver();
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<rgw::sal::Bucket> sal_bucket;
  ASSERT_EQ(driver->load_bucket(&dp, rgw_bucket("", bucket_name),
			        &sal_bucket, null_yield), 0);
  auto sal_object = sal_bucket->get_object(rgw_obj_key("attrdir1"));

  struct stat st2;
  rgw::sal::Attrs attrs;
  memset(&st2, 0, sizeof(st2));

  int rc = sal_object->stat_fsio_view(&dp, &st2, &attrs, 0);
  if (rc == -ENOTSUP) {
    GTEST_SKIP() << "driver has no positional view";
  }
  ASSERT_EQ(rc, 0);
  ASSERT_TRUE(S_ISDIR(st2.st_mode));

  /* on the nsfs/posix drivers these live on the directory's .folder
   * sentinel, not on the directory inode */
  ASSERT_NE(attrs.find(RGW_ATTR_UNIX1), attrs.end())
      << "directory unix attrs were not resolved";
  ASSERT_NE(attrs.find(RGW_ATTR_UNIX_KEY1), attrs.end());

  (void) rgw_fh_rele(fs, dfh, RGW_FH_RELE_FLAG_NONE);
}

/* END ALL TESTS */

TEST(OPEN2, DOTFILE_IS_LISTED)
{
  /* Hiding names which begin with '.' is a client convention -- ls
   * filters them, readdir(3) does not.  A server which drops them from
   * the listing is a different thing:  the client cannot un-hide what it
   * was never sent, so `ls -a` shows nothing, `rm -rf` leaves the
   * directory non-empty, and tar/rsync are silently lossy.
   *
   * What must stay suppressed is our own on-disk schema -- .shadow,
   * .versions, .folder and the fs_strategy temporaries -- which is why
   * the fix is a reserved-name predicate rather than dropping the skip.
   *
   * RED: fails until Directory::fill_cache stops skipping every dot
   * name (rgw_sal_nsfs.cc:1849). */
  reset_object(".bashrc");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup(".bashrc")), 0);

  std::string body{"export EDITOR=vi\n"};
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, 0, body.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

  /* it exists by every other means */
  struct rgw_file_handle* fh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, bucket_fh, ".bashrc", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_NONE), 0);
  ASSERT_NE(fh, nullptr);
  struct stat st;
  ASSERT_EQ(rgw_getattr(fs, fh, &st, RGW_GETATTR_FLAG_NONE), 0);
  ASSERT_EQ(st.st_size, (off_t) body.length());
  (void) rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);

  /* ... but a listing must show it too, or the namespace is incoherent */
  std::vector<std::string> names;
  uint64_t offset = 0;
  bool eof = false;
  do {
    ASSERT_EQ(rgw_readdir(fs, bucket_fh, &offset, collect_names_cb, &names,
			  &eof, RGW_READDIR_FLAG_NONE), 0);
  } while (! eof);

  ASSERT_NE(std::find(names.begin(), names.end(), ".bashrc"), names.end())
      << "created dotfile was not listed";

  /* our own schema stays hidden */
  for (const auto& reserved : {".shadow", ".versions", ".folder"}) {
    ASSERT_EQ(std::find(names.begin(), names.end(), reserved), names.end())
	<< "internal name " << reserved << " leaked into the listing";
  }
}

TEST(OPEN2, PUBLISHED_ETAG_IS_BARE)
{
  /* publish() stamps RGW_ATTR_ETAG from the shadow's content.  It must
   * store bare hex, with no trailing NUL:  dump_etag() emits the
   * attribute verbatim, so a stored NUL travels inside the quoted ETag
   * header, and If-Match then compares unequal for every object we
   * publish -- rgw_string_unquote() yields 32 bytes against a 33-byte
   * attribute, and std::string comparison checks length first.
   *
   * The S3 side of that cannot be asserted from librgw;  the stored
   * length is the invariant which produces it. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  reset_object("baretag1");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("baretag1")), 0);

  std::string body{"content for the digest"};
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, 0, body.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  ASSERT_TRUE(sf::exists(published_path("baretag1")));

  auto* driver = rgw::g_rgwlib->get_driver();
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<rgw::sal::Bucket> sal_bucket;
  ASSERT_EQ(driver->load_bucket(&dp, rgw_bucket("", bucket_name),
			        &sal_bucket, null_yield), 0);
  auto sal_object = sal_bucket->get_object(rgw_obj_key("baretag1"));

  struct stat st;
  rgw::sal::Attrs attrs;
  memset(&st, 0, sizeof(st));
  int rc = sal_object->stat_fsio_view(&dp, &st, &attrs, 0);
  if (rc == -ENOTSUP) {
    GTEST_SKIP() << "driver has no positional view";
  }
  ASSERT_EQ(rc, 0);

  auto it = attrs.find(RGW_ATTR_ETAG);
  ASSERT_NE(it, attrs.end()) << "publish did not stamp an etag";

  std::string stored = it->second.to_str();
  expect_published_etag_shape(stored);
}

TEST(OPEN2, PUBLISHED_ETAG_MATCHES_LISTING)
{
  /* publish() computes a real MD5 from the shadow's content and stamps it
   * as RGW_ATTR_ETAG, which is what HEAD returns.  The listing entry it
   * adds must carry the same value -- a listing that reports the
   * synthesized change token while HEAD reports a digest is a LIST/HEAD
   * disagreement, the same class of bug as the trailing NUL that
   * PUBLISHED_ETAG_IS_BARE pins down.
   *
   * The bucket listing is cached, and a cold-cache list rebuilds it from
   * disk -- which would repair whatever the incremental add got wrong and
   * hide the defect.  So list once to warm the cache before publishing
   * the object under test. */
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  auto* driver = rgw::g_rgwlib->get_driver();
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<rgw::sal::Bucket> sal_bucket;
  ASSERT_EQ(driver->load_bucket(&dp, rgw_bucket("", bucket_name),
				&sal_bucket, null_yield), 0);

  /* warm the listing cache */
  {
    rgw::sal::Bucket::ListParams params;
    rgw::sal::Bucket::ListResults results;
    ASSERT_EQ(sal_bucket->list(&dp, params, 1000, results, null_yield), 0);
  }

  reset_object("etagpub1");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("etagpub1")), 0);

  std::string body{"etag listing agreement"};
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, 0, body.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  ASSERT_TRUE(sf::exists(published_path("etagpub1")));

  /* the object's own etag, as HEAD would report it */
  auto sal_object = sal_bucket->get_object(rgw_obj_key("etagpub1"));
  struct stat st;
  rgw::sal::Attrs attrs;
  memset(&st, 0, sizeof(st));
  int rc = sal_object->stat_fsio_view(&dp, &st, &attrs, 0);
  if (rc == -ENOTSUP) {
    GTEST_SKIP() << "driver has no positional view";
  }
  ASSERT_EQ(rc, 0);
  auto it = attrs.find(RGW_ATTR_ETAG);
  ASSERT_NE(it, attrs.end()) << "published object carries no etag";
  std::string object_etag = it->second.to_str();

  /* the etag the listing reports for the same object */
  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  ASSERT_EQ(sal_bucket->list(&dp, params, 1000, results, null_yield), 0);

  std::string listed_etag;
  bool found = false;
  for (auto& o : results.objs) {
    if (o.key.name == "etagpub1") {
      listed_etag = o.meta.etag;
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found) << "published object absent from the listing";
  ASSERT_EQ(listed_etag, object_etag)
    << "listing etag disagrees with the object's own etag";
}

/*
 * ---------------------------------------------------------------------
 * NFS behaviour in a versioned bucket.
 *
 * librgw exposes no way to list or address non-current versions -- that
 * is a separate piece of work -- but the driver must already behave
 * correctly for an NFS client operating in a versioned bucket.  These
 * pin the parts that are observable today.
 *
 * The bucket is made versioned through the SAL, since PutBucketVersioning
 * is an S3 op with no rgw_file equivalent.  See librgw_sal_fixture.h.
 * ---------------------------------------------------------------------
 */


/* ---- reflink detection -------------------------------------------------
 *
 * POSIXStrategy::clone_file() forks the shadow with copy_file_range(),
 * which reflinks on a filesystem that supports it and otherwise copies
 * every byte.  copy_file_data() then degrades again, to a 64 KiB
 * read/write loop, on EXDEV/ENOSYS/EOPNOTSUPP.  All three tiers succeed,
 * so a COW fork silently becomes O(size) with no signal anywhere.  These
 * helpers make the difference observable.
 */

/* How many of a file's extents the filesystem reports as shared with
 * another file.  Negative is the errno from the ioctl.
 *
 * FIEMAP_FLAG_SYNC forces writeback first, so the map describes what is
 * on disk rather than what is still dirty in page cache. */
static int shared_extent_count(const sf::path& p)
{
  int fd = ::open(p.c_str(), O_RDONLY);
  if (fd < 0) {
    return -errno;
  }
  enum { MAX_EXTENTS = 512 };
  std::vector<char> buf(sizeof(struct fiemap) +
			MAX_EXTENTS * sizeof(struct fiemap_extent), 0);
  auto* fm = reinterpret_cast<struct fiemap*>(buf.data());
  fm->fm_start = 0;
  fm->fm_length = FIEMAP_MAX_OFFSET;
  fm->fm_flags = FIEMAP_FLAG_SYNC;
  fm->fm_extent_count = MAX_EXTENTS;

  if (::ioctl(fd, FS_IOC_FIEMAP, fm) < 0) {
    int err = -errno;
    ::close(fd);
    return err;
  }
  int shared = 0;
  for (uint32_t i = 0; i < fm->fm_mapped_extents; ++i) {
    if (fm->fm_extents[i].fe_flags & FIEMAP_EXTENT_SHARED) {
      ++shared;
    }
  }
  ::close(fd);
  return shared;
}

/* Whether this filesystem can reflink at all.
 *
 * FICLONE is a poor choice for the driver's own copy precisely because it
 * fails outright where reflink is unsupported, rather than falling back --
 * which is exactly what makes it an oracle here.  Without it a negative
 * result below is ambiguous between "this filesystem does not do reflink"
 * and "the driver stopped reflinking", and only the second is a bug. */
static bool fs_supports_reflink(const sf::path& dir)
{
  sf::path src = dir / ".reflink-probe-src";
  sf::path dst = dir / ".reflink-probe-dst";
  bool ok = false;

  int sfd = ::open(src.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  if (sfd >= 0) {
    /* cloning an empty file can succeed without exercising sharing */
    std::vector<char> blk(1 << 20, 'r');
    [[maybe_unused]] ssize_t nw = ::write(sfd, blk.data(), blk.size());
    ::fsync(sfd);
    int dfd = ::open(dst.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
    if (dfd >= 0) {
      ok = (::ioctl(dfd, FICLONE, sfd) == 0);
      ::close(dfd);
    }
    ::close(sfd);
  }
  std::error_code ec;
  sf::remove(src, ec);
  sf::remove(dst, ec);
  return ok;
}

TEST(OPEN2, SHADOW_FORK_IS_REFLINKED)
{
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }
  /* scratch lives beside the namespace root, on the same filesystem but
   * outside any bucket, so it cannot be mistaken for an object */
  const sf::path scratch = nsfs_base().parent_path();
  if (! fs_supports_reflink(scratch)) {
    GTEST_SKIP() << "filesystem does not support reflink";
  }

  const std::string name{"reflink1"};
  const size_t chunk = 1 << 20;
  const int chunks = 4;

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup(name)), 0);

  /* xfs keeps a small file inline in the inode, where there are no
   * extents to share, so write enough to force real allocation */
  {
    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    std::string body(chunk, 'z');
    for (int i = 0; i < chunks; ++i) {
      ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, i * chunk, chunk)), 0);
    }
    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  }
  ASSERT_TRUE(sf::exists(published_path(name)));

  /* Control, taken before any fork:  a copy this test makes itself, byte
   * by byte, must report no shared extents.
   *
   * It has to be written here rather than delegated.  cp(1) defaults to
   * --reflink=auto and would share, and copy_file_range() is the very
   * call under test;  either would make the control unable to fail, and
   * a control that cannot fail proves nothing about the subject. */
  const sf::path control = scratch / ".reflink-control";
  {
    int rfd = ::open(published_path(name).c_str(), O_RDONLY);
    ASSERT_GE(rfd, 0);
    int wfd = ::open(control.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
    ASSERT_GE(wfd, 0);
    std::vector<char> buf(65536);
    off_t off = 0;
    for (;;) {
      ssize_t nr = ::pread(rfd, buf.data(), buf.size(), off);
      ASSERT_GE(nr, 0);
      if (nr == 0) {
	break;
      }
      ASSERT_EQ(::pwrite(wfd, buf.data(), nr, off), nr);
      off += nr;
    }
    ::fsync(wfd);
    ::close(wfd);
    ::close(rfd);
    ASSERT_EQ(off, (off_t)(chunk * chunks)) << "control copy is short";
  }
  const int control_shared = shared_extent_count(control);
  ASSERT_GE(control_shared, 0)
      << "FIEMAP on the control failed: " << cpp_strerror(-control_shared);
  ASSERT_EQ(control_shared, 0)
      << "a byte-by-byte copy reported shared extents;  the detector "
	 "cannot distinguish a reflink from a copy, so this test proves "
	 "nothing";

  /* Subject:  reopening an existing object for write, without TRUNC,
   * COW-forks it into .shadow/ via clone_file(). */
  {
    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_NONE);
    ASSERT_EQ(get<0>(ofw), 0);
    ASSERT_TRUE(sf::exists(shadow_path(name))) << "no shadow was forked";

    const int shadow_shared = shared_extent_count(shadow_path(name));
    ASSERT_GE(shadow_shared, 0)
	<< "FIEMAP on the shadow failed: " << cpp_strerror(-shadow_shared);
    EXPECT_GT(shadow_shared, 0)
	<< "the shadow shares no extents with its object:  clone_file() "
	   "copied " << (chunk * chunks) << " bytes instead of reflinking "
	   "them, so the COW fork is O(size).  copy_file_range() degrades "
	   "silently, so nothing else would report this";

    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  }

  std::error_code ec;
  sf::remove(control, ec);
  ASSERT_EQ(o2h->unlink(), 0);
}

/* Per-process name.  A versioned bucket accumulates by design:  each run
 * of this suite adds two more versions of every key the VER_ tests write,
 * so a fixed name makes them pass only against a freshly wiped data root
 * and fail on every re-run -- "n which is: 12, 2" after six runs.  A fresh
 * bucket per process restores the clean-root behaviour without the fixture
 * having to delete versions itself, which would couple setup to the very
 * operations these tests exercise:  a regression in version delete would
 * then break setup and fail all ten at once, hiding the cause. */
static const std::string ver_bucket_name{
  "sorrydave-ver-" + std::to_string(::getpid())};
static struct rgw_file_handle* ver_bucket_fh{nullptr};

TEST(OPEN2, VER_SETUP)
{
  if (! have_fs_layout()) {
    GTEST_SKIP() << "not a filesystem-backed driver";
  }

  struct stat st;
  st.st_uid = 0; st.st_gid = 0; st.st_mode = 755;

  int rc = rgw_lookup(fs, fs->root_fh, ver_bucket_name.c_str(), &ver_bucket_fh,
		      nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  if (rc != 0) {
    rc = rgw_mkdir(fs, fs->root_fh, ver_bucket_name.c_str(), &st,
		   RGW_SETATTR_UID|RGW_SETATTR_GID|RGW_SETATTR_MODE,
		   &ver_bucket_fh, RGW_MKDIR_FLAG_NONE);
  }
  ASSERT_EQ(rc, 0);
  ASSERT_NE(ver_bucket_fh, nullptr);

  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");
  ASSERT_EQ(librgw_test::set_bucket_versioning(
	      &dp, ver_bucket_name, librgw_test::Versioning::Enabled), 0);

  /* assert the fixture took, rather than assuming it did */
  librgw_test::Versioning got{librgw_test::Versioning::Off};
  ASSERT_EQ(librgw_test::get_bucket_versioning(&dp, ver_bucket_name, got), 0);
  ASSERT_EQ(got, librgw_test::Versioning::Enabled)
    << "versioning fixture did not take effect";
}

/* Writing the same name twice must leave two versions, not one.  If this
 * fails everything below is measuring the wrong thing. */
TEST(OPEN2, VER_WRITE_TWICE_MAKES_TWO_VERSIONS)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("vtwice")), 0);

  for (auto* body : {"first", "second"}) {
    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    std::string b{body};
    ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), b, 0, b.length())), 0);
    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  }

  std::vector<rgw_bucket_dir_entry> objs;
  ASSERT_EQ(librgw_test::list_bucket(&dp, ver_bucket_name, true, objs), 0);

  int n = 0;
  for (auto& o : objs) {
    if (o.key.name == "vtwice") {
      ++n;
    }
  }
  ASSERT_EQ(n, 2) << "two NFS writes did not produce two versions";

  /* and only one of them may be current.  Read from the incremental
   * cache deliberately -- a rebuild recomputes the flag, so listing
   * through one would repair a stale FLAG_CURRENT and hide it. */
  int currents = 0;
  for (auto& o : objs) {
    if (o.key.name == "vtwice" &&
	(o.flags & rgw_bucket_dir_entry::FLAG_CURRENT)) {
      ++currents;
    }
  }
  ASSERT_EQ(currents, 1)
    << "publishing a new version left the previous one flagged current";
}

/* unlink is POSIX from the NFS side:  the name goes away.  In a
 * versioned bucket that must not destroy history -- S3 semantics for a
 * delete without a versionId is a delete marker over a retained
 * version, and NFS deletes act immediately on S3. */
TEST(OPEN2, VER_PUBLISHED_ETAG_TRACKS_VERSION_ID)
{
  /* rgw_non_md5_etag's nsfs contract:  the etag is the same
   * mtime-<base36>-ino-<base36> string as the version id, so on a
   * versioned bucket ETag equals versionId.
   *
   * That is not free -- publish() computes the etag from the shadow
   * before the renameat(), and the version id is computed after it.  The
   * two agree only because setting an xattr updates ctime rather than
   * mtime and a rename moves the name rather than the file.  If either
   * assumption stops holding the strings drift apart, and nothing else
   * in the suite would notice.
   *
   * With the option off the two must *differ* -- the etag is a digest of
   * the content and the version id is a change token.  Asserting that
   * too is what keeps this from being a check that passes either way. */
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("vetagvid")), 0);

  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  std::string body{"etag tracks version id"};
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, 0, body.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

  std::vector<rgw_bucket_dir_entry> objs;
  ASSERT_EQ(librgw_test::list_bucket(&dp, ver_bucket_name, true, objs), 0);

  int found = 0;
  for (auto& o : objs) {
    if (o.key.name != "vetagvid") {
      continue;
    }
    ++found;
    ASSERT_FALSE(o.key.instance.empty())
        << "NFS write did not stamp a version id";
    ASSERT_FALSE(o.meta.etag.empty()) << "NFS write did not stamp an etag";
    expect_published_etag_shape(o.meta.etag);

    if (g_ceph_context->_conf->rgw_non_md5_etag) {
      ASSERT_EQ(o.meta.etag, o.key.instance)
          << "etag and version id disagree; publish() computed them across "
             "the rename and something moved mtime";
    } else {
      ASSERT_NE(o.meta.etag, o.key.instance)
          << "etag is supposed to be a content digest here, not the "
             "change token";
    }
  }
  ASSERT_EQ(found, 1) << "expected exactly one version of vetagvid";
}

TEST(OPEN2, VER_UNLINK_CREATES_DELETE_MARKER)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("vdel")), 0);

  std::string body{"to be deleted"};
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, 0, body.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

  /* exercise the incremental cache path, not a rebuild */
  ASSERT_EQ(librgw_test::warm_listing_cache(&dp, ver_bucket_name), 0);

  ASSERT_EQ(rgw_unlink(fs, ver_bucket_fh, "vdel", RGW_UNLINK_FLAG_NONE), 0);

  std::vector<rgw_bucket_dir_entry> objs;
  ASSERT_EQ(librgw_test::list_bucket(&dp, ver_bucket_name, true, objs), 0);

  int versions = 0, markers = 0, current = 0;
  for (auto& o : objs) {
    if (o.key.name != "vdel") {
      continue;
    }
    ++versions;
    if (o.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
      ++markers;
    }
    if (o.flags & rgw_bucket_dir_entry::FLAG_CURRENT) {
      ++current;
    }
  }

  ASSERT_EQ(markers, 1) << "NFS unlink did not leave a delete marker";
  ASSERT_EQ(versions, 2)
    << "the deleted version was not retained alongside the marker";
  ASSERT_EQ(current, 1) << "exactly one entry must be current";
}

/* the name must be gone from the NFS view even though the data is not */
TEST(OPEN2, VER_UNLINK_LOOKUP_IS_ENOENT)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  struct rgw_file_handle* fh{nullptr};
  int rc = rgw_lookup(fs, ver_bucket_fh, "vdel", &fh, nullptr, 0,
		      RGW_LOOKUP_FLAG_NONE);
  ASSERT_NE(rc, 0) << "unlinked name still resolves over NFS";
}

/* the demoted version's data must still be on disk under .versions/ */
TEST(OPEN2, VER_UNLINK_PRIOR_VERSION_ON_DISK)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  auto vdir = nsfs_base() / ver_bucket_name / ".versions";
  ASSERT_TRUE(sf::is_directory(vdir)) << "no .versions/ after a versioned unlink";

  int entries = 0;
  for (auto& de : sf::directory_iterator(vdir)) {
    if (de.path().filename().string().rfind("vdel", 0) == 0) {
      ++entries;
    }
  }
  /* the demoted version and the delete marker */
  ASSERT_GE(entries, 2) << "demoted version missing from .versions/";
}

/* A non-current version's listed etag must be its own etag, not the
 * synthesized change token -- the same rule publish() follows.  The
 * token is for objects that carry no etag, i.e. sideloaded files. */
TEST(OPEN2, VER_DEMOTED_ETAG_MATCHES_OBJECT)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("vetag")), 0);

  std::string body{"demoted etag body"};
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, 0, body.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);

  /* the etag of the version that is about to be demoted */
  std::string published_etag;
  ASSERT_EQ(librgw_test::get_object_attr(&dp, ver_bucket_name,
					 rgw_obj_key("vetag"),
					 RGW_ATTR_ETAG, published_etag), 0);
  ASSERT_FALSE(published_etag.empty());

  ASSERT_EQ(librgw_test::warm_listing_cache(&dp, ver_bucket_name), 0);

  /* unlinking demotes it and adds a delete marker */
  ASSERT_EQ(rgw_unlink(fs, ver_bucket_fh, "vetag", RGW_UNLINK_FLAG_NONE), 0);

  std::vector<rgw_bucket_dir_entry> objs;
  ASSERT_EQ(librgw_test::list_bucket(&dp, ver_bucket_name, true, objs), 0);

  bool found = false;
  for (auto& o : objs) {
    if (o.key.name != "vetag") {
      continue;
    }
    if (o.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
      continue; /* a marker has no content and no digest */
    }
    found = true;
    ASSERT_EQ(o.meta.etag, published_etag)
      << "demoted version listed with an etag that is not its own";
  }
  ASSERT_TRUE(found) << "demoted version absent from the version listing";
}

/*
 * Dot-prefixed objects in a versioned bucket.
 *
 * A version entry is named "<key>_<version_id>", so for a key that
 * itself begins with a dot every one of its versions does too.  The
 * scans of .versions/ used to skip any name starting with a dot, which
 * hid all of them -- from version enumeration, from newest-version
 * resolution, and from promotion on delete.  That became reachable when
 * ordinary dotfiles started being listed;  before then the driver hid
 * them everywhere and was at least consistent.
 */

TEST(OPEN2, VER_DOTFILE_TWO_VERSIONS)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup(".hidden")), 0);

  for (auto* body : {"dot first", "dot second"}) {
    auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
    ASSERT_EQ(get<0>(ofw), 0);
    std::string b{body};
    ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), b, 0, b.length())), 0);
    ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
  }

  /* The rows added incrementally as each version was published would
   * satisfy this on their own.  Drop the cache so the listing has to
   * rebuild by walking .versions/ -- that walk is the thing under test,
   * and it is the only path that has to recognise a version entry whose
   * name begins with a dot. */
  ASSERT_EQ(librgw_test::invalidate_listing_cache(&dp, ver_bucket_name), 0);

  std::vector<rgw_bucket_dir_entry> objs;
  ASSERT_EQ(librgw_test::list_bucket(&dp, ver_bucket_name, true, objs), 0);

  int n = 0;
  for (auto& o : objs) {
    if (o.key.name == ".hidden") {
      ++n;
    }
  }
  ASSERT_EQ(n, 2)
    << "versions of a dot-prefixed key were not enumerated from the store";
}

/* the version entries must be on disk under their real ids, not hidden
 * or collapsed -- the listing could in principle be right for the wrong
 * reason, so check the store directly */
TEST(OPEN2, VER_DOTFILE_VERSIONS_ON_DISK)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  auto vdir = nsfs_base() / ver_bucket_name / ".versions";
  ASSERT_TRUE(sf::is_directory(vdir));

  int entries = 0;
  for (auto& de : sf::directory_iterator(vdir)) {
    auto fn = de.path().filename().string();
    if (fn.rfind(".hidden_", 0) == 0) {
      ++entries;
      EXPECT_EQ(fn.find("_null"), std::string::npos)
	<< "dot-prefixed version stored as the null version: " << fn;
    }
  }
  ASSERT_GE(entries, 1)
    << "no version entry for a dot-prefixed key in .versions/";
}

/* reading the name back must give the newest content, which is what
 * newest-version resolution decides */
TEST(OPEN2, VER_DOTFILE_READS_NEWEST)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup(".hidden")), 0);

  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto rr = o2h->read(get<1>(ofr), 0, 64);
  ASSERT_EQ(get<0>(rr), 0);
  ASSERT_EQ(get<1>(rr), std::string("dot second"))
    << "a dot-prefixed key did not resolve to its newest version";
  ASSERT_EQ(o2h->close(get<1>(ofr)), 0);
}

/* and it must still be listed as an ordinary object -- the reserved-name
 * predicate suppresses driver names, not every leading dot */
TEST(OPEN2, VER_DOTFILE_LISTED_AS_OBJECT)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  ASSERT_EQ(librgw_test::invalidate_listing_cache(&dp, ver_bucket_name), 0);

  std::vector<rgw_bucket_dir_entry> objs;
  ASSERT_EQ(librgw_test::list_bucket(&dp, ver_bucket_name, false, objs), 0);

  bool found = false;
  for (auto& o : objs) {
    if (o.key.name == ".hidden") {
      found = true;
    }
    EXPECT_NE(o.key.name, ".versions") << "driver name leaked into a listing";
    EXPECT_NE(o.key.name, ".shadow") << "driver name leaked into a listing";
  }
  ASSERT_TRUE(found) << "dot-prefixed object absent from the ordinary listing";
}

/* unlink of a dot-prefixed key must behave like any other:  a delete
 * marker over a retained version */
TEST(OPEN2, VER_DOTFILE_UNLINK_CREATES_DELETE_MARKER)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  ASSERT_EQ(librgw_test::warm_listing_cache(&dp, ver_bucket_name), 0);
  ASSERT_EQ(rgw_unlink(fs, ver_bucket_fh, ".hidden", RGW_UNLINK_FLAG_NONE), 0);

  ASSERT_EQ(librgw_test::invalidate_listing_cache(&dp, ver_bucket_name), 0);

  std::vector<rgw_bucket_dir_entry> objs;
  ASSERT_EQ(librgw_test::list_bucket(&dp, ver_bucket_name, true, objs), 0);

  int versions = 0, markers = 0;
  for (auto& o : objs) {
    if (o.key.name != ".hidden") {
      continue;
    }
    ++versions;
    if (o.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
      ++markers;
    }
  }
  ASSERT_EQ(markers, 1)
    << "unlink of a dot-prefixed key left no delete marker";
  ASSERT_EQ(versions, 3)
    << "both prior versions must survive beside the marker";

  struct rgw_file_handle* fh{nullptr};
  ASSERT_NE(rgw_lookup(fs, ver_bucket_fh, ".hidden", &fh, nullptr, 0,
		       RGW_LOOKUP_FLAG_NONE), 0)
    << "unlinked dot-prefixed name still resolves over NFS";
}

/*
 * Suspended versioning.
 *
 * Suspended is not "off".  The bucket is still versioned() -- versions
 * made while it was enabled survive untouched -- but it is no longer
 * versioning_enabled(), and S3 says a write then creates or replaces the
 * *null* version rather than minting a new one.  It is the one mode in
 * which a write destroys data:  the previous null version's content is
 * gone.  So repeated writes must not accumulate versions.
 *
 * These run last because they leave the bucket suspended for anything
 * that follows;  the final test restores it.
 */

static int ver_count(const DoutPrefixProvider* dp, const std::string& key,
		     int* nulls = nullptr, int* currents = nullptr)
{
  std::vector<rgw_bucket_dir_entry> objs;
  if (librgw_test::list_bucket(dp, ver_bucket_name, true, objs) != 0) {
    return -1;
  }
  int n = 0;
  if (nulls) *nulls = 0;
  if (currents) *currents = 0;
  for (auto& o : objs) {
    if (o.key.name != key) {
      continue;
    }
    ++n;
    if (nulls && o.key.instance == "null") {
      ++(*nulls);
    }
    if (currents && (o.flags & rgw_bucket_dir_entry::FLAG_CURRENT)) {
      ++(*currents);
    }
  }
  return n;
}

static void ver_write(const char* key, const std::string& body)
{
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup(key)), 0);
  auto ofw = o2h->open(O_RDWR, RGW_OPEN_FLAG_CREATE);
  ASSERT_EQ(get<0>(ofw), 0);
  ASSERT_EQ(get<0>(o2h->write(get<1>(ofw), body, 0, body.length())), 0);
  ASSERT_EQ(o2h->close(get<1>(ofw)), 0);
}

TEST(OPEN2, VER_SUSPEND_PRESERVES_ENABLED_VERSIONS)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  /* two versions while enabled */
  ver_write("vsusp", "enabled one");
  ver_write("vsusp", "enabled two");
  ASSERT_EQ(librgw_test::invalidate_listing_cache(&dp, ver_bucket_name), 0);
  ASSERT_EQ(ver_count(&dp, "vsusp"), 2) << "setup did not produce two versions";

  ASSERT_EQ(librgw_test::set_bucket_versioning(
	      &dp, ver_bucket_name, librgw_test::Versioning::Suspended), 0);
  librgw_test::Versioning got{librgw_test::Versioning::Off};
  ASSERT_EQ(librgw_test::get_bucket_versioning(&dp, ver_bucket_name, got), 0);
  ASSERT_EQ(got, librgw_test::Versioning::Suspended)
    << "suspend fixture did not take effect";

  ver_write("vsusp", "suspended one");

  ASSERT_EQ(librgw_test::invalidate_listing_cache(&dp, ver_bucket_name), 0);
  int nulls = 0;
  int n = ver_count(&dp, "vsusp", &nulls);
  ASSERT_EQ(n, 3)
    << "suspending must retain versions made while enabled";
  ASSERT_EQ(nulls, 1)
    << "a write to a suspended bucket must create exactly one null version";
}

TEST(OPEN2, VER_SUSPEND_WRITES_DO_NOT_ACCUMULATE)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  /* two further writes while suspended:  each replaces the null version
   * rather than adding one, so the count must not move */
  ver_write("vsusp", "suspended two");
  ver_write("vsusp", "suspended three");

  ASSERT_EQ(librgw_test::invalidate_listing_cache(&dp, ver_bucket_name), 0);
  int nulls = 0;
  int n = ver_count(&dp, "vsusp", &nulls);
  ASSERT_EQ(n, 3)
    << "writes to a suspended bucket accumulated versions";
  ASSERT_EQ(nulls, 1) << "more than one null version";

  /* and the newest content is what a reader sees */
  std::unique_ptr<Open2Helper> o2h =
      std::make_unique<Open2Helper>(fs, ver_bucket_fh);
  ASSERT_EQ(get<0>(o2h->lookup("vsusp")), 0);
  auto ofr = o2h->open(O_RDONLY, RGW_OPEN_FLAG_NONE);
  ASSERT_EQ(get<0>(ofr), 0);
  auto rr = o2h->read(get<1>(ofr), 0, 64);
  ASSERT_EQ(get<0>(rr), 0);
  ASSERT_EQ(get<1>(rr), std::string("suspended three"));
  ASSERT_EQ(o2h->close(get<1>(ofr)), 0);
}

/* only one version of a key may be current, at any versioning state.
 * Checked against the incremental cache rather than a rebuild:  a
 * rebuild recomputes the flag, so it would repair a stale one and hide
 * it. */
TEST(OPEN2, VER_SUSPEND_ONE_CURRENT_IN_CACHE)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");

  ver_write("vsuspcur", "first while suspended");
  ver_write("vsuspcur", "second while suspended");

  int currents = 0;
  int n = ver_count(&dp, "vsuspcur", nullptr, &currents);
  ASSERT_GE(n, 1);
  ASSERT_EQ(currents, 1)
    << "the listing cache holds more than one current version";
}

/* The store is the authority, but note its shape:  the current version
 * IS the top-level file, and only non-current versions live in
 * .versions/.  So the null version a suspended write creates is not in
 * .versions/ at all -- it is the object itself, carrying a version-id
 * xattr of "null".  Asserting a .versions/<key>_null entry here would be
 * asserting a model of the store rather than the store. */
TEST(OPEN2, VER_SUSPEND_ON_DISK)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  auto vdir = nsfs_base() / ver_bucket_name / ".versions";
  ASSERT_TRUE(sf::is_directory(vdir));

  int nulls = 0, reals = 0;
  for (auto& de : sf::directory_iterator(vdir)) {
    auto fn = de.path().filename().string();
    if (fn.rfind("vsusp_", 0) != 0) {
      continue;
    }
    if (fn == "vsusp_null") {
      ++nulls;
    } else {
      ++reals;
    }
  }
  ASSERT_EQ(reals, 2) << "enabled-era versions did not survive suspension";
  ASSERT_EQ(nulls, 0)
    << "the null version is the current object, not a .versions/ entry";

  /* the current object carries the null version id */
  auto cur = nsfs_base() / ver_bucket_name / "vsusp";
  ASSERT_TRUE(sf::exists(cur)) << "no current object after a suspended write";

  char buf[64];
  std::string vid_x = std::string("user.nsfs.") + "version_id";
  ssize_t len = ::getxattr(cur.c_str(), vid_x.c_str(), buf, sizeof(buf));
  ASSERT_GT(len, 0) << "current object carries no version id";
  ASSERT_EQ(std::string(buf, len), std::string("null"))
    << "a write to a suspended bucket must stamp the null version id";
}

TEST(OPEN2, VER_SUSPEND_RESTORE)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }
  const DoutPrefix dp(g_ceph_context, dout_subsys, "write2 test: ");
  ASSERT_EQ(librgw_test::set_bucket_versioning(
	      &dp, ver_bucket_name, librgw_test::Versioning::Enabled), 0);
}

/*
 * The finalize interval means two different things.
 *
 * v3 sends no signal that a write has finished, so librgw infers the
 * close from the writer going quiet.  On an unversioned bucket that
 * interval is an S3-visibility SLA.  On a versioned one every publish
 * mints a permanent version, so it is instead the length of pause
 * treated as still-writing, and has to exceed application think-time --
 * otherwise a client that dribbles writes to one file mints a version
 * per pause.
 */

static sf::path ver_published_path(const std::string& obj)
{
  return nsfs_base() / ver_bucket_name / obj;
}

static sf::path ver_shadow_path(const std::string& obj)
{
  return nsfs_base() / ver_bucket_name / ".shadow" / obj;
}

/* The interval is chosen from a flag resolved once, when the stateless
 * open is created -- not cached on the bucket handle, which outlives any
 * versioning change and would keep a mounted export on the wrong
 * interval indefinitely. */
TEST(OPEN2, VER_TIMER_FLAG_RESOLVED_PER_OPEN)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }

  auto stateless_versioned = [](struct rgw_file_handle* fh) -> bool {
    auto* rgw_fh = rgw::get_rgwfh(fh);
    auto* f = std::get_if<rgw::RGWFileHandle::file>(&rgw_fh->variant_type);
    return f && f->versioned_bucket;
  };

  std::string body{"DDDD"};
  size_t nbytes{0};

  struct rgw_file_handle* vfh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, ver_bucket_fh, "vflag", &vfh, nullptr, 0,
		       RGW_LOOKUP_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_open(fs, vfh, O_RDWR,
		     RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_write(fs, vfh, 0, body.length(), &nbytes,
		      (void*) body.c_str(), RGW_OPEN_FLAG_V3), 0);
  EXPECT_TRUE(stateless_versioned(vfh))
    << "a stateless open in a versioned bucket did not resolve as versioned";
  ASSERT_EQ(rgw_close(fs, vfh, RGW_CLOSE_FLAG_NONE), 0);

  reset_object("pflag");
  struct rgw_file_handle* pfh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, bucket_fh, "pflag", &pfh, nullptr, 0,
		       RGW_LOOKUP_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_open(fs, pfh, O_RDWR,
		     RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_write(fs, pfh, 0, body.length(), &nbytes,
		      (void*) body.c_str(), RGW_OPEN_FLAG_V3), 0);
  EXPECT_FALSE(stateless_versioned(pfh))
    << "a stateless open in an unversioned bucket resolved as versioned";
  ASSERT_EQ(rgw_close(fs, pfh, RGW_CLOSE_FLAG_NONE), 0);
}

TEST(OPEN2, VER_TIMER_LONGER_IN_VERSIONED_BUCKET)
{
  if (! ver_bucket_fh) {
    GTEST_SKIP() << "versioned bucket unavailable";
  }

  /* a short SLA and a long still-writing tolerance */
  g_conf().set_val("rgw_nfs_stateless_finalize_secs", "1");
  g_conf().set_val("rgw_nfs_stateless_finalize_versioned_secs", "3600");
  g_conf().apply_changes(nullptr);

  std::string body{"CCCC"};
  size_t nbytes{0};

  /* control:  the unversioned bucket must finalize inside the window.
   * Without it a versioned bucket that never publishes for an unrelated
   * reason would satisfy the assertion below. */
  reset_object("timerctl");
  struct rgw_file_handle* pfh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, bucket_fh, "timerctl", &pfh, nullptr, 0,
		       RGW_LOOKUP_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_open(fs, pfh, O_RDWR,
		     RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_write(fs, pfh, 0, body.length(), &nbytes,
		      (void*) body.c_str(), RGW_OPEN_FLAG_V3), 0);

  /* subject:  same shape, in the versioned bucket */
  struct rgw_file_handle* vfh{nullptr};
  ASSERT_EQ(rgw_lookup(fs, ver_bucket_fh, "timerver", &vfh, nullptr, 0,
		       RGW_LOOKUP_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_open(fs, vfh, O_RDWR,
		     RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE), 0);
  ASSERT_EQ(rgw_write(fs, vfh, 0, body.length(), &nbytes,
		      (void*) body.c_str(), RGW_OPEN_FLAG_V3), 0);

  ASSERT_TRUE(sf::exists(ver_shadow_path("timerver")));

  std::this_thread::sleep_for(std::chrono::seconds(4));

  EXPECT_TRUE(sf::exists(published_path("timerctl")))
    << "control: an unversioned stateless write did not finalize on the "
       "short interval, so this test cannot distinguish anything";

  EXPECT_FALSE(sf::exists(ver_published_path("timerver")))
    << "a versioned bucket finalized on the unversioned interval";
  EXPECT_TRUE(sf::exists(ver_shadow_path("timerver")))
    << "the versioned write's shadow went away without publishing";

  /* close both:  the versioned one publishes here, on the last write
   * open being returned, which is the rule the timer only stands in for */
  ASSERT_EQ(rgw_close(fs, pfh, RGW_CLOSE_FLAG_NONE), 0);
  ASSERT_EQ(rgw_close(fs, vfh, RGW_CLOSE_FLAG_NONE), 0);
  ASSERT_TRUE(sf::exists(ver_published_path("timerver")));

  g_conf().set_val("rgw_nfs_stateless_finalize_secs", "300");
  g_conf().set_val("rgw_nfs_stateless_finalize_versioned_secs", "1800");
  g_conf().apply_changes(nullptr);
}

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
