// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include <stdint.h>
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
#include "global/global_init.h"

#define dout_subsys ceph_subsys_rgw

namespace {
  librgw_t rgw = nullptr;
  string uid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;

  bool do_pre_list = false;
  bool do_put = false;
  bool do_bulk = false;
  bool do_writev = false;
  bool do_readv = false;
  bool do_verify = false;
  bool do_get = false;
  bool do_delete = false;
  bool do_stat = false; // stat objects (not buckets)
  bool do_hexdump = false;

  bool object_open = false;

  string bucket_name = "sorry_dave";
  string object_name = "jocaml";

  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *object_fh = nullptr;

  typedef std::tuple<string,uint64_t, struct rgw_file_handle*> fid_type;
  std::vector<fid_type> fids;

  std::uniform_int_distribution<uint8_t> uint_dist;
  std::mt19937 rng;

  constexpr int iovcnt = 16;
  constexpr int page_size = 65536;
  constexpr int seed = 8675309;

  struct ZPage
  {
    char data[page_size];
    uint64_t cksum;
  }; /* ZPage */
  
  struct ZPageSet
  {
    std::vector<ZPage*> pages;
    struct iovec* iovs;

    ZPageSet(int n) {
      pages.reserve(n);
      iovs = (struct iovec*) calloc(n, sizeof(struct iovec));
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = new ZPage();
	for (int data_ix = 0; data_ix < page_size; ++data_ix) {
	  p->data[data_ix] = uint_dist(rng);
	} // data_ix
	p->cksum = XXH64(p->data, page_size, seed);
	pages.emplace_back(p);
	// and iovs
	struct iovec* iov = &iovs[page_ix];
	iov->iov_base = p->data;
	iov->iov_len = page_size;
      } // page_ix
    }

    int size() { return pages.size(); }

    struct iovec* get_iovs() { return iovs; }

    bool operator==(const ZPageSet& rhs) {
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p1 = pages[page_ix];
	ZPage* p2 = rhs.pages[page_ix];
	if (p1->cksum != p2->cksum)
	  return false;
      }
      return true;
    }

    bool operator==(const rgw_uio* uio) {
      uint64_t cksum;
      int vix = 0, off = 0;
      rgw_vio* vio = &uio->uio_vio[vix];
      int vio_len = vio->vio_len;
      char *data;

      for (int ix = 0; ix < iovcnt; ++ix) {
	ZPage* p1 = pages[ix];
	data = static_cast<char*>(vio->vio_base) + off;
	cksum = XXH64(data, page_size, seed);

	if (p1->cksum != cksum) {
	  int r = memcmp(data, p1->data, page_size);
	  std::cout << "problem at ix " << ix << " r " << r<< std::endl;
	  return false;
	}

	off += page_size;
	if (off >= vio_len) {
	  vio = &uio->uio_vio[++vix];
	  vio_len = vio->vio_len;
	  off = 0;
	}
      }
      return true;
    }
    
    void cksum() {
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = pages[page_ix];
	p->cksum = XXH64(p->data, page_size, seed);
      }
    }

    void reset_iovs() { // VOP_READ and VOP_WRITE update
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = pages[page_ix];
	struct iovec* iov = &iovs[page_ix];
	iov->iov_base = p->data;
	iov->iov_len = page_size;
      }
    }

    ~ZPageSet() {
      for (unsigned int ix = 0; ix < pages.size(); ++ix)
	delete pages[ix];
      free(iovs);
    }
  }; /* ZPageSet */

  rgw_uio uio[1];
  ZPageSet zp_set1{iovcnt}; // 1M random data in 16 64K pages

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount(rgw, uid.c_str(), access_key.c_str(), secret_key.c_str(),
		      &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(LibRGW, LOOKUP_BUCKET) {
  int ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		       RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

extern "C" {
  static bool r2_cb(const char* name, void *arg, uint64_t offset,
		    uint32_t flags) {
    // don't need arg--it would point to fids
    fids.push_back(fid_type(name, offset, nullptr));
    return true; /* XXX ? */
  }
}

TEST(LibRGW, LIST_OBJECTS) {
  if (do_pre_list) {
    /* list objects via readdir, bucketwise */
    using std::get;

    ldout(g_ceph_context, 0) << __func__ << " readdir on bucket "
			     << bucket_name << dendl;
    bool eof = false;
    uint64_t offset = 0;
    int ret = rgw_readdir(fs, bucket_fh, &offset, r2_cb, &fids,
			  &eof, RGW_READDIR_FLAG_NONE);
    for (auto& fid : fids) {
      std::cout << "fname: " << get<0>(fid) << " fid: " << get<1>(fid)
		<< std::endl;
    }
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, LOOKUP_OBJECT) {
  if (do_get || do_stat || do_put || do_bulk || do_readv || do_writev) {
    int ret = rgw_lookup(fs, bucket_fh, object_name.c_str(), &object_fh,
			RGW_LOOKUP_FLAG_CREATE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, OBJ_OPEN) {
  if (do_get || do_put || do_readv || do_writev) {
    int ret = rgw_open(fs, object_fh, 0 /* posix flags */, 0 /* flags */);
    ASSERT_EQ(ret, 0);
    object_open = true;
  }
}

TEST(LibRGW, PUT_OBJECT) {
  if (do_put) {
    size_t nbytes;
    string data = "hi mom"; // fix this
    int ret = rgw_write(fs, object_fh, 0, data.length(), &nbytes,
			(void*) data.c_str(), RGW_WRITE_FLAG_NONE);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(nbytes, data.length());
  }
}

TEST(LibRGW, GET_OBJECT) {
  if (do_get) {
    char sbuf[512];
    memset(sbuf, 0, 512);
    size_t nread;
    int ret = rgw_read(fs, object_fh, 0 /* off */, 512 /* len */, &nread, sbuf,
		       RGW_READ_FLAG_NONE);
    ASSERT_EQ(ret, 0);
    buffer::list bl;
    bl.push_back(buffer::create_static(nread, sbuf));
    if (do_hexdump) {
      dout(15) << "";
      bl.hexdump(*_dout);
      *_dout << dendl;
    }
  }
}

TEST(LibRGW, STAT_OBJECT) {
  if (do_stat) {
    struct stat st;
    int ret = rgw_getattr(fs, object_fh, &st, RGW_GETATTR_FLAG_NONE);
    ASSERT_EQ(ret, 0);
    dout(15) << "rgw_getattr on " << object_name << " size = "
	     << st.st_size << dendl;
  }
}

TEST(LibRGW, WRITE_READ_VERIFY)
{
  if (do_bulk && do_put) {
    ZPageSet zp_set1{iovcnt}; // 1M random data in 16 64K pages
    struct iovec *iovs = zp_set1.get_iovs();

    /* read after write POSIX-style */
    size_t nbytes, off = 0;
    for (int ix = 0; ix < 16; ++ix, off += page_size) {
      struct iovec *iov = &iovs[ix];
      int ret = rgw_write(fs, object_fh, off, page_size, &nbytes,
			  iov->iov_base, RGW_WRITE_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(nbytes, size_t(page_size));
    }
    zp_set1.reset_iovs();
  }
}

/* "functions that call alloca are not inlined"
 * --alexandre oliva
 * http://gcc.gnu.org/ml/gcc-help/2004-04/msg00158.html
 */
#define alloca_uio()				\
  do {\
    int uiosz = sizeof(rgw_uio) + iovcnt*sizeof(rgw_vio);		\
    uio = static_cast<rgw_uio*>(alloca(uiosz));				\
    memset(uio, 0, uiosz);						\
    uio->uio_vio = reinterpret_cast<rgw_vio*>(uio+sizeof(rgw_uio));	\
  } while (0);								\

TEST(LibRGW, WRITEV)
{
  if (do_writev) {
    rgw_uio* uio;
    struct iovec *iovs = zp_set1.get_iovs();
    alloca_uio();
    ASSERT_NE(uio, nullptr);

    for (int ix = 0; ix < iovcnt; ++ix) {
      struct iovec *iov = &iovs[ix];
      rgw_vio *vio = &(uio->uio_vio[ix]);
      vio->vio_base = iov->iov_base;
      vio->vio_len = iov->iov_len;
      vio->vio_u1 = iov; // private data
    }
    uio->uio_cnt = iovcnt;
    uio->uio_offset = iovcnt * page_size;

    int ret = rgw_writev(fs, object_fh, uio, RGW_WRITE_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, READV)
{
  if (do_readv) {
    memset(uio, 0, sizeof(rgw_uio));
    uio->uio_offset = 0; // ok, it was already 0
    uio->uio_resid = UINT64_MAX;
    int ret = rgw_readv(fs, object_fh, uio, RGW_READ_FLAG_NONE);
    ASSERT_EQ(ret, 0);
    buffer::list bl;
    for (unsigned int ix = 0; ix < uio->uio_cnt; ++ix) {
      rgw_vio *vio = &(uio->uio_vio[ix]);
      bl.push_back(
	buffer::create_static(vio->vio_len,
			      static_cast<char*>(vio->vio_base)));
    }

    /* length check */
    ASSERT_EQ(uint32_t{bl.length()}, uint32_t{iovcnt*page_size});

    if (do_hexdump) {
      dout(15) << "";
      bl.hexdump(*_dout);
      *_dout << dendl;
    }
  }
}

TEST(LibRGW, READV_AFTER_WRITEV)
{
  /* checksum data */
  if (do_readv && do_writev && do_verify) {
    ASSERT_TRUE(zp_set1 == uio);
  }
}

TEST(LibRGW, DELETE_OBJECT) {
  if (do_delete) {
    int ret = rgw_unlink(fs, bucket_fh, object_name.c_str(),
			 RGW_UNLINK_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, CLEANUP) {
  if (do_readv) {
    // release resources
    ASSERT_NE(uio->uio_rele, nullptr);
    if (uio->uio_rele) {
      uio->uio_rele(uio, RGW_UIO_NONE);
    }
  }
  int ret;
  if (object_open) {
    ret = rgw_close(fs, object_fh, RGW_CLOSE_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
  if (object_fh) {
    ret = rgw_fh_rele(fs, object_fh, 0 /* flags */);
    ASSERT_EQ(ret, 0);
  }
  ret = rgw_fh_rele(fs, bucket_fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs, RGW_UMOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, SHUTDOWN) {
  librgw_shutdown(rgw);
}

int main(int argc, char *argv[])
{
  char *v{nullptr};
  string val;
  vector<const char*> args;

  argv_to_vec(argc, const_cast<const char**>(argv), args);
  env_to_vec(args);

  v = getenv("AWS_ACCESS_KEY_ID");
  if (v) {
    access_key = v;
  }

  v = getenv("AWS_SECRET_ACCESS_KEY");
  if (v) {
    secret_key = v;
  }

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_witharg(args, arg_iter, &val, "--access",
			      (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--secret",
				     (char*) nullptr)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      uid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_flag(args, arg_iter, "--get",
					    (char*) nullptr)) {
      do_get = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--stat",
					    (char*) nullptr)) {
      do_stat = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--put",
					    (char*) nullptr)) {
      do_put = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--bulk",
					    (char*) nullptr)) {
      do_bulk = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--writev",
					    (char*) nullptr)) {
      do_writev = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--readv",
					    (char*) nullptr)) {
      do_readv = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--verify",
					    (char*) nullptr)) {
      do_verify = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete",
					    (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--prelist",
					    (char*) nullptr)) {
      do_pre_list = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--hexdump",
					    (char*) nullptr)) {
      do_hexdump = true;
    } else {
      ++arg_iter;
    }
  }

  /* dont accidentally run as anonymous */
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
