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
#include <fstream>
#include <stack>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw/rgw_file.h"
#include "rgw/rgw_lib_frontend.h" // direct requests

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw;
  using std::get;
  using std::string;

  librgw_t rgw_h = nullptr;
  string userid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;
  CephContext* cct = nullptr;

  uint32_t owner_uid = 867;
  uint32_t owner_gid = 5309;

  uint32_t magic_uid = 1701;
  uint32_t magic_gid = 9876;

  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

  string bucket_name("nfsroot");
  string dirs1_bucket_name("bdirs1");
  string readf_name("toyland");
  string readf_out_name("rgwlib_readf.out");
  std::string writef_name{"bigbird"};

  int n_dirs1_dirs = 3;
  int n_dirs1_objs = 2;

  class obj_rec
  {
  public:
    string name;
    struct rgw_file_handle* fh;
    struct rgw_file_handle* parent_fh;
    RGWFileHandle* rgw_fh; // alias into fh

    struct state {
      bool readdir;
      state() : readdir(false) {}
    } state;

    obj_rec(string _name, struct rgw_file_handle* _fh,
	    struct rgw_file_handle* _parent_fh, RGWFileHandle* _rgw_fh)
      : name(std::move(_name)), fh(_fh), parent_fh(_parent_fh),
	rgw_fh(_rgw_fh) {}

    void clear() {
      fh = nullptr;
      rgw_fh = nullptr;
    }

    void sync() {
      if (fh)
	rgw_fh = get_rgwfh(fh);
    }

    friend ostream& operator<<(ostream& os, const obj_rec& rec);
  };

  ostream& operator<<(ostream& os, const obj_rec& rec)
  {
    RGWFileHandle* rgw_fh = rec.rgw_fh;
    if (rgw_fh) {
      const char* type = rgw_fh->is_dir() ? "DIR " : "FILE ";
      os << rec.rgw_fh->full_object_name()
	 << " (" << rec.rgw_fh->object_name() << "): "
	 << type;
    }
    return os;
  }
  
  std::stack<obj_rec> obj_stack;
  std::deque<obj_rec> cleanup_queue;

  typedef std::vector<obj_rec> obj_vec;
  typedef std::tuple<obj_rec, obj_vec> dirs1_rec;
  typedef std::vector<dirs1_rec> dirs1_vec;

  dirs1_vec dirs_vec;

  struct obj_rec_st
  {
    const obj_rec& obj;
    const struct stat& st;

    obj_rec_st(const obj_rec& _obj, const struct stat& _st)
      : obj(_obj), st(_st) {}
  };

  ostream& operator<<(ostream& os, const obj_rec_st& rec)
  {
    RGWFileHandle* rgw_fh = rec.obj.rgw_fh;
    if (rgw_fh) {
      const char* type = rgw_fh->is_dir() ? "DIR " : "FILE ";
      os << rgw_fh->full_object_name()
	 << " (" << rgw_fh->object_name() << "): "
	 << type;
      const struct stat& st = rec.st;
      switch(uint8_t(rgw_fh->is_dir())) {
      case 1:
	os << " mode: " << st.st_mode;
	os << " nlinks: " << st.st_nlink;
	break;
      case 0:
      default:
	os << " mode: " << st.st_mode;
	os << " size: " << st.st_size;
	// xxx
	break;
      }
    }
    return os;
  }

  bool do_hier1 = false;
  bool do_dirs1 = false;
  bool do_readf = false;
  bool do_writef = false;
  bool do_marker1 = false;
  bool do_create = false;
  bool do_delete = false;
  bool do_rename = false;
  bool do_setattr = false;
  bool verbose = false;

  string marker_dir("nfs_marker");
  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *marker_fh;
  static constexpr int marker_nobjs = 2*1024;
  std::deque<obj_rec> marker_objs;

  using dirent_t = std::tuple<std::string, uint64_t>;
  struct dirent_vec
  {
    std::vector<dirent_t> obj_names;
    uint32_t count;
    dirent_vec() : count(0) {}
  };

  obj_rec dirs1_b{dirs1_bucket_name, nullptr, nullptr, nullptr};

  dirs1_vec renames_vec;

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw_h, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw_h, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount(rgw_h, userid.c_str(), access_key.c_str(),
		      secret_key.c_str(), &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);

  cct = static_cast<RGWLibFS*>(fs->fs_private)->get_context();
}

TEST(LibRGW, SETUP_HIER1)
{
  if (do_hier1) {
    (void) rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		      RGW_LOOKUP_FLAG_NONE);
    if (! bucket_fh) {
      if (do_create) {
	struct stat st;

	st.st_uid = owner_uid;
	st.st_gid = owner_gid;
	st.st_mode = 755;

	int rc = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), &st,
			  create_mask, &bucket_fh, RGW_MKDIR_FLAG_NONE);
	ASSERT_EQ(rc, 0);
      }
    }

    ASSERT_NE(bucket_fh, nullptr);

    if (do_create) {
      /* create objects directly */
      std::vector<std::string> obj_names =
	{"foo/bar/baz/quux",
	 "foo/f1",
	 "foo/f2",
	 "foo/bar/f1",
	 "foo/bar/d1/",
	 "foo/bar/baz/hungry",
	 "foo/bar/baz/hungry/",
	 "foo/bar/baz/momma",
	 "foo/bar/baz/bear/",
	 "foo/bar/baz/sasquatch",
	 "foo/bar/baz/sasquatch/",
	 "foo/bar/baz/frobozz"};

      buffer::list bl; // empty object
      RGWLibFS *fs_private = static_cast<RGWLibFS*>(fs->fs_private);

      for (const auto& obj_name : obj_names) {
	if (verbose) {
	  std::cout << "creating: " << bucket_name << ":" << obj_name
		    << std::endl;
	}
	RGWPutObjRequest req(cct, fs_private->get_user(), bucket_name, obj_name,
			    bl);
	int rc = rgwlib.get_fe()->execute_req(&req);
	int rc2 = req.get_ret();
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(rc2, 0);
      }
    }
  }
}

TEST(LibRGW, SETUP_DIRS1) {
  if (do_dirs1) {
    int rc;
    struct stat st;

    st.st_uid = owner_uid;
    st.st_gid = owner_gid;
    st.st_mode = 755;

    dirs1_b.parent_fh = fs->root_fh;

    (void) rgw_lookup(fs, dirs1_b.parent_fh, dirs1_bucket_name.c_str(),
		      &dirs1_b.fh, RGW_LOOKUP_FLAG_NONE);

    if (! dirs1_b.fh) {
      if (do_create) {
	rc = rgw_mkdir(fs, dirs1_b.parent_fh, dirs1_b.name.c_str(), &st,
		      create_mask, &dirs1_b.fh, RGW_MKDIR_FLAG_NONE);
	ASSERT_EQ(rc, 0);
      } else {
	/* no top-level dir and can't create it--skip remaining tests */
	return;
      }
    }
    dirs1_b.sync();

    /* make top-level dirs */
    int d_ix;
    obj_vec ovec;
    for (d_ix = 0; d_ix < n_dirs1_dirs; ++d_ix) {
      std::string dname{"dir_"};
      dname += to_string(d_ix);
      obj_rec dir{dname, nullptr, dirs1_b.fh, nullptr};
      ovec.clear();

      (void) rgw_lookup(fs, dir.parent_fh, dir.name.c_str(), &dir.fh,
			RGW_LOOKUP_FLAG_NONE);
      if (! dir.fh) {
	if (do_create) {
	  rc = rgw_mkdir(fs, dir.parent_fh, dir.name.c_str(), &st, create_mask,
			 &dir.fh, RGW_MKDIR_FLAG_NONE);
	  ASSERT_EQ(rc, 0);
	}
      }

      ASSERT_NE(dir.fh, nullptr);
      dir.sync();
      ASSERT_NE(dir.rgw_fh, nullptr);
      ASSERT_TRUE(dir.rgw_fh->is_dir());

      int f_ix;
      for (f_ix = 0; f_ix < n_dirs1_objs; ++f_ix) {
	/* child dir */
	std::string sdname{"sdir_"};
	sdname += to_string(f_ix);
	obj_rec sdir{sdname, nullptr, dir.fh, nullptr};

	(void) rgw_lookup(fs, sdir.parent_fh, sdir.name.c_str(), &sdir.fh,
			  RGW_LOOKUP_FLAG_NONE);

	if (! sdir.fh) {
	  if (do_create) {
	    rc = rgw_mkdir(fs, sdir.parent_fh, sdir.name.c_str(), &st,
			  create_mask, &sdir.fh, RGW_MKDIR_FLAG_NONE);
	    ASSERT_EQ(rc, 0);
	  }
	}

	ASSERT_NE(sdir.fh, nullptr); // suppress !lookup && !create case

	sdir.sync();
	ASSERT_TRUE(sdir.rgw_fh->is_dir());
	ovec.push_back(sdir);

	/* child file */
	std::string sfname{"sfile_"};

	sfname += to_string(f_ix);
	obj_rec sf{sfname, nullptr, dir.fh, nullptr};

	(void) rgw_lookup(fs, sf.parent_fh, sf.name.c_str(), &sf.fh,
			  RGW_LOOKUP_FLAG_NONE);

	if (! sf.fh) {
	  if (do_create) {
	    /* make a new file object (the hard way) */
	    rc = rgw_lookup(fs, sf.parent_fh, sf.name.c_str(), &sf.fh,
			    RGW_LOOKUP_FLAG_CREATE);
	    ASSERT_EQ(rc, 0);
	    sf.sync();
	    ASSERT_TRUE(sf.rgw_fh->is_file());

	    /* because we made it the hard way, fixup attributes */
	    struct stat st;
	    st.st_uid = owner_uid;
	    st.st_gid = owner_gid;
	    st.st_mode = 644;
	    sf.rgw_fh->create_stat(&st, create_mask);

	    /* open handle */
	    rc = rgw_open(fs, sf.fh, 0 /* posix flags */, 0 /* flags */);
	    ASSERT_EQ(rc, 0);
	    ASSERT_TRUE(sf.rgw_fh->is_open());
	    /* stage seq write */
	    size_t nbytes;
	    string data = "data for " + sf.name;
	    rc = rgw_write(fs, sf.fh, 0, data.length(), &nbytes,
			   (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
	    ASSERT_EQ(rc, 0);
	    ASSERT_EQ(nbytes, data.length());
	    /* commit write transaction */
	    rc = rgw_close(fs, sf.fh, 0 /* flags */);
	    ASSERT_EQ(rc, 0);
	  }
	} else {
	  sf.sync();
	  ASSERT_TRUE(sf.rgw_fh->is_file());
	}
	
	if (sf.fh)
	  ovec.push_back(sf);
      }
      dirs_vec.push_back(dirs1_rec{dir, ovec});
    }
  } /* dirs1 top-level !exist */
}

TEST(LibRGW, SETATTR) {
  if (do_dirs1) {
    if (do_setattr) {

      int rc;
      struct stat st;

      st.st_uid = owner_uid;
      st.st_gid = owner_gid;
      st.st_mode = 755;

      std::string dname{"dir_0"};
      obj_rec dir{dname, nullptr, dirs1_b.fh, nullptr};

      /* dir_0 MUST exist and MUST be resident */
      (void) rgw_lookup(fs, dir.parent_fh, dir.name.c_str(), &dir.fh,
			RGW_LOOKUP_FLAG_NONE);

      ASSERT_NE(dir.fh, nullptr);
      dir.sync();
      ASSERT_NE(dir.rgw_fh, nullptr);
      ASSERT_TRUE(dir.rgw_fh->is_dir());

      /* child file */
      std::string sfname{"setattr_file_0"};
      obj_rec sf{sfname, nullptr, dir.fh, nullptr};

      (void) rgw_lookup(fs, sf.parent_fh, sf.name.c_str(), &sf.fh,
			RGW_LOOKUP_FLAG_NONE);

      if (! sf.fh) {
	/* make a new file object (the hard way) */
	rc = rgw_lookup(fs, sf.parent_fh, sf.name.c_str(), &sf.fh,
			RGW_LOOKUP_FLAG_CREATE);
	ASSERT_EQ(rc, 0);
	sf.sync();
	ASSERT_TRUE(sf.rgw_fh->is_file());

	/* because we made it the hard way, fixup attributes */
	st.st_uid = owner_uid;
	st.st_gid = owner_gid;
	st.st_mode = 644;
	sf.rgw_fh->create_stat(&st, create_mask);

	/* open handle */
	rc = rgw_open(fs, sf.fh, 0 /* posix flags */, 0 /* flags */);
	ASSERT_EQ(rc, 0);
	ASSERT_TRUE(sf.rgw_fh->is_open());
	/* stage seq write */
	size_t nbytes;
	string data = "data for " + sf.name;
	rc = rgw_write(fs, sf.fh, 0, data.length(), &nbytes,
		      (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(nbytes, data.length());
	/* commit write transaction */
	rc = rgw_close(fs, sf.fh, 0 /* flags */);
	ASSERT_EQ(rc, 0);
      } else {
	sf.sync();
	ASSERT_TRUE(sf.rgw_fh->is_file());
      }

      /* sf MUST now be materialized--now change it's attributes */
      st.st_uid = magic_uid;
      st.st_gid = magic_gid;

      rc = rgw_setattr(fs, sf.fh, &st, create_mask, RGW_SETATTR_FLAG_NONE);
      ASSERT_EQ(rc, 0);

      /* force evict--subsequent lookups must reload */
      static_cast<RGWLibFS*>(fs->fs_private)->release_evict(sf.rgw_fh);

      sf.clear();

      /* revalidate -- expect magic uid and gid */
      (void) rgw_lookup(fs, sf.parent_fh, sf.name.c_str(), &sf.fh,
			RGW_LOOKUP_FLAG_NONE);
      sf.sync();
      ASSERT_NE(sf.fh, nullptr);

      memset(&st, 0, sizeof(struct stat)); /* nothing up my sleeve... */

      rc =  rgw_getattr(fs, sf.fh, &st, RGW_GETATTR_FLAG_NONE);
      ASSERT_EQ(rc, 0);

      ASSERT_EQ(st.st_uid, magic_uid);
      ASSERT_EQ(st.st_gid, magic_gid);

      /* release 1 ref on sf */
      rgw_fh_rele(fs, sf.fh, RGW_FH_RELE_FLAG_NONE);

      /* release 1 ref on dir */
      rgw_fh_rele(fs, dir.fh, RGW_FH_RELE_FLAG_NONE);
    } /* dirs1 */
  }
}

TEST(LibRGW, RGW_CREATE_DIRS1) {
  /* verify rgw_create (create [empty] file objects the easy way) */
  if (do_dirs1) {
    if (do_create) {
      int rc;
      struct stat st;

      st.st_uid = owner_uid;
      st.st_gid = owner_gid;
      st.st_mode = 644;

      for (auto& dirs_rec : dirs_vec) {
	/* create 1 more file in each sdir */
	obj_rec& dir = get<0>(dirs_rec);
	std::string sfname{"sfile_" + to_string(n_dirs1_objs)};
	obj_rec sf{sfname, nullptr, dir.fh, nullptr};
	(void) rgw_lookup(fs, sf.parent_fh, sf.name.c_str(), &sf.fh,
			  RGW_LOOKUP_FLAG_NONE);
	if (! sf.fh) {
	  rc = rgw_create(fs, sf.parent_fh, sf.name.c_str(), &st, create_mask,
			  &sf.fh, 0 /* posix flags */, RGW_CREATE_FLAG_NONE);
	  ASSERT_EQ(rc, 0);
	}
	sf.sync();
      }
      n_dirs1_objs++;
    }
  }
}

TEST(LibRGW, RGW_SETUP_RENAME1) {
  /* verify rgw_create (create [empty] file objects the easy way) */
  if (do_rename) {
    int rc;
    struct stat st;
    obj_vec ovec;

    st.st_uid = owner_uid;
    st.st_gid = owner_gid;
    st.st_mode = 755;

    for (int b_ix : {0, 1}) {
      std::string bname{"brename_" + to_string(b_ix)};
      obj_rec brec{bname, nullptr, nullptr, nullptr};
      (void) rgw_lookup(fs, fs->root_fh, brec.name.c_str(), &brec.fh,
			RGW_LOOKUP_FLAG_NONE);
      if (! brec.fh) {
	if (do_create) {
	  struct stat st;
	  int rc = rgw_mkdir(fs, fs->root_fh, brec.name.c_str(), &st,
			    create_mask, &brec.fh, RGW_MKDIR_FLAG_NONE);
	  ASSERT_EQ(rc, 0);
	}
      }
      ASSERT_NE(brec.fh, nullptr);
      brec.sync();

      st.st_mode = 644; /* file mask */

      for (int f_ix : {0, 1}) {
	std::string rfname{"rfile_"};
	rfname += to_string(f_ix);
	obj_rec rf{rfname, nullptr, brec.fh, nullptr};
	(void) rgw_lookup(fs, rf.parent_fh, rf.name.c_str(), &rf.fh,
			  RGW_LOOKUP_FLAG_NONE);
	if (! rf.fh) {
	  rc = rgw_create(fs, rf.parent_fh, rf.name.c_str(), &st, create_mask,
			  &rf.fh, 0 /* posix flags */, RGW_CREATE_FLAG_NONE);
	  ASSERT_EQ(rc, 0);
	}
	rf.sync();
	ovec.push_back(rf);
      }
      renames_vec.push_back(dirs1_rec{brec, ovec});
      ovec.clear();
    } /* b_ix */
  }
}

TEST(LibRGW, RGW_INTRABUCKET_RENAME1) {
  /* rgw_rename a file within a bucket */
  if (do_rename) {
    int rc;
    obj_rec& bdir0 = get<0>(renames_vec[0]);
    obj_rec& src_obj = get<1>(renames_vec[0])[0];
    std::string rfname{"rfile_r0"};
    if (verbose) {
      std::cout << "rename file " << src_obj.name << " to "
		<< rfname << " (bucket " << bdir0.name << ")"
		<< std::endl;
    }
    rc = rgw_rename(fs, bdir0.fh, src_obj.name.c_str(), bdir0.fh,
		    rfname.c_str(), RGW_RENAME_FLAG_NONE);
    ASSERT_EQ(rc, 0);
  }
}

TEST(LibRGW, RGW_CROSSBUCKET_RENAME1) {
  /* rgw_rename a file within a bucket */
  if (do_rename) {
    int rc;
    obj_rec& bdir0 = get<0>(renames_vec[0]);
    obj_rec& bdir1 = get<0>(renames_vec[1]);
    obj_rec& src_obj = get<1>(renames_vec[0])[1];
    std::string rfname{"rfile_rhilldog"};
    if (verbose) {
      std::cout << "rename file " << src_obj.name
		<< " (bucket " << bdir0.name << ") to "
		<< rfname << " (bucket " << bdir1.name << ")"
		<< std::endl;
    }
    rc = rgw_rename(fs, bdir0.fh, src_obj.name.c_str(), bdir1.fh,
		    rfname.c_str(), RGW_RENAME_FLAG_NONE);
    ASSERT_EQ(rc, 0);
  }
}

TEST(LibRGW, BAD_DELETES_DIRS1) {
  if (do_dirs1) {
    int rc;

    if (dirs_vec.size() == 0) {
      /* skip */
      return;
    }

    if (do_delete) {
      /* try to unlink a non-empty directory (bucket) */
      rc = rgw_unlink(fs, dirs1_b.parent_fh, dirs1_b.name.c_str(),
		      RGW_UNLINK_FLAG_NONE);
      ASSERT_NE(rc, 0);
    }
    /* try to unlink a non-empty directory (non-bucket) */
    obj_rec& sdir_0 = get<1>(dirs_vec[0])[0];
    ASSERT_EQ(sdir_0.name, "sdir_0");
    ASSERT_TRUE(sdir_0.rgw_fh->is_dir());
    /* XXX we can't enforce this currently */
#if 0
    ASSERT_EQ(sdir_0.name, "sdir_0");
    ASSERT_TRUE(sdir_0.rgw_fh->is_dir());
    rc = rgw_unlink(fs, sdir_0.parent_fh, sdir_0.name.c_str(),
		    RGW_UNLINK_FLAG_NONE);
    ASSERT_NE(rc, 0);
#endif
  }
}

TEST(LibRGW, GETATTR_DIRS1)
{
  if (do_dirs1) {
    int rc;
    struct stat st;
    for (auto& dirs_rec : dirs_vec) {
      obj_rec& dir = get<0>(dirs_rec);
      if (verbose) {
	std::cout << "scanning objects in "
		  << dir.rgw_fh->full_object_name()
		  << std::endl;
      }
      for (auto& sobj : get<1>(dirs_rec)) {
	rc = rgw_getattr(fs, sobj.fh, &st, RGW_GETATTR_FLAG_NONE);
	ASSERT_EQ(rc, 0);
	/* validate, pretty-print */
	if (sobj.rgw_fh->object_name().find("sfile") != std::string::npos) {
	  ASSERT_TRUE(sobj.rgw_fh->is_file());
	  ASSERT_TRUE(S_ISREG(st.st_mode));
	}
	if (sobj.rgw_fh->object_name().find("sdir") != std::string::npos) {
	  ASSERT_TRUE(sobj.rgw_fh->is_dir());
	  ASSERT_TRUE(S_ISDIR(st.st_mode));
	}
	/* validate Unix owners */
	ASSERT_EQ(st.st_uid, owner_uid);
	ASSERT_EQ(st.st_gid, owner_gid);
	if (verbose) {
	  obj_rec_st rec_st{sobj, st};
	  std::cout << "\t"
		    << rec_st
		    << std::endl;
	}
      }
    }
  }
}

TEST(LibRGW, READ_DIRS1)
{
  if (do_dirs1) {
    int rc;
    char buf[256];
    size_t nread;
    for (auto& dirs_rec : dirs_vec) {
      obj_rec& dir = get<0>(dirs_rec);
      if (verbose) {
	std::cout << "read back objects in "
		  << dir.rgw_fh->full_object_name()
		  << std::endl;
      }
      for (auto& sobj : get<1>(dirs_rec)) {
	/* only the first 2 file objects have data */
	if ((sobj.rgw_fh->object_name().find("sfile_0")
	     != std::string::npos) ||
	    (sobj.rgw_fh->object_name().find("sfile_1")
	     != std::string::npos)) {
	  ASSERT_TRUE(sobj.rgw_fh->is_file());
	  ASSERT_EQ(sobj.rgw_fh->get_size(), 16UL);
	  // do it
	  memset(buf, 0, 256);
	  if (verbose) {
	    std::cout << "reading 0,256 " << sobj.rgw_fh->relative_object_name()
		      << std::endl;
	  }
	  rc = rgw_read(fs, sobj.fh, 0, 256, &nread, buf, RGW_READ_FLAG_NONE);
	  ASSERT_EQ(rc, 0);
	  if (verbose) {
	    std::cout << "\tread back from " << sobj.name
		      << " : \"" << buf << "\""
		      << std::endl;
	  }
	}
      }
    }
  }
}

TEST(LibRGW, READF_SETUP1)
{
  struct stat st;
  if (do_dirs1) {
    if (do_create) {
      if ((! stat(readf_out_name.c_str(), &st)) &&
	  (S_ISREG(st.st_mode)) &&
	  (st.st_size == 6291456))
	return;
      ofstream of;
      of.open(readf_out_name, ios::out|ios::app|ios::binary);
      for (int ix1 = 0; ix1 < 6; ++ix1) {
	for (int ix2 = 0; ix2 < 1024*1024; ++ix2) {
	  of << ix1;
	}
      }
    }
  }
}

TEST(LibRGW, READF_DIRS1) {
  if (do_dirs1) {
    if (do_readf) {
      obj_rec fobj{readf_name, nullptr, dirs1_b.fh, nullptr};

      int rc = rgw_lookup(fs, dirs1_b.fh, fobj.name.c_str(), &fobj.fh,
			  RGW_LOOKUP_FLAG_NONE);
      ASSERT_EQ(rc, 0);
      ASSERT_NE(fobj.fh, nullptr);
      fobj.sync();

      ofstream of;
      of.open(readf_out_name, ios::out|ios::app|ios::binary);
      int bufsz = 1024 * 1024 * sizeof(char);
      char *buffer = (char*) malloc(bufsz);

      uint64_t offset = 0;
      uint64_t length = bufsz;
      for (int ix = 0; ix < 6; ++ix) {
	size_t nread = 0;
	memset(buffer, 0, length); // XXX
	rc = rgw_read(fs, fobj.fh, offset, length, &nread, buffer,
		      RGW_READ_FLAG_NONE);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(nread, length);
	of.write(buffer, length);
	offset += nread;
      }
      of.close();
      free(buffer);
      rgw_fh_rele(fs, fobj.fh, 0 /* flags */);
    }
  }
}

TEST(LibRGW, WRITEF_DIRS1) {
  if (do_dirs1) {
    if (do_writef) {
      int rc;
      ifstream ifs;
      ifs.open(readf_out_name, ios::out|ios::app|ios::binary);
      ASSERT_TRUE(ifs.is_open());

      obj_rec fobj{writef_name, nullptr, dirs1_b.fh, nullptr};

      (void) rgw_lookup(fs, fobj.parent_fh, fobj.name.c_str(), &fobj.fh,
			RGW_LOOKUP_FLAG_NONE);
      if (! fobj.fh) {
	if (do_create) {
	  /* make a new file object (the hard way) */
	  rc = rgw_lookup(fs, fobj.parent_fh, fobj.name.c_str(), &fobj.fh,
			  RGW_LOOKUP_FLAG_CREATE);
	  ASSERT_EQ(rc, 0);
	}
      }
      ASSERT_NE(fobj.fh, nullptr);
      fobj.sync();

      /* begin write transaction */
      rc = rgw_open(fs, fobj.fh, 0 /* posix flags */, 0 /* flags */);
      ASSERT_EQ(rc, 0);
      ASSERT_TRUE(fobj.rgw_fh->is_open());

      int bufsz = 1024 * 1024 * sizeof(char);
      char *buffer = (char*) malloc(bufsz);

      uint64_t offset = 0;
      uint64_t length = bufsz;
      for (int ix = 0; ix < 6; ++ix) {
	ASSERT_TRUE(ifs.good());
	ifs.read(buffer, bufsz);
	size_t nwritten = 0;
	string str;
	str.assign(buffer, 4);
	if (verbose) {
	  std::cout << "read and writing " << length << " bytes"
		    << " from " << readf_out_name
		    << " at offset " << offset
		    << " (" << str << "... [first 4 chars])"
		    << std::endl;
	}
	char* leakbuf = (char*) malloc(bufsz);
	memcpy(leakbuf, buffer, length);
	rc = rgw_write(fs, fobj.fh, offset, length, &nwritten, leakbuf,
		      RGW_WRITE_FLAG_NONE);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(nwritten, length);
	offset += length;
      }

      /* commit write transaction */
      rc = rgw_close(fs, fobj.fh, RGW_CLOSE_FLAG_NONE);
      ASSERT_EQ(rc, 0);

      ifs.close();
      free(buffer);
      rgw_fh_rele(fs, fobj.fh, 0 /* flags */);
    }
  }
}

TEST(LibRGW, RELEASE_DIRS1) {
  if (do_dirs1) {
    /* force release of handles for children of dirs1--force subsequent
     * checks to reload them from the cluster.
     *
     * while doing this, verify handle cleanup and correct LRU state
     * (not reachable)
     */
    int rc;
    for (auto& dirs_rec : dirs_vec) {
      for (auto& obj : get<1>(dirs_rec)) {
	if (verbose) {
	  std::cout << "release " << obj.name
		    << " type: " << obj.rgw_fh->stype()
		    << " refs: " << obj.rgw_fh->get_refcnt()
		    << std::endl;
	}
	ASSERT_EQ(obj.rgw_fh->get_refcnt(), 2UL);
	rc = rgw_fh_rele(fs, obj.fh, 0 /* flags */);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(obj.rgw_fh->get_refcnt(), 1UL);
	/* try-discard handle */
	/* clear obj_rec vec */
      }
    }
  }
}

extern "C" {
  static bool r1_cb(const char* name, void *arg, uint64_t offset,
		    uint32_t flags) {
    struct rgw_file_handle* parent_fh
      = static_cast<struct rgw_file_handle*>(arg);
    RGWFileHandle* rgw_fh = get_rgwfh(parent_fh);
    lsubdout(cct, rgw, 10) << __func__
			   << " bucket=" << rgw_fh->bucket_name()
			   << " dir=" << rgw_fh->full_object_name()
			   << " called back name=" << name
			   << " flags=" << flags
			   << dendl;
    string name_str{name};
    if (! ((name_str == ".") ||
	   (name_str == ".."))) {
      obj_stack.push(
	obj_rec{std::move(name_str), nullptr, parent_fh, nullptr});
    }
    return true; /* XXX */
  }
}

TEST(LibRGW, HIER1) {
  if (do_hier1) {
    int rc;
    obj_stack.push(
      obj_rec{bucket_name, nullptr, nullptr, nullptr});
    while (! obj_stack.empty()) {
      auto& elt = obj_stack.top();
      if (! elt.fh) {
	struct rgw_file_handle* parent_fh = elt.parent_fh
	  ? elt.parent_fh : fs->root_fh;
	RGWFileHandle* pfh = get_rgwfh(parent_fh);
	rgw::ignore(pfh);
	lsubdout(cct, rgw, 10)
	  << "rgw_lookup:"
	  << " parent object_name()=" << pfh->object_name()
	  << " parent full_object_name()=" << pfh->full_object_name()
	  << " elt.name=" << elt.name
	  << dendl;
	rc = rgw_lookup(fs, parent_fh, elt.name.c_str(), &elt.fh,
			RGW_LOOKUP_FLAG_NONE);
	ASSERT_EQ(rc, 0);
	// XXXX
	RGWFileHandle* efh = get_rgwfh(elt.fh);
	rgw::ignore(efh);
	lsubdout(cct, rgw, 10)
	  << "rgw_lookup result:"
	  << " elt object_name()=" << efh->object_name()
	  << " elt full_object_name()=" << efh->full_object_name()
	  << " elt.name=" << elt.name
	  << dendl;

	ASSERT_NE(elt.fh, nullptr);
	elt.rgw_fh = get_rgwfh(elt.fh);
	elt.parent_fh = elt.rgw_fh->get_parent()->get_fh();
	ASSERT_EQ(elt.parent_fh, parent_fh);
	continue;
      } else {
	// we have a handle in some state in top position
	switch(elt.fh->fh_type) {
	case RGW_FS_TYPE_DIRECTORY:
	  if (! elt.state.readdir) {
	    // descending
	    uint64_t offset = 0;
	    bool eof; // XXX
	    lsubdout(cct, rgw, 10)
	      << "readdir in"
	      << " bucket: " << elt.rgw_fh->bucket_name()
	      << " object_name: " << elt.rgw_fh->object_name()
	      << " full_name: " << elt.rgw_fh->full_object_name()
	      << dendl;
	    rc = rgw_readdir(fs, elt.fh, &offset, r1_cb, elt.fh, &eof,
			    RGW_READDIR_FLAG_DOTDOT);
	    elt.state.readdir = true;
	    ASSERT_EQ(rc, 0);
	    // ASSERT_TRUE(eof); // XXXX working incorrectly w/single readdir
	  } else {
	    // ascending
	    std::cout << elt << std::endl;
	    cleanup_queue.push_back(elt);
	    obj_stack.pop();
	  }
	  break;
	case RGW_FS_TYPE_FILE:
	  // ascending
	  std::cout << elt << std::endl;
	  cleanup_queue.push_back(elt);
	  obj_stack.pop();
	  break;
	default:
	  abort();
	};
      }
    }
  }
}

TEST(LibRGW, MARKER1_SETUP_BUCKET) {
  /* "large" directory enumeration test.  this one deals only with
   * file objects */
  if (do_marker1) {
    struct stat st;
    int ret;

    st.st_uid = owner_uid;
    st.st_gid = owner_gid;
    st.st_mode = 755;

    if (do_create) {
      ret = rgw_mkdir(fs, bucket_fh, marker_dir.c_str(), &st, create_mask,
		      &marker_fh, RGW_MKDIR_FLAG_NONE);
    } else {
      ret = rgw_lookup(fs, bucket_fh, marker_dir.c_str(), &marker_fh,
		       RGW_LOOKUP_FLAG_NONE);
    }
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, MARKER1_SETUP_OBJECTS)
{
  /* "large" directory enumeration test.  this one deals only with
   * file objects */
  if (do_marker1 && do_create) {
    int ret;

    for (int ix = 0; ix < marker_nobjs; ++ix) {
      std::string object_name("f_");
      object_name += to_string(ix);
      obj_rec obj{object_name, nullptr, marker_fh, nullptr};
      // lookup object--all operations are by handle
      ret = rgw_lookup(fs, marker_fh, obj.name.c_str(), &obj.fh,
		       RGW_LOOKUP_FLAG_CREATE);
      ASSERT_EQ(ret, 0);
      obj.rgw_fh = get_rgwfh(obj.fh);
      // open object--open transaction
      ret = rgw_open(fs, obj.fh, 0 /* posix flags */, RGW_OPEN_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_TRUE(obj.rgw_fh->is_open());
      // unstable write data
      size_t nbytes;
      string data("data for ");
      data += object_name;
      int ret = rgw_write(fs, obj.fh, 0, data.length(), &nbytes,
			  (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(nbytes, data.length());
      // commit transaction (write on close)
      ret = rgw_close(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);
      // save for cleanup
      marker_objs.push_back(obj);
    }
  }
}

extern "C" {
  static bool r2_cb(const char* name, void *arg, uint64_t offset,
		    uint32_t flags) {
    dirent_vec& dvec =
      *(static_cast<dirent_vec*>(arg));
    lsubdout(cct, rgw, 10) << __func__
			   << " bucket=" << bucket_name
			   << " dir=" << marker_dir
			   << " iv count=" << dvec.count
			   << " called back name=" << name
			   << " flags=" << flags
			   << dendl;
    string name_str{name};
    if (! ((name_str == ".") ||
	   (name_str == ".."))) {
      dvec.obj_names.push_back(dirent_t{std::move(name_str), offset});
    }
    return true; /* XXX */
  }
}

TEST(LibRGW, MARKER1_READDIR)
{
  if (do_marker1) {
    using std::get;

    dirent_vec dvec;
    uint64_t offset = 0;
    bool eof = false;

    /* because RGWReaddirRequest::default_max is 1000 (XXX make
     * configurable?) and marker_nobjs is 5*1024, the number
     * of required rgw_readdir operations N should be
     * marker_nobjs/1000 < N < marker_nobjs/1000+1, i.e., 6 when
     * marker_nobjs==5*1024 */
    uint32_t max_iterations = marker_nobjs/1000+1;

    do {
      ASSERT_TRUE(dvec.count <= max_iterations);
      int ret = rgw_readdir(fs, marker_fh, &offset, r2_cb, &dvec, &eof,
			    RGW_READDIR_FLAG_DOTDOT);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(offset, get<1>(dvec.obj_names.back())); // cookie check
      ++dvec.count;
    } while(!eof);
    std::cout << "Read " << dvec.obj_names.size() << " objects in "
	      << marker_dir.c_str() << std::endl;
  }
}

TEST(LibRGW, MARKER1_OBJ_CLEANUP)
{
  int rc;
  for (auto& obj : marker_objs) {
    if (obj.fh) {
      if (do_delete) {
	if (verbose) {
	  std::cout << "unlinking: " << bucket_name << ":" << obj.name
		    << std::endl;
	}
	rc = rgw_unlink(fs, marker_fh, obj.name.c_str(), RGW_UNLINK_FLAG_NONE);
      }
      rc = rgw_fh_rele(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(rc, 0);
    }
  }
  marker_objs.clear();
}

TEST(LibRGW, CLEANUP) {
  int rc;

  if (do_marker1) {
    cleanup_queue.push_back(
      obj_rec{bucket_name, bucket_fh, fs->root_fh, get_rgwfh(fs->root_fh)});
  }

  for (auto& elt : cleanup_queue) {
    if (elt.fh) {
      rc = rgw_fh_rele(fs, elt.fh, 0 /* flags */);
      ASSERT_EQ(rc, 0);
    }
  }
  cleanup_queue.clear();
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs, RGW_UMOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, SHUTDOWN) {
  librgw_shutdown(rgw_h);
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
    } else if (ceph_argparse_flag(args, arg_iter, "--hier1",
					    (char*) nullptr)) {
      do_hier1 = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--dirs1",
					    (char*) nullptr)) {
      do_dirs1 = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--marker1",
					    (char*) nullptr)) {
      do_marker1 = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--setattr",
					    (char*) nullptr)) {
      do_setattr = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
					    (char*) nullptr)) {
      do_create = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete",
					    (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--rename",
					    (char*) nullptr)) {
      do_rename = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--readf",
					    (char*) nullptr)) {
      do_readf = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--writef",
					    (char*) nullptr)) {
      do_writef = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--verbose",
					    (char*) nullptr)) {
      verbose = true;
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
