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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {
  librgw_t rgw = nullptr;
  string uid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;

  bool do_create = false;

  uint32_t owner_uid = 867;
  uint32_t owner_gid = 5309;
  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

  string bucket_name = "sdlc-test";

  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *object_fh = nullptr;

  std::vector<std::string> obj_names = {
      ".teste-c134272.txt.20260113110131.c134272",
      "/catalog/sitip.tiptb001_emitente.ddl.json",
      "/catalog/sitip.tiptb002_documento.ddl.json",
      "/catalog/sitip.tiptb004_ocorrencia_documento.ddl.json",
      "/catalog/sitip.tiptb005_evento.ddl.json",
      "/catalog/sitip.tiptb006_situacao_evento.ddl.json",
      "/catalog/sitip.tiptb007_ocorrencia_evento.ddl.json",
      "/catalog/sitip.tiptb008_lote_evento.ddl.json",
      "/catalog/sitip.tiptb009_processamento_lote.ddl.json",
      "/catalog/sitip.tiptb010_tipo_evento.ddl.json",
      "/catalog/sitip.tiptb011_periodo_movimento.ddl.json",
      "/catalog/sitip.tiptb013_ocorrencia_lote.ddl.json",
      "/catalog/sitip.tiptb014_acao_periodo_movto.ddl.json",
      "file",
      "my_schema_20260127/",
      "my_schema_20260127/test_table/data/"
      "35e9dbf6-10f2-44bf-9034-9c68aaedb54f.parquet",
      "my_schema_20260127/test_table/metadata/"
      "00000-6cb10a86-6b9e-446f-a662-38f403961f5d.metadata.json",
      "my_schema_20260127/test_table/metadata/"
      "00001-7d0c79e7-990a-4ed5-9650-e6c55d610e36.metadata.json",
      "my_schema_20260127/test_table/metadata/"
      "14521311-8384-4bac-acc8-44c653699c43-m0.avro",
      "my_schema_20260127/test_table/metadata/"
      "snap-7089720011427558043-1-14521311-8384-4bac-acc8-44c653699c43.avro",
      "my_schema_3/",
      "my_schema_3/test_table/data/"
      "f14e4a46-8d04-4d34-bbb9-298de3bbde7f.parquet",
      "my_schema_3/test_table/metadata/"
      "00000-c6fae701-5b4f-4b7c-911e-c7710614bbd1.metadata.json",
      "my_schema_3/test_table/metadata/"
      "00001-f8d6e35e-f2a8-42f5-9466-654b3405e8b6.metadata.json",
      "my_schema_3/test_table/metadata/"
      "213de0bb-5d74-4d21-98f2-c55ec15e89d8-m0.avro",
      "my_schema_3/test_table/metadata/"
      "snap-4490949654564070735-1-213de0bb-5d74-4d21-98f2-c55ec15e89d8.avro",
      "teste-c134272.txt"
      };

  typedef std::tuple<string,uint64_t, struct rgw_file_handle*> fid_type;
  std::vector<fid_type> fids;

  std::uniform_int_distribution<uint8_t> uint_dist;
  std::mt19937 rng;

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, INIT1) {
  int ret = librgw_create(&rgw, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw, nullptr);
}

TEST(LibRGW, MOUNT1) {
  int ret = rgw_mount2(rgw, uid.c_str(), access_key.c_str(), secret_key.c_str(),
                       "/", &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(LibRGW, CREATE_BUCKET) {
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

TEST(LibRGW, LOOKUP_BUCKET) {
  int ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, CREATE_OBJECTS) {
  if (do_create) {

    size_t nbytes;
    string data = "great justice";

    for (auto& obj_name : obj_names) {
      int ret = rgw_lookup(fs, bucket_fh, obj_name.c_str(), &object_fh,
                           nullptr, 0, RGW_LOOKUP_FLAG_CREATE);
      ASSERT_EQ(ret, 0);

      ret = rgw_open(fs, object_fh, 0 /* posix flags */, 0 /* flags */);
      ASSERT_EQ(ret, 0);

      ret = rgw_write(fs, object_fh, 0, data.length(), &nbytes,
                      (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(nbytes, data.length());
      /* commit write transaction */
      ret = rgw_close(fs, object_fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);
    }
  } /* create */
}

extern "C" {
  static int r2_cb(const char* name, void *arg, uint64_t offset,
		    struct stat *st, uint32_t st_mask,
		    uint32_t flags) {
    // don't need arg--it would point to fids
    fids.push_back(fid_type(name, offset, nullptr));
    return true; /* XXX ? */
  }
}

TEST(LibRGW, LIST_OBJECTS) {

  if (! do_create) {
    /* list objects via readdir, bucketwise */
    using std::get;

    ldout(g_ceph_context, 0)
        << __func__ << " readdir on bucket " << bucket_name << dendl;
    bool eof = false;
    uint64_t offset = 0;
    int ret = rgw_readdir(
        fs, bucket_fh, &offset, r2_cb, &fids, &eof, RGW_READDIR_FLAG_NONE);
    for (auto& fid : fids) {
      std::cout << "fname: " << get<0>(fid) << " fid: " << get<1>(fid)
                << std::endl;
    }
    ASSERT_EQ(ret, 0);
  } /* !create  */
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
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      uid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
					    (char*) nullptr)) {
      do_create = true;
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
