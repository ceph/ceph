// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <iostream>
#include <errno.h>

#include "include/types.h"
#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "gtest/gtest.h"

using namespace librados;

#include "cls/fifo/cls_fifo_client.h"


using namespace rados::cls::fifo;

static CephContext *cct(librados::IoCtx& ioctx)
{
  return reinterpret_cast<CephContext *>(ioctx.cct());
}

static int fifo_create(IoCtx& ioctx,
                       const string& oid,
                       const string& id,
                       const ClsFIFO::MetaCreateParams& params)
{
  ObjectWriteOperation op;

  int r = ClsFIFO::meta_create(&op, id, params);
  if (r < 0) {
    return r;
  }

  return ioctx.operate(oid, &op);
}

TEST(ClsFIFO, TestCreate) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";
  string oid = fifo_id;

  ASSERT_EQ(-EINVAL, fifo_create(ioctx, oid, string(),
                                  ClsFIFO::MetaCreateParams()));

  ASSERT_EQ(-EINVAL, fifo_create(ioctx, oid, fifo_id,
                     ClsFIFO::MetaCreateParams()
                     .max_part_size(0)));

  ASSERT_EQ(-EINVAL, fifo_create(ioctx, oid, fifo_id,
                     ClsFIFO::MetaCreateParams()
                     .max_entry_size(0)));
  
  /* first successful create */
  ASSERT_EQ(0, fifo_create(ioctx, oid, fifo_id,
               ClsFIFO::MetaCreateParams()));

  uint64_t size;
  struct timespec ts;
  ASSERT_EQ(0, ioctx.stat2(oid, &size, &ts));
  ASSERT_GT(size, 0);

  /* test idempotency */
  ASSERT_EQ(0, fifo_create(ioctx, oid, fifo_id,
               ClsFIFO::MetaCreateParams()));

  uint64_t size2;
  struct timespec ts2;
  ASSERT_EQ(0, ioctx.stat2(oid, &size2, &ts2));
  ASSERT_EQ(size2, size);

  ASSERT_EQ(-EEXIST, fifo_create(ioctx, oid, fifo_id,
               ClsFIFO::MetaCreateParams()
               .exclusive(true)));

  ASSERT_EQ(-EEXIST, fifo_create(ioctx, oid, fifo_id,
               ClsFIFO::MetaCreateParams()
               .oid_prefix("myprefix")
               .exclusive(false)));

  ASSERT_EQ(-EEXIST, fifo_create(ioctx, oid, "foo",
               ClsFIFO::MetaCreateParams()
               .exclusive(false)));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsFIFO, TestGetInfo) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";
  string oid = fifo_id;

  fifo_info_t info;

  /* first successful create */
  ASSERT_EQ(0, fifo_create(ioctx, oid, fifo_id,
               ClsFIFO::MetaCreateParams()));

  uint32_t part_header_size;
  uint32_t part_entry_overhead;

  ASSERT_EQ(0, ClsFIFO::meta_get(ioctx, oid,
               ClsFIFO::MetaGetParams(), &info,
               &part_header_size, &part_entry_overhead));

  ASSERT_GT(part_header_size, 0);
  ASSERT_GT(part_entry_overhead, 0);

  ASSERT_TRUE(!info.objv.instance.empty());

  ASSERT_EQ(0, ClsFIFO::meta_get(ioctx, oid,
               ClsFIFO::MetaGetParams()
               .objv(info.objv),
               &info,
               &part_header_size, &part_entry_overhead));

  fifo_objv_t objv;
  objv.instance="foo";
  objv.ver = 12;
  ASSERT_EQ(-ECANCELED, ClsFIFO::meta_get(ioctx, oid,
               ClsFIFO::MetaGetParams()
               .objv(objv),
               &info,
               &part_header_size, &part_entry_overhead));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(FIFO, TestOpenDefault) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo(cct(ioctx), fifo_id, &ioctx);

  /* pre-open ops that should fail */
  ASSERT_EQ(-EINVAL, fifo.read_meta());

  bufferlist bl;
  ASSERT_EQ(-EINVAL, fifo.push(bl));

  ASSERT_EQ(-EINVAL, fifo.list(100, nullopt, nullptr, nullptr));
  ASSERT_EQ(-EINVAL, fifo.trim(string()));

  ASSERT_EQ(-ENOENT, fifo.open(false));

  /* first successful create */
  ASSERT_EQ(0, fifo.open(true));

  ASSERT_EQ(0, fifo.read_meta()); /* force reading from backend */

  auto info = fifo.get_meta();

  ASSERT_EQ(info.id, fifo_id);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(FIFO, TestOpenParams) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo(cct(ioctx), fifo_id, &ioctx);

  uint64_t max_part_size = 10 * 1024;
  uint64_t max_entry_size = 128;
  string oid_prefix = "foo.123.";

  fifo_objv_t objv;
  objv.instance = "fooz";
  objv.ver = 10;


  /* first successful create */
  ASSERT_EQ(0, fifo.open(true,
                         ClsFIFO::MetaCreateParams()
                         .max_part_size(max_part_size)
                         .max_entry_size(max_entry_size)
                         .oid_prefix(oid_prefix)
                         .objv(objv)));

  ASSERT_EQ(0, fifo.read_meta()); /* force reading from backend */

  auto info = fifo.get_meta();

  ASSERT_EQ(info.id, fifo_id);
  ASSERT_EQ(info.data_params.max_part_size, max_part_size);
  ASSERT_EQ(info.data_params.max_entry_size, max_entry_size);
  ASSERT_EQ(info.objv, objv);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

template <class T>
static int decode_entry(fifo_entry& entry,
                        T *val,
                        string *marker)
{
  *marker = entry.marker;
  auto iter = entry.data.cbegin();

  try {
    decode(*val, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

TEST(FIFO, TestPushListTrim) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo(cct(ioctx), fifo_id, &ioctx);

  /* first successful create */
  ASSERT_EQ(0, fifo.open(true));

  uint32_t max_entries = 10;

  for (uint32_t i = 0; i < max_entries; ++i) {
    bufferlist bl;
    encode(i, bl);
    ASSERT_EQ(0, fifo.push(bl));
  }

  string marker;

  /* get entries one by one */

  for (uint32_t i = 0; i < max_entries; ++i) {
    vector<fifo_entry> result;
    bool more;
    ASSERT_EQ(0, fifo.list(1, marker, &result, &more));

    bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);
    ASSERT_EQ(1, result.size());

    uint32_t val;
    ASSERT_EQ(0, decode_entry(result.front(), &val, &marker));

    ASSERT_EQ(i, val);
  }

  /* get all entries at once */
  vector<fifo_entry> result;
  bool more;
  ASSERT_EQ(0, fifo.list(max_entries * 10, string(), &result, &more));

  ASSERT_FALSE(more);
  ASSERT_EQ(max_entries, result.size());

  string markers[max_entries];


  for (uint32_t i = 0; i < max_entries; ++i) {
    uint32_t val;

    ASSERT_EQ(0, decode_entry(result[i], &val, &markers[i]));
    ASSERT_EQ(i, val);
  }

  uint32_t min_entry = 0;

  /* trim one entry */
  fifo.trim(markers[min_entry]);
  ++min_entry;

  ASSERT_EQ(0, fifo.list(max_entries * 10, string(), &result, &more));

  ASSERT_FALSE(more);
  ASSERT_EQ(max_entries - min_entry, result.size());

  for (uint32_t i = min_entry; i < max_entries; ++i) {
    uint32_t val;

    ASSERT_EQ(0, decode_entry(result[i - min_entry], &val, &markers[i]));
    ASSERT_EQ(i, val);
  }


  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(FIFO, TestPushTooBig) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo(cct(ioctx), fifo_id, &ioctx);

  uint64_t max_part_size = 2048;
  uint64_t max_entry_size = 128;

  char buf[max_entry_size + 1];
  memset(buf, 0, sizeof(buf));

  /* first successful create */
  ASSERT_EQ(0, fifo.open(true,
                         ClsFIFO::MetaCreateParams()
                         .max_part_size(max_part_size)
                         .max_entry_size(max_entry_size)));

  bufferlist bl;
  bl.append(buf, sizeof(buf));

  ASSERT_EQ(-EINVAL, fifo.push(bl));


  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(FIFO, TestMultipleParts) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo(cct(ioctx), fifo_id, &ioctx);

  uint64_t max_part_size = 2048;
  uint64_t max_entry_size = 128;

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));

  /* create */
  ASSERT_EQ(0, fifo.open(true,
                         ClsFIFO::MetaCreateParams()
                         .max_part_size(max_part_size)
                         .max_entry_size(max_entry_size)));

  uint32_t part_header_size;
  uint32_t part_entry_overhead;

  fifo.get_part_layout_info(&part_header_size, &part_entry_overhead);

  int entries_per_part = (max_part_size - part_header_size) / (max_entry_size + part_entry_overhead);

  int max_entries = entries_per_part * 4 + 1;

  /* push enough entries */
  for (int i = 0; i < max_entries; ++i) {
    bufferlist bl;

    *(int *)buf = i;
    bl.append(buf, sizeof(buf));

    ASSERT_EQ(0, fifo.push(bl));
  }

  auto info = fifo.get_meta();

  ASSERT_EQ(info.id, fifo_id);
  ASSERT_GT(info.head_part_num, 0); /* head should have advanced */


  /* list all at once */
  vector<fifo_entry> result;
  bool more;
  ASSERT_EQ(0, fifo.list(max_entries, string(), &result, &more));
  ASSERT_EQ(false, more);

  ASSERT_EQ(max_entries, result.size());

  for (int i = 0; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }


  /* list one at a time */
  string marker;
  for (int i = 0; i < max_entries; ++i) {
    ASSERT_EQ(0, fifo.list(1, marker, &result, &more));

    ASSERT_EQ(result.size(), 1);
    bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    auto& entry = result[0];

    auto& bl = entry.data;
    marker = entry.marker;

    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  /* trim one at a time */
  marker.clear();
  for (int i = 0; i < max_entries; ++i) {
    /* read single entry */
    ASSERT_EQ(0, fifo.list(1, marker, &result, &more));

    ASSERT_EQ(result.size(), 1);
    bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);

    marker = result[0].marker;

    /* trim */
    ASSERT_EQ(0, fifo.trim(marker));

    /* check tail */
    info = fifo.get_meta();
    ASSERT_EQ(info.tail_part_num, i / entries_per_part);

    /* try to read all again, see how many entries left */
    ASSERT_EQ(0, fifo.list(max_entries, marker, &result, &more));
    ASSERT_EQ(max_entries - i - 1, result.size());
    ASSERT_EQ(false, more);
  }

  /* tail now should point at head */
  info = fifo.get_meta();
  ASSERT_EQ(info.head_part_num, info.tail_part_num);

  fifo_part_info part_info;

  /* check old tails are removed */
  for (int i = 0; i < info.tail_part_num; ++i) {
    ASSERT_EQ(-ENOENT, fifo.get_part_info(i, &part_info));
  }

  /* check curent tail exists */
  ASSERT_EQ(0, fifo.get_part_info(info.tail_part_num, &part_info));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(FIFO, TestTwoPushers) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo(cct(ioctx), fifo_id, &ioctx);

  uint64_t max_part_size = 2048;
  uint64_t max_entry_size = 128;

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));

  /* create */
  ASSERT_EQ(0, fifo.open(true,
                         ClsFIFO::MetaCreateParams()
                         .max_part_size(max_part_size)
                         .max_entry_size(max_entry_size)));

  uint32_t part_header_size;
  uint32_t part_entry_overhead;

  fifo.get_part_layout_info(&part_header_size, &part_entry_overhead);

  int entries_per_part = (max_part_size - part_header_size) / (max_entry_size + part_entry_overhead);

  int max_entries = entries_per_part * 4 + 1;

  FIFO fifo2(cct(ioctx), fifo_id, &ioctx);

  /* open second one */
  ASSERT_EQ(0, fifo2.open(true,
                         ClsFIFO::MetaCreateParams()));

  vector<FIFO *> fifos(2);
  fifos[0] = &fifo;
  fifos[1] = &fifo2;

  for (int i = 0; i < max_entries; ++i) {
    bufferlist bl;

    *(int *)buf = i;
    bl.append(buf, sizeof(buf));

    auto& f = fifos[i % fifos.size()];

    ASSERT_EQ(0, f->push(bl));
  }

  /* list all by both */
  vector<fifo_entry> result;
  bool more;
  ASSERT_EQ(0, fifo2.list(max_entries, string(), &result, &more));

  ASSERT_EQ(false, more);

  ASSERT_EQ(max_entries, result.size());

  ASSERT_EQ(0, fifo.list(max_entries, string(), &result, &more));
  ASSERT_EQ(false, more);

  ASSERT_EQ(max_entries, result.size());

  for (int i = 0; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(FIFO, TestTwoPushersTrim) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo1(cct(ioctx), fifo_id, &ioctx);

  uint64_t max_part_size = 2048;
  uint64_t max_entry_size = 128;

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));

  /* create */
  ASSERT_EQ(0, fifo1.open(true,
                          ClsFIFO::MetaCreateParams()
                          .max_part_size(max_part_size)
                          .max_entry_size(max_entry_size)));

  uint32_t part_header_size;
  uint32_t part_entry_overhead;

  fifo1.get_part_layout_info(&part_header_size, &part_entry_overhead);

  int entries_per_part = (max_part_size - part_header_size) / (max_entry_size + part_entry_overhead);

  int max_entries = entries_per_part * 4 + 1;

  FIFO fifo2(cct(ioctx), fifo_id, &ioctx);

  /* open second one */
  ASSERT_EQ(0, fifo2.open(true,
                         ClsFIFO::MetaCreateParams()));

  /* push one entry to fifo2 and the rest to fifo1 */

  for (int i = 0; i < max_entries; ++i) {
    bufferlist bl;

    *(int *)buf = i;
    bl.append(buf, sizeof(buf));

    FIFO *f = (i < 1 ? &fifo2 : &fifo1);

    ASSERT_EQ(0, f->push(bl));
  }

  /* trim half by fifo1 */
  int num = max_entries / 2;

  vector<fifo_entry> result;
  bool more;
  ASSERT_EQ(0, fifo1.list(num, string(), &result, &more));

  ASSERT_EQ(true, more);
  ASSERT_EQ(num, result.size());

  for (int i = 0; i < num; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  auto& entry = result[num - 1];
  auto& marker = entry.marker;

  ASSERT_EQ(0, fifo1.trim(marker));

  /* list what's left by fifo2 */

  int left = max_entries - num;

  ASSERT_EQ(0, fifo2.list(left, marker, &result, &more));
  ASSERT_EQ(left, result.size());
  ASSERT_EQ(false, more);

  for (int i = num; i < max_entries; ++i) {
    auto& bl = result[i - num].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(FIFO, TestPushBatch) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";

  FIFO fifo(cct(ioctx), fifo_id, &ioctx);

  uint64_t max_part_size = 2048;
  uint64_t max_entry_size = 128;

  char buf[max_entry_size];
  memset(buf, 0, sizeof(buf));

  /* create */
  ASSERT_EQ(0, fifo.open(true,
                          ClsFIFO::MetaCreateParams()
                          .max_part_size(max_part_size)
                          .max_entry_size(max_entry_size)));

  uint32_t part_header_size;
  uint32_t part_entry_overhead;

  fifo.get_part_layout_info(&part_header_size, &part_entry_overhead);

  int entries_per_part = (max_part_size - part_header_size) / (max_entry_size + part_entry_overhead);

  int max_entries = entries_per_part * 4 + 1; /* enough entries to span multiple parts */

  vector<bufferlist> bufs;

  for (int i = 0; i < max_entries; ++i) {
    bufferlist bl;

    *(int *)buf = i;
    bl.append(buf, sizeof(buf));

    bufs.push_back(bl);
  }

  ASSERT_EQ(0, fifo.push(bufs));

  /* list all */

  vector<fifo_entry> result;
  bool more;
  ASSERT_EQ(0, fifo.list(max_entries, string(), &result, &more));

  ASSERT_EQ(false, more);
  ASSERT_EQ(max_entries, result.size());

  for (int i = 0; i < max_entries; ++i) {
    auto& bl = result[i].data;
    ASSERT_EQ(i, *(int *)bl.c_str());
  }

  auto& info = fifo.get_meta();
  ASSERT_EQ(info.head_part_num, 4);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
