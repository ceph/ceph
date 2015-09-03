// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/int_types.h"
#include "include/rados/librados.h"
#include "include/rbd_types.h"
#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/config.h"

#include "gtest/gtest.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include <algorithm>
#include <sstream>

#include "test/librados/test.h"
#include "common/errno.h"
#include "include/interval_set.h"
#include "include/stringify.h"

#include <boost/scope_exit.hpp>

using namespace std;

static int get_features(bool *old_format, uint64_t *features)
{
  const char *c = getenv("RBD_FEATURES");
  if (c) {
    stringstream ss;
    ss << c;
    ss >> *features;
    if (ss.fail())
      return -EINVAL;
    *old_format = false;
    cout << "using new format!" << std::endl;
  } else {
    *old_format = true;
    cout << "using old format" << std::endl;
  }

  return 0;
}

static int create_image_full(rados_ioctx_t ioctx, const char *name,
			      uint64_t size, int *order, int old_format,
			      uint64_t features)
{
  if (old_format) {
    return rbd_create(ioctx, name, size, order);
  } else if ((features & RBD_FEATURE_STRIPINGV2) != 0) {
    return rbd_create3(ioctx, name, size, features, order, 65536, 16);
  } else {
    return rbd_create2(ioctx, name, size, features, order);
  }
}

static int create_image(rados_ioctx_t ioctx, const char *name,
			uint64_t size, int *order)
{
  bool old_format;
  uint64_t features;

  int r = get_features(&old_format, &features);
  if (r < 0)
    return r;
  return create_image_full(ioctx, name, size, order, old_format, features);
}

static int create_image_pp(librbd::RBD &rbd,
			   librados::IoCtx &ioctx,
			   const char *name,
			   uint64_t size, int *order) {
  bool old_format;
  uint64_t features;
  int r = get_features(&old_format, &features);
  if (r < 0)
    return r;
  if (old_format) {
    return rbd.create(ioctx, name, size, order);
  } else {
    return rbd.create2(ioctx, name, size, features, order);
  }
}

TEST(LibRBD, CreateAndStat)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));

  rbd_image_info_t info;
  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  printf("image has size %llu and order %d\n", (unsigned long long) info.size, info.order);
  ASSERT_EQ(info.size, size);
  ASSERT_EQ(info.order, order);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, CreateAndStatPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::image_info_t info;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size);
    ASSERT_EQ(info.order, order);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(LibRBD, ResizeAndStat)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_info_t info;
  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  ASSERT_EQ(0, rbd_resize(image, size * 4));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size * 4);

  ASSERT_EQ(0, rbd_resize(image, size / 2));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size / 2);
  
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, ResizeAndStatPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::image_info_t info;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));
    
    ASSERT_EQ(0, image.resize(size * 4));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size * 4);
    
    ASSERT_EQ(0, image.resize(size / 2));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size / 2);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

int test_ls(rados_ioctx_t io_ctx, size_t num_expected, ...)
{
  int num_images, i, j;
  char *names, *cur_name;
  va_list ap;
  size_t max_size = 1024;

  names = (char *) malloc(sizeof(char *) * 1024);
  int len = rbd_list(io_ctx, names, &max_size);

  for (i = 0, num_images = 0, cur_name = names; cur_name < names + len; i++) {
    printf("image: %s\n", cur_name);
    cur_name += strlen(cur_name) + 1;
    num_images++;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    printf("expected = %s\n", expected);
    int found = 0;
    for (j = 0, cur_name = names; j < num_images; j++) {
      if (cur_name[0] == '_') {
	cur_name += strlen(cur_name) + 1;
	continue;
      }
      if (strcmp(cur_name, expected) == 0) {
	printf("found %s\n", cur_name);
	cur_name[0] = '_';
	found = 1;
	break;
      }
    }
    assert(found);
  }
  va_end(ap);

  for (i = 0, cur_name = names; cur_name < names + len; i++) {
    assert(cur_name[0] == '_');
    cur_name += strlen(cur_name) + 1;
  }
  free(names);

  return num_images;
}

TEST(LibRBD, TestCreateLsDelete)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  int order = 0;
  const char *name = "testimg";
  const char *name2 = "testimg2";
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(1, test_ls(ioctx, 1, name));
  ASSERT_EQ(0, create_image(ioctx, name2, size, &order));
  ASSERT_EQ(2, test_ls(ioctx, 2, name, name2));
  ASSERT_EQ(0, rbd_remove(ioctx, name));
  ASSERT_EQ(1, test_ls(ioctx, 1, name2));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

int test_ls_pp(librbd::RBD& rbd, librados::IoCtx& io_ctx, size_t num_expected, ...)
{
  int r;
  size_t i;
  va_list ap;
  vector<string> names;
  r = rbd.list(io_ctx, names);
  if (r == -ENOENT)
    r = 0;
  assert(r >= 0);
  cout << "num images is: " << names.size() << endl
	    << "expected: " << num_expected << endl;
  int num = names.size();

  for (i = 0; i < names.size(); i++) {
    cout << "image: " << names[i] << endl;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    cout << "expected = " << expected << endl;
    vector<string>::iterator listed_name = find(names.begin(), names.end(), string(expected));
    assert(listed_name != names.end());
    names.erase(listed_name);
  }
  va_end(ap);

  assert(names.empty());

  return num;
}

TEST(LibRBD, TestCreateLsDeletePP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    const char *name2 = "testimg2";
    uint64_t size = 2 << 20;  
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name));
    ASSERT_EQ(0, rbd.create(ioctx, name2, size, &order));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name, name2));
    ASSERT_EQ(0, rbd.remove(ioctx, name));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name2));
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}


static int print_progress_percent(uint64_t offset, uint64_t src_size,
				     void *data)
{
  float percent = ((float)offset * 100) / src_size;
  printf("%3.2f%% done\n", percent);
  return 0; 
}

TEST(LibRBD, TestCopy)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  const char *name2 = "testimg2";
  const char *name3 = "testimg3";

  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));
  ASSERT_EQ(1, test_ls(ioctx, 1, name));
  ASSERT_EQ(0, rbd_copy(image, ioctx, name2));
  ASSERT_EQ(2, test_ls(ioctx, 2, name, name2));
  ASSERT_EQ(0, rbd_copy_with_progress(image, ioctx, name3, print_progress_percent, NULL));
  ASSERT_EQ(3, test_ls(ioctx, 3, name, name2, name3));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

class PrintProgress : public librbd::ProgressContext
{
public:
  int update_progress(uint64_t offset, uint64_t src_size)
  {
    float percent = ((float)offset * 100) / src_size;
    printf("%3.2f%% done\n", percent);
    return 0;
  }
};

TEST(LibRBD, TestCopyPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();
  
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    const char *name2 = "testimg2";
    const char *name3 = "testimg3";
    uint64_t size = 2 << 20;
    PrintProgress pp;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name));
    ASSERT_EQ(0, image.copy(ioctx, name2));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name, name2));
    ASSERT_EQ(0, image.copy_with_progress(ioctx, name3, pp));
    ASSERT_EQ(3, test_ls_pp(rbd, ioctx, 3, name, name2, name3));
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

int test_ls_snaps(rbd_image_t image, int num_expected, ...)
{
  int num_snaps, i, j, max_size = 10;
  va_list ap;
  rbd_snap_info_t snaps[max_size];
  num_snaps = rbd_snap_list(image, snaps, &max_size);
  printf("num snaps is: %d\nexpected: %d\n", num_snaps, num_expected);

  for (i = 0; i < num_snaps; i++) {
    printf("snap: %s\n", snaps[i].name);
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    uint64_t expected_size = va_arg(ap, uint64_t);
    int found = 0;
    for (j = 0; j < num_snaps; j++) {
      if (snaps[j].name == NULL)
	continue;
      if (strcmp(snaps[j].name, expected) == 0) {
	printf("found %s with size %llu\n", snaps[j].name, (unsigned long long) snaps[j].size);
	assert(snaps[j].size == expected_size);
	free((void *) snaps[j].name);
	snaps[j].name = NULL;
	found = 1;
	break;
      }
    }
    assert(found);
  }
  va_end(ap);

  for (i = 0; i < num_snaps; i++) {
    assert(snaps[i].name == NULL);
  }

  return num_snaps;
}

TEST(LibRBD, TestCreateLsDeleteSnap)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;
  uint64_t size2 = 4 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  ASSERT_EQ(0, rbd_snap_create(image, "snap1"));
  ASSERT_EQ(1, test_ls_snaps(image, 1, "snap1", size));
  ASSERT_EQ(0, rbd_resize(image, size2));
  ASSERT_EQ(0, rbd_snap_create(image, "snap2"));
  ASSERT_EQ(2, test_ls_snaps(image, 2, "snap1", size, "snap2", size2));
  ASSERT_EQ(0, rbd_snap_remove(image, "snap1"));
  ASSERT_EQ(1, test_ls_snaps(image, 1, "snap2", size2));
  ASSERT_EQ(0, rbd_snap_remove(image, "snap2"));
  ASSERT_EQ(0, test_ls_snaps(image, 0));
  
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

int test_ls_snaps(librbd::Image& image, size_t num_expected, ...)
{
  int r;
  size_t i, j;
  va_list ap;
  vector<librbd::snap_info_t> snaps;
  r = image.snap_list(snaps);
  assert(r >= 0);
  cout << "num snaps is: " << snaps.size() << endl
	    << "expected: " << num_expected << endl;

  for (i = 0; i < snaps.size(); i++) {
    cout << "snap: " << snaps[i].name << endl;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    uint64_t expected_size = va_arg(ap, uint64_t);
    int found = 0;
    for (j = 0; j < snaps.size(); j++) {
      if (snaps[j].name == "")
	continue;
      if (strcmp(snaps[j].name.c_str(), expected) == 0) {
	cout << "found " << snaps[j].name << " with size " << snaps[j].size << endl;
	assert(snaps[j].size == expected_size);
	snaps[j].name = "";
	found = 1;
	break;
      }
    }
    assert(found);
  }
  va_end(ap);

  for (i = 0; i < snaps.size(); i++) {
    assert(snaps[i].name == "");
  }

  return snaps.size();
}

TEST(LibRBD, TestCreateLsDeleteSnapPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    uint64_t size2 = 4 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));

    ASSERT_FALSE(image.snap_exists("snap1"));
    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_TRUE(image.snap_exists("snap1"));
    ASSERT_EQ(1, test_ls_snaps(image, 1, "snap1", size));
    ASSERT_EQ(0, image.resize(size2));
    ASSERT_FALSE(image.snap_exists("snap2"));
    ASSERT_EQ(0, image.snap_create("snap2"));
    ASSERT_TRUE(image.snap_exists("snap2"));
    ASSERT_EQ(2, test_ls_snaps(image, 2, "snap1", size, "snap2", size2));
    ASSERT_EQ(0, image.snap_remove("snap1"));
    ASSERT_FALSE(image.snap_exists("snap1"));
    ASSERT_EQ(1, test_ls_snaps(image, 1, "snap2", size2));
    ASSERT_EQ(0, image.snap_remove("snap2"));
    ASSERT_FALSE(image.snap_exists("snap2"));
    ASSERT_EQ(0, test_ls_snaps(image, 0));
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}



#define TEST_IO_SIZE 512
#define TEST_IO_TO_SNAP_SIZE 80

void simple_write_cb(rbd_completion_t cb, void *arg)
{
  printf("write completion cb called!\n");
}

void simple_read_cb(rbd_completion_t cb, void *arg)
{
  printf("read completion cb called!\n");
}

void aio_write_test_data(rbd_image_t image, const char *test_data, uint64_t off, size_t len)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  printf("created completion\n");
  rbd_aio_write(image, off, len, test_data, comp);
  printf("started write\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  assert(r == 0);
  printf("finished write\n");
  rbd_aio_release(comp);
}

void write_test_data(rbd_image_t image, const char *test_data, uint64_t off, size_t len)
{
  ssize_t written;
  written = rbd_write(image, off, len, test_data);
  printf("wrote: %d\n", (int) written);
  assert(written == (ssize_t)len);
}

void aio_discard_test_data(rbd_image_t image, uint64_t off, uint64_t len)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  rbd_aio_discard(image, off, len, comp);
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  assert(r == 0);
  printf("aio discard: %d~%d = %d\n", (int)off, (int)len, (int)r);
  rbd_aio_release(comp);
}

void discard_test_data(rbd_image_t image, uint64_t off, size_t len)
{
  ssize_t written;
  written = rbd_discard(image, off, len);
  printf("discard: %d~%d = %d\n", (int)off, (int)len, (int)written);
  assert(written == (ssize_t)len);
}

void aio_read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len)
{
  rbd_completion_t comp;
  char *result = (char *)malloc(len + 1);

  assert(result);
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  printf("created completion\n");
  rbd_aio_read(image, off, len, result, comp);
  printf("started read\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  assert(r == (ssize_t)len);
  rbd_aio_release(comp);
  if (memcmp(result, expected, len)) {
    printf("read: %s\nexpected: %s\n", result, expected);
    assert(memcmp(result, expected, len) == 0);
  }
  free(result);
}

void read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len)
{
  ssize_t read;
  char *result = (char *)malloc(len + 1);

  assert(result);
  read = rbd_read(image, off, len, result);
  printf("read: %d\n", (int) read);
  assert(read == (ssize_t)len);
  result[len] = '\0';
  if (memcmp(result, expected, len)) {
    printf("read: %s\nexpected: %s\n", result, expected);
    assert(memcmp(result, expected, len) == 0);
  }
  free(result);
}

TEST(LibRBD, TestIO)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  char test_data[TEST_IO_SIZE + 1];
  char zero_data[TEST_IO_SIZE + 1];
  int i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';
  memset(zero_data, 0, sizeof(zero_data));

  for (i = 0; i < 5; ++i)
    write_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_write_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 0; i < 5; ++i)
    read_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_read_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  // discard 2nd, 4th sections.
  discard_test_data(image, TEST_IO_SIZE, TEST_IO_SIZE);
  aio_discard_test_data(image, TEST_IO_SIZE*3, TEST_IO_SIZE);

  read_test_data(image, test_data,  0, TEST_IO_SIZE);
  read_test_data(image,  zero_data, TEST_IO_SIZE, TEST_IO_SIZE);
  read_test_data(image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE);
  read_test_data(image,  zero_data, TEST_IO_SIZE*3, TEST_IO_SIZE);
  read_test_data(image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE);
  
  rbd_image_info_t info;
  rbd_completion_t comp;
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  // can't read or write starting past end
  ASSERT_EQ(-EINVAL, rbd_write(image, info.size, 1, test_data));
  ASSERT_EQ(-EINVAL, rbd_read(image, info.size, 1, test_data));
  // reading through end returns amount up to end
  ASSERT_EQ(10, rbd_read(image, info.size - 10, 100, test_data));
  // writing through end returns amount up to end
  ASSERT_EQ(10, rbd_write(image, info.size - 10, 100, test_data));

  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  ASSERT_EQ(0, rbd_aio_write(image, info.size, 1, test_data, comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EINVAL, rbd_aio_get_return_value(comp));
  rbd_aio_release(comp);

  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  ASSERT_EQ(0, rbd_aio_read(image, info.size, 1, test_data, comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EINVAL, rbd_aio_get_return_value(comp));
  rbd_aio_release(comp);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, TestEmptyDiscard)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 20 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  aio_discard_test_data(image, 0, 1*1024*1024);
  aio_discard_test_data(image, 0, 4*1024*1024);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}


void simple_write_cb_pp(librbd::completion_t cb, void *arg)
{
  cout << "write completion cb called!" << endl;
}

void simple_read_cb_pp(librbd::completion_t cb, void *arg)
{
  cout << "read completion cb called!" << endl;
}

void aio_write_test_data(librbd::Image& image, const char *test_data, off_t off)
{
  ceph::bufferlist bl;
  bl.append(test_data, strlen(test_data));
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  printf("created completion\n");
  image.aio_write(off, strlen(test_data), bl, comp);
  printf("started write\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r >= 0);
  printf("finished write\n");
  comp->release();
}

void aio_discard_test_data(librbd::Image& image, off_t off, size_t len)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  image.aio_discard(off, len, comp);
  comp->wait_for_complete();
  int r = comp->get_return_value();
  assert(r >= 0);
  comp->release();
}

void write_test_data(librbd::Image& image, const char *test_data, off_t off)
{
  size_t written;
  size_t len = strlen(test_data);
  ceph::bufferlist bl;
  bl.append(test_data, len);
  written = image.write(off, len, bl);
  printf("wrote: %u\n", (unsigned int) written);
  assert(written == bl.length());
}

void discard_test_data(librbd::Image& image, off_t off, size_t len)
{
  size_t written;
  written = image.discard(off, len);
  printf("discard: %u~%u\n", (unsigned)off, (unsigned)len);
  assert(written == len);
}

void aio_read_test_data(librbd::Image& image, const char *expected, off_t off, size_t expected_len)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_read_cb_pp);
  ceph::bufferlist bl;
  printf("created completion\n");
  image.aio_read(off, expected_len, bl, comp);
  printf("started read\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r == TEST_IO_SIZE);
  assert(strncmp(expected, bl.c_str(), TEST_IO_SIZE) == 0);
  printf("finished read\n");
  comp->release();
}

void read_test_data(librbd::Image& image, const char *expected, off_t off, size_t expected_len)
{
  int read, total_read = 0;
  size_t len = expected_len;
  ceph::bufferlist bl;
  read = image.read(off + total_read, len, bl);
  assert(read >= 0);
  printf("read: %u\n", (unsigned int) read);
  printf("read: %s\nexpected: %s\n", bl.c_str(), expected);
  assert(strncmp(bl.c_str(), expected, expected_len) == 0);
}

TEST(LibRBD, TestIOPP) 
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));

    char test_data[TEST_IO_SIZE + 1];
    char zero_data[TEST_IO_SIZE + 1];
    int i;
    
    srand(time(0));
    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    test_data[TEST_IO_SIZE] = '\0';
    memset(zero_data, 0, sizeof(zero_data));

    for (i = 0; i < 5; ++i)
      write_test_data(image, test_data, strlen(test_data) * i);
    
    for (i = 5; i < 10; ++i)
      aio_write_test_data(image, test_data, strlen(test_data) * i);
    
    for (i = 0; i < 5; ++i)
      read_test_data(image, test_data, strlen(test_data) * i, TEST_IO_SIZE);
    
    for (i = 5; i < 10; ++i)
      aio_read_test_data(image, test_data, strlen(test_data) * i, TEST_IO_SIZE);

    // discard 2nd, 4th sections.
    discard_test_data(image, TEST_IO_SIZE, TEST_IO_SIZE);
    aio_discard_test_data(image, TEST_IO_SIZE*3, TEST_IO_SIZE);
    
    read_test_data(image, test_data,  0, TEST_IO_SIZE);
    read_test_data(image,  zero_data, TEST_IO_SIZE, TEST_IO_SIZE);
    read_test_data(image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE);
    read_test_data(image,  zero_data, TEST_IO_SIZE*3, TEST_IO_SIZE);
    read_test_data(image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}


TEST(LibRBD, TestIOToSnapshot)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t isize = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name, isize, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  int i, r;
  rbd_image_t image_at_snap;
  char orig_data[TEST_IO_TO_SNAP_SIZE + 1];
  char test_data[TEST_IO_TO_SNAP_SIZE + 1];

  for (i = 0; i < TEST_IO_TO_SNAP_SIZE; ++i)
    test_data[i] = (char) (i + 48);
  test_data[TEST_IO_TO_SNAP_SIZE] = '\0';
  orig_data[TEST_IO_TO_SNAP_SIZE] = '\0';

  r = rbd_read(image, 0, TEST_IO_TO_SNAP_SIZE, orig_data);
  ASSERT_EQ(r, TEST_IO_TO_SNAP_SIZE);

  ASSERT_EQ(0, test_ls_snaps(image, 0));
  ASSERT_EQ(0, rbd_snap_create(image, "orig"));
  ASSERT_EQ(1, test_ls_snaps(image, 1, "orig", isize));
  read_test_data(image, orig_data, 0, TEST_IO_TO_SNAP_SIZE);

  printf("write test data!\n");
  write_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);
  ASSERT_EQ(0, rbd_snap_create(image, "written"));
  ASSERT_EQ(2, test_ls_snaps(image, 2, "orig", isize, "written", isize));

  read_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  rbd_snap_set(image, "orig");
  read_test_data(image, orig_data, 0, TEST_IO_TO_SNAP_SIZE);

  rbd_snap_set(image, "written");
  read_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  rbd_snap_set(image, "orig");

  r = rbd_write(image, 0, TEST_IO_TO_SNAP_SIZE, test_data);
  printf("write to snapshot returned %d\n", r);
  ASSERT_LT(r, 0);
  cout << cpp_strerror(-r) << std::endl;

  read_test_data(image, orig_data, 0, TEST_IO_TO_SNAP_SIZE);
  rbd_snap_set(image, "written");
  read_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  r = rbd_snap_rollback(image, "orig");
  ASSERT_EQ(r, -EROFS);

  r = rbd_snap_set(image, NULL);
  ASSERT_EQ(r, 0);
  r = rbd_snap_rollback(image, "orig");
  ASSERT_EQ(r, 0);

  write_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  rbd_flush(image);

  printf("opening testimg@orig\n");
  ASSERT_EQ(0, rbd_open(ioctx, name, &image_at_snap, "orig"));
  read_test_data(image_at_snap, orig_data, 0, TEST_IO_TO_SNAP_SIZE);
  r = rbd_write(image_at_snap, 0, TEST_IO_TO_SNAP_SIZE, test_data);
  printf("write to snapshot returned %d\n", r);
  ASSERT_LT(r, 0);
  cout << cpp_strerror(-r) << std::endl;
  ASSERT_EQ(0, rbd_close(image_at_snap));

  ASSERT_EQ(2, test_ls_snaps(image, 2, "orig", isize, "written", isize));
  ASSERT_EQ(0, rbd_snap_remove(image, "written"));
  ASSERT_EQ(1, test_ls_snaps(image, 1, "orig", isize));
  ASSERT_EQ(0, rbd_snap_remove(image, "orig"));
  ASSERT_EQ(0, test_ls_snaps(image, 0));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, TestClone)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  rbd_image_info_t pinfo, cinfo;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent, child;
  int order = 0;

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx, "parent", 4<<20, &order, false, features));
  ASSERT_EQ(0, rbd_open(ioctx, "parent", &parent, NULL));
  printf("made parent image \"parent\"\n");

  char *data = (char *)"testdata";
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 0, strlen(data), data));

  // can't clone a non-snapshot, expect failure
  EXPECT_NE(0, rbd_clone(ioctx, "parent", NULL, ioctx, "child", features, &order));

  // verify that there is no parent info on "parent"
  char ppool[1], pname[1], psnapname[1];
  ASSERT_EQ(-ENOENT, rbd_get_parent_info(parent, ppool, sizeof(ppool),
	    pname, sizeof(pname), psnapname, sizeof(psnapname)));
  printf("parent has no parent info\n");

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  printf("made snapshot \"parent@parent_snap\"\n");
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, "parent", &parent, "parent_snap"));

  ASSERT_EQ(-EINVAL, rbd_clone(ioctx, "parent", "parent_snap", ioctx, "child",
	    features, &order));

  // unprotected image should fail unprotect
  ASSERT_EQ(-EINVAL, rbd_snap_unprotect(parent, "parent_snap"));
  printf("can't unprotect an unprotected snap\n");

  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));
  // protecting again should fail
  ASSERT_EQ(-EBUSY, rbd_snap_protect(parent, "parent_snap"));
  printf("can't protect a protected snap\n");

  // This clone and open should work
  ASSERT_EQ(0, rbd_clone(ioctx, "parent", "parent_snap", ioctx, "child",
	    features, &order));
  ASSERT_EQ(0, rbd_open(ioctx, "child", &child, NULL));
  printf("made and opened clone \"child\"\n");

  // check read
  read_test_data(child, data, 0, strlen(data));

  // check write
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(child, 20, strlen(data), data));
  read_test_data(child, data, 20, strlen(data));
  read_test_data(child, data, 0, strlen(data));

  // check attributes
  ASSERT_EQ(0, rbd_stat(parent, &pinfo, sizeof(pinfo)));
  ASSERT_EQ(0, rbd_stat(child, &cinfo, sizeof(cinfo)));
  EXPECT_EQ(cinfo.size, pinfo.size);
  uint64_t overlap;
  rbd_get_overlap(child, &overlap);
  EXPECT_EQ(overlap, pinfo.size);
  EXPECT_EQ(cinfo.obj_size, pinfo.obj_size);
  EXPECT_EQ(cinfo.order, pinfo.order);
  printf("sizes and overlaps are good between parent and child\n");

  // sizing down child results in changing overlap and size, not parent size
  ASSERT_EQ(0, rbd_resize(child, 2UL<<20));
  ASSERT_EQ(0, rbd_stat(child, &cinfo, sizeof(cinfo)));
  rbd_get_overlap(child, &overlap);
  ASSERT_EQ(overlap, 2UL<<20);
  ASSERT_EQ(cinfo.size, 2UL<<20);
  ASSERT_EQ(0, rbd_resize(child, 4UL<<20));
  ASSERT_EQ(0, rbd_stat(child, &cinfo, sizeof(cinfo)));
  rbd_get_overlap(child, &overlap);
  ASSERT_EQ(overlap, 2UL<<20);
  ASSERT_EQ(cinfo.size, 4UL<<20);
  printf("sized down clone, changed overlap\n");

  // sizing back up doesn't change that
  ASSERT_EQ(0, rbd_resize(child, 5UL<<20));
  ASSERT_EQ(0, rbd_stat(child, &cinfo, sizeof(cinfo)));
  rbd_get_overlap(child, &overlap);
  ASSERT_EQ(overlap, 2UL<<20);
  ASSERT_EQ(cinfo.size, 5UL<<20);
  ASSERT_EQ(0, rbd_stat(parent, &pinfo, sizeof(pinfo)));
  printf("parent info: size %lld obj_size %lld parent_pool %lld\n",
	 (unsigned long long)pinfo.size, (unsigned long long)pinfo.obj_size,
	 (unsigned long long)pinfo.parent_pool);
  ASSERT_EQ(pinfo.size, 4UL<<20);
  printf("sized up clone, changed size but not overlap or parent's size\n");
  
  ASSERT_EQ(0, rbd_close(child));

  ASSERT_EQ(-EBUSY, rbd_snap_remove(parent, "parent_snap"));
  printf("can't remove parent while child still exists\n");
  ASSERT_EQ(0, rbd_remove(ioctx, "child"));
  ASSERT_EQ(-EBUSY, rbd_snap_remove(parent, "parent_snap"));
  printf("can't remove parent while still protected\n");
  ASSERT_EQ(0, rbd_snap_unprotect(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_remove(parent, "parent_snap"));
  printf("removed parent snap after unprotecting\n");

  ASSERT_EQ(0, rbd_close(parent));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, TestClone2)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent, child;
  int order = 0;

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx, "parent", 4<<20, &order, false, features));
  ASSERT_EQ(0, rbd_open(ioctx, "parent", &parent, NULL));
  printf("made parent image \"parent\"\n");

  char *data = (char *)"testdata";
  char *childata = (char *)"childata";
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 0, strlen(data), data));
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 12, strlen(data), data));

  // can't clone a non-snapshot, expect failure
  EXPECT_NE(0, rbd_clone(ioctx, "parent", NULL, ioctx, "child", features, &order));

  // verify that there is no parent info on "parent"
  char ppool[1], pname[1], psnapname[1];
  ASSERT_EQ(-ENOENT, rbd_get_parent_info(parent, ppool, sizeof(ppool),
	    pname, sizeof(pname), psnapname, sizeof(psnapname)));
  printf("parent has no parent info\n");

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  printf("made snapshot \"parent@parent_snap\"\n");
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, "parent", &parent, "parent_snap"));

  ASSERT_EQ(-EINVAL, rbd_clone(ioctx, "parent", "parent_snap", ioctx, "child",
	    features, &order));

  // unprotected image should fail unprotect
  ASSERT_EQ(-EINVAL, rbd_snap_unprotect(parent, "parent_snap"));
  printf("can't unprotect an unprotected snap\n");

  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));
  // protecting again should fail
  ASSERT_EQ(-EBUSY, rbd_snap_protect(parent, "parent_snap"));
  printf("can't protect a protected snap\n");

  // This clone and open should work
  ASSERT_EQ(0, rbd_clone(ioctx, "parent", "parent_snap", ioctx, "child",
	    features, &order));
  ASSERT_EQ(0, rbd_open(ioctx, "child", &child, NULL));
  printf("made and opened clone \"child\"\n");

  // write something in
  ASSERT_EQ((ssize_t)strlen(childata), rbd_write(child, 20, strlen(childata), childata));

  char test[strlen(data) * 2];
  ASSERT_EQ((ssize_t)strlen(data), rbd_read(child, 20, strlen(data), test));
  ASSERT_EQ(0, memcmp(test, childata, strlen(childata)));

  // overlap
  ASSERT_EQ((ssize_t)sizeof(test), rbd_read(child, 20 - strlen(data), sizeof(test), test));
  ASSERT_EQ(0, memcmp(test, data, strlen(data)));
  ASSERT_EQ(0, memcmp(test + strlen(data), childata, strlen(childata)));

  // all parent
  ASSERT_EQ((ssize_t)sizeof(test), rbd_read(child, 0, sizeof(test), test));
  ASSERT_EQ(0, memcmp(test, data, strlen(data)));

  ASSERT_EQ(0, rbd_close(child));
  ASSERT_EQ(0, rbd_close(parent));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

static void test_list_children(rbd_image_t image, ssize_t num_expected, ...)
{
  va_list ap;
  va_start(ap, num_expected);
  size_t pools_len = 100;
  size_t children_len = 100;
  char *pools = NULL;
  char *children = NULL;
  ssize_t num_children;

  do {
    free(pools);
    free(children);
    pools = (char *) malloc(pools_len);
    children = (char *) malloc(children_len);
    num_children = rbd_list_children(image, pools, &pools_len,
				     children, &children_len);
  } while (num_children == -ERANGE);

  ASSERT_EQ(num_expected, num_children);
  for (ssize_t i = num_expected; i > 0; --i) {
    char *expected_pool = va_arg(ap, char *);
    char *expected_image = va_arg(ap, char *);
    char *pool = pools;
    char *image = children;
    bool found = 0;
    printf("\ntrying to find %s/%s\n", expected_pool, expected_image);
    for (ssize_t j = 0; j < num_children; ++j) {
      printf("checking %s/%s\n", pool, image);
      if (strcmp(expected_pool, pool) == 0 &&
	  strcmp(expected_image, image) == 0) {
	printf("found child %s/%s\n\n", pool, image);
	found = 1;
	break;
      }
      pool += strlen(pool) + 1;
      image += strlen(image) + 1;
      if (j == num_children - 1) {
	ASSERT_EQ(pool - pools - 1, (ssize_t) pools_len);
	ASSERT_EQ(image - children - 1, (ssize_t) children_len);
      }
    }
    ASSERT_TRUE(found);
  }
  va_end(ap);

  if (pools)
    free(pools);
  if (children)
    free(children);
}

TEST(LibRBD, ListChildren)
{
  rados_t cluster;
  rados_ioctx_t ioctx1, ioctx2;
  string pool_name1 = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name1, &cluster));
  string pool_name2 = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name2, &cluster));
  rados_ioctx_create(cluster, pool_name1.c_str(), &ioctx1);
  rados_ioctx_create(cluster, pool_name2.c_str(), &ioctx2);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent;
  int order = 0;

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx1, "parent", 4<<20, &order,
				 false, features));
  ASSERT_EQ(0, rbd_open(ioctx1, "parent", &parent, NULL));
  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_set(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));

  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx1, "parent", &parent, "parent_snap"));

  ASSERT_EQ(0, rbd_clone(ioctx1, "parent", "parent_snap", ioctx2, "child1",
	    features, &order));
  test_list_children(parent, 1, pool_name2.c_str(), "child1");

  ASSERT_EQ(0, rbd_clone(ioctx1, "parent", "parent_snap", ioctx1, "child2",
	    features, &order));
  test_list_children(parent, 2, pool_name2.c_str(), "child1",
		     pool_name1.c_str(), "child2");

  ASSERT_EQ(0, rbd_clone(ioctx1, "parent", "parent_snap", ioctx2, "child3",
	    features, &order));
  test_list_children(parent, 3, pool_name2.c_str(), "child1",
		     pool_name1.c_str(), "child2",
		     pool_name2.c_str(), "child3");

  ASSERT_EQ(0, rbd_clone(ioctx1, "parent", "parent_snap", ioctx2, "child4",
	    features, &order));
  test_list_children(parent, 4, pool_name2.c_str(), "child1",
		     pool_name1.c_str(), "child2",
		     pool_name2.c_str(), "child3",
		     pool_name2.c_str(), "child4");

  ASSERT_EQ(0, rbd_remove(ioctx2, "child1"));
  test_list_children(parent, 3,
		     pool_name1.c_str(), "child2",
		     pool_name2.c_str(), "child3",
		     pool_name2.c_str(), "child4");

  ASSERT_EQ(0, rbd_remove(ioctx2, "child3"));
  test_list_children(parent, 2,
		     pool_name1.c_str(), "child2",
		     pool_name2.c_str(), "child4");

  ASSERT_EQ(0, rbd_remove(ioctx2, "child4"));
  test_list_children(parent, 1,
		     pool_name1.c_str(), "child2");

  ASSERT_EQ(0, rbd_remove(ioctx1, "child2"));
  test_list_children(parent, 0);

  ASSERT_EQ(0, rbd_snap_unprotect(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_remove(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_remove(ioctx1, "parent"));
  rados_ioctx_destroy(ioctx1);
  rados_ioctx_destroy(ioctx2);
  // destroy_one_pool also closes the cluster; do this one step at a time
  ASSERT_EQ(0, rados_pool_delete(cluster, pool_name1.c_str()));
  ASSERT_EQ(0, destroy_one_pool(pool_name2, &cluster));
}

TEST(LibRBD, LockingPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    std::string cookie1 = "foo";
    std::string cookie2 = "bar";

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));

    // no lockers initially
    std::list<librbd::locker_t> lockers;
    std::string tag;
    bool exclusive;
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_EQ(0u, lockers.size());
    ASSERT_EQ("", tag);

    // exclusive lock is exclusive
    ASSERT_EQ(0, image.lock_exclusive(cookie1));
    ASSERT_EQ(-EEXIST, image.lock_exclusive(cookie1));
    ASSERT_EQ(-EBUSY, image.lock_exclusive(""));
    ASSERT_EQ(-EEXIST, image.lock_shared(cookie1, ""));
    ASSERT_EQ(-EBUSY, image.lock_shared(cookie1, "test"));
    ASSERT_EQ(-EBUSY, image.lock_shared("", "test"));
    ASSERT_EQ(-EBUSY, image.lock_shared("", ""));

    // list exclusive
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_TRUE(exclusive);
    ASSERT_EQ("", tag);
    ASSERT_EQ(1u, lockers.size());
    ASSERT_EQ(cookie1, lockers.front().cookie);

    // unlock
    ASSERT_EQ(-ENOENT, image.unlock(""));
    ASSERT_EQ(-ENOENT, image.unlock(cookie2));
    ASSERT_EQ(0, image.unlock(cookie1));
    ASSERT_EQ(-ENOENT, image.unlock(cookie1));
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_EQ(0u, lockers.size());

    ASSERT_EQ(0, image.lock_shared(cookie1, ""));
    ASSERT_EQ(-EEXIST, image.lock_shared(cookie1, ""));
    ASSERT_EQ(0, image.lock_shared(cookie2, ""));
    ASSERT_EQ(-EEXIST, image.lock_shared(cookie2, ""));
    ASSERT_EQ(-EEXIST, image.lock_exclusive(cookie1));
    ASSERT_EQ(-EEXIST, image.lock_exclusive(cookie2));
    ASSERT_EQ(-EBUSY, image.lock_exclusive(""));
    ASSERT_EQ(-EBUSY, image.lock_exclusive("test"));

    // list shared
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_EQ(2u, lockers.size());
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(LibRBD, FlushAio)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;
  size_t num_aios = 256;

  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  char test_data[TEST_IO_SIZE + 1];
  size_t i;
  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }

  rbd_completion_t write_comps[num_aios];
  for (i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &write_comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
			       write_comps[i]));
  }

  rbd_completion_t flush_comp;
  ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &flush_comp));
  ASSERT_EQ(0, rbd_aio_flush(image, flush_comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(flush_comp));
  ASSERT_EQ(1, rbd_aio_is_complete(flush_comp));
  rbd_aio_release(flush_comp);

  for (i = 0; i < num_aios; ++i) {
    ASSERT_EQ(1, rbd_aio_is_complete(write_comps[i]));
    rbd_aio_release(write_comps[i]);
  }

  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_remove(ioctx, name));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, FlushAioPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    size_t num_aios = 256;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));

    char test_data[TEST_IO_SIZE + 1];
    size_t i;
    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }

    librbd::RBD::AioCompletion *write_comps[num_aios];
    for (i = 0; i < num_aios; ++i) {
      ceph::bufferlist bl;
      bl.append(test_data, strlen(test_data));
      write_comps[i] = new librbd::RBD::AioCompletion(NULL, NULL);
      uint64_t offset = rand() % (size - TEST_IO_SIZE);
      ASSERT_EQ(0, image.aio_write(offset, TEST_IO_SIZE, bl,
				   write_comps[i]));
    }

    librbd::RBD::AioCompletion *flush_comp =
      new librbd::RBD::AioCompletion(NULL, NULL);
    ASSERT_EQ(0, image.aio_flush(flush_comp));
    ASSERT_EQ(0, flush_comp->wait_for_complete());
    ASSERT_EQ(1, flush_comp->is_complete());
    delete flush_comp;

    for (i = 0; i < num_aios; ++i) {
      librbd::RBD::AioCompletion *comp = write_comps[i];
      ASSERT_EQ(1, comp->is_complete());
      delete comp;
    }
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}


int iterate_cb(uint64_t off, size_t len, int exists, void *arg)
{
  //cout << "iterate_cb " << off << "~" << len << std::endl;
  interval_set<uint64_t> *diff = static_cast<interval_set<uint64_t> *>(arg);
  diff->insert(off, len);
  return 0;
}

void scribble(librbd::Image& image, int n, int max, interval_set<uint64_t> *exists, interval_set<uint64_t> *what)
{
  uint64_t size;
  image.size(&size);
  interval_set<uint64_t> exists_at_start = *exists;
  for (int i=0; i<n; i++) {
    uint64_t off = rand() % (size - max + 1);
    uint64_t len = 1 + rand() % max;
    if (rand() % 4 == 0) {
      ASSERT_EQ((int)len, image.discard(off, len));
      interval_set<uint64_t> w;      
      w.insert(off, len);

      // the zeroed bit no longer exists...
      w.intersection_of(*exists); 
      exists->subtract(w);

      // the bits we discarded are no long written...
      interval_set<uint64_t> w2 = w;
      w2.intersection_of(*what);
      what->subtract(w2);

      // except for the extents that existed at the start that we overwrote.
      interval_set<uint64_t> w3;
      w3.insert(off, len);
      w3.intersection_of(exists_at_start);
      what->union_of(w3);

    } else {
      bufferlist bl;
      bl.append(buffer::create(len));
      bl.zero();
      ASSERT_EQ((int)len, image.write(off, len, bl));
      interval_set<uint64_t> w;
      w.insert(off, len);
      what->union_of(w);
      exists->union_of(w);
    }
  }
}

TEST(LibRBD, DiffIterate)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  int seed = getpid();
  cout << "seed " << seed << std::endl;
  srand(seed);

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 20 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));

    interval_set<uint64_t> exists;
    interval_set<uint64_t> one, two;
    scribble(image, 10, 102400, &exists, &one);
    cout << " wrote " << one << std::endl;
    ASSERT_EQ(0, image.snap_create("one"));
    scribble(image, 10, 102400, &exists, &two);
    cout << " wrote " << two << std::endl;

    interval_set<uint64_t> diff;
    ASSERT_EQ(0, image.diff_iterate("one", 0, size, iterate_cb, (void *)&diff));
    cout << " diff was " << diff << std::endl;
    if (!two.subset_of(diff)) {
      interval_set<uint64_t> i;
      i.intersection_of(two, diff);
      interval_set<uint64_t> l = two;
      l.subtract(i);
      cout << " ... two - (two*diff) = " << l << std::endl;     
    }
    ASSERT_TRUE(two.subset_of(diff));
  }
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

struct diff_extent {
  diff_extent(uint64_t offset, uint64_t length, bool exists) :
    offset(offset), length(length), exists(exists) {}
  uint64_t offset;
  uint64_t length;
  bool exists;
  bool operator==(const diff_extent& o) const {
    return offset == o.offset && length == o.length && exists == o.exists;
  }
};

ostream& operator<<(ostream & o, const diff_extent& e) {
  return o << '(' << e.offset << '~' << e.length << ' ' << (e.exists ? "true" : "false") << ')';
}

int vector_iterate_cb(uint64_t off, size_t len, int exists, void *arg)
{
  cout << "iterate_cb " << off << "~" << len << std::endl;
  vector<diff_extent> *diff = static_cast<vector<diff_extent> *>(arg);
  diff->push_back(diff_extent(off, len, exists));
  return 0;
}

TEST(LibRBD, DiffIterateDiscard)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  int seed = getpid();
  cout << "seed " << seed << std::endl;
  srand(seed);

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 20 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));

    vector<diff_extent> extents;
    ceph::bufferlist bl;

    ASSERT_EQ(0, image.diff_iterate(NULL, 0, size,
				    vector_iterate_cb, (void *) &extents));
    ASSERT_EQ(0u, extents.size());

    char data[256];
    memset(data, 1, sizeof(data));
    bl.append(data, 256);
    ASSERT_EQ(256, image.write(0, 256, bl));
    ASSERT_EQ(0, image.diff_iterate(NULL, 0, size,
				    vector_iterate_cb, (void *) &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 256, true), extents[0]);

    int obj_ofs = 256;
    ASSERT_EQ(obj_ofs, image.discard(0, obj_ofs));

    extents.clear();
    ASSERT_EQ(0, image.diff_iterate(NULL, 0, size,
				    vector_iterate_cb, (void *) &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_EQ(256, image.write(0, 256, bl));
    ASSERT_EQ(0, image.diff_iterate(NULL, 0, size,
				    vector_iterate_cb, (void *) &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 256, true), extents[0]);
    ASSERT_EQ(0, image.snap_create("snap2"));

    ASSERT_EQ(obj_ofs, image.discard(0, obj_ofs));

    extents.clear();
    ASSERT_EQ(0, image.snap_set("snap2"));
    ASSERT_EQ(0, image.diff_iterate("snap1", 0, size,
				    vector_iterate_cb, (void *) &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 256, true), extents[0]);

    ASSERT_EQ(0, image.snap_set(NULL));
    ASSERT_EQ(1 << order, image.discard(0, 1 << order));
    ASSERT_EQ(0, image.snap_create("snap3"));
    ASSERT_EQ(0, image.snap_set("snap3"));

    extents.clear();
    ASSERT_EQ(0, image.diff_iterate("snap1", 0, size,
				    vector_iterate_cb, (void *) &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 256, false), extents[0]);
  }
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(LibRBD, DiffIterateStress)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  int seed = getpid();
  cout << "seed " << seed << std::endl;
  srand(seed);

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    const char *name = "testimg";
    uint64_t size = 400 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name, NULL));

    interval_set<uint64_t> curexists;
    vector<interval_set<uint64_t> > wrote;
    vector<interval_set<uint64_t> > exists;
    vector<string> snap;
    int n = 20;
    for (int i=0; i<n; i++) {
      interval_set<uint64_t> w;
      scribble(image, 10, 8192000, &curexists, &w);
      cout << " i=" << i << " exists " << curexists << " wrote " << w << std::endl;
      string s = "snap" + stringify(i);
      ASSERT_EQ(0, image.snap_create(s.c_str()));
      wrote.push_back(w);
      exists.push_back(curexists);
      snap.push_back(s);
    }

    for (int i=0; i<n-1; i++) {
      for (int j=i+1; j<n; j++) {
	interval_set<uint64_t> diff, actual, uex;
	for (int k=i+1; k<=j; k++)
	  diff.union_of(wrote[k]);
	cout << "from " << i << " to " << j << " diff " << diff << std::endl;

	// limit to extents that exists both at the beginning and at the end
	uex.union_of(exists[i], exists[j]);
	diff.intersection_of(uex);
	cout << "  limited diff " << diff << std::endl;

	image.snap_set(snap[j].c_str());
	ASSERT_EQ(0, image.diff_iterate(snap[i].c_str(), 0, size, iterate_cb, (void *)&actual));
	cout << " actual was " << actual << std::endl;
	if (!diff.subset_of(actual)) {
	  interval_set<uint64_t> i;
	  i.intersection_of(diff, actual);
	  interval_set<uint64_t> l = diff;
	  l.subtract(i);
	  cout << " ... diff - (actual*diff) = " << l << std::endl;     
	}
	ASSERT_TRUE(diff.subset_of(actual));
      }
    }

  }
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(LibRBD, ZeroLengthWrite)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  char read_data[1];
  ASSERT_EQ(0, rbd_write(image, 0, 0, NULL));
  ASSERT_EQ(1, rbd_read(image, 0, 1, read_data));
  ASSERT_EQ('\0', read_data[0]);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}


TEST(LibRBD, ZeroLengthDiscard)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  const char *data = "blah";
  char read_data[strlen(data)];
  ASSERT_EQ((int)strlen(data), rbd_write(image, 0, strlen(data), data));
  ASSERT_EQ(0, rbd_discard(image, 0, 0));
  ASSERT_EQ((int)strlen(data), rbd_read(image, 0, strlen(data), read_data));
  ASSERT_EQ(0, memcmp(data, read_data, strlen(data)));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, ZeroLengthRead)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  char read_data[1];
  ASSERT_EQ(0, rbd_read(image, 0, 0, read_data));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, LargeCacheRead)
{
  if (!g_conf->rbd_cache) {
    std::cout << "SKIPPING due to disabled cache" << std::endl;
    return;
  }

  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  uint64_t orig_cache_size = g_conf->rbd_cache_size;
  g_conf->set_val("rbd_cache_size", "16777216");
  BOOST_SCOPE_EXIT( (orig_cache_size) ) {
    g_conf->set_val("rbd_cache_size", stringify(orig_cache_size).c_str());
  } BOOST_SCOPE_EXIT_END;
  ASSERT_EQ(16777216, g_conf->rbd_cache_size);

  rbd_image_t image;
  int order = 0;
  const char *name = "testimg";
  uint64_t size = g_conf->rbd_cache_size + 1;

  ASSERT_EQ(0, create_image(ioctx, name, size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image, NULL));

  std::string buffer(1 << order, '1');
  for (size_t offs = 0; offs < size; offs += buffer.size()) {
    size_t len = std::min<uint64_t>(buffer.size(), size - offs);
    ASSERT_EQ(static_cast<ssize_t>(len),
	      rbd_write(image, offs, len, buffer.c_str()));
  }

  ASSERT_EQ(0, rbd_invalidate_cache(image));

  buffer.resize(size);
  ASSERT_EQ(static_cast<ssize_t>(size-1024), rbd_read(image, 1024, size, &buffer[0]));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, TestPendingAio)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t image;
  int order = 0;

  std::string name = "testimg";

  uint64_t size = 4 << 20;
  ASSERT_EQ(0, create_image_full(ioctx, name.c_str(), size, &order,
				 false, features));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  char test_data[TEST_IO_SIZE];
  for (size_t i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }

  size_t num_aios = 256;
  rbd_completion_t comps[num_aios];
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
                               comps[i]));
  }
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }
  ASSERT_EQ(0, rbd_invalidate_cache(image));

  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_LE(0, rbd_aio_read(image, offset, TEST_IO_SIZE, test_data,
                              comps[i]));
  }

  ASSERT_EQ(0, rbd_close(image));
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(1, rbd_aio_is_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRBD, BlockingAIO)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = "testimg";
  uint64_t size = 1 << 20;
  int order = 18;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  CephContext *cct = reinterpret_cast<CephContext*>(ioctx.cct());
  cct->_conf->set_val_or_die("rbd_non_blocking_aio", "0");

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  bufferlist bl;
  bl.append(std::string(256, '1'));

  librbd::RBD::AioCompletion *write_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, image.aio_write(0, bl.length(), bl, write_comp));

  librbd::RBD::AioCompletion *flush_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, image.aio_flush(flush_comp));
  ASSERT_EQ(0, flush_comp->wait_for_complete());
  ASSERT_EQ(0, flush_comp->get_return_value());
  flush_comp->release();

  ASSERT_EQ(1, write_comp->is_complete());
  ASSERT_EQ(0, write_comp->get_return_value());
  write_comp->release();

  librbd::RBD::AioCompletion *discard_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, image.aio_discard(128, 128, discard_comp));
  ASSERT_EQ(0, discard_comp->wait_for_complete());
  discard_comp->release();

  librbd::RBD::AioCompletion *read_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  bufferlist read_bl;
  image.aio_read(0, bl.length(), read_bl, read_comp);
  ASSERT_EQ(0, read_comp->wait_for_complete());
  ASSERT_EQ(bl.length(), read_comp->get_return_value());
  read_comp->release();

  bufferlist expected_bl;
  expected_bl.append(std::string(128, '1'));
  expected_bl.append(std::string(128, '\0'));
  ASSERT_TRUE(expected_bl.contents_equal(read_bl));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}
