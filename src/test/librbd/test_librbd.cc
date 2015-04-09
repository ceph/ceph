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
#include <set>
#include <vector>

#include "test/librados/test.h"
#include "test/librbd/test_fixture.h"
#include "common/errno.h"
#include "include/interval_set.h"
#include "include/stringify.h"

#include <boost/scope_exit.hpp>

using namespace std;

#define ASSERT_PASSED(x, args...) \
  do {                            \
    bool passed = false;          \
    x(args, &passed);             \
    ASSERT_TRUE(passed);          \
  } while(0)

void register_test_librbd() {
}

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

class TestLibRBD : public ::testing::Test {
public:

  TestLibRBD() : m_pool_number() {
  }

  static void SetUpTestCase() {
    static bool seeded = false;
    if (!seeded) {
      seeded = true;
      int seed = getpid();
      cout << "seed " << seed << std::endl;
      srand(seed);
    }

    _pool_names.clear();
    _unique_pool_names.clear();
    _image_number = 0;
    ASSERT_EQ("", connect_cluster(&_cluster));
  }

  static void TearDownTestCase() {
    rados_shutdown(_cluster);
    _pool_names.insert(_pool_names.end(), _unique_pool_names.begin(),
		       _unique_pool_names.end());
    for (size_t i = 1; i < _pool_names.size(); ++i) {
      ASSERT_EQ(0, _rados.pool_delete(_pool_names[i].c_str()));
    }
    if (!_pool_names.empty()) {
      ASSERT_EQ(0, destroy_one_pool_pp(_pool_names[0], _rados));
    }
  }

  virtual void SetUp() {
    ASSERT_NE("", m_pool_name = create_pool());
  }

  void validate_object_map(rbd_image_t image, bool *passed) {
    uint64_t flags;
    ASSERT_EQ(0, rbd_get_flags(image, &flags));
    *passed = ((flags & RBD_FLAG_OBJECT_MAP_INVALID) == 0);
  }

  void validate_object_map(librbd::Image &image, bool *passed) {
    uint64_t flags;
    ASSERT_EQ(0, image.get_flags(&flags));
    *passed = ((flags & RBD_FLAG_OBJECT_MAP_INVALID) == 0);
  }

  std::string get_temp_image_name() {
    ++_image_number;
    return "image" + stringify(_image_number);
  }

  std::string create_pool(bool unique = false) {
    std::string pool_name;
    if (unique) {
      pool_name = get_temp_pool_name();
      EXPECT_EQ("", create_one_pool_pp(pool_name, _rados));
      _unique_pool_names.push_back(pool_name);
    } else if (m_pool_number < _pool_names.size()) {
      pool_name = _pool_names[m_pool_number];
    } else {
      pool_name = get_temp_pool_name();
      EXPECT_EQ("", create_one_pool_pp(pool_name, _rados));
      _pool_names.push_back(pool_name);
    }
    ++m_pool_number;
    return pool_name;
  }

  static std::vector<std::string> _pool_names;
  static std::vector<std::string> _unique_pool_names;
  static rados_t _cluster;
  static librados::Rados _rados;
  static uint64_t _image_number;

  std::string m_pool_name;
  uint32_t m_pool_number;

};

std::vector<std::string> TestLibRBD::_pool_names;
std::vector<std::string> TestLibRBD::_unique_pool_names;
rados_t TestLibRBD::_cluster;
librados::Rados TestLibRBD::_rados;
uint64_t TestLibRBD::_image_number = 0;

TEST_F(TestLibRBD, CreateAndStat)
{
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  rbd_image_info_t info;
  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  printf("image has size %llu and order %d\n", (unsigned long long) info.size, info.order);
  ASSERT_EQ(info.size, size);
  ASSERT_EQ(info.order, order);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, CreateAndStatPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::image_info_t info;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size);
    ASSERT_EQ(info.order, order);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, ResizeAndStat)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_info_t info;
  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(0, rbd_resize(image, size * 4));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size * 4);

  ASSERT_EQ(0, rbd_resize(image, size / 2));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size / 2);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, ResizeAndStatPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::image_info_t info;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
    
    ASSERT_EQ(0, image.resize(size * 4));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size * 4);
    
    ASSERT_EQ(0, image.resize(size / 2));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size / 2);
    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

int test_ls(rados_ioctx_t io_ctx, size_t num_expected, ...)
{
  int num_images, i;
  char *names, *cur_name;
  va_list ap;
  size_t max_size = 1024;

  names = (char *) malloc(sizeof(char) * 1024);
  int len = rbd_list(io_ctx, names, &max_size);

  std::set<std::string> image_names;
  for (i = 0, num_images = 0, cur_name = names; cur_name < names + len; i++) {
    printf("image: %s\n", cur_name);
    image_names.insert(cur_name);
    cur_name += strlen(cur_name) + 1;
    num_images++;
  }
  free(names);

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    printf("expected = %s\n", expected);
    std::set<std::string>::iterator it = image_names.find(expected);
    if (it != image_names.end()) {
      printf("found %s\n", cur_name);
      image_names.erase(it);
    } else {
      ADD_FAILURE() << "Unable to find image " << expected;
      va_end(ap);
      return -ENOENT;
    }
  }
  va_end(ap);

  if (!image_names.empty()) {
    ADD_FAILURE() << "Unexpected images discovered";
    return -EINVAL;
  }
  return num_images;
}

TEST_F(TestLibRBD, TestCreateLsDelete)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, create_pool(true).c_str(), &ioctx);

  int order = 0;
  std::string name = get_temp_image_name();
  std::string name2 = get_temp_image_name();
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(1, test_ls(ioctx, 1, name.c_str()));
  ASSERT_EQ(0, create_image(ioctx, name2.c_str(), size, &order));
  ASSERT_EQ(2, test_ls(ioctx, 2, name.c_str(), name2.c_str()));
  ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
  ASSERT_EQ(1, test_ls(ioctx, 1, name2.c_str()));

  rados_ioctx_destroy(ioctx);
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
  EXPECT_TRUE(r >= 0);
  cout << "num images is: " << names.size() << std::endl
	    << "expected: " << num_expected << std::endl;
  int num = names.size();

  for (i = 0; i < names.size(); i++) {
    cout << "image: " << names[i] << std::endl;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    cout << "expected = " << expected << std::endl;
    vector<string>::iterator listed_name = find(names.begin(), names.end(), string(expected));
    if (listed_name == names.end()) {
      ADD_FAILURE() << "Unable to find image " << expected;
      va_end(ap);
      return -ENOENT;
    }
    names.erase(listed_name);
  }
  va_end(ap);

  if (!names.empty()) {
    ADD_FAILURE() << "Unexpected images discovered";
    return -EINVAL;
  }
  return num;
}

TEST_F(TestLibRBD, TestCreateLsDeletePP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(create_pool(true).c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    std::string name2 = get_temp_image_name();
    uint64_t size = 2 << 20;  
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name.c_str()));
    ASSERT_EQ(0, rbd.create(ioctx, name2.c_str(), size, &order));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name.c_str(), name2.c_str()));
    ASSERT_EQ(0, rbd.remove(ioctx, name.c_str()));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name2.c_str()));
  }

  ioctx.close();
}


static int print_progress_percent(uint64_t offset, uint64_t src_size,
				     void *data)
{
  float percent = ((float)offset * 100) / src_size;
  printf("%3.2f%% done\n", percent);
  return 0; 
}

TEST_F(TestLibRBD, TestCopy)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, create_pool(true).c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  std::string name2 = get_temp_image_name();
  std::string name3 = get_temp_image_name();

  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(1, test_ls(ioctx, 1, name.c_str()));
  ASSERT_EQ(0, rbd_copy(image, ioctx, name2.c_str()));
  ASSERT_EQ(2, test_ls(ioctx, 2, name.c_str(), name2.c_str()));
  ASSERT_EQ(0, rbd_copy_with_progress(image, ioctx, name3.c_str(),
				      print_progress_percent, NULL));
  ASSERT_EQ(3, test_ls(ioctx, 3, name.c_str(), name2.c_str(), name3.c_str()));

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
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

TEST_F(TestLibRBD, TestCopyPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(create_pool(true).c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    std::string name2 = get_temp_image_name();
    std::string name3 = get_temp_image_name();
    uint64_t size = 2 << 20;
    PrintProgress pp;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name.c_str()));
    ASSERT_EQ(0, image.copy(ioctx, name2.c_str()));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name.c_str(), name2.c_str()));
    ASSERT_EQ(0, image.copy_with_progress(ioctx, name3.c_str(), pp));
    ASSERT_EQ(3, test_ls_pp(rbd, ioctx, 3, name.c_str(), name2.c_str(),
			    name3.c_str()));
  }

  ioctx.close();
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
    bool found = false;
    for (j = 0; j < num_snaps; j++) {
      if (snaps[j].name == NULL)
	continue;
      if (strcmp(snaps[j].name, expected) == 0) {
	printf("found %s with size %llu\n", snaps[j].name, (unsigned long long) snaps[j].size);
	EXPECT_EQ(expected_size, snaps[j].size);
	free((void *) snaps[j].name);
	snaps[j].name = NULL;
	found = true;
	break;
      }
    }
    EXPECT_TRUE(found);
  }
  va_end(ap);

  for (i = 0; i < num_snaps; i++) {
    EXPECT_EQ((const char *)0, snaps[i].name);
  }

  return num_snaps;
}

TEST_F(TestLibRBD, TestCreateLsDeleteSnap)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  uint64_t size2 = 4 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

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
}

int test_ls_snaps(librbd::Image& image, size_t num_expected, ...)
{
  int r;
  size_t i, j;
  va_list ap;
  vector<librbd::snap_info_t> snaps;
  r = image.snap_list(snaps);
  EXPECT_TRUE(r >= 0);
  cout << "num snaps is: " << snaps.size() << std::endl
	    << "expected: " << num_expected << std::endl;

  for (i = 0; i < snaps.size(); i++) {
    cout << "snap: " << snaps[i].name << std::endl;
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
	cout << "found " << snaps[j].name << " with size " << snaps[j].size
	     << std::endl;
	EXPECT_EQ(expected_size, snaps[j].size);
	snaps[j].name = "";
	found = 1;
	break;
      }
    }
    EXPECT_TRUE(found);
  }
  va_end(ap);

  for (i = 0; i < snaps.size(); i++) {
    EXPECT_EQ("", snaps[i].name);
  }

  return snaps.size();
}

TEST_F(TestLibRBD, TestCreateLsDeleteSnapPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    uint64_t size2 = 4 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

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

void aio_write_test_data(rbd_image_t image, const char *test_data, uint64_t off, size_t len, uint32_t iohint, bool *passed)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  printf("created completion\n");
  if (iohint)
    rbd_aio_write2(image, off, len, test_data, comp, iohint);
  else
    rbd_aio_write(image, off, len, test_data, comp);
  printf("started write\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  ASSERT_EQ(0, r);
  printf("finished write\n");
  rbd_aio_release(comp);
  *passed = true;
}

void write_test_data(rbd_image_t image, const char *test_data, uint64_t off, size_t len, uint32_t iohint, bool *passed)
{
  ssize_t written;
  if (iohint)
    written = rbd_write2(image, off, len, test_data, iohint);
  else
    written = rbd_write(image, off, len, test_data);
  printf("wrote: %d\n", (int) written);
  ASSERT_EQ(len, static_cast<size_t>(written));
  *passed = true;
}

void aio_discard_test_data(rbd_image_t image, uint64_t off, uint64_t len, bool *passed)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  rbd_aio_discard(image, off, len, comp);
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  ASSERT_EQ(0, r);
  printf("aio discard: %d~%d = %d\n", (int)off, (int)len, (int)r);
  rbd_aio_release(comp);
  *passed = true;
}

void discard_test_data(rbd_image_t image, uint64_t off, size_t len, bool *passed)
{
  ssize_t written;
  written = rbd_discard(image, off, len);
  printf("discard: %d~%d = %d\n", (int)off, (int)len, (int)written);
  ASSERT_EQ(len, static_cast<size_t>(written));
  *passed = true;
}

void aio_read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len, uint32_t iohint, bool *passed)
{
  rbd_completion_t comp;
  char *result = (char *)malloc(len + 1);

  ASSERT_NE(static_cast<char *>(NULL), result);
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  printf("created completion\n");
  if (iohint)
    rbd_aio_read2(image, off, len, result, comp, iohint);
  else
    rbd_aio_read(image, off, len, result, comp);
  printf("started read\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  ASSERT_EQ(len, static_cast<size_t>(r));
  rbd_aio_release(comp);
  if (memcmp(result, expected, len)) {
    printf("read: %s\nexpected: %s\n", result, expected);
    ASSERT_EQ(0, memcmp(result, expected, len));
  }
  free(result);
  *passed = true;
}

void read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len, uint32_t iohint, bool *passed)
{
  ssize_t read;
  char *result = (char *)malloc(len + 1);

  ASSERT_NE(static_cast<char *>(NULL), result);
  if (iohint)
    read = rbd_read2(image, off, len, result, iohint);
  else
    read = rbd_read(image, off, len, result);
  printf("read: %d\n", (int) read);
  ASSERT_EQ(len, static_cast<size_t>(read));
  result[len] = '\0';
  if (memcmp(result, expected, len)) {
    printf("read: %s\nexpected: %s\n", result, expected);
    ASSERT_EQ(0, memcmp(result, expected, len));
  }
  free(result);
  *passed = true;
}

TEST_F(TestLibRBD, TestIO)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  char test_data[TEST_IO_SIZE + 1];
  char zero_data[TEST_IO_SIZE + 1];
  int i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';
  memset(zero_data, 0, sizeof(zero_data));

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(write_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_write_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(read_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_read_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  // discard 2nd, 4th sections.
  ASSERT_PASSED(discard_test_data, image, TEST_IO_SIZE, TEST_IO_SIZE);
  ASSERT_PASSED(aio_discard_test_data, image, TEST_IO_SIZE*3, TEST_IO_SIZE);

  ASSERT_PASSED(read_test_data, image, test_data,  0, TEST_IO_SIZE, 0);
  ASSERT_PASSED(read_test_data, image,  zero_data, TEST_IO_SIZE, TEST_IO_SIZE, 0);
  ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE, 0);
  ASSERT_PASSED(read_test_data, image,  zero_data, TEST_IO_SIZE*3, TEST_IO_SIZE, 0);
  ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE, 0);
  
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

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestIOWithIOHint)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  char test_data[TEST_IO_SIZE + 1];
  char zero_data[TEST_IO_SIZE + 1];
  int i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';
  memset(zero_data, 0, sizeof(zero_data));

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(write_test_data, image, test_data, TEST_IO_SIZE * i,
		  TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_write_test_data, image, test_data, TEST_IO_SIZE * i,
		  TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(read_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE,
		  LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_read_test_data, image, test_data, TEST_IO_SIZE * i,
		  TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL|LIBRADOS_OP_FLAG_FADVISE_DONTNEED);

  // discard 2nd, 4th sections.
  ASSERT_PASSED(discard_test_data, image, TEST_IO_SIZE, TEST_IO_SIZE);
  ASSERT_PASSED(aio_discard_test_data, image, TEST_IO_SIZE*3, TEST_IO_SIZE);

  ASSERT_PASSED(read_test_data, image, test_data,  0, TEST_IO_SIZE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
  ASSERT_PASSED(read_test_data, image,  zero_data, TEST_IO_SIZE, TEST_IO_SIZE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
  ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
  ASSERT_PASSED(read_test_data, image,  zero_data, TEST_IO_SIZE*3, TEST_IO_SIZE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
  ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE, 0);

  rbd_image_info_t info;
  rbd_completion_t comp;
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  // can't read or write starting past end
  ASSERT_EQ(-EINVAL, rbd_write(image, info.size, 1, test_data));
  ASSERT_EQ(-EINVAL, rbd_read(image, info.size, 1, test_data));
  // reading through end returns amount up to end
  ASSERT_EQ(10, rbd_read2(image, info.size - 10, 100, test_data,
			  LIBRADOS_OP_FLAG_FADVISE_NOCACHE));
  // writing through end returns amount up to end
  ASSERT_EQ(10, rbd_write2(image, info.size - 10, 100, test_data,
			    LIBRADOS_OP_FLAG_FADVISE_DONTNEED));

  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  ASSERT_EQ(0, rbd_aio_read2(image, info.size, 1, test_data, comp,
			     LIBRADOS_OP_FLAG_FADVISE_DONTNEED));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EINVAL, rbd_aio_get_return_value(comp));
  rbd_aio_release(comp);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestEmptyDiscard)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_PASSED(aio_discard_test_data, image, 0, 1*1024*1024);
  ASSERT_PASSED(aio_discard_test_data, image, 0, 4*1024*1024);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}


void simple_write_cb_pp(librbd::completion_t cb, void *arg)
{
  cout << "write completion cb called!" << std::endl;
}

void simple_read_cb_pp(librbd::completion_t cb, void *arg)
{
  cout << "read completion cb called!" << std::endl;
}

void aio_write_test_data(librbd::Image& image, const char *test_data,
			 off_t off, uint32_t iohint, bool *passed)
{
  ceph::bufferlist bl;
  bl.append(test_data, strlen(test_data));
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  printf("created completion\n");
  if (iohint)
    image.aio_write2(off, strlen(test_data), bl, comp, iohint);
  else
    image.aio_write(off, strlen(test_data), bl, comp);
  printf("started write\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  ASSERT_TRUE(r >= 0);
  printf("finished write\n");
  comp->release();
  *passed = true;
}

void aio_discard_test_data(librbd::Image& image, off_t off, size_t len, bool *passed)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  image.aio_discard(off, len, comp);
  comp->wait_for_complete();
  int r = comp->get_return_value();
  ASSERT_TRUE(r >= 0);
  comp->release();
  *passed = true;
}

void write_test_data(librbd::Image& image, const char *test_data, off_t off, uint32_t iohint, bool *passed)
{
  size_t written;
  size_t len = strlen(test_data);
  ceph::bufferlist bl;
  bl.append(test_data, len);
  if (iohint)
    written = image.write2(off, len, bl, iohint);
  else
    written = image.write(off, len, bl);
  printf("wrote: %u\n", (unsigned int) written);
  ASSERT_EQ(bl.length(), written);
  *passed = true;
}

void discard_test_data(librbd::Image& image, off_t off, size_t len, bool *passed)
{
  size_t written;
  written = image.discard(off, len);
  printf("discard: %u~%u\n", (unsigned)off, (unsigned)len);
  ASSERT_EQ(len, written);
  *passed = true;
}

void aio_read_test_data(librbd::Image& image, const char *expected, off_t off, size_t expected_len, uint32_t iohint, bool *passed)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_read_cb_pp);
  ceph::bufferlist bl;
  printf("created completion\n");
  if (iohint)
    image.aio_read2(off, expected_len, bl, comp, iohint);
  else
    image.aio_read(off, expected_len, bl, comp);
  printf("started read\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  ASSERT_EQ(TEST_IO_SIZE, r);
  ASSERT_EQ(0, memcmp(expected, bl.c_str(), TEST_IO_SIZE));
  printf("finished read\n");
  comp->release();
  *passed = true;
}

void read_test_data(librbd::Image& image, const char *expected, off_t off, size_t expected_len, uint32_t iohint, bool *passed)
{
  int read, total_read = 0;
  size_t len = expected_len;
  ceph::bufferlist bl;
  if (iohint)
    read = image.read2(off + total_read, len, bl, iohint);
  else
    read = image.read(off + total_read, len, bl);
  ASSERT_TRUE(read >= 0);
  std::string bl_str(bl.c_str(), read);

  printf("read: %u\n", (unsigned int) read);
  int result = memcmp(bl_str.c_str(), expected, expected_len);
  if (result != 0) {
    printf("read: %s\nexpected: %s\n", bl_str.c_str(), expected);
    ASSERT_EQ(0, result);
  }
  *passed = true;
}

TEST_F(TestLibRBD, TestIOPP) 
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    char test_data[TEST_IO_SIZE + 1];
    char zero_data[TEST_IO_SIZE + 1];
    int i;
    
    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    test_data[TEST_IO_SIZE] = '\0';
    memset(zero_data, 0, sizeof(zero_data));

    for (i = 0; i < 5; ++i)
      ASSERT_PASSED(write_test_data, image, test_data, strlen(test_data) * i, 0);
    
    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_write_test_data, image, test_data, strlen(test_data) * i, 0);
    
    for (i = 0; i < 5; ++i)
      ASSERT_PASSED(read_test_data, image, test_data, strlen(test_data) * i, TEST_IO_SIZE, 0);
    
    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_read_test_data, image, test_data, strlen(test_data) * i, TEST_IO_SIZE, 0);

    // discard 2nd, 4th sections.
    ASSERT_PASSED(discard_test_data, image, TEST_IO_SIZE, TEST_IO_SIZE);
    ASSERT_PASSED(aio_discard_test_data, image, TEST_IO_SIZE*3, TEST_IO_SIZE);
    
    ASSERT_PASSED(read_test_data, image, test_data,  0, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image,  zero_data, TEST_IO_SIZE, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image,  zero_data, TEST_IO_SIZE*3, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE, 0);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestIOPPWithIOHint)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    char test_data[TEST_IO_SIZE + 1];
    int i;

    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }

    for (i = 0; i < 5; ++i)
      ASSERT_PASSED(write_test_data, image, test_data, strlen(test_data) * i,
		    LIBRADOS_OP_FLAG_FADVISE_NOCACHE);

    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_write_test_data, image, test_data, strlen(test_data) * i,
		    LIBRADOS_OP_FLAG_FADVISE_DONTNEED);

    ASSERT_PASSED(read_test_data, image, test_data, strlen(test_data),
		  TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_RANDOM);

    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_read_test_data, image, test_data, strlen(test_data) * i,
		    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL|LIBRADOS_OP_FLAG_FADVISE_DONTNEED);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}



TEST_F(TestLibRBD, TestIOToSnapshot)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t isize = 2 << 20;
  
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), isize, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

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
  ASSERT_PASSED(read_test_data, image, orig_data, 0, TEST_IO_TO_SNAP_SIZE, 0);

  printf("write test data!\n");
  ASSERT_PASSED(write_test_data, image, test_data, 0, TEST_IO_TO_SNAP_SIZE, 0);
  ASSERT_EQ(0, rbd_snap_create(image, "written"));
  ASSERT_EQ(2, test_ls_snaps(image, 2, "orig", isize, "written", isize));

  ASSERT_PASSED(read_test_data, image, test_data, 0, TEST_IO_TO_SNAP_SIZE, 0);

  rbd_snap_set(image, "orig");
  ASSERT_PASSED(read_test_data, image, orig_data, 0, TEST_IO_TO_SNAP_SIZE, 0);

  rbd_snap_set(image, "written");
  ASSERT_PASSED(read_test_data, image, test_data, 0, TEST_IO_TO_SNAP_SIZE, 0);

  rbd_snap_set(image, "orig");

  r = rbd_write(image, 0, TEST_IO_TO_SNAP_SIZE, test_data);
  printf("write to snapshot returned %d\n", r);
  ASSERT_LT(r, 0);
  cout << cpp_strerror(-r) << std::endl;

  ASSERT_PASSED(read_test_data, image, orig_data, 0, TEST_IO_TO_SNAP_SIZE, 0);
  rbd_snap_set(image, "written");
  ASSERT_PASSED(read_test_data, image, test_data, 0, TEST_IO_TO_SNAP_SIZE, 0);

  r = rbd_snap_rollback(image, "orig");
  ASSERT_EQ(r, -EROFS);

  r = rbd_snap_set(image, NULL);
  ASSERT_EQ(r, 0);
  r = rbd_snap_rollback(image, "orig");
  ASSERT_EQ(r, 0);

  ASSERT_PASSED(write_test_data, image, test_data, 0, TEST_IO_TO_SNAP_SIZE, 0);

  rbd_flush(image);

  printf("opening testimg@orig\n");
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image_at_snap, "orig"));
  ASSERT_PASSED(read_test_data, image_at_snap, orig_data, 0, TEST_IO_TO_SNAP_SIZE, 0);
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

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestClone)
{
  rados_ioctx_t ioctx;
  rbd_image_info_t pinfo, cinfo;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent, child;
  int order = 0;

  std::string parent_name = get_temp_image_name();
  std::string child_name = get_temp_image_name();

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx, parent_name.c_str(), 4<<20, &order,
				 false, features));
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, NULL));
  printf("made parent image \"parent\"\n");

  char *data = (char *)"testdata";
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 0, strlen(data), data));

  // can't clone a non-snapshot, expect failure
  EXPECT_NE(0, rbd_clone(ioctx, parent_name.c_str(), NULL, ioctx,
			 child_name.c_str(), features, &order));

  // verify that there is no parent info on "parent"
  ASSERT_EQ(-ENOENT, rbd_get_parent_info(parent, NULL, 0, NULL, 0, NULL, 0));
  printf("parent has no parent info\n");

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  printf("made snapshot \"parent@parent_snap\"\n");
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, "parent_snap"));

  ASSERT_EQ(-EINVAL, rbd_clone(ioctx, parent_name.c_str(), "parent_snap", ioctx,
			       child_name.c_str(), features, &order));

  // unprotected image should fail unprotect
  ASSERT_EQ(-EINVAL, rbd_snap_unprotect(parent, "parent_snap"));
  printf("can't unprotect an unprotected snap\n");

  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));
  // protecting again should fail
  ASSERT_EQ(-EBUSY, rbd_snap_protect(parent, "parent_snap"));
  printf("can't protect a protected snap\n");

  // This clone and open should work
  ASSERT_EQ(0, rbd_clone(ioctx, parent_name.c_str(), "parent_snap", ioctx,
			 child_name.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx, child_name.c_str(), &child, NULL));
  printf("made and opened clone \"child\"\n");

  // check read
  ASSERT_PASSED(read_test_data, child, data, 0, strlen(data), 0);

  // check write
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(child, 20, strlen(data), data));
  ASSERT_PASSED(read_test_data, child, data, 20, strlen(data), 0);
  ASSERT_PASSED(read_test_data, child, data, 0, strlen(data), 0);

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
  
  ASSERT_PASSED(validate_object_map, child);
  ASSERT_EQ(0, rbd_close(child));

  ASSERT_PASSED(validate_object_map, parent);
  ASSERT_EQ(-EBUSY, rbd_snap_remove(parent, "parent_snap"));
  printf("can't remove parent while child still exists\n");
  ASSERT_EQ(0, rbd_remove(ioctx, child_name.c_str()));
  ASSERT_EQ(-EBUSY, rbd_snap_remove(parent, "parent_snap"));
  printf("can't remove parent while still protected\n");
  ASSERT_EQ(0, rbd_snap_unprotect(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_remove(parent, "parent_snap"));
  printf("removed parent snap after unprotecting\n");

  ASSERT_EQ(0, rbd_close(parent));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestClone2)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent, child;
  int order = 0;

  std::string parent_name = get_temp_image_name();
  std::string child_name = get_temp_image_name();

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx, parent_name.c_str(), 4<<20, &order,
				 false, features));
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, NULL));
  printf("made parent image \"parent\"\n");

  char *data = (char *)"testdata";
  char *childata = (char *)"childata";
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 0, strlen(data), data));
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 12, strlen(data), data));

  // can't clone a non-snapshot, expect failure
  EXPECT_NE(0, rbd_clone(ioctx, parent_name.c_str(), NULL, ioctx,
			 child_name.c_str(), features, &order));

  // verify that there is no parent info on "parent"
  ASSERT_EQ(-ENOENT, rbd_get_parent_info(parent, NULL, 0, NULL, 0, NULL, 0));
  printf("parent has no parent info\n");

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  printf("made snapshot \"parent@parent_snap\"\n");
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, "parent_snap"));

  ASSERT_EQ(-EINVAL, rbd_clone(ioctx, parent_name.c_str(), "parent_snap", ioctx,
			       child_name.c_str(), features, &order));

  // unprotected image should fail unprotect
  ASSERT_EQ(-EINVAL, rbd_snap_unprotect(parent, "parent_snap"));
  printf("can't unprotect an unprotected snap\n");

  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));
  // protecting again should fail
  ASSERT_EQ(-EBUSY, rbd_snap_protect(parent, "parent_snap"));
  printf("can't protect a protected snap\n");

  // This clone and open should work
  ASSERT_EQ(0, rbd_clone(ioctx, parent_name.c_str(), "parent_snap", ioctx,
			 child_name.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx, child_name.c_str(), &child, NULL));
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

  ASSERT_PASSED(validate_object_map, child);
  ASSERT_PASSED(validate_object_map, parent);

  ASSERT_EQ(0, rbd_close(child));
  ASSERT_EQ(0, rbd_close(parent));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestCoR)
{
  if (!g_conf->rbd_clone_copy_on_read) {
    std::cout << "SKIPPING due to disabled rbd_copy_on_read" << std::endl;
    return;
  }

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent, child;
  int order = 12; // smallest object size is 4K
  const uint64_t image_size = 4<<20;
  const int object_size = 1<<12;
  const int object_num = image_size / object_size;
  map<uint64_t, uint64_t> write_tracker;
  set<string> obj_checker;
  rbd_image_info_t p_info, c_info;
  rados_list_ctx_t list_ctx;
  const char *entry;

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx, "parent", image_size, &order, false, features));
  ASSERT_EQ(0, rbd_open(ioctx, "parent", &parent, NULL));
  printf("made parent image \"parent\": %ldK (%d * %dK)\n", 
         image_size, object_num, object_size/1024);

  // write something into parent
  char test_data[TEST_IO_SIZE + 1];
  char zero_data[TEST_IO_SIZE + 1];
  int i;
  int count = 0;

  for (i = 0; i < TEST_IO_SIZE; ++i) 
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  test_data[TEST_IO_SIZE] = '\0';
  memset(zero_data, 0, sizeof(zero_data));

  // generate a random map which covers every objects with random offset
  while (count < 100) {
    uint64_t ono = rand() % object_num;
    if (write_tracker.find(ono) == write_tracker.end()) {
      uint64_t offset = rand() % (object_size - TEST_IO_SIZE);
      write_tracker.insert(pair<uint64_t, uint64_t>(ono, offset));
      count++;
    }
  }

  printf("generated random write map:\n");
  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr)
    printf("\t [%-8ld, %-8ld]\n", itr->first, itr->second);

  printf("write data based on random map\n");
  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr) {
    printf("\twrite object-%-4ld\t", itr->first);
    ASSERT_PASSED(write_test_data, parent, test_data, itr->first * object_size + itr->second, TEST_IO_SIZE, 0);
  }

  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
         itr != write_tracker.end(); ++itr) {
    printf("\tread object-%-4ld\t", itr->first);
    ASSERT_PASSED(read_test_data, parent, test_data, itr->first * object_size + itr->second, TEST_IO_SIZE, 0);
  }

  // find out what objects the parent image has generated
  ASSERT_EQ(0, rbd_stat(parent, &p_info, sizeof(p_info)));
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &list_ctx));
  while (rados_nobjects_list_next(list_ctx, &entry, NULL, NULL) != -ENOENT) {
    if (strstr(entry, p_info.block_name_prefix)) {
      const char *block_name_suffix = entry + strlen(p_info.block_name_prefix) + 1;
      obj_checker.insert(block_name_suffix);
    }
  }
  rados_nobjects_list_close(list_ctx);
  ASSERT_EQ(obj_checker.size(), write_tracker.size());

  // create a snapshot, reopen as the parent we're interested in and protect it
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, "parent", &parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));
  ASSERT_PASSED(validate_object_map, parent);
  ASSERT_EQ(0, rbd_close(parent));
  printf("made snapshot \"parent@parent_snap\" and protect it\n");

  // create a copy-on-read clone and open it
  ASSERT_EQ(0, rbd_clone(ioctx, "parent", "parent_snap", ioctx, "child",
	    features, &order));
  ASSERT_EQ(0, rbd_open(ioctx, "child", &child, NULL));
  printf("made and opened clone \"child\"\n");

  printf("read from \"child\"\n");
  {
    map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
    printf("\tread object-%-4ld\t", itr->first);
    ASSERT_PASSED(read_test_data, child, test_data, itr->first * object_size + itr->second, TEST_IO_SIZE, 0);
  }

  for (map<uint64_t, uint64_t>::iterator itr = write_tracker.begin();
       itr != write_tracker.end(); ++itr) {
    printf("\tread object-%-4ld\t", itr->first);
    ASSERT_PASSED(read_test_data, child, test_data, itr->first * object_size + itr->second, TEST_IO_SIZE, 0);
  }

  printf("read again reversely\n");
  for (map<uint64_t, uint64_t>::iterator itr = --write_tracker.end();
     itr != write_tracker.begin(); --itr) {
    printf("\tread object-%-4ld\t", itr->first);
    ASSERT_PASSED(read_test_data, child, test_data, itr->first * object_size + itr->second, TEST_IO_SIZE, 0);
  }

  // close child to flush all copy-on-read
  ASSERT_EQ(0, rbd_close(child));

  printf("check whether child image has the same set of objects as parent\n");
  ASSERT_EQ(0, rbd_open(ioctx, "child", &child, NULL));
  ASSERT_EQ(0, rbd_stat(child, &c_info, sizeof(c_info)));
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &list_ctx));
  while (rados_nobjects_list_next(list_ctx, &entry, NULL, NULL) != -ENOENT) {
    if (strstr(entry, c_info.block_name_prefix)) {
      const char *block_name_suffix = entry + strlen(c_info.block_name_prefix) + 1;
      set<string>::iterator it = obj_checker.find(block_name_suffix);
      ASSERT_TRUE(it != obj_checker.end());
      obj_checker.erase(it);
    }
  }
  rados_nobjects_list_close(list_ctx);
  ASSERT_TRUE(obj_checker.empty());
  ASSERT_PASSED(validate_object_map, child);
  ASSERT_EQ(0, rbd_close(child));

  rados_ioctx_destroy(ioctx);
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

TEST_F(TestLibRBD, ListChildren)
{
  rados_ioctx_t ioctx1, ioctx2;
  string pool_name1 = create_pool(true);
  string pool_name2 = create_pool(true);
  ASSERT_NE("", pool_name2);

  rados_ioctx_create(_cluster, pool_name1.c_str(), &ioctx1);
  rados_ioctx_create(_cluster, pool_name2.c_str(), &ioctx2);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent;
  int order = 0;

  std::string parent_name = get_temp_image_name();
  std::string child_name1 = get_temp_image_name();
  std::string child_name2 = get_temp_image_name();
  std::string child_name3 = get_temp_image_name();
  std::string child_name4 = get_temp_image_name();

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx1, parent_name.c_str(), 4<<20, &order,
				 false, features));
  ASSERT_EQ(0, rbd_open(ioctx1, parent_name.c_str(), &parent, NULL));
  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_set(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));

  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx1, parent_name.c_str(), &parent, "parent_snap"));

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx2,
			 child_name1.c_str(), features, &order));
  test_list_children(parent, 1, pool_name2.c_str(), child_name1.c_str());

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx1,
			 child_name2.c_str(), features, &order));
  test_list_children(parent, 2, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str());

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx2,
		         child_name3.c_str(), features, &order));
  test_list_children(parent, 3, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str());

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx2,
			 child_name4.c_str(), features, &order));
  test_list_children(parent, 4, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name1.c_str()));
  test_list_children(parent, 3,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name3.c_str()));
  test_list_children(parent, 2,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name4.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name4.c_str()));
  test_list_children(parent, 1,
		     pool_name1.c_str(), child_name2.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx1, child_name2.c_str()));
  test_list_children(parent, 0);

  ASSERT_EQ(0, rbd_snap_unprotect(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_remove(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_remove(ioctx1, parent_name.c_str()));
  rados_ioctx_destroy(ioctx1);
  rados_ioctx_destroy(ioctx2);
}

TEST_F(TestLibRBD, ListChildrenTiered)
{
  string pool_name1 = m_pool_name;
  string pool_name2 = create_pool(true);
  string pool_name3 = create_pool(true);
  ASSERT_NE("", pool_name2);
  ASSERT_NE("", pool_name3);

  std::string cmdstr = "{\"prefix\": \"osd tier add\", \"pool\": \"" +
     pool_name1 + "\", \"tierpool\":\"" + pool_name3 + "\", \"force_nonempty\":\"\"}";
  char *cmd[1];
  cmd[0] = (char *)cmdstr.c_str();
  ASSERT_EQ(0, rados_mon_command(_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));

  cmdstr = "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" +
     pool_name3 + "\", \"mode\":\"writeback\"}";
  cmd[0] = (char *)cmdstr.c_str();
  ASSERT_EQ(0, rados_mon_command(_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));

  cmdstr = "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" +
     pool_name1 + "\", \"overlaypool\":\"" + pool_name3 + "\"}";
  cmd[0] = (char *)cmdstr.c_str();
  ASSERT_EQ(0, rados_mon_command(_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));

  EXPECT_EQ(0, rados_wait_for_latest_osdmap(_cluster));

  string parent_name = get_temp_image_name();
  string child_name1 = get_temp_image_name();
  string child_name2 = get_temp_image_name();
  string child_name3 = get_temp_image_name();
  string child_name4 = get_temp_image_name();

  rados_ioctx_t ioctx1, ioctx2;
  rados_ioctx_create(_cluster, pool_name1.c_str(), &ioctx1);
  rados_ioctx_create(_cluster, pool_name2.c_str(), &ioctx2);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t parent;
  int order = 0;

  // make a parent to clone from
  ASSERT_EQ(0, create_image_full(ioctx1, parent_name.c_str(), 4<<20, &order,
				 false, features));
  ASSERT_EQ(0, rbd_open(ioctx1, parent_name.c_str(), &parent, NULL));
  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_set(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));

  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx1, parent_name.c_str(), &parent, "parent_snap"));

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx2,
			 child_name1.c_str(), features, &order));
  test_list_children(parent, 1, pool_name2.c_str(), child_name1.c_str());

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx1,
			 child_name2.c_str(), features, &order));
  test_list_children(parent, 2, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str());

  // read from the cache to populate it
  rbd_image_t tier_image;
  ASSERT_EQ(0, rbd_open(ioctx1, child_name2.c_str(), &tier_image, NULL));
  size_t len = 4 * 1024 * 1024;
  char* buf = (char*)malloc(len);
  ssize_t size = rbd_read(tier_image, 0, len, buf);
  ASSERT_GT(size, 0);
  free(buf);
  ASSERT_EQ(0, rbd_close(tier_image));

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx2,
			 child_name3.c_str(), features, &order));
  test_list_children(parent, 3, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str());

  ASSERT_EQ(0, rbd_clone(ioctx1, parent_name.c_str(), "parent_snap", ioctx2,
			 child_name4.c_str(), features, &order));
  test_list_children(parent, 4, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name1.c_str()));
  test_list_children(parent, 3,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name3.c_str()));
  test_list_children(parent, 2,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name4.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name4.c_str()));
  test_list_children(parent, 1,
		     pool_name1.c_str(), child_name2.c_str());

  ASSERT_EQ(0, rbd_remove(ioctx1, child_name2.c_str()));
  test_list_children(parent, 0);

  ASSERT_EQ(0, rbd_snap_unprotect(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_remove(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_remove(ioctx1, parent_name.c_str()));
  rados_ioctx_destroy(ioctx1);
  rados_ioctx_destroy(ioctx2);
  cmdstr = "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" +
     pool_name1 + "\"}";
  cmd[0] = (char *)cmdstr.c_str();
  ASSERT_EQ(0, rados_mon_command(_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));
  cmdstr = "{\"prefix\": \"osd tier remove\", \"pool\": \"" +
     pool_name1 + "\", \"tierpool\":\"" + pool_name3 + "\"}";
  cmd[0] = (char *)cmdstr.c_str();
  ASSERT_EQ(0, rados_mon_command(_cluster, (const char **)cmd, 1, "", 0, NULL, 0, NULL, 0));
}

TEST_F(TestLibRBD, LockingPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    std::string cookie1 = "foo";
    std::string cookie2 = "bar";

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

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
}

TEST_F(TestLibRBD, FlushAio)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  size_t num_aios = 256;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

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

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, FlushAioPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    size_t num_aios = 256;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

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
    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
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

TEST_F(TestLibRBD, DiffIterate)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

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

TEST_F(TestLibRBD, DiffIterateDiscard)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

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
  ASSERT_PASSED(validate_object_map, image);
}

TEST_F(TestLibRBD, DiffIterateStress)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 400 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

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

  ASSERT_PASSED(validate_object_map, image);
}

TEST_F(TestLibRBD, DiffIterateRegression6926)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  vector<diff_extent> extents;
  ceph::bufferlist bl;

  ASSERT_EQ(0, image.diff_iterate(NULL, 0, size,
      			    vector_iterate_cb, (void *) &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_EQ(0, image.snap_create("snap1"));
  char data[256];
  memset(data, 1, sizeof(data));
  bl.append(data, 256);
  ASSERT_EQ(256, image.write(0, 256, bl));

  extents.clear();
  ASSERT_EQ(0, image.diff_iterate(NULL, 0, size,
      			    vector_iterate_cb, (void *) &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true), extents[0]);

  ASSERT_EQ(0, image.snap_set("snap1"));
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate(NULL, 0, size,
      			    vector_iterate_cb, (void *) &extents));
  ASSERT_EQ(static_cast<size_t>(0), extents.size());
}

TEST_F(TestLibRBD, ZeroLengthWrite)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  char read_data[1];
  ASSERT_EQ(0, rbd_write(image, 0, 0, NULL));
  ASSERT_EQ(1, rbd_read(image, 0, 1, read_data));
  ASSERT_EQ('\0', read_data[0]);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}


TEST_F(TestLibRBD, ZeroLengthDiscard)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  const char *data = "blah";
  char read_data[strlen(data)];
  ASSERT_EQ((int)strlen(data), rbd_write(image, 0, strlen(data), data));
  ASSERT_EQ(0, rbd_discard(image, 0, 0));
  ASSERT_EQ((int)strlen(data), rbd_read(image, 0, strlen(data), read_data));
  ASSERT_EQ(0, memcmp(data, read_data, strlen(data)));

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, ZeroLengthRead)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  char read_data[1];
  ASSERT_EQ(0, rbd_read(image, 0, 0, read_data));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, LargeCacheRead)
{
  if (!g_conf->rbd_cache) {
    std::cout << "SKIPPING due to disabled cache" << std::endl;
    return;
  }

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

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
}

TEST_F(TestLibRBD, TestPendingAio)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  int features = RBD_FEATURE_LAYERING;
  rbd_image_t image;
  int order = 0;

  std::string name = get_temp_image_name();

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

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));
  for (size_t i = 0; i < num_aios; ++i) {
    ASSERT_EQ(1, rbd_aio_is_complete(comps[i]));
    rbd_aio_release(comps[i]);
  }

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, SnapCreateViaLockOwner)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_EXCLUSIVE_LOCK);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  bufferlist bl;
  ASSERT_EQ(0, image1.write(0, 0, bl));

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.snap_create("snap1"));
  ASSERT_TRUE(image1.snap_exists("snap1"));
  ASSERT_TRUE(image2.snap_exists("snap1"));

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
}

TEST_F(TestLibRBD, FlattenViaLockOwner)
{
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string parent_name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(), size, &order));

  librbd::Image parent_image;
  ASSERT_EQ(0, rbd.open(ioctx, parent_image, parent_name.c_str(), NULL));
  ASSERT_EQ(0, parent_image.snap_create("snap1"));
  ASSERT_EQ(0, parent_image.snap_protect("snap1"));

  uint64_t features;
  ASSERT_EQ(0, parent_image.features(&features));

  std::string name = get_temp_image_name();
  EXPECT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "snap1", ioctx,
			 name.c_str(), features, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  bufferlist bl;
  ASSERT_EQ(0, image1.write(0, 0, bl));

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.flatten());

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
  ASSERT_PASSED(validate_object_map, image1);
}

TEST_F(TestLibRBD, ResizeViaLockOwner)
{
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  bufferlist bl;
  ASSERT_EQ(0, image1.write(0, 0, bl));

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.resize(0));

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
  ASSERT_PASSED(validate_object_map, image1);
}

class RBDWriter : public Thread {
 public:
   RBDWriter(librbd::Image &image) : m_image(image) {};
 protected:
  void *entry() {
    librbd::image_info_t info;
    int r = m_image.stat(info, sizeof(info));
    assert(r == 0);
    bufferlist bl;
    bl.append("foo");
    for (unsigned i = 0; i < info.num_objs; ++i) {
      r = m_image.write((1 << info.order) * i, bl.length(), bl);
      assert(r == (int) bl.length());
    }
    return NULL;
  }
 private:
  librbd::Image &m_image;
};

TEST_F(TestLibRBD, ObjectMapConsistentSnap)
{
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << 20;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  RBDWriter writer(image1);
  writer.create();

  int num_snaps = 10;
  for (int i = 0; i < num_snaps; ++i) {
    std::string snap_name = "snap" + stringify(i);
    ASSERT_EQ(0, image1.snap_create(snap_name.c_str()));
  }

  writer.join();

  for (int i = 0; i < num_snaps; ++i) {
    std::string snap_name = "snap" + stringify(i);
    ASSERT_EQ(0, image1.snap_set(snap_name.c_str()));
    ASSERT_PASSED(validate_object_map, image1);
  }

  ASSERT_EQ(0, image1.snap_set(NULL));
  ASSERT_PASSED(validate_object_map, image1);
}
