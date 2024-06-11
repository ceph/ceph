// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#include "include/event_type.h"
#include "include/err.h"
#include "common/ceph_mutex.h"
#include "json_spirit/json_spirit.h"
#include "test/librados/crimson_utils.h"

#include "gtest/gtest.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <poll.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <sstream>
#include <list>
#include <set>
#include <thread>
#include <vector>
#include <limits>

#include "test/librados/test.h"
#include "test/librados/test_cxx.h"
#include "test/librbd/test_support.h"
#include "common/event_socket.h"
#include "include/interval_set.h"
#include "include/stringify.h"

#include <boost/assign/list_of.hpp>
#include <boost/scope_exit.hpp>

#ifdef HAVE_EVENTFD
#include <sys/eventfd.h>
#endif

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

using namespace std;

using std::chrono::seconds;

#define ASSERT_PASSED0(x)         \
  do {                            \
    bool passed = false;          \
    x(&passed);                   \
    ASSERT_TRUE(passed);          \
  } while(0)

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
  if (c && strlen(c) > 0) {
    stringstream ss;
    ss << c;
    ss >> *features;
    if (ss.fail())
      return -EINVAL;
    *old_format = false;
    cout << "using new format!" << std::endl;
  } else {
    *old_format = true;
    *features = 0;
    cout << "using old format" << std::endl;
  }

  return 0;
}

static int create_image_full(rados_ioctx_t ioctx, const char *name,
                             uint64_t size, int *order, int old_format,
                             uint64_t features)
{
  if (old_format) {
    // ensure old-format tests actually use the old format
    int r = rados_conf_set(rados_ioctx_get_cluster(ioctx),
                           "rbd_default_format", "1");
    if (r < 0) {
      return r;
    }
    return rbd_create(ioctx, name, size, order);
  } else if ((features & RBD_FEATURE_STRIPINGV2) != 0) {
    uint64_t stripe_unit = IMAGE_STRIPE_UNIT;
    if (*order) {
      // use a conservative stripe_unit for non default order
      stripe_unit = (1ull << (*order-1));
    }

    printf("creating image with stripe unit: %" PRIu64 ", "
	   "stripe count: %" PRIu64 "\n",
           stripe_unit, IMAGE_STRIPE_COUNT);
    return rbd_create3(ioctx, name, size, features, order,
                       stripe_unit, IMAGE_STRIPE_COUNT);
  } else {
    return rbd_create2(ioctx, name, size, features, order);
  }
}

static int clone_image(rados_ioctx_t p_ioctx,
                       rbd_image_t p_image, const char *p_name,
                       const char *p_snap_name, rados_ioctx_t c_ioctx,
                       const char *c_name, uint64_t features, int *c_order)
{
  uint64_t stripe_unit, stripe_count;

  int r;
  r = rbd_get_stripe_unit(p_image, &stripe_unit);
  if (r != 0) {
    return r;
  }

  r = rbd_get_stripe_count(p_image, &stripe_count);
  if (r != 0) {
    return r;
  }

  return rbd_clone2(p_ioctx, p_name, p_snap_name, c_ioctx,
                    c_name, features, c_order, stripe_unit, stripe_count);
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
    librados::Rados rados(ioctx);
    int r = rados.conf_set("rbd_default_format", "1");
    if (r < 0) {
      return r;
    }
    return rbd.create(ioctx, name, size, order);
  } else {
    return rbd.create2(ioctx, name, size, features, order);
  }
}



void simple_write_cb(rbd_completion_t cb, void *arg)
{
  printf("write completion cb called!\n");
}

void simple_read_cb(rbd_completion_t cb, void *arg)
{
  printf("read completion cb called!\n");
}

void aio_write_test_data_and_poll(rbd_image_t image, int fd, const char *test_data,
                                  uint64_t off, size_t len, uint32_t iohint, bool *passed)
{
  rbd_completion_t comp;
  uint64_t data = 0x123;
  rbd_aio_create_completion((void*)&data, (rbd_callback_t) simple_write_cb, &comp);
  printf("created completion\n");
  printf("started write\n");
  if (iohint)
    rbd_aio_write2(image, off, len, test_data, comp, iohint);
  else
    rbd_aio_write(image, off, len, test_data, comp);

  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = POLLIN;

  ASSERT_EQ(1, poll(&pfd, 1, -1));
  ASSERT_TRUE(pfd.revents & POLLIN);

  rbd_completion_t comps[1];
  ASSERT_EQ(1, rbd_poll_io_events(image, comps, 1));
  uint64_t count;
  ASSERT_EQ(static_cast<ssize_t>(sizeof(count)),
            read(fd, &count, sizeof(count)));
  int r = rbd_aio_get_return_value(comps[0]);
  ASSERT_TRUE(rbd_aio_is_complete(comps[0]));
  ASSERT_TRUE(*(uint64_t*)rbd_aio_get_arg(comps[0]) == data);
  printf("return value is: %d\n", r);
  ASSERT_EQ(0, r);
  printf("finished write\n");
  rbd_aio_release(comps[0]);
  *passed = true;
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

void aio_read_test_data_and_poll(rbd_image_t image, int fd, const char *expected,
                                 uint64_t off, size_t len, uint32_t iohint, bool *passed)
{
  rbd_completion_t comp;
  char *result = (char *)malloc(len + 1);

  ASSERT_NE(static_cast<char *>(NULL), result);
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  printf("created completion\n");
  printf("started read\n");
  if (iohint)
    rbd_aio_read2(image, off, len, result, comp, iohint);
  else
    rbd_aio_read(image, off, len, result, comp);

  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = POLLIN;

  ASSERT_EQ(1, poll(&pfd, 1, -1));
  ASSERT_TRUE(pfd.revents & POLLIN);

  rbd_completion_t comps[1];
  ASSERT_EQ(1, rbd_poll_io_events(image, comps, 1));
  uint64_t count;
  ASSERT_EQ(static_cast<ssize_t>(sizeof(count)),
            read(fd, &count, sizeof(count)));

  int r = rbd_aio_get_return_value(comps[0]);
  ASSERT_TRUE(rbd_aio_is_complete(comps[0]));
  printf("return value is: %d\n", r);
  ASSERT_EQ(len, static_cast<size_t>(r));
  rbd_aio_release(comps[0]);
  if (memcmp(result, expected, len)) {
    printf("read: %s\nexpected: %s\n", result, expected);
    ASSERT_EQ(0, memcmp(result, expected, len));
  }
  free(result);
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

void aio_writesame_test_data(rbd_image_t image, const char *test_data, uint64_t off, uint64_t len,
                             uint64_t data_len, uint32_t iohint, bool *passed)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  printf("created completion\n");
  int r;
  r = rbd_aio_writesame(image, off, len, test_data, data_len, comp, iohint);
  printf("started writesame\n");
  if (len % data_len) {
    ASSERT_EQ(-EINVAL, r);
    printf("expected fail, finished writesame\n");
    rbd_aio_release(comp);
    *passed = true;
    return;
  }

  rbd_aio_wait_for_complete(comp);
  r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  ASSERT_EQ(0, r);
  printf("finished writesame\n");
  rbd_aio_release(comp);

  //verify data
  printf("to verify the data\n");
  ssize_t read;
  char *result = (char *)malloc(data_len+ 1);
  ASSERT_NE(static_cast<char *>(NULL), result);
  uint64_t left = len;
  while (left > 0) {
    read = rbd_read(image, off, data_len, result);
    ASSERT_EQ(data_len, static_cast<size_t>(read));
    result[data_len] = '\0';
    if (memcmp(result, test_data, data_len)) {
      printf("read: %d ~ %d\n", (int) off, (int) read);
      printf("read: %s\nexpected: %s\n", result, test_data);
      ASSERT_EQ(0, memcmp(result, test_data, data_len));
    }
    off += data_len;
    left -= data_len;
  }
  ASSERT_EQ(0U, left);
  free(result);
  printf("verified\n");

  *passed = true;
}

void writesame_test_data(rbd_image_t image, const char *test_data, uint64_t off, uint64_t len,
                         uint64_t data_len, uint32_t iohint, bool *passed)
{
  ssize_t written;
  written = rbd_writesame(image, off, len, test_data, data_len, iohint);
  if (len % data_len) {
    ASSERT_EQ(-EINVAL, written);
    printf("expected fail, finished writesame\n");
    *passed = true;
    return;
  }
  ASSERT_EQ(len, static_cast<size_t>(written));
  printf("wrote: %d\n", (int) written);

  //verify data
  printf("to verify the data\n");
  ssize_t read;
  char *result = (char *)malloc(data_len+ 1);
  ASSERT_NE(static_cast<char *>(NULL), result);
  uint64_t left = len;
  while (left > 0) {
    read = rbd_read(image, off, data_len, result);
    ASSERT_EQ(data_len, static_cast<size_t>(read));
    result[data_len] = '\0';
    if (memcmp(result, test_data, data_len)) {
      printf("read: %d ~ %d\n", (int) off, (int) read);
      printf("read: %s\nexpected: %s\n", result, test_data);
      ASSERT_EQ(0, memcmp(result, test_data, data_len));
    }
    off += data_len;
    left -= data_len;
  }
  ASSERT_EQ(0U, left);
  free(result);
  printf("verified\n");

  *passed = true;
}

void aio_compare_and_write_test_data(rbd_image_t image, const char *cmp_data,
                                     const char *test_data, uint64_t off,
                                     size_t len, uint32_t iohint, bool *passed)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  printf("created completion\n");

  uint64_t mismatch_offset;
  rbd_aio_compare_and_write(image, off, len, cmp_data, test_data, comp, &mismatch_offset, iohint);
  printf("started aio compare and write\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  ASSERT_EQ(0, r);
  printf("finished aio compare and write\n");
  rbd_aio_release(comp);
  *passed = true;
}

void compare_and_write_test_data(rbd_image_t image, const char *cmp_data,
                                 const char *test_data, uint64_t off, size_t len,
                                 uint64_t *mismatch_off, uint32_t iohint, bool *passed)
{
  printf("start compare and write\n");
  ssize_t written;
  written = rbd_compare_and_write(image, off, len, cmp_data, test_data, mismatch_off, iohint);
  printf("compare and  wrote: %d\n", (int) written);
  ASSERT_EQ(len, static_cast<size_t>(written));
  *passed = true;
}

class TestLibRBD : public ::testing::Test {
public:

  TestLibRBD() : m_pool_number() {
  }

  static void SetUpTestCase() {
    _pool_names.clear();
    _unique_pool_names.clear();
    _image_number = 0;
    ASSERT_EQ("", connect_cluster(&_cluster));
    ASSERT_EQ("", connect_cluster_pp(_rados));

    create_optional_data_pool();
  }

  static void TearDownTestCase() {
    rados_shutdown(_cluster);
    _rados.wait_for_latest_osdmap();
    _pool_names.insert(_pool_names.end(), _unique_pool_names.begin(),
                       _unique_pool_names.end());
    for (size_t i = 1; i < _pool_names.size(); ++i) {
      ASSERT_EQ(0, _rados.pool_delete(_pool_names[i].c_str()));
    }
    if (!_pool_names.empty()) {
      ASSERT_EQ(0, destroy_one_pool_pp(_pool_names[0], _rados));
    }
  }

  void SetUp() override {
    ASSERT_NE("", m_pool_name = create_pool());
  }

  bool is_skip_partial_discard_enabled() {
    std::string value;
    EXPECT_EQ(0, _rados.conf_get("rbd_skip_partial_discard", value));
    return value == "true";
  }

  bool is_skip_partial_discard_enabled(rbd_image_t image) {
    if (is_skip_partial_discard_enabled()) {
      rbd_flush(image);
      uint64_t features;
      EXPECT_EQ(0, rbd_get_features(image, &features));
      return !(features & RBD_FEATURE_DIRTY_CACHE);
    }
    return false;
  }

  bool is_skip_partial_discard_enabled(librbd::Image& image) {
    if (is_skip_partial_discard_enabled()) {
      image.flush();
      uint64_t features;
      EXPECT_EQ(0, image.features(&features));
      return !(features & RBD_FEATURE_DIRTY_CACHE);
    }
    return false;
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

  static std::string get_temp_image_name() {
    ++_image_number;
    return "image" + stringify(_image_number);
  }

  static void create_optional_data_pool() {
    bool created = false;
    std::string data_pool;
    ASSERT_EQ(0, create_image_data_pool(_rados, data_pool, &created));
    if (!data_pool.empty()) {
      printf("using image data pool: %s\n", data_pool.c_str());
      if (created) {
        _unique_pool_names.push_back(data_pool);
      }
    }
  }

  std::string create_pool(bool unique = false) {
    librados::Rados rados;
    std::string pool_name;
    if (unique) {
      pool_name = get_temp_pool_name("test-librbd-");
      EXPECT_EQ("", create_one_pool_pp(pool_name, rados));
      _unique_pool_names.push_back(pool_name);
    } else if (m_pool_number < _pool_names.size()) {
      pool_name = _pool_names[m_pool_number];
    } else {
      pool_name = get_temp_pool_name("test-librbd-");
      EXPECT_EQ("", create_one_pool_pp(pool_name, rados));
      _pool_names.push_back(pool_name);
    }
    ++m_pool_number;
    return pool_name;
  }

  void test_io(rbd_image_t image) {
    bool skip_discard = is_skip_partial_discard_enabled(image);

    char test_data[TEST_IO_SIZE + 1];
    char zero_data[TEST_IO_SIZE + 1];
    char mismatch_data[TEST_IO_SIZE + 1];
    int i;
    uint64_t mismatch_offset;

    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    test_data[TEST_IO_SIZE] = '\0';
    memset(zero_data, 0, sizeof(zero_data));
    memset(mismatch_data, 9, sizeof(mismatch_data));

    for (i = 0; i < 5; ++i)
      ASSERT_PASSED(write_test_data, image, test_data, TEST_IO_SIZE * i,
                    TEST_IO_SIZE, 0);

    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_write_test_data, image, test_data, TEST_IO_SIZE * i,
                    TEST_IO_SIZE, 0);

    for (i = 0; i < 5; ++i)
      ASSERT_PASSED(compare_and_write_test_data, image, test_data, test_data,
                    TEST_IO_SIZE * i, TEST_IO_SIZE, &mismatch_offset, 0);

    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_compare_and_write_test_data, image, test_data, test_data,
                    TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

    for (i = 0; i < 5; ++i)
      ASSERT_PASSED(read_test_data, image, test_data, TEST_IO_SIZE * i,
                    TEST_IO_SIZE, 0);

    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_read_test_data, image, test_data, TEST_IO_SIZE * i,
                    TEST_IO_SIZE, 0);

    // discard 2nd, 4th sections.
    ASSERT_PASSED(discard_test_data, image, TEST_IO_SIZE, TEST_IO_SIZE);
    ASSERT_PASSED(aio_discard_test_data, image, TEST_IO_SIZE*3, TEST_IO_SIZE);

    ASSERT_PASSED(read_test_data, image, test_data,  0, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
                  TEST_IO_SIZE, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*2,
                  TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
                  TEST_IO_SIZE*3, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*4,
                  TEST_IO_SIZE, 0);

    for (i = 0; i < 15; ++i) {
      if (i % 3 == 2) {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, 0);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, 0);
      } else if (i % 3 == 1) {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE + i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE + i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      } else {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      }
    }
    for (i = 0; i < 15; ++i) {
      if (i % 3 == 2) {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data,
                      TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE,
                      0);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data,
                      TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE,
                      0);
      } else if (i % 3 == 1) {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data,
                      TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data,
                      TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      } else {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data,
                      TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data,
                      TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      }
    }

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

    ASSERT_PASSED(write_test_data, image, zero_data, 0, TEST_IO_SIZE,
                  LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
    mismatch_offset = 123;
    ASSERT_EQ(-EILSEQ, rbd_compare_and_write(image, 0, TEST_IO_SIZE,
              mismatch_data, mismatch_data, &mismatch_offset, 0));
    ASSERT_EQ(0U, mismatch_offset);
    rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
    mismatch_offset = 123;
    ASSERT_EQ(0, rbd_aio_compare_and_write(image, 0, TEST_IO_SIZE, mismatch_data,
              mismatch_data, comp, &mismatch_offset, 0));
    ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
    ASSERT_EQ(-EILSEQ, rbd_aio_get_return_value(comp));
    ASSERT_EQ(0U, mismatch_offset);
    rbd_aio_release(comp);

    ASSERT_PASSED(validate_object_map, image);
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

TEST_F(TestLibRBD, CreateWithSameDataPool)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  rbd_image_t image;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  bool old_format;
  uint64_t features;
  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

  rbd_image_options_t image_options;
  rbd_image_options_create(&image_options);
  BOOST_SCOPE_EXIT( (&image_options) ) {
    rbd_image_options_destroy(image_options);
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, rbd_image_options_set_uint64(image_options,
                                            RBD_IMAGE_OPTION_FEATURES,
                                            features));
  ASSERT_EQ(0, rbd_image_options_set_string(image_options,
                                            RBD_IMAGE_OPTION_DATA_POOL,
                                            m_pool_name.c_str()));

  ASSERT_EQ(0, rbd_create4(ioctx, name.c_str(), size, image_options));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

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

TEST_F(TestLibRBD, GetId)
{
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), 0, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  char id[4096];
  if (!is_feature_enabled(0)) {
    // V1 image
    ASSERT_EQ(-EINVAL, rbd_get_id(image, id, sizeof(id)));
  } else {
    ASSERT_EQ(-ERANGE, rbd_get_id(image, id, 0));
    ASSERT_EQ(0, rbd_get_id(image, id, sizeof(id)));
    ASSERT_LT(0U, strlen(id));

    ASSERT_EQ(0, rbd_close(image));
    ASSERT_EQ(0, rbd_open_by_id(ioctx, id, &image, NULL));
    size_t name_len = 0;
    ASSERT_EQ(-ERANGE, rbd_get_name(image, NULL, &name_len));
    ASSERT_EQ(name_len, name.size() + 1);
    char image_name[name_len];
    ASSERT_EQ(0, rbd_get_name(image, image_name, &name_len));
    ASSERT_STREQ(name.c_str(), image_name);
  }

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, GetIdPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();

  std::string id;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), 0, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
  if (!is_feature_enabled(0)) {
    // V1 image
    ASSERT_EQ(-EINVAL, image.get_id(&id));
  } else {
    ASSERT_EQ(0, image.get_id(&id));
    ASSERT_LT(0U, id.size());

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, rbd.open_by_id(ioctx, image, id.c_str(), NULL));
    std::string image_name;
    ASSERT_EQ(0, image.get_name(&image_name));
    ASSERT_EQ(name, image_name);
  }
}

TEST_F(TestLibRBD, GetBlockNamePrefix)
{
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), 0, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  char prefix[4096];
  ASSERT_EQ(-ERANGE, rbd_get_block_name_prefix(image, prefix, 0));
  ASSERT_EQ(0, rbd_get_block_name_prefix(image, prefix, sizeof(prefix)));
  ASSERT_LT(0U, strlen(prefix));

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, GetBlockNamePrefixPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), 0, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
  ASSERT_LT(0U, image.get_block_name_prefix().size());
}

TEST_F(TestLibRBD, TestGetCreateTimestamp)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), 0, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  struct timespec timestamp;
  ASSERT_EQ(0, rbd_get_create_timestamp(image, &timestamp));
  ASSERT_LT(0, timestamp.tv_sec);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, GetCreateTimestampPP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), 0, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  struct timespec timestamp;
  ASSERT_EQ(0, image.get_create_timestamp(&timestamp));
  ASSERT_LT(0, timestamp.tv_sec);
}

TEST_F(TestLibRBD, OpenAio)
{
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  rbd_image_info_t info;
  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));

  rbd_completion_t open_comp;
  ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &open_comp));
  ASSERT_EQ(0, rbd_aio_open(ioctx, name.c_str(), &image, NULL, open_comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(open_comp));
  ASSERT_EQ(1, rbd_aio_is_complete(open_comp));
  ASSERT_EQ(0, rbd_aio_get_return_value(open_comp));
  rbd_aio_release(open_comp);

  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  printf("image has size %llu and order %d\n", (unsigned long long) info.size, info.order);
  ASSERT_EQ(info.size, size);
  ASSERT_EQ(info.order, order);

  rbd_completion_t close_comp;
  ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &close_comp));
  ASSERT_EQ(0, rbd_aio_close(image, close_comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(close_comp));
  ASSERT_EQ(1, rbd_aio_is_complete(close_comp));
  ASSERT_EQ(0, rbd_aio_get_return_value(close_comp));
  rbd_aio_release(close_comp);

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, OpenAioFail)
{
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  std::string name = get_temp_image_name();
  rbd_image_t image;
  rbd_completion_t open_comp;
  ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &open_comp));
  ASSERT_EQ(0, rbd_aio_open(ioctx, name.c_str(), &image, NULL, open_comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(open_comp));
  ASSERT_EQ(1, rbd_aio_is_complete(open_comp));
  ASSERT_EQ(-ENOENT, rbd_aio_get_return_value(open_comp));
  rbd_aio_release(open_comp);

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, OpenAioPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::image_info_t info;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::RBD::AioCompletion *open_comp =
      new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, rbd.aio_open(ioctx, image, name.c_str(), NULL, open_comp));
  ASSERT_EQ(0, open_comp->wait_for_complete());
  ASSERT_EQ(1, open_comp->is_complete());
  ASSERT_EQ(0, open_comp->get_return_value());
  open_comp->release();

  ASSERT_EQ(0, image.stat(info, sizeof(info)));
  ASSERT_EQ(info.size, size);
  ASSERT_EQ(info.order, order);

  // reopen
  open_comp = new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, rbd.aio_open(ioctx, image, name.c_str(), NULL, open_comp));
  ASSERT_EQ(0, open_comp->wait_for_complete());
  ASSERT_EQ(1, open_comp->is_complete());
  ASSERT_EQ(0, open_comp->get_return_value());
  open_comp->release();

  // close
  librbd::RBD::AioCompletion *close_comp =
      new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, image.aio_close(close_comp));
  ASSERT_EQ(0, close_comp->wait_for_complete());
  ASSERT_EQ(1, close_comp->is_complete());
  ASSERT_EQ(0, close_comp->get_return_value());
  close_comp->release();

  // close closed image
  close_comp = new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(-EINVAL, image.aio_close(close_comp));
  close_comp->release();

  ioctx.close();
}

TEST_F(TestLibRBD, OpenAioFailPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    std::string name = get_temp_image_name();

    librbd::RBD::AioCompletion *open_comp =
      new librbd::RBD::AioCompletion(NULL, NULL);
    ASSERT_EQ(0, rbd.aio_open(ioctx, image, name.c_str(), NULL, open_comp));
    ASSERT_EQ(0, open_comp->wait_for_complete());
    ASSERT_EQ(1, open_comp->is_complete());
    ASSERT_EQ(-ENOENT, open_comp->get_return_value());
    open_comp->release();
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

  // downsizing without allowing shrink should fail
  // and image size should not change
  ASSERT_EQ(-EINVAL, rbd_resize2(image, size / 4, false, NULL, NULL));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size / 2);

  ASSERT_EQ(0, rbd_resize2(image, size / 4, true, NULL, NULL));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size / 4);

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

TEST_F(TestLibRBD, UpdateWatchAndResize)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  struct Watcher {
    rbd_image_t &m_image;
    std::mutex m_lock;
    std::condition_variable m_cond;
    size_t m_size = 0;
    static void cb(void *arg) {
      Watcher *watcher = static_cast<Watcher *>(arg);
      watcher->handle_notify();
    }
    explicit Watcher(rbd_image_t &image) : m_image(image) {}
    void handle_notify() {
      rbd_image_info_t info;
      ASSERT_EQ(0, rbd_stat(m_image, &info, sizeof(info)));
      std::lock_guard<std::mutex> locker(m_lock);
      m_size = info.size;
      m_cond.notify_one();
    }
    void wait_for_size(size_t size) {
      std::unique_lock<std::mutex> locker(m_lock);
      ASSERT_TRUE(m_cond.wait_for(locker, seconds(5),
      				  [size, this] {
				    return this->m_size == size;}));
    }
  } watcher(image);
  uint64_t handle;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(0, rbd_update_watch(image, &handle, Watcher::cb, &watcher));

  ASSERT_EQ(0, rbd_resize(image, size * 4));
  watcher.wait_for_size(size * 4);

  ASSERT_EQ(0, rbd_resize(image, size / 2));
  watcher.wait_for_size(size / 2);

  ASSERT_EQ(0, rbd_update_unwatch(image, handle));

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, UpdateWatchAndResizePP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    struct Watcher : public librbd::UpdateWatchCtx {
      explicit Watcher(librbd::Image &image) : m_image(image) {
      }
      void handle_notify() override {
        librbd::image_info_t info;
	ASSERT_EQ(0, m_image.stat(info, sizeof(info)));
        std::lock_guard<std::mutex> locker(m_lock);
        m_size = info.size;
	m_cond.notify_one();
      }
      void wait_for_size(size_t size) {
	std::unique_lock<std::mutex> locker(m_lock);
	ASSERT_TRUE(m_cond.wait_for(locker, seconds(5),
				    [size, this] {
				      return this->m_size == size;}));
      }
      librbd::Image &m_image;
      std::mutex m_lock;
      std::condition_variable m_cond;
      size_t m_size = 0;
    } watcher(image);
    uint64_t handle;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    ASSERT_EQ(0, image.update_watch(&watcher, &handle));

    ASSERT_EQ(0, image.resize(size * 4));
    watcher.wait_for_size(size * 4);

    ASSERT_EQ(0, image.resize(size / 2));
    watcher.wait_for_size(size / 2);

    ASSERT_EQ(0, image.update_unwatch(handle));
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
      printf("found %s\n", expected);
      image_names.erase(it);
      printf("erased %s\n", expected);
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

  ASSERT_EQ(0, test_ls(ioctx, 0));
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(1, test_ls(ioctx, 1, name.c_str()));
  ASSERT_EQ(0, create_image(ioctx, name2.c_str(), size, &order));
  ASSERT_EQ(2, test_ls(ioctx, 2, name.c_str(), name2.c_str()));
  ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
  ASSERT_EQ(1, test_ls(ioctx, 1, name2.c_str()));

  ASSERT_EQ(-ENOENT, rbd_remove(ioctx, name.c_str()));

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
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name2.c_str(), size, &order));
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
  rbd_image_t image2;
  rbd_image_t image3;
  int order = 0;
  std::string name = get_temp_image_name();
  std::string name2 = get_temp_image_name();
  std::string name3 = get_temp_image_name();

  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(1, test_ls(ioctx, 1, name.c_str()));

  size_t sum_key_len = 0;
  size_t sum_value_len = 0;
  std::string key;
  std::string val;
  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_set(image, key.c_str(), val.c_str()));

    sum_key_len += (key.size() + 1);
    sum_value_len += (val.size() + 1);
  }

  char keys[1024];
  char vals[1024];
  size_t keys_len = sizeof(keys);
  size_t vals_len = sizeof(vals);

  char value[1024];
  size_t value_len = sizeof(value);

  ASSERT_EQ(0, rbd_copy(image, ioctx, name2.c_str()));
  ASSERT_EQ(2, test_ls(ioctx, 2, name.c_str(), name2.c_str()));
  ASSERT_EQ(0, rbd_open(ioctx, name2.c_str(), &image2, NULL));
  ASSERT_EQ(0, rbd_metadata_list(image2, "key", 70, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, sum_key_len);
  ASSERT_EQ(vals_len, sum_value_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(image2, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }

  ASSERT_EQ(0, rbd_copy_with_progress(image, ioctx, name3.c_str(),
				      print_progress_percent, NULL));
  ASSERT_EQ(3, test_ls(ioctx, 3, name.c_str(), name2.c_str(), name3.c_str()));

  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  ASSERT_EQ(0, rbd_open(ioctx, name3.c_str(), &image3, NULL));
  ASSERT_EQ(0, rbd_metadata_list(image3, "key", 70, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, sum_key_len);
  ASSERT_EQ(vals_len, sum_value_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(image3, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }

  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_close(image2));
  ASSERT_EQ(0, rbd_close(image3));
  rados_ioctx_destroy(ioctx);
}

class PrintProgress : public librbd::ProgressContext
{
public:
  int update_progress(uint64_t offset, uint64_t src_size) override
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
    librbd::Image image2;
    librbd::Image image3;
    int order = 0;
    std::string name = get_temp_image_name();
    std::string name2 = get_temp_image_name();
    std::string name3 = get_temp_image_name();
    uint64_t size = 2 << 20;
    PrintProgress pp;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string key;
    std::string val;
    for (int i = 1; i <= 70; i++) {
      key = "key" + stringify(i);
      val = "value" + stringify(i);
      ASSERT_EQ(0, image.metadata_set(key, val));
    }

    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name.c_str()));
    ASSERT_EQ(0, image.copy(ioctx, name2.c_str()));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name.c_str(), name2.c_str()));
    ASSERT_EQ(0, rbd.open(ioctx, image2, name2.c_str(), NULL));

    map<string, bufferlist> pairs;
    std::string value;
    ASSERT_EQ(0, image2.metadata_list("", 70, &pairs));
    ASSERT_EQ(70U, pairs.size());

    for (int i = 1; i <= 70; i++) {
      key = "key" + stringify(i);
      val = "value" + stringify(i);
      ASSERT_EQ(0, image2.metadata_get(key.c_str(), &value));
      ASSERT_STREQ(val.c_str(), value.c_str());
    }

    ASSERT_EQ(0, image.copy_with_progress(ioctx, name3.c_str(), pp));
    ASSERT_EQ(3, test_ls_pp(rbd, ioctx, 3, name.c_str(), name2.c_str(),
                           name3.c_str()));
    ASSERT_EQ(0, rbd.open(ioctx, image3, name3.c_str(), NULL));

    pairs.clear();
    ASSERT_EQ(0, image3.metadata_list("", 70, &pairs));
    ASSERT_EQ(70U, pairs.size());

    for (int i = 1; i <= 70; i++) {
      key = "key" + stringify(i);
      val = "value" + stringify(i);
      ASSERT_EQ(0, image3.metadata_get(key.c_str(), &value));
      ASSERT_STREQ(val.c_str(), value.c_str());
    }
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestDeepCopy)
{
  REQUIRE_FORMAT_V2();
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, create_pool(true).c_str(), &ioctx);
  BOOST_SCOPE_EXIT_ALL( (&ioctx) ) {
    rados_ioctx_destroy(ioctx);
  };

  rbd_image_t image;
  rbd_image_t image2;
  rbd_image_t image3;
  rbd_image_t image4;
  rbd_image_t image5;
  rbd_image_t image6;
  int order = 0;
  std::string name = get_temp_image_name();
  std::string name2 = get_temp_image_name();
  std::string name3 = get_temp_image_name();
  std::string name4 = get_temp_image_name();
  std::string name5 = get_temp_image_name();
  std::string name6 = get_temp_image_name();

  uint64_t size = 2 << 20;

  rbd_image_options_t opts;
  rbd_image_options_create(&opts);
  BOOST_SCOPE_EXIT_ALL( (&opts) ) {
    rbd_image_options_destroy(opts);
  };

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  BOOST_SCOPE_EXIT_ALL( (&image) ) {
    ASSERT_EQ(0, rbd_close(image));
  };
  ASSERT_EQ(1, test_ls(ioctx, 1, name.c_str()));

  size_t sum_key_len = 0;
  size_t sum_value_len = 0;
  std::string key;
  std::string val;
  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_set(image, key.c_str(), val.c_str()));

    sum_key_len += (key.size() + 1);
    sum_value_len += (val.size() + 1);
  }

  char keys[1024];
  char vals[1024];
  size_t keys_len = sizeof(keys);
  size_t vals_len = sizeof(vals);

  char value[1024];
  size_t value_len = sizeof(value);

  ASSERT_EQ(0, rbd_deep_copy(image, ioctx, name2.c_str(), opts));
  ASSERT_EQ(2, test_ls(ioctx, 2, name.c_str(), name2.c_str()));
  ASSERT_EQ(0, rbd_open(ioctx, name2.c_str(), &image2, NULL));
  BOOST_SCOPE_EXIT_ALL( (&image2) ) {
    ASSERT_EQ(0, rbd_close(image2));
  };
  ASSERT_EQ(0, rbd_metadata_list(image2, "key", 70, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, sum_key_len);
  ASSERT_EQ(vals_len, sum_value_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(image2, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }

  ASSERT_EQ(0, rbd_deep_copy_with_progress(image, ioctx, name3.c_str(), opts,
                                           print_progress_percent, NULL));
  ASSERT_EQ(3, test_ls(ioctx, 3, name.c_str(), name2.c_str(), name3.c_str()));

  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  ASSERT_EQ(0, rbd_open(ioctx, name3.c_str(), &image3, NULL));
  BOOST_SCOPE_EXIT_ALL( (&image3) ) {
    ASSERT_EQ(0, rbd_close(image3));
  };
  ASSERT_EQ(0, rbd_metadata_list(image3, "key", 70, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, sum_key_len);
  ASSERT_EQ(vals_len, sum_value_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(image3, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }

  ASSERT_EQ(0, rbd_snap_create(image, "deep_snap"));
  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, "deep_snap"));
  ASSERT_EQ(0, rbd_snap_protect(image, "deep_snap"));
  ASSERT_EQ(0, rbd_clone3(ioctx, name.c_str(), "deep_snap", ioctx,
            name4.c_str(), opts));

  ASSERT_EQ(4, test_ls(ioctx, 4, name.c_str(), name2.c_str(), name3.c_str(),
            name4.c_str()));
  ASSERT_EQ(0, rbd_open(ioctx, name4.c_str(), &image4, NULL));
  BOOST_SCOPE_EXIT_ALL( (&image4) ) {
    ASSERT_EQ(0, rbd_close(image4));
  };
  ASSERT_EQ(0, rbd_snap_create(image4, "deep_snap"));

  ASSERT_EQ(0, rbd_deep_copy(image4, ioctx, name5.c_str(), opts));
  ASSERT_EQ(5, test_ls(ioctx, 5, name.c_str(), name2.c_str(), name3.c_str(),
            name4.c_str(), name5.c_str()));
  ASSERT_EQ(0, rbd_open(ioctx, name5.c_str(), &image5, NULL));
  BOOST_SCOPE_EXIT_ALL( (&image5) ) {
    ASSERT_EQ(0, rbd_close(image5));
  };
  ASSERT_EQ(0, rbd_metadata_list(image5, "key", 70, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, sum_key_len);
  ASSERT_EQ(vals_len, sum_value_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(image5, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }

  ASSERT_EQ(0, rbd_deep_copy_with_progress(image4, ioctx, name6.c_str(), opts,
                                           print_progress_percent, NULL));
  ASSERT_EQ(6, test_ls(ioctx, 6, name.c_str(), name2.c_str(), name3.c_str(),
            name4.c_str(), name5.c_str(), name6.c_str()));

  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  ASSERT_EQ(0, rbd_open(ioctx, name6.c_str(), &image6, NULL));
  BOOST_SCOPE_EXIT_ALL( (&image6) ) {
    ASSERT_EQ(0, rbd_close(image6));
  };
  ASSERT_EQ(0, rbd_metadata_list(image6, "key", 70, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, sum_key_len);
  ASSERT_EQ(vals_len, sum_value_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(image6, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }
}

TEST_F(TestLibRBD, TestDeepCopyPP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(create_pool(true).c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    librbd::Image image2;
    librbd::Image image3;
    int order = 0;
    std::string name = get_temp_image_name();
    std::string name2 = get_temp_image_name();
    std::string name3 = get_temp_image_name();
    uint64_t size = 2 << 20;
    librbd::ImageOptions opts;
    PrintProgress pp;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string key;
    std::string val;
    for (int i = 1; i <= 70; i++) {
      key = "key" + stringify(i);
      val = "value" + stringify(i);
      ASSERT_EQ(0, image.metadata_set(key, val));
    }

    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name.c_str()));
    ASSERT_EQ(0, image.deep_copy(ioctx, name2.c_str(), opts));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name.c_str(), name2.c_str()));
    ASSERT_EQ(0, rbd.open(ioctx, image2, name2.c_str(), NULL));

    map<string, bufferlist> pairs;
    std::string value;
    ASSERT_EQ(0, image2.metadata_list("", 70, &pairs));
    ASSERT_EQ(70U, pairs.size());

    for (int i = 1; i <= 70; i++) {
      key = "key" + stringify(i);
      val = "value" + stringify(i);
      ASSERT_EQ(0, image2.metadata_get(key.c_str(), &value));
      ASSERT_STREQ(val.c_str(), value.c_str());
    }

    ASSERT_EQ(0, image.deep_copy_with_progress(ioctx, name3.c_str(), opts, pp));
    ASSERT_EQ(3, test_ls_pp(rbd, ioctx, 3, name.c_str(), name2.c_str(),
                            name3.c_str()));
    ASSERT_EQ(0, rbd.open(ioctx, image3, name3.c_str(), NULL));

    pairs.clear();
    ASSERT_EQ(0, image3.metadata_list("", 70, &pairs));
    ASSERT_EQ(70U, pairs.size());

    for (int i = 1; i <= 70; i++) {
      key = "key" + stringify(i);
      val = "value" + stringify(i);
      ASSERT_EQ(0, image3.metadata_get(key.c_str(), &value));
      ASSERT_STREQ(val.c_str(), value.c_str());
    }
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

int test_get_snapshot_timestamp(rbd_image_t image, uint64_t snap_id)
{
  struct timespec timestamp;
  EXPECT_EQ(0, rbd_snap_get_timestamp(image, snap_id, &timestamp));
  EXPECT_LT(0, timestamp.tv_sec);
  return 0;
}

TEST_F(TestLibRBD, TestGetSnapShotTimeStamp)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int num_snaps, max_size = 10;
  rbd_snap_info_t snaps[max_size];

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(-ENOENT, rbd_snap_get_timestamp(image, 0, NULL));

  ASSERT_EQ(0, rbd_snap_create(image, "snap1"));
  num_snaps = rbd_snap_list(image, snaps, &max_size);
  ASSERT_EQ(1, num_snaps);
  ASSERT_EQ(0, test_get_snapshot_timestamp(image, snaps[0].id));
  free((void *)snaps[0].name);

  ASSERT_EQ(0, rbd_snap_create(image, "snap2"));
  num_snaps = rbd_snap_list(image, snaps, &max_size);
  ASSERT_EQ(2, num_snaps);
  ASSERT_EQ(0, test_get_snapshot_timestamp(image, snaps[0].id));
  ASSERT_EQ(0, test_get_snapshot_timestamp(image, snaps[1].id));
  free((void *)snaps[0].name);
  free((void *)snaps[1].name);

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
   
    bool exists;
    ASSERT_EQ(0, image.snap_exists2("snap1", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_EQ(0, image.snap_exists2("snap1", &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(1, test_ls_snaps(image, 1, "snap1", size));
    ASSERT_EQ(0, image.resize(size2));
    ASSERT_EQ(0, image.snap_exists2("snap2", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, image.snap_create("snap2"));
    ASSERT_EQ(0, image.snap_exists2("snap2", &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(2, test_ls_snaps(image, 2, "snap1", size, "snap2", size2));
    ASSERT_EQ(0, image.snap_remove("snap1"));
    ASSERT_EQ(0, image.snap_exists2("snap1", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(1, test_ls_snaps(image, 1, "snap2", size2));
    ASSERT_EQ(0, image.snap_remove("snap2"));
    ASSERT_EQ(0, image.snap_exists2("snap2", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, test_ls_snaps(image, 0));
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestGetNameIdSnapPP)
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

    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_EQ(0, image.snap_create("snap2"));
    ASSERT_EQ(0, image.snap_create("snap3"));
    vector<librbd::snap_info_t> snaps;
    int r = image.snap_list(snaps);
    EXPECT_TRUE(r >= 0);

    for (size_t i = 0; i < snaps.size(); ++i) {
      std::string expected_snap_name;
      image.snap_get_name(snaps[i].id, &expected_snap_name);
      ASSERT_EQ(expected_snap_name, snaps[i].name);
    }

    for (size_t i = 0; i < snaps.size(); ++i) {
      uint64_t expected_snap_id;
      image.snap_get_id(snaps[i].name, &expected_snap_id);
      ASSERT_EQ(expected_snap_id, snaps[i].id);
    }

    ASSERT_EQ(0, image.snap_remove("snap1"));
    ASSERT_EQ(0, image.snap_remove("snap2"));
    ASSERT_EQ(0, image.snap_remove("snap3"));
    ASSERT_EQ(0, test_ls_snaps(image, 0));
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCreateLsRenameSnapPP)
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
    
    bool exists;
    ASSERT_EQ(0, image.snap_exists2("snap1", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_EQ(0, image.snap_exists2("snap1", &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(1, test_ls_snaps(image, 1, "snap1", size));
    ASSERT_EQ(0, image.resize(size2));
    ASSERT_EQ(0, image.snap_exists2("snap2", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, image.snap_create("snap2"));
    ASSERT_EQ(0, image.snap_exists2("snap2", &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(2, test_ls_snaps(image, 2, "snap1", size, "snap2", size2));
    ASSERT_EQ(0, image.snap_rename("snap1","snap1-rename"));
    ASSERT_EQ(2, test_ls_snaps(image, 2, "snap1-rename", size, "snap2", size2));
    ASSERT_EQ(0, image.snap_exists2("snap1", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, image.snap_exists2("snap1-rename", &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(0, image.snap_remove("snap1-rename"));
    ASSERT_EQ(0, image.snap_rename("snap2","snap2-rename"));
    ASSERT_EQ(1, test_ls_snaps(image, 1, "snap2-rename", size2));
    ASSERT_EQ(0, image.snap_exists2("snap2", &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, image.snap_exists2("snap2-rename", &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(0, image.snap_remove("snap2-rename"));
    ASSERT_EQ(0, test_ls_snaps(image, 0));
  }

  ioctx.close();
}

TEST_F(TestLibRBD, ConcurrentCreatesUnvalidatedPool)
{
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, create_pool(true).c_str(),
                                  &ioctx));

  std::vector<std::string> names;
  for (int i = 0; i < 4; i++) {
    names.push_back(get_temp_image_name());
  }
  uint64_t size = 2 << 20;

  std::vector<std::thread> threads;
  for (const auto& name : names) {
    threads.emplace_back([ioctx, &name, size]() {
      int order = 0;
      ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& name : names) {
    ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
  }
  rados_ioctx_destroy(ioctx);
}

static void remove_full_try(rados_ioctx_t ioctx, const std::string& image_name,
                            const std::string& data_pool_name)
{
  int order = 0;
  uint64_t quota = 10 << 20;
  uint64_t size = 5 * quota;
  ASSERT_EQ(0, create_image(ioctx, image_name.c_str(), size, &order));

  std::string cmdstr = "{\"prefix\": \"osd pool set-quota\", \"pool\": \"" +
      data_pool_name + "\", \"field\": \"max_bytes\", \"val\": \"" +
      std::to_string(quota) + "\"}";
  char *cmd[1];
  cmd[0] = (char *)cmdstr.c_str();
  ASSERT_EQ(0, rados_mon_command(rados_ioctx_get_cluster(ioctx),
                                 (const char **)cmd, 1, "", 0, nullptr, 0,
                                 nullptr, 0));

  rados_set_pool_full_try(ioctx);

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, image_name.c_str(), &image, nullptr));

  uint64_t off;
  size_t len = 1 << 20;
  ssize_t ret;
  for (off = 0; off < size; off += len) {
    ret = rbd_write_zeroes(image, off, len,
                           RBD_WRITE_ZEROES_FLAG_THICK_PROVISION,
                           LIBRADOS_OP_FLAG_FADVISE_FUA);
    if (ret < 0) {
      break;
    }
    ASSERT_EQ(ret, len);
    sleep(1);
  }
  ASSERT_TRUE(off >= quota && off < size);
  ASSERT_EQ(ret, -EDQUOT);

  ASSERT_EQ(0, rbd_close(image));

  // If using a separate data pool, also make the metadata pool full
  char pool_name[80];
  ASSERT_GT(rados_ioctx_get_pool_name(ioctx, pool_name, sizeof(pool_name)), 0);
  if (pool_name != data_pool_name) {
    struct rados_pool_stat_t st;
    ASSERT_EQ(0, rados_ioctx_pool_stat(ioctx, &st));
    cmdstr = "{\"prefix\": \"osd pool set-quota\", \"pool\": \"" +
        std::string(pool_name) + "\", \"field\": \"max_objects\", \"val\": \"" +
        std::to_string(st.num_objects) + "\"}";
    cmd[0] = (char *)cmdstr.c_str();
    ASSERT_EQ(0, rados_mon_command(rados_ioctx_get_cluster(ioctx),
                                   (const char **)cmd, 1, "", 0, nullptr, 0,
                                   nullptr, 0));

    for (int i = 0; i < 50; i++) {
      auto temp_image_name = TestLibRBD::get_temp_image_name();
      ret = create_image(ioctx, temp_image_name.c_str(), size, &order);
      if (ret < 0) {
        break;
      }
      sleep(1);
    }
    ASSERT_EQ(ret, -EDQUOT);
  }

  // make sure we have latest map that marked the pool(s) full
  ASSERT_EQ(0, rados_wait_for_latest_osdmap(rados_ioctx_get_cluster(ioctx)));
  ASSERT_EQ(0, rbd_remove(ioctx, image_name.c_str()));
}

TEST_F(TestLibRBD, RemoveFullTry)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));
  REQUIRE(!is_librados_test_stub(_rados));

  rados_ioctx_t ioctx;
  auto pool_name = create_pool(true);
  ASSERT_EQ(0, rados_ioctx_create(_cluster, pool_name.c_str(), &ioctx));
  ASSERT_EQ(0, rbd_pool_init(ioctx, true));
  // cancel out rbd_default_data_pool -- we need an image without
  // a separate data pool
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "conf_rbd_default_data_pool",
                                     pool_name.c_str()));

  auto image_name = get_temp_image_name();
  remove_full_try(ioctx, image_name, pool_name);

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, RemoveFullTryDataPool)
{
  REQUIRE_FORMAT_V2();
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));
  REQUIRE(!is_librados_test_stub(_rados));

  rados_ioctx_t ioctx;
  auto pool_name = create_pool(true);
  auto data_pool_name = create_pool(true);
  ASSERT_EQ(0, rados_ioctx_create(_cluster, pool_name.c_str(), &ioctx));
  ASSERT_EQ(0, rbd_pool_init(ioctx, true));
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "conf_rbd_default_data_pool",
                                     data_pool_name.c_str()));

  auto image_name = get_temp_image_name();
  remove_full_try(ioctx, image_name, data_pool_name);

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, RemoveFullTryNamespace)
{
  REQUIRE_FORMAT_V2();
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));
  REQUIRE(!is_librados_test_stub(_rados));

  rados_ioctx_t ioctx;
  auto pool_name = create_pool(true);
  ASSERT_EQ(0, rados_ioctx_create(_cluster, pool_name.c_str(), &ioctx));
  // cancel out rbd_default_data_pool -- we need an image without
  // a separate data pool
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "conf_rbd_default_data_pool",
                                     pool_name.c_str()));
  ASSERT_EQ(0, rbd_namespace_create(ioctx, "name1"));
  rados_ioctx_set_namespace(ioctx, "name1");

  auto image_name = get_temp_image_name();
  remove_full_try(ioctx, image_name, pool_name);

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, RemoveFullTryNamespaceDataPool)
{
  REQUIRE_FORMAT_V2();
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));
  REQUIRE(!is_librados_test_stub(_rados));

  rados_ioctx_t ioctx;
  auto pool_name = create_pool(true);
  auto data_pool_name = create_pool(true);
  ASSERT_EQ(0, rados_ioctx_create(_cluster, pool_name.c_str(), &ioctx));
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "conf_rbd_default_data_pool",
                                     data_pool_name.c_str()));
  ASSERT_EQ(0, rbd_namespace_create(ioctx, "name1"));
  rados_ioctx_set_namespace(ioctx, "name1");

  auto image_name = get_temp_image_name();
  remove_full_try(ioctx, image_name, data_pool_name);

  rados_ioctx_destroy(ioctx);
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
  ASSERT_EQ(0, rados_conf_set(_cluster, "rbd_read_from_replica_policy", "balance"));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  test_io(image);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestEncryptionLUKS1)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 32 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rados_conf_set(
          _cluster, "rbd_read_from_replica_policy", "balance"));

  rbd_image_t image;
  rbd_encryption_luks1_format_options_t luks1_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  rbd_encryption_luks2_format_options_t luks2_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  rbd_encryption_luks_format_options_t luks_opts = {
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

#ifndef HAVE_LIBCRYPTSETUP
  ASSERT_EQ(-ENOTSUP, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
#else
  ASSERT_EQ(-EINVAL, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
  ASSERT_EQ(0, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_EQ(-EEXIST, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));

  test_io(image);

  ASSERT_PASSED(write_test_data, image, "test", 0, 4, 0);
  ASSERT_EQ(0, rbd_close(image));

  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(-EINVAL, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_EQ(0, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_PASSED(read_test_data, image, "test", 0, 4, 0);
  ASSERT_EQ(0, rbd_close(image));

  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(-EINVAL, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_EQ(0, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
  ASSERT_PASSED(read_test_data, image, "test", 0, 4, 0);
#endif

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestEncryptionLUKS2)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 32 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rados_conf_set(
          _cluster, "rbd_read_from_replica_policy", "balance"));

  rbd_image_t image;
  rbd_encryption_luks1_format_options_t luks1_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  rbd_encryption_luks2_format_options_t luks2_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  rbd_encryption_luks_format_options_t luks_opts = {
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

#ifndef HAVE_LIBCRYPTSETUP
  ASSERT_EQ(-ENOTSUP, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
  ASSERT_EQ(-ENOTSUP, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
#else
  ASSERT_EQ(-EINVAL, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
  ASSERT_EQ(0, rbd_encryption_format(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_EQ(-EEXIST, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));

  test_io(image);

  ASSERT_PASSED(write_test_data, image, "test", 0, 4, 0);
  ASSERT_EQ(0, rbd_close(image));

  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(-EINVAL, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_EQ(0, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)));
  ASSERT_PASSED(read_test_data, image, "test", 0, 4, 0);
  ASSERT_EQ(0, rbd_close(image));

  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(-EINVAL, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)));
  ASSERT_EQ(0, rbd_encryption_load(
          image, RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)));
  ASSERT_PASSED(read_test_data, image, "test", 0, 4, 0);
#endif

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

#ifdef HAVE_LIBCRYPTSETUP

TEST_F(TestLibRBD, TestCloneEncryption)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);
  ASSERT_EQ(0, rados_conf_set(
          _cluster, "rbd_read_from_replica_policy", "balance"));

  // create base image, write 'a's
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 256 << 20;
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_PASSED(write_test_data, image, "aaaa", 0, 4, 0);
  ASSERT_EQ(0, rbd_flush(image));

  // clone, encrypt with LUKS1, write 'b's
  ASSERT_EQ(0, rbd_snap_create(image, "snap"));
  ASSERT_EQ(0, rbd_snap_protect(image, "snap"));

  rbd_image_options_t image_opts;
  rbd_image_options_create(&image_opts);
  BOOST_SCOPE_EXIT_ALL( (&image_opts) ) {
    rbd_image_options_destroy(image_opts);
  };
  std::string child1_name = get_temp_image_name();
  ASSERT_EQ(0, rbd_clone3(ioctx, name.c_str(), "snap", ioctx,
                          child1_name.c_str(), image_opts));

  rbd_image_t child1;
  ASSERT_EQ(0, rbd_open(ioctx, child1_name.c_str(), &child1, NULL));

  rbd_encryption_luks1_format_options_t child1_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(-EINVAL, rbd_encryption_load(
          child1, RBD_ENCRYPTION_FORMAT_LUKS1, &child1_opts,
          sizeof(child1_opts)));
  ASSERT_EQ(0, rbd_encryption_format(
          child1, RBD_ENCRYPTION_FORMAT_LUKS1, &child1_opts,
          sizeof(child1_opts)));
  ASSERT_EQ(0, rbd_encryption_load(
          child1, RBD_ENCRYPTION_FORMAT_LUKS1, &child1_opts,
          sizeof(child1_opts)));
  ASSERT_PASSED(write_test_data, child1, "bbbb", 64 << 20, 4, 0);
  ASSERT_EQ(0, rbd_flush(child1));

  // clone, encrypt with LUKS2 (same passphrase), write 'c's
  ASSERT_EQ(0, rbd_snap_create(child1, "snap"));
  ASSERT_EQ(0, rbd_snap_protect(child1, "snap"));

  std::string child2_name = get_temp_image_name();
  ASSERT_EQ(0, rbd_clone3(ioctx, child1_name.c_str(), "snap", ioctx,
                          child2_name.c_str(), image_opts));

  rbd_image_t child2;
  ASSERT_EQ(0, rbd_open(ioctx, child2_name.c_str(), &child2, NULL));

  rbd_encryption_luks2_format_options_t child2_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, rbd_encryption_format(
          child2, RBD_ENCRYPTION_FORMAT_LUKS2, &child2_opts,
          sizeof(child2_opts)));
  rbd_encryption_luks_format_options_t child2_lopts = {
          .passphrase = "password",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, rbd_encryption_load(
          child2, RBD_ENCRYPTION_FORMAT_LUKS, &child2_lopts,
          sizeof(child2_lopts)));
  ASSERT_PASSED(write_test_data, child2, "cccc", 128 << 20, 4, 0);
  ASSERT_EQ(0, rbd_flush(child2));

  // clone, encrypt with LUKS2 (different passphrase)
  ASSERT_EQ(0, rbd_snap_create(child2, "snap"));
  ASSERT_EQ(0, rbd_snap_protect(child2, "snap"));

  std::string child3_name = get_temp_image_name();
  ASSERT_EQ(0, rbd_clone3(ioctx, child2_name.c_str(), "snap", ioctx,
                          child3_name.c_str(), image_opts));

  rbd_image_t child3;
  ASSERT_EQ(0, rbd_open(ioctx, child3_name.c_str(), &child3, NULL));

  rbd_encryption_luks2_format_options_t child3_opts = {
          .alg = RBD_ENCRYPTION_ALGORITHM_AES256,
          .passphrase = "12345678",
          .passphrase_size = 8,
  };
  ASSERT_EQ(0, rbd_encryption_format(
          child3, RBD_ENCRYPTION_FORMAT_LUKS2, &child3_opts,
          sizeof(child3_opts)));
  ASSERT_EQ(-EPERM, rbd_encryption_load(
        child3, RBD_ENCRYPTION_FORMAT_LUKS2, &child3_opts,
        sizeof(child3_opts)));

  // verify child3 data
  rbd_encryption_spec_t specs[] = {
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child3_opts,
            .opts_size = sizeof(child3_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child2_opts,
            .opts_size = sizeof(child2_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS1,
            .opts = &child1_opts,
            .opts_size = sizeof(child1_opts)}
  };

  ASSERT_EQ(0, rbd_encryption_load2(child3, specs, 3));

  ASSERT_PASSED(read_test_data, child3, "aaaa", 0, 4, 0);
  ASSERT_PASSED(read_test_data, child3, "bbbb", 64 << 20, 4, 0);
  ASSERT_PASSED(read_test_data, child3, "cccc", 128 << 20, 4, 0);

  // clone without formatting
  ASSERT_EQ(0, rbd_snap_create(child3, "snap"));
  ASSERT_EQ(0, rbd_snap_protect(child3, "snap"));

  std::string child4_name = get_temp_image_name();
  ASSERT_EQ(0, rbd_clone3(ioctx, child3_name.c_str(), "snap", ioctx,
                          child4_name.c_str(), image_opts));

  rbd_image_t child4;
  ASSERT_EQ(0, rbd_open(ioctx, child4_name.c_str(), &child4, NULL));

  rbd_encryption_spec_t child4_specs[] = {
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child3_opts,
            .opts_size = sizeof(child3_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child3_opts,
            .opts_size = sizeof(child3_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS2,
            .opts = &child2_opts,
            .opts_size = sizeof(child2_opts)},
          { .format = RBD_ENCRYPTION_FORMAT_LUKS1,
            .opts = &child1_opts,
            .opts_size = sizeof(child1_opts)}
  };

  ASSERT_EQ(0, rbd_encryption_load2(child4, child4_specs, 4));

  // flatten child4
  ASSERT_EQ(0, rbd_flatten(child4));

  // reopen child4 and load encryption
  ASSERT_EQ(0, rbd_close(child4));
  ASSERT_EQ(0, rbd_open(ioctx, child4_name.c_str(), &child4, NULL));
  ASSERT_EQ(0, rbd_encryption_load(
          child4, RBD_ENCRYPTION_FORMAT_LUKS2, &child3_opts,
          sizeof(child3_opts)));

  // verify flattend image
  ASSERT_PASSED(read_test_data, child4, "aaaa", 0, 4, 0);
  ASSERT_PASSED(read_test_data, child4, "bbbb", 64 << 20, 4, 0);
  ASSERT_PASSED(read_test_data, child4, "cccc", 128 << 20, 4, 0);

  ASSERT_EQ(0, rbd_close(child4));
  ASSERT_EQ(0, rbd_remove(ioctx, child4_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(child3, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(child3, "snap"));
  ASSERT_EQ(0, rbd_close(child3));
  ASSERT_EQ(0, rbd_remove(ioctx, child3_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(child2, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(child2, "snap"));
  ASSERT_EQ(0, rbd_close(child2));
  ASSERT_EQ(0, rbd_remove(ioctx, child2_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(child1, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(child1, "snap"));
  ASSERT_EQ(0, rbd_close(child1));
  ASSERT_EQ(0, rbd_remove(ioctx, child1_name.c_str()));
  ASSERT_EQ(0, rbd_snap_unprotect(image, "snap"));
  ASSERT_EQ(0, rbd_snap_remove(image, "snap"));
  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, LUKS1UnderLUKS2WithoutResize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string parent_name = get_temp_image_name();
  std::string clone_name = get_temp_image_name();
  uint64_t data_size = 25 << 20;
  uint64_t luks1_meta_size = 4 << 20;
  uint64_t luks2_meta_size = 16 << 20;
  std::string parent_passphrase = "parent passphrase";
  std::string clone_passphrase = "clone passphrase";

  {
    int order = 22;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(),
                                 luks1_meta_size + data_size, &order));
    librbd::Image parent;
    ASSERT_EQ(0, rbd.open(ioctx, parent, parent_name.c_str(), nullptr));

    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, parent_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));

    ASSERT_EQ(0, parent.snap_create("snap"));
    ASSERT_EQ(0, parent.snap_protect("snap"));
    uint64_t features;
    ASSERT_EQ(0, parent.features(&features));
    ASSERT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "snap", ioctx,
                           clone_name.c_str(), features, &order));
  }

  {
    librbd::Image clone;
    ASSERT_EQ(0, rbd.open(ioctx, clone, clone_name.c_str(), nullptr));

    librbd::encryption_luks2_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, clone_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                         sizeof(fopts)));

    librbd::encryption_luks_format_options_t opts1 = {parent_passphrase};
    librbd::encryption_luks_format_options_t opts2 = {clone_passphrase};
    librbd::encryption_spec_t specs[] = {
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts2, sizeof(opts2)},
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts1, sizeof(opts1)}};
    ASSERT_EQ(0, clone.encryption_load2(specs, std::size(specs)));

    uint64_t size;
    ASSERT_EQ(0, clone.size(&size));
    EXPECT_EQ(data_size + luks1_meta_size - luks2_meta_size, size);
    uint64_t overlap;
    ASSERT_EQ(0, clone.overlap(&overlap));
    EXPECT_EQ(data_size + luks1_meta_size - luks2_meta_size, overlap);

    ceph::bufferlist expected_bl;
    expected_bl.append(std::string(
        data_size + luks1_meta_size - luks2_meta_size, 'a'));

    ceph::bufferlist read_bl;
    ASSERT_EQ(expected_bl.length(),
              clone.read(0, expected_bl.length(), read_bl));
    EXPECT_TRUE(expected_bl.contents_equal(read_bl));
  }
}

TEST_F(TestLibRBD, LUKS2UnderLUKS1WithoutResize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string parent_name = get_temp_image_name();
  std::string clone_name = get_temp_image_name();
  uint64_t data_size = 25 << 20;
  uint64_t luks1_meta_size = 4 << 20;
  uint64_t luks2_meta_size = 16 << 20;
  std::string parent_passphrase = "parent passphrase";
  std::string clone_passphrase = "clone passphrase";

  {
    int order = 22;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(),
                                 luks2_meta_size + data_size, &order));
    librbd::Image parent;
    ASSERT_EQ(0, rbd.open(ioctx, parent, parent_name.c_str(), nullptr));

    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, parent_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));

    ASSERT_EQ(0, parent.snap_create("snap"));
    ASSERT_EQ(0, parent.snap_protect("snap"));
    uint64_t features;
    ASSERT_EQ(0, parent.features(&features));
    ASSERT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "snap", ioctx,
                           clone_name.c_str(), features, &order));
  }

  {
    librbd::Image clone;
    ASSERT_EQ(0, rbd.open(ioctx, clone, clone_name.c_str(), nullptr));

    librbd::encryption_luks1_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, clone_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                         sizeof(fopts)));

    librbd::encryption_luks_format_options_t opts1 = {parent_passphrase};
    librbd::encryption_luks_format_options_t opts2 = {clone_passphrase};
    librbd::encryption_spec_t specs[] = {
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts2, sizeof(opts2)},
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts1, sizeof(opts1)}};
    ASSERT_EQ(0, clone.encryption_load2(specs, std::size(specs)));

    uint64_t size;
    ASSERT_EQ(0, clone.size(&size));
    EXPECT_EQ(data_size + luks2_meta_size - luks1_meta_size, size);
    uint64_t overlap;
    ASSERT_EQ(0, clone.overlap(&overlap));
    EXPECT_EQ(data_size, overlap);

    ceph::bufferlist expected_bl;
    expected_bl.append(std::string(data_size, 'a'));
    expected_bl.append_zero(luks2_meta_size - luks1_meta_size);

    ceph::bufferlist read_bl;
    ASSERT_EQ(expected_bl.length(),
              clone.read(0, expected_bl.length(), read_bl));
    EXPECT_TRUE(expected_bl.contents_equal(read_bl));
  }
}

TEST_F(TestLibRBD, EncryptionFormatNoData)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  auto name = get_temp_image_name();
  uint64_t luks1_meta_size = 4 << 20;
  std::string passphrase = "some passphrase";

  {
    int order = 0;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), luks1_meta_size - 1,
                                 &order));
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    librbd::encryption_luks1_format_options_t opts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
    ASSERT_EQ(-ENOSPC, image.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1,
                                               &opts, sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(luks1_meta_size - 1, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));
    ASSERT_EQ(0, image.resize(luks1_meta_size));

    librbd::encryption_luks1_format_options_t opts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
    ASSERT_EQ(0, image.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &opts,
                                         sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(0, size);
  }
}

TEST_F(TestLibRBD, EncryptionLoadBadSize)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  auto name = get_temp_image_name();
  uint64_t luks1_meta_size = 4 << 20;
  std::string passphrase = "some passphrase";

  {
    int order = 0;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), luks1_meta_size,
                                 &order));
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    librbd::encryption_luks1_format_options_t opts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
    ASSERT_EQ(0, image.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &opts,
                                         sizeof(opts)));
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(0, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                       sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(0, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));
    ASSERT_EQ(0, image.resize(luks1_meta_size - 1));

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(-EINVAL, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                             sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(luks1_meta_size - 1, size);
  }
}

TEST_F(TestLibRBD, EncryptionLoadBadStripePattern)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  bool old_format;
  uint64_t features;
  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

  librbd::RBD rbd;
  auto name1 = get_temp_image_name();
  auto name2 = get_temp_image_name();
  auto name3 = get_temp_image_name();
  std::string passphrase = "some passphrase";

  {
    int order = 22;
    ASSERT_EQ(0, rbd.create3(ioctx, name1.c_str(), 20 << 20, features, &order,
                             2 << 20, 2));
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name1.c_str(), nullptr));

    librbd::encryption_luks1_format_options_t opts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
    ASSERT_EQ(0, image.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &opts,
                                         sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(12 << 20, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name1.c_str(), nullptr));

    // different but compatible striping pattern
    librbd::ImageOptions image_opts;
    ASSERT_EQ(0, image_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, 1 << 20));
    ASSERT_EQ(0, image_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, 2));
    ASSERT_EQ(0, image.deep_copy(ioctx, name2.c_str(), image_opts));
  }
  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name2.c_str(), nullptr));

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(0, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                       sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(12 << 20, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name1.c_str(), nullptr));

    // incompatible striping pattern
    librbd::ImageOptions image_opts;
    ASSERT_EQ(0, image_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, 1 << 20));
    ASSERT_EQ(0, image_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, 3));
    ASSERT_EQ(0, image.deep_copy(ioctx, name3.c_str(), image_opts));
  }
  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name3.c_str(), nullptr));

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(-EINVAL, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                             sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(20 << 20, size);
  }
}

TEST_F(TestLibRBD, EncryptionLoadFormatMismatch)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name1 = get_temp_image_name();
  std::string name2 = get_temp_image_name();
  std::string name3 = get_temp_image_name();
  std::string passphrase = "some passphrase";

  librbd::encryption_luks1_format_options_t luks1_opts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  librbd::encryption_luks2_format_options_t luks2_opts = {
      RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
  librbd::encryption_luks_format_options_t luks_opts = {passphrase};

#define LUKS_ONE {RBD_ENCRYPTION_FORMAT_LUKS1, &luks1_opts, sizeof(luks1_opts)}
#define LUKS_TWO {RBD_ENCRYPTION_FORMAT_LUKS2, &luks2_opts, sizeof(luks2_opts)}
#define LUKS_ANY {RBD_ENCRYPTION_FORMAT_LUKS, &luks_opts, sizeof(luks_opts)}

  const std::vector<librbd::encryption_spec_t> bad_specs[] = {
      {},
      {LUKS_ONE},
      {LUKS_TWO},
      {LUKS_ONE, LUKS_ONE},
      {LUKS_ONE, LUKS_TWO},
      {LUKS_ONE, LUKS_ANY},
      {LUKS_TWO, LUKS_TWO},
      {LUKS_ANY, LUKS_TWO},
      {LUKS_ONE, LUKS_ONE, LUKS_ONE},
      {LUKS_ONE, LUKS_ONE, LUKS_TWO},
      {LUKS_ONE, LUKS_ONE, LUKS_ANY},
      {LUKS_ONE, LUKS_TWO, LUKS_ONE},
      {LUKS_ONE, LUKS_TWO, LUKS_TWO},
      {LUKS_ONE, LUKS_TWO, LUKS_ANY},
      {LUKS_ONE, LUKS_ANY, LUKS_ONE},
      {LUKS_ONE, LUKS_ANY, LUKS_TWO},
      {LUKS_ONE, LUKS_ANY, LUKS_ANY},
      {LUKS_TWO, LUKS_ONE, LUKS_TWO},
      {LUKS_TWO, LUKS_TWO, LUKS_ONE},
      {LUKS_TWO, LUKS_TWO, LUKS_TWO},
      {LUKS_TWO, LUKS_TWO, LUKS_ANY},
      {LUKS_TWO, LUKS_ANY, LUKS_TWO},
      {LUKS_ANY, LUKS_ONE, LUKS_TWO},
      {LUKS_ANY, LUKS_TWO, LUKS_ONE},
      {LUKS_ANY, LUKS_TWO, LUKS_TWO},
      {LUKS_ANY, LUKS_TWO, LUKS_ANY},
      {LUKS_ANY, LUKS_ANY, LUKS_TWO},
      {LUKS_ANY, LUKS_ANY, LUKS_ANY, LUKS_ANY}};

  const std::vector<librbd::encryption_spec_t> good_specs[] = {
      {LUKS_ANY},
      {LUKS_TWO, LUKS_ONE},
      {LUKS_TWO, LUKS_ANY},
      {LUKS_ANY, LUKS_ONE},
      {LUKS_ANY, LUKS_ANY},
      {LUKS_TWO, LUKS_ONE, LUKS_ONE},
      {LUKS_TWO, LUKS_ONE, LUKS_ANY},
      {LUKS_TWO, LUKS_ANY, LUKS_ONE},
      {LUKS_TWO, LUKS_ANY, LUKS_ANY},
      {LUKS_ANY, LUKS_ONE, LUKS_ONE},
      {LUKS_ANY, LUKS_ONE, LUKS_ANY},
      {LUKS_ANY, LUKS_ANY, LUKS_ONE},
      {LUKS_ANY, LUKS_ANY, LUKS_ANY}};

  static_assert(std::size(bad_specs) + std::size(good_specs) == 1 + 3 + 9 + 27 + 1);

#undef LUKS_ONE
#undef LUKS_TWO
#undef LUKS_ANY

  {
    int order = 0;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name1.c_str(), 20 << 20, &order));
    librbd::Image image1;
    ASSERT_EQ(0, rbd.open(ioctx, image1, name1.c_str(), nullptr));
    ASSERT_EQ(0, image1.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1,
                                          &luks1_opts, sizeof(luks1_opts)));

    ASSERT_EQ(0, image1.snap_create("snap"));
    ASSERT_EQ(0, image1.snap_protect("snap"));
    uint64_t features;
    ASSERT_EQ(0, image1.features(&features));
    ASSERT_EQ(0, rbd.clone(ioctx, name1.c_str(), "snap", ioctx, name2.c_str(),
                           features, &order));

    librbd::Image image2;
    ASSERT_EQ(0, rbd.open(ioctx, image2, name2.c_str(), nullptr));
    ASSERT_EQ(0, image2.snap_create("snap"));
    ASSERT_EQ(0, image2.snap_protect("snap"));
    ASSERT_EQ(0, rbd.clone(ioctx, name2.c_str(), "snap", ioctx, name3.c_str(),
                           features, &order));

    librbd::Image image3;
    ASSERT_EQ(0, rbd.open(ioctx, image3, name3.c_str(), nullptr));
    ASSERT_EQ(0, image3.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2,
                                          &luks2_opts, sizeof(luks2_opts)));
  }

  {
    librbd::Image image3;
    ASSERT_EQ(0, rbd.open(ioctx, image3, name3.c_str(), nullptr));
    for (auto& specs : bad_specs) {
      ASSERT_EQ(-EINVAL, image3.encryption_load2(specs.data(), specs.size()));
    }
  }

  for (auto& specs : good_specs) {
    librbd::Image image3;
    ASSERT_EQ(0, rbd.open(ioctx, image3, name3.c_str(), nullptr));
    ASSERT_EQ(0, image3.encryption_load2(specs.data(), specs.size()));
  }
}

TEST_F(TestLibRBD, EncryptedResize)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  auto name = get_temp_image_name();
  uint64_t luks2_meta_size = 16 << 20;
  uint64_t data_size = 10 << 20;
  std::string passphrase = "some passphrase";

  {
    int order = 0;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(),
                                 luks2_meta_size + data_size, &order));
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(luks2_meta_size + data_size, size);

    librbd::encryption_luks2_format_options_t opts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
    ASSERT_EQ(0, image.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &opts,
                                         sizeof(opts)));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(data_size, size);
    ASSERT_EQ(0, image.resize(data_size * 3));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(data_size * 3, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(luks2_meta_size + data_size * 3, size);

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(0, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                       sizeof(opts)));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(data_size * 3, size);
    ASSERT_EQ(0, image.resize(data_size / 2));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(data_size / 2, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(luks2_meta_size + data_size / 2, size);

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(0, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                       sizeof(opts)));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(data_size / 2, size);
    ASSERT_EQ(0, image.resize(0));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(0, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(luks2_meta_size, size);

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(0, image.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                       sizeof(opts)));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(0, size);
    ASSERT_EQ(0, image.resize(data_size));
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(data_size, size);
  }

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    ASSERT_EQ(luks2_meta_size + data_size, size);
  }
}

TEST_F(TestLibRBD, EncryptedFlattenSmallData)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string parent_name = get_temp_image_name();
  std::string clone_name = get_temp_image_name();
  uint64_t data_size = 5000;
  uint64_t luks2_meta_size = 16 << 20;
  std::string passphrase = "some passphrase";

  {
    int order = 22;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(),
                                 luks2_meta_size + data_size, &order));
    librbd::Image parent;
    ASSERT_EQ(0, rbd.open(ioctx, parent, parent_name.c_str(), nullptr));

    librbd::encryption_luks2_format_options_t opts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &opts,
                                          sizeof(opts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));

    ASSERT_EQ(0, parent.snap_create("snap"));
    ASSERT_EQ(0, parent.snap_protect("snap"));
    uint64_t features;
    ASSERT_EQ(0, parent.features(&features));
    ASSERT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "snap", ioctx,
                           clone_name.c_str(), features, &order));
  }

  {
    librbd::Image clone;
    ASSERT_EQ(0, rbd.open(ioctx, clone, clone_name.c_str(), nullptr));

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(0, clone.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                       sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, clone.size(&size));
    ASSERT_EQ(data_size, size);
    uint64_t overlap;
    ASSERT_EQ(0, clone.overlap(&overlap));
    ASSERT_EQ(data_size, overlap);

    ceph::bufferlist expected_bl;
    expected_bl.append(std::string(data_size, 'a'));

    ceph::bufferlist read_bl1;
    ASSERT_EQ(data_size, clone.read(0, data_size, read_bl1));
    ASSERT_TRUE(expected_bl.contents_equal(read_bl1));

    ASSERT_EQ(0, clone.flatten());

    ceph::bufferlist read_bl2;
    ASSERT_EQ(data_size, clone.read(0, data_size, read_bl2));
    ASSERT_TRUE(expected_bl.contents_equal(read_bl2));
  }

  {
    librbd::Image clone;
    ASSERT_EQ(0, rbd.open(ioctx, clone, clone_name.c_str(), nullptr));

    librbd::encryption_luks_format_options_t opts = {passphrase};
    ASSERT_EQ(0, clone.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                       sizeof(opts)));
    uint64_t size;
    ASSERT_EQ(0, clone.size(&size));
    ASSERT_EQ(data_size, size);
    uint64_t overlap;
    ASSERT_EQ(0, clone.overlap(&overlap));
    ASSERT_EQ(0, overlap);

    ceph::bufferlist expected_bl;
    expected_bl.append(std::string(data_size, 'a'));

    ceph::bufferlist read_bl;
    ASSERT_EQ(data_size, clone.read(0, data_size, read_bl));
    ASSERT_TRUE(expected_bl.contents_equal(read_bl));
  }
}

struct LUKSOnePassphrase {
  int load(librbd::Image& clone) {
    librbd::encryption_luks_format_options_t opts = {m_passphrase};
    return clone.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                 sizeof(opts));
  }

  int load_flattened(librbd::Image& clone) {
    return load(clone);
  }

  std::string m_passphrase = "some passphrase";
};

struct LUKSTwoPassphrases {
  int load(librbd::Image& clone) {
    librbd::encryption_luks_format_options_t opts1 = {m_parent_passphrase};
    librbd::encryption_luks_format_options_t opts2 = {m_clone_passphrase};
    librbd::encryption_spec_t specs[] = {
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts2, sizeof(opts2)},
        {RBD_ENCRYPTION_FORMAT_LUKS, &opts1, sizeof(opts1)}};
    return clone.encryption_load2(specs, std::size(specs));
  }

  int load_flattened(librbd::Image& clone) {
    librbd::encryption_luks_format_options_t opts = {m_clone_passphrase};
    return clone.encryption_load(RBD_ENCRYPTION_FORMAT_LUKS, &opts,
                                 sizeof(opts));
  }

  std::string m_parent_passphrase = "parent passphrase";
  std::string m_clone_passphrase = "clone passphrase";
};

struct PlaintextUnderLUKS1 : LUKSOnePassphrase {
protected:
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));

    // before taking a parent snapshot, (temporarily) add extra space
    // to the parent to account for upcoming LUKS1 header in the clone,
    // making the clone able to reach all parent data
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks1_meta_size));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    librbd::encryption_luks1_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                         sizeof(fopts)));
    *passed = true;
  }
};

struct PlaintextUnderLUKS2 : LUKSOnePassphrase {
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));

    // before taking a parent snapshot, (temporarily) add extra space
    // to the parent to account for upcoming LUKS2 header in the clone,
    // making the clone able to reach all parent data
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks2_meta_size));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    librbd::encryption_luks2_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                         sizeof(fopts)));
    *passed = true;
  }
};

struct UnformattedLUKS1 : LUKSOnePassphrase {
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks1_meta_size));
    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    *passed = true;
  }
};

struct LUKS1UnderLUKS1 : LUKSTwoPassphrases {
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks1_meta_size));
    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    librbd::encryption_luks1_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                         sizeof(fopts)));
    *passed = true;
  }
};

struct LUKS1UnderLUKS2 : LUKSTwoPassphrases {
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    uint64_t luks1_meta_size = 4 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks1_meta_size));
    librbd::encryption_luks1_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));

    // before taking a parent snapshot, (temporarily) add extra space
    // to the parent to account for upcoming LUKS2 header in the clone,
    // making the clone able to reach all parent data
    // space taken by LUKS1 header in the parent would be reused
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks2_meta_size - luks1_meta_size));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    librbd::encryption_luks2_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                         sizeof(fopts)));
    *passed = true;
  }
};

struct UnformattedLUKS2 : LUKSOnePassphrase {
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks2_meta_size));
    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    *passed = true;
  }
};

struct LUKS2UnderLUKS2 : LUKSTwoPassphrases {
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks2_meta_size));
    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    librbd::encryption_luks2_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                         sizeof(fopts)));
    *passed = true;
  }
};

struct LUKS2UnderLUKS1 : LUKSTwoPassphrases {
  void setup_parent(librbd::Image& parent, uint64_t data_size, bool* passed) {
    uint64_t luks2_meta_size = 16 << 20;
    ASSERT_EQ(0, parent.resize(data_size + luks2_meta_size));
    librbd::encryption_luks2_format_options_t fopts = {
        RBD_ENCRYPTION_ALGORITHM_AES256, m_parent_passphrase};
    ASSERT_EQ(0, parent.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS2, &fopts,
                                          sizeof(fopts)));

    ceph::bufferlist bl;
    bl.append(std::string(data_size, 'a'));
    ASSERT_EQ(data_size, parent.write(0, data_size, bl));
    *passed = true;
  }

  void setup_clone(librbd::Image& clone, uint64_t data_size, bool* passed) {
    librbd::encryption_luks1_format_options_t fopts =
        {RBD_ENCRYPTION_ALGORITHM_AES256, m_clone_passphrase};
    ASSERT_EQ(0, clone.encryption_format(RBD_ENCRYPTION_FORMAT_LUKS1, &fopts,
                                         sizeof(fopts)));

    // after loading encryption on the clone, one can get rid of
    // unneeded space allowance in the clone arising from LUKS2 header
    // in the parent being bigger than LUKS1 header in the clone
    ASSERT_EQ(0, load(clone));
    ASSERT_EQ(0, clone.resize(data_size));
    *passed = true;
  }
};

template <typename FormatPolicy>
class EncryptedFlattenTest : public TestLibRBD, FormatPolicy {
protected:
  void create_and_setup(bool* passed) {
    ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), m_ioctx));

    int order = 22;
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, m_parent_name.c_str(),
                                 m_data_size, &order));
    librbd::Image parent;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, parent, m_parent_name.c_str(), nullptr));
    ASSERT_PASSED(FormatPolicy::setup_parent, parent, m_data_size);

    ASSERT_EQ(0, parent.snap_create("snap"));
    ASSERT_EQ(0, parent.snap_protect("snap"));
    uint64_t features;
    ASSERT_EQ(0, parent.features(&features));
    ASSERT_EQ(0, m_rbd.clone(m_ioctx, m_parent_name.c_str(), "snap", m_ioctx,
                             m_clone_name.c_str(), features, &order));
    librbd::Image clone;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, clone, m_clone_name.c_str(), nullptr));
    ASSERT_PASSED(FormatPolicy::setup_clone, clone, m_data_size);

    *passed = true;
  }

  void open_and_load(librbd::Image& clone, bool* passed) {
    ASSERT_EQ(0, m_rbd.open(m_ioctx, clone, m_clone_name.c_str(), nullptr));
    ASSERT_EQ(0, FormatPolicy::load(clone));
    *passed = true;
  }

  void open_and_load_flattened(librbd::Image& clone, bool* passed) {
    ASSERT_EQ(0, m_rbd.open(m_ioctx, clone, m_clone_name.c_str(), nullptr));
    ASSERT_EQ(0, FormatPolicy::load_flattened(clone));
    *passed = true;
  }

  void verify_size_and_overlap(librbd::Image& image, uint64_t expected_size,
                               uint64_t expected_overlap) {
    uint64_t size;
    ASSERT_EQ(0, image.size(&size));
    EXPECT_EQ(expected_size, size);
    uint64_t overlap;
    ASSERT_EQ(0, image.overlap(&overlap));
    EXPECT_EQ(expected_overlap, overlap);
  }

  void verify_data(librbd::Image& image, const ceph::bufferlist& expected_bl) {
    ceph::bufferlist read_bl;
    ASSERT_EQ(expected_bl.length(),
              image.read(0, expected_bl.length(), read_bl));
    EXPECT_TRUE(expected_bl.contents_equal(read_bl));
  }

  librados::IoCtx m_ioctx;
  librbd::RBD m_rbd;
  std::string m_parent_name = get_temp_image_name();
  std::string m_clone_name = get_temp_image_name();
  uint64_t m_data_size = 25 << 20;
};

using EncryptedFlattenTestTypes =
    ::testing::Types<PlaintextUnderLUKS1, PlaintextUnderLUKS2,
                     UnformattedLUKS1, LUKS1UnderLUKS1, LUKS1UnderLUKS2,
                     UnformattedLUKS2, LUKS2UnderLUKS2, LUKS2UnderLUKS1>;
TYPED_TEST_SUITE(EncryptedFlattenTest, EncryptedFlattenTestTypes);

TYPED_TEST(EncryptedFlattenTest, Simple)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_PASSED0(this->create_and_setup);

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(this->m_data_size, 'a'));

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load, clone);
    this->verify_size_and_overlap(clone, this->m_data_size, this->m_data_size);
    this->verify_data(clone, expected_bl);
    ASSERT_EQ(0, clone.flatten());
    this->verify_data(clone, expected_bl);
  }

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load_flattened, clone);
    this->verify_size_and_overlap(clone, this->m_data_size, 0);
    this->verify_data(clone, expected_bl);
  }
}

TYPED_TEST(EncryptedFlattenTest, Grow)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_PASSED0(this->create_and_setup);

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(this->m_data_size, 'a'));
  expected_bl.append_zero(1);

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load, clone);
    ASSERT_EQ(0, clone.resize(this->m_data_size + 1));
    this->verify_size_and_overlap(clone, this->m_data_size + 1,
                                  this->m_data_size);
    this->verify_data(clone, expected_bl);
    ASSERT_EQ(0, clone.flatten());
    this->verify_data(clone, expected_bl);
  }

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load_flattened, clone);
    this->verify_size_and_overlap(clone, this->m_data_size + 1, 0);
    this->verify_data(clone, expected_bl);
  }
}

TYPED_TEST(EncryptedFlattenTest, Shrink)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_PASSED0(this->create_and_setup);

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(this->m_data_size - 1, 'a'));

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load, clone);
    ASSERT_EQ(0, clone.resize(this->m_data_size - 1));
    this->verify_size_and_overlap(clone, this->m_data_size - 1,
                                  this->m_data_size - 1);
    this->verify_data(clone, expected_bl);
    ASSERT_EQ(0, clone.flatten());
    this->verify_data(clone, expected_bl);
  }

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load_flattened, clone);
    this->verify_size_and_overlap(clone, this->m_data_size - 1, 0);
    this->verify_data(clone, expected_bl);
  }
}

TYPED_TEST(EncryptedFlattenTest, ShrinkToOne)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_PASSED0(this->create_and_setup);

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(1, 'a'));

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load, clone);
    ASSERT_EQ(0, clone.resize(1));
    this->verify_size_and_overlap(clone, 1, 1);
    this->verify_data(clone, expected_bl);
    ASSERT_EQ(0, clone.flatten());
    this->verify_data(clone, expected_bl);
  }

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load_flattened, clone);
    this->verify_size_and_overlap(clone, 1, 0);
    this->verify_data(clone, expected_bl);
  }
}

TYPED_TEST(EncryptedFlattenTest, ShrinkToOneAfterSnapshot)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_PASSED0(this->create_and_setup);

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(1, 'a'));

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load, clone);
    ASSERT_EQ(0, clone.snap_create("snap"));
    ASSERT_EQ(0, clone.resize(1));
    this->verify_size_and_overlap(clone, 1, 1);
    this->verify_data(clone, expected_bl);
    ASSERT_EQ(0, clone.flatten());
    this->verify_data(clone, expected_bl);
  }

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load_flattened, clone);
    this->verify_size_and_overlap(clone, 1, 0);
    this->verify_data(clone, expected_bl);
  }
}

TYPED_TEST(EncryptedFlattenTest, MinOverlap)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_PASSED0(this->create_and_setup);

  ceph::bufferlist expected_bl;
  expected_bl.append(std::string(1, 'a'));
  expected_bl.append_zero(this->m_data_size - 1);

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load, clone);
    ASSERT_EQ(0, clone.resize(1));
    ASSERT_EQ(0, clone.resize(this->m_data_size));
    this->verify_size_and_overlap(clone, this->m_data_size, 1);
    this->verify_data(clone, expected_bl);
    ASSERT_EQ(0, clone.flatten());
    this->verify_data(clone, expected_bl);
  }

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load_flattened, clone);
    this->verify_size_and_overlap(clone, this->m_data_size, 0);
    this->verify_data(clone, expected_bl);
  }
}

TYPED_TEST(EncryptedFlattenTest, ZeroOverlap)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));
  REQUIRE(!is_feature_enabled(RBD_FEATURE_JOURNALING));

  ASSERT_PASSED0(this->create_and_setup);

  ceph::bufferlist expected_bl;
  expected_bl.append_zero(this->m_data_size);

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load, clone);
    ASSERT_EQ(0, clone.resize(0));
    ASSERT_EQ(0, clone.resize(this->m_data_size));
    this->verify_size_and_overlap(clone, this->m_data_size, 0);
    this->verify_data(clone, expected_bl);
    ASSERT_EQ(0, clone.flatten());
    this->verify_data(clone, expected_bl);
  }

  {
    librbd::Image clone;
    ASSERT_PASSED(this->open_and_load_flattened, clone);
    this->verify_size_and_overlap(clone, this->m_data_size, 0);
    this->verify_data(clone, expected_bl);
  }
}

#endif

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

  bool skip_discard = is_skip_partial_discard_enabled(image);

  char test_data[TEST_IO_SIZE + 1];
  char zero_data[TEST_IO_SIZE + 1];
  char mismatch_data[TEST_IO_SIZE + 1];
  int i;
  uint64_t mismatch_offset;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';
  memset(zero_data, 0, sizeof(zero_data));
  memset(mismatch_data, 9, sizeof(mismatch_data));

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(write_test_data, image, test_data, TEST_IO_SIZE * i,
		  TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_write_test_data, image, test_data, TEST_IO_SIZE * i,
		  TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(compare_and_write_test_data, image, test_data, test_data,
      TEST_IO_SIZE * i, TEST_IO_SIZE, &mismatch_offset, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_compare_and_write_test_data, image, test_data, test_data,
      TEST_IO_SIZE * i, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);

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
  ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
		TEST_IO_SIZE, TEST_IO_SIZE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
  ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
  ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
		TEST_IO_SIZE*3, TEST_IO_SIZE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
  ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE, 0);

  for (i = 0; i < 15; ++i) {
    if (i % 3 == 2) {
      ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
    } else if (i % 3 == 1) {
      ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
    } else {
      ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
    }
  }
  for (i = 0; i < 15; ++i) {
    if (i % 3 == 2) {
      ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
      ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
    } else if (i % 3 == 1) {
      ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
      ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
    } else {
      ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
      ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32,
                    TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
    }
  }

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

  ASSERT_PASSED(write_test_data, image, zero_data, 0, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
  mismatch_offset = 123;
  ASSERT_EQ(-EILSEQ, rbd_compare_and_write(image, 0, TEST_IO_SIZE, mismatch_data, mismatch_data,
                                           &mismatch_offset, LIBRADOS_OP_FLAG_FADVISE_DONTNEED));
  ASSERT_EQ(0U, mismatch_offset);
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  mismatch_offset = 123;
  ASSERT_EQ(0, rbd_aio_compare_and_write(image, 0, TEST_IO_SIZE, mismatch_data, mismatch_data,
                                         comp, &mismatch_offset, LIBRADOS_OP_FLAG_FADVISE_DONTNEED));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EILSEQ, rbd_aio_get_return_value(comp));
  ASSERT_EQ(0U, mismatch_offset);
  rbd_aio_release(comp);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestDataPoolIO)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  std::string data_pool_name = create_pool(true);

  rbd_image_t image;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  bool old_format;
  uint64_t features;
  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

  rbd_image_options_t image_options;
  rbd_image_options_create(&image_options);
  BOOST_SCOPE_EXIT( (&image_options) ) {
    rbd_image_options_destroy(image_options);
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, rbd_image_options_set_uint64(image_options,
                                            RBD_IMAGE_OPTION_FEATURES,
                                            features));
  ASSERT_EQ(0, rbd_image_options_set_string(image_options,
                                            RBD_IMAGE_OPTION_DATA_POOL,
                                            data_pool_name.c_str()));

  ASSERT_EQ(0, rbd_create4(ioctx, name.c_str(), size, image_options));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_NE(-1, rbd_get_data_pool_id(image));

  bool skip_discard = is_skip_partial_discard_enabled(image);

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
  ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
		TEST_IO_SIZE, TEST_IO_SIZE, 0);
  ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE, 0);
  ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
		TEST_IO_SIZE*3, TEST_IO_SIZE, 0);
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

TEST_F(TestLibRBD, TestCompareAndWriteMismatch)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */
  off_t off = 512;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // We only support to compare and write the same amount of (len) bytes
  std::string cmp_buffer("This is a test");
  std::string write_buffer("Write this !!!");
  std::string mismatch_buffer("This will fail");
  std::string read_buffer(cmp_buffer.length(), '1');

  ssize_t written = rbd_write(image, off, cmp_buffer.length(),
                              cmp_buffer.data());
  ASSERT_EQ(cmp_buffer.length(), written);

  // Compare should fail because of mismatch
  uint64_t mismatch_off = 0;
  written = rbd_compare_and_write(image, off, write_buffer.length(),
                                  mismatch_buffer.data(), write_buffer.data(),
                                  &mismatch_off, 0);
  ASSERT_EQ(-EILSEQ, written);
  ASSERT_EQ(5U, mismatch_off);

  // check nothing was written
  ssize_t read = rbd_read(image, off, read_buffer.length(), read_buffer.data());
  ASSERT_EQ(read_buffer.length(), read);
  ASSERT_EQ(cmp_buffer, read_buffer);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteMismatch)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */
  off_t off = 512;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // We only support to compare and write the same amount of (len) bytes
  std::string cmp_buffer("This is a test");
  std::string write_buffer("Write this !!!");
  std::string mismatch_buffer("This will fail");
  std::string read_buffer(cmp_buffer.length(), '1');

  ssize_t written = rbd_write(image, off, cmp_buffer.length(),
                              cmp_buffer.data());
  ASSERT_EQ(cmp_buffer.length(), written);

  // Compare should fail because of mismatch
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_write(image, off, write_buffer.length(),
                                      mismatch_buffer.data(),
                                      write_buffer.data(), comp,
                                      &mismatch_off, 0);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EILSEQ, rbd_aio_get_return_value(comp));
  ASSERT_EQ(5U, mismatch_off);
  rbd_aio_release(comp);

  // check nothing was written
  ssize_t read = rbd_read(image, off, read_buffer.length(), read_buffer.data());
  ASSERT_EQ(read_buffer.length(), read);
  ASSERT_EQ(cmp_buffer, read_buffer);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestCompareAndWriteSuccess)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */
  off_t off = 512;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // We only support to compare and write the same amount of (len) bytes
  std::string cmp_buffer("This is a test");
  std::string write_buffer("Write this !!!");
  std::string read_buffer(cmp_buffer.length(), '1');

  ssize_t written = rbd_write(image, off, cmp_buffer.length(),
                              cmp_buffer.data());
  ASSERT_EQ(cmp_buffer.length(), written);

  /*
   * we compare against the written buffer (cmp_buffer) and write the buffer
   * We expect: len bytes written
   */
  uint64_t mismatch_off = 0;
  written = rbd_compare_and_write(image, off, write_buffer.length(),
                                  cmp_buffer.data(), write_buffer.data(),
                                  &mismatch_off, 0);
  ASSERT_EQ(write_buffer.length(), written);
  ASSERT_EQ(0U, mismatch_off);

  ssize_t read = rbd_read(image, off, read_buffer.length(), read_buffer.data());
  ASSERT_EQ(read_buffer.length(), read);
  ASSERT_EQ(write_buffer, read_buffer);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteSuccess)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */
  off_t off = 512;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // We only support to compare and write the same amount of (len) bytes
  std::string cmp_buffer("This is a test");
  std::string write_buffer("Write this !!!");
  std::string read_buffer(cmp_buffer.length(), '1');

  ssize_t written = rbd_write(image, off, cmp_buffer.length(),
                              cmp_buffer.data());
  ASSERT_EQ(cmp_buffer.length(), written);

  /*
   * we compare against the written buffer (cmp_buffer) and write the buffer
   * We expect: len bytes written
   */
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_write(image, off, write_buffer.length(),
                                      cmp_buffer.data(), write_buffer.data(),
                                      comp, &mismatch_off, 0);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rbd_aio_get_return_value(comp));
  ASSERT_EQ(0U, mismatch_off);
  rbd_aio_release(comp);

  ssize_t read = rbd_read(image, off, read_buffer.length(), read_buffer.data());
  ASSERT_EQ(read_buffer.length(), read);
  ASSERT_EQ(write_buffer, read_buffer);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}


TEST_F(TestLibRBD, TestCompareAndWriteStripeUnitUnaligned)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // large write test => we allow stripe unit size writes (aligned)
  uint64_t stripe_unit;
  rbd_get_stripe_unit(image, &stripe_unit);
  std::string large_write_buffer(stripe_unit, '2');
  std::string large_cmp_buffer(stripe_unit * 2, '4');

  ssize_t written = rbd_write(image, stripe_unit, large_cmp_buffer.length(),
                              large_cmp_buffer.data());
  ASSERT_EQ(large_cmp_buffer.length(), written);

  /*
    * compare and write at offset stripe_unit + 1 and stripe unit size
    * Expect fail because access exceeds stripe (unaligned)
    */
  uint64_t mismatch_off = 0;
  written = rbd_compare_and_write(image, stripe_unit + 1, stripe_unit,
                                  large_cmp_buffer.data(),
                                  large_write_buffer.data(),
                                  &mismatch_off, 0);
  ASSERT_EQ(-EINVAL, written);
  ASSERT_EQ(0U, mismatch_off);

  // check nothing has been written
  std::string large_read_buffer(large_cmp_buffer.length(), '5');
  ssize_t read = rbd_read(image, stripe_unit, large_read_buffer.length(),
                          large_read_buffer.data());
  ASSERT_EQ(large_read_buffer.length(), read);
  auto buffer_mismatch = std::mismatch(large_cmp_buffer.begin(),
                                       large_cmp_buffer.end(),
                                       large_read_buffer.begin());
  ASSERT_EQ(large_read_buffer.end(), buffer_mismatch.second);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteStripeUnitUnaligned)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // large write test => we allow stripe unit size writes (aligned)
  uint64_t stripe_unit;
  rbd_get_stripe_unit(image, &stripe_unit);
  std::string large_write_buffer(stripe_unit, '2');
  std::string large_cmp_buffer(stripe_unit * 2, '4');

  ssize_t written = rbd_write(image, stripe_unit, large_cmp_buffer.length(),
                              large_cmp_buffer.data());
  ASSERT_EQ(large_cmp_buffer.length(), written);

  /*
    * compare and write at offset stripe_unit + 1 and stripe unit size
    * Expect fail because access spans stripe unit boundary (unaligned)
    */
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_write(image, stripe_unit + 1, stripe_unit,
                                      large_cmp_buffer.data(),
                                      large_write_buffer.data(),
                                      comp, &mismatch_off, 0);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EINVAL, rbd_aio_get_return_value(comp));
  ASSERT_EQ(0U, mismatch_off);
  rbd_aio_release(comp);

  // check nothing has been written
  std::string large_read_buffer(large_cmp_buffer.length(), '5');
  ssize_t read = rbd_read(image, stripe_unit, large_read_buffer.length(),
                          large_read_buffer.data());
  ASSERT_EQ(large_read_buffer.length(), read);
  auto buffer_mismatch = std::mismatch(large_cmp_buffer.begin(),
                                       large_cmp_buffer.end(),
                                       large_read_buffer.begin());
  ASSERT_EQ(large_read_buffer.end(), buffer_mismatch.second);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestCompareAndWriteTooLarge)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // large write test => we allow stripe unit size writes (aligned)
  uint64_t stripe_unit;
  rbd_get_stripe_unit(image, &stripe_unit);
  std::string large_write_buffer(stripe_unit * 2, '2');
  std::string large_cmp_buffer(large_write_buffer.length(), '4');

  ssize_t written = rbd_write(image, stripe_unit, large_cmp_buffer.length(),
                              large_cmp_buffer.data());
  ASSERT_EQ(large_cmp_buffer.length(), written);

  /*
    * compare and write at offset stripe_unit and stripe unit size + 1
    * Expect fail because access is larger than stripe unit size
    */
  uint64_t mismatch_off = 0;
  written = rbd_compare_and_write(image, stripe_unit, stripe_unit + 1,
                                  large_cmp_buffer.data(),
                                  large_write_buffer.data(),
                                  &mismatch_off, 0);
  ASSERT_EQ(-EINVAL, written);
  ASSERT_EQ(0U, mismatch_off);

  // check nothing has been written
  std::string large_read_buffer(large_cmp_buffer.length(), '5');
  ssize_t read = rbd_read(image, stripe_unit, large_read_buffer.length(),
                          large_read_buffer.data());
  ASSERT_EQ(large_read_buffer.length(), read);
  auto buffer_mismatch = std::mismatch(large_cmp_buffer.begin(),
                                       large_cmp_buffer.end(),
                                       large_read_buffer.begin());
  ASSERT_EQ(large_read_buffer.end(), buffer_mismatch.second);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteTooLarge)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // large write test => we allow stripe unit size writes (aligned)
  uint64_t stripe_unit;
  rbd_get_stripe_unit(image, &stripe_unit);
  std::string large_write_buffer(stripe_unit * 2, '2');
  std::string large_cmp_buffer(large_write_buffer.length(), '4');

  ssize_t written = rbd_write(image, stripe_unit, large_cmp_buffer.length(),
                              large_cmp_buffer.data());
  ASSERT_EQ(large_cmp_buffer.length(), written);

  /*
    * compare and write at offset stripe_unit and stripe unit size + 1
    * Expect fail because access is larger than stripe unit size
    */
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_write(image, stripe_unit, stripe_unit + 1,
                                      large_cmp_buffer.data(),
                                      large_write_buffer.data(),
                                      comp, &mismatch_off, 0);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EINVAL, rbd_aio_get_return_value(comp));
  ASSERT_EQ(0U, mismatch_off);
  rbd_aio_release(comp);

  // check nothing has been written
  std::string large_read_buffer(large_cmp_buffer.length(), '5');
  ssize_t read = rbd_read(image, stripe_unit, large_read_buffer.length(),
                          large_read_buffer.data());
  ASSERT_EQ(large_read_buffer.length(), read);
  auto buffer_mismatch = std::mismatch(large_cmp_buffer.begin(),
                                       large_cmp_buffer.end(),
                                       large_read_buffer.begin());
  ASSERT_EQ(large_read_buffer.end(), buffer_mismatch.second);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestCompareAndWriteStripeUnitSuccess)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // large write test => we allow stripe unit size writes (aligned)
  uint64_t stripe_unit;
  rbd_get_stripe_unit(image, &stripe_unit);
  std::string large_write_buffer(stripe_unit, '2');
  std::string large_cmp_buffer(stripe_unit * 2, '4');

  ssize_t written = rbd_write(image, stripe_unit, large_cmp_buffer.length(),
                              large_cmp_buffer.data());
  ASSERT_EQ(large_cmp_buffer.length(), written);

  // aligned stripe unit size access => expect success
  uint64_t mismatch_off = 0;
  written = rbd_compare_and_write(image, stripe_unit, stripe_unit,
                                  large_cmp_buffer.data(),
                                  large_write_buffer.data(),
                                  &mismatch_off, 0);
  ASSERT_EQ(stripe_unit, written);
  ASSERT_EQ(0U, mismatch_off);

  // check stripe_unit bytes of large_write_buffer were written
  std::string large_read_buffer(large_cmp_buffer.length(), '5');
  ssize_t read = rbd_read(image, stripe_unit, large_read_buffer.length(),
                          large_read_buffer.data());
  ASSERT_EQ(large_read_buffer.length(), read);
  auto buffer_mismatch = std::mismatch(large_read_buffer.begin(),
                                       large_read_buffer.begin() + stripe_unit,
                                       large_write_buffer.begin());
  ASSERT_EQ(large_write_buffer.end(), buffer_mismatch.second);
  // check data beyond stripe_unit size was not overwritten
  buffer_mismatch = std::mismatch(large_read_buffer.begin() + stripe_unit,
                                  large_read_buffer.end(),
                                  large_cmp_buffer.begin());
  ASSERT_EQ(large_cmp_buffer.begin() + stripe_unit, buffer_mismatch.second);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteStripeUnitSuccess)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  // large write test => we allow stripe unit size writes (aligned)
  uint64_t stripe_unit;
  rbd_get_stripe_unit(image, &stripe_unit);
  std::string large_write_buffer(stripe_unit, '2');
  std::string large_cmp_buffer(stripe_unit * 2, '4');

  ssize_t written = rbd_write(image, stripe_unit, large_cmp_buffer.length(),
                              large_cmp_buffer.data());
  ASSERT_EQ(large_cmp_buffer.length(), written);

  // aligned stripe unit size access => expect success
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_write(image, stripe_unit, stripe_unit,
                                      large_cmp_buffer.data(),
                                      large_write_buffer.data(),
                                      comp, &mismatch_off, 0);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rbd_aio_get_return_value(comp));
  ASSERT_EQ(0U, mismatch_off);
  rbd_aio_release(comp);

  // check stripe_unit bytes of large_write_buffer were written
  std::string large_read_buffer(large_cmp_buffer.length(), '5');
  ssize_t read = rbd_read(image, stripe_unit, large_read_buffer.length(),
                          large_read_buffer.data());
  ASSERT_EQ(large_read_buffer.length(), read);
  auto buffer_mismatch = std::mismatch(large_read_buffer.begin(),
                                       large_read_buffer.begin() + stripe_unit,
                                       large_write_buffer.begin());
  ASSERT_EQ(large_write_buffer.end(), buffer_mismatch.second);
  // check data beyond stripe_unit size was not overwritten
  buffer_mismatch = std::mismatch(large_read_buffer.begin() + stripe_unit,
                                  large_read_buffer.end(),
                                  large_cmp_buffer.begin());
  ASSERT_EQ(large_cmp_buffer.begin() + stripe_unit, buffer_mismatch.second);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteVIovecLenDiffers)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */
  off_t off = 512;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  std::string cmp_buffer("This is a test");
  size_t cmp_len = cmp_buffer.length();

  std::string write_buffer("Write this !!!");
  struct iovec write_iovs[] = {
    {.iov_base = &write_buffer[0], .iov_len = 6},
    {.iov_base = &write_buffer[6], .iov_len = 5},
    {.iov_base = &write_buffer[11], .iov_len = 3}
  };

  ASSERT_EQ(cmp_len, rbd_write(image, off, cmp_len, cmp_buffer.data()));

  // should fail because compare iovec len cannot be different to write iovec len
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_writev(image, off,
                                       write_iovs /* cmp_iovs */, 1,
                                       write_iovs, std::size(write_iovs),
                                       comp, &mismatch_off, 0);
  ASSERT_EQ(-EINVAL, ret);
  ASSERT_EQ(0U, mismatch_off);
  rbd_aio_release(comp);

  // check nothing was written
  std::string read_buffer(cmp_buffer.length(), '1');
  ssize_t read = rbd_read(image, off, read_buffer.length(), read_buffer.data());
  ASSERT_EQ(read_buffer.length(), read);
  ASSERT_EQ(cmp_buffer, read_buffer);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteVMismatch)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */
  off_t off = 512;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  std::string cmp_buffer("This is a test");
  int cmp_len = cmp_buffer.length();

  std::string write_buffer("Write this !!!");
  struct iovec write_iovs[] = {
    {.iov_base = &write_buffer[0], .iov_len = 6},
    {.iov_base = &write_buffer[6], .iov_len = 5},
    {.iov_base = &write_buffer[11], .iov_len = 3}
  };

  std::string mismatch_buffer("This will fail");
  struct iovec mismatch_iovs[] = {
    {.iov_base = &mismatch_buffer[0], .iov_len = 5},
    {.iov_base = &mismatch_buffer[5], .iov_len = 5},
    {.iov_base = &mismatch_buffer[10], .iov_len = 4}
  };

  ASSERT_EQ(cmp_len, rbd_write(image, off, cmp_len, cmp_buffer.data()));

  // this should execute the compare but fail because of mismatch
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_writev(image, off,
                                       mismatch_iovs /* cmp_iovs */,
                                       std::size(mismatch_iovs),
                                       write_iovs, std::size(write_iovs),
                                       comp, &mismatch_off, 0);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(-EILSEQ, rbd_aio_get_return_value(comp));
  ASSERT_EQ(5U, mismatch_off);
  rbd_aio_release(comp);

  // check nothing was written
  std::string read_buffer(cmp_buffer.length(), '1');
  ssize_t read = rbd_read(image, off, read_buffer.length(), read_buffer.data());
  ASSERT_EQ(read_buffer.length(), read);
  ASSERT_EQ(cmp_buffer, read_buffer);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestAioCompareAndWriteVSuccess)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20; /* 20MiB */
  off_t off = 512;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  std::string cmp_buffer("This is a test");
  struct iovec cmp_iovs[] = {
    {.iov_base = &cmp_buffer[0], .iov_len = 5},
    {.iov_base = &cmp_buffer[5], .iov_len = 3},
    {.iov_base = &cmp_buffer[8], .iov_len = 2},
    {.iov_base = &cmp_buffer[10], .iov_len = 4}
  };
  size_t cmp_len = cmp_buffer.length();

  std::string write_buffer("Write this !!!");
  struct iovec write_iovs[] = {
    {.iov_base = &write_buffer[0], .iov_len = 6},
    {.iov_base = &write_buffer[6], .iov_len = 5},
    {.iov_base = &write_buffer[11], .iov_len = 3}
  };

  ASSERT_EQ(cmp_len, rbd_write(image, off, cmp_len, cmp_buffer.data()));

  // compare against the buffer written before => should succeed
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  uint64_t mismatch_off = 0;
  int ret = rbd_aio_compare_and_writev(image, off,
                                       cmp_iovs, std::size(cmp_iovs),
                                       write_iovs, std::size(write_iovs),
                                       comp, &mismatch_off, 0);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rbd_aio_get_return_value(comp));
  ASSERT_EQ(0U, mismatch_off);
  rbd_aio_release(comp);

  // check data was successfully written
  std::string read_buffer(cmp_buffer.length(), '1');
  ssize_t read = rbd_read(image, off, read_buffer.length(), read_buffer.data());
  ASSERT_EQ(read_buffer.length(), read);
  ASSERT_EQ(write_buffer, read_buffer);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestScatterGatherIO)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  std::string write_buffer("This is a test");
  // These iovecs should produce a length overflow
  struct iovec bad_iovs[] = {
    {.iov_base = &write_buffer[0], .iov_len = 5},
    {.iov_base = NULL, .iov_len = std::numeric_limits<size_t>::max()}
  };
  struct iovec write_iovs[] = {
    {.iov_base = &write_buffer[0],  .iov_len = 5},
    {.iov_base = &write_buffer[5],  .iov_len = 3},
    {.iov_base = &write_buffer[8],  .iov_len = 2},
    {.iov_base = &write_buffer[10], .iov_len = 4}
  };

  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, NULL, &comp);
  ASSERT_EQ(-EINVAL, rbd_aio_writev(image, write_iovs, 0, 0, comp));
  ASSERT_EQ(-EINVAL, rbd_aio_writev(image, bad_iovs, 2, 0, comp));
  ASSERT_EQ(0, rbd_aio_writev(image, write_iovs,
                              sizeof(write_iovs) / sizeof(struct iovec),
                              1<<order, comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rbd_aio_get_return_value(comp));
  rbd_aio_release(comp);

  std::string read_buffer(write_buffer.size(), '1');
  struct iovec read_iovs[] = {
    {.iov_base = &read_buffer[0],  .iov_len = 4},
    {.iov_base = &read_buffer[8],  .iov_len = 4},
    {.iov_base = &read_buffer[12], .iov_len = 2}
  };

  rbd_aio_create_completion(NULL, NULL, &comp);
  ASSERT_EQ(-EINVAL, rbd_aio_readv(image, read_iovs, 0, 0, comp));
  ASSERT_EQ(-EINVAL, rbd_aio_readv(image, bad_iovs, 2, 0, comp));
  ASSERT_EQ(0, rbd_aio_readv(image, read_iovs,
                             sizeof(read_iovs) / sizeof(struct iovec),
                             1<<order, comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(10, rbd_aio_get_return_value(comp));
  rbd_aio_release(comp);
  ASSERT_EQ("This1111 is a ", read_buffer);

  std::string linear_buffer(write_buffer.size(), '1');
  struct iovec linear_iovs[] = {
    {.iov_base = &linear_buffer[4], .iov_len = 4}
  };
  rbd_aio_create_completion(NULL, NULL, &comp);
  ASSERT_EQ(0, rbd_aio_readv(image, linear_iovs,
                             sizeof(linear_iovs) / sizeof(struct iovec),
                             1<<order, comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(comp));
  ASSERT_EQ(4, rbd_aio_get_return_value(comp));
  rbd_aio_release(comp);
  ASSERT_EQ("1111This111111", linear_buffer);

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
  ASSERT_PASSED(aio_discard_test_data, image, 3*1024*1024, 1*1024*1024);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestFUA)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image_write;
  rbd_image_t image_read;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image_write, NULL));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image_read, NULL));

  // enable writeback cache
  rbd_flush(image_write);

  char test_data[TEST_IO_SIZE + 1];
  int i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';
  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(write_test_data, image_write, test_data,
                  TEST_IO_SIZE * i, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_FUA);

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(read_test_data, image_read, test_data,
                  TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_write_test_data, image_write, test_data,
                  TEST_IO_SIZE * i, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_FUA);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_read_test_data, image_read, test_data,
                  TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  ASSERT_PASSED(validate_object_map, image_write);
  ASSERT_PASSED(validate_object_map, image_read);
  ASSERT_EQ(0, rbd_close(image_write));
  ASSERT_EQ(0, rbd_close(image_read));
  ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
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
  ASSERT_EQ(0, r);
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
  ASSERT_EQ(0, r);
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
  int read;
  size_t len = expected_len;
  ceph::bufferlist bl;
  if (iohint)
    read = image.read2(off, len, bl, iohint);
  else
    read = image.read(off, len, bl);
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

void aio_writesame_test_data(librbd::Image& image, const char *test_data, off_t off,
                             size_t len, size_t data_len, uint32_t iohint, bool *passed)
{
  ceph::bufferlist bl;
  bl.append(test_data, data_len);
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  printf("created completion\n");
  int r;
  r = image.aio_writesame(off, len, bl, comp, iohint);
  printf("started writesame\n");
  if (len % data_len) {
    ASSERT_EQ(-EINVAL, r);
    printf("expected fail, finished writesame\n");
    comp->release();
    *passed = true;
    return;
  }

  comp->wait_for_complete();
  r = comp->get_return_value();
  printf("return value is: %d\n", r);
  ASSERT_EQ(0, r);
  printf("finished writesame\n");
  comp->release();

  //verify data
  printf("to verify the data\n");
  int read;
  uint64_t left = len;
  while (left > 0) {
    ceph::bufferlist bl;
    read = image.read(off, data_len, bl);
    ASSERT_EQ(data_len, static_cast<size_t>(read));
    std::string bl_str(bl.c_str(), read);
    int result = memcmp(bl_str.c_str(), test_data, data_len);
    if (result !=0 ) {
      printf("read: %u ~ %u\n", (unsigned int) off, (unsigned int) read);
      printf("read: %s\nexpected: %s\n", bl_str.c_str(), test_data);
      ASSERT_EQ(0, result);
    }
    off += data_len;
    left -= data_len;
  }
  ASSERT_EQ(0U, left);
  printf("verified\n");

  *passed = true;
}

void writesame_test_data(librbd::Image& image, const char *test_data, off_t off,
                         ssize_t len, size_t data_len, uint32_t iohint,
                         bool *passed)
{
  ssize_t written;
  ceph::bufferlist bl;
  bl.append(test_data, data_len);
  written = image.writesame(off, len, bl, iohint);
  if (len % data_len) {
    ASSERT_EQ(-EINVAL, written);
    printf("expected fail, finished writesame\n");
    *passed = true;
    return;
  }
  ASSERT_EQ(len, written);
  printf("wrote: %u\n", (unsigned int) written);
  *passed = true;

  //verify data
  printf("to verify the data\n");
  int read;
  uint64_t left = len;
  while (left > 0) {
    ceph::bufferlist bl;
    read = image.read(off, data_len, bl);
    ASSERT_EQ(data_len, static_cast<size_t>(read));
    std::string bl_str(bl.c_str(), read);
    int result = memcmp(bl_str.c_str(), test_data, data_len);
    if (result !=0 ) {
      printf("read: %u ~ %u\n", (unsigned int) off, (unsigned int) read);
      printf("read: %s\nexpected: %s\n", bl_str.c_str(), test_data);
      ASSERT_EQ(0, result);
    }
    off += data_len;
    left -= data_len;
  }
  ASSERT_EQ(0U, left);
  printf("verified\n");

  *passed = true;
}

void aio_compare_and_write_test_data(librbd::Image& image, const char *cmp_data,
                                     const char *test_data, off_t off, ssize_t len,
                                     uint32_t iohint, bool *passed)
{
  ceph::bufferlist cmp_bl;
  cmp_bl.append(cmp_data, strlen(cmp_data));
  ceph::bufferlist test_bl;
  test_bl.append(test_data, strlen(test_data));
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  printf("created completion\n");

  uint64_t mismatch_offset;
  image.aio_compare_and_write(off, len, cmp_bl, test_bl, comp, &mismatch_offset, iohint);
  printf("started aio compare and write\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  ASSERT_EQ(0, r);
  printf("finished aio compare and write\n");
  comp->release();
  *passed = true;
}

void compare_and_write_test_data(librbd::Image& image, const char *cmp_data, const char *test_data,
                                 off_t off, ssize_t len, uint64_t *mismatch_off, uint32_t iohint, bool *passed)
{
  size_t written;
  ceph::bufferlist cmp_bl;
  cmp_bl.append(cmp_data, strlen(cmp_data));
  ceph::bufferlist test_bl;
  test_bl.append(test_data, strlen(test_data));
  printf("start compare and write\n");
  written = image.compare_and_write(off, len, cmp_bl, test_bl, mismatch_off, iohint);
  printf("compare and  wrote: %d\n", (int) written);
  ASSERT_EQ(len, static_cast<ssize_t>(written));
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

    bool skip_discard = this->is_skip_partial_discard_enabled(image);

    char test_data[TEST_IO_SIZE + 1];
    char zero_data[TEST_IO_SIZE + 1];
    int i;
    uint64_t mismatch_offset;

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
      ASSERT_PASSED(compare_and_write_test_data, image, test_data, test_data, TEST_IO_SIZE * i,
        TEST_IO_SIZE, &mismatch_offset, 0);

    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_compare_and_write_test_data, image, test_data, test_data, TEST_IO_SIZE * i,
        TEST_IO_SIZE, 0);

    for (i = 0; i < 5; ++i)
      ASSERT_PASSED(read_test_data, image, test_data, strlen(test_data) * i, TEST_IO_SIZE, 0);

    for (i = 5; i < 10; ++i)
      ASSERT_PASSED(aio_read_test_data, image, test_data, strlen(test_data) * i, TEST_IO_SIZE, 0);

    // discard 2nd, 4th sections.
    ASSERT_PASSED(discard_test_data, image, TEST_IO_SIZE, TEST_IO_SIZE);
    ASSERT_PASSED(aio_discard_test_data, image, TEST_IO_SIZE*3, TEST_IO_SIZE);

    ASSERT_PASSED(read_test_data, image, test_data,  0, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
		  TEST_IO_SIZE, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, skip_discard ? test_data : zero_data,
		  TEST_IO_SIZE*3, TEST_IO_SIZE, 0);
    ASSERT_PASSED(read_test_data, image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE, 0);

    for (i = 0; i < 15; ++i) {
      if (i % 3 == 2) {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, 0);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, 0);
      } else if (i % 3 == 1) {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      } else {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      }
    }
    for (i = 0; i < 15; ++i) {
      if (i % 3 == 2) {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, 0);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, 0);
      } else if (i % 3 == 1) {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE + i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      } else {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE * i, TEST_IO_SIZE * i * 32, TEST_IO_SIZE, 0);
      }
    }

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

static void compare_written(librbd::Image& image, off_t off, size_t len,
                            const std::string& buffer, bool *passed)
{
  bufferlist read_bl;
  ssize_t read = image.read(off, len, read_bl);
  ASSERT_EQ(len, read);
  std::string read_buffer(read_bl.c_str(), read);
  ASSERT_EQ(buffer.substr(0, len), read_buffer);
  *passed = true;
}

TEST_F(TestLibRBD, TestCompareAndWriteCompareTooSmallPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    std::string small_buffer("Too small");
    ceph::bufferlist small_bl;
    small_bl.append(&small_buffer[0], 4);
    small_bl.append(&small_buffer[4], 4);

    // should fail because compare bufferlist cannot be smaller than len
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(off, cmp_bl.length(),
                                      small_bl, /* cmp_bl */
                                      write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(-EINVAL, written);
    ASSERT_EQ(0U, mismatch_off);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteCompareTooSmallPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    std::string small_buffer("Too small");
    ceph::bufferlist small_bl;
    small_bl.append(&small_buffer[0], 4);
    small_bl.append(&small_buffer[4], 4);

    // should fail because compare bufferlist cannot be smaller than len
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(off, cmp_bl.length(),
                                          small_bl, /* cmp_bl */
                                          write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(-EINVAL, ret);
    ASSERT_EQ(0U, mismatch_off);
    comp->release();

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteWriteTooSmallPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    std::string small_buffer("Too small");
    ceph::bufferlist small_bl;
    small_bl.append(&small_buffer[0], 4);
    small_bl.append(&small_buffer[4], 4);

    // should fail because write bufferlist cannot be smaller than len
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(off, cmp_bl.length(),
                                      cmp_bl,
                                      small_bl, /* write_bl */
                                      &mismatch_off, 0);
    ASSERT_EQ(-EINVAL, written);
    ASSERT_EQ(0U, mismatch_off);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteWriteTooSmallPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    std::string small_buffer("Too small");
    ceph::bufferlist small_bl;
    small_bl.append(&small_buffer[0], 4);
    small_bl.append(&small_buffer[4], 4);

    // should fail because write bufferlist cannot be smaller than len
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(off, cmp_bl.length(),
                                          cmp_bl,
                                          small_bl, /* write_bl */
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(-EINVAL, ret);
    ASSERT_EQ(0U, mismatch_off);
    comp->release();

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteMismatchPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    std::string mismatch_buffer("This will fail");
    ceph::bufferlist mismatch_bl;
    mismatch_bl.append(&mismatch_buffer[0], 5);
    mismatch_bl.append(&mismatch_buffer[5], 5);
    mismatch_bl.append(&mismatch_buffer[10], 4);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    // this should execute the compare but fail because of mismatch
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(off, write_bl.length(),
                                      mismatch_bl, /* cmp_bl */
                                      write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(-EILSEQ, written);
    ASSERT_EQ(5U, mismatch_off);

    // check that nothing was written
    ASSERT_PASSED(compare_written, image, off, cmp_bl.length(), cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteMismatchPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    std::string mismatch_buffer("This will fail");
    ceph::bufferlist mismatch_bl;
    mismatch_bl.append(&mismatch_buffer[0], 5);
    mismatch_bl.append(&mismatch_buffer[5], 5);
    mismatch_bl.append(&mismatch_buffer[10], 4);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    // this should execute the compare but fail because of mismatch
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(off, write_bl.length(),
                                          mismatch_bl, /* cmp_bl */
                                          write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(0, ret);
    comp->wait_for_complete();
    ssize_t aio_ret = comp->get_return_value();
    ASSERT_EQ(-EILSEQ, aio_ret);
    ASSERT_EQ(5U, mismatch_off);
    comp->release();

    // check that nothing was written
    ASSERT_PASSED(compare_written, image, off, cmp_bl.length(), cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteMismatchBufferlistGreaterLenPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    std::string mismatch_buffer("This will fail");
    ceph::bufferlist mismatch_bl;
    mismatch_bl.append(&mismatch_buffer[0], 5);
    mismatch_bl.append(&mismatch_buffer[5], 5);
    mismatch_bl.append(&mismatch_buffer[10], 4);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    /*
     * we allow cmp_bl and write_bl to be greater than len so this
     * should execute the compare but fail because of mismatch
     */
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(off, cmp_bl.length() - 1,
                                      mismatch_bl, /* cmp_bl */
                                      write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(-EILSEQ, written);
    ASSERT_EQ(5U, mismatch_off);

    // check that nothing was written
    ASSERT_PASSED(compare_written, image, off, cmp_bl.length(), cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteMismatchBufferlistGreaterLenPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    std::string mismatch_buffer("This will fail");
    ceph::bufferlist mismatch_bl;
    mismatch_bl.append(&mismatch_buffer[0], 5);
    mismatch_bl.append(&mismatch_buffer[5], 5);
    mismatch_bl.append(&mismatch_buffer[10], 4);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    /*
     * we allow cmp_bl and write_bl to be greater than len so this
     * should execute the compare but fail because of mismatch
     */
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(off, cmp_bl.length() - 1,
                                          mismatch_bl, /* cmp_bl */
                                          write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(0, ret);
    comp->wait_for_complete();
    ssize_t aio_ret = comp->get_return_value();
    ASSERT_EQ(-EILSEQ, aio_ret);
    ASSERT_EQ(5U, mismatch_off);
    comp->release();

    // check that nothing was written
    ASSERT_PASSED(compare_written, image, off, cmp_bl.length(), cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteSuccessPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    // compare against the buffer written before => should succeed
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(off, cmp_bl.length(),
                                      cmp_bl,
                                      write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(write_bl.length(), written);
    ASSERT_EQ(0U, mismatch_off);

    // check write_bl was written
    ASSERT_PASSED(compare_written, image, off, write_bl.length(), write_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteSuccessPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    ssize_t written = image.write(off, cmp_bl.length(), cmp_bl);
    ASSERT_EQ(cmp_bl.length(), written);

    // compare against the buffer written before => should succeed
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(off, write_bl.length(),
                                          cmp_bl,
                                          write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(0, ret);
    comp->wait_for_complete();
    ssize_t aio_ret = comp->get_return_value();
    ASSERT_EQ(0, aio_ret);
    ASSERT_EQ(0U, mismatch_off);
    comp->release();

    // check write_bl was written
    ASSERT_PASSED(compare_written, image, off, write_bl.length(), write_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteSuccessBufferlistGreaterLenPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    std::string mismatch_buffer("This will fail");
    ceph::bufferlist mismatch_bl;
    mismatch_bl.append(&mismatch_buffer[0], 5);
    mismatch_bl.append(&mismatch_buffer[5], 5);
    mismatch_bl.append(&mismatch_buffer[10], 4);

    /*
     * Test len < cmp_bl & write_bl => should succeed but only compare
     * len bytes resp. only write len bytes
     */
    ssize_t written = image.write(off, mismatch_bl.length(), mismatch_bl);
    ASSERT_EQ(mismatch_bl.length(), written);

    size_t len_m1 = cmp_bl.length() - 1;
    written = image.write(off, len_m1, cmp_bl);
    ASSERT_EQ(len_m1, written);
    // the content of the image at off should now be "This is a tesl"

    uint64_t mismatch_off = 0;
    written = image.compare_and_write(off, len_m1,
                                      cmp_bl,
                                      write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(len_m1, written);
    ASSERT_EQ(0U, mismatch_off);

    // check that only write_bl.length() - 1 bytes were written
    ASSERT_PASSED(compare_written, image, off, len_m1, write_buffer);
    ASSERT_PASSED(compare_written, image, off + len_m1, 1, std::string("l"));

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteSuccessBufferlistGreaterLenPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */
    off_t off = 512;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string cmp_buffer("This is a test");
    ceph::bufferlist cmp_bl;
    cmp_bl.append(&cmp_buffer[0], 5);
    cmp_bl.append(&cmp_buffer[5], 3);
    cmp_bl.append(&cmp_buffer[8], 2);
    cmp_bl.append(&cmp_buffer[10], 4);

    std::string write_buffer("Write this !!!");
    ceph::bufferlist write_bl;
    write_bl.append(&write_buffer[0], 6);
    write_bl.append(&write_buffer[6], 5);
    write_bl.append(&write_buffer[11], 3);

    std::string mismatch_buffer("This will fail");
    ceph::bufferlist mismatch_bl;
    mismatch_bl.append(&mismatch_buffer[0], 5);
    mismatch_bl.append(&mismatch_buffer[5], 5);
    mismatch_bl.append(&mismatch_buffer[10], 4);

    /*
     * Test len < cmp_bl & write_bl => should succeed but only compare
     * len bytes resp. only write len bytes
     */
    ssize_t written = image.write(off, mismatch_bl.length(), mismatch_bl);
    ASSERT_EQ(mismatch_bl.length(), written);

    size_t len_m1 = cmp_bl.length() - 1;
    written = image.write(off, len_m1, cmp_bl);
    ASSERT_EQ(len_m1, written);
    // the content of the image at off should now be "This is a tesl"

    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(off, len_m1,
                                          cmp_bl,
                                          write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(0, ret);
    comp->wait_for_complete();
    ssize_t aio_ret = comp->get_return_value();
    ASSERT_EQ(0, aio_ret);
    ASSERT_EQ(0U, mismatch_off);
    comp->release();

    // check that only write_bl.length() - 1 bytes were written
    ASSERT_PASSED(compare_written, image, off, len_m1, write_buffer);
    ASSERT_PASSED(compare_written, image, off + len_m1, 1, std::string("l"));

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteStripeUnitUnalignedPP)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    // large write test => we allow stripe unit size writes (aligned)
    uint64_t stripe_unit = image.get_stripe_unit();
    std::string large_write_buffer(stripe_unit, '2');
    ceph::bufferlist large_write_bl;
    large_write_bl.append(large_write_buffer.data(),
                          large_write_buffer.length());

    std::string large_cmp_buffer(stripe_unit * 2, '3');
    ceph::bufferlist large_cmp_bl;
    large_cmp_bl.append(large_cmp_buffer.data(), large_cmp_buffer.length());

    ssize_t written = image.write(stripe_unit, large_cmp_bl.length(),
                                  large_cmp_bl);
    ASSERT_EQ(large_cmp_bl.length(), written);

    /*
     * compare and write at offset stripe_unit + 1 and stripe unit size
     * Expect fail because access exceeds stripe
     */
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(stripe_unit + 1, stripe_unit,
                                      large_cmp_bl,
                                      large_write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(-EINVAL, written);
    ASSERT_EQ(0U, mismatch_off);

    // check nothing has been written
    ASSERT_PASSED(compare_written, image, stripe_unit, large_cmp_bl.length(),
                  large_cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteStripeUnitUnalignedPP)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    // large write test => we allow stripe unit size writes (aligned)
    uint64_t stripe_unit = image.get_stripe_unit();
    std::string large_write_buffer(stripe_unit, '2');
    ceph::bufferlist large_write_bl;
    large_write_bl.append(large_write_buffer.data(),
                          large_write_buffer.length());

    std::string large_cmp_buffer(stripe_unit * 2, '3');
    ceph::bufferlist large_cmp_bl;
    large_cmp_bl.append(large_cmp_buffer.data(), large_cmp_buffer.length());

    ssize_t written = image.write(stripe_unit, large_cmp_bl.length(),
                                  large_cmp_bl);
    ASSERT_EQ(large_cmp_bl.length(), written);

    /*
     * compare and write at offset stripe_unit + 1 and stripe unit size
     * Expect fail because access exceeds stripe
     */
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(stripe_unit + 1, stripe_unit,
                                          large_cmp_bl,
                                          large_write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(0, ret);
    comp->wait_for_complete();
    ssize_t aio_ret = comp->get_return_value();
    ASSERT_EQ(-EINVAL, aio_ret);
    ASSERT_EQ(0U, mismatch_off);
    comp->release();

    // check nothing has been written
    ASSERT_PASSED(compare_written, image, stripe_unit, large_cmp_bl.length(),
                  large_cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteTooLargePP)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    // large write test => we allow stripe unit size writes (aligned)
    uint64_t stripe_unit = image.get_stripe_unit();
    std::string large_write_buffer(stripe_unit * 2, '2');
    ceph::bufferlist large_write_bl;
    large_write_bl.append(large_write_buffer.data(),
                          large_write_buffer.length());

    std::string large_cmp_buffer(stripe_unit * 2, '3');
    ceph::bufferlist large_cmp_bl;
    large_cmp_bl.append(large_cmp_buffer.data(), large_cmp_buffer.length());

    ssize_t written = image.write(stripe_unit, large_cmp_bl.length(),
                                  large_cmp_bl);
    ASSERT_EQ(large_cmp_bl.length(), written);

    /*
     * compare and write at offset stripe_unit and stripe unit size + 1
     * Expect fail because access is larger than stripe unit size
     */
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(stripe_unit, stripe_unit + 1,
                                      large_cmp_bl,
                                      large_write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(-EINVAL, written);
    ASSERT_EQ(0U, mismatch_off);

    // check nothing has been written
    ASSERT_PASSED(compare_written, image, stripe_unit, large_cmp_bl.length(),
                  large_cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteTooLargePP)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    // large write test => we allow stripe unit size writes (aligned)
    uint64_t stripe_unit = image.get_stripe_unit();
    std::string large_write_buffer(stripe_unit * 2, '2');
    ceph::bufferlist large_write_bl;
    large_write_bl.append(large_write_buffer.data(),
                          large_write_buffer.length());

    std::string large_cmp_buffer(stripe_unit * 2, '3');
    ceph::bufferlist large_cmp_bl;
    large_cmp_bl.append(large_cmp_buffer.data(), large_cmp_buffer.length());

    ssize_t written = image.write(stripe_unit, large_cmp_bl.length(),
                                  large_cmp_bl);
    ASSERT_EQ(large_cmp_bl.length(), written);

    /*
     * compare and write at offset stripe_unit and stripe unit size + 1
     * Expect fail because access is larger than stripe unit size
     */
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(stripe_unit, stripe_unit + 1,
                                          large_cmp_bl,
                                          large_write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(0, ret);
    comp->wait_for_complete();
    ssize_t aio_ret = comp->get_return_value();
    ASSERT_EQ(-EINVAL, aio_ret);
    ASSERT_EQ(0U, mismatch_off);
    comp->release();

    // check nothing has been written
    ASSERT_PASSED(compare_written, image, stripe_unit, large_cmp_bl.length(),
                  large_cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestCompareAndWriteStripeUnitSuccessPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    // large write test => we allow stripe unit size writes (aligned)
    uint64_t stripe_unit = image.get_stripe_unit();
    std::string large_write_buffer(stripe_unit * 2, '2');
    ceph::bufferlist large_write_bl;
    large_write_bl.append(large_write_buffer.data(),
                          large_write_buffer.length());

    std::string large_cmp_buffer(stripe_unit * 2, '3');
    ceph::bufferlist large_cmp_bl;
    large_cmp_bl.append(large_cmp_buffer.data(), large_cmp_buffer.length());

    ssize_t written = image.write(stripe_unit, large_cmp_bl.length(),
                                  large_cmp_bl);
    ASSERT_EQ(large_cmp_bl.length(), written);

    // aligned stripe unit size access => expect success
    uint64_t mismatch_off = 0;
    written = image.compare_and_write(stripe_unit, stripe_unit,
                                      large_cmp_bl,
                                      large_write_bl,
                                      &mismatch_off, 0);
    ASSERT_EQ(stripe_unit, written);
    ASSERT_EQ(0U, mismatch_off);

    // check large_write_bl was written and nothing beyond
    ASSERT_PASSED(compare_written, image, stripe_unit, stripe_unit,
                  large_write_buffer);
    ASSERT_PASSED(compare_written, image, stripe_unit * 2, stripe_unit,
                  large_cmp_buffer);

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, TestAioCompareAndWriteStripeUnitSuccessPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = get_temp_image_name();
    uint64_t size = 20 << 20; /* 20MiB */

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    // large write test => we allow stripe unit size writes (aligned)
    uint64_t stripe_unit = image.get_stripe_unit();
    std::string large_write_buffer(stripe_unit * 2, '2');
    ceph::bufferlist large_write_bl;
    large_write_bl.append(large_write_buffer.data(),
                          large_write_buffer.length());

    std::string large_cmp_buffer(stripe_unit * 2, '3');
    ceph::bufferlist large_cmp_bl;
    large_cmp_bl.append(large_cmp_buffer.data(),
                        large_cmp_buffer.length());

    ssize_t written = image.write(stripe_unit, large_cmp_bl.length(),
                                  large_cmp_bl);
    ASSERT_EQ(large_cmp_bl.length(), written);

    // aligned stripe unit size access => expect success
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(
        NULL, (librbd::callback_t) simple_write_cb_pp);
    uint64_t mismatch_off = 0;
    int ret = image.aio_compare_and_write(stripe_unit, stripe_unit,
                                          large_cmp_bl,
                                          large_write_bl,
                                          comp, &mismatch_off, 0);
    ASSERT_EQ(0, ret);
    comp->wait_for_complete();
    ssize_t aio_ret = comp->get_return_value();
    ASSERT_EQ(0, aio_ret);
    ASSERT_EQ(0U, mismatch_off);
    comp->release();

    // check large_write_bl was written and nothing beyond
    ASSERT_PASSED(compare_written, image, stripe_unit, stripe_unit,
                  large_write_buffer);
    ASSERT_PASSED(compare_written, image, stripe_unit * 2, stripe_unit,
                  large_cmp_buffer);

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
    char zero_data[TEST_IO_SIZE + 1];
    test_data[TEST_IO_SIZE] = '\0';
    int i;

    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    memset(zero_data, 0, sizeof(zero_data));

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

    for (i = 0; i < 15; ++i) {
      if (i % 3 == 2) {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      } else if (i % 3 == 1) {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE + i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE + i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      } else {
        ASSERT_PASSED(writesame_test_data, image, test_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
        ASSERT_PASSED(writesame_test_data, image, zero_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      }
    }
    for (i = 0; i < 15; ++i) {
      if (i % 3 == 2) {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32 + i, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
      } else if (i % 3 == 1) {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE + i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE + i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
      } else {
        ASSERT_PASSED(aio_writesame_test_data, image, test_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
        ASSERT_PASSED(aio_writesame_test_data, image, zero_data, TEST_IO_SIZE * i,
                      TEST_IO_SIZE * i * 32, TEST_IO_SIZE, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
      }
    }

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
  cout << strerror(-r) << std::endl;

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
  cout << strerror(-r) << std::endl;
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

TEST_F(TestLibRBD, TestSnapshotDeletedIo)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t isize = 2 << 20;

  int r;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), isize, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(0, rbd_snap_create(image, "orig"));

  r = rbd_snap_set(image, "orig");
  ASSERT_EQ(r, 0);

  ASSERT_EQ(0, rbd_snap_remove(image, "orig"));
  char test[20];
  ASSERT_EQ(-ENOENT, rbd_read(image, 20, 20, test));

  r = rbd_snap_set(image, NULL);
  ASSERT_EQ(r, 0);

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestClone)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  ASSERT_EQ(0, rados_conf_set(_cluster, "rbd_default_clone_format", "1"));
  BOOST_SCOPE_EXIT_ALL(&) {
    ASSERT_EQ(0, rados_conf_set(_cluster, "rbd_default_clone_format", "auto"));
  };

  rados_ioctx_t ioctx;
  rbd_image_info_t pinfo, cinfo;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  bool old_format;
  uint64_t features;
  rbd_image_t parent, child;
  int order = 0;

  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

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
  EXPECT_NE(0, clone_image(ioctx, parent, parent_name.c_str(), NULL, ioctx,
                           child_name.c_str(), features, &order));

  // verify that there is no parent info on "parent"
  ASSERT_EQ(-ENOENT, rbd_get_parent_info(parent, NULL, 0, NULL, 0, NULL, 0));
  printf("parent has no parent info\n");

  // create 70 metadatas to verify we can clone all key/value pairs
  std::string key;
  std::string val;
  size_t sum_key_len = 0;
  size_t sum_value_len = 0;
  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_set(parent, key.c_str(), val.c_str()));

    sum_key_len += (key.size() + 1);
    sum_value_len += (val.size() + 1);
  }

  char keys[1024];
  char vals[1024];
  size_t keys_len = sizeof(keys);
  size_t vals_len = sizeof(vals);

  char value[1024];
  size_t value_len = sizeof(value);

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  printf("made snapshot \"parent@parent_snap\"\n");
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, "parent_snap"));

  ASSERT_EQ(-EINVAL, clone_image(ioctx, parent, parent_name.c_str(), "parent_snap",
                                 ioctx, child_name.c_str(), features, &order));

  // unprotected image should fail unprotect
  ASSERT_EQ(-EINVAL, rbd_snap_unprotect(parent, "parent_snap"));
  printf("can't unprotect an unprotected snap\n");

  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));
  // protecting again should fail
  ASSERT_EQ(-EBUSY, rbd_snap_protect(parent, "parent_snap"));
  printf("can't protect a protected snap\n");

  // This clone and open should work
  ASSERT_EQ(0, clone_image(ioctx, parent, parent_name.c_str(), "parent_snap",
                           ioctx, child_name.c_str(), features, &order));
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

  // check key/value pairs in child image
  ASSERT_EQ(0, rbd_metadata_list(child, "key", 70, keys, &keys_len, vals,
                                &vals_len));
  ASSERT_EQ(sum_key_len, keys_len);
  ASSERT_EQ(sum_value_len, vals_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(child, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }
  printf("child image successfully cloned all image-meta pairs\n");

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
  printf("parent info: size %llu obj_size %llu parent_pool %llu\n",
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
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  ASSERT_EQ(0, rados_conf_set(_cluster, "rbd_default_clone_format", "2"));
  BOOST_SCOPE_EXIT_ALL(&) {
    ASSERT_EQ(0, rados_conf_set(_cluster, "rbd_default_clone_format", "auto"));
  };

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  bool old_format;
  uint64_t features;
  rbd_image_t parent, child;
  int order = 0;

  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

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
  EXPECT_NE(0, clone_image(ioctx, parent, parent_name.c_str(), NULL, ioctx,
                           child_name.c_str(), features, &order));

  // verify that there is no parent info on "parent"
  ASSERT_EQ(-ENOENT, rbd_get_parent_info(parent, NULL, 0, NULL, 0, NULL, 0));
  printf("parent has no parent info\n");

  // create 70 metadatas to verify we can clone all key/value pairs
  std::string key;
  std::string val;
  size_t sum_key_len = 0;
  size_t sum_value_len = 0;
  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_set(parent, key.c_str(), val.c_str()));

    sum_key_len += (key.size() + 1);
    sum_value_len += (val.size() + 1);
  }

  char keys[1024];
  char vals[1024];
  size_t keys_len = sizeof(keys);
  size_t vals_len = sizeof(vals);

  char value[1024];
  size_t value_len = sizeof(value);

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  printf("made snapshot \"parent@parent_snap\"\n");
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, "parent_snap"));

  // This clone and open should work
  ASSERT_EQ(0, clone_image(ioctx, parent, parent_name.c_str(), "parent_snap",
                           ioctx, child_name.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx, child_name.c_str(), &child, NULL));
  printf("made and opened clone \"child\"\n");

  // check key/value pairs in child image
  ASSERT_EQ(0, rbd_metadata_list(child, "key", 70, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(sum_key_len, keys_len);
  ASSERT_EQ(sum_value_len, vals_len);

  for (int i = 1; i <= 70; i++) {
    key = "key" + stringify(i);
    val = "value" + stringify(i);
    ASSERT_EQ(0, rbd_metadata_get(child, key.c_str(), value, &value_len));
    ASSERT_STREQ(val.c_str(), value);

    value_len = sizeof(value);
  }
  printf("child image successfully cloned all image-meta pairs\n");

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

  rbd_snap_info_t snaps[2];
  int max_snaps = 2;
  ASSERT_EQ(1, rbd_snap_list(parent, snaps, &max_snaps));
  rbd_snap_list_end(snaps);

  ASSERT_EQ(0, rbd_snap_remove_by_id(parent, snaps[0].id));

  rbd_snap_namespace_type_t snap_namespace_type;
  ASSERT_EQ(0, rbd_snap_get_namespace_type(parent, snaps[0].id,
                                           &snap_namespace_type));
  ASSERT_EQ(RBD_SNAP_NAMESPACE_TYPE_TRASH, snap_namespace_type);

  char original_name[32];
  ASSERT_EQ(0, rbd_snap_get_trash_namespace(parent, snaps[0].id,
                                            original_name,
                                            sizeof(original_name)));
  ASSERT_EQ(0, strcmp("parent_snap", original_name));

  ASSERT_EQ(0, rbd_close(child));
  ASSERT_EQ(0, rbd_close(parent));
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

static void test_list_children2(rbd_image_t image, int num_expected, ...)
{
  int num_children, i, j, max_size = 10;
  va_list ap;
  rbd_child_info_t children[max_size];
  num_children = rbd_list_children2(image, children, &max_size);
  printf("num children is: %d\nexpected: %d\n", num_children, num_expected);

  for (i = 0; i < num_children; i++) {
    printf("child: %s\n", children[i].image_name);
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected_id = va_arg(ap, char *);
    char *expected_pool = va_arg(ap, char *);
    char *expected_image = va_arg(ap, char *);
    bool expected_trash = va_arg(ap, int);
    bool found = false;
    for (j = 0; j < num_children; j++) {
      if (children[j].pool_name == NULL ||
          children[j].image_name == NULL ||
          children[j].image_id == NULL)
        continue;
      if (strcmp(children[j].image_id, expected_id) == 0 &&
          strcmp(children[j].pool_name, expected_pool) == 0 &&
          strcmp(children[j].image_name, expected_image) == 0 &&
          children[j].trash == expected_trash) {
        printf("found child %s/%s/%s\n\n", children[j].pool_name, children[j].image_name, children[j].image_id);
        rbd_list_child_cleanup(&children[j]);
        children[j].pool_name = NULL;
        children[j].image_name = NULL;
        children[j].image_id = NULL;
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found);
  }
  va_end(ap);

  for (i = 0; i < num_children; i++) {
    EXPECT_EQ((const char *)0, children[i].pool_name);
    EXPECT_EQ((const char *)0, children[i].image_name);
    EXPECT_EQ((const char *)0, children[i].image_id);
  }
}

TEST_F(TestLibRBD, ListChildren)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::RBD rbd;
  rados_ioctx_t ioctx1, ioctx2;
  string pool_name1 = create_pool(true);
  string pool_name2 = create_pool(true);
  ASSERT_NE("", pool_name2);

  rados_ioctx_create(_cluster, pool_name1.c_str(), &ioctx1);
  rados_ioctx_create(_cluster, pool_name2.c_str(), &ioctx2);

  rbd_image_t image1;
  rbd_image_t image2;
  rbd_image_t image3;
  rbd_image_t image4;

  bool old_format;
  uint64_t features;
  rbd_image_t parent;
  int order = 0;

  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

  std::string parent_name = get_temp_image_name();
  std::string child_name1 = get_temp_image_name();
  std::string child_name2 = get_temp_image_name();
  std::string child_name3 = get_temp_image_name();
  std::string child_name4 = get_temp_image_name();

  char child_id1[4096];
  char child_id2[4096];
  char child_id3[4096];
  char child_id4[4096];

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

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx2, child_name1.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx2, child_name1.c_str(), &image1, NULL));
  ASSERT_EQ(0, rbd_get_id(image1, child_id1, sizeof(child_id1)));
  test_list_children(parent, 1, pool_name2.c_str(), child_name1.c_str());
  test_list_children2(parent, 1,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false);

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx1, child_name2.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx1, child_name2.c_str(), &image2, NULL));
  ASSERT_EQ(0, rbd_get_id(image2, child_id2, sizeof(child_id2)));
  test_list_children(parent, 2, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str());
  test_list_children2(parent, 2,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false);

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx2, child_name3.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx2, child_name3.c_str(), &image3, NULL));
  ASSERT_EQ(0, rbd_get_id(image3, child_id3, sizeof(child_id3)));
  test_list_children(parent, 3, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str());
  test_list_children2(parent, 3,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), false);

  librados::IoCtx ioctx3;
  ASSERT_EQ(0, _rados.ioctx_create(pool_name2.c_str(), ioctx3));
  ASSERT_EQ(0, rbd_close(image3));
  ASSERT_EQ(0, rbd.trash_move(ioctx3, child_name3.c_str(), 0));
  test_list_children(parent, 2, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str());
  test_list_children2(parent, 3,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), true);

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx2, child_name4.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx2, child_name4.c_str(), &image4, NULL));
  ASSERT_EQ(0, rbd_get_id(image4, child_id4, sizeof(child_id4)));
  test_list_children(parent, 3, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 4,
                     child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                     child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                     child_id3, pool_name2.c_str(), child_name3.c_str(), true,
                     child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd.trash_restore(ioctx3, child_id3, ""));
  test_list_children(parent, 4, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 4,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), false,
                      child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd_close(image1));
  ASSERT_EQ(0, rbd_remove(ioctx2, child_name1.c_str()));
  test_list_children(parent, 3,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 3,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), false,
                      child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name3.c_str()));
  test_list_children(parent, 2,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 2,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd_close(image4));
  ASSERT_EQ(0, rbd_remove(ioctx2, child_name4.c_str()));
  test_list_children(parent, 1,
		     pool_name1.c_str(), child_name2.c_str());
  test_list_children2(parent, 1,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false);
  

  ASSERT_EQ(0, rbd_close(image2));
  ASSERT_EQ(0, rbd_remove(ioctx1, child_name2.c_str()));
  test_list_children(parent, 0);
  test_list_children2(parent, 0);

  ASSERT_EQ(0, rbd_snap_unprotect(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_snap_remove(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_remove(ioctx1, parent_name.c_str()));
  rados_ioctx_destroy(ioctx1);
  rados_ioctx_destroy(ioctx2);
}

TEST_F(TestLibRBD, ListChildrenTiered)
{
  SKIP_IF_CRIMSON();
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::RBD rbd;
  string pool_name1 = create_pool(true);
  string pool_name2 = create_pool(true);
  string pool_name3 = create_pool(true);
  ASSERT_NE("", pool_name1);
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

  char child_id1[4096];
  char child_id2[4096];
  char child_id3[4096];
  char child_id4[4096];

  rbd_image_t image1;
  rbd_image_t image2;
  rbd_image_t image3;
  rbd_image_t image4;

  rados_ioctx_t ioctx1, ioctx2;
  rados_ioctx_create(_cluster, pool_name1.c_str(), &ioctx1);
  rados_ioctx_create(_cluster, pool_name2.c_str(), &ioctx2);

  bool old_format;
  uint64_t features;
  rbd_image_t parent;
  int order = 0;

  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

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

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx2, child_name1.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx2, child_name1.c_str(), &image1, NULL));
  ASSERT_EQ(0, rbd_get_id(image1, child_id1, sizeof(child_id1)));
  test_list_children(parent, 1, pool_name2.c_str(), child_name1.c_str());
  test_list_children2(parent, 1,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false);

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx1, child_name2.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx1, child_name2.c_str(), &image2, NULL));
  ASSERT_EQ(0, rbd_get_id(image2, child_id2, sizeof(child_id2)));
  test_list_children(parent, 2, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str());
  test_list_children2(parent, 2,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false);

  // read from the cache to populate it
  rbd_image_t tier_image;
  ASSERT_EQ(0, rbd_open(ioctx1, child_name2.c_str(), &tier_image, NULL));
  size_t len = 4 * 1024 * 1024;
  char* buf = (char*)malloc(len);
  ssize_t size = rbd_read(tier_image, 0, len, buf);
  ASSERT_GT(size, 0);
  free(buf);
  ASSERT_EQ(0, rbd_close(tier_image));

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx2, child_name3.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx2, child_name3.c_str(), &image3, NULL));
  ASSERT_EQ(0, rbd_get_id(image3, child_id3, sizeof(child_id3)));
  test_list_children(parent, 3, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str());
  test_list_children2(parent, 3,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), false);

  librados::IoCtx ioctx3;
  ASSERT_EQ(0, _rados.ioctx_create(pool_name2.c_str(), ioctx3));
  ASSERT_EQ(0, rbd_close(image3));
  ASSERT_EQ(0, rbd.trash_move(ioctx3, child_name3.c_str(), 0));
  test_list_children(parent, 2, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str());
  test_list_children2(parent, 3,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), true);

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "parent_snap",
                           ioctx2, child_name4.c_str(), features, &order));
  ASSERT_EQ(0, rbd_open(ioctx2, child_name4.c_str(), &image4, NULL));
  ASSERT_EQ(0, rbd_get_id(image4, child_id4, sizeof(child_id4)));
  test_list_children(parent, 3, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 4,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), true,
                      child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd.trash_restore(ioctx3, child_id3, ""));
  test_list_children(parent, 4, pool_name2.c_str(), child_name1.c_str(),
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 4,
                      child_id1, pool_name2.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, pool_name2.c_str(), child_name3.c_str(), false,
                      child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd_close(image1));
  ASSERT_EQ(0, rbd_remove(ioctx2, child_name1.c_str()));
  test_list_children(parent, 3,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name3.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 3,
                     child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                     child_id3, pool_name2.c_str(), child_name3.c_str(), false,
                     child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd_remove(ioctx2, child_name3.c_str()));
  test_list_children(parent, 2,
		     pool_name1.c_str(), child_name2.c_str(),
		     pool_name2.c_str(), child_name4.c_str());
  test_list_children2(parent, 2,
                     child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                     child_id4, pool_name2.c_str(), child_name4.c_str(), false);

  ASSERT_EQ(0, rbd_close(image4));
  ASSERT_EQ(0, rbd_remove(ioctx2, child_name4.c_str()));
  test_list_children(parent, 1,
		     pool_name1.c_str(), child_name2.c_str());
  test_list_children2(parent, 1,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false);

  ASSERT_EQ(0, rbd_close(image2));
  ASSERT_EQ(0, rbd_remove(ioctx1, child_name2.c_str()));
  test_list_children(parent, 0);
  test_list_children2(parent, 0);

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
    const size_t num_aios = 256;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    char test_data[TEST_IO_SIZE + 1];
    size_t i;
    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    test_data[TEST_IO_SIZE] = '\0';

    librbd::RBD::AioCompletion *write_comps[num_aios];
    ceph::bufferlist bls[num_aios];
    for (i = 0; i < num_aios; ++i) {
      bls[i].append(test_data, strlen(test_data));
      write_comps[i] = new librbd::RBD::AioCompletion(NULL, NULL);
      uint64_t offset = rand() % (size - TEST_IO_SIZE);
      ASSERT_EQ(0, image.aio_write(offset, TEST_IO_SIZE, bls[i],
				   write_comps[i]));
    }

    librbd::RBD::AioCompletion *flush_comp =
      new librbd::RBD::AioCompletion(NULL, NULL);
    ASSERT_EQ(0, image.aio_flush(flush_comp));
    ASSERT_EQ(0, flush_comp->wait_for_complete());
    ASSERT_EQ(1, flush_comp->is_complete());
    flush_comp->release();

    for (i = 0; i < num_aios; ++i) {
      librbd::RBD::AioCompletion *comp = write_comps[i];
      ASSERT_EQ(1, comp->is_complete());
      comp->release();
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

static int iterate_error_cb(uint64_t off, size_t len, int exists, void *arg)
{
  return -EINVAL;
}

void scribble(librbd::Image& image, int n, int max, bool skip_discard,
              interval_set<uint64_t> *exists,
              interval_set<uint64_t> *what)
{
  uint64_t size;
  image.size(&size);
  interval_set<uint64_t> exists_at_start = *exists;

  for (int i=0; i<n; i++) {
    uint64_t off = rand() % (size - max + 1);
    uint64_t len = 1 + rand() % max;
    if (!skip_discard && rand() % 4 == 0) {
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

interval_set<uint64_t> round_diff_interval(const interval_set<uint64_t>& diff,
                                           uint64_t object_size)
{
  if (object_size == 0) {
    return diff;
  }

  interval_set<uint64_t> rounded_diff;
  for (interval_set<uint64_t>::const_iterator it = diff.begin();
       it != diff.end(); ++it) {
    uint64_t off = it.get_start();
    uint64_t len = it.get_len();
    off -= off % object_size;
    len += (object_size - (len % object_size));
    interval_set<uint64_t> interval;
    interval.insert(off, len);
    rounded_diff.union_of(interval);
  }
  return rounded_diff;
}

template <typename T>
class DiffIterateTest : public TestLibRBD {
public:
  static const uint8_t whole_object = T::whole_object;
};

template <bool _whole_object>
class DiffIterateParams {
public:
  static const uint8_t whole_object = _whole_object;
};

typedef ::testing::Types<DiffIterateParams<false>,
                         DiffIterateParams<true> > DiffIterateTypes;
TYPED_TEST_SUITE(DiffIterateTest, DiffIterateTypes);

TYPED_TEST(DiffIterateTest, DiffIterate)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = this->get_temp_image_name();
    uint64_t size = 20 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    bool skip_discard = this->is_skip_partial_discard_enabled(image);

    uint64_t object_size = 0;
    if (this->whole_object) {
      object_size = 1 << order;
    }

    interval_set<uint64_t> exists;
    interval_set<uint64_t> one, two;
    scribble(image, 10, 102400, skip_discard, &exists, &one);
    cout << " wrote " << one << std::endl;
    ASSERT_EQ(0, image.snap_create("one"));
    scribble(image, 10, 102400, skip_discard, &exists, &two);

    two = round_diff_interval(two, object_size);
    cout << " wrote " << two << std::endl;

    interval_set<uint64_t> diff;
    ASSERT_EQ(0, image.diff_iterate2("one", 0, size, true, this->whole_object,
                                     iterate_cb, (void *)&diff));
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
  diff_extent(uint64_t _offset, uint64_t _length, bool _exists,
              uint64_t object_size) :
    offset(_offset), length(_length), exists(_exists)
  {
    if (object_size != 0) {
      offset -= offset % object_size;
      length = object_size;
    }
  }
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
  //cout << "iterate_cb " << off << "~" << len << std::endl;
  vector<diff_extent> *diff = static_cast<vector<diff_extent> *>(arg);
  diff->push_back(diff_extent(off, len, exists, 0));
  return 0;
}

TYPED_TEST(DiffIterateTest, DiffIterateDeterministic)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));

  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(this->_cluster, this->m_pool_name.c_str(),
                                  &ioctx));

  rbd_image_t image;
  int order = 22;
  std::string name = this->get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  std::vector<diff_extent> extents;
  ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_EQ(-ENOENT, rbd_diff_iterate2(image, "snap1", 0, size, true,
                                       this->whole_object, vector_iterate_cb,
                                       &extents));

  ASSERT_EQ(0, rbd_snap_create(image, "snap1"));

  std::string buf(256, '1');
  ASSERT_EQ(256, rbd_write(image, 0, 256, buf.data()));
  ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  extents.clear();

  ASSERT_EQ(0, rbd_snap_create(image, "snap2"));

  ASSERT_EQ(256, rbd_write(image, 1 << order, 256, buf.data()));
  ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  ASSERT_EQ(0, rbd_snap_create(image, "snap3"));

  // 1. beginning of time -> HEAD
  ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 2. snap1 -> HEAD
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 3. snap2 -> HEAD
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap2", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 4. snap3 -> HEAD
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap3", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, rbd_snap_set(image, "snap3"));

  // 5. beginning of time -> snap3
  ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 6. snap1 -> snap3
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 7. snap2 -> snap3
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap2", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 8. snap3 -> snap3
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap3", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, rbd_snap_set(image, "snap2"));

  // 9. beginning of time -> snap2
  ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  extents.clear();

  // 10. snap1 -> snap2
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  extents.clear();

  // 11. snap2 -> snap2
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap2", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  // 12. snap3 -> snap2
  ASSERT_EQ(-EINVAL, rbd_diff_iterate2(image, "snap3", 0, size, true,
                                       this->whole_object, vector_iterate_cb,
                                       &extents));

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, rbd_snap_set(image, "snap1"));

  // 13. beginning of time -> snap1
  ASSERT_EQ(0, rbd_diff_iterate2(image, NULL, 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  // 14. snap1 -> snap1
  ASSERT_EQ(0, rbd_diff_iterate2(image, "snap1", 0, size, true, this->whole_object,
                                 vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  // 15. snap2 -> snap1
  ASSERT_EQ(-EINVAL, rbd_diff_iterate2(image, "snap2", 0, size, true,
                                       this->whole_object, vector_iterate_cb,
                                       &extents));

  // 16. snap3 -> snap1
  ASSERT_EQ(-EINVAL, rbd_diff_iterate2(image, "snap3", 0, size, true,
                                       this->whole_object, vector_iterate_cb,
                                       &extents));

  ASSERT_PASSED(this->validate_object_map, image);

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

TYPED_TEST(DiffIterateTest, DiffIterateDeterministicPP)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 22;
  std::string name = this->get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  std::vector<diff_extent> extents;
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_EQ(-ENOENT, image.diff_iterate2("snap1", 0, size, true,
                                         this->whole_object, vector_iterate_cb,
                                         &extents));

  ASSERT_EQ(0, image.snap_create("snap1"));

  ceph::bufferlist bl;
  bl.append(std::string(256, '1'));
  ASSERT_EQ(256, image.write(0, 256, bl));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  extents.clear();

  ASSERT_EQ(0, image.snap_create("snap2"));

  ASSERT_EQ(256, image.write(1 << order, 256, bl));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  ASSERT_EQ(0, image.snap_create("snap3"));

  // 1. beginning of time -> HEAD
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 2. snap1 -> HEAD
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 3. snap2 -> HEAD
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 4. snap3 -> HEAD
  ASSERT_EQ(0, image.diff_iterate2("snap3", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, image.snap_set("snap3"));

  // 5. beginning of time -> snap3
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 6. snap1 -> snap3
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 7. snap2 -> snap3
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 8. snap3 -> snap3
  ASSERT_EQ(0, image.diff_iterate2("snap3", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, image.snap_set("snap2"));

  // 9. beginning of time -> snap2
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  extents.clear();

  // 10. snap1 -> snap2
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  extents.clear();

  // 11. snap2 -> snap2
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  // 12. snap3 -> snap2
  ASSERT_EQ(-EINVAL, image.diff_iterate2("snap3", 0, size, true,
                                         this->whole_object, vector_iterate_cb,
                                         &extents));

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, image.snap_set("snap1"));

  // 13. beginning of time -> snap1
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  // 14. snap1 -> snap1
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  // 15. snap2 -> snap1
  ASSERT_EQ(-EINVAL, image.diff_iterate2("snap2", 0, size, true,
                                         this->whole_object, vector_iterate_cb,
                                         &extents));

  // 16. snap3 -> snap1
  ASSERT_EQ(-EINVAL, image.diff_iterate2("snap3", 0, size, true,
                                         this->whole_object, vector_iterate_cb,
                                         &extents));

  ASSERT_PASSED(this->validate_object_map, image);
}

TYPED_TEST(DiffIterateTest, DiffIterateDiscard)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 22;
  std::string name = this->get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  std::vector<diff_extent> extents;
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ceph::bufferlist bl;
  bl.append(std::string(256, '1'));
  ASSERT_EQ(256, image.write(0, 256, bl));
  ASSERT_EQ(256, image.write(1 << order, 256, bl));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  ASSERT_EQ(size, image.discard(0, size));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_EQ(0, image.snap_create("snap1"));

  ASSERT_EQ(256, image.write(0, 256, bl));
  ASSERT_EQ(256, image.write(1 << order, 256, bl));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  ASSERT_EQ(0, image.snap_create("snap2"));

  ASSERT_EQ(1 << order, image.discard(0, 1 << order));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  ASSERT_EQ(0, image.snap_create("snap3"));

  // 1. beginning of time -> HEAD
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 2. snap1 -> HEAD
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 3. snap2 -> HEAD
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, false, object_size), extents[0]);
  extents.clear();

  // 4. snap3 -> HEAD
  ASSERT_EQ(0, image.diff_iterate2("snap3", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, image.snap_set("snap3"));

  // 5. beginning of time -> snap3
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 6. snap1 -> snap3
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[0]);
  extents.clear();

  // 7. snap2 -> snap3
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, false, object_size), extents[0]);
  extents.clear();

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, image.snap_set("snap2"));

  // 8. beginning of time -> snap2
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  // 9. snap1 -> snap2
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(2u, extents.size());
  ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
  ASSERT_EQ(diff_extent(1 << order, 256, true, object_size), extents[1]);
  extents.clear();

  ASSERT_PASSED(this->validate_object_map, image);
  ASSERT_EQ(0, image.snap_set("snap1"));

  // 10. beginning of time -> snap1
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_PASSED(this->validate_object_map, image);
}

TYPED_TEST(DiffIterateTest, DiffIterateTruncate)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 22;
  std::string name = this->get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  ASSERT_EQ(0, image.snap_create("snap0"));

  ceph::bufferlist bl;
  bl.append(std::string(512 << 10, '1'));
  ASSERT_EQ(512 << 10, image.write(0, 512 << 10, bl));
  ASSERT_EQ(0, image.snap_create("snap1"));
  ASSERT_EQ(512 << 10, image.write(512 << 10, 512 << 10, bl));
  ASSERT_EQ(0, image.snap_create("snap2"));

  std::vector<diff_extent> extents;
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 1024 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 1024 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(512 << 10, 512 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_EQ(256 << 10, image.discard(768 << 10, 256 << 10));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 768 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 768 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(512 << 10, 256 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(768 << 10, 256 << 10, this->whole_object, object_size),
            extents[0]);
  extents.clear();

  ASSERT_EQ(256 << 10, image.discard(512 << 10, 256 << 10));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 512 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 512 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  if (this->whole_object &&
      (is_feature_enabled(RBD_FEATURE_OBJECT_MAP) ||
       is_feature_enabled(RBD_FEATURE_FAST_DIFF))) {
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 0, true, object_size), extents[0]);
    extents.clear();
  } else {
    ASSERT_EQ(0u, extents.size());
  }
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(512 << 10, 512 << 10, this->whole_object, object_size),
            extents[0]);
  extents.clear();

  ASSERT_EQ(256 << 10, image.discard(256 << 10, 256 << 10));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(256 << 10, 256 << 10, this->whole_object, object_size),
            extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(256 << 10, 768 << 10, this->whole_object, object_size),
            extents[0]);
  extents.clear();

  ASSERT_EQ(256 << 10, image.discard(0, 256 << 10));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  if (this->whole_object &&
      (is_feature_enabled(RBD_FEATURE_OBJECT_MAP) ||
       is_feature_enabled(RBD_FEATURE_FAST_DIFF))) {
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 0, true, object_size), extents[0]);
    extents.clear();
  } else {
    ASSERT_EQ(0u, extents.size());
  }
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  if (this->whole_object &&
      (is_feature_enabled(RBD_FEATURE_OBJECT_MAP) ||
       is_feature_enabled(RBD_FEATURE_FAST_DIFF))) {
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 0, true, object_size), extents[0]);
    extents.clear();
  } else {
    ASSERT_EQ(0u, extents.size());
  }
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 512 << 10, this->whole_object, object_size),
            extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 1024 << 10, this->whole_object, object_size),
            extents[0]);
  extents.clear();

  ASSERT_PASSED(this->validate_object_map, image);
}

TYPED_TEST(DiffIterateTest, DiffIterateWriteAndTruncate)
{
  REQUIRE(!is_feature_enabled(RBD_FEATURE_STRIPINGV2));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 22;
  std::string name = this->get_temp_image_name();
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  ASSERT_EQ(0, image.snap_create("snap0"));

  ceph::bufferlist bl;
  bl.append(std::string(512 << 10, '1'));
  ASSERT_EQ(512 << 10, image.write(0, 512 << 10, bl));
  ASSERT_EQ(0, image.snap_create("snap1"));
  ASSERT_EQ(512 << 10, image.write(512 << 10, 512 << 10, bl));
  ASSERT_EQ(0, image.snap_create("snap2"));

  std::vector<diff_extent> extents;
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 1024 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 1024 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(512 << 10, 512 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(0u, extents.size());

  ASSERT_EQ(1 << 10, image.write(767 << 10, 1 << 10, bl));
  ASSERT_EQ(256 << 10, image.discard(768 << 10, 256 << 10));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 768 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 768 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(512 << 10, 256 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  if (this->whole_object) {
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 0, true, object_size), extents[0]);
  } else {
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(767 << 10, 1 << 10, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(768 << 10, 256 << 10, false, object_size),
              extents[1]);
  }
  extents.clear();

  ASSERT_EQ(2 << 10, image.write(510 << 10, 2 << 10, bl));
  ASSERT_EQ(256 << 10, image.discard(512 << 10, 256 << 10));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 512 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 512 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(510 << 10, 2 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  if (this->whole_object) {
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 0, true, object_size), extents[0]);
  } else {
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(510 << 10, 2 << 10, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(512 << 10, 512 << 10, false, object_size),
              extents[1]);
  }
  extents.clear();

  ASSERT_EQ(3 << 10, image.write(253 << 10, 3 << 10, bl));
  ASSERT_EQ(256 << 10, image.discard(256 << 10, 256 << 10));
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap0", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(diff_extent(0, 256 << 10, true, object_size), extents[0]);
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap1", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  if (this->whole_object) {
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 0, true, object_size), extents[0]);
  } else {
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(253 << 10, 3 << 10, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(256 << 10, 256 << 10, false, object_size),
              extents[1]);
  }
  extents.clear();
  ASSERT_EQ(0, image.diff_iterate2("snap2", 0, size, true, this->whole_object,
                                   vector_iterate_cb, &extents));
  if (this->whole_object) {
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 0, true, object_size), extents[0]);
  } else {
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(253 << 10, 3 << 10, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(256 << 10, 768 << 10, false, object_size),
              extents[1]);
  }
  extents.clear();

  ASSERT_PASSED(this->validate_object_map, image);
}

TYPED_TEST(DiffIterateTest, DiffIterateStress)
{
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)this->_rados.cct()));
  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = this->get_temp_image_name();
  uint64_t size = 400 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  bool skip_discard = this->is_skip_partial_discard_enabled(image);

  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  interval_set<uint64_t> curexists;
  vector<interval_set<uint64_t> > wrote;
  vector<interval_set<uint64_t> > exists;
  vector<string> snap;
  int n = 20;
  for (int i=0; i<n; i++) {
    interval_set<uint64_t> w;
    scribble(image, 10, 8192000, skip_discard, &curexists, &w);
    cout << " i=" << i << " exists " << curexists << " wrote " << w << std::endl;
    string s = "snap" + stringify(i);
    ASSERT_EQ(0, image.snap_create(s.c_str()));
    wrote.push_back(w);
    exists.push_back(curexists);
    snap.push_back(s);
  }

  for (int h=0; h<n-1; h++) {
    for (int i=0; i<n-h-1; i++) {
      for (int j=(h==0 ? i+1 : n-1); j<n; j++) {
        interval_set<uint64_t> diff, actual, uex;
        for (int k=i+1; k<=j; k++)
          diff.union_of(wrote[k]);
        cout << "from " << i << " to "
             << (h != 0 ? string("HEAD") : stringify(j)) << " diff "
             << round_diff_interval(diff, object_size) << std::endl;

        // limit to extents that exists both at the beginning and at the end
        uex.union_of(exists[i], exists[j]);
        diff.intersection_of(uex);
        diff = round_diff_interval(diff, object_size);
        cout << " limited diff " << diff << std::endl;

        ASSERT_EQ(0, image.snap_set(h==0 ? snap[j].c_str() : NULL));
        ASSERT_EQ(0, image.diff_iterate2(snap[i].c_str(), 0, size, true,
                                         this->whole_object, iterate_cb,
                                         (void *)&actual));
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
    ASSERT_EQ(0, image.snap_set(NULL));
    ASSERT_EQ(0, image.snap_remove(snap[n-h-1].c_str()));
  }

  ASSERT_PASSED(this->validate_object_map, image);
}

TYPED_TEST(DiffIterateTest, DiffIterateParent)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 22;
    std::string name = this->get_temp_image_name();
    ssize_t size = 20 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    uint64_t features;
    ASSERT_EQ(0, image.features(&features));
    uint64_t object_size = 0;
    if (this->whole_object) {
      object_size = 1 << order;
    }

    ceph::bufferlist bl;
    bl.append(std::string(size, '1'));
    ASSERT_EQ(size, image.write(0, size, bl));
    ASSERT_EQ(0, image.snap_create("snap"));
    ASSERT_EQ(0, image.snap_protect("snap"));

    std::string clone_name = this->get_temp_image_name();
    ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "snap", ioctx,
                           clone_name.c_str(), features, &order));
    librbd::Image clone;
    ASSERT_EQ(0, rbd.open(ioctx, clone, clone_name.c_str(), NULL));

    std::vector<diff_extent> extents;
    ASSERT_EQ(0, clone.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                     vector_iterate_cb, &extents));
    ASSERT_EQ(5u, extents.size());
    ASSERT_EQ(diff_extent(0, 4194304, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(4194304, 4194304, true, object_size), extents[1]);
    ASSERT_EQ(diff_extent(8388608, 4194304, true, object_size), extents[2]);
    ASSERT_EQ(diff_extent(12582912, 4194304, true, object_size), extents[3]);
    ASSERT_EQ(diff_extent(16777216, 4194304, true, object_size), extents[4]);
    extents.clear();

    ASSERT_EQ(0, clone.resize(size / 2));
    ASSERT_EQ(0, clone.resize(size));
    ASSERT_EQ(1, clone.write(size - 1, 1, bl));

    ASSERT_EQ(0, clone.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                     vector_iterate_cb, &extents));
    ASSERT_EQ(4u, extents.size());
    ASSERT_EQ(diff_extent(0, 4194304, true, object_size), extents[0]);
    ASSERT_EQ(diff_extent(4194304, 4194304, true, object_size), extents[1]);
    ASSERT_EQ(diff_extent(8388608, 2097152, true, object_size), extents[2]);
    // hole (parent overlap = 10M) followed by copyup'ed object
    ASSERT_EQ(diff_extent(16777216, 4194304, true, object_size), extents[3]);

    ASSERT_PASSED(this->validate_object_map, image);
    ASSERT_PASSED(this->validate_object_map, clone);
  }

  ioctx.close();
}

TYPED_TEST(DiffIterateTest, DiffIterateIgnoreParent)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  std::string name = this->get_temp_image_name();
  uint64_t size = 20 << 20;
  int order = 0;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  bool skip_discard = this->is_skip_partial_discard_enabled(image);

  uint64_t features;
  ASSERT_EQ(0, image.features(&features));
  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  bufferlist bl;
  bl.append(buffer::create(size));
  bl.zero();
  interval_set<uint64_t> one;
  one.insert(0, size);
  ASSERT_EQ((int)size, image.write(0, size, bl));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));

  std::string clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "one", ioctx, clone_name.c_str(),
                         features, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, clone_name.c_str(), NULL));

  interval_set<uint64_t> exists;
  interval_set<uint64_t> two;
  scribble(image, 10, 102400, skip_discard, &exists, &two);
  two = round_diff_interval(two, object_size);
  cout << " wrote " << two << " to clone" << std::endl;

  interval_set<uint64_t> diff;
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, false, this->whole_object,
                                   iterate_cb, (void *)&diff));
  cout << " diff was " << diff << std::endl;
  if (!this->whole_object) {
    ASSERT_FALSE(one.subset_of(diff));
  }
  ASSERT_TRUE(two.subset_of(diff));
}

TYPED_TEST(DiffIterateTest, DiffIterateCallbackError)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 0;
    std::string name = this->get_temp_image_name();
    uint64_t size = 20 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    bool skip_discard = this->is_skip_partial_discard_enabled(image);

    interval_set<uint64_t> exists;
    interval_set<uint64_t> one;
    scribble(image, 10, 102400, skip_discard, &exists, &one);
    cout << " wrote " << one << std::endl;

    interval_set<uint64_t> diff;
    ASSERT_EQ(-EINVAL, image.diff_iterate2(NULL, 0, size, true,
                                           this->whole_object,
                                           iterate_error_cb, NULL));
  }
  ioctx.close();
}

TYPED_TEST(DiffIterateTest, DiffIterateParentDiscard)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  std::string name = this->get_temp_image_name();
  uint64_t size = 20 << 20;
  int order = 0;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  bool skip_discard = this->is_skip_partial_discard_enabled(image);

  uint64_t features;
  ASSERT_EQ(0, image.features(&features));
  uint64_t object_size = 0;
  if (this->whole_object) {
    object_size = 1 << order;
  }

  interval_set<uint64_t> exists;
  interval_set<uint64_t> one;
  scribble(image, 10, 102400, skip_discard, &exists, &one);
  ASSERT_EQ(0, image.snap_create("one"));

  ASSERT_EQ(1 << order, image.discard(0, 1 << order));
  ASSERT_EQ(0, image.snap_create("two"));
  ASSERT_EQ(0, image.snap_protect("two"));
  exists.clear();
  one.clear();

  std::string clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "two", ioctx,
                         clone_name.c_str(), features, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, clone_name.c_str(), NULL));

  interval_set<uint64_t> two;
  scribble(image, 10, 102400, skip_discard, &exists, &two);
  two = round_diff_interval(two, object_size);

  interval_set<uint64_t> diff;
  ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                   iterate_cb, (void *)&diff));
  ASSERT_TRUE(two.subset_of(diff));
}

TYPED_TEST(DiffIterateTest, DiffIterateUnalignedSmall)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 22;
    std::string name = this->get_temp_image_name();
    ssize_t data_end = 8 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(),
                                 data_end + (2 << 20), &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    ceph::bufferlist bl;
    bl.append(std::string(data_end, '1'));
    ASSERT_EQ(data_end, image.write(0, data_end, bl));

    std::vector<diff_extent> extents;
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, 0, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_EQ(0, image.diff_iterate2(NULL, 5000005, 1234, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(5000005, 1234, true, 0), extents[0]);
    extents.clear();

    ASSERT_EQ(0, image.diff_iterate2(NULL, data_end - 1, 0, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_EQ(0, image.diff_iterate2(NULL, data_end - 1, 1, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(data_end - 1, 1, true, 0), extents[0]);
    extents.clear();

    ASSERT_EQ(0, image.diff_iterate2(NULL, data_end - 1, 2, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(data_end - 1, 1, true, 0), extents[0]);
    extents.clear();

    ASSERT_EQ(0, image.diff_iterate2(NULL, data_end, 0, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_EQ(0, image.diff_iterate2(NULL, data_end, 1, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(0u, extents.size());

    ASSERT_PASSED(this->validate_object_map, image);
  }

  ioctx.close();
}

TYPED_TEST(DiffIterateTest, DiffIterateUnaligned)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 22;
    std::string name = this->get_temp_image_name();
    ssize_t size = 20 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    ceph::bufferlist bl;
    bl.append(std::string(size, '1'));
    ASSERT_EQ(size, image.write(0, size, bl));

    std::vector<diff_extent> extents;
    ASSERT_EQ(0, image.diff_iterate2(NULL, 8376263, 4260970, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(3u, extents.size());
    ASSERT_EQ(diff_extent(8376263, 12345, true, 0), extents[0]);
    ASSERT_EQ(diff_extent(8388608, 4194304, true, 0), extents[1]);
    ASSERT_EQ(diff_extent(12582912, 54321, true, 0), extents[2]);
    extents.clear();

    // length is clipped up to end
    ASSERT_EQ(0, image.diff_iterate2(NULL, size - 1, size, true,
                                     this->whole_object, vector_iterate_cb,
                                     &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(size - 1, 1, true, 0), extents[0]);
    extents.clear();

    // offset past end
    ASSERT_EQ(-EINVAL, image.diff_iterate2(NULL, size, size, true,
                                           this->whole_object,
                                           vector_iterate_cb, &extents));

    ASSERT_PASSED(this->validate_object_map, image);
  }

  ioctx.close();
}

TYPED_TEST(DiffIterateTest, DiffIterateTryAcquireLock)
{
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    int order = 22;
    std::string name = this->get_temp_image_name();
    ssize_t size = 20 << 20;

    uint64_t object_size = 0;
    if (this->whole_object) {
      object_size = 1 << order;
    }

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

    librbd::Image image1;
    ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

    ceph::bufferlist bl;
    bl.append(std::string(256, '1'));
    ASSERT_EQ(256, image1.write(0, 256, bl));
    ASSERT_EQ(0, image1.flush());

    bool lock_owner;
    ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
    ASSERT_TRUE(lock_owner);

    librbd::Image image2;
    ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

    std::vector<diff_extent> extents;
    ASSERT_EQ(0, image2.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                      vector_iterate_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
    extents.clear();

    ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
    ASSERT_FALSE(lock_owner);

    ASSERT_EQ(0, image1.close());
    ASSERT_EQ(0, image2.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                      vector_iterate_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
    extents.clear();

    ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
    ASSERT_FALSE(lock_owner);

    sleep(5);
    ASSERT_EQ(0, image2.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                      vector_iterate_cb, &extents));
    ASSERT_EQ(1u, extents.size());
    ASSERT_EQ(diff_extent(0, 256, true, object_size), extents[0]);
    extents.clear();

    ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
    if (this->whole_object &&
        (is_feature_enabled(RBD_FEATURE_OBJECT_MAP) ||
         is_feature_enabled(RBD_FEATURE_FAST_DIFF))) {
      ASSERT_TRUE(lock_owner);
    } else {
      ASSERT_FALSE(lock_owner);
    }

    ASSERT_PASSED(this->validate_object_map, image2);
  }

  ioctx.close();
}

TYPED_TEST(DiffIterateTest, DiffIterateStriping)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, this->_rados.ioctx_create(this->m_pool_name.c_str(), ioctx));

  bool old_format;
  uint64_t features;
  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 22;
    std::string name = this->get_temp_image_name();
    ssize_t size = 24 << 20;

    ASSERT_EQ(0, rbd.create3(ioctx, name.c_str(), size, features, &order,
                             1 << 20, 3));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    ceph::bufferlist bl;
    bl.append(std::string(size, '1'));
    ASSERT_EQ(size, image.write(0, size, bl));

    std::vector<diff_extent> extents;
    ASSERT_EQ(0, image.diff_iterate2(NULL, 0, size, true, this->whole_object,
                                     vector_iterate_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(0, 12 << 20, true, 0), extents[0]);
    ASSERT_EQ(diff_extent(12 << 20, 12 << 20, true, 0), extents[1]);
    extents.clear();

    ASSERT_EQ(0, image.snap_create("one"));
    ASSERT_EQ(size, image.discard(0, size));

    ASSERT_EQ(0, image.diff_iterate2("one", 0, size, true, this->whole_object,
                                     vector_iterate_cb, &extents));
    ASSERT_EQ(2u, extents.size());
    ASSERT_EQ(diff_extent(0, 12 << 20, false, 0), extents[0]);
    ASSERT_EQ(diff_extent(12 << 20, 12 << 20, false, 0), extents[1]);
    extents.clear();

    ASSERT_EQ(1 << 20, image.write(0, 1 << 20, bl));
    ASSERT_EQ(2 << 20, image.write(2 << 20, 2 << 20, bl));
    ASSERT_EQ(2 << 20, image.write(5 << 20, 2 << 20, bl));
    ASSERT_EQ(2 << 20, image.write(8 << 20, 2 << 20, bl));
    ASSERT_EQ(13 << 20, image.write(11 << 20, 13 << 20, bl));

    ASSERT_EQ(0, image.diff_iterate2("one", 0, size, true, this->whole_object,
                                     vector_iterate_cb, &extents));
    ASSERT_EQ(10u, extents.size());
    ASSERT_EQ(diff_extent(0, 1 << 20, true, 0), extents[0]);
    ASSERT_EQ(diff_extent(1 << 20, 1 << 20, false, 0), extents[1]);
    ASSERT_EQ(diff_extent(2 << 20, 2 << 20, true, 0), extents[2]);
    ASSERT_EQ(diff_extent(4 << 20, 1 << 20, false, 0), extents[3]);
    ASSERT_EQ(diff_extent(5 << 20, 2 << 20, true, 0), extents[4]);
    ASSERT_EQ(diff_extent(7 << 20, 1 << 20, false, 0), extents[5]);
    ASSERT_EQ(diff_extent(8 << 20, 2 << 20, true, 0), extents[6]);
    ASSERT_EQ(diff_extent(10 << 20, 1 << 20, false, 0), extents[7]);
    ASSERT_EQ(diff_extent(11 << 20, 1 << 20, true, 0), extents[8]);
    ASSERT_EQ(diff_extent(12 << 20, 12 << 20, true, 0), extents[9]);

    ASSERT_PASSED(this->validate_object_map, image);
  }

  ioctx.close();
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

  const char data[] = "blah";
  ASSERT_EQ(0, rbd_write(image, 0, 0, data));
  ASSERT_EQ(0, rbd_write(image, 0, 0, (char*)0x123));
  ASSERT_EQ(0, rbd_write(image, 0, 0, NULL));

  char read_data[1];
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

  const char data[] = "blah";
  char read_data[sizeof(data)];
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
  ASSERT_EQ(0, rbd_read(image, 0, 0, (char*)0x123));
  ASSERT_EQ(0, rbd_read(image, 0, 0, NULL));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, LargeCacheRead)
{
  std::string config_value;
  ASSERT_EQ(0, _rados.conf_get("rbd_cache", config_value));
  if (config_value == "false") {
    GTEST_SKIP() << "Skipping due to disabled cache";
  }

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  uint32_t new_cache_size = 1 << 20;
  std::string orig_cache_size;
  ASSERT_EQ(0, _rados.conf_get("rbd_cache_size", orig_cache_size));
  ASSERT_EQ(0, _rados.conf_set("rbd_cache_size",
                               stringify(new_cache_size).c_str()));
  ASSERT_EQ(0, _rados.conf_get("rbd_cache_size", config_value));
  ASSERT_EQ(stringify(new_cache_size), config_value);
  BOOST_SCOPE_EXIT( (orig_cache_size) ) {
    ASSERT_EQ(0, _rados.conf_set("rbd_cache_size", orig_cache_size.c_str()));
  } BOOST_SCOPE_EXIT_END;

  rbd_image_t image;
  int order = 21;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << order;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  std::string buffer(1 << order, '1');
 
  ASSERT_EQ(static_cast<ssize_t>(buffer.size()),
	    rbd_write(image, 0, buffer.size(), buffer.c_str()));

  ASSERT_EQ(0, rbd_invalidate_cache(image));

  ASSERT_EQ(static_cast<ssize_t>(buffer.size()), 
  	    rbd_read(image, 0, buffer.size(), &buffer[0]));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestPendingAio)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  bool old_format;
  uint64_t features;
  rbd_image_t image;
  int order = 0;

  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);

  std::string name = get_temp_image_name();

  uint64_t size = 4 << 20;
  ASSERT_EQ(0, create_image_full(ioctx, name.c_str(), size, &order,
				 false, features));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(0, rbd_invalidate_cache(image));

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

void compare_and_write_copyup(librados::IoCtx &ioctx, bool deep_copyup,
                              bool *passed)
{
  librbd::RBD rbd;
  std::string parent_name = TestLibRBD::get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(), size, &order));

  librbd::Image parent_image;
  ASSERT_EQ(0, rbd.open(ioctx, parent_image, parent_name.c_str(), NULL));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  ASSERT_EQ((ssize_t)bl.length(), parent_image.write(0, bl.length(), bl));

  ASSERT_EQ(0, parent_image.snap_create("snap1"));
  ASSERT_EQ(0, parent_image.snap_protect("snap1"));

  uint64_t features;
  ASSERT_EQ(0, parent_image.features(&features));

  std::string clone_name = TestLibRBD::get_temp_image_name();
  EXPECT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "snap1", ioctx,
       clone_name.c_str(), features, &order));

  librbd::Image clone_image;
  ASSERT_EQ(0, rbd.open(ioctx, clone_image, clone_name.c_str(), NULL));
  if (deep_copyup) {
    ASSERT_EQ(0, clone_image.snap_create("snap1"));
  }

  bufferlist cmp_bl;
  cmp_bl.append(std::string(512, '1'));
  bufferlist write_bl;
  write_bl.append(std::string(512, '2'));
  uint64_t mismatch_off = 0;
  ASSERT_EQ((ssize_t)write_bl.length(),
            clone_image.compare_and_write(512, write_bl.length(), cmp_bl,
                                          write_bl, &mismatch_off, 0));
  ASSERT_EQ(0U, mismatch_off);
  bufferlist read_bl;
  ASSERT_EQ(4096, clone_image.read(0, 4096, read_bl));

  bufferlist expected_bl;
  expected_bl.append(std::string(512, '1'));
  expected_bl.append(std::string(512, '2'));
  expected_bl.append(std::string(3072, '1'));
  ASSERT_TRUE(expected_bl.contents_equal(read_bl));
  *passed = true;
}

TEST_F(TestLibRBD, CompareAndWriteCopyup)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  ASSERT_PASSED(compare_and_write_copyup, ioctx, false);
  ASSERT_PASSED(compare_and_write_copyup, ioctx, true);
}

void compare_and_write_copyup_mismatch(librados::IoCtx &ioctx,
                                       bool deep_copyup, bool *passed)
{
  librbd::RBD rbd;
  std::string parent_name = TestLibRBD::get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(), size, &order));

  librbd::Image parent_image;
  ASSERT_EQ(0, rbd.open(ioctx, parent_image, parent_name.c_str(), NULL));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  ASSERT_EQ((ssize_t)bl.length(), parent_image.write(0, bl.length(), bl));

  ASSERT_EQ(0, parent_image.snap_create("snap1"));
  ASSERT_EQ(0, parent_image.snap_protect("snap1"));

  uint64_t features;
  ASSERT_EQ(0, parent_image.features(&features));

  std::string clone_name = TestLibRBD::get_temp_image_name();
  EXPECT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "snap1", ioctx,
       clone_name.c_str(), features, &order));

  librbd::Image clone_image;
  ASSERT_EQ(0, rbd.open(ioctx, clone_image, clone_name.c_str(), NULL));
  if (deep_copyup) {
    ASSERT_EQ(0, clone_image.snap_create("snap1"));
  }

  bufferlist cmp_bl;
  cmp_bl.append(std::string(48, '1'));
  cmp_bl.append(std::string(464, '3'));
  bufferlist write_bl;
  write_bl.append(std::string(512, '2'));
  uint64_t mismatch_off = 0;
  ASSERT_EQ(-EILSEQ,
            clone_image.compare_and_write(512, write_bl.length(), cmp_bl,
                                          write_bl, &mismatch_off, 0));
  ASSERT_EQ(48U, mismatch_off);

  bufferlist read_bl;
  ASSERT_EQ(4096, clone_image.read(0, 4096, read_bl));

  ASSERT_TRUE(bl.contents_equal(read_bl));
  *passed = true;
}

TEST_F(TestLibRBD, CompareAndWriteCopyupMismatch)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  ASSERT_PASSED(compare_and_write_copyup_mismatch, ioctx, false);
  ASSERT_PASSED(compare_and_write_copyup_mismatch, ioctx, true);
}

TEST_F(TestLibRBD, Flatten)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string parent_name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(), size, &order));

  librbd::Image parent_image;
  ASSERT_EQ(0, rbd.open(ioctx, parent_image, parent_name.c_str(), NULL));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  ASSERT_EQ((ssize_t)bl.length(), parent_image.write(0, bl.length(), bl));

  ASSERT_EQ(0, parent_image.snap_create("snap1"));
  ASSERT_EQ(0, parent_image.snap_protect("snap1"));

  uint64_t features;
  ASSERT_EQ(0, parent_image.features(&features));

  std::string clone_name = get_temp_image_name();
  EXPECT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "snap1", ioctx,
       clone_name.c_str(), features, &order));

  librbd::Image clone_image;
  ASSERT_EQ(0, rbd.open(ioctx, clone_image, clone_name.c_str(), NULL));
  ASSERT_EQ(0, clone_image.flatten());

  librbd::RBD::AioCompletion *read_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  bufferlist read_bl;
  clone_image.aio_read(0, bl.length(), read_bl, read_comp);
  ASSERT_EQ(0, read_comp->wait_for_complete());
  ASSERT_EQ((ssize_t)bl.length(), read_comp->get_return_value());
  read_comp->release();
  ASSERT_TRUE(bl.contents_equal(read_bl));

  ASSERT_PASSED(validate_object_map, clone_image);
}

TEST_F(TestLibRBD, Sparsify)
{
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));
  BOOST_SCOPE_EXIT_ALL(&ioctx) {
    rados_ioctx_destroy(ioctx);
  };

  const size_t CHUNK_SIZE = 4096 * 2;
  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = CHUNK_SIZE * 1024;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  BOOST_SCOPE_EXIT_ALL(&image) {
    rbd_close(image);
  };

  char test_data[4 * CHUNK_SIZE + 1];
  for (size_t i = 0; i < 4 ; ++i) {
    for (size_t j = 0; j < CHUNK_SIZE; j++) {
      if (i % 2) {
        test_data[i * CHUNK_SIZE + j] = (char)(rand() % (126 - 33) + 33);
      } else {
        test_data[i * CHUNK_SIZE + j] = '\0';
      }
    }
  }
  test_data[4 * CHUNK_SIZE] = '\0';

  ASSERT_PASSED(write_test_data, image, test_data, 0, 4 * CHUNK_SIZE, 0);
  ASSERT_EQ(0, rbd_flush(image));

  ASSERT_EQ(-EINVAL, rbd_sparsify(image, 16));
  ASSERT_EQ(-EINVAL, rbd_sparsify(image, 1 << (order + 1)));
  ASSERT_EQ(-EINVAL, rbd_sparsify(image, 4096 + 1));
  ASSERT_EQ(0, rbd_sparsify(image, 4096));

  ASSERT_PASSED(read_test_data, image, test_data, 0, 4 * CHUNK_SIZE, 0);
}

TEST_F(TestLibRBD, SparsifyPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 12 * 1024 * 1024;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

  bufferlist bl;
  bl.append(std::string(4096, '\0'));
  bl.append(std::string(4096, '1'));
  bl.append(std::string(4096, '\0'));
  ASSERT_EQ((ssize_t)bl.length(), image.write(0, bl.length(), bl));
  ASSERT_EQ(0, image.flush());

  ASSERT_EQ(-EINVAL, image.sparsify(16));
  ASSERT_EQ(-EINVAL, image.sparsify(1 << (order + 1)));
  ASSERT_EQ(-EINVAL, image.sparsify(4096 + 1));
  ASSERT_EQ(0, image.sparsify(4096));

  bufferlist read_bl;
  ASSERT_EQ((ssize_t)bl.length(), image.read(0, bl.length(), read_bl));
  ASSERT_TRUE(bl.contents_equal(read_bl));

  ASSERT_PASSED(validate_object_map, image);
}

TEST_F(TestLibRBD, SnapshotLimit)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  uint64_t limit;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(0, rbd_snap_get_limit(image, &limit));
  ASSERT_EQ(UINT64_MAX, limit);
  ASSERT_EQ(0, rbd_snap_set_limit(image, 2));
  ASSERT_EQ(0, rbd_snap_get_limit(image, &limit));
  ASSERT_EQ(2U, limit);

  ASSERT_EQ(0, rbd_snap_create(image, "snap1"));
  ASSERT_EQ(-ERANGE, rbd_snap_set_limit(image, 0));
  ASSERT_EQ(0, rbd_snap_create(image, "snap2"));
  ASSERT_EQ(-EDQUOT, rbd_snap_create(image, "snap3"));
  ASSERT_EQ(0, rbd_snap_set_limit(image, UINT64_MAX));
  ASSERT_EQ(0, rbd_snap_create(image, "snap3"));
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}
  

TEST_F(TestLibRBD, SnapshotLimitPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    std::string name = get_temp_image_name();
    uint64_t size = 2 << 20;
    int order = 0;
    uint64_t limit;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    ASSERT_EQ(0, image.snap_get_limit(&limit));
    ASSERT_EQ(UINT64_MAX, limit);
    ASSERT_EQ(0, image.snap_set_limit(2));
    ASSERT_EQ(0, image.snap_get_limit(&limit));
    ASSERT_EQ(2U, limit);

    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_EQ(-ERANGE, image.snap_set_limit(0));
    ASSERT_EQ(0, image.snap_create("snap2"));
    ASSERT_EQ(-EDQUOT, image.snap_create("snap3"));
    ASSERT_EQ(0, image.snap_set_limit(UINT64_MAX));
    ASSERT_EQ(0, image.snap_create("snap3"));
  }

  ioctx.close();
}

TEST_F(TestLibRBD, RebuildObjectMapViaLockOwner)
{
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_OBJECT_MAP);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  std::string object_map_oid;
  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    std::string image_id;
    ASSERT_EQ(0, get_image_id(image, &image_id));
    object_map_oid = RBD_OBJECT_MAP_PREFIX + image_id;
  }

  // corrupt the object map
  bufferlist bl;
  bl.append("foo");
  ASSERT_EQ(0, ioctx.write(object_map_oid, bl, bl.length(), 0));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  bool lock_owner;
  bl.clear();
  ASSERT_EQ(0, image1.write(0, 0, bl));
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  uint64_t flags;
  ASSERT_EQ(0, image1.get_flags(&flags));
  ASSERT_TRUE((flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));
  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  PrintProgress prog_ctx;
  ASSERT_EQ(0, image2.rebuild_object_map(prog_ctx));
  ASSERT_PASSED(validate_object_map, image1);
  ASSERT_PASSED(validate_object_map, image2);
}

TEST_F(TestLibRBD, RenameViaLockOwner)
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

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  std::string new_name = get_temp_image_name();
  ASSERT_EQ(0, rbd.rename(ioctx, name.c_str(), new_name.c_str()));
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  bufferlist bl;
  ASSERT_EQ(0, image1.write(0, 0, bl));
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  name = new_name;
  new_name = get_temp_image_name();
  ASSERT_EQ(0, rbd.rename(ioctx, name.c_str(), new_name.c_str()));
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, new_name.c_str(), NULL));
}

TEST_F(TestLibRBD, SnapCreateViaLockOwner)
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

  // switch to writeback cache
  ASSERT_EQ(0, image1.flush());

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  ASSERT_EQ((ssize_t)bl.length(), image1.write(0, bl.length(), bl));

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.snap_create("snap1"));
  bool exists;
  ASSERT_EQ(0, image1.snap_exists2("snap1", &exists));
  ASSERT_TRUE(exists);
  ASSERT_EQ(0, image2.snap_exists2("snap1", &exists));
  ASSERT_TRUE(exists);

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
}

TEST_F(TestLibRBD, SnapRemoveViaLockOwner)
{
  REQUIRE_FEATURE(RBD_FEATURE_FAST_DIFF);

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
  ASSERT_EQ(0, image1.snap_create("snap1"));

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.snap_remove("snap1"));
  bool exists;
  ASSERT_EQ(0, image1.snap_exists2("snap1", &exists));
  ASSERT_FALSE(exists);
  ASSERT_EQ(0, image2.snap_exists2("snap1", &exists));
  ASSERT_FALSE(exists);

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
}

TEST_F(TestLibRBD, UpdateFeaturesViaLockOwner) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  librbd::RBD rbd;
  int order = 0;
  //creates full with rbd default features
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  bool lock_owner;
  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));
  bufferlist bl;
  ASSERT_EQ(0, image1.write(0, 0, bl));
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));
  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.update_features(RBD_FEATURE_OBJECT_MAP, false));
  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.update_features(RBD_FEATURE_OBJECT_MAP, true));
  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

}

TEST_F(TestLibRBD, EnableJournalingViaLockOwner)
{
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

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

  ASSERT_EQ(0, image2.update_features(RBD_FEATURE_JOURNALING, false));

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.update_features(RBD_FEATURE_JOURNALING, true));

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);
  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
}

TEST_F(TestLibRBD, SnapRemove2)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

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
  ASSERT_EQ(0, image1.snap_create("snap1"));
  bool exists;
  ASSERT_EQ(0, image1.snap_exists2("snap1", &exists));
  ASSERT_TRUE(exists);
  ASSERT_EQ(0, image1.snap_protect("snap1"));
  bool is_protected;
  ASSERT_EQ(0, image1.snap_is_protected("snap1", &is_protected));
  ASSERT_TRUE(is_protected);

  uint64_t features;
  ASSERT_EQ(0, image1.features(&features));

  std::string child_name = get_temp_image_name();
  EXPECT_EQ(0, rbd.clone(ioctx, name.c_str(), "snap1", ioctx,
			 child_name.c_str(), features, &order));

  ASSERT_EQ(0, image1.snap_exists2("snap1", &exists));
  ASSERT_TRUE(exists);
  ASSERT_EQ(0, image1.snap_is_protected("snap1", &is_protected));
  ASSERT_TRUE(is_protected);

  ASSERT_EQ(-EBUSY, image1.snap_remove("snap1"));
  PrintProgress pp;
  ASSERT_EQ(0, image1.snap_remove2("snap1", RBD_SNAP_REMOVE_FORCE, pp));
  ASSERT_EQ(0, image1.snap_exists2("snap1", &exists));
  ASSERT_FALSE(exists);
}

TEST_F(TestLibRBD, SnapRenameViaLockOwner)
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
  ASSERT_EQ(0, image1.snap_create("snap1"));

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.snap_rename("snap1", "snap1-rename"));
  bool exists;
  ASSERT_EQ(0, image1.snap_exists2("snap1-rename", &exists));
  ASSERT_TRUE(exists);
  ASSERT_EQ(0, image2.snap_exists2("snap1-rename", &exists));
  ASSERT_TRUE(exists);

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
}

TEST_F(TestLibRBD, SnapProtectViaLockOwner)
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
  ASSERT_EQ(0, image1.snap_create("snap1"));

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.snap_protect("snap1"));
  bool is_protected;
  ASSERT_EQ(0, image2.snap_is_protected("snap1", &is_protected));
  ASSERT_TRUE(is_protected);
  ASSERT_EQ(0, image1.snap_is_protected("snap1", &is_protected));
  ASSERT_TRUE(is_protected);

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
}

TEST_F(TestLibRBD, SnapUnprotectViaLockOwner)
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
  ASSERT_EQ(0, image1.snap_create("snap1"));
  ASSERT_EQ(0, image1.snap_protect("snap1"));
  bool is_protected;
  ASSERT_EQ(0, image1.snap_is_protected("snap1", &is_protected));
  ASSERT_TRUE(is_protected);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  ASSERT_EQ(0, image2.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, image2.snap_unprotect("snap1"));
  ASSERT_EQ(0, image2.snap_is_protected("snap1", &is_protected));
  ASSERT_FALSE(is_protected);
  ASSERT_EQ(0, image1.snap_is_protected("snap1", &is_protected));
  ASSERT_FALSE(is_protected);

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

TEST_F(TestLibRBD, SparsifyViaLockOwner)
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

  ASSERT_EQ(0, image2.sparsify(4096));

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);
  ASSERT_PASSED(validate_object_map, image1);
}

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

  int num_snaps = 10;
  for (int i = 0; i < num_snaps; ++i) {
    std::string snap_name = "snap" + stringify(i);
    ASSERT_EQ(0, image1.snap_create(snap_name.c_str()));
  }


  thread writer([&image1](){
      librbd::image_info_t info;
      int r = image1.stat(info, sizeof(info));
      ceph_assert(r == 0);
      bufferlist bl;
      bl.append("foo");
      for (unsigned i = 0; i < info.num_objs; ++i) {
	r = image1.write((1 << info.order) * i, bl.length(), bl);
	ceph_assert(r == (int) bl.length());
      }
    });
  writer.join();

  for (int i = 0; i < num_snaps; ++i) {
    std::string snap_name = "snap" + stringify(i);
    ASSERT_EQ(0, image1.snap_set(snap_name.c_str()));
    ASSERT_PASSED(validate_object_map, image1);
  }

  ASSERT_EQ(0, image1.snap_set(NULL));
  ASSERT_PASSED(validate_object_map, image1);
}

void memset_rand(char *buf, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    buf[i] = (char) (rand() % (126 - 33) + 33);
  }
}

TEST_F(TestLibRBD, Metadata)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  rbd_image_t image1;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image1, NULL));

  char keys[1024];
  char vals[1024];
  size_t keys_len = sizeof(keys);
  size_t vals_len = sizeof(vals);

  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);

  ASSERT_EQ(0, rbd_metadata_list(image, "key", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(0U, keys_len);
  ASSERT_EQ(0U, vals_len);

  char value[1024];
  size_t value_len = sizeof(value);
  memset_rand(value, value_len);

  ASSERT_EQ(0, rbd_metadata_set(image1, "key1", "value1"));
  ASSERT_EQ(0, rbd_metadata_set(image1, "key2", "value2"));
  ASSERT_EQ(0, rbd_metadata_get(image1, "key1", value, &value_len));
  ASSERT_STREQ(value, "value1");
  value_len = 1;
  ASSERT_EQ(-ERANGE, rbd_metadata_get(image1, "key1", value, &value_len));
  ASSERT_EQ(value_len, strlen("value1") + 1);

  ASSERT_EQ(-ERANGE, rbd_metadata_list(image1, "key", 0, keys, &keys_len, vals,
                                       &vals_len));
  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_metadata_list(image1, "key", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1 + strlen("key2") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1 + strlen("value2") + 1);
  ASSERT_STREQ(keys, "key1");
  ASSERT_STREQ(keys + strlen(keys) + 1, "key2");
  ASSERT_STREQ(vals, "value1");
  ASSERT_STREQ(vals + strlen(vals) + 1, "value2");

  ASSERT_EQ(0, rbd_metadata_remove(image1, "key1"));
  ASSERT_EQ(-ENOENT, rbd_metadata_remove(image1, "key3"));
  value_len = sizeof(value);
  ASSERT_EQ(-ENOENT, rbd_metadata_get(image1, "key3", value, &value_len));
  ASSERT_EQ(0, rbd_metadata_list(image1, "key", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, strlen("key2") + 1);
  ASSERT_EQ(vals_len, strlen("value2") + 1);
  ASSERT_STREQ(keys, "key2");
  ASSERT_STREQ(vals, "value2");

  // test config setting
  ASSERT_EQ(0, rbd_metadata_set(image1, "conf_rbd_cache", "false"));
  ASSERT_EQ(-EINVAL, rbd_metadata_set(image1, "conf_rbd_cache", "INVALID_VAL"));
  ASSERT_EQ(0, rbd_metadata_remove(image1, "conf_rbd_cache"));

  // test metadata with snapshot adding
  ASSERT_EQ(0, rbd_snap_create(image1, "snap1"));
  ASSERT_EQ(0, rbd_snap_protect(image1, "snap1"));
  ASSERT_EQ(0, rbd_snap_set(image1, "snap1"));

  ASSERT_EQ(-EROFS, rbd_metadata_set(image1, "key1", "value1"));
  ASSERT_EQ(-EROFS, rbd_metadata_remove(image1, "key2"));

  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_metadata_list(image1, "key", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, strlen("key2") + 1);
  ASSERT_EQ(vals_len, strlen("value2") + 1);
  ASSERT_STREQ(keys, "key2");
  ASSERT_STREQ(vals, "value2");

  ASSERT_EQ(0, rbd_snap_set(image1, NULL));
  ASSERT_EQ(0, rbd_metadata_set(image1, "key1", "value1"));
  ASSERT_EQ(0, rbd_metadata_set(image1, "key3", "value3"));
  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_metadata_list(image1, "key", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len,
            strlen("key1") + 1 + strlen("key2") + 1 + strlen("key3") + 1);
  ASSERT_EQ(vals_len,
            strlen("value1") + 1 + strlen("value2") + 1 + strlen("value3") + 1);
  ASSERT_STREQ(keys, "key1");
  ASSERT_STREQ(keys + strlen("key1") + 1, "key2");
  ASSERT_STREQ(keys + strlen("key1") + 1 + strlen("key2") + 1, "key3");
  ASSERT_STREQ(vals, "value1");
  ASSERT_STREQ(vals + strlen("value1") + 1, "value2");
  ASSERT_STREQ(vals + strlen("value1") + 1 + strlen("value2") + 1, "value3");

  // test metadata with cloning
  uint64_t features;
  ASSERT_EQ(0, rbd_get_features(image1, &features));

  string cname = get_temp_image_name();
  EXPECT_EQ(0, rbd_clone(ioctx, name.c_str(), "snap1", ioctx,
                         cname.c_str(), features, &order));
  rbd_image_t image2;
  ASSERT_EQ(0, rbd_open(ioctx, cname.c_str(), &image2, NULL));
  ASSERT_EQ(0, rbd_metadata_set(image2, "key4", "value4"));

  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_metadata_list(image2, "key", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1 + strlen("key2") + 1 + strlen("key3") +
            1 + strlen("key4") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1 + strlen("value2") + 1 +
            strlen("value3") + 1 + strlen("value4") + 1);
  ASSERT_STREQ(keys + strlen("key1") + 1 + strlen("key2") + 1 + strlen("key3") +
               1, "key4");
  ASSERT_STREQ(vals + strlen("value1") + 1 + strlen("value2") + 1 +
               strlen("value3") + 1, "value4");

  ASSERT_EQ(0, rbd_metadata_list(image1, "key", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len,
            strlen("key1") + 1 + strlen("key2") + 1 + strlen("key3") + 1);
  ASSERT_EQ(vals_len,
            strlen("value1") + 1 + strlen("value2") + 1 + strlen("value3") + 1);
  ASSERT_EQ(-ENOENT, rbd_metadata_get(image1, "key4", value, &value_len));

  // test short buffer cases
  keys_len = strlen("key1") + 1;
  vals_len = strlen("value1") + 1;
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_metadata_list(image2, "key", 1, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1);
  ASSERT_STREQ(keys, "key1");
  ASSERT_STREQ(vals, "value1");

  ASSERT_EQ(-ERANGE, rbd_metadata_list(image2, "key", 2, keys, &keys_len, vals,
                                       &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1 + strlen("key2") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1 + strlen("value2") + 1);

  ASSERT_EQ(-ERANGE, rbd_metadata_list(image2, "key", 0, keys, &keys_len, vals,
                                       &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1 + strlen("key2") + 1 + strlen("key3") +
            1 + strlen("key4") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1 + strlen("value2") + 1 +
            strlen("value3") + 1 + strlen("value4") + 1);

  // test `start` param
  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_metadata_list(image2, "key2", 0, keys, &keys_len, vals,
                                 &vals_len));
  ASSERT_EQ(keys_len, strlen("key3") + 1 + strlen("key4") + 1);
  ASSERT_EQ(vals_len, strlen("value3") + 1 + strlen("value4") + 1);
  ASSERT_STREQ(keys, "key3");
  ASSERT_STREQ(vals, "value3");

  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_close(image1));
  ASSERT_EQ(0, rbd_close(image2));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, MetadataPP)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  uint64_t features;
  string value;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));
  map<string, bufferlist> pairs;
  ASSERT_EQ(0, image1.metadata_list("key", 0, &pairs));
  ASSERT_TRUE(pairs.empty());

  ASSERT_EQ(0, image1.metadata_set("key1", "value1"));
  ASSERT_EQ(0, image1.metadata_set("key2", "value2"));
  ASSERT_EQ(0, image1.metadata_get("key1", &value));
  ASSERT_EQ(0, strcmp("value1", value.c_str()));
  ASSERT_EQ(0, image1.metadata_list("key", 0, &pairs));
  ASSERT_EQ(2U, pairs.size());
  ASSERT_EQ(0, strncmp("value1", pairs["key1"].c_str(), 6));
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));

  pairs.clear();
  ASSERT_EQ(0, image1.metadata_remove("key1"));
  ASSERT_EQ(-ENOENT, image1.metadata_remove("key3"));
  ASSERT_TRUE(image1.metadata_get("key3", &value) < 0);
  ASSERT_EQ(0, image1.metadata_list("key", 0, &pairs));
  ASSERT_EQ(1U, pairs.size());
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));

  // test config setting
  ASSERT_EQ(0, image1.metadata_set("conf_rbd_cache", "false"));
  ASSERT_EQ(-EINVAL, image1.metadata_set("conf_rbd_cache", "INVALID_VALUE"));
  ASSERT_EQ(0, image1.metadata_remove("conf_rbd_cache"));

  // test metadata with snapshot adding
  ASSERT_EQ(0, image1.snap_create("snap1"));
  ASSERT_EQ(0, image1.snap_protect("snap1"));
  ASSERT_EQ(0, image1.snap_set("snap1"));

  pairs.clear();
  ASSERT_EQ(-EROFS, image1.metadata_set("key1", "value1"));
  ASSERT_EQ(-EROFS, image1.metadata_remove("key2"));
  ASSERT_EQ(0, image1.metadata_list("key", 0, &pairs));
  ASSERT_EQ(1U, pairs.size());
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));

  ASSERT_EQ(0, image1.snap_set(NULL));
  ASSERT_EQ(0, image1.metadata_set("key1", "value1"));
  ASSERT_EQ(0, image1.metadata_set("key3", "value3"));
  ASSERT_EQ(0, image1.metadata_list("key", 0, &pairs));
  ASSERT_EQ(3U, pairs.size());
  ASSERT_EQ(0, strncmp("value1", pairs["key1"].c_str(), 6));
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));
  ASSERT_EQ(0, strncmp("value3", pairs["key3"].c_str(), 6));

  // test metadata with cloning
  string cname = get_temp_image_name();
  librbd::Image image2;
  ASSERT_EQ(0, image1.features(&features));
  EXPECT_EQ(0, rbd.clone(ioctx, name.c_str(), "snap1", ioctx,
                         cname.c_str(), features, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image2, cname.c_str(), NULL));
  ASSERT_EQ(0, image2.metadata_set("key4", "value4"));
  pairs.clear();
  ASSERT_EQ(0, image2.metadata_list("key", 0, &pairs));
  ASSERT_EQ(4U, pairs.size());
  pairs.clear();
  ASSERT_EQ(0, image1.metadata_list("key", 0, &pairs));
  ASSERT_EQ(3U, pairs.size());
  ASSERT_EQ(-ENOENT, image1.metadata_get("key4", &value));
}

TEST_F(TestLibRBD, UpdateFeatures)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  uint8_t old_format;
  ASSERT_EQ(0, image.old_format(&old_format));
  if (old_format) {
    ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_EXCLUSIVE_LOCK, true));
    return;
  }

  uint64_t features;
  ASSERT_EQ(0, image.features(&features));

  // must provide a single feature
  ASSERT_EQ(-EINVAL, image.update_features(0, true));

  uint64_t disable_features;
  disable_features = features & (RBD_FEATURE_EXCLUSIVE_LOCK |
                                 RBD_FEATURE_OBJECT_MAP |
                                 RBD_FEATURE_FAST_DIFF |
                                 RBD_FEATURE_JOURNALING);
  if (disable_features != 0) {
    ASSERT_EQ(0, image.update_features(disable_features, false));
  }

  ASSERT_EQ(0, image.features(&features));
  ASSERT_EQ(0U, features & disable_features);

  // cannot enable object map nor journaling w/o exclusive lock
  ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_OBJECT_MAP, true));
  ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_JOURNALING, true));
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_EXCLUSIVE_LOCK, true));

  ASSERT_EQ(0, image.features(&features));
  ASSERT_NE(0U, features & RBD_FEATURE_EXCLUSIVE_LOCK);

  // can enable fast diff w/o object map
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_FAST_DIFF, true));
  ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_OBJECT_MAP, true));
  ASSERT_EQ(0, image.features(&features));
  ASSERT_NE(0U, features & RBD_FEATURE_OBJECT_MAP);

  uint64_t expected_flags = RBD_FLAG_OBJECT_MAP_INVALID |
	  RBD_FLAG_FAST_DIFF_INVALID;
  uint64_t flags;
  ASSERT_EQ(0, image.get_flags(&flags));
  ASSERT_EQ(expected_flags, flags);

  ASSERT_EQ(0, image.update_features(RBD_FEATURE_OBJECT_MAP, false));
  ASSERT_EQ(0, image.features(&features));
  ASSERT_EQ(0U, features & RBD_FEATURE_OBJECT_MAP);

  // can disable object map w/ fast diff
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_OBJECT_MAP, true));
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_OBJECT_MAP, false));
  ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_FAST_DIFF, false));
  ASSERT_EQ(0, image.features(&features));
  ASSERT_EQ(0U, features & RBD_FEATURE_FAST_DIFF);

  ASSERT_EQ(0, image.get_flags(&flags));
  ASSERT_EQ(0U, flags);

  // cannot disable exclusive lock w/ object map
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_OBJECT_MAP, true));
  ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_EXCLUSIVE_LOCK, false));
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_OBJECT_MAP, false));

  // cannot disable exclusive lock w/ journaling
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_JOURNALING, true));
  ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_EXCLUSIVE_LOCK, false));
  ASSERT_EQ(0, image.update_features(RBD_FEATURE_JOURNALING, false));

  ASSERT_EQ(0, image.get_flags(&flags));
  ASSERT_EQ(0U, flags);

  ASSERT_EQ(0, image.update_features(RBD_FEATURE_EXCLUSIVE_LOCK, false));

  ASSERT_EQ(0, image.features(&features));
  if ((features & RBD_FEATURE_DEEP_FLATTEN) != 0) {
    ASSERT_EQ(0, image.update_features(RBD_FEATURE_DEEP_FLATTEN, false));
  }
  ASSERT_EQ(-EINVAL, image.update_features(RBD_FEATURE_DEEP_FLATTEN, true));
}

TEST_F(TestLibRBD, FeaturesBitmaskString)
{
  librbd::RBD rbd;
  uint64_t features = RBD_FEATURES_DEFAULT;

  std::string features_str;
  std::string expected_str = "deep-flatten,exclusive-lock,fast-diff,layering,object-map";
  rbd.features_to_string(features, &features_str);
  ASSERT_EQ(expected_str, features_str);

  features = RBD_FEATURE_LAYERING;
  features_str = "";
  expected_str = "layering";
  rbd.features_to_string(features, &features_str);
  ASSERT_EQ(expected_str, features_str);

  uint64_t features_bitmask;
  features_str = "deep-flatten,exclusive-lock,fast-diff,layering,object-map";
  rbd.features_from_string(features_str, &features_bitmask);
  ASSERT_EQ(features_bitmask, RBD_FEATURES_DEFAULT);

  features_str = "layering";
  features_bitmask = 0;
  rbd.features_from_string(features_str, &features_bitmask);
  ASSERT_EQ(features_bitmask, RBD_FEATURE_LAYERING);
}

TEST_F(TestLibRBD, RebuildObjectMap)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << 20;
  int order = 18;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  PrintProgress prog_ctx;
  std::string object_map_oid;
  bufferlist bl;
  bl.append("foo");
  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    uint64_t features;
    ASSERT_EQ(0, image.features(&features));
    if ((features & RBD_FEATURE_OBJECT_MAP) == 0) {
      ASSERT_EQ(-EINVAL, image.rebuild_object_map(prog_ctx));
      return;
    }

    ASSERT_EQ((ssize_t)bl.length(), image.write(0, bl.length(), bl));

    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_EQ((ssize_t)bl.length(), image.write(1<<order, bl.length(), bl));

    std::string image_id;
    ASSERT_EQ(0, get_image_id(image, &image_id));
    object_map_oid = RBD_OBJECT_MAP_PREFIX + image_id;
  }

  // corrupt the object map
  ASSERT_EQ(0, ioctx.write(object_map_oid, bl, bl.length(), 0));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  bool lock_owner;
  bl.clear();
  ASSERT_EQ(0, image1.write(0, 0, bl));
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  uint64_t flags;
  ASSERT_EQ(0, image1.get_flags(&flags));
  ASSERT_TRUE((flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0);

  ASSERT_EQ(0, image1.rebuild_object_map(prog_ctx));

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  bufferlist read_bl;
  ASSERT_EQ((ssize_t)bl.length(), image2.read(0, bl.length(), read_bl));
  ASSERT_TRUE(bl.contents_equal(read_bl));

  read_bl.clear();
  ASSERT_EQ((ssize_t)bl.length(), image2.read(1<<order, bl.length(), read_bl));
  ASSERT_TRUE(bl.contents_equal(read_bl));

  ASSERT_PASSED(validate_object_map, image1);
  ASSERT_PASSED(validate_object_map, image2);
}

TEST_F(TestLibRBD, RebuildNewObjectMap)
{
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  std::string name = get_temp_image_name();
  uint64_t size = 1 << 20;
  int order = 18;
  uint64_t features = RBD_FEATURE_EXCLUSIVE_LOCK;
  ASSERT_EQ(0, create_image_full(ioctx, name.c_str(), size, &order,
				 false, features));

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(0, rbd_update_features(image, RBD_FEATURE_OBJECT_MAP, true));
  ASSERT_EQ(0, rbd_rebuild_object_map(image, print_progress_percent, NULL));

  ASSERT_PASSED(validate_object_map, image);

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, CheckObjectMap)
{
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << 20;
  int order = 18;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  PrintProgress prog_ctx;
  bufferlist bl1;
  bufferlist bl2;
  bl1.append("foo");
  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    uint64_t features;
    ASSERT_EQ(0, image.features(&features));

    ASSERT_EQ((ssize_t)bl1.length(), image.write(0, bl1.length(), bl1));

    ASSERT_EQ(0, image.snap_create("snap1"));
    ASSERT_EQ((ssize_t)bl1.length(), image.write(1<<order, bl1.length(), bl1));
  }

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  std::string image_id;
  ASSERT_EQ(0, get_image_id(image1, &image_id));

  std::string object_map_oid = RBD_OBJECT_MAP_PREFIX + image_id;

  ASSERT_LT(0, ioctx.read(object_map_oid, bl2, 1024, 0));

  bool lock_owner;
  ASSERT_EQ((ssize_t)bl1.length(), image1.write(3 * (1 << 18), bl1.length(), bl1));
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  //reopen image to reread now corrupt object map from disk
  image1.close();

  bl1.clear();
  ASSERT_LT(0, ioctx.read(object_map_oid, bl1, 1024, 0));
  ASSERT_FALSE(bl1.contents_equal(bl2));

  ASSERT_EQ(0, ioctx.write_full(object_map_oid, bl2));
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  uint64_t flags;
  ASSERT_EQ(0, image1.get_flags(&flags));
  ASSERT_TRUE((flags & RBD_FLAG_OBJECT_MAP_INVALID) == 0);

  ASSERT_EQ(0, image1.check_object_map(prog_ctx));

  ASSERT_EQ(0, image1.get_flags(&flags));
  ASSERT_TRUE((flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0);
}

TEST_F(TestLibRBD, BlockingAIO)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << 20;
  int order = 18;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  std::string non_blocking_aio;
  ASSERT_EQ(0, _rados.conf_get("rbd_non_blocking_aio", non_blocking_aio));
  ASSERT_EQ(0, _rados.conf_set("rbd_non_blocking_aio", "0"));
  BOOST_SCOPE_EXIT( (non_blocking_aio) ) {
    ASSERT_EQ(0, _rados.conf_set("rbd_non_blocking_aio",
                                 non_blocking_aio.c_str()));
  } BOOST_SCOPE_EXIT_END;

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  bool skip_discard = this->is_skip_partial_discard_enabled(image);

  bufferlist bl;
  ASSERT_EQ(0, image.write(0, bl.length(), bl));

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
  ASSERT_EQ((ssize_t)bl.length(), read_comp->get_return_value());
  read_comp->release();

  bufferlist expected_bl;
  expected_bl.append(std::string(128, '1'));
  expected_bl.append(std::string(128, skip_discard ? '1' : '\0'));
  ASSERT_TRUE(expected_bl.contents_equal(read_bl));
}

TEST_F(TestLibRBD, ExclusiveLockTransition)
{
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  std::list<librbd::RBD::AioCompletion *> comps;
  ceph::bufferlist bl;
  bl.append(std::string(1 << order, '1'));
  for (size_t object_no = 0; object_no < (size >> 12); ++object_no) {
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL,
                                                                      NULL);
    comps.push_back(comp);
    if (object_no % 2 == 0) {
      ASSERT_EQ(0, image1.aio_write(object_no << order, bl.length(), bl, comp));
    } else {
      ASSERT_EQ(0, image2.aio_write(object_no << order, bl.length(), bl, comp));
    }
  }

  while (!comps.empty()) {
    librbd::RBD::AioCompletion *comp = comps.front();
    comps.pop_front();
    ASSERT_EQ(0, comp->wait_for_complete());
    ASSERT_EQ(1, comp->is_complete());
    comp->release();
  }

  librbd::Image image3;
  ASSERT_EQ(0, rbd.open(ioctx, image3, name.c_str(), NULL));
  for (size_t object_no = 0; object_no < (size >> 12); ++object_no) {
    bufferlist read_bl;
    ASSERT_EQ((ssize_t)bl.length(), image3.read(object_no << order, bl.length(),
						read_bl));
    ASSERT_TRUE(bl.contents_equal(read_bl));
  }

  ASSERT_PASSED(validate_object_map, image1);
  ASSERT_PASSED(validate_object_map, image2);
  ASSERT_PASSED(validate_object_map, image3);
}

TEST_F(TestLibRBD, ExclusiveLockReadTransition)
{
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));

  bool lock_owner;
  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_FALSE(lock_owner);

  // journaling should force read ops to acquire the lock
  bufferlist read_bl;
  ASSERT_EQ(0, image1.read(0, 0, read_bl));

  ASSERT_EQ(0, image1.is_exclusive_lock_owner(&lock_owner));
  ASSERT_TRUE(lock_owner);

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

  std::list<librbd::RBD::AioCompletion *> comps;
  std::list<bufferlist> read_bls;
  for (size_t object_no = 0; object_no < (size >> 12); ++object_no) {
    librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL,
                                                                      NULL);
    comps.push_back(comp);
    read_bls.emplace_back();
    if (object_no % 2 == 0) {
      ASSERT_EQ(0, image1.aio_read(object_no << order, 1 << order, read_bls.back(), comp));
    } else {
      ASSERT_EQ(0, image2.aio_read(object_no << order, 1 << order, read_bls.back(), comp));
    }
  }

  while (!comps.empty()) {
    librbd::RBD::AioCompletion *comp = comps.front();
    comps.pop_front();
    ASSERT_EQ(0, comp->wait_for_complete());
    ASSERT_EQ(1, comp->is_complete());
    comp->release();
  }
}

TEST_F(TestLibRBD, CacheMayCopyOnWrite) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));

  std::string clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "one", ioctx, clone_name.c_str(),
                         RBD_FEATURE_LAYERING, &order));

  librbd::Image clone;
  ASSERT_EQ(0, rbd.open(ioctx, clone, clone_name.c_str(), NULL));
  ASSERT_EQ(0, clone.flush());

  bufferlist expect_bl;
  expect_bl.append(std::string(1024, '\0'));

  // test double read path
  bufferlist read_bl;
  uint64_t offset = 0;
  ASSERT_EQ(1024, clone.read(offset + 2048, 1024, read_bl));
  ASSERT_TRUE(expect_bl.contents_equal(read_bl));

  bufferlist write_bl;
  write_bl.append(std::string(1024, '1'));
  ASSERT_EQ(1024, clone.write(offset, write_bl.length(), write_bl));

  read_bl.clear();
  ASSERT_EQ(1024, clone.read(offset + 2048, 1024, read_bl));
  ASSERT_TRUE(expect_bl.contents_equal(read_bl));

  // test read retry path
  offset = 1 << order;
  ASSERT_EQ(1024, clone.write(offset, write_bl.length(), write_bl));

  read_bl.clear();
  ASSERT_EQ(1024, clone.read(offset + 2048, 1024, read_bl));
  ASSERT_TRUE(expect_bl.contents_equal(read_bl));
}

TEST_F(TestLibRBD, FlushEmptyOpsOnExternalSnapshot) {
  std::string cache_enabled;
  ASSERT_EQ(0, _rados.conf_get("rbd_cache", cache_enabled));
  ASSERT_EQ(0, _rados.conf_set("rbd_cache", "false"));
  BOOST_SCOPE_EXIT( (cache_enabled) ) {
    ASSERT_EQ(0, _rados.conf_set("rbd_cache", cache_enabled.c_str()));
  } BOOST_SCOPE_EXIT_END;

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << 18;
  int order = 0;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image1;
  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));
  ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));
  ASSERT_EQ(0, image1.snap_create("snap1"));

  librbd::RBD::AioCompletion *read_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  bufferlist read_bl;
  image2.aio_read(0, 1024, read_bl, read_comp);
  ASSERT_EQ(0, read_comp->wait_for_complete());
  read_comp->release();
}

TEST_F(TestLibRBD, TestImageOptions)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  //make create image options
  uint64_t features = RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2 ;
  uint64_t order = 0;
  uint64_t stripe_unit = IMAGE_STRIPE_UNIT;
  uint64_t stripe_count = IMAGE_STRIPE_COUNT;
  rbd_image_options_t opts;
  rbd_image_options_create(&opts);

  bool is_set;
  ASSERT_EQ(-EINVAL, rbd_image_options_is_set(opts, 12345, &is_set));
  ASSERT_EQ(0, rbd_image_options_is_set(opts, RBD_IMAGE_OPTION_FORMAT,
                                        &is_set));
  ASSERT_FALSE(is_set);

  ASSERT_EQ(0, rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FORMAT,
	  2));
  ASSERT_EQ(0, rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_FEATURES,
	  features));
  ASSERT_EQ(0, rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_ORDER,
	  order));
  ASSERT_EQ(0, rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_UNIT,
	  stripe_unit));
  ASSERT_EQ(0, rbd_image_options_set_uint64(opts, RBD_IMAGE_OPTION_STRIPE_COUNT,
	  stripe_count));

  ASSERT_EQ(0, rbd_image_options_is_set(opts, RBD_IMAGE_OPTION_FORMAT,
                                        &is_set));
  ASSERT_TRUE(is_set);

  std::string parent_name = get_temp_image_name();

  // make parent
  ASSERT_EQ(0, rbd_create4(ioctx, parent_name.c_str(), 4<<20, opts));

  // check order is returned in opts
  ASSERT_EQ(0, rbd_image_options_get_uint64(opts, RBD_IMAGE_OPTION_ORDER,
	  &order));
  ASSERT_NE((uint64_t)0, order);

  // write some data to parent
  rbd_image_t parent;
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, NULL));
  char *data = (char *)"testdata";
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 0, strlen(data), data));
  ASSERT_EQ((ssize_t)strlen(data), rbd_write(parent, 12, strlen(data), data));

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, rbd_snap_create(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_close(parent));
  ASSERT_EQ(0, rbd_open(ioctx, parent_name.c_str(), &parent, "parent_snap"));

  // clone
  std::string child_name = get_temp_image_name();
  ASSERT_EQ(0, rbd_snap_protect(parent, "parent_snap"));
  ASSERT_EQ(0, rbd_clone3(ioctx, parent_name.c_str(), "parent_snap", ioctx,
	  child_name.c_str(), opts));

  // copy
  std::string copy1_name = get_temp_image_name();
  ASSERT_EQ(0, rbd_copy3(parent, ioctx, copy1_name.c_str(), opts));
  std::string copy2_name = get_temp_image_name();
  ASSERT_EQ(0, rbd_copy_with_progress3(parent, ioctx, copy2_name.c_str(), opts,
	  print_progress_percent, NULL));

  ASSERT_EQ(0, rbd_close(parent));

  rbd_image_options_destroy(opts);

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestImageOptionsPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  //make create image options
  uint64_t features = RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2 ;
  uint64_t order = 0;
  uint64_t stripe_unit = IMAGE_STRIPE_UNIT;
  uint64_t stripe_count = IMAGE_STRIPE_COUNT;
  librbd::ImageOptions opts;
  ASSERT_EQ(0, opts.set(RBD_IMAGE_OPTION_FORMAT, static_cast<uint64_t>(2)));
  ASSERT_EQ(0, opts.set(RBD_IMAGE_OPTION_FEATURES, features));
  ASSERT_EQ(0, opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  librbd::RBD rbd;
  std::string parent_name = get_temp_image_name();

  // make parent
  ASSERT_EQ(0, rbd.create4(ioctx, parent_name.c_str(), 4<<20, opts));

  // check order is returned in opts
  ASSERT_EQ(0, opts.get(RBD_IMAGE_OPTION_ORDER, &order));
  ASSERT_NE((uint64_t)0, order);

  // write some data to parent
  librbd::Image parent;
  ASSERT_EQ(0, rbd.open(ioctx, parent, parent_name.c_str(), NULL));

  ssize_t len = 1024;
  bufferlist bl;
  bl.append(buffer::create(len));
  bl.zero();
  ASSERT_EQ(len, parent.write(0, len, bl));
  ASSERT_EQ(len, parent.write(len, len, bl));

  // create a snapshot, reopen as the parent we're interested in
  ASSERT_EQ(0, parent.snap_create("parent_snap"));
  ASSERT_EQ(0, parent.close());
  ASSERT_EQ(0, rbd.open(ioctx, parent, parent_name.c_str(), "parent_snap"));

  // clone
  std::string child_name = get_temp_image_name();
  ASSERT_EQ(0, parent.snap_protect("parent_snap"));
  ASSERT_EQ(0, rbd.clone3(ioctx, parent_name.c_str(), "parent_snap", ioctx,
	  child_name.c_str(), opts));

  // copy
  std::string copy1_name = get_temp_image_name();
  ASSERT_EQ(0, parent.copy3(ioctx, copy1_name.c_str(), opts));
  std::string copy2_name = get_temp_image_name();
  PrintProgress pp;
  ASSERT_EQ(0, parent.copy_with_progress3(ioctx, copy2_name.c_str(), opts, pp));

  ASSERT_EQ(0, parent.close());
}

TEST_F(TestLibRBD, EventSocketPipe)
{
  EventSocket event_sock;
  int pipe_fd[2]; // read and write fd
  char buf[32];

  ASSERT_EQ(0, pipe(pipe_fd));

  ASSERT_FALSE(event_sock.is_valid());

  ASSERT_EQ(-EINVAL, event_sock.init(pipe_fd[1], EVENT_SOCKET_TYPE_NONE));
  ASSERT_FALSE(event_sock.is_valid());

  ASSERT_EQ(-EINVAL, event_sock.init(pipe_fd[1], 44));
  ASSERT_FALSE(event_sock.is_valid());

#ifndef HAVE_EVENTFD
  ASSERT_EQ(-EINVAL, event_sock.init(pipe_fd[1], EVENT_SOCKET_TYPE_EVENTFD));
  ASSERT_FALSE(event_sock.is_valid());
#endif

  ASSERT_EQ(0, event_sock.init(pipe_fd[1], EVENT_SOCKET_TYPE_PIPE));
  ASSERT_TRUE(event_sock.is_valid());
  ASSERT_EQ(0, event_sock.notify());
  ASSERT_EQ(1, read(pipe_fd[0], buf, 32));
  ASSERT_EQ('i', buf[0]);

  close(pipe_fd[0]);
  close(pipe_fd[1]);
}

TEST_F(TestLibRBD, EventSocketEventfd)
{
#ifdef HAVE_EVENTFD
  EventSocket event_sock;
  int event_fd;
  struct pollfd poll_fd;
  char buf[32];

  event_fd = eventfd(0, EFD_NONBLOCK);
  ASSERT_NE(-1, event_fd);

  ASSERT_FALSE(event_sock.is_valid());

  ASSERT_EQ(-EINVAL, event_sock.init(event_fd, EVENT_SOCKET_TYPE_NONE));
  ASSERT_FALSE(event_sock.is_valid());

  ASSERT_EQ(-EINVAL, event_sock.init(event_fd, 44));
  ASSERT_FALSE(event_sock.is_valid());

  ASSERT_EQ(0, event_sock.init(event_fd, EVENT_SOCKET_TYPE_EVENTFD));
  ASSERT_TRUE(event_sock.is_valid());
  ASSERT_EQ(0, event_sock.notify());

  poll_fd.fd = event_fd;
  poll_fd.events = POLLIN;
  ASSERT_EQ(1, poll(&poll_fd, 1, -1));
  ASSERT_TRUE(poll_fd.revents & POLLIN);

  ASSERT_EQ(static_cast<ssize_t>(sizeof(uint64_t)), read(event_fd, buf, 32));
  ASSERT_EQ(1U, *reinterpret_cast<uint64_t *>(buf));

  close(event_fd);
#endif
}

TEST_F(TestLibRBD, ImagePollIO)
{
#ifdef HAVE_EVENTFD
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int fd = eventfd(0, EFD_NONBLOCK);

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(0, rbd_set_image_notification(image, fd, EVENT_SOCKET_TYPE_EVENTFD));

  char test_data[TEST_IO_SIZE + 1];
  char zero_data[TEST_IO_SIZE + 1];
  int i;

  for (i = 0; i < TEST_IO_SIZE; ++i)
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  test_data[TEST_IO_SIZE] = '\0';
  memset(zero_data, 0, sizeof(zero_data));

  for (i = 0; i < 5; ++i)
    ASSERT_PASSED(write_test_data, image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_write_test_data_and_poll, image, fd, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  for (i = 5; i < 10; ++i)
    ASSERT_PASSED(aio_read_test_data_and_poll, image, fd, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE, 0);

  ASSERT_EQ(0, rbd_close(image));
  rados_ioctx_destroy(ioctx);
#endif
}

namespace librbd {

static bool operator==(const image_spec_t &lhs, const image_spec_t &rhs) {
  return (lhs.id == rhs.id && lhs.name == rhs.name);
}

static bool operator==(const linked_image_spec_t &lhs,
                       const linked_image_spec_t &rhs) {
  return (lhs.pool_id == rhs.pool_id &&
          lhs.pool_name == rhs.pool_name &&
          lhs.pool_namespace == rhs.pool_namespace &&
          lhs.image_id == rhs.image_id &&
          lhs.image_name == rhs.image_name &&
          lhs.trash == rhs.trash);
}

static bool operator==(const mirror_peer_t &lhs, const mirror_peer_t &rhs) {
  return (lhs.uuid == rhs.uuid &&
          lhs.cluster_name == rhs.cluster_name &&
          lhs.client_name == rhs.client_name);
}

static std::ostream& operator<<(std::ostream &os, const mirror_peer_t &peer) {
  os << "uuid=" << peer.uuid << ", "
     << "cluster=" << peer.cluster_name << ", "
     << "client=" << peer.client_name;
  return os;
}

} // namespace librbd

TEST_F(TestLibRBD, Mirror) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;

  std::vector<librbd::mirror_peer_t> expected_peers;
  std::vector<librbd::mirror_peer_t> peers;
  ASSERT_EQ(0, rbd.mirror_peer_list(ioctx, &peers));
  ASSERT_EQ(expected_peers, peers);

  std::string uuid1;
  ASSERT_EQ(-EINVAL, rbd.mirror_peer_add(ioctx, &uuid1, "cluster1", "client"));

  rbd_mirror_mode_t mirror_mode;
  ASSERT_EQ(0, rbd.mirror_mode_get(ioctx, &mirror_mode));
  ASSERT_EQ(RBD_MIRROR_MODE_DISABLED, mirror_mode);

  ASSERT_EQ(0, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_IMAGE));
  ASSERT_EQ(0, rbd.mirror_mode_get(ioctx, &mirror_mode));

  // Add some images to the pool
  int order = 0;
  std::string parent_name = get_temp_image_name();
  std::string child_name = get_temp_image_name();
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, parent_name.c_str(), 2 << 20,
                               &order));
  bool old_format;
  uint64_t features;
  ASSERT_EQ(0, get_features(&old_format, &features));
  if ((features & RBD_FEATURE_LAYERING) != 0) {
    librbd::Image parent;
    ASSERT_EQ(0, rbd.open(ioctx, parent, parent_name.c_str(), NULL));
    ASSERT_EQ(0, parent.snap_create("parent_snap"));
    ASSERT_EQ(0, parent.close());
    ASSERT_EQ(0, rbd.open(ioctx, parent, parent_name.c_str(), "parent_snap"));
    ASSERT_EQ(0, parent.snap_protect("parent_snap"));
    ASSERT_EQ(0, parent.close());
    ASSERT_EQ(0, rbd.clone(ioctx, parent_name.c_str(), "parent_snap", ioctx,
                           child_name.c_str(), features, &order));
  }

  ASSERT_EQ(RBD_MIRROR_MODE_IMAGE, mirror_mode);

  ASSERT_EQ(0, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_POOL));
  ASSERT_EQ(0, rbd.mirror_mode_get(ioctx, &mirror_mode));
  ASSERT_EQ(RBD_MIRROR_MODE_POOL, mirror_mode);

  std::string uuid2;
  std::string uuid3;
  ASSERT_EQ(0, rbd.mirror_peer_add(ioctx, &uuid1, "cluster1", "client"));
  ASSERT_EQ(0, rbd.mirror_peer_add(ioctx, &uuid2, "cluster2", "admin"));
  ASSERT_EQ(-EEXIST, rbd.mirror_peer_add(ioctx, &uuid3, "cluster1", "foo"));
  ASSERT_EQ(0, rbd.mirror_peer_add(ioctx, &uuid3, "cluster3", "admin"));

  ASSERT_EQ(0, rbd.mirror_peer_list(ioctx, &peers));
  auto sort_peers = [](const librbd::mirror_peer_t &lhs,
                         const librbd::mirror_peer_t &rhs) {
      return lhs.uuid < rhs.uuid;
    };
  expected_peers = {
    {uuid1, "cluster1", "client"},
    {uuid2, "cluster2", "admin"},
    {uuid3, "cluster3", "admin"}};
  std::sort(expected_peers.begin(), expected_peers.end(), sort_peers);
  ASSERT_EQ(expected_peers, peers);

  ASSERT_EQ(0, rbd.mirror_peer_remove(ioctx, "uuid4"));
  ASSERT_EQ(0, rbd.mirror_peer_remove(ioctx, uuid2));

  ASSERT_EQ(-ENOENT, rbd.mirror_peer_set_client(ioctx, "uuid4", "new client"));
  ASSERT_EQ(0, rbd.mirror_peer_set_client(ioctx, uuid1, "new client"));

  ASSERT_EQ(-ENOENT, rbd.mirror_peer_set_cluster(ioctx, "uuid4",
                                                 "new cluster"));
  ASSERT_EQ(0, rbd.mirror_peer_set_cluster(ioctx, uuid3, "new cluster"));

  ASSERT_EQ(0, rbd.mirror_peer_list(ioctx, &peers));
  expected_peers = {
    {uuid1, "cluster1", "new client"},
    {uuid3, "new cluster", "admin"}};
  std::sort(expected_peers.begin(), expected_peers.end(), sort_peers);
  ASSERT_EQ(expected_peers, peers);

  ASSERT_EQ(-EBUSY, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_DISABLED));
  ASSERT_EQ(0, rbd.mirror_peer_remove(ioctx, uuid1));
  ASSERT_EQ(0, rbd.mirror_peer_remove(ioctx, uuid3));
  ASSERT_EQ(0, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestLibRBD, MirrorPeerAttributes) {
  REQUIRE(!is_librados_test_stub(_rados));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_IMAGE));

  std::string uuid;
  ASSERT_EQ(0, rbd.mirror_peer_add(ioctx, &uuid, "remote_cluster", "client"));

  std::map<std::string, std::string> attributes;
  ASSERT_EQ(-ENOENT, rbd.mirror_peer_get_attributes(ioctx, uuid, &attributes));
  ASSERT_EQ(-ENOENT, rbd.mirror_peer_set_attributes(ioctx, "missing uuid",
                                                    attributes));

  std::map<std::string, std::string> expected_attributes{
    {"mon_host", "1.2.3.4"},
    {"key", "ABC"}};
  ASSERT_EQ(0, rbd.mirror_peer_set_attributes(ioctx, uuid,
                                              expected_attributes));

  ASSERT_EQ(0, rbd.mirror_peer_get_attributes(ioctx, uuid,
                                              &attributes));
  ASSERT_EQ(expected_attributes, attributes);

  ASSERT_EQ(0, rbd.mirror_peer_remove(ioctx, uuid));
  ASSERT_EQ(0, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestLibRBD, CreateWithMirrorEnabled) {
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_IMAGE));

  librbd::ImageOptions image_options;
  ASSERT_EQ(0, image_options.set(
                 RBD_IMAGE_OPTION_MIRROR_IMAGE_MODE,
                 static_cast<uint64_t>(RBD_MIRROR_IMAGE_MODE_SNAPSHOT)));

  std::string parent_name = get_temp_image_name();
  ASSERT_EQ(0, rbd.create4(ioctx, parent_name.c_str(), 2<<20, image_options));

  librbd::Image parent_image;
  ASSERT_EQ(0, rbd.open(ioctx, parent_image, parent_name.c_str(), NULL));

  librbd::mirror_image_mode_t mirror_image_mode;
  ASSERT_EQ(0, parent_image.mirror_image_get_mode(&mirror_image_mode));
  ASSERT_EQ(RBD_MIRROR_IMAGE_MODE_SNAPSHOT, mirror_image_mode);

  ASSERT_EQ(0, parent_image.snap_create("parent_snap"));
  ASSERT_EQ(0, parent_image.snap_protect("parent_snap"));

  std::string child_name = get_temp_image_name();
  ASSERT_EQ(0, rbd.clone3(ioctx, parent_name.c_str(), "parent_snap", ioctx,
            child_name.c_str(), image_options));

  librbd::Image child_image;
  ASSERT_EQ(0, rbd.open(ioctx, child_image, child_name.c_str(), NULL));

  ASSERT_EQ(0, child_image.mirror_image_get_mode(&mirror_image_mode));
  ASSERT_EQ(RBD_MIRROR_IMAGE_MODE_SNAPSHOT, mirror_image_mode);

  ASSERT_EQ(0, child_image.mirror_image_disable(true));
  ASSERT_EQ(0, parent_image.mirror_image_disable(true));
  ASSERT_EQ(0, rbd.mirror_mode_set(ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestLibRBD, FlushCacheWithCopyupOnExternalSnapshot) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 0;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  bufferlist bl;
  bl.append(std::string(size, '1'));
  ASSERT_EQ((int)size, image.write(0, size, bl));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));

  std::string clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "one", ioctx, clone_name.c_str(),
                         RBD_FEATURE_LAYERING, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, clone_name.c_str(), NULL));

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, clone_name.c_str(), NULL));

  // prepare CoW writeback that will be flushed on next op
  bl.clear();
  bl.append(std::string(1, '1'));
  ASSERT_EQ(0, image.flush());
  ASSERT_EQ(1, image.write(0, 1, bl));
  ASSERT_EQ(0, image2.snap_create("snap1"));

  librbd::RBD::AioCompletion *read_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  bufferlist read_bl;
  image.aio_read(0, 1024, read_bl, read_comp);
  ASSERT_EQ(0, read_comp->wait_for_complete());
  read_comp->release();
}

TEST_F(TestLibRBD, ExclusiveLock)
{
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  static char buf[10];

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));

  rbd_image_t image1;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image1, NULL));

  int lock_owner;
  ASSERT_EQ(0, rbd_lock_acquire(image1, RBD_LOCK_MODE_EXCLUSIVE));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image1, &lock_owner));
  ASSERT_TRUE(lock_owner);

  rbd_lock_mode_t lock_mode;
  char *lock_owners[1];
  size_t max_lock_owners = 0;
  ASSERT_EQ(-ERANGE, rbd_lock_get_owners(image1, &lock_mode, lock_owners,
                                         &max_lock_owners));
  ASSERT_EQ(1U, max_lock_owners);

  ASSERT_EQ(0, rbd_lock_get_owners(image1, &lock_mode, lock_owners,
                                   &max_lock_owners));
  ASSERT_EQ(RBD_LOCK_MODE_EXCLUSIVE, lock_mode);
  ASSERT_STRNE("", lock_owners[0]);
  ASSERT_EQ(1U, max_lock_owners);

  rbd_image_t image2;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image2, NULL));

  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image2, &lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(-EOPNOTSUPP, rbd_lock_break(image1, RBD_LOCK_MODE_SHARED, ""));
  ASSERT_EQ(-EBUSY, rbd_lock_break(image1, RBD_LOCK_MODE_EXCLUSIVE,
                                   "not the owner"));

  ASSERT_EQ(0, rbd_lock_release(image1));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image1, &lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(-ENOENT, rbd_lock_break(image1, RBD_LOCK_MODE_EXCLUSIVE,
                                    lock_owners[0]));
  rbd_lock_get_owners_cleanup(lock_owners, max_lock_owners);

  ASSERT_EQ(-EROFS, rbd_write(image1, 0, sizeof(buf), buf));
  ASSERT_EQ((ssize_t)sizeof(buf), rbd_write(image2, 0, sizeof(buf), buf));

  ASSERT_EQ(0, rbd_lock_acquire(image2, RBD_LOCK_MODE_EXCLUSIVE));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image2, &lock_owner));
  ASSERT_TRUE(lock_owner);

  ASSERT_EQ(0, rbd_lock_release(image2));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image2, &lock_owner));
  ASSERT_FALSE(lock_owner);

  ASSERT_EQ(0, rbd_lock_acquire(image1, RBD_LOCK_MODE_EXCLUSIVE));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image1, &lock_owner));
  ASSERT_TRUE(lock_owner);

  ASSERT_EQ((ssize_t)sizeof(buf), rbd_write(image1, 0, sizeof(buf), buf));
  ASSERT_EQ(-EROFS, rbd_write(image2, 0, sizeof(buf), buf));

  ASSERT_EQ(0, rbd_lock_release(image1));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image1, &lock_owner));
  ASSERT_FALSE(lock_owner);

  int owner_id = -1;
  std::mutex lock;
  const auto pingpong = [&](int m_id, rbd_image_t &m_image) {
      for (int i = 0; i < 10; i++) {
	{
	  std::lock_guard<std::mutex> locker(lock);
	  if (owner_id == m_id) {
	    std::cout << m_id << ": releasing exclusive lock" << std::endl;
	    EXPECT_EQ(0, rbd_lock_release(m_image));
	    int lock_owner;
	    EXPECT_EQ(0, rbd_is_exclusive_lock_owner(m_image, &lock_owner));
	    EXPECT_FALSE(lock_owner);
	    owner_id = -1;
	    std::cout << m_id << ": exclusive lock released" << std::endl;
	    continue;
	  }
	}

	std::cout << m_id << ": acquiring exclusive lock" << std::endl;
        int r;
        do {
          r = rbd_lock_acquire(m_image, RBD_LOCK_MODE_EXCLUSIVE);
          if (r == -EROFS) {
            usleep(1000);
          }
        } while (r == -EROFS);
	EXPECT_EQ(0, r);

	int lock_owner;
	EXPECT_EQ(0, rbd_is_exclusive_lock_owner(m_image, &lock_owner));
	EXPECT_TRUE(lock_owner);
	std::cout << m_id << ": exclusive lock acquired" << std::endl;
	{
	  std::lock_guard<std::mutex> locker(lock);
	  owner_id = m_id;
	}
	usleep(rand() % 50000);
      }

      std::lock_guard<std::mutex> locker(lock);
      if (owner_id == m_id) {
	EXPECT_EQ(0, rbd_lock_release(m_image));
	int lock_owner;
	EXPECT_EQ(0, rbd_is_exclusive_lock_owner(m_image, &lock_owner));
	EXPECT_FALSE(lock_owner);
	owner_id = -1;
      }
  };
  thread ping(bind(pingpong, 1, ref(image1)));
  thread pong(bind(pingpong, 2, ref(image2)));

  ping.join();
  pong.join();

  ASSERT_EQ(0, rbd_lock_acquire(image2, RBD_LOCK_MODE_EXCLUSIVE));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image2, &lock_owner));
  ASSERT_TRUE(lock_owner);

  ASSERT_EQ(0, rbd_close(image2));

  ASSERT_EQ(0, rbd_lock_acquire(image1, RBD_LOCK_MODE_EXCLUSIVE));
  ASSERT_EQ(0, rbd_is_exclusive_lock_owner(image1, &lock_owner));
  ASSERT_TRUE(lock_owner);

  ASSERT_EQ(0, rbd_close(image1));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, BreakLock)
{
  SKIP_IF_CRIMSON();
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);
  REQUIRE(!is_rbd_pwl_enabled((CephContext *)_rados.cct()));

  static char buf[10];

  rados_t blocklist_cluster;
  ASSERT_EQ("", connect_cluster(&blocklist_cluster));

  rados_ioctx_t ioctx, blocklist_ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));
  ASSERT_EQ(0, rados_ioctx_create(blocklist_cluster, m_pool_name.c_str(),
                                  &blocklist_ioctx));

  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  int order = 0;
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));

  rbd_image_t image, blocklist_image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(0, rbd_open(blocklist_ioctx, name.c_str(), &blocklist_image, NULL));

  ASSERT_EQ(0, rbd_metadata_set(image, "conf_rbd_blocklist_on_break_lock", "true"));
  ASSERT_EQ(0, rbd_lock_acquire(blocklist_image, RBD_LOCK_MODE_EXCLUSIVE));

  rbd_lock_mode_t lock_mode;
  char *lock_owners[1];
  size_t max_lock_owners = 1;
  ASSERT_EQ(0, rbd_lock_get_owners(image, &lock_mode, lock_owners,
                                   &max_lock_owners));
  ASSERT_EQ(RBD_LOCK_MODE_EXCLUSIVE, lock_mode);
  ASSERT_STRNE("", lock_owners[0]);
  ASSERT_EQ(1U, max_lock_owners);

  ASSERT_EQ(0, rbd_lock_break(image, RBD_LOCK_MODE_EXCLUSIVE, lock_owners[0]));
  ASSERT_EQ(0, rbd_lock_acquire(image, RBD_LOCK_MODE_EXCLUSIVE));
  EXPECT_EQ(0, rados_wait_for_latest_osdmap(blocklist_cluster));

  ASSERT_EQ((ssize_t)sizeof(buf), rbd_write(image, 0, sizeof(buf), buf));
  ASSERT_EQ(-EBLOCKLISTED, rbd_write(blocklist_image, 0, sizeof(buf), buf));

  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_close(blocklist_image));

  rbd_lock_get_owners_cleanup(lock_owners, max_lock_owners);

  rados_ioctx_destroy(ioctx);
  rados_ioctx_destroy(blocklist_ioctx);
  rados_shutdown(blocklist_cluster);
}

TEST_F(TestLibRBD, DiscardAfterWrite)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();
  uint64_t size = 1 << 20;
  int order = 18;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  if (this->is_skip_partial_discard_enabled(image)) {
    return;
  }

  // enable writeback cache
  ASSERT_EQ(0, image.flush());

  bufferlist bl;
  bl.append(std::string(256, '1'));

  librbd::RBD::AioCompletion *write_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, image.aio_write(0, bl.length(), bl, write_comp));
  ASSERT_EQ(0, write_comp->wait_for_complete());
  write_comp->release();

  librbd::RBD::AioCompletion *discard_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  ASSERT_EQ(0, image.aio_discard(0, 256, discard_comp));
  ASSERT_EQ(0, discard_comp->wait_for_complete());
  discard_comp->release();

  librbd::RBD::AioCompletion *read_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);
  bufferlist read_bl;
  image.aio_read(0, bl.length(), read_bl, read_comp);
  ASSERT_EQ(0, read_comp->wait_for_complete());
  ASSERT_EQ(bl.length(), read_comp->get_return_value());
  ASSERT_TRUE(read_bl.is_zero());
  read_comp->release();
}

TEST_F(TestLibRBD, DefaultFeatures) {
  std::string orig_default_features;
  ASSERT_EQ(0, _rados.conf_get("rbd_default_features", orig_default_features));
  BOOST_SCOPE_EXIT_ALL(orig_default_features) {
    ASSERT_EQ(0, _rados.conf_set("rbd_default_features",
                                 orig_default_features.c_str()));
  };

  std::list<std::pair<std::string, std::string> > feature_names_to_bitmask = {
    {"", orig_default_features},
    {"layering", "1"},
    {"layering, exclusive-lock", "5"},
    {"exclusive-lock,journaling", "68"},
    {"125", "125"}
  };

  for (auto &pair : feature_names_to_bitmask) {
    ASSERT_EQ(0, _rados.conf_set("rbd_default_features", pair.first.c_str()));
    std::string features;
    ASSERT_EQ(0, _rados.conf_get("rbd_default_features", features));
    ASSERT_EQ(pair.second, features);
  }
}

TEST_F(TestLibRBD, TestTrashMoveAndPurge) {
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

  std::string image_id;
  ASSERT_EQ(0, image.get_id(&image_id));
  image.close();

  ASSERT_EQ(0, rbd.trash_move(ioctx, name.c_str(), 0));

  std::vector<std::string> images;
  ASSERT_EQ(0, rbd.list(ioctx, images));
  for (const auto& image : images) {
    ASSERT_TRUE(image != name);
  }

  librbd::trash_image_info_t info;
  ASSERT_EQ(-ENOENT, rbd.trash_get(ioctx, "dummy image id", &info));
  ASSERT_EQ(0, rbd.trash_get(ioctx, image_id.c_str(), &info));
  ASSERT_EQ(image_id, info.id);

  std::vector<librbd::trash_image_info_t> entries;
  ASSERT_EQ(0, rbd.trash_list(ioctx, entries));
  ASSERT_FALSE(entries.empty());
  ASSERT_EQ(entries.begin()->id, image_id);

  entries.clear();
  PrintProgress pp;
  ASSERT_EQ(0, rbd.trash_remove_with_progress(ioctx, image_id.c_str(),
                                              false, pp));
  ASSERT_EQ(0, rbd.trash_list(ioctx, entries));
  ASSERT_TRUE(entries.empty());
}

TEST_F(TestLibRBD, TestTrashMoveAndPurgeNonExpiredDelay) {
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

  std::string image_id;
  ASSERT_EQ(0, image.get_id(&image_id));
  image.close();

  ASSERT_EQ(0, rbd.trash_move(ioctx, name.c_str(), 100));

  PrintProgress pp;
  ASSERT_EQ(-EPERM, rbd.trash_remove_with_progress(ioctx, image_id.c_str(),
                                                   false, pp));

  PrintProgress pp2;
  ASSERT_EQ(0, rbd.trash_remove_with_progress(ioctx, image_id.c_str(),
                                              true, pp2));
}

TEST_F(TestLibRBD, TestTrashPurge) {
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name1 = get_temp_image_name();
  std::string name2 = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name1.c_str(), size, &order));
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name2.c_str(), size, &order));

  librbd::Image image1;
  ASSERT_EQ(0, rbd.open(ioctx, image1, name1.c_str(), nullptr));

  std::string image_id1;
  ASSERT_EQ(0, image1.get_id(&image_id1));
  image1.close();

  ASSERT_EQ(0, rbd.trash_move(ioctx, name1.c_str(), 0));

  librbd::Image image2;
  ASSERT_EQ(0, rbd.open(ioctx, image2, name2.c_str(), nullptr));
  ceph::bufferlist bl;
  bl.append(std::string(1024, '0'));
  ASSERT_EQ(1024, image2.write(0, 1024, bl));

  std::string image_id2;
  ASSERT_EQ(0, image2.get_id(&image_id2));
  image2.close();

  ASSERT_EQ(0, rbd.trash_move(ioctx, name2.c_str(), 100));
  ASSERT_EQ(0, rbd.trash_purge(ioctx, 0, -1));

  std::vector<librbd::trash_image_info_t> entries;
  ASSERT_EQ(0, rbd.trash_list(ioctx, entries));
  ASSERT_EQ(1U, entries.size());
  ASSERT_EQ(image_id2, entries[0].id);
  ASSERT_EQ(name2, entries[0].name);
  entries.clear();

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  float threshold = 0.0;
  if (!is_librados_test_stub(_rados)) {
    // real cluster usage reports have a long latency to update
    threshold = -1.0;
  }

  ASSERT_EQ(0, rbd.trash_purge(ioctx, now.tv_sec+1000, threshold));
  ASSERT_EQ(0, rbd.trash_list(ioctx, entries));
  ASSERT_EQ(0U, entries.size());
}

TEST_F(TestLibRBD, TestTrashMoveAndRestore) {
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

  std::string image_id;
  ASSERT_EQ(0, image.get_id(&image_id));
  image.close();

  ASSERT_EQ(0, rbd.trash_move(ioctx, name.c_str(), 10));

  std::vector<std::string> images;
  ASSERT_EQ(0, rbd.list(ioctx, images));
  for (const auto& image : images) {
    ASSERT_TRUE(image != name);
  }

  std::vector<librbd::trash_image_info_t> entries;
  ASSERT_EQ(0, rbd.trash_list(ioctx, entries));
  ASSERT_FALSE(entries.empty());
  ASSERT_EQ(entries.begin()->id, image_id);

  images.clear();
  ASSERT_EQ(0, rbd.trash_restore(ioctx, image_id.c_str(), ""));
  ASSERT_EQ(0, rbd.list(ioctx, images));
  ASSERT_FALSE(images.empty());
  bool found = false;
  for (const auto& image : images) {
    if (image == name) {
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found);
}

TEST_F(TestLibRBD, TestListWatchers) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  std::list<librbd::image_watcher_t> watchers;

  // No watchers
  ASSERT_EQ(0, rbd.open_read_only(ioctx, image, name.c_str(), nullptr));
  ASSERT_EQ(0, image.list_watchers(watchers));
  ASSERT_EQ(0U, watchers.size());
  ASSERT_EQ(0, image.close());

  // One watcher
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));
  ASSERT_EQ(0, image.list_watchers(watchers));
  ASSERT_EQ(1U, watchers.size());
  auto watcher1 = watchers.front();
  ASSERT_EQ(0, image.close());

  // (Still) one watcher
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));
  ASSERT_EQ(0, image.list_watchers(watchers));
  ASSERT_EQ(1U, watchers.size());
  auto watcher2 = watchers.front();
  ASSERT_EQ(0, image.close());

  EXPECT_EQ(watcher1.addr, watcher2.addr);
  EXPECT_EQ(watcher1.id, watcher2.id);
}

TEST_F(TestLibRBD, TestSetSnapById) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  std::string name = get_temp_image_name();

  uint64_t size = 1 << 18;
  int order = 12;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));
  ASSERT_EQ(0, image.snap_create("snap"));

  vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_EQ(1U, snaps.size());

  ASSERT_EQ(0, image.snap_set_by_id(snaps[0].id));
  ASSERT_EQ(0, image.snap_set_by_id(CEPH_NOSNAP));
}

TEST_F(TestLibRBD, Namespaces) {
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));
  rados_remove(ioctx, RBD_NAMESPACE);

  ASSERT_EQ(0, rbd_namespace_create(ioctx, "name1"));
  ASSERT_EQ(0, rbd_namespace_create(ioctx, "name2"));
  ASSERT_EQ(0, rbd_namespace_create(ioctx, "name3"));
  ASSERT_EQ(0, rbd_namespace_remove(ioctx, "name2"));

  char names[1024];
  size_t max_size = sizeof(names);
  int len = rbd_namespace_list(ioctx, names, &max_size);

  std::vector<std::string> cpp_names;
  for (char* cur_name = names; cur_name < names + len; ) {
    cpp_names.push_back(cur_name);
    cur_name += strlen(cur_name) + 1;
  }
  ASSERT_EQ(2U, cpp_names.size());
  ASSERT_EQ("name1", cpp_names[0]);
  ASSERT_EQ("name3", cpp_names[1]);
  bool exists;
  ASSERT_EQ(0, rbd_namespace_exists(ioctx, "name2", &exists));
  ASSERT_FALSE(exists);
  ASSERT_EQ(0, rbd_namespace_exists(ioctx, "name3", &exists));
  ASSERT_TRUE(exists);
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, NamespacesPP) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_NAMESPACE);

  librbd::RBD rbd;
  ASSERT_EQ(-EINVAL, rbd.namespace_create(ioctx, ""));
  ASSERT_EQ(-EINVAL, rbd.namespace_remove(ioctx, ""));

  ASSERT_EQ(0, rbd.namespace_create(ioctx, "name1"));
  ASSERT_EQ(-EEXIST, rbd.namespace_create(ioctx, "name1"));
  ASSERT_EQ(0, rbd.namespace_create(ioctx, "name2"));
  ASSERT_EQ(0, rbd.namespace_create(ioctx, "name3"));
  ASSERT_EQ(0, rbd.namespace_remove(ioctx, "name2"));
  ASSERT_EQ(-ENOENT, rbd.namespace_remove(ioctx, "name2"));

  std::vector<std::string> names;
  ASSERT_EQ(0, rbd.namespace_list(ioctx, &names));
  ASSERT_EQ(2U, names.size());
  ASSERT_EQ("name1", names[0]);
  ASSERT_EQ("name3", names[1]);
  bool exists;
  ASSERT_EQ(0, rbd.namespace_exists(ioctx, "name2", &exists));
  ASSERT_FALSE(exists);
  ASSERT_EQ(0, rbd.namespace_exists(ioctx, "name3", &exists));
  ASSERT_TRUE(exists);

  librados::IoCtx ns_io_ctx;
  ns_io_ctx.dup(ioctx);

  std::string name = get_temp_image_name();
  int order = 0;
  uint64_t features = 0;
  if (!get_features(&features)) {
    // old format doesn't support namespaces
    ns_io_ctx.set_namespace("name1");
    ASSERT_EQ(-EINVAL, create_image_pp(rbd, ns_io_ctx, name.c_str(), 0,
                                       &order));
    return;
  }

  ns_io_ctx.set_namespace("missing");
  ASSERT_EQ(-ENOENT, create_image_pp(rbd, ns_io_ctx, name.c_str(), 0, &order));

  ns_io_ctx.set_namespace("name1");
  ASSERT_EQ(0, create_image_pp(rbd, ns_io_ctx, name.c_str(), 0, &order));
  ASSERT_EQ(-EBUSY, rbd.namespace_remove(ns_io_ctx, "name1"));

  std::string image_id;
  {
    librbd::Image image;
    ASSERT_EQ(-ENOENT, rbd.open(ioctx, image, name.c_str(), NULL));
    ASSERT_EQ(0, rbd.open(ns_io_ctx, image, name.c_str(), NULL));
    ASSERT_EQ(0, get_image_id(image, &image_id));
  }

  ASSERT_EQ(-ENOENT, rbd.trash_move(ioctx, name.c_str(), 0));
  ASSERT_EQ(0, rbd.trash_move(ns_io_ctx, name.c_str(), 0));
  ASSERT_EQ(-EBUSY, rbd.namespace_remove(ns_io_ctx, "name1"));

  PrintProgress pp;
  ASSERT_EQ(-ENOENT, rbd.trash_remove_with_progress(ioctx, image_id.c_str(),
                                                    false, pp));
  ASSERT_EQ(0, rbd.trash_remove_with_progress(ns_io_ctx, image_id.c_str(),
                                              false, pp));
  ASSERT_EQ(0, rbd.namespace_remove(ns_io_ctx, "name1"));

  names.clear();
  ASSERT_EQ(0, rbd.namespace_list(ioctx, &names));
  ASSERT_EQ(1U, names.size());
  ASSERT_EQ("name3", names[0]);
}

TEST_F(TestLibRBD, Migration) {
  bool old_format;
  uint64_t features;
  ASSERT_EQ(0, get_features(&old_format, &features));

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);
  BOOST_SCOPE_EXIT(&ioctx) {
    rados_ioctx_destroy(ioctx);
  } BOOST_SCOPE_EXIT_END;

  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));

  rbd_image_options_t image_options;
  rbd_image_options_create(&image_options);
  BOOST_SCOPE_EXIT(&image_options) {
    rbd_image_options_destroy(image_options);
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, rbd_migration_prepare(ioctx, name.c_str(), ioctx, name.c_str(),
                                     image_options));

  rbd_image_migration_status_t status;
  ASSERT_EQ(0, rbd_migration_status(ioctx, name.c_str(), &status,
                                    sizeof(status)));
  ASSERT_EQ(status.source_pool_id, rados_ioctx_get_id(ioctx));
  ASSERT_EQ(status.source_image_name, name);
  if (old_format) {
    ASSERT_EQ(status.source_image_id, string());
  } else {
    ASSERT_NE(status.source_image_id, string());
    ASSERT_EQ(-EROFS, rbd_trash_remove(ioctx, status.source_image_id, false));
    ASSERT_EQ(-EINVAL, rbd_trash_restore(ioctx, status.source_image_id, name.c_str()));
  }
  ASSERT_EQ(status.dest_pool_id, rados_ioctx_get_id(ioctx));
  ASSERT_EQ(status.dest_image_name, name);
  ASSERT_NE(status.dest_image_id, string());
  ASSERT_EQ(status.state, RBD_IMAGE_MIGRATION_STATE_PREPARED);
  rbd_migration_status_cleanup(&status);

  rbd_image_t image;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  char source_spec[2048];
  size_t source_spec_length = sizeof(source_spec);
  ASSERT_EQ(0, rbd_get_migration_source_spec(image, source_spec,
                                             &source_spec_length));
  json_spirit::mValue json_source_spec;
  json_spirit::read(source_spec, json_source_spec);
  EXPECT_EQ(0, rbd_close(image));

  ASSERT_EQ(-EBUSY, rbd_remove(ioctx, name.c_str()));
  ASSERT_EQ(-EBUSY, rbd_trash_move(ioctx, name.c_str(), 0));

  ASSERT_EQ(0, rbd_migration_execute(ioctx, name.c_str()));

  ASSERT_EQ(0, rbd_migration_status(ioctx, name.c_str(), &status,
                                    sizeof(status)));
  ASSERT_EQ(status.state, RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  rbd_migration_status_cleanup(&status);

  ASSERT_EQ(0, rbd_migration_commit(ioctx, name.c_str()));

  std::string new_name = get_temp_image_name();

  ASSERT_EQ(0, rbd_migration_prepare(ioctx, name.c_str(), ioctx,
                                     new_name.c_str(), image_options));

  ASSERT_EQ(-EBUSY, rbd_remove(ioctx, new_name.c_str()));
  ASSERT_EQ(-EBUSY, rbd_trash_move(ioctx, new_name.c_str(), 0));

  ASSERT_EQ(0, rbd_migration_abort(ioctx, name.c_str()));

  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  EXPECT_EQ(0, rbd_close(image));
}

TEST_F(TestLibRBD, MigrationPP) {
  bool old_format;
  uint64_t features;
  ASSERT_EQ(0, get_features(&old_format, &features));

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  librbd::RBD rbd;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::ImageOptions image_options;

  ASSERT_EQ(0, rbd.migration_prepare(ioctx, name.c_str(), ioctx, name.c_str(),
                                     image_options));

  librbd::image_migration_status_t status;
  ASSERT_EQ(0, rbd.migration_status(ioctx, name.c_str(), &status,
                                    sizeof(status)));
  ASSERT_EQ(status.source_pool_id, ioctx.get_id());
  ASSERT_EQ(status.source_image_name, name);
  if (old_format) {
    ASSERT_EQ(status.source_image_id, "");
  } else {
    ASSERT_NE(status.source_image_id, "");
    ASSERT_EQ(-EROFS, rbd.trash_remove(ioctx, status.source_image_id.c_str(), false));
    ASSERT_EQ(-EINVAL, rbd.trash_restore(ioctx, status.source_image_id.c_str(), name.c_str()));
  }
  ASSERT_EQ(status.dest_pool_id, ioctx.get_id());
  ASSERT_EQ(status.dest_image_name, name);
  ASSERT_NE(status.dest_image_id, "");
  ASSERT_EQ(status.state, RBD_IMAGE_MIGRATION_STATE_PREPARED);

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
  std::string source_spec;
  ASSERT_EQ(0, image.get_migration_source_spec(&source_spec));
  json_spirit::mValue json_source_spec;
  json_spirit::read(source_spec, json_source_spec);
  json_spirit::mObject json_source_spec_obj = json_source_spec.get_obj();
  ASSERT_EQ("native", json_source_spec_obj["type"].get_str());
  ASSERT_EQ(ioctx.get_id(), json_source_spec_obj["pool_id"].get_int64());
  ASSERT_EQ("", json_source_spec_obj["pool_namespace"].get_str());
  ASSERT_EQ(1, json_source_spec_obj.count("image_name"));
  if (!old_format) {
    ASSERT_EQ(1, json_source_spec_obj.count("image_id"));
  }
  ASSERT_EQ(0, image.close());

  ASSERT_EQ(-EBUSY, rbd.remove(ioctx, name.c_str()));
  ASSERT_EQ(-EBUSY, rbd.trash_move(ioctx, name.c_str(), 0));

  ASSERT_EQ(0, rbd.migration_execute(ioctx, name.c_str()));

  ASSERT_EQ(0, rbd.migration_status(ioctx, name.c_str(), &status,
                                    sizeof(status)));
  ASSERT_EQ(status.state, RBD_IMAGE_MIGRATION_STATE_EXECUTED);

  ASSERT_EQ(0, rbd.migration_commit(ioctx, name.c_str()));

  std::string new_name = get_temp_image_name();

  ASSERT_EQ(0, rbd.migration_prepare(ioctx, name.c_str(), ioctx,
                                     new_name.c_str(), image_options));

  ASSERT_EQ(-EBUSY, rbd.remove(ioctx, new_name.c_str()));
  ASSERT_EQ(-EBUSY, rbd.trash_move(ioctx, new_name.c_str(), 0));

  ASSERT_EQ(0, rbd.migration_abort(ioctx, name.c_str()));

  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));
}

TEST_F(TestLibRBD, TestGetAccessTimestamp)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;  
  struct timespec timestamp;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(0, rbd_get_access_timestamp(image, &timestamp));
  ASSERT_LT(0, timestamp.tv_sec);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, TestGetModifyTimestamp)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;  
  struct timespec timestamp;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));
  ASSERT_EQ(0, rbd_get_modify_timestamp(image, &timestamp));
  ASSERT_LT(0, timestamp.tv_sec);

  ASSERT_PASSED(validate_object_map, image);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, ZeroOverlapFlatten) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image parent_image;
  std::string name = get_temp_image_name();

  uint64_t size = 1;
  int order = 0;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, parent_image, name.c_str(), NULL));

  uint64_t features;
  ASSERT_EQ(0, parent_image.features(&features));

  ASSERT_EQ(0, parent_image.snap_create("snap"));
  ASSERT_EQ(0, parent_image.snap_protect("snap"));

  std::string clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "snap", ioctx, clone_name.c_str(),
                         features, &order));

  librbd::Image clone_image;
  ASSERT_EQ(0, rbd.open(ioctx, clone_image, clone_name.c_str(), NULL));
  ASSERT_EQ(0, clone_image.resize(0));
  ASSERT_EQ(0, clone_image.flatten());
}

TEST_F(TestLibRBD, PoolMetadata)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  char keys[1024];
  char vals[1024];
  size_t keys_len = sizeof(keys);
  size_t vals_len = sizeof(vals);

  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);

  ASSERT_EQ(0, rbd_pool_metadata_list(ioctx, "", 0, keys, &keys_len, vals,
                                      &vals_len));
  ASSERT_EQ(0U, keys_len);
  ASSERT_EQ(0U, vals_len);

  char value[1024];
  size_t value_len = sizeof(value);
  memset_rand(value, value_len);

  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "key1", "value1"));
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "key2", "value2"));
  ASSERT_EQ(0, rbd_pool_metadata_get(ioctx, "key1", value, &value_len));
  ASSERT_STREQ(value, "value1");
  value_len = 1;
  ASSERT_EQ(-ERANGE, rbd_pool_metadata_get(ioctx, "key1", value, &value_len));
  ASSERT_EQ(value_len, strlen("value1") + 1);

  ASSERT_EQ(-ERANGE, rbd_pool_metadata_list(ioctx, "", 0, keys, &keys_len, vals,
                                            &vals_len));
  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_pool_metadata_list(ioctx, "", 0, keys, &keys_len, vals,
                                      &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1 + strlen("key2") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1 + strlen("value2") + 1);
  ASSERT_STREQ(keys, "key1");
  ASSERT_STREQ(keys + strlen(keys) + 1, "key2");
  ASSERT_STREQ(vals, "value1");
  ASSERT_STREQ(vals + strlen(vals) + 1, "value2");

  ASSERT_EQ(0, rbd_pool_metadata_remove(ioctx, "key1"));
  ASSERT_EQ(-ENOENT, rbd_pool_metadata_remove(ioctx, "key3"));
  value_len = sizeof(value);
  ASSERT_EQ(-ENOENT, rbd_pool_metadata_get(ioctx, "key3", value, &value_len));
  ASSERT_EQ(0, rbd_pool_metadata_list(ioctx, "", 0, keys, &keys_len, vals,
                                      &vals_len));
  ASSERT_EQ(keys_len, strlen("key2") + 1);
  ASSERT_EQ(vals_len, strlen("value2") + 1);
  ASSERT_STREQ(keys, "key2");
  ASSERT_STREQ(vals, "value2");

  // test config setting
  ASSERT_EQ(-EINVAL, rbd_pool_metadata_set(ioctx, "conf_UNKNOWN", "false"));
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "conf_rbd_cache", "false"));
  ASSERT_EQ(-EINVAL, rbd_pool_metadata_set(ioctx, "conf_rbd_cache", "INVALID"));
  ASSERT_EQ(0, rbd_pool_metadata_remove(ioctx, "conf_rbd_cache"));

  // test short buffer cases
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "key1", "value1"));
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "key3", "value3"));
  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "key4", "value4"));

  keys_len = strlen("key1") + 1;
  vals_len = strlen("value1") + 1;
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_pool_metadata_list(ioctx, "", 1, keys, &keys_len, vals,
                                      &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1);
  ASSERT_STREQ(keys, "key1");
  ASSERT_STREQ(vals, "value1");

  ASSERT_EQ(-ERANGE, rbd_pool_metadata_list(ioctx, "", 2, keys, &keys_len, vals,
                                            &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1 + strlen("key2") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1 + strlen("value2") + 1);

  ASSERT_EQ(-ERANGE, rbd_pool_metadata_list(ioctx, "", 0, keys, &keys_len, vals,
                                            &vals_len));
  ASSERT_EQ(keys_len, strlen("key1") + 1 + strlen("key2") + 1 + strlen("key3") +
            1 + strlen("key4") + 1);
  ASSERT_EQ(vals_len, strlen("value1") + 1 + strlen("value2") + 1 +
            strlen("value3") + 1 + strlen("value4") + 1);

  // test `start` param
  keys_len = sizeof(keys);
  vals_len = sizeof(vals);
  memset_rand(keys, keys_len);
  memset_rand(vals, vals_len);
  ASSERT_EQ(0, rbd_pool_metadata_list(ioctx, "key2", 0, keys, &keys_len, vals,
                                      &vals_len));
  ASSERT_EQ(keys_len, strlen("key3") + 1 + strlen("key4") + 1);
  ASSERT_EQ(vals_len, strlen("value3") + 1 + strlen("value4") + 1);
  ASSERT_STREQ(keys, "key3");
  ASSERT_STREQ(vals, "value3");

  //cleanup
  ASSERT_EQ(0, rbd_pool_metadata_remove(ioctx, "key1"));
  ASSERT_EQ(0, rbd_pool_metadata_remove(ioctx, "key2"));
  ASSERT_EQ(0, rbd_pool_metadata_remove(ioctx, "key3"));
  ASSERT_EQ(0, rbd_pool_metadata_remove(ioctx, "key4"));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, PoolMetadataPP)
{
  REQUIRE_FORMAT_V2();

  librbd::RBD rbd;
  string value;
  map<string, bufferlist> pairs;

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  ASSERT_EQ(0, rbd.pool_metadata_list(ioctx, "", 0, &pairs));
  ASSERT_TRUE(pairs.empty());

  ASSERT_EQ(0, rbd.pool_metadata_set(ioctx, "key1", "value1"));
  ASSERT_EQ(0, rbd.pool_metadata_set(ioctx, "key2", "value2"));
  ASSERT_EQ(0, rbd.pool_metadata_get(ioctx, "key1", &value));
  ASSERT_EQ(value, "value1");
  ASSERT_EQ(0, rbd.pool_metadata_list(ioctx, "", 0, &pairs));
  ASSERT_EQ(2U, pairs.size());
  ASSERT_EQ(0, strncmp("value1", pairs["key1"].c_str(), 6));
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));

  ASSERT_EQ(0, rbd.pool_metadata_remove(ioctx, "key1"));
  ASSERT_EQ(-ENOENT, rbd.pool_metadata_remove(ioctx, "key3"));
  ASSERT_EQ(-ENOENT, rbd.pool_metadata_get(ioctx, "key3", &value));
  pairs.clear();
  ASSERT_EQ(0, rbd.pool_metadata_list(ioctx, "", 0, &pairs));
  ASSERT_EQ(1U, pairs.size());
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));

  // test `start` param
  ASSERT_EQ(0, rbd.pool_metadata_set(ioctx, "key1", "value1"));
  ASSERT_EQ(0, rbd.pool_metadata_set(ioctx, "key3", "value3"));

  pairs.clear();
  ASSERT_EQ(0, rbd.pool_metadata_list(ioctx, "key2", 0, &pairs));
  ASSERT_EQ(1U, pairs.size());
  ASSERT_EQ(0, strncmp("value3", pairs["key3"].c_str(), 6));

  // test config setting
  ASSERT_EQ(-EINVAL, rbd.pool_metadata_set(ioctx, "conf_UNKNOWN", "false"));
  ASSERT_EQ(0, rbd.pool_metadata_set(ioctx, "conf_rbd_cache", "false"));
  ASSERT_EQ(-EINVAL, rbd.pool_metadata_set(ioctx, "conf_rbd_cache", "INVALID"));
  ASSERT_EQ(0, rbd.pool_metadata_remove(ioctx, "conf_rbd_cache"));

  // cleanup
  ASSERT_EQ(0, rbd.pool_metadata_remove(ioctx, "key1"));
  ASSERT_EQ(0, rbd.pool_metadata_remove(ioctx, "key2"));
  ASSERT_EQ(0, rbd.pool_metadata_remove(ioctx, "key3"));
}

TEST_F(TestLibRBD, Config)
{
  REQUIRE_FORMAT_V2();

  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  ASSERT_EQ(0, rbd_pool_metadata_set(ioctx, "conf_rbd_cache", "false"));

  rbd_config_option_t options[1024];
  int max_options = 0;
  ASSERT_EQ(-ERANGE, rbd_config_pool_list(ioctx, options, &max_options));
  ASSERT_EQ(0, rbd_config_pool_list(ioctx, options, &max_options));
  ASSERT_GT(max_options, 0);
  ASSERT_LT(max_options, 1024);
  for (int i = 0; i < max_options; i++) {
    if (options[i].name == std::string("rbd_cache")) {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_POOL);
      ASSERT_STREQ("false", options[i].value);
    } else {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }
  rbd_config_pool_list_cleanup(options, max_options);

  rbd_image_t image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image, NULL));

  ASSERT_EQ(0, rbd_config_image_list(image, options, &max_options));
  for (int i = 0; i < max_options; i++) {
    if (options[i].name == std::string("rbd_cache")) {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_POOL);
      ASSERT_STREQ("false", options[i].value);
    } else {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }
  rbd_config_image_list_cleanup(options, max_options);

  ASSERT_EQ(0, rbd_metadata_set(image, "conf_rbd_cache", "true"));

  ASSERT_EQ(0, rbd_config_image_list(image, options, &max_options));
  for (int i = 0; i < max_options; i++) {
    if (options[i].name == std::string("rbd_cache")) {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_IMAGE);
      ASSERT_STREQ("true", options[i].value);
    } else {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }
  rbd_config_image_list_cleanup(options, max_options);

  ASSERT_EQ(0, rbd_metadata_remove(image, "conf_rbd_cache"));

  ASSERT_EQ(0, rbd_config_image_list(image, options, &max_options));
  for (int i = 0; i < max_options; i++) {
    if (options[i].name == std::string("rbd_cache")) {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_POOL);
      ASSERT_STREQ("false", options[i].value);
    } else {
      ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }
  rbd_config_image_list_cleanup(options, max_options);

  ASSERT_EQ(0, rbd_close(image));

  ASSERT_EQ(0, rbd_pool_metadata_remove(ioctx, "conf_rbd_cache"));

  ASSERT_EQ(-ERANGE, rbd_config_pool_list(ioctx, options, &max_options));
  ASSERT_EQ(0, rbd_config_pool_list(ioctx, options, &max_options));
  for (int i = 0; i < max_options; i++) {
    ASSERT_EQ(options[i].source, RBD_CONFIG_SOURCE_CONFIG);
  }
  rbd_config_pool_list_cleanup(options, max_options);

  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, ConfigPP)
{
  REQUIRE_FORMAT_V2();

  librbd::RBD rbd;
  string value;

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  ASSERT_EQ(0, rbd.pool_metadata_set(ioctx, "conf_rbd_cache", "false"));

  std::vector<librbd::config_option_t> options;
  ASSERT_EQ(0, rbd.config_list(ioctx, &options));
  for (auto &option : options) {
    if (option.name == std::string("rbd_cache")) {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_POOL);
      ASSERT_EQ("false", option.value);
    } else {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }

  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));

  options.clear();
  ASSERT_EQ(0, image.config_list(&options));
  for (auto &option : options) {
    if (option.name == std::string("rbd_cache")) {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_POOL);
      ASSERT_EQ("false", option.value);
    } else {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }

  ASSERT_EQ(0, image.metadata_set("conf_rbd_cache", "true"));

  options.clear();
  ASSERT_EQ(0, image.config_list(&options));
  for (auto &option : options) {
    if (option.name == std::string("rbd_cache")) {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_IMAGE);
      ASSERT_EQ("true", option.value);
    } else {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }

  ASSERT_EQ(0, image.metadata_remove("conf_rbd_cache"));

  options.clear();
  ASSERT_EQ(0, image.config_list(&options));
  for (auto &option : options) {
    if (option.name == std::string("rbd_cache")) {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_POOL);
      ASSERT_EQ("false", option.value);
    } else {
      ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_CONFIG);
    }
  }

  ASSERT_EQ(0, rbd.pool_metadata_remove(ioctx, "conf_rbd_cache"));

  options.clear();
  ASSERT_EQ(0, rbd.config_list(ioctx, &options));
  for (auto &option : options) {
    ASSERT_EQ(option.source, RBD_CONFIG_SOURCE_CONFIG);
  }
}

TEST_F(TestLibRBD, PoolStatsPP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(create_pool(true).c_str(), ioctx));

  librbd::RBD rbd;
  std::string image_name;
  uint64_t size = 2 << 20;
  uint64_t expected_size = 0;
  for (size_t idx = 0; idx < 4; ++idx) {
    image_name = get_temp_image_name();

    int order = 0;
    ASSERT_EQ(0, create_image_pp(rbd, ioctx, image_name.c_str(), size, &order));
    expected_size += size;
  }

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, image_name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("snap1"));
  ASSERT_EQ(0, image.resize(0));
  ASSERT_EQ(0, image.close());
  uint64_t expect_head_size = (expected_size - size);

  uint64_t image_count;
  uint64_t provisioned_bytes;
  uint64_t max_provisioned_bytes;
  uint64_t snap_count;
  uint64_t trash_image_count;
  uint64_t trash_provisioned_bytes;
  uint64_t trash_max_provisioned_bytes;
  uint64_t trash_snap_count;

  librbd::PoolStats pool_stats1;
  pool_stats1.add(RBD_POOL_STAT_OPTION_IMAGES, &image_count);
  pool_stats1.add(RBD_POOL_STAT_OPTION_IMAGE_PROVISIONED_BYTES,
                  &provisioned_bytes);
  ASSERT_EQ(0, rbd.pool_stats_get(ioctx, &pool_stats1));

  ASSERT_EQ(4U, image_count);
  ASSERT_EQ(expect_head_size, provisioned_bytes);

  pool_stats1.add(RBD_POOL_STAT_OPTION_IMAGE_MAX_PROVISIONED_BYTES,
                  &max_provisioned_bytes);
  ASSERT_EQ(0, rbd.pool_stats_get(ioctx, &pool_stats1));
  ASSERT_EQ(4U, image_count);
  ASSERT_EQ(expect_head_size, provisioned_bytes);
  ASSERT_EQ(expected_size, max_provisioned_bytes);

  librbd::PoolStats pool_stats2;
  pool_stats2.add(RBD_POOL_STAT_OPTION_IMAGE_SNAPSHOTS, &snap_count);
  pool_stats2.add(RBD_POOL_STAT_OPTION_TRASH_IMAGES, &trash_image_count);
  pool_stats2.add(RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS, &trash_snap_count);
  ASSERT_EQ(0, rbd.pool_stats_get(ioctx, &pool_stats2));
  ASSERT_EQ(1U, snap_count);
  ASSERT_EQ(0U, trash_image_count);
  ASSERT_EQ(0U, trash_snap_count);

  ASSERT_EQ(0, rbd.trash_move(ioctx, image_name.c_str(), 0));

  librbd::PoolStats pool_stats3;
  pool_stats3.add(RBD_POOL_STAT_OPTION_TRASH_IMAGES, &trash_image_count);
  pool_stats3.add(RBD_POOL_STAT_OPTION_TRASH_PROVISIONED_BYTES,
                  &trash_provisioned_bytes);
  pool_stats3.add(RBD_POOL_STAT_OPTION_TRASH_MAX_PROVISIONED_BYTES,
                  &trash_max_provisioned_bytes);
  pool_stats3.add(RBD_POOL_STAT_OPTION_TRASH_SNAPSHOTS, &trash_snap_count);
  ASSERT_EQ(0, rbd.pool_stats_get(ioctx, &pool_stats3));
  ASSERT_EQ(1U, trash_image_count);
  ASSERT_EQ(0U, trash_provisioned_bytes);
  ASSERT_EQ(size, trash_max_provisioned_bytes);
  ASSERT_EQ(1U, trash_snap_count);
}

TEST_F(TestLibRBD, ImageSpec) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(create_pool(true).c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image parent_image;
  std::string name = get_temp_image_name();

  uint64_t size = 1;
  int order = 0;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, parent_image, name.c_str(), NULL));

  std::string parent_id;
  ASSERT_EQ(0, parent_image.get_id(&parent_id));

  uint64_t features;
  ASSERT_EQ(0, parent_image.features(&features));

  ASSERT_EQ(0, parent_image.snap_create("snap"));
  ASSERT_EQ(0, parent_image.snap_protect("snap"));

  std::string clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "snap", ioctx, clone_name.c_str(),
                         features, &order));

  librbd::Image clone_image;
  ASSERT_EQ(0, rbd.open(ioctx, clone_image, clone_name.c_str(), NULL));

  std::string clone_id;
  ASSERT_EQ(0, clone_image.get_id(&clone_id));

  std::vector<librbd::image_spec_t> images;
  ASSERT_EQ(0, rbd.list2(ioctx, &images));

  std::vector<librbd::image_spec_t> expected_images{
    {.id = parent_id, .name = name},
    {.id = clone_id, .name = clone_name}
  };
  std::sort(expected_images.begin(), expected_images.end(),
            [](const librbd::image_spec_t& lhs, const librbd::image_spec_t &rhs) {
              return lhs.name < rhs.name;
            });
  ASSERT_EQ(expected_images, images);

  librbd::linked_image_spec_t parent_image_spec;
  librbd::snap_spec_t parent_snap_spec;
  ASSERT_EQ(0, clone_image.get_parent(&parent_image_spec, &parent_snap_spec));

  librbd::linked_image_spec_t expected_parent_image_spec{
    .pool_id = ioctx.get_id(),
    .pool_name = ioctx.get_pool_name(),
    .pool_namespace = ioctx.get_namespace(),
    .image_id = parent_id,
    .image_name = name,
    .trash = false
  };
  ASSERT_EQ(expected_parent_image_spec, parent_image_spec);
  ASSERT_EQ(RBD_SNAP_NAMESPACE_TYPE_USER, parent_snap_spec.namespace_type);
  ASSERT_EQ("snap", parent_snap_spec.name);

  std::vector<librbd::linked_image_spec_t> children;
  ASSERT_EQ(0, parent_image.list_children3(&children));

  std::vector<librbd::linked_image_spec_t> expected_children{
    {
      .pool_id = ioctx.get_id(),
      .pool_name = ioctx.get_pool_name(),
      .pool_namespace = ioctx.get_namespace(),
      .image_id = clone_id,
      .image_name = clone_name,
      .trash = false
    }
  };
  ASSERT_EQ(expected_children, children);

  children.clear();
  ASSERT_EQ(0, parent_image.list_descendants(&children));
  ASSERT_EQ(expected_children, children);

  ASSERT_EQ(0, clone_image.snap_create("snap"));
  ASSERT_EQ(0, clone_image.snap_protect("snap"));

  auto grand_clone_name = this->get_temp_image_name();
  ASSERT_EQ(0, rbd.clone(ioctx, clone_name.c_str(), "snap", ioctx,
                         grand_clone_name.c_str(), features, &order));
  librbd::Image grand_clone_image;
  ASSERT_EQ(0, rbd.open(ioctx, grand_clone_image, grand_clone_name.c_str(),
                        nullptr));
  std::string grand_clone_id;
  ASSERT_EQ(0, grand_clone_image.get_id(&grand_clone_id));

  children.clear();
  ASSERT_EQ(0, parent_image.list_children3(&children));
  ASSERT_EQ(expected_children, children);

  children.clear();
  ASSERT_EQ(0, parent_image.list_descendants(&children));
  expected_children.push_back(
    {
      .pool_id = ioctx.get_id(),
      .pool_name = ioctx.get_pool_name(),
      .pool_namespace = ioctx.get_namespace(),
      .image_id = grand_clone_id,
      .image_name = grand_clone_name,
      .trash = false
    }
  );
  ASSERT_EQ(expected_children, children);
}

void super_simple_write_cb_pp(librbd::completion_t cb, void *arg)
{
}

TEST_F(TestLibRBD, DISABLED_TestSeqWriteAIOPP)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    int order = 21;
    std::string name = get_temp_image_name();
    uint64_t size = 5 * (1 << order);

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    char test_data[(TEST_IO_SIZE + 1) * 10];

    for (int i = 0; i < 10; i++) {
      for (uint64_t j = 0; j < TEST_IO_SIZE; j++) {
        test_data[(TEST_IO_SIZE + 1) * i + j] = (char)(rand() % (126 - 33) + 33);
      }
      test_data[(TEST_IO_SIZE + 1) * i + TEST_IO_SIZE] = '\0';
    }

    struct timespec start_time;
    clock_gettime(CLOCK_REALTIME, &start_time);

    std::list<librbd::RBD::AioCompletion *> comps;
    for (uint64_t i = 0; i < size / TEST_IO_SIZE; ++i) {
      char *p = test_data + (TEST_IO_SIZE + 1) * (i % 10);
      ceph::bufferlist bl;
      bl.append(p, strlen(p));
      auto comp = new librbd::RBD::AioCompletion(
          NULL, (librbd::callback_t) super_simple_write_cb_pp);
      image.aio_write(strlen(p) * i, strlen(p), bl, comp);
      comps.push_back(comp);
      if (i % 1000 == 0) {
        cout << i << " reqs sent" << std::endl;
        image.flush();
        for (auto comp : comps) {
          comp->wait_for_complete();
          ASSERT_EQ(0, comp->get_return_value());
          comp->release();
        }
        comps.clear();
      }
    }
    int i = 0;
    for (auto comp : comps) {
      comp->wait_for_complete();
      ASSERT_EQ(0, comp->get_return_value());
      comp->release();
      if (i % 1000 == 0) {
        std::cout << i << " reqs completed" << std::endl;
      }
      i++;
    }
    comps.clear();

    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    int duration = end_time.tv_sec * 1000 + end_time.tv_nsec / 1000000 -
      start_time.tv_sec * 1000 - start_time.tv_nsec / 1000000;
    std::cout << "duration: " << duration << " msec" << std::endl;

    for (uint64_t i = 0; i < size / TEST_IO_SIZE; ++i) {
      char *p = test_data + (TEST_IO_SIZE + 1) * (i % 10);
      ASSERT_PASSED(read_test_data, image, p, strlen(p) * i, TEST_IO_SIZE, 0);
    }

    ASSERT_PASSED(validate_object_map, image);
  }

  ioctx.close();
}

TEST_F(TestLibRBD, SnapRemoveWithChildMissing)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  ASSERT_EQ(0, rados_conf_set(_cluster, "rbd_default_clone_format", "2"));
  BOOST_SCOPE_EXIT_ALL(&) {
    ASSERT_EQ(0, rados_conf_set(_cluster, "rbd_default_clone_format", "auto"));
  };

  librbd::RBD rbd;
  rados_ioctx_t ioctx1, ioctx2;
  string pool_name1 = create_pool(true);
  rados_ioctx_create(_cluster, pool_name1.c_str(), &ioctx1);
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx2));

  bool old_format;
  uint64_t features;
  rbd_image_t parent, child1, child2, child3;
  int order = 0;
  char child_id1[4096];
  char child_id2[4096];
  char child_id3[4096];

  ASSERT_EQ(0, get_features(&old_format, &features));
  ASSERT_FALSE(old_format);
  std::string parent_name = get_temp_image_name();
  std::string child_name1 = get_temp_image_name();
  std::string child_name2 = get_temp_image_name();
  std::string child_name3 = get_temp_image_name();
  ASSERT_EQ(0, create_image_full(ioctx1, parent_name.c_str(), 4<<20, &order,
            false, features));
  ASSERT_EQ(0, rbd_open(ioctx1, parent_name.c_str(), &parent, NULL));
  ASSERT_EQ(0, rbd_snap_create(parent, "snap1"));
  ASSERT_EQ(0, rbd_snap_create(parent, "snap2"));

  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "snap1",
                           ioctx2, child_name1.c_str(), features, &order));
  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "snap2",
                           ioctx1, child_name2.c_str(), features, &order));
  ASSERT_EQ(0, clone_image(ioctx1, parent, parent_name.c_str(), "snap2",
                           ioctx2, child_name3.c_str(), features, &order));

  ASSERT_EQ(0, rbd_open(ioctx2, child_name1.c_str(), &child1, NULL));
  ASSERT_EQ(0, rbd_open(ioctx1, child_name2.c_str(), &child2, NULL));
  ASSERT_EQ(0, rbd_open(ioctx2, child_name3.c_str(), &child3, NULL));
  ASSERT_EQ(0, rbd_get_id(child1, child_id1, sizeof(child_id1)));
  ASSERT_EQ(0, rbd_get_id(child2, child_id2, sizeof(child_id2)));
  ASSERT_EQ(0, rbd_get_id(child3, child_id3, sizeof(child_id3)));
  test_list_children2(parent, 3,
                      child_id1, m_pool_name.c_str(), child_name1.c_str(), false,
                      child_id2, pool_name1.c_str(), child_name2.c_str(), false,
                      child_id3, m_pool_name.c_str(), child_name3.c_str(), false);

  size_t max_size = 10;
  rbd_linked_image_spec_t children[max_size];
  ASSERT_EQ(0, rbd_list_children3(parent, children, &max_size));
  ASSERT_EQ(3, static_cast<int>(max_size));
  rbd_linked_image_spec_list_cleanup(children, max_size);

  ASSERT_EQ(0, rbd_close(child1));
  ASSERT_EQ(0, rbd_close(child2));
  ASSERT_EQ(0, rbd_close(child3));
  rados_ioctx_destroy(ioctx2);
  ASSERT_EQ(0, rados_pool_delete(_cluster, m_pool_name.c_str()));
  _pool_names.erase(std::remove(_pool_names.begin(),
                                _pool_names.end(), m_pool_name),
                    _pool_names.end());
  EXPECT_EQ(0, rados_wait_for_latest_osdmap(_cluster));

  ASSERT_EQ(0, rbd_list_children3(parent, children, &max_size));
  ASSERT_EQ(3, static_cast<int>(max_size));
  rbd_linked_image_spec_list_cleanup(children, max_size);
  ASSERT_EQ(0, rbd_snap_remove(parent, "snap1"));
  ASSERT_EQ(0, rbd_list_children3(parent, children, &max_size));
  ASSERT_EQ(2, static_cast<int>(max_size));
  rbd_linked_image_spec_list_cleanup(children, max_size);

  ASSERT_EQ(0, rbd_remove(ioctx1, child_name2.c_str()));
  ASSERT_EQ(0, rbd_list_children3(parent, children, &max_size));
  ASSERT_EQ(1, static_cast<int>(max_size));
  rbd_linked_image_spec_list_cleanup(children, max_size);

  ASSERT_EQ(0, rbd_snap_remove(parent, "snap2"));
  ASSERT_EQ(0, rbd_list_children3(parent, children, &max_size));
  ASSERT_EQ(0, static_cast<int>(max_size));
  rbd_linked_image_spec_list_cleanup(children, max_size);
  test_list_children2(parent, 0);
  ASSERT_EQ(0, test_ls_snaps(parent, 0));

  ASSERT_EQ(0, rbd_close(parent));
  rados_ioctx_destroy(ioctx1);
}

TEST_F(TestLibRBD, QuiesceWatch)
{
  rados_ioctx_t ioctx;
  rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx);

  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image(ioctx, name.c_str(), size, &order));

  rbd_image_t image1, image2;
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image1, NULL));
  ASSERT_EQ(0, rbd_open(ioctx, name.c_str(), &image2, NULL));

  struct Watcher {
    static void quiesce_cb(void *arg) {
      Watcher *watcher = static_cast<Watcher *>(arg);
      watcher->handle_quiesce();
    }
    static void unquiesce_cb(void *arg) {
      Watcher *watcher = static_cast<Watcher *>(arg);
      watcher->handle_unquiesce();
    }

    rbd_image_t &image;
    uint64_t handle = 0;
    size_t quiesce_count = 0;
    size_t unquiesce_count = 0;

    ceph::mutex lock = ceph::make_mutex("lock");
    ceph::condition_variable cv;

    Watcher(rbd_image_t &image) : image(image) {
    }

    void handle_quiesce() {
      ASSERT_EQ(quiesce_count, unquiesce_count);
      quiesce_count++;
      rbd_quiesce_complete(image, handle, 0);
    }
    void handle_unquiesce() {
      std::unique_lock locker(lock);
      unquiesce_count++;
      ASSERT_EQ(quiesce_count, unquiesce_count);
      cv.notify_one();
    }
    bool wait_for_unquiesce(size_t c) {
      std::unique_lock locker(lock);
      return cv.wait_for(locker, seconds(60),
                         [this, c]() { return unquiesce_count >= c; });
    }
  } watcher1(image1), watcher2(image2);

  ASSERT_EQ(0, rbd_quiesce_watch(image1, Watcher::quiesce_cb,
                                 Watcher::unquiesce_cb, &watcher1,
                                 &watcher1.handle));
  ASSERT_EQ(0, rbd_quiesce_watch(image2, Watcher::quiesce_cb,
                                 Watcher::unquiesce_cb, &watcher2,
                                 &watcher2.handle));

  ASSERT_EQ(0, rbd_snap_create(image1, "snap1"));
  ASSERT_EQ(1U, watcher1.quiesce_count);
  ASSERT_TRUE(watcher1.wait_for_unquiesce(1U));
  ASSERT_EQ(1U, watcher2.quiesce_count);
  ASSERT_TRUE(watcher2.wait_for_unquiesce(1U));

  ASSERT_EQ(0, rbd_snap_create(image2, "snap2"));
  ASSERT_EQ(2U, watcher1.quiesce_count);
  ASSERT_TRUE(watcher1.wait_for_unquiesce(2U));
  ASSERT_EQ(2U, watcher2.quiesce_count);
  ASSERT_TRUE(watcher2.wait_for_unquiesce(2U));

  ASSERT_EQ(0, rbd_quiesce_unwatch(image1, watcher1.handle));

  ASSERT_EQ(0, rbd_snap_create(image1, "snap3"));
  ASSERT_EQ(2U, watcher1.quiesce_count);
  ASSERT_EQ(2U, watcher1.unquiesce_count);
  ASSERT_EQ(3U, watcher2.quiesce_count);
  ASSERT_TRUE(watcher2.wait_for_unquiesce(3U));

  ASSERT_EQ(0, rbd_quiesce_unwatch(image2, watcher2.handle));

  ASSERT_EQ(0, rbd_snap_remove(image1, "snap1"));
  ASSERT_EQ(0, rbd_snap_remove(image1, "snap2"));
  ASSERT_EQ(0, rbd_snap_remove(image1, "snap3"));
  ASSERT_EQ(0, rbd_close(image1));
  ASSERT_EQ(0, rbd_close(image2));
  ASSERT_EQ(0, rbd_remove(ioctx, name.c_str()));
  rados_ioctx_destroy(ioctx);
}

TEST_F(TestLibRBD, QuiesceWatchPP)
{
  librbd::RBD rbd;
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));
  std::string name = get_temp_image_name();
  int order = 0;
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  {
    librbd::Image image1, image2;
    ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));
    ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

    struct Watcher : public librbd::QuiesceWatchCtx {
      librbd::Image &image;
      uint64_t handle = 0;
      size_t quiesce_count = 0;
      size_t unquiesce_count = 0;

      ceph::mutex lock = ceph::make_mutex("lock");
      ceph::condition_variable cv;

      Watcher(librbd::Image &image) : image(image) {
      }

      void handle_quiesce() override {
        ASSERT_EQ(quiesce_count, unquiesce_count);
        quiesce_count++;
        image.quiesce_complete(handle, 0);
      }
      void handle_unquiesce() override {
        std::unique_lock locker(lock);
        unquiesce_count++;
        ASSERT_EQ(quiesce_count, unquiesce_count);
        cv.notify_one();
      }
      bool wait_for_unquiesce(size_t c) {
        std::unique_lock locker(lock);
        return cv.wait_for(locker, seconds(60),
                           [this, c]() { return unquiesce_count >= c; });
      }
    } watcher1(image1), watcher2(image2);

    ASSERT_EQ(0, image1.quiesce_watch(&watcher1, &watcher1.handle));
    ASSERT_EQ(0, image2.quiesce_watch(&watcher2, &watcher2.handle));

    ASSERT_EQ(0, image1.snap_create("snap1"));
    ASSERT_EQ(1U, watcher1.quiesce_count);
    ASSERT_TRUE(watcher1.wait_for_unquiesce(1U));
    ASSERT_EQ(1U, watcher2.quiesce_count);
    ASSERT_TRUE(watcher2.wait_for_unquiesce(1U));

    ASSERT_EQ(0, image2.snap_create("snap2"));
    ASSERT_EQ(2U, watcher1.quiesce_count);
    ASSERT_TRUE(watcher1.wait_for_unquiesce(2U));
    ASSERT_EQ(2U, watcher2.quiesce_count);
    ASSERT_TRUE(watcher2.wait_for_unquiesce(2U));

    ASSERT_EQ(0, image1.quiesce_unwatch(watcher1.handle));

    ASSERT_EQ(0, image1.snap_create("snap3"));
    ASSERT_EQ(2U, watcher1.quiesce_count);
    ASSERT_EQ(2U, watcher1.unquiesce_count);
    ASSERT_EQ(3U, watcher2.quiesce_count);
    ASSERT_TRUE(watcher2.wait_for_unquiesce(3U));

    ASSERT_EQ(0, image2.quiesce_unwatch(watcher2.handle));

    ASSERT_EQ(0, image1.snap_remove("snap1"));
    ASSERT_EQ(0, image1.snap_remove("snap2"));
    ASSERT_EQ(0, image1.snap_remove("snap3"));
  }

  ASSERT_EQ(0, rbd.remove(ioctx, name.c_str()));
  ioctx.close();
}

TEST_F(TestLibRBD, QuiesceWatchError)
{
  SKIP_IF_CRIMSON();
  librbd::RBD rbd;
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));
  std::string name = get_temp_image_name();
  int order = 0;
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  {
    librbd::Image image1, image2;
    ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));
    ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));

    struct Watcher : public librbd::QuiesceWatchCtx {
      librbd::Image &image;
      int r;
      uint64_t handle;
      size_t quiesce_count = 0;
      size_t unquiesce_count = 0;

      ceph::mutex lock = ceph::make_mutex("lock");
      ceph::condition_variable cv;

      Watcher(librbd::Image &image, int r) : image(image), r(r) {
      }

      void reset_counters() {
        quiesce_count = 0;
        unquiesce_count = 0;
      }

      void handle_quiesce() override {
        quiesce_count++;
        image.quiesce_complete(handle, r);
      }

      void handle_unquiesce() override {
        std::unique_lock locker(lock);
        unquiesce_count++;
        cv.notify_one();
      }

      bool wait_for_unquiesce() {
        std::unique_lock locker(lock);
        return cv.wait_for(locker, seconds(60),
                           [this]() {
                             return quiesce_count == unquiesce_count;
                           });
      }
    } watcher10(image1, -EINVAL), watcher11(image1, 0), watcher20(image2, 0);

    ASSERT_EQ(0, image1.quiesce_watch(&watcher10, &watcher10.handle));
    ASSERT_EQ(0, image1.quiesce_watch(&watcher11, &watcher11.handle));
    ASSERT_EQ(0, image2.quiesce_watch(&watcher20, &watcher20.handle));

    ASSERT_EQ(-EINVAL, image1.snap_create("snap1"));
    ASSERT_GT(watcher10.quiesce_count, 0U);
    ASSERT_EQ(watcher10.unquiesce_count, 0U);
    ASSERT_GT(watcher11.quiesce_count, 0U);
    ASSERT_TRUE(watcher11.wait_for_unquiesce());
    ASSERT_GT(watcher20.quiesce_count, 0U);
    ASSERT_TRUE(watcher20.wait_for_unquiesce());

    PrintProgress prog_ctx;
    watcher10.reset_counters();
    watcher11.reset_counters();
    watcher20.reset_counters();
    ASSERT_EQ(0, image2.snap_create2("snap2",
                                     RBD_SNAP_CREATE_IGNORE_QUIESCE_ERROR,
                                     prog_ctx));
    ASSERT_GT(watcher10.quiesce_count, 0U);
    ASSERT_EQ(watcher10.unquiesce_count, 0U);
    ASSERT_GT(watcher11.quiesce_count, 0U);
    ASSERT_TRUE(watcher11.wait_for_unquiesce());
    ASSERT_GT(watcher20.quiesce_count, 0U);
    ASSERT_TRUE(watcher20.wait_for_unquiesce());

    ASSERT_EQ(0, image1.quiesce_unwatch(watcher10.handle));

    watcher11.reset_counters();
    watcher20.reset_counters();
    ASSERT_EQ(0, image1.snap_create("snap3"));
    ASSERT_GT(watcher11.quiesce_count, 0U);
    ASSERT_TRUE(watcher11.wait_for_unquiesce());
    ASSERT_GT(watcher20.quiesce_count, 0U);
    ASSERT_TRUE(watcher20.wait_for_unquiesce());

    ASSERT_EQ(0, image1.quiesce_unwatch(watcher11.handle));

    watcher20.reset_counters();
    ASSERT_EQ(0, image2.snap_create2("snap4", RBD_SNAP_CREATE_SKIP_QUIESCE,
                                     prog_ctx));
    ASSERT_EQ(watcher20.quiesce_count, 0U);
    ASSERT_EQ(watcher20.unquiesce_count, 0U);

    ASSERT_EQ(0, image2.quiesce_unwatch(watcher20.handle));

    ASSERT_EQ(0, image1.snap_remove("snap2"));
    ASSERT_EQ(0, image1.snap_remove("snap3"));
    ASSERT_EQ(0, image1.snap_remove("snap4"));
  }

  ASSERT_EQ(0, rbd.remove(ioctx, name.c_str()));
  ioctx.close();
}

TEST_F(TestLibRBD, QuiesceWatchTimeout)
{
  REQUIRE(!is_librados_test_stub(_rados));

  ASSERT_EQ(0, _rados.conf_set("rbd_quiesce_notification_attempts", "2"));

  librbd::RBD rbd;
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));
  std::string name = get_temp_image_name();
  int order = 0;
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  {
    librbd::Image image;
    ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

    struct Watcher : public librbd::QuiesceWatchCtx {
      librbd::Image &image;
      std::mutex m_lock;
      std::condition_variable m_cond;
      size_t quiesce_count = 0;
      size_t unquiesce_count = 0;

      Watcher(librbd::Image &image) : image(image) {
      }

      void handle_quiesce() override {
        std::lock_guard<std::mutex> locker(m_lock);
        quiesce_count++;
        m_cond.notify_one();
      }

      void handle_unquiesce() override {
        std::lock_guard<std::mutex> locker(m_lock);
        unquiesce_count++;
        m_cond.notify_one();
      }

      void wait_for_quiesce() {
        std::unique_lock<std::mutex> locker(m_lock);
        ASSERT_TRUE(m_cond.wait_for(locker, seconds(60),
                                    [this] {
                                      return quiesce_count >= 1;
                                    }));
      }

      void wait_for_unquiesce() {
        std::unique_lock<std::mutex> locker(m_lock);
        ASSERT_TRUE(m_cond.wait_for(locker, seconds(60),
                                    [this] {
                                      return quiesce_count == unquiesce_count;
                                    }));
        quiesce_count = unquiesce_count = 0;
      }
    } watcher(image);
    uint64_t handle;

    ASSERT_EQ(0, image.quiesce_watch(&watcher, &handle));

    std::cerr << "test quiesce is not long enough to time out" << std::endl;

    thread quiesce1([&image, &watcher, handle]() {
      watcher.wait_for_quiesce();
      sleep(8);
      image.quiesce_complete(handle, 0);
    });

    ASSERT_EQ(0, image.snap_create("snap1"));
    quiesce1.join();
    ASSERT_GE(watcher.quiesce_count, 1U);
    watcher.wait_for_unquiesce();

    std::cerr << "test quiesce is timed out" << std::endl;

    bool timed_out = false;
    thread quiesce2([&image, &watcher, handle, &timed_out]() {
      watcher.wait_for_quiesce();
      for (int i = 0; !timed_out && i < 60; i++) {
        std::cerr << "waiting for timed out ... " << i << std::endl;
        sleep(1);
      }
      image.quiesce_complete(handle, 0);
    });

    ASSERT_EQ(-ETIMEDOUT, image.snap_create("snap2"));
    timed_out = true;
    quiesce2.join();
    ASSERT_GE(watcher.quiesce_count, 1U);
    watcher.wait_for_unquiesce();

    thread quiesce3([&image, handle, &watcher]() {
      watcher.wait_for_quiesce();
      image.quiesce_complete(handle, 0);
    });

    std::cerr << "test retry succeeds" << std::endl;

    ASSERT_EQ(0, image.snap_create("snap2"));
    quiesce3.join();
    ASSERT_GE(watcher.quiesce_count, 1U);
    watcher.wait_for_unquiesce();

    ASSERT_EQ(0, image.snap_remove("snap1"));
    ASSERT_EQ(0, image.snap_remove("snap2"));
  }

  ASSERT_EQ(0, rbd.remove(ioctx, name.c_str()));
  ioctx.close();
}

TEST_F(TestLibRBD, WriteZeroes) {
  librbd::RBD rbd;
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));
  std::string name = get_temp_image_name();
  int order = 0;
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  // 1s from [0, 256) / length 256
  char data[256];
  memset(data, 1, sizeof(data));
  bufferlist bl;
  bl.append(data, 256);
  ASSERT_EQ(256, image.write(0, 256, bl));

  interval_set<uint64_t> diff;
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  auto expected_diff = interval_set<uint64_t>{{{0, 256}}};
  ASSERT_EQ(expected_diff, diff);

  // writes zero passed the current end extents.
  // Now 1s from [0, 192) / length 192
  ASSERT_EQ(size - 192,
            image.write_zeroes(192, size - 192, 0U, 0));
  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 192}}};
  ASSERT_EQ(expected_diff, diff);

  // zero an existing extent and truncate some off the end
  // Now 1s from [64, 192) / length 192
  ASSERT_EQ(64, image.write_zeroes(0, 64, 0U, 0));

  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 192}}};
  ASSERT_EQ(expected_diff, diff);

  bufferlist expected_bl;
  expected_bl.append_zero(64);
  bufferlist sub_bl;
  sub_bl.substr_of(bl, 0, 128);
  expected_bl.claim_append(sub_bl);
  expected_bl.append_zero(size - 192);

  bufferlist read_bl;
  EXPECT_EQ(size, image.read(0, size, read_bl));
  EXPECT_EQ(expected_bl, read_bl);

  ASSERT_EQ(0, image.close());
}

TEST_F(TestLibRBD, WriteZeroesThickProvision) {
  librbd::RBD rbd;
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));
  std::string name = get_temp_image_name();
  int order = 0;
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  librbd::Image image;
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), NULL));

  interval_set<uint64_t> diff;
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  auto expected_diff = interval_set<uint64_t>{{}};
  ASSERT_EQ(expected_diff, diff);

  // writes unaligned zeroes as a prepend
  ASSERT_EQ(128, image.write_zeroes(
              0, 128, RBD_WRITE_ZEROES_FLAG_THICK_PROVISION, 0));
  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 128}}};
  ASSERT_EQ(expected_diff, diff);

  ASSERT_EQ(512, image.write_zeroes(
              384, 512, RBD_WRITE_ZEROES_FLAG_THICK_PROVISION, 0));
  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 896}}};
  ASSERT_EQ(expected_diff, diff);

  // prepend with write-same
  ASSERT_EQ(640, image.write_zeroes(
              896, 640, RBD_WRITE_ZEROES_FLAG_THICK_PROVISION, 0));
  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 1536}}};
  ASSERT_EQ(expected_diff, diff);

  // write-same with append
  ASSERT_EQ(640, image.write_zeroes(
              1536, 640, RBD_WRITE_ZEROES_FLAG_THICK_PROVISION, 0));
  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 2176}}};
  ASSERT_EQ(expected_diff, diff);

  // prepend + write-same + append
  ASSERT_EQ(768, image.write_zeroes(
              2176, 768, RBD_WRITE_ZEROES_FLAG_THICK_PROVISION, 0));
  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 2944}}};

  // write-same
  ASSERT_EQ(1024, image.write_zeroes(
              3072, 1024, RBD_WRITE_ZEROES_FLAG_THICK_PROVISION, 0));
  diff.clear();
  ASSERT_EQ(0, image.diff_iterate2(nullptr, 0, size, false, false,
                                   iterate_cb, (void *)&diff));
  expected_diff = interval_set<uint64_t>{{{0, 4096}}};

  bufferlist expected_bl;
  expected_bl.append_zero(size);

  bufferlist read_bl;
  EXPECT_EQ(size, image.read(0, size, read_bl));
  EXPECT_EQ(expected_bl, read_bl);

  ASSERT_EQ(0, image.close());
}

TEST_F(TestLibRBD, ConcurrentOperations)
{
  SKIP_IF_CRIMSON();
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::RBD rbd;
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));
  std::string name = get_temp_image_name();
  int order = 0;
  uint64_t size = 2 << 20;
  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  // Test creating/removing many snapshots simultaneously

  std::vector<librbd::Image> images(10);
  std::vector<librbd::RBD::AioCompletion *> comps;

  for (auto &image : images) {
    auto comp = new librbd::RBD::AioCompletion(NULL, NULL);
    ASSERT_EQ(0, rbd.aio_open(ioctx, image, name.c_str(), NULL, comp));
    comps.push_back(comp);
  }

  for (auto &comp : comps) {
    ASSERT_EQ(0, comp->wait_for_complete());
    ASSERT_EQ(1, comp->is_complete());
    ASSERT_EQ(0, comp->get_return_value());
    comp->release();
  }
  comps.clear();

  std::vector<std::thread> threads;
  int i = 0;
  for (auto &image : images) {
    std::string snap_name = "snap" + stringify(i++);
    threads.emplace_back([&image, snap_name]() {
      int r = image.snap_create(snap_name.c_str());
      ceph_assert(r == 0);
    });
  }

  for (auto &t : threads) {
    t.join();
  }
  threads.clear();

  i = 0;
  for (auto &image : images) {
    std::string snap_name = "snap" + stringify(i++);
    threads.emplace_back([&image, snap_name](){
      int r = image.snap_remove(snap_name.c_str());
      ceph_assert(r == 0);
    });
  }

  for (auto &t : threads) {
    t.join();
  }
  threads.clear();

  for (auto &image : images) {
    auto comp = new librbd::RBD::AioCompletion(NULL, NULL);
    ASSERT_EQ(0, image.aio_close(comp));
    comps.push_back(comp);
  }

  for (auto &comp : comps) {
    ASSERT_EQ(0, comp->wait_for_complete());
    ASSERT_EQ(1, comp->is_complete());
    ASSERT_EQ(0, comp->get_return_value());
    comp->release();
  }
  comps.clear();

  // Test shutdown
  {
    librbd::Image image1, image2, image3;
    ASSERT_EQ(0, rbd.open(ioctx, image1, name.c_str(), NULL));
    ASSERT_EQ(0, rbd.open(ioctx, image2, name.c_str(), NULL));
    ASSERT_EQ(0, rbd.open(ioctx, image3, name.c_str(), NULL));

    ASSERT_EQ(0, image1.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE));

    struct Watcher : public librbd::QuiesceWatchCtx {
      size_t count = 0;

      ceph::mutex lock = ceph::make_mutex("lock");
      ceph::condition_variable cv;

      void handle_quiesce() override {
        std::unique_lock locker(lock);
        count++;
        cv.notify_one();
      }

      void handle_unquiesce() override {
      }

      bool wait_for_quiesce(size_t c) {
        std::unique_lock locker(lock);
        return cv.wait_for(locker, seconds(60),
                           [this, c]() { return count >= c; });
      }
    } watcher;
    uint64_t handle;
    ASSERT_EQ(0, image2.quiesce_watch(&watcher, &handle));

    auto close1_comp = new librbd::RBD::AioCompletion(NULL, NULL);

    std::thread create_snap1([&image1, close1_comp]() {
      int r = image1.snap_create("snap1");
      ceph_assert(r == 0);
      r = image1.aio_close(close1_comp);
      ceph_assert(r == 0);
    });

    ASSERT_TRUE(watcher.wait_for_quiesce(1));

    std::thread create_snap2([&image2]() {
      int r = image2.snap_create("snap2");
      ceph_assert(r == 0);
    });

    std::thread create_snap3([&image3]() {
      int r = image3.snap_create("snap3");
      ceph_assert(r == 0);
    });

    image2.quiesce_complete(handle, 0);
    create_snap1.join();

    ASSERT_TRUE(watcher.wait_for_quiesce(2));
    image2.quiesce_complete(handle, 0);

    ASSERT_TRUE(watcher.wait_for_quiesce(3));
    image2.quiesce_complete(handle, 0);

    ASSERT_EQ(0, close1_comp->wait_for_complete());
    ASSERT_EQ(1, close1_comp->is_complete());
    ASSERT_EQ(0, close1_comp->get_return_value());
    close1_comp->release();

    create_snap2.join();
    create_snap3.join();

    ASSERT_EQ(0, image2.quiesce_unwatch(handle));
    ASSERT_EQ(0, image2.snap_remove("snap1"));
    ASSERT_EQ(0, image2.snap_remove("snap2"));
    ASSERT_EQ(0, image2.snap_remove("snap3"));
  }

  ASSERT_EQ(0, rbd.remove(ioctx, name.c_str()));
  ioctx.close();
}


// poorman's ceph_assert()
namespace ceph {
  void __ceph_assert_fail(const char *assertion, const char *file, int line,
			  const char *func) {
    ceph_abort();
  }
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"
