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

#define __STDC_FORMAT_MACROS
#include "include/assert.h"
#include "include/rbd/librbd.hpp"
#include "include/rados/librados.hpp"
#include "include/buffer.h"

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <errno.h>
#include <memory>
#include <sys/types.h>
#include <string>
#include <vector>

using namespace std;

#define TEST_IMAGE "testimg"
#define TEST_POOL "librbdtest"
#define TEST_SNAP "testsnap"
#define TEST_IO_SIZE 513
#define MB_BYTES(mb) (mb << 20)

librbd::RBD *rbd;

void test_create_and_stat(librados::IoCtx& io_ctx, const char *name, size_t size)
{
  librbd::image_info_t info;
  librbd::Image image;
  int order = 0;
  assert(rbd->create(io_ctx, name, size, &order) == 0);
  assert(rbd->open(io_ctx, image, name, NULL) == 0);
  assert(image.stat(info, sizeof(info)) == 0);
  cout << "image has size " << info.size << " and order " << info.order << endl;
  assert(info.size == size);
  assert(info.order == order);
}

void test_resize_and_stat(librbd::Image& image, size_t size)
{
  librbd::image_info_t info;
  assert(image.resize(size) == 0);
  assert(image.stat(info, sizeof(info)) == 0);
  cout << "image has size " << info.size << " and order " << info.order << endl;
  assert(info.size == size);
}

void test_ls(librados::IoCtx& io_ctx, size_t num_expected, ...)
{
  int r;
  size_t i;
  char *expected;
  va_list ap;
  vector<string> names;
  r = rbd->list(io_ctx, names);
  if (r == -ENOENT)
    r = 0;
  assert(r >= 0);
  cout << "num images is: " << names.size() << endl
       << "expected: " << num_expected << endl;
  assert(names.size() == num_expected);

  for (i = 0; i < names.size(); i++) {
    cout << "image: " << names[i] << endl;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    expected = va_arg(ap, char *);
    cout << "expected = " << expected << endl;
    vector<string>::iterator listed_name = find(names.begin(), names.end(), string(expected));
    assert(listed_name != names.end());
    names.erase(listed_name);
  }
  assert(names.empty());
}

void test_delete(librados::IoCtx& io_ctx, const char *name)
{
  assert(rbd->remove(io_ctx, name) == 0);
}

void test_create_snap(librbd::Image& image, const char *name)
{
  assert(image.snap_create(name) == 0);
}

void test_ls_snaps(librbd::Image& image, size_t num_expected, ...)
{
  int r;
  size_t i, j, expected_size;
  char *expected;
  va_list ap;
  vector<librbd::snap_info_t> snaps;
  r = image.snap_list(snaps);
  assert(r >= 0);
  cout << "num snaps is: " << snaps.size() << endl
       << "expected: " << num_expected << endl;
  assert(snaps.size() == num_expected);

  for (i = 0; i < snaps.size(); i++) {
    cout << "snap: " << snaps[i].name << endl;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    expected = va_arg(ap, char *);
    expected_size = va_arg(ap, int);
    int found = 0;
    for (j = 0; j < snaps.size(); j++) {
      if (snaps[j].name == "")
	continue;
      if (strcmp(snaps[j].name.c_str(), expected) == 0) {
	cout << "found " << snaps[j].name << " with size " << snaps[j].size << endl;
	assert(snaps[j].size == (size_t) expected_size);
	snaps[j].name = "";
	found = 1;
	break;
      }
    }
    assert(found);
  }

  for (i = 0; i < snaps.size(); i++) {
    assert(snaps[i].name == "");
  }
}

void test_delete_snap(librbd::Image& image, const char *name)
{
  assert(image.snap_remove(name) == 0);
}

void simple_write_cb(librbd::completion_t cb, void *arg)
{
  cout << "write completion cb called!" << endl;
}

void simple_read_cb(librbd::completion_t cb, void *arg)
{
  cout << "read completion cb called!" << endl;
}

void aio_write_test_data(librbd::Image& image, const char *test_data, off_t off)
{
  ceph::bufferlist bl;
  bl.append(test_data, strlen(test_data));
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb);
  printf("created completion\n");
  image.aio_write(off, strlen(test_data), bl, comp);
  printf("started write\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r == 0);
  printf("finished write\n");
  comp->release();
}

void write_test_data(librbd::Image& image, const char *test_data, off_t off)
{
  int written;
  size_t len = strlen(test_data);
  ceph::bufferlist bl;
  bl.append(test_data, len);
  written = image.write(off, len, bl);
  assert(written >= 0);
  printf("wrote: %u\n", (unsigned int) written);
}

void aio_read_test_data(librbd::Image& image, const char *expected, off_t off)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_read_cb);
  ceph::bufferlist bl;
  printf("created completion\n");
  image.aio_read(off, strlen(expected), bl, comp);
  printf("started read\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r == TEST_IO_SIZE - 1);
  assert(strncmp(expected, bl.c_str(), TEST_IO_SIZE - 1) == 0);
  printf("finished read\n");
  comp->release();
}

void read_test_data(librbd::Image& image, const char *expected, off_t off)
{
  int read, total_read = 0;
  size_t expected_len = strlen(expected);
  size_t len = expected_len;
  ceph::bufferlist bl;
  read = image.read(off + total_read, len, bl);
  assert(read >= 0);
  printf("read: %u\n", (unsigned int) read);
  printf("read: %s\nexpected: %s\n", bl.c_str(), expected);
  assert(strncmp(bl.c_str(), expected, expected_len) == 0);
}

void test_io(librados::IoCtx& io_ctx, librbd::Image& image)
{
  char test_data[TEST_IO_SIZE];
  int i;

  srand(time(0));
  for (i = 0; i < TEST_IO_SIZE - 1; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE - 1] = '\0';

  for (i = 0; i < 5; ++i)
    write_test_data(image, test_data, strlen(test_data) * i);

  for (i = 5; i < 10; ++i)
    aio_write_test_data(image, test_data, strlen(test_data) * i);

  for (i = 0; i < 5; ++i)
    read_test_data(image, test_data, strlen(test_data) * i);

  for (i = 5; i < 10; ++i)
    aio_read_test_data(image, test_data, strlen(test_data) * i);

}

int main(int argc, const char **argv) 
{
  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  rbd = new librbd::RBD();
  assert(rados.init(NULL) == 0);
  assert(rados.conf_read_file(NULL) == 0);
  rados.reopen_log();
  assert(rados.connect() == 0);
  if (rados.pool_lookup(TEST_POOL) != -ENOENT) {
    int r = rados.pool_delete(TEST_POOL);
    printf("rados.pool_delete returned %d\n", r);
  }
  int r = rados.pool_create(TEST_POOL);
  printf("rados.pool_create returned %d\n", r);
  assert(rados.ioctx_create(TEST_POOL, io_ctx) == 0);
  test_ls(io_ctx, 0);
  test_create_and_stat(io_ctx, TEST_IMAGE, MB_BYTES(1));
  assert(rbd->open(io_ctx, image, TEST_IMAGE, NULL) == 0);
  test_ls(io_ctx, 1, TEST_IMAGE);
  test_ls_snaps(image, 0);
  test_create_snap(image, TEST_SNAP);
  test_ls_snaps(image, 1, TEST_SNAP, MB_BYTES(1));
  test_resize_and_stat(image, MB_BYTES(2));
  test_io(io_ctx, image);
  test_create_snap(image, TEST_SNAP "1");
  test_ls_snaps(image, 2, TEST_SNAP, MB_BYTES(1), TEST_SNAP "1", MB_BYTES(2));
  test_delete_snap(image, TEST_SNAP);
  test_ls_snaps(image, 1, TEST_SNAP "1", MB_BYTES(2));
  test_delete_snap(image, TEST_SNAP "1");
  test_ls_snaps(image, 0);
  test_create_and_stat(io_ctx, TEST_IMAGE "1", MB_BYTES(2));
  test_ls(io_ctx, 2, TEST_IMAGE, TEST_IMAGE "1");
  test_delete(io_ctx, TEST_IMAGE);
  test_ls(io_ctx, 1, TEST_IMAGE "1");
  test_delete(io_ctx, TEST_IMAGE "1");
  test_ls(io_ctx, 0);
  delete rbd;
  return 0;
}
