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

#define __STDC_FORMAT_MACROS
#include "include/rbd/librbd.hpp"
#include "include/buffer.h"

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <errno.h>
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

void test_create_and_stat(librbd::pool_t pool, const char *name, size_t size)
{
  librbd::image_info_t info;
  librbd::image_t image;
  int order = 0;
  assert(rbd->create(pool, name, size, &order) == 0);
  assert(rbd->open_image(pool, name, &image, NULL) == 0);
  assert(rbd->stat(image, info) == 0);
  cout << "image has size " << info.size << " and order " << info.order << endl;
  assert(info.size == size);
  assert(info.order == order);
  assert(rbd->close_image(image) == 0);
}

void test_resize_and_stat(librbd::image_t image, size_t size)
{
  librbd::image_info_t info;
  assert(rbd->resize(image, size) == 0);
  assert(rbd->stat(image, info) == 0);
  cout << "image has size " << info.size << " and order " << info.order << endl;
  assert(info.size == size);
}

void test_ls(librbd::pool_t pool, size_t num_expected, ...)
{
  int r;
  size_t i;
  char *expected;
  va_list ap;
  vector<string> names;
  r = rbd->list(pool, names);
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

void test_delete(librbd::pool_t pool, const char *name)
{
  assert(rbd->remove(pool, name) == 0);
}

void test_create_snap(librbd::image_t image, const char *name)
{
  assert(rbd->create_snap(image, name) == 0);
}

void test_ls_snaps(librbd::image_t image, size_t num_expected, ...)
{
  int r;
  size_t i, j, expected_size;
  char *expected;
  va_list ap;
  vector<librbd::snap_info_t> snaps;
  r = rbd->list_snaps(image, snaps);
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

void test_delete_snap(librbd::image_t image, const char *name)
{
  assert(rbd->remove_snap(image, name) == 0);
}

void simple_write_cb(librbd::completion_t cb, void *arg)
{
  cout << "write completion cb called!" << endl;
}

void simple_read_cb(librbd::completion_t cb, void *arg)
{
  cout << "read completion cb called!" << endl;
}

void aio_write_test_data(librbd::image_t image, const char *test_data, off_t off)
{
  ceph::bufferlist bl;
  bl.append(test_data, strlen(test_data));
  librbd::RBD::AioCompletion *comp = rbd->aio_create_completion(NULL, (librbd::callback_t) simple_write_cb);
  printf("created completion\n");
  rbd->aio_write(image, off, strlen(test_data), bl, comp);
  printf("started write\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r == 0);
  printf("finished write\n");
  delete comp;
}

void write_test_data(librbd::image_t image, const char *test_data, off_t off)
{
  size_t written;
  size_t len = strlen(test_data);
  ceph::bufferlist bl;
  bl.append(test_data, len);
  written = rbd->write(image, off, len, bl);
  assert(written >= 0);
  printf("wrote: %u\n", (unsigned int) written);
}

void aio_read_test_data(librbd::image_t image, const char *expected, off_t off)
{
  librbd::RBD::AioCompletion *comp = rbd->aio_create_completion(NULL, (librbd::callback_t) simple_read_cb);
  ceph::bufferlist bl;
  printf("created completion\n");
  rbd->aio_read(image, off, strlen(expected), bl, comp);
  printf("started read\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r == TEST_IO_SIZE - 1);
  assert(strncmp(expected, bl.c_str(), TEST_IO_SIZE) == 0);
  printf("finished read\n");
  delete comp;
}

void read_test_data(librbd::image_t image, const char *expected, off_t off)
{
  size_t read, total_read = 0;
  size_t expected_len = strlen(expected);
  size_t len = expected_len;
  ceph::bufferlist bl;
  read = rbd->read(image, off + total_read, len, bl);
  assert(read >= 0);
  printf("read: %u\n", (unsigned int) read);
  printf("read: %s\nexpected: %s\n", bl.c_str(), expected);
  assert(strncmp(bl.c_str(), expected, expected_len) == 0);
}

void test_io(librbd::pool_t pool, librbd::image_t image)
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

  for (i = 0; i < 5; ++i)
    aio_write_test_data(image, test_data, strlen(test_data) * i);

  for (i = 0; i < 5; ++i)
    read_test_data(image, test_data, strlen(test_data) * i);

  for (i = 0; i < 5; ++i)
    aio_read_test_data(image, test_data, strlen(test_data) * i);

}

int main(int argc, const char **argv) 
{
  rbd = new librbd::RBD();
  librbd::pool_t pool;
  librbd::image_t image;
  assert(rbd->initialize(0, NULL) == 0);
  assert(rbd->open_pool(TEST_POOL, &pool) == 0);
  test_ls(pool, 0);
  test_create_and_stat(pool, TEST_IMAGE, MB_BYTES(1));
  assert(rbd->open_image(pool, TEST_IMAGE, &image, NULL) == 0);
  test_ls(pool, 1, TEST_IMAGE);
  test_ls_snaps(image, 0);
  test_create_snap(image, TEST_SNAP);
  test_ls_snaps(image, 1, TEST_SNAP, MB_BYTES(1));
  test_resize_and_stat(image, MB_BYTES(2));
  test_io(pool, image);
  test_create_snap(image, TEST_SNAP "1");
  test_ls_snaps(image, 2, TEST_SNAP, MB_BYTES(1), TEST_SNAP "1", MB_BYTES(2));
  test_delete_snap(image, TEST_SNAP);
  test_ls_snaps(image, 1, TEST_SNAP "1", MB_BYTES(2));
  test_delete_snap(image, TEST_SNAP "1");
  test_ls_snaps(image, 0);
  assert(rbd->close_image(image) == 0);
  test_create_and_stat(pool, TEST_IMAGE "1", MB_BYTES(2));
  test_ls(pool, 2, TEST_IMAGE, TEST_IMAGE "1");
  test_delete(pool, TEST_IMAGE);
  test_ls(pool, 1, TEST_IMAGE "1");
  test_delete(pool, TEST_IMAGE "1");
  test_ls(pool, 0);
  rbd->close_pool(pool);
  rbd->shutdown();
  delete rbd;
  return 0;
}
