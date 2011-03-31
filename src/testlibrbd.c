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
#include "include/rados/librados.h"
#include "include/rbd/librbd.h"

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define TEST_IMAGE "testimg"
#define TEST_POOL "librbdtest"
#define TEST_SNAP "testsnap"
#define TEST_IO_SIZE 512
#define TEST_IO_TO_SNAP_SIZE 80
#define MB_BYTES(mb) (mb << 20)

void test_create_and_stat(rados_ioctx_t io_ctx, const char *name, size_t size)
{
  rbd_image_info_t info;
  rbd_image_t image;
  int order = 0;
  assert(rbd_create(io_ctx, name, size, &order) == 0);
  assert(rbd_open(io_ctx, name, &image, NULL) == 0);
  assert(rbd_stat(image, &info, sizeof(info)) == 0);
  printf("image has size %llu and order %d\n", (unsigned long long) info.size, info.order);
  assert(info.size == size);
  assert(info.order == order);
  assert(rbd_close(image) == 0);
}

void test_resize_and_stat(rbd_image_t image, size_t size)
{
  rbd_image_info_t info;
  assert(rbd_resize(image, size) == 0);
  assert(rbd_stat(image, &info, sizeof(info)) == 0);
  printf("image has size %llu and order %d\n", (unsigned long long) info.size, info.order);
  assert(info.size == size);
}

void test_ls(rados_ioctx_t io_ctx, size_t num_expected, ...)
{
  int num_images, i, j;
  char *expected, *names, *cur_name;
  va_list ap;
  size_t max_size = 1024;
  names = (char *) malloc(sizeof(char *) * 1024);
  printf("names is %p\n", names);
  num_images = rbd_list(io_ctx, names, &max_size);
  printf("names is %p\n", names);
  printf("num images is: %d\nexpected: %d\n", num_images, (int)num_expected);
  assert(num_images >= 0);
  assert(num_images == (int)num_expected);

  for (i = 0, cur_name = names; i < num_images; i++) {
    printf("image: %s\n", cur_name);
    cur_name += strlen(cur_name) + 1;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    expected = va_arg(ap, char *);
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

  for (i = 0, cur_name = names; i < num_images; i++) {
    assert(cur_name[0] == '_');
    cur_name += strlen(cur_name) + 1;
  }
  free(names);
}

void test_delete(rados_ioctx_t io_ctx, const char *name)
{
  assert(rbd_remove(io_ctx, name) == 0);
}

void test_create_snap(rbd_image_t image, const char *name)
{
  assert(rbd_snap_create(image, name) == 0);
}

void test_ls_snaps(rbd_image_t image, int num_expected, ...)
{
  rbd_snap_info_t *snaps;
  int num_snaps, i, j, expected_size, max_size = 10;
  char *expected;
  va_list ap;
  snaps = (rbd_snap_info_t *) malloc(sizeof(rbd_snap_info_t *) * 10);
  num_snaps = rbd_snap_list(image, snaps, &max_size);
  printf("num snaps is: %d\nexpected: %d\n", num_snaps, num_expected);
  assert(num_snaps == num_expected);

  for (i = 0; i < num_snaps; i++) {
    printf("snap: %s\n", snaps[i].name);
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    expected = va_arg(ap, char *);
    expected_size = va_arg(ap, int);
    int found = 0;
    for (j = 0; j < num_snaps; j++) {
      if (snaps[j].name == NULL)
	continue;
      if (strcmp(snaps[j].name, expected) == 0) {
	printf("found %s with size %llu\n", snaps[j].name, (unsigned long long) snaps[j].size);
	assert((int)snaps[j].size == expected_size);
	free((void *) snaps[j].name);
	snaps[j].name = NULL;
	found = 1;
	break;
      }
    }
    assert(found);
  }

  for (i = 0; i < num_snaps; i++) {
    assert(snaps[i].name == NULL);
  }
  free(snaps);
}

void test_delete_snap(rbd_image_t image, const char *name)
{
  assert(rbd_snap_remove(image, name) == 0);
}

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
  assert(written == len);
}

void aio_read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len)
{
  rbd_completion_t comp;
  char *result;
  assert((result = malloc(sizeof(result) * (len + 1))) != 0);

  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  printf("created completion\n");
  rbd_aio_read(image, off, len, result, comp);
  printf("started read\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  assert(r == len);
  rbd_aio_release(comp);
  printf("read: %s\nexpected: %s\n", result, expected);
  assert(memcmp(result, expected, len) == 0);
  free(result);
}

void read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len)
{
  ssize_t read;
  char *result;
  assert((result = malloc(sizeof(result) * (len + 1))) != 0);

  read = rbd_read(image, off, len, result);
  printf("read: %d\n", (int) read);
  assert(read == len);
  result[len] = '\0';
  printf("read: %s\nexpected: %s\n", result, expected);
  assert(memcmp(result, expected, len) == 0);
  free(result);
}

void test_io(rados_ioctx_t io, rbd_image_t image)
{
  char test_data[TEST_IO_SIZE + 1];
  int i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';

  for (i = 0; i < 5; ++i)
    write_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_write_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 0; i < 5; ++i)
    read_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_read_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
}

void test_io_to_snapshot(rados_ioctx_t io_ctx, rbd_image_t image, size_t isize)
{
  int i, r;
  rbd_image_t image_at_snap;
  char orig_data[TEST_IO_TO_SNAP_SIZE + 1];
  char test_data[TEST_IO_TO_SNAP_SIZE + 1];

  for (i = 0; i < TEST_IO_TO_SNAP_SIZE - 1; ++i)
    test_data[i] = (char) (i + 48);
  test_data[TEST_IO_TO_SNAP_SIZE] = '\0';

  r = rbd_read(image, 0, TEST_IO_TO_SNAP_SIZE, orig_data);
  assert(r == TEST_IO_TO_SNAP_SIZE);

  test_ls_snaps(image, 0);
  test_create_snap(image, "orig");
  test_ls_snaps(image, 1, "orig", isize);
  read_test_data(image, orig_data, 0, TEST_IO_TO_SNAP_SIZE);

  printf("write test data!\n");
  write_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);
  test_create_snap(image, "written");
  test_ls_snaps(image, 2, "orig", isize, "written", isize);

  read_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  rbd_snap_set(image, "orig");
  read_test_data(image, orig_data, 0, TEST_IO_TO_SNAP_SIZE);

  rbd_snap_set(image, "written");
  read_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  rbd_snap_set(image, "orig");

  r = rbd_write(image, 0, TEST_IO_TO_SNAP_SIZE, test_data);
  printf("write to snapshot returned %d\n", r);
  assert(r < 0);
  printf("%s\n", strerror(-r));

  read_test_data(image, orig_data, 0, TEST_IO_TO_SNAP_SIZE);
  rbd_snap_set(image, "written");
  read_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  r = rbd_snap_rollback(image, "orig");
  printf("rbd_snap_rollback returned %d\n", r);
  assert(r >= 0);

  r = rbd_snap_set(image, NULL);
  assert(r == 0);
  write_test_data(image, test_data, 0, TEST_IO_TO_SNAP_SIZE);

  printf("opening testimg@orig\n");
  assert(rbd_open(io_ctx, TEST_IMAGE, &image_at_snap, "orig") >= 0);
  read_test_data(image_at_snap, orig_data, 0, TEST_IO_TO_SNAP_SIZE);
  r = rbd_write(image_at_snap, 0, TEST_IO_TO_SNAP_SIZE, test_data);
  printf("write to snapshot returned %d\n", r);
  assert(r < 0);
  printf("%s\n", strerror(-r));
  assert(rbd_close(image_at_snap) == 0);

  test_ls_snaps(image, 2, "orig", isize, "written", isize);
  test_delete_snap(image, "written");
  test_ls_snaps(image, 1, "orig", isize);
  test_delete_snap(image, "orig");
  test_ls_snaps(image, 0);
}

int main(int argc, const char **argv) 
{
  rados_t cluster;
  rados_ioctx_t io_ctx;
  rbd_image_t image;

  srand(time(0));

  assert(rados_create(&cluster, NULL) == 0);
  assert(rados_conf_read_file(cluster, NULL) == 0);
  rados_reopen_log(cluster);
  assert(rados_connect(cluster) == 0);

  if (rados_pool_lookup(cluster, TEST_POOL) != -ENOENT) {
    int r = rados_pool_delete(cluster, TEST_POOL);
    printf("rados_pool_delete returned %d\n", r);
  }
  int r = rados_pool_create(cluster, TEST_POOL);
  printf("rados_pool_create returned %d\n", r);

  assert(rados_ioctx_create(cluster, TEST_POOL, &io_ctx) == 0);
  test_ls(io_ctx, 0);

  test_create_and_stat(io_ctx, TEST_IMAGE, MB_BYTES(1));
  assert(rbd_open(io_ctx, TEST_IMAGE, &image, NULL) == 0);

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

  test_io_to_snapshot(io_ctx, image, MB_BYTES(2));
  assert(rbd_close(image) == 0);

  test_create_and_stat(io_ctx, TEST_IMAGE "1", MB_BYTES(2));
  test_ls(io_ctx, 2, TEST_IMAGE, TEST_IMAGE "1");

  test_delete(io_ctx, TEST_IMAGE);
  test_ls(io_ctx, 1, TEST_IMAGE "1");

  test_delete(io_ctx, TEST_IMAGE "1");
  test_ls(io_ctx, 0);

  rados_ioctx_destroy(io_ctx);
  rados_shutdown(cluster);

  return 0;
}
