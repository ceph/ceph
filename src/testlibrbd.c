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
#define TEST_IO_SIZE 513
#define MB_BYTES(mb) (mb << 20)

void test_create_and_stat(rbd_pool_t pool, const char *name, size_t size)
{
  rbd_image_info_t info;
  rbd_image_t image;
  int order = 0;
  assert(rbd_create(pool, name, size, &order) == 0);
  assert(rbd_open_image(pool, name, &image, NULL) == 0);
  assert(rbd_stat(image, &info) == 0);
  printf("image has size %llu and order %d\n", (unsigned long long) info.size, info.order);
  assert(info.size == size);
  assert(info.order == order);
  assert(rbd_close_image(image) == 0);
}

void test_resize_and_stat(rbd_image_t image, size_t size)
{
  rbd_image_info_t info;
  assert(rbd_resize(image, size) == 0);
  assert(rbd_stat(image, &info) == 0);
  printf("image has size %llu and order %d\n", (unsigned long long) info.size, info.order);
  assert(info.size == size);
}

void test_ls(rbd_pool_t pool, int num_expected, ...)
{
  char **names;
  int num_images, i, j;
  char *expected;
  va_list ap;
  names = (char **) malloc(sizeof(char **) * 10);
  num_images = rbd_list(pool, names, 10);
  printf("num images is: %d\nexpected: %d\n", num_images, num_expected);
  assert(num_images == num_expected);

  for (i = 0; i < num_images; i++) {
    printf("image: %s\n", names[i]);
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    expected = va_arg(ap, char *);
    printf("expected = %s\n", expected);
    int found = 0;
    for (j = 0; j < num_images; j++) {
      if (names[j] == NULL)
	continue;
      if (strcmp(names[j], expected) == 0) {
	printf("found %s\n", names[j]);
	free(names[j]);
	names[j] = NULL;
	found = 1;
	break;
      }
    }
    assert(found);
  }

  for (i = 0; i < num_images; i++) {
    assert(names[i] == NULL);
  }
  free(names);
}

void test_delete(rbd_pool_t pool, const char *name)
{
  assert(rbd_remove(pool, name) == 0);
}

void test_create_snap(rbd_image_t image, const char *name)
{
  assert(rbd_create_snap(image, name) == 0);
}

void test_ls_snaps(rbd_image_t image, int num_expected, ...)
{
  rbd_snap_info_t *snaps;
  int num_snaps, i, j, expected_size;
  char *expected;
  va_list ap;
  snaps = (rbd_snap_info_t *) malloc(sizeof(rbd_snap_info_t *) * 10);
  num_snaps = rbd_list_snaps(image, snaps, 10);
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
	assert(snaps[j].size == expected_size);
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
  assert(rbd_remove_snap(image, name) == 0);
}

void simple_write_cb(rbd_completion_t cb, void *arg)
{
  printf("write completion cb called!\n");
}

void simple_read_cb(rbd_completion_t cb, void *arg)
{
  printf("read completion cb called!\n");
}

void aio_write_test_data(rbd_image_t image, const char *test_data, off_t off)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  printf("created completion\n");
  rbd_aio_write(image, off, strlen(test_data), test_data, comp);
  printf("started write\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  assert(r == 0);
  printf("finished write\n");
  rbd_aio_release(comp);
}

void write_test_data(rbd_image_t image, const char *test_data, off_t off)
{
  size_t written;
  size_t len = strlen(test_data);
  while (len > 0) {
    written = rbd_write(image, off, len, test_data);
    assert(written >= 0);
    len -= written;
    off += written;
    printf("wrote: %u\n", (unsigned int) written);
  }
}

void aio_read_test_data(rbd_image_t image, const char *expected, off_t off)
{
  rbd_completion_t comp;
  char result[TEST_IO_SIZE];
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  printf("created completion\n");
  rbd_aio_read(image, off, strlen(expected), result, comp);
  printf("started read\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  assert(r == TEST_IO_SIZE - 1);
  assert(strncmp(expected, result, TEST_IO_SIZE) == 0);
  printf("finished read\n");
  rbd_aio_release(comp);
}

void read_test_data(rbd_image_t image, const char *expected, off_t off)
{
  size_t read, total_read = 0;
  size_t expected_len = strlen(expected);
  size_t len = expected_len;
  char result[TEST_IO_SIZE];
  char *buf = result;
  while (len > 0) {
    read = rbd_read(image, off + total_read, len, buf);
    assert(read >= 0);
    total_read += read;
    buf += read;
    len -= read;
    printf("read: %u\n", (unsigned int) read);
  }
  printf("read: %s\nexpected: %s\n", result, expected);
  assert(strncmp(result, expected, expected_len) == 0);
}

void test_io(rbd_pool_t pool, rbd_image_t image)
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
  rbd_pool_t pool;
  rbd_image_t image;
  assert(rbd_initialize(0, NULL) == 0);
  assert(rbd_open_pool(TEST_POOL, &pool) == 0);
  test_ls(pool, 0);
  test_create_and_stat(pool, TEST_IMAGE, MB_BYTES(1));
  assert(rbd_open_image(pool, TEST_IMAGE, &image, NULL) == 0);
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
  assert(rbd_close_image(image) == 0);
  test_create_and_stat(pool, TEST_IMAGE "1", MB_BYTES(2));
  test_ls(pool, 2, TEST_IMAGE, TEST_IMAGE "1");
  test_delete(pool, TEST_IMAGE);
  test_ls(pool, 1, TEST_IMAGE "1");
  test_delete(pool, TEST_IMAGE "1");
  test_ls(pool, 0);
  rbd_close_pool(pool);
  rbd_shutdown();
  return 0;
}
