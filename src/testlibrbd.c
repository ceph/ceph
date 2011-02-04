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

#define TEST_IMAGE "testimg"
#define TEST_POOL "librbdtest"
#define TEST_SNAP "testsnap"
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
