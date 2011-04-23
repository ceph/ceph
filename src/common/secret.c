// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <keyutils.h>
#include <sys/types.h>

#include "common/armor.h"

int read_secret_from_file(const char *filename, char *secret, size_t max_len)
{
  char *end;
  int fd;
  int len;

  fd = open(filename, O_RDONLY);
  if (fd < 0) {
    perror("unable to read secretfile");
    return -1;
  }
  len = read(fd, secret, max_len);
  if (len <= 0) {
    perror("unable to read secret from file");
    return -1;
  }
  end = secret;
  while (end < secret + len && *end && *end != '\n' && *end != '\r')
    end++;
  *end = '\0';
  close(fd);

  return 0;
}

static int add_secret_to_kernel(const char *secret, const char *key_name)
{
  /* try to submit key to kernel via the keys api */
  key_serial_t serial;
  int ret;
  int secret_len = strlen(secret);
  char payload[((secret_len * 3) / 4) + 4];

  ret = ceph_unarmor(payload, payload+sizeof(payload), secret, secret+secret_len);
  if (ret < 0) {
    printf("secret is not valid base64: %s.\n", strerror(-ret));
    return ret;
  }

  serial = add_key("ceph", key_name, payload, sizeof(payload), KEY_SPEC_USER_KEYRING);
  if (serial < 0) {
    printf("error adding secret to kernel %s %s: %s", key_name, payload, strerror(errno));
    ret = -errno;
  }

  return ret;
}

int get_secret_option(const char *secret, const char *key_name, char *secret_option, size_t max_len)
{
  int ret;
  char option[strlen(secret) + strlen(key_name) + 7];

  ret = add_secret_to_kernel(secret, key_name);
  if (ret < 0) {
    if (ret == -ENODEV || ret == -ENOSYS) {
      /* running against older kernel; fall back to secret= in options */
      sprintf(option, "secret=%s", secret);
      ret = 0;
    } else {
      printf("adding ceph secret key to kernel failed: %s.\n", strerror(-ret));
      return ret;
    }
  } else {
    /* add key= option to identify key to use */
    sprintf(option, "key=%s", key_name);
  }

  if (strlen(option) + 1 > max_len) {
    ret = -ERANGE;
  } else {
    strcpy(secret_option, option);
  }

  return ret;
}
