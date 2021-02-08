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
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <keyutils.h>

#include "include/compat.h"
#include "common/armor.h"
#include "common/safe_io.h"

int read_secret_from_file(const char *filename, char *secret, size_t max_len)
{
  char *end;
  int fd;
  int len;

  fd = open(filename, O_RDONLY);
  if (fd < 0) {
    perror("unable to read secretfile");
    return -errno;
  }
  len = safe_read(fd, secret, max_len);
  if (len <= 0) {
    perror("unable to read secret from file");
    close(fd);
    return len ? len : -ENODATA;
  }
  end = secret;
  while (end < secret + len && *end && *end != '\n' && *end != '\r')
    end++;
  *end = '\0';
  close(fd);

  return 0;
}

int set_kernel_secret(const char *secret, const char *key_name)
{
  /* try to submit key to kernel via the keys api */
  key_serial_t serial;
  int ret;
  int secret_len = strlen(secret);
  char payload[((secret_len * 3) / 4) + 4];

  if (!secret_len) {
    fprintf(stderr, "secret is empty.\n");
    return -EINVAL;
  }

  ret = ceph_unarmor(payload, payload+sizeof(payload), secret, secret+secret_len);
  if (ret < 0) {
    char error_buf[80];
    fprintf(stderr, "secret is not valid base64: %s.\n",
	    ceph_strerror_r(-ret, error_buf, sizeof(error_buf)));
    return ret;
  }

  serial = add_key("ceph", key_name, payload, ret, KEY_SPEC_PROCESS_KEYRING);
  if (serial == -1) {
    ret = -errno;
  }

  return ret;
}

int is_kernel_secret(const char *key_name)
{
  key_serial_t serial;
  serial = request_key("ceph", key_name, NULL, KEY_SPEC_USER_KEYRING);
  return serial != -1;
}
