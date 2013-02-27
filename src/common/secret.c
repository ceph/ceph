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
#include "common/safe_io.h"

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
  len = safe_read(fd, secret, max_len);
  if (len <= 0) {
    perror("unable to read secret from file");
    close(fd);
    return -1;
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
	    strerror_r(-ret, error_buf, sizeof(error_buf)));
    return ret;
  }

  serial = add_key("ceph", key_name, payload, sizeof(payload), KEY_SPEC_PROCESS_KEYRING);
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

int get_secret_option(const char *secret, const char *key_name,
		      char *secret_option, size_t max_len)
{
  if (!key_name) {
    return -EINVAL;
  }

  int ret = 0;
  int olen = strlen(key_name) + 7;
  if (secret) {
    olen += strlen(secret);
  }
  char option[olen+1];
  int use_key = 1;

  option[olen] = '\0';


  if (secret) {
    ret = set_kernel_secret(secret, key_name);
    if (ret < 0) {
      if (ret == -ENODEV || ret == -ENOSYS) {
	/* running against older kernel; fall back to secret= in options */
	snprintf(option, olen, "secret=%s", secret);
	ret = 0;
	use_key = 0;
      } else {
        char error_buf[80];
	fprintf(stderr, "adding ceph secret key to kernel failed: %s.\n",
		strerror_r(-ret, error_buf, sizeof(error_buf)));
	return ret;
      }
    }
  }

  if (use_key) {
    /* add key= option to identify key to use */
    snprintf(option, olen, "key=%s", key_name);
  }

  if (strlen(option) + 1 > max_len) {
    ret = -ERANGE;
  } else {
    secret_option[max_len-1] = '\0';
    strncpy(secret_option, option, max_len-1);
  }

  return ret;
}
