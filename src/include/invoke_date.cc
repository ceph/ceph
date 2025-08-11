// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "utime.h"
#include "common/safe_io.h"
#include "common/SubProcess.h"

#include <sstream>

#include <errno.h>

int utime_t::invoke_date(const std::string& date_str, utime_t *result) {
  char buf[256];

  SubProcess bin_date("/bin/date", SubProcess::CLOSE, SubProcess::PIPE,
						      SubProcess::KEEP);
  bin_date.add_cmd_args("-d", date_str.c_str(), "+%s %N", NULL);

  int r = bin_date.spawn();
  if (r < 0) return r;

  ssize_t n = safe_read(bin_date.get_stdout(), buf, sizeof(buf));

  r = bin_date.join();
  if (r || n <= 0) return -EINVAL;

  uint64_t epoch, nsec;
  std::istringstream iss(buf);

  iss >> epoch;
  iss >> nsec;

  *result = utime_t(epoch, nsec);

  return 0;
}
