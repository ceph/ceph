// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

// essentially the same as ceph's PrCtl.h, copied into the dmclock library

#ifdef HAVE_SYS_PRCTL_H
#include <iostream>
#include <sys/prctl.h>
#include <errno.h>

struct PrCtl {
  int saved_state = -1;
  int set_dumpable(int new_state) {
    int r = prctl(PR_SET_DUMPABLE, new_state);
    if (r) {
      r = -errno;
      std::cerr << "warning: unable to " << (new_state ? "set" : "unset")
                << " dumpable flag: " << strerror(r)
                << std::endl;
    }
    return r;
  }
  PrCtl(int new_state = 0) {
    int r = prctl(PR_GET_DUMPABLE);
    if (r == -1) {
      r = errno;
      std::cerr << "warning: unable to get dumpable flag: " << strerror(r)
                << std::endl;
    } else if (r != new_state) {
      if (!set_dumpable(new_state)) {
        saved_state = r;
      }
    }
  }
  ~PrCtl() {
    if (saved_state < 0) {
      return;
    }
    set_dumpable(saved_state);
  }
};
#else
struct PrCtl {};
#endif
