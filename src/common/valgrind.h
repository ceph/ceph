// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_VALGRIND_H
#define CEPH_VALGRIND_H

#include "acconfig.h"

#if defined(HAVE_VALGRIND_HELGRIND_H) && !defined(NDEBUG)
  #include <valgrind/helgrind.h>
#else
  #define ANNOTATE_HAPPENS_AFTER(x)             (void)0
  #define ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(x) (void)0
  #define ANNOTATE_HAPPENS_BEFORE(x)            (void)0

  #define ANNOTATE_BENIGN_RACE_SIZED(address, size, description) (void)0
#endif

#endif // CEPH_VALGRIND_H
