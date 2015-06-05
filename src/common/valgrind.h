// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_VALGRIND_H
#define CEPH_VALGRIND_H

#ifdef HAVE_VALGRIND_HELGRIND_H
  #include <valgrind/helgrind.h>
#else
  #define ANNOTATE_HAPPENS_AFTER(x)             do {} while (0)
  #define ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(x) ANNOTATE_HAPPENS_AFTER(x)
  #define ANNOTATE_HAPPENS_BEFORE(x)            ANNOTATE_HAPPENS_AFTER(x)
#endif

#endif // CEPH_VALGRIND_H
