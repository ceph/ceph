// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MON_PROGRESS_EVENT_H
#define CEPH_MON_PROGRESS_EVENT_H

#include <list>
#include <string>

#include "include/encoding.h"

namespace ceph { class Formatter; }

struct ProgressEvent {
  std::string message;                  ///< event description
  float progress = 0.0f;                  ///< [0..1]
  bool add_to_ceph_s = false;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static std::list<ProgressEvent> generate_test_instances();
};
WRITE_CLASS_ENCODER(ProgressEvent)

#endif
