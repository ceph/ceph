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

#include "include/encoding_string.h"
#include "common/Formatter.h"

struct ProgressEvent {
  std::string message;                  ///< event description
  float progress = 0.0f;                  ///< [0..1]
  bool add_to_ceph_s = false;
  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(message, bl);
    encode(progress, bl);
    encode(add_to_ceph_s, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(2, p);
    decode(message, p);
    decode(progress, p);
    if (struct_v >= 2){
	decode(add_to_ceph_s, p);
    } else {
      if (!message.empty()) {
	add_to_ceph_s = true;
      }
    }
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_string("message", message);
    f->dump_float("progress", progress);
    f->dump_bool("add_to_ceph_s", add_to_ceph_s);
  }
  static std::list<ProgressEvent> generate_test_instances() {
    std::list<ProgressEvent> o;
    o.emplace_back();
    o.emplace_back();
    o.back().message = "test message";
    o.back().progress = 0.5;
    o.back().add_to_ceph_s = true;
    return o;
  }
};
WRITE_CLASS_ENCODER(ProgressEvent)

#endif
