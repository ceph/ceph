// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once
#include "include/rados/buffer.h"
#include "include/encoding.h"
#include "common/Formatter.h"
#include <string>

// Control-plane-only bucket attribute. Affects only the dedup background
// worker (whether it processes this bucket). Has no impact on S3 data-plane
// operations (PutObject, GetObject, DeleteObject, etc.).

struct RGWBucketDedupPolicy {
  enum Status : uint8_t {
    NONE = 0,     // no policy set (default: dedup allowed)
    ENABLED = 1,  // dedup explicitly allowed
    DISABLED = 2  // dedup explicitly denied
  };

  Status status = NONE;

  bool is_dedup_denied() const {
    return status == DISABLED;
  }

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(static_cast<uint8_t>(status), bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    uint8_t s;
    decode(s, bl);
    status = static_cast<Status>(s);
    DECODE_FINISH(bl);
  }

  void dump_xml(ceph::Formatter *f) const {
    const char* status_str = (status == DISABLED) ? "Disabled" : "Enabled";
    f->open_object_section("DedupConfiguration");
    f->dump_string("Status", status_str);
    f->close_section();
  }

  static bool decode_xml_status(const std::string& status_str, Status& out) {
    if (status_str == "Enabled") {
      out = ENABLED;
      return true;
    } else if (status_str == "Disabled") {
      out = DISABLED;
      return true;
    }
    return false;
  }
};
WRITE_CLASS_ENCODER(RGWBucketDedupPolicy)
