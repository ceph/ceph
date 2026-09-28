// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "include/buffer.h"
#include "include/encoding.h"
#include "common/ceph_time.h"

#include "rgw_cksum.h"

/* What a multipart part's record holds, in this format.
 *
 * Here rather than in the driver because the record is the staging
 * layout's:  MPUStrategy spells `part-%05d`, `parts-size-<n>` and this,
 * and a strategy has to be able to read what its own layout wrote.  It
 * moved out of rgw_sal_nsfs.h for that, and the driver includes it from
 * here.
 *
 * NooBaa's format keeps the same facts as three plain attributes of
 * their own instead, which is why reading a part's record is a question
 * MPUStrategy answers rather than one the driver answers directly. */
#define RGW_NSFS_ATTR_MPUPLOAD "mp_upload"

struct NSFSUploadPartInfo {
  uint32_t num{0};
  uint64_t size{0};
  std::string etag;
  ceph::real_time mtime;
  std::optional<rgw::cksum::Cksum> cksum;

  /* Where this part's bytes are, and how many.
   *
   * shared:  they are in the upload's shared data file rather than in
   * this part's own file, at `offset` within it.  A layout giving every
   * part its own file leaves this false and offset zero.
   *
   * stored:  bytes actually written.  NOT `size`, which is the
   * accounted, pre-filter length the client sent -- what ListParts must
   * report and what quota bills.  The two differ whenever compression
   * or AEAD is active, and only this one describes the file.
   *
   * Recording the location rather than inferring it from sizes is what
   * lets assembly cope with a part which was not placed:  one which
   * arrived before the stride was established, one which exceeded its
   * extent and diverted, and part 1 re-uploaded at a different size,
   * which invalidates every offset already assigned.  Inferring from
   * sizes would produce a corrupt object in that last case rather than
   * a slow one.
   *
   * It is also the only truthful source for part 1 under the strided
   * layout:  its file is linked to the shared file, so they are one
   * inode and a stat reports the whole upload. */
  bool shared{false};
  uint64_t offset{0};
  uint64_t stored{0};

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    encode(num, bl);
    encode(etag, bl);
    encode(mtime, bl);
    encode(cksum, bl);
    encode(size, bl);
    encode(shared, bl);
    encode(offset, bl);
    encode(stored, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 1, 1, bl);
    decode(num, bl);
    decode(etag, bl);
    decode(mtime, bl);
    if (struct_v > 1) {
      decode(cksum, bl);
    }
    if (struct_v > 2) {
      decode(size, bl);
    }
    if (struct_v > 3) {
      decode(shared, bl);
      decode(offset, bl);
      decode(stored, bl);
    } else {
      /* a record written before placement was recorded describes a part
       * in its own file;  no other layout existed then */
      shared = false;
      offset = 0;
      stored = size;
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(NSFSUploadPartInfo)
