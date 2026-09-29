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
#include "rgw_acl.h"
#include "rgw_common.h"

namespace rgw::sal { class NSFSDriver; }
using rgw::sal::NSFSDriver;

/* The two records a multipart upload keeps on disk:  one for the
 * upload and one for each of its parts.
 *
 * Here rather than in the driver because both are the staging
 * layout's.  MPUStrategy spells `part-%05d`, `parts-size-<n>`, the
 * meta file and the contents of both records, and a strategy has to
 * be able to read what its own layout wrote.  They moved out of
 * rgw_sal_nsfs.h for that, and the driver includes them from here.
 *
 * NooBaa's format keeps the same facts differently -- three plain
 * attributes for a part, a JSON document for the upload -- which is
 * why reading either is a question MPUStrategy answers rather than
 * one the driver answers directly. */

/* What an upload's own record holds.
 *
 * `upload_info` is what CreateMultipartUpload settled and every later
 * operation must honour:  placement and storage class, object lock,
 * and the checksum algorithm.  MPUStrategy::upload_info() is the
 * question that yields it, and the identity above it -- key and
 * upload id -- is staged_upload()'s, which the listing path asks of
 * every entry and must stay cheap. */
#define RGW_NSFS_ATTR_MPUPLOAD "mp_upload"

namespace rgw { namespace sal {

struct NSFSMPObj {
  std::string oid;
  std::string upload_id;
  ACLOwner owner;
  multipart_upload_info upload_info;
  std::string meta;

  /* An upload without an id is a new one, so generate it.
   *
   * This used to try from_meta() first, treating the key as possibly
   * already being a meta string.  from_meta() splits at the last dot
   * and cannot tell a meta from a key that merely contains one, so
   * every key ending `.bin` or `.jpg` yielded the upload id `bin` or
   * `jpg` and no id was generated at all.  Two uploads in a bucket
   * whose keys shared a suffix then shared an upload id, and with the
   * staging directory named for that id they shared a directory:  each
   * completed object was assembled from both sets of parts, silently.
   *
   * A caller holding a real meta splits it itself and passes both
   * halves, because only that caller knows which it has. */
  NSFSMPObj(NSFSDriver* driver, const std::string& _oid,
	     std::optional<std::string> _upload_id, ACLOwner& _owner) {
    if (_upload_id && !_upload_id->empty()) {
      init(_oid, *_upload_id, _owner);
    } else if (!_oid.empty()) {
      init_gen(driver, _oid, _owner);
    }
  }
  /* parse <objname>.<uploadid> — the format produced by get_meta().
   * Only for a caller which knows it holds a meta;  an upload id
   * contains no dot, so the last one is the separator. */
  bool from_meta(const std::string& meta_name, ACLOwner& _owner) {
    auto pos = meta_name.rfind('.');
    if (pos == std::string::npos || pos == 0) return false;
    std::string _oid = meta_name.substr(0, pos);
    std::string _upload_id = meta_name.substr(pos + 1);
    if (_upload_id.empty()) return false;
    init(_oid, _upload_id, _owner);
    return true;
  }
  void init(const std::string& _oid, const std::string& _upload_id, ACLOwner& _owner) {
    oid = _oid;
    upload_id = _upload_id;
    owner = _owner;
    meta = oid + "." + upload_id;
  }
  void init_gen(NSFSDriver* driver, const std::string& _oid, ACLOwner& _owner);
  void clear() {
    oid = "";
    meta = "";
    upload_id = "";
  }
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(oid, bl);
    encode(upload_id, bl);
    encode(owner, bl);
    encode(upload_info, bl);
    encode(meta, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(oid, bl);
    decode(upload_id, bl);
    decode(owner, bl);
    decode(upload_info, bl);
    decode(meta, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(NSFSMPObj)

} } /* namespace rgw::sal */


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
