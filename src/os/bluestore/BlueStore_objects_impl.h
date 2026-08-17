// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_OBJECTS_IMPL_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_OBJECTS_IMPL_H

#include <atomic>
#include <bit>
#include <mutex>
#include <condition_variable>
#include <memory_resource>
#include <new>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/optional.hpp>

#include "bluestore_types.h"

namespace bluestore {

  struct printer {
    static constexpr uint16_t PTR = 1;   // pointer to Blob
    static constexpr uint16_t NICK = 2;  // a nickname of this Blob
    static constexpr uint16_t DISK = 4;  // disk allocations of Blob
    static constexpr uint16_t SDISK = 8; // shortened version of disk allocaitons
    static constexpr uint16_t USE = 16;  // use tracker
    static constexpr uint16_t SUSE = 32; // shortened use tracker
    static constexpr uint16_t CHK = 64;  // checksum, full dump
    static constexpr uint16_t SCHK = 128; // only base checksum info
    static constexpr uint16_t BUF = 256;  // print Blob's buffers (takes cache lock)
    static constexpr uint16_t SBUF = 512; // short print Blob's buffers (takes cache lock)
    static constexpr uint16_t ATTRS = 1024; // print attrs in onode
    static constexpr uint16_t JUSTID = 2048; // used to suppress printing length, spanning and shared blob
  };

  struct Blob;
  typedef boost::intrusive_ptr<Blob> BlobRef;

  /// a logical extent, pointing to (some portion of) a blob
  typedef boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true> > ExtentBase; //making an alias to avoid build warnings

  struct Extent : public ExtentBase {
    MEMPOOL_CLASS_HELPERS();

    uint32_t logical_offset = 0;      ///< logical offset
    uint32_t blob_offset = 0;         ///< blob offset
    uint32_t length = 0;              ///< length
    BlobRef  blob;                    ///< the blob with our data

    /// ctor for lookup only
    explicit Extent(uint32_t lo) : ExtentBase(), logical_offset(lo) { }
    /// ctor for delayed initialization (see decode_some())
    explicit Extent() : ExtentBase() {
    }
    /// ctor for general usage
    Extent(uint32_t lo, uint32_t o, uint32_t l, BlobRef& b)
      : ExtentBase(),
        logical_offset(lo), blob_offset(o), length(l) {
      assign_blob(b);
    }
    ~Extent();
    struct printer : public bluestore::printer {
      const Extent& ext;
      uint16_t mode;
      printer(const Extent& ext, uint16_t mode)
      :ext(ext), mode(mode) {}
    };
    friend std::ostream& operator<<(std::ostream& out, const printer &p);
    printer print(uint16_t mode) const {
      return printer(*this, mode);
    }

    void dump(ceph::Formatter* f) const;

    void assign_blob(const BlobRef& b);

    // comparators for intrusive_set
    friend bool operator<(const Extent &a, const Extent &b) {
      return a.logical_offset < b.logical_offset;
    }
    friend bool operator>(const Extent &a, const Extent &b) {
      return a.logical_offset > b.logical_offset;
    }
    friend bool operator==(const Extent &a, const Extent &b) {
      return a.logical_offset == b.logical_offset;
    }

    uint32_t blob_start() const {
      return logical_offset - blob_offset;
    }

    uint32_t blob_end() const;

    uint32_t logical_end() const {
      return logical_offset + length;
    }

    // return true if any piece of the blob is out of
    // the given range [o, o + l].
    bool blob_escapes_range(uint32_t o, uint32_t l) const {
      return blob_start() < o || blob_end() > o + l;
    }
  };

  std::ostream& operator<<(std::ostream& out, const Extent& e);
}

#endif