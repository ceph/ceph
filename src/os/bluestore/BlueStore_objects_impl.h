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

#include <boost/intrusive_ptr.hpp>

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

  // forward declarations from BlueStore_object.h
  struct Collection;
  typedef boost::intrusive_ptr<Collection> CollectionRef;

  struct Blob;
  typedef boost::intrusive_ptr<Blob> BlobRef;

  struct Onode;
  typedef boost::intrusive_ptr<Onode> OnodeRef;
  struct OnodeSpace;
  struct OnodeCacheShard;

  struct Extent;
  struct ExtentMap;
  struct OldExtent;
  struct OldExtentMap;

  struct SharedBlob;
  typedef boost::intrusive_ptr<SharedBlob> SharedBlobRef;
  struct SharedBlobSet;

  struct TransContext;

  struct DeferredBatch;

  class OpSequencer;
  typedef boost::intrusive_ptr<OpSequencer> OpSequencerRef;
  struct deferred_osr_queue_t;

  struct WriteContext;

  struct BigDeferredWriteContext;

  struct GarbageCollector;

  struct printer;
}

#endif
