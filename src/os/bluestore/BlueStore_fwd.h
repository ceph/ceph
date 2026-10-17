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

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_COMPONENTS_FWD_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_COMPONENTS_FWD_H
#include <boost/intrusive_ptr.hpp>

namespace bluestore {
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
  struct AioContext;
  struct printer;
}

#endif