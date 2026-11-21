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
  // Types held by boost::intrusive_ptr below get their refcounting hooks
  // from hidden friends (Blob, Onode, SharedBlob) or from RefCountedObject
  // (OpSequencer). Neither is found until the type is complete, so declare
  // the hooks here for code that uses a *Ref before then.
  struct Collection;
  typedef boost::intrusive_ptr<Collection> CollectionRef;
  struct Blob;
  typedef boost::intrusive_ptr<Blob> BlobRef;
  void intrusive_ptr_add_ref(Blob *b);
  void intrusive_ptr_release(Blob *b);
  struct Onode;
  typedef boost::intrusive_ptr<Onode> OnodeRef;
  void intrusive_ptr_add_ref(Onode *o);
  void intrusive_ptr_release(Onode *o);
  struct OnodeSpace;
  struct OnodeCacheShard;
  struct Extent;
  struct ExtentMap;
  struct OldExtent;
  struct SharedBlob;
  typedef boost::intrusive_ptr<SharedBlob> SharedBlobRef;
  void intrusive_ptr_add_ref(SharedBlob *b);
  void intrusive_ptr_release(SharedBlob *b);
  struct SharedBlobSet;
  struct TransContext;
  struct DeferredBatch;
  class OpSequencer;
  typedef boost::intrusive_ptr<OpSequencer> OpSequencerRef;
  inline void intrusive_ptr_add_ref(OpSequencer *o);
  inline void intrusive_ptr_release(OpSequencer *o);
  struct deferred_osr_queue_t;
  struct WriteContext;
  struct BigDeferredWriteContext;
  struct GarbageCollector;
  struct AioContext;
  struct printer;
}

#endif
