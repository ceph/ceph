// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef BLUESTORE_DEBUG
#define BLUESTORE_DEBUG

#include "BlueStore.h"
#include "Writer.h"

/*
 * BlueStore_debug gathers functions that are needed for in-depth testing.
 * The functions defined here are not to be used to provide any production functionality;
 * instead, functions here are allowed to work properly only in limited context
 * provided their side effect do not corrupt wider BlueStore state.
 */

class BlueStore_debug : public BlueStore
{
  public:

  // create txc for BlueStore operation
  TransContext* create_TransContext(
    CollectionHandle& ch,          // collection to operate on
    Context* on_commit = nullptr); // on commit completion

  // push transaction into BlueStore state machine
  void exec_TransContext(
    TransContext* txc); // txc created by create_TransContext

  // get disk allocation units used by the object
  void add_Transaction(
    TransContext* txc, // txc created by create_TransContext
    Transaction* t);   // Transaction to execute; competion Contexts are ignored

  // invoke Writer::write_no_read
  void write_no_read(
    TransContext* txc, // txc created by create_TransContext
    coll_t cid,
    ghobject_t oid,
    uint32_t fadvise_flags,
    uint32_t location,
    bufferlist& data);

  // get allocations that are in use by the oid
  void get_used_disk(
    coll_t cid,
    ghobject_t oid,
    interval_set<uint64_t, std::map, false>& disk_used); // appends, does not clear

};

#endif