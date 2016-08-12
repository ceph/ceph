// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECTRANSACTION_H
#define ECTRANSACTION_H

#include "OSD.h"
#include "PGBackend.h"
#include "ECUtil.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "PGTransaction.h"

namespace ECTransaction {
  void get_append_objects(
    const PGTransaction &t,
    set<hobject_t, hobject_t::BitwiseComparator> *out);
  void generate_transactions(
    PGTransaction &t,
    map<
      hobject_t, ECUtil::HashInfoRef, hobject_t::BitwiseComparator
      > &hash_infos,
    ErasureCodeInterfaceRef &ecimpl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    vector<pg_log_entry_t> &entries,
    map<shard_id_t, ObjectStore::Transaction> *transactions,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_added,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_removed,
    stringstream *out = 0);
};

#endif
