// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Lluis Pamies-Juarez <lluis@pamies.cat>
 * Copyright (C) 2013 HGST
 *
 * Author: Lluis Pamies-Juarez <lluis@pamies.cat>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_ENCODEDPG_H
#define CEPH_ENCODEDPG_H

#include <boost/optional.hpp>

#include "include/assert.h"
#include "common/cmdparse.h"

#include "PG.h"
#include "OSD.h"
#include "Watch.h"
#include "OpRequest.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"

#include "common/sharedptr_registry.hpp"

class EncodedPG : public PG {
public:
  EncodedPG(OSDService *o, OSDMapRef curmap,
	       const PGPool &_pool, pg_t p, const hobject_t& oid,
	       const hobject_t& ioid);
  ~EncodedPG() {}

  void on_removal(ObjectStore::Transaction *t);

  void mark_all_unfound_lost(int how);
  void dump_recovery_info(ceph::Formatter* f) const;
  void calc_trim_to();
  void check_local();
  int start_recovery_ops(int max, RecoveryCtx *prctx,
    ThreadPool::TPHandle &handle);

  void _clear_recovery_state();
  void check_recovery_sources(OSDMapRef);
  void _split_into(pg_t, PG*, unsigned int);
  coll_t get_temp_coll();
  bool have_temp_coll();

  void do_request(
    OpRequestRef op,
    ThreadPool::TPHandle &handle
  );

  void handle_watch_timeout(WatchRef watch);

  void do_op(OpRequestRef op);
  void do_sub_op(OpRequestRef op);
  void do_sub_op_reply(OpRequestRef op);
  void do_scan(
    OpRequestRef op,
    ThreadPool::TPHandle &handle
  );
  void do_backfill(OpRequestRef op);
  void do_push(OpRequestRef op);
  void do_pull(OpRequestRef op);
  void do_push_reply(OpRequestRef op);
  void snap_trimmer();

  int do_command(cmdmap_t cmdmap, ostream& ss,
			 bufferlist& idata, bufferlist& odata);

  bool same_for_read_since(epoch_t e);
  bool same_for_modify_since(epoch_t e);
  bool same_for_rep_modify_since(epoch_t e);

  void on_role_change();
  void on_change(ObjectStore::Transaction *t);
  void on_activate();
  void on_flushed();
  void on_shutdown();
  void check_blacklisted_watchers();
  void get_watchers(std::list<obj_watch_item_t>&);
};

#endif
