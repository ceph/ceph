// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_RAID4PG_H
#define CEPH_RAID4PG_H


#include "PG.h"

#include "messages/MOSDOp.h"


class RAID4PG : public PG {
public:  

protected:

  /*
  void prepare_log_transaction(ObjectStore::Transaction& t, 
			       MOSDOp *op, eversion_t& version, 
			       objectrev_t crev, objectrev_t rev,
			       eversion_t trim_to);
  void prepare_op_transaction(ObjectStore::Transaction& t, 
			      MOSDOp *op, eversion_t& version, 
			      objectrev_t crev, objectrev_t rev);
  */

  void op_stat(MOSDOp *op);
  int op_read(MOSDOp *op);
  void op_modify(MOSDOp *op);
  void op_rep_modify(MOSDOp *op);
  void op_push(MOSDOp *op);
  void op_pull(MOSDOp *op);


  void clean_up_local(ObjectStore::Transaction& t);
  void cancel_recovery();
  int start_recovery_ops(int m) { return 0; }

  bool snap_trimmer() { return true; }
  
public:
  RAID4PG(OSD *o, pg_t p) : PG(o,p) { }

  bool preprocess_op(MOSDOp *op, utime_t now);
  void do_op(MOSDOp *op);
  void do_sub_op(MOSDSubOp *op);
  void do_sub_op_reply(MOSDSubOpReply *r);

  bool same_for_read_since(epoch_t e);
  bool same_for_modify_since(epoch_t e);
  bool same_for_rep_modify_since(epoch_t e);

  bool is_missing_object(object_t oid);
  void wait_for_missing_object(object_t oid, Message *op);

  void on_osd_failure(int o);
  void on_acker_change();
  void on_role_change();
  void on_change();
  void on_shutdown() {}
};


#endif
