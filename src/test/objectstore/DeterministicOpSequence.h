// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 New Dream Network
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef FILESTORE_DTRMNSTC_SEQ_OPS_H_
#define FILESTORE_DTRMNSTC_SEQ_OPS_H_

#include <iostream>
#include <fstream>
#include <set>
#include "os/ObjectStore.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>

#include "TestObjectStoreState.h"

typedef boost::mt11213b rngen_t;

class DeterministicOpSequence : public TestObjectStoreState {
 public:
  DeterministicOpSequence(ObjectStore *store,
			  std::string status = std::string());
  virtual ~DeterministicOpSequence();

  virtual void generate(int seed, int num_txs);

  static ghobject_t get_txn_object(coll_t cid) {
    ghobject_t oid(hobject_t(sobject_t("txn", CEPH_NOSNAP)));
    spg_t pgid;
    bool r = cid.is_pg(&pgid);
    if (r) {
      oid.hobj.set_hash(pgid.ps());
    }
    return oid;
  }

protected:
  enum {
    DSOP_TOUCH = 0,
    DSOP_WRITE = 1,
    DSOP_CLONE = 2,
    DSOP_CLONE_RANGE = 3,
    DSOP_OBJ_REMOVE = 4,
    DSOP_COLL_MOVE = 5,
    DSOP_SET_ATTRS = 6,
    DSOP_COLL_CREATE = 7,

    DSOP_FIRST = DSOP_TOUCH,
    DSOP_LAST = DSOP_COLL_CREATE,
  };

  int32_t txn;

  ObjectStore::CollectionHandle m_ch;
  std::ofstream m_status;

  bool run_one_op(int op, rngen_t& gen);

  void note_txn(coll_entry_t *entry, ObjectStore::Transaction *t);
  bool do_touch(rngen_t& gen);
  bool do_remove(rngen_t& gen);
  bool do_write(rngen_t& gen);
  bool do_clone(rngen_t& gen);
  bool do_clone_range(rngen_t& gen);
  bool do_coll_move(rngen_t& gen);
  bool do_set_attrs(rngen_t& gen);
  bool do_coll_create(rngen_t& gen);

  virtual void _do_touch(coll_entry_t *entry, hobject_t& obj);
  virtual void _do_remove(coll_entry_t *entry, hobject_t& obj);
  virtual void _do_write(coll_entry_t *entry, hobject_t& obj, uint64_t off,
      uint64_t len, const bufferlist& data);
  virtual void _do_set_attrs(coll_entry_t *entry,
			     hobject_t &obj,
			     const map<string, bufferlist> &attrs);
  virtual void _do_clone(coll_entry_t *entry, hobject_t& orig_obj, hobject_t& new_obj);
  virtual void _do_clone_range(coll_entry_t *entry, hobject_t& orig_obj,
      hobject_t& new_obj, uint64_t srcoff, uint64_t srclen, uint64_t dstoff);
  virtual void _do_write_and_clone_range(coll_entry_t *entry, hobject_t& orig_obj,
      hobject_t& new_obj, uint64_t srcoff, uint64_t srclen,
      uint64_t dstoff, bufferlist& bl);
  virtual void _do_coll_move(coll_entry_t *entry, hobject_t& orig_obj, hobject_t& new_obj);
  virtual void _do_coll_create(coll_entry_t *entry, uint32_t pg_num, uint64_t num_objs);

  int _gen_coll_id(rngen_t& gen);
  int _gen_obj_id(rngen_t& gen);
  void _print_status(int seq, int op);

 private:
  bool _prepare_clone(
    rngen_t& gen,
    coll_entry_t **entry_ret,
    int *orig_obj_id,
    hobject_t *orig_obj_ret,
    int *new_obj_id,
    hobject_t *new_obj_ret);
};


#endif /* FILESTORE_DTRMNSTC_SEQ_OPS_H_ */
