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
#ifndef VERIFY_FILESTORE_H_
#define VERIFY_FILESTORE_H_

#include <iostream>
#include <fstream>
#include <set>
#include "os/FileStore.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>

#include "TestFileStoreState.h"
#include "DeterministicOpSequence.h"

class VerifyFileStore : public DeterministicOpSequence {

 private:
  boost::shared_ptr<ObjectStore> m_store_to_verify;
  bool m_do_verify;

 public:
  VerifyFileStore(FileStore *store_to_verify, FileStore *our_store,
      std::string status_file) :
        DeterministicOpSequence(our_store, status_file), m_do_verify(false) {
    m_store_to_verify.reset(store_to_verify);

//    int err;
//    err = m_store_to_verify->mount();
//    ceph_assert(err == 0);
  }
  ~VerifyFileStore() { }

  void generate(int seed, int verify_at);

 private:
  virtual void _do_touch(coll_t coll, hobject_t& obj);
  virtual void _do_write(coll_t coll, hobject_t& obj, uint64_t off,
      uint64_t len, const bufferlist& data);
  virtual void _do_clone(coll_t coll, hobject_t& orig_obj, hobject_t& new_obj);
  virtual void _do_coll_move(coll_t new_coll, coll_t old_coll, hobject_t& obj);
};

#endif /* VERIFY_FILESTORE_H_ */
