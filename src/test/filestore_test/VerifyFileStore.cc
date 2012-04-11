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
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include <sstream>
#include "os/FileStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "common/config.h"

#include "DeterministicOpSequence.h"
#include "VerifyFileStore.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "filestore_verify "

void VerifyFileStore::generate(int seed, int verify_at) {

  std::ostringstream ss;
  ss << "generate run " << verify_at << " --seed " << seed;

  if (m_status.is_open()) {
    m_status << ss.str() << std::endl;
    m_status.flush();
  }

  dout(0) << ss.str() << dendl;

  rngen_t gen(seed);
  boost::uniform_int<> op_rng(DSOP_FIRST, DSOP_LAST);

  for (int i = 1; i <= verify_at; i++) {
    if (i == verify_at)
      m_do_verify = true;

    int op = op_rng(gen);
    _print_status(i, op);
    dout(0) << "generate seq " << i << " op " << op << dendl;
    run_one_op(op, gen);
  }
}

void VerifyFileStore::_do_touch(coll_t coll, hobject_t& obj) {
  dout(0) << "verify _do_touch entry" << dendl;
  DeterministicOpSequence::_do_touch(coll, obj);

  if (!m_do_verify)
    return;

  dout(0) << "verify _do_touch exit" << dendl;
}

void VerifyFileStore::_do_write(coll_t coll, hobject_t& obj,
    uint64_t off, uint64_t len, const bufferlist& data)
{
  dout(0) << "verify _do_write entry" << dendl;
  DeterministicOpSequence::_do_write(coll, obj, off, len, data);

  if (!m_do_verify)
    return;

  int got;
  bufferlist our_data;
  got = m_store->read(coll, obj, off, len, our_data);

  if (got < 0) {
    dout(0) << "verify _do_write error: got " << got << dendl;
    return;
  }

  if (got != (int) len) {
    dout(0) << "verify _do_write error: got " << got
        << " but expected " << len << dendl;
    return;
  }

  m_store->umount();
  m_store_to_verify->mount();

  bufferlist their_data;
  got = m_store_to_verify->read(coll, obj, off, len, their_data);

  if (got < 0) {
    dout(0) << "verify _do_write error: got " << got << dendl;
    return;
  }

  if (got != (int) len) {
    dout(0) << "verify _do_write error: got " << got
        << " but expected " << len << dendl;
    return;
  }

  bufferlist::iterator our_it = our_data.begin();
  bufferlist::iterator their_it = their_data.begin();
  bool matching = true;
  for (int i = 0; !our_it.end() && !their_it.end(); ++our_it, ++their_it, ++i) {
    if (*our_it != *their_it) {
      dout(0) << "verify _do_write error: does not match at pos " << i << dendl;
      matching = false;
      break;
    }
  }

  if (!(our_it.end() && their_it.end())) {
    dout(0) << "verify _do_write error: no matching" << dendl;
    matching = false;
  }

  m_store_to_verify->umount();
  m_store->mount();

  dout(0) << "verify _do_write "
      << (matching ? "do" : "don't") << " match" << dendl;

  dout(0) << "verify _do_write exit" << dendl;
}

void VerifyFileStore::_do_clone(coll_t coll, hobject_t& orig_obj,
    hobject_t& new_obj)
{
  dout(0) << "verify _do_clone entry" << dendl;
  DeterministicOpSequence::_do_clone(coll, orig_obj, new_obj);

  if (!m_do_verify)
    return;

  dout(0) << "verify _do_clone exit" << dendl;
}

void VerifyFileStore::_do_coll_move(coll_t new_coll, coll_t old_coll,
    hobject_t& obj)
{
  dout(0) << "verify _do_coll_move entry" << dendl;
  DeterministicOpSequence::_do_coll_move(new_coll, old_coll, obj);

  if (!m_do_verify)
    return;

  dout(0) << "verify _do_coll_move exit" << dendl;
}
