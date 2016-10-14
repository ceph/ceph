// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Sebastien Ponce <sebastien.ponce@cern.ch>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/dout.h"

#include "libradosstriper/MultiAioCompletionImpl.h"

void libradosstriper::MultiAioCompletionImpl::complete_request(ssize_t r)
{
  lock.Lock();
  if (rval >= 0) {
    if (r < 0 && r != -EEXIST)
      rval = r;
    else if (r > 0)
      rval += r;
  }
  assert(pending_complete);
  int count = --pending_complete;
  if (!count && !building) {
    complete();
  }
  put_unlock();
}

void libradosstriper::MultiAioCompletionImpl::safe_request(ssize_t r)
{
  lock.Lock();
  if (rval >= 0) {
    if (r < 0 && r != -EEXIST)
      rval = r;
  }
  assert(pending_safe);
  int count = --pending_safe;
  if (!count && !building) {
    safe();
  }
  put_unlock();
}

void libradosstriper::MultiAioCompletionImpl::finish_adding_requests()
{
  lock.Lock();
  assert(building);
  building = false;
  if (!pending_complete)
    complete();
  if (!pending_safe)
    safe();
  lock.Unlock();
}
