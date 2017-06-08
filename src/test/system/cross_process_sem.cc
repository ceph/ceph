// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2011 New Dream Network
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation.  See file COPYING.
*
*/

#include "cross_process_sem.h"

#include <errno.h>
#include <semaphore.h>
#include <stdlib.h>
#include <sys/mman.h>

/* We put our cross-process semaphore into a page of memory mapped with mmap. */
struct cross_process_sem_data_t
{
  sem_t sem;
};

/* A factory function is a good choice here because we want to be able to
 * return an error code. It does force heap allocation, but that is the
 * easiest way to use synchronization primitives anyway. Most programmers don't
 * care about destroying semaphores before the process finishes. It's pretty
 * difficult to get it right and there is usually no benefit.
 */
int CrossProcessSem::
create(int initial_val, CrossProcessSem** res)
{
  struct cross_process_sem_data_t *data = static_cast < cross_process_sem_data_t*> (
    mmap(NULL, sizeof(struct cross_process_sem_data_t),
       PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, 0, 0));
  if (data == MAP_FAILED) {
    int err = errno;
    return err;
  }
  int ret = sem_init(&data->sem, 1, initial_val);
  if (ret) {
    return ret;
  }
  *res = new CrossProcessSem(data);
  return 0;
}

CrossProcessSem::
~CrossProcessSem()
{
  munmap(m_data, sizeof(struct cross_process_sem_data_t));
  m_data = NULL;
}

void CrossProcessSem::
wait()
{
  while(true) {
    int ret = sem_wait(&m_data->sem);
    if (ret == 0)
      return;
    int err = errno;
    if (err == -EINTR)
      continue;
    abort();
  }
}

void CrossProcessSem::
post()
{
  int ret = sem_post(&m_data->sem);
  if (ret == -1) {
    abort();
  }
}

int CrossProcessSem::
reinit(int dval)
{
  if (dval < 0)
    return -EINVAL;
  int cval;
  if (sem_getvalue(&m_data->sem, &cval) == -1)
    return errno;
  if (cval < dval) {
    int diff = dval - cval;
    for (int i = 0; i < diff; ++i)
      sem_post(&m_data->sem);
  }
  else {
    int diff = cval - dval;
    for (int i = 0; i < diff; ++i)
      sem_wait(&m_data->sem);
  }
  return 0;
}

CrossProcessSem::
CrossProcessSem(struct cross_process_sem_data_t *data)
  : m_data(data)
{
}
