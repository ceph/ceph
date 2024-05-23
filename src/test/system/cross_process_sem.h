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

struct cross_process_sem_data_t;

class CrossProcessSem
{
public:
  static int create(int initial_val, CrossProcessSem** ret);
  ~CrossProcessSem();

  /* Initialize the semaphore. Must be called before any operations */
  int init();

  /* Semaphore wait */
  void wait();

  /* Semaphore post */
  void post();

  /* Reinitialize the semaphore to the desired value.
   * NOT thread-safe if it is in use at the time!
   */ 
  int reinit(int dval);

private:
  explicit CrossProcessSem(struct cross_process_sem_data_t *data);
  struct cross_process_sem_data_t *m_data;
};
