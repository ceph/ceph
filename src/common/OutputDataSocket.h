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

#ifndef CEPH_COMMON_OUTPUTDATASOCKET_H
#define CEPH_COMMON_OUTPUTDATASOCKET_H

#include "common/ceph_mutex.h"
#include "common/Thread.h"
#include "include/buffer.h"

class CephContext;

class OutputDataSocket : public Thread
{
public:
  OutputDataSocket(CephContext *cct, uint64_t _backlog);
  ~OutputDataSocket() override;

  bool init(const std::string &path);
  
  void append_output(bufferlist& bl);

protected:
  virtual void init_connection(bufferlist& bl) {}
  void shutdown();

  std::string create_shutdown_pipe(int *pipe_rd, int *pipe_wr);
  std::string bind_and_listen(const std::string &sock_path, int *fd);

  void *entry() override;
  bool do_accept();

  void handle_connection(int fd);
  void close_connection(int fd);

  int dump_data(int fd);

  CephContext *m_cct;
  uint64_t data_max_backlog;
  std::string m_path;
  int m_sock_fd;
  int m_shutdown_rd_fd;
  int m_shutdown_wr_fd;
  bool going_down;

  uint64_t data_size;

  std::list<bufferlist> data;

  ceph::mutex m_lock = ceph::make_mutex("OutputDataSocket::m_lock");
  ceph::condition_variable cond;

  bufferlist delim;
};

#endif
