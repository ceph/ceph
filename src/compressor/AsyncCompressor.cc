// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/dout.h"
#include "common/errno.h"
#include "AsyncCompressor.h"

#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix *_dout << "compressor "

AsyncCompressor::AsyncCompressor(CephContext *c):
  compressor(Compressor::create(c, c->_conf->async_compressor_type)), cct(c),
  job_id(0),
  compress_tp(cct, "AsyncCompressor::compressor_tp", "tp_async_compr", cct->_conf->async_compressor_threads, "async_compressor_threads"),
  job_lock("AsyncCompressor::job_lock"),
  compress_wq(this, c->_conf->async_compressor_thread_timeout, c->_conf->async_compressor_thread_suicide_timeout, &compress_tp) {
}

void AsyncCompressor::init()
{
  ldout(cct, 10) << __func__ << dendl;
  compress_tp.start();
}

void AsyncCompressor::terminate()
{
  ldout(cct, 10) << __func__ << dendl;
  compress_tp.stop();
}

uint64_t AsyncCompressor::async_compress(bufferlist &data)
{
  uint64_t id = job_id.inc();
  pair<unordered_map<uint64_t, Job>::iterator, bool> it;
  {
    Mutex::Locker l(job_lock);
    it = jobs.insert(make_pair(id, Job(id, true)));
    it.first->second.data = data;
  }
  compress_wq.queue(&it.first->second);
  ldout(cct, 10) << __func__ << " insert async compress job id=" << id << dendl;
  return id;
}

uint64_t AsyncCompressor::async_decompress(bufferlist &data)
{
  uint64_t id = job_id.inc();
  pair<unordered_map<uint64_t, Job>::iterator, bool> it;
  {
    Mutex::Locker l(job_lock);
    it = jobs.insert(make_pair(id, Job(id, false)));
    it.first->second.data = data;
  }
  compress_wq.queue(&it.first->second);
  ldout(cct, 10) << __func__ << " insert async decompress job id=" << id << dendl;
  return id;
}

int AsyncCompressor::get_compress_data(uint64_t compress_id, bufferlist &data, bool blocking, bool *finished)
{
  assert(finished);
  Mutex::Locker l(job_lock);
  unordered_map<uint64_t, Job>::iterator it = jobs.find(compress_id);
  if (it == jobs.end() || !it->second.is_compress) {
    ldout(cct, 10) << __func__ << " missing to get compress job id=" << compress_id << dendl;
    return -ENOENT;
  }
  int status;

 retry:
  status = it->second.status.read();
  if (status == DONE) {
    ldout(cct, 20) << __func__ << " successfully getting compressed data, job id=" << compress_id << dendl;
    *finished = true;
    data.swap(it->second.data);
    jobs.erase(it);
  } else if (status == ERROR) {
    ldout(cct, 20) << __func__ << " compressed data failed, job id=" << compress_id << dendl;
    jobs.erase(it);
    return -EIO;
  } else if (blocking) {
    if (it->second.status.compare_and_swap(WAIT, DONE)) {
      ldout(cct, 10) << __func__ << " compress job id=" << compress_id << " hasn't finished, abort!"<< dendl;
      if (compressor->compress(it->second.data, data)) {
        ldout(cct, 1) << __func__ << " compress job id=" << compress_id << " failed!"<< dendl;
        it->second.status.set(ERROR);
        return -EIO;
      }
      *finished = true;
    } else {
      job_lock.Unlock();
      usleep(1000);
      job_lock.Lock();
      goto retry;
    }
  } else {
    ldout(cct, 10) << __func__ << " compress job id=" << compress_id << " hasn't finished."<< dendl;
    *finished = false;
  }
  return 0;
}

int AsyncCompressor::get_decompress_data(uint64_t decompress_id, bufferlist &data, bool blocking, bool *finished)
{
  assert(finished);
  Mutex::Locker l(job_lock);
  unordered_map<uint64_t, Job>::iterator it = jobs.find(decompress_id);
  if (it == jobs.end() || it->second.is_compress) {
    ldout(cct, 10) << __func__ << " missing to get decompress job id=" << decompress_id << dendl;
    return -ENOENT;
  }
  int status;

 retry:
  status = it->second.status.read();
  if (status == DONE) {
    ldout(cct, 20) << __func__ << " successfully getting decompressed data, job id=" << decompress_id << dendl;
    *finished = true;
    data.swap(it->second.data);
    jobs.erase(it);
  } else if (status == ERROR) {
    ldout(cct, 20) << __func__ << " compressed data failed, job id=" << decompress_id << dendl;
    jobs.erase(it);
    return -EIO;
  } else if (blocking) {
    if (it->second.status.compare_and_swap(WAIT, DONE)) {
      ldout(cct, 10) << __func__ << " decompress job id=" << decompress_id << " hasn't started, abort!"<< dendl;
      if (compressor->decompress(it->second.data, data)) {
        ldout(cct, 1) << __func__ << " decompress job id=" << decompress_id << " failed!"<< dendl;
        it->second.status.set(ERROR);
        return -EIO;
      }
      *finished = true;
    } else {
      job_lock.Unlock();
      usleep(1000);
      job_lock.Lock();
      goto retry;
    }
  } else {
    ldout(cct, 10) << __func__ << " decompress job id=" << decompress_id << " hasn't finished."<< dendl;
    *finished = false;
  }
  return 0;
}
