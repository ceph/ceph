// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "Common/Compat.h"
#include <cerrno>

extern "C" {
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "Common/FileUtils.h"
#include "Common/System.h"
#include "CephBroker.h"

using namespace Hypertable;

atomic_t CephBroker::ms_next_fd = ATOMIC_INIT(0);

CephBroker::CephBroker(PropertiesPtr &cfg) {
  m_verbose = cfg->get_bool("Hypertable.Verbose");
  /* do other stuff */
}

CephBroker::~CephBroker() { /* destroy client? */}

void CephBroker::open(ResponseCallbackOpen *cb, const char *fname, uint32_t bufsz)
{

}

void CephBroker::create(ResponseCallbackOpen *cb, const char *fname, bool overwrite,
       int32_t bufsz, int16_t replication, int64_t blksz){

}

void CephBroker::close(ResponseCallback *cb, uint32_t fd){

}

void CephBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount){

}

void CephBroker::append(ResponseCallbackAppend *cb, uint32_t fd,
		    uint32_t amount, const void CephBroker::*data, bool sync)
{

}
void CephBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset){

}

void CephBroker::remove(ResponseCallback *cb, const char *fname){

}

void CephBroker::length(ResponseCallbackLength *cb, const char *fname){

}

void CephBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
		   uint32_t amount){

}

void CephBroker::mkdirs(ResponseCallback *cb, const char *dname){

}

void CephBroker::rmdir(ResponseCallback *cb, const char *dname){

}

void CephBroker::flush(ResponseCallback *cb, uint32_t fd){

}

void CephBroker::status(ResponseCallback *cb){

}

void CephBroker::shutdown(ResponseCallback *cb){

}

void CephBroker::readdir(ResponseCallbackReaddir *cb, const char *dname){

}

void CephBroker::exists(ResponseCallbackExists *cb, const char *fname){

}

void CephBroker::rename(ResponseCallback *cb, const char *src, const char *dst){

}

void CephBroker::debug(ResponseCallback *, int32_t command,
		   StaticBuffer &serialized_parameters){

}

void CephBroker::report_error(ResponseCallback *cb, int error) {

}
