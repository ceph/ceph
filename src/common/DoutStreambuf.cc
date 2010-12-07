// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "config.h"
#include "common/DoutStreambuf.h"
#include "common/Mutex.h"

#include <assert.h>
#include <fstream>
#include <iostream>
#include <streambuf>
#include <string.h>
#include <syslog.h>

///////////////////////////// Globals /////////////////////////////
// TODO: get rid of this lock using thread-local storage
extern Mutex _dout_lock;

/* True if we should output high-priority messages to stderr */
static bool use_stderr = true;

///////////////////////////// Helper functions /////////////////////////////
/* Complain about errors even without a logfile */
static void primitive_log(const std::string &str)
{
  std::cerr << str;
  std::cerr.flush();
  syslog(LOG_USER | LOG_NOTICE, "%s", str.c_str());
}

static inline bool prio_is_visible_on_stderr(int prio)
{
  return prio <= 15;
}

static inline int dout_prio_to_syslog_prio(int prio)
{
  if (prio <= 3)
    return LOG_CRIT;
  if (prio <= 5)
    return LOG_ERR;
  if (prio <= 15)
    return LOG_WARNING;
  if (prio <= 30)
    return LOG_NOTICE;
  if (prio <= 40)
    return LOG_INFO;
  return LOG_DEBUG;
}

///////////////////////////// DoutStreambuf /////////////////////////////
template <typename charT, typename traits>
DoutStreambuf<charT, traits>::DoutStreambuf()
{
  // Initialize get pointer to zero so that underflow is called on the first read.
  this->setg(0, 0, 0);

  // Initialize output_buffer
  _clear_output_buffer();
}

// This function is called when the output buffer is filled.
// In this function, the buffer should be written to wherever it should
// be written to (in this case, the streambuf object that this is controlling).
template <typename charT, typename traits>
typename DoutStreambuf<charT, traits>::int_type
DoutStreambuf<charT, traits>::overflow(DoutStreambuf<charT, traits>::int_type c)
{
  {
    // zero-terminate the buffer
    charT* end_ptr = this->pptr();
    *end_ptr++ = '\0';
    //std::cout << "overflow with '" << obuf << "'" << std::endl;
  }

  // Loop over all lines in the buffer.
  int prio = 100;
  charT* start = obuf;
  while (true) {
    charT* end = strchr(start, '\n');
    if (start == end) {
      // skip zero-length lines
      ++start;
      continue;
    }
    if (*start == '\1') {
      // Decode some control characters
      ++start;
      unsigned char tmp = *((unsigned char*)start);
      if (tmp != 0) {
	prio = tmp;
	++start;
      }
    }
    if (end) {
      *end = '\0';
    }
    if (*start == '\0') {
      // empty buffer
      break;
    }

    // Now 'start' points to a NULL-terminated string, which we want to output
    // with priority 'prio'
    if (flags & DOUTSB_FLAG_SYSLOG) {
      syslog(LOG_USER | dout_prio_to_syslog_prio(prio), "%s", start);
    }
    if (flags & DOUTSB_FLAG_STDOUT) {
      puts(start);
    }
    if (flags & DOUTSB_FLAG_STDERR) {
      if (prio_is_visible_on_stderr(prio)) {
	fputs(start, stderr);
	fputc('\n', stderr);
      }
    }

    if (!end) {
      break;
    }

    start = end + 1;
  }

  _clear_output_buffer();

  // A value different than EOF (or traits::eof() for other traits) signals success.
  // If the function fails, either EOF (or traits::eof() for other traits) is returned or an
  // exception is thrown.
  return traits_ty::not_eof(c);
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::set_use_stderr(bool val)
{
  _dout_lock.Lock();
  use_stderr = val;
  if (val)
    flags |= DOUTSB_FLAG_STDERR;
  else
    flags &= ~DOUTSB_FLAG_STDERR;
  _dout_lock.Unlock();
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::read_global_configuration()
{
  assert(_dout_lock.is_locked());
  flags = 0;

  if (g_conf.log_to_syslog) {
    flags |= DOUTSB_FLAG_SYSLOG;
  }
  if (g_conf.log_to_stdout) {
    flags |= DOUTSB_FLAG_STDOUT;
  }
  if (use_stderr) {
    flags |= DOUTSB_FLAG_STDERR;
  }
//  if (g_conf.log_to_file) {
//    if (read_log_to_file_configuration()) {
//      flags |= DOUTSB_FLAG_FILE;
//    }
//  }
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::
set_flags(int flags_)
{
  assert(_dout_lock.is_locked());
  flags = flags_;
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::
set_prio(int prio)
{
  charT* p = this->pptr();
  *p++ = '\1';
  *p++ = ((unsigned char)prio);
  this->pbump(2);
}

// This is called to flush the buffer.
// This is called when we're done with the file stream (or when .flush() is called).
template <typename charT, typename traits>
typename DoutStreambuf<charT, traits>::int_type
DoutStreambuf<charT, traits>::sync()
{
  //std::cout << "flush!" << std::endl;
  typename DoutStreambuf<charT, traits>::int_type
    ret(this->overflow(traits_ty::eof()));
  if (ret == traits_ty::eof())
    return -1;

  return 0;
}

template <typename charT, typename traits>
typename DoutStreambuf<charT, traits>::int_type
DoutStreambuf<charT, traits>::underflow()
{
  // We can't read from this
  // TODO: some more elegant way of preventing callers from trying to get input from this stream
  assert(0);
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::_clear_output_buffer()
{
  // Set up the put pointer.
  // Overflow is called when this buffer is filled
  this->setp(obuf, obuf + OBUF_SZ - 5);
}

// Explicit template instantiation
template class DoutStreambuf <char>;
