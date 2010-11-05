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

#include "common/SyslogStreambuf.h"

#include <assert.h>
#include <fstream>
#include <iostream>
#include <streambuf>
#include <syslog.h>
#include <string.h>

template <typename charT, typename traits>
SyslogStreambuf<charT, traits>::SyslogStreambuf()
{
  // Initialize get pointer to zero so that underflow is called on the first read.
  this->setg(0, 0, 0);

  // Set up the put pointer.
  // Overflow is called when this buffer is filled
  this->setp(obuf, obuf + OBUF_SZ - 2);

  // Zero the output buffer
  memset(obuf, 0, OBUF_SZ);
}

// This function is called when the output buffer is filled.
// In this function, the buffer should be written to wherever it should
// be written to (in this case, the streambuf object that this is controlling).
template <typename charT, typename traits>
typename SyslogStreambuf<charT, traits>::int_type
SyslogStreambuf<charT, traits>::overflow(SyslogStreambuf<charT, traits>::int_type c)
{
  charT* end = this->pptr();

  // Add an eof character to the buffer if we need to.
  if(!traits_ty::eq_int_type(c, traits_ty::eof())) {
    *end++ = traits_ty::to_char_type(c);
  }
  *end++ = '\0';

  //std::cout << "overflow with '" << obuf << "'" << std::endl;

  // int_type ilen = end - obuf; // Compute the write length.
  syslog(LOG_USER | LOG_NOTICE, "%s", obuf);

  // Reset put pointers
  setp(obuf, obuf + OBUF_SZ - 1);
  obuf[0] = '\0';

  // A value different than EOF (or traits::eof() for other traits) signals success.
  // If the function fails, either EOF (or traits::eof() for other traits) is returned or an
  // exception is thrown.
  return traits_ty::not_eof(c);
}

// This is called to flush the buffer.
// This is called when we're done with the file stream (or when .flush() is called).
template <typename charT, typename traits>
typename SyslogStreambuf<charT, traits>::int_type
SyslogStreambuf<charT, traits>::sync()
{
  //std::cout << "flush!" << std::endl;

  // Don't bother calling overflow if there's nothing to syslog
  if (obuf[0] == '\0')
    return 0;

  typename SyslogStreambuf<charT, traits>::int_type
    ret(this->overflow(traits_ty::eof()));
  if (ret == traits_ty::eof())
    return -1;

  return 0;
}

template <typename charT, typename traits>
typename SyslogStreambuf<charT, traits>::int_type
SyslogStreambuf<charT, traits>::underflow()
{
  // We can't read from this
  // TODO: some more elegant way of preventing callers from trying to get input from this stream
  assert(0);
}

// Explicit template instantiation
template class SyslogStreambuf <char>;
