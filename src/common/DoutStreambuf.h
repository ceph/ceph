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

/*
 * DoutStreambuf
 *
 * The stream buffer used by dout
 */
#ifndef CEPH_DOUT_STREAMBUF_H
#define CEPH_DOUT_STREAMBUF_H

#include <iosfwd>

template <typename charT, typename traits = std::char_traits<charT> >
class DoutStreambuf : public std::basic_streambuf<charT, traits>
{
public:
  typedef traits traits_ty;
  typedef typename traits_ty::int_type int_type;
  typedef typename traits_ty::pos_type pos_type;
  typedef typename traits_ty::off_type off_type;

  // The size of the input and output buffers.
  static const size_t OBUF_SZ = 32000;

  DoutStreambuf();

protected:
  // Called when the buffer fills up
  virtual int_type overflow(int_type c);

  // Called when the buffer is flushed
  virtual int_type sync();

  // Called when we try to read, but there are no more chars in the buffer
  virtual int_type underflow();

private:
  // Output buffer
  charT obuf[OBUF_SZ];
};

#endif
