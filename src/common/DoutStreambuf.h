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
  enum dout_streambuf_flags_t {
    DOUTSB_FLAG_SYSLOG =          0x01,
    DOUTSB_FLAG_STDOUT =          0x02,
    DOUTSB_FLAG_STDERR =          0x04,
  };

  typedef traits traits_ty;
  typedef typename traits_ty::int_type int_type;
  typedef typename traits_ty::pos_type pos_type;
  typedef typename traits_ty::off_type off_type;

  // The size of the output buffer.
  static const size_t OBUF_SZ = 32000;

  DoutStreambuf();

  // Set or clear the use_stderr bit
  // If this bit is cleared, we don't bother outputting high priority messages
  // on stderr any more. We should clear the bit after daemonizing, since
  // stderr -> /dev/null at that point.
  void set_use_stderr(bool val);

  // Set the flags based on the global configuration
  void read_global_configuration();

  // Set the flags directly (for debug use only)
  void set_flags(int flags_);

protected:
  // Called when the buffer fills up
  virtual int_type overflow(int_type c);

  // Called when the buffer is flushed
  virtual int_type sync();

  // Called when we try to read, but there are no more chars in the buffer
  virtual int_type underflow();

private:
  void _clear_output_buffer();

  // Output buffer
  charT obuf[OBUF_SZ];

  int flags;
};

#endif
