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
#include <string>

class md_config_t;

template <typename charT, typename traits = std::char_traits<charT> >
class DoutStreambuf : public std::basic_streambuf<charT, traits>
{
public:
  enum dout_streambuf_flags_t {
    DOUTSB_FLAG_SYSLOG =          0x01,
    DOUTSB_FLAG_STDOUT =          0x02,
    DOUTSB_FLAG_STDERR_SOME =     0x04,
    DOUTSB_FLAG_STDERR_ALL =      0x08,
    DOUTSB_FLAG_STDERR =          0x0c,
    DOUTSB_FLAG_OFILE =           0x10,
  };

  typedef traits traits_ty;
  typedef typename traits_ty::int_type int_type;
  typedef typename traits_ty::pos_type pos_type;
  typedef typename traits_ty::off_type off_type;

  // The size of the output buffer.
  static const size_t OBUF_SZ = 32000;

  DoutStreambuf();
  ~DoutStreambuf();

  // Call when you close stderr.  Not strictly necessary, since we would get an
  // error the next time we tried to write to stdedrr. But nicer than waiting
  // for the error to happen.
  void handle_stderr_closed();

  // Set the flags based on the global configuration
  void read_global_config(const md_config_t *conf);

  // Set the priority of the messages being put into the stream
  void set_prio(int prio);

  // Call after calling daemon()
  // A change in the process ID sometimes requires us to change our output
  // path name.
  int handle_pid_change(const md_config_t *conf);

  // Create a rank symlink to the log file
  int create_rank_symlink(int n);

  std::string config_to_str() const;

  // Output a string directly to the file and to syslog
  // (if those sinks are active)
  void dout_emergency_to_file_and_syslog(const char * const str) const;

protected:
  // Called when the buffer fills up
  virtual int_type overflow(int_type c);

  // Called when the buffer is flushed
  virtual int_type sync();

  // Called when we try to read, but there are no more chars in the buffer
  virtual int_type underflow();

private:
  friend void dout_emergency(const char * const str);
  friend void dout_emergency(const std::string &str);

  void _clear_output_buffer();
  std::string _calculate_opath(const md_config_t *conf) const;
  std::string _get_symlink_dir(const md_config_t *conf) const;
  int _read_ofile_config(const md_config_t *conf);
  int _rotate_files(const md_config_t *conf, const std::string &base);

  std::string type_name;

  // Output buffer
  charT obuf[OBUF_SZ];

  // Output flags
  int flags;

  // ofile stuff
  int ofd;
  std::string opath;
  std::string symlink_dir;
  std::string isym_path;
  std::string rsym_path;
};

// Secret evil interfaces for writing logs without taking the lock.
// DO NOT USE THESE unless you have a really good reason.
extern void dout_emergency(const char * const str);
extern void dout_emergency(const std::string &str);

#endif
