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
#include "common/errno.h"
#include "common/Mutex.h"

#include <assert.h>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <streambuf>
#include <string.h>
#include <syslog.h>

///////////////////////////// Globals /////////////////////////////
// TODO: get rid of this lock using thread-local storage
extern Mutex _dout_lock;

/* True if we should output high-priority messages to stderr */
static bool use_stderr = true;

//////////////////////// Helper functions //////////////////////////
static bool empty(const char *str)
{
  if (!str)
    return true;
  if (!str[0])
    return true;
  return false;
}

static string cpp_str(const char *str)
{
  if (!str)
    return "(NULL)";
  if (str[0] == '\0')
    return "(empty)";
  return str;
}

static std::string normalize_relative(const char *from)
{
  if (from[0] == '/')
    return string(from);

  std::auto_ptr <char> cwd(get_current_dir_name());
  ostringstream oss;
  oss << "/" << *cwd << "/" << from;
  return oss.str();
}

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

static int safe_write(int fd, const char *buf, signed int len)
{
  int res;

  assert(len != 0);
  while (1) {
    res = write(fd, buf, len);
    if (res < 0) {
      int err = errno;
      if (err != EINTR) {
	ostringstream oss;
	oss << __func__ << ": failed to write to fd " << fd << ": "
	    << cpp_strerror(err) << "\n";
	primitive_log(oss.str());
	return err;
      }
    }
    len -= res;
    buf += res;
    if (len <= 0)
      return 0;
  }
}

static int create_symlink(const string &oldpath, const string &newpath)
{
  while (1) {
    if (::symlink(oldpath.c_str(), newpath.c_str()) == 0)
      return 0;
    int err = errno;
    if (err == EEXIST) {
      // Handle EEXIST
      if (::unlink(newpath.c_str())) {
	err = errno;
	ostringstream oss;
	oss << __func__ << ": failed to remove '" << newpath << "': "
	    << cpp_strerror(err) << "\n";
	primitive_log(oss.str());
	return err;
      }
    }
    else {
      // Other errors
      ostringstream oss;
      oss << __func__ << ": failed to symlink(oldpath='" << oldpath
	  << "', newpath='" << newpath << "'): " << cpp_strerror(err) << "\n";
      primitive_log(oss.str());
      return err;
    }
  }
}

static std::string get_basename(const std::string &filename)
{
  size_t last_slash = filename.find_last_of("/");
  if (last_slash == std::string::npos)
    return filename;
  return filename.substr(last_slash + 1);
}

///////////////////////////// DoutStreambuf /////////////////////////////
template <typename charT, typename traits>
DoutStreambuf<charT, traits>::DoutStreambuf()
  : flags(0), ofd(-1)
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
    *end_ptr++ = '\0';
//    char buf[1000];
//    hex2str(obuf, end_ptr - obuf, buf, sizeof(buf));
//    printf("overflow buffer: '%s'\n", buf);
  }

  // Loop over all lines in the buffer.
  int prio = 100;
  charT* start = obuf;
  while (true) {
    char* end = strchrnul(start, '\n');
    if (start == end) {
      if (*start == '\0')
	break;
      // skip zero-length lines
      ++start;
      continue;
    }
    if (*start == '\1') {
      // Decode some control characters
      ++start;
      unsigned char tmp = *((unsigned char*)start);
      prio = tmp - 11;
      ++start;
    }
    *end = '\n';
    char next = *(end+1);
    *(end+1) = '\0';

    // Now 'start' points to a NULL-terminated string, which we want to
    // output with priority 'prio'
    int len = strlen(start);
    if (flags & DOUTSB_FLAG_SYSLOG) {
      //printf("syslogging: '%s' len=%d\n", start, len);
      syslog(LOG_USER | dout_prio_to_syslog_prio(prio), "%s", start);
    }
    if (flags & DOUTSB_FLAG_STDOUT) {
      // Just write directly out to the stdout fileno. There's no point in
      // using something like fputs to write to a temporary buffer,
      // because we would just have to flush that temporary buffer
      // immediately.
      if (safe_write(STDOUT_FILENO, start, len))
	flags &= ~DOUTSB_FLAG_STDOUT;
    }
    if (flags & DOUTSB_FLAG_STDERR) {
      // Only write to stderr if the message is important enough.
      if (prio_is_visible_on_stderr(prio)) {
	if (safe_write(STDERR_FILENO, start, len))
	  flags &= ~DOUTSB_FLAG_STDERR;
      }
    }
    if (flags & DOUTSB_FLAG_OFILE) {
      if (safe_write(ofd, start, len))
	flags &= ~DOUTSB_FLAG_OFILE;
    }

    *(end+1) = next;
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
void DoutStreambuf<charT, traits>::read_global_config()
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
  if (g_conf.log_to_file) {
    if (_read_ofile_config()) {
      flags |= DOUTSB_FLAG_OFILE;
    }
  }
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
  unsigned char val = (prio + 11);
  *p++ = val;
  this->pbump(2);
}

// call after calling daemon()
template <typename charT, typename traits>
int DoutStreambuf<charT, traits>::rename_output_file()
{
  Mutex::Locker l(_dout_lock);
  if (!(flags & DOUTSB_FLAG_OFILE))
    return 0;

  string new_opath(_calculate_opath());
  if (opath == new_opath)
    return 0;

  int ret = ::rename(opath.c_str(), new_opath.c_str());
  if (ret) {
    int err = errno;
    ostringstream oss;
    oss << __func__ << ": failed to rename '" << opath << "' to "
        << "'" << new_opath << "': " << cpp_strerror(err) << "\n";
    primitive_log(oss.str());
    return err;
  }

  opath = new_opath;

//  // $type.$id symlink
//  if (g_conf.log_per_instance && _dout_name_symlink_path[0])
//    create_symlink(_dout_name_symlink_path);
//  if (_dout_rank_symlink_path[0])
//    create_symlink(_dout_rank_symlink_path);

  return 0;
}

template <typename charT, typename traits>
std::string DoutStreambuf<charT, traits>::config_to_str() const
{
  assert(_dout_lock.is_locked());
  ostringstream oss;
  oss << "g_conf.log_to_syslog = " << g_conf.log_to_syslog << "\n";
  oss << "g_conf.log_to_stdout = " << g_conf.log_to_stdout << "\n";
  oss << "use_stderr = " << use_stderr << "\n";
  oss << "g_conf.log_to_file = " << g_conf.log_to_file << "\n";
  oss << "g_conf.log_file = '" << cpp_str(g_conf.log_file) << "'\n";
  oss << "flags = 0x" << std::hex << flags << std::dec << "\n";
  oss << "ofd = " << ofd << "\n";
  oss << "opath = '" << opath << "'\n";
  return oss.str();
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

template <typename charT, typename traits>
std::string DoutStreambuf<charT, traits>::_calculate_opath() const
{
  assert(_dout_lock.is_locked());
  if (!empty(g_conf.log_file)) {
    return normalize_relative(g_conf.log_file);
  }

  if (g_conf.log_per_instance) {
    char hostname[255];
    memset(hostname, 0, sizeof(hostname));
    int ret = gethostname(hostname, sizeof(hostname));
    if (ret) {
      int err = errno;
      ostringstream oss;
      oss << __func__ << ": error calling gethostname: " << cpp_strerror(err) << "\n";
      primitive_log(oss.str());
      return "";
    }
    ostringstream oss;
    oss << hostname << "." << getpid();
    return oss.str();
  }
  else {
    ostringstream oss;
    oss << g_conf.type << "." << g_conf.id << ".log";
    return oss.str();
  }
}

template <typename charT, typename traits>
bool DoutStreambuf<charT, traits>::_read_ofile_config()
{
  opath = _calculate_opath();
  if (opath.empty()) {
    ostringstream oss;
    oss << __func__ << ": _calculate_opath failed.\n";
    primitive_log(oss.str());
    return false;
  }

  assert(ofd == -1);
  ofd = open(opath.c_str(),
	    O_CREAT | O_WRONLY | O_CLOEXEC | O_APPEND, S_IWUSR | S_IRUSR);
  if (ofd < 0) {
    int err = errno;
    ostringstream oss;
    oss << "failed to open log file '" << opath << "': "
	<< cpp_strerror(err) << "\n";
    primitive_log(oss.str());
    return false;
  }

  return true;
}

// Explicit template instantiation
template class DoutStreambuf <char>;
