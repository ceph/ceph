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

#include "common/config.h"
#include "common/DoutStreambuf.h"
#include "common/entity_name.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/safe_io.h"

#include <values.h>
#include <assert.h>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <time.h>
#include <sstream>
#include <streambuf>
#include <string.h>
#include <syslog.h>

///////////////////////////// Constants /////////////////////////////
#define TIME_FMT "%04d-%02d-%02d %02d:%02d:%02d.%06ld"
#define TIME_FMT_SZ 26

///////////////////////////// Globals /////////////////////////////
extern DoutStreambuf <char> *_doss;

// TODO: get rid of this lock using thread-local storage
extern pthread_mutex_t _dout_lock;

//////////////////////// Helper functions //////////////////////////
// Try a 0-byte write to a file descriptor to see if it open.
static bool fd_is_open(int fd)
{
  char buf;
  ssize_t res = TEMP_FAILURE_RETRY(write(fd, &buf, 0));
  return (res == 0);
}

static std::string normalize_relative(const char *from)
{
  if (from[0] == '/')
    return string(from);

  char c[512];
  char *cwd = getcwd(c, sizeof(c));
  ostringstream oss;
  oss << cwd << "/" << from;
  return oss.str();
}

static inline bool prio_is_visible_on_stderr(signed int prio)
{
  return prio == -1;
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

static std::string get_basename(const std::string &filename)
{
  size_t last_slash = filename.find_last_of("/");
  if (last_slash == std::string::npos)
    return filename;
  return filename.substr(last_slash + 1);
}

static std::string get_dirname(const std::string &filename)
{
  size_t last_slash = filename.find_last_of("/");
  if (last_slash == std::string::npos)
    return ".";
  if (last_slash == 0)
    return filename;
  return filename.substr(0, last_slash);
}

static int create_symlink(string oldpath, const string &newpath)
{
  // Create relative symlink if the files are in the same directory
  if (get_dirname(oldpath) == get_dirname(newpath)) {
    oldpath = string("./") + get_basename(oldpath);
  }

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
	dout_emergency(oss.str());
	return err;
      }
    }
    else {
      // Other errors
      ostringstream oss;
      oss << __func__ << ": failed to symlink(oldpath='" << oldpath
	  << "', newpath='" << newpath << "'): " << cpp_strerror(err) << "\n";
      dout_emergency(oss.str());
      return err;
    }
  }
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

template <typename charT, typename traits>
DoutStreambuf<charT, traits>::~DoutStreambuf()
{
  if (ofd != -1) {
    TEMP_FAILURE_RETRY(::close(ofd));
    ofd = -1;
  }
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

  // Get priority of message
  signed int prio = obuf[TIME_FMT_SZ] - 12;

  // Prepend date to buffer
  struct timeval tv;
  gettimeofday(&tv, NULL);
  struct tm tm;
  localtime_r(&tv.tv_sec, &tm);
  snprintf(obuf, TIME_FMT_SZ + 1, TIME_FMT,
	   tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
	   tm.tm_hour, tm.tm_min, tm.tm_sec,
	   tv.tv_usec);

  // This byte was NULLed by snprintf, but we'd rather have a space there.
  obuf[TIME_FMT_SZ] = ' ';

  // Now 'obuf' points to a NULL-terminated string, which we want to
  // output with priority 'prio'
  int len = strlen(obuf);
  if (flags & DOUTSB_FLAG_SYSLOG) {
    syslog(LOG_USER | dout_prio_to_syslog_prio(prio), "%s",
	   obuf + TIME_FMT_SZ + 1);
  }
  if (flags & DOUTSB_FLAG_STDOUT) {
    // Just write directly out to the stdout fileno. There's no point in
    // using something like fputs to write to a temporary buffer,
    // because we would just have to flush that temporary buffer
    // immediately.
    if (safe_write(STDOUT_FILENO, obuf, len))
      flags &= ~DOUTSB_FLAG_STDOUT;
  }
  if (flags & DOUTSB_FLAG_STDERR) {
    if ((flags & DOUTSB_FLAG_STDERR_ALL) || (prio == -1)) {
      if (safe_write(STDERR_FILENO, obuf, len))
	flags &= ~DOUTSB_FLAG_STDERR;
    }
  }
  if (flags & DOUTSB_FLAG_OFILE) {
    if (safe_write(ofd, obuf, len))
      flags &= ~DOUTSB_FLAG_OFILE;
  }

  _clear_output_buffer();

  // A value different than EOF (or traits::eof() for other traits) signals success.
  // If the function fails, either EOF (or traits::eof() for other traits) is returned or an
  // exception is thrown.
  return traits_ty::not_eof(c);
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::handle_stderr_closed()
{
  // should hold the dout_lock here
  flags &= ~DOUTSB_FLAG_STDERR;
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::read_global_config(const md_config_t *conf)
{
  type_name = conf->name.get_type_name();

  // should hold the dout_lock here
  flags = 0;

  if (ofd != -1) {
    TEMP_FAILURE_RETRY(::close(ofd));
    ofd = -1;
  }

  if (conf->log_to_syslog) {
    flags |= DOUTSB_FLAG_SYSLOG;
  }

  if ((conf->log_to_stderr != LOG_TO_STDERR_NONE) &&
       fd_is_open(STDERR_FILENO)) {
    switch (conf->log_to_stderr) {
      case LOG_TO_STDERR_SOME:
	flags |= DOUTSB_FLAG_STDERR_SOME;
	break;
      case LOG_TO_STDERR_ALL:
	flags |= DOUTSB_FLAG_STDERR_ALL;
	break;
      default:
	ostringstream oss;
	oss << "DoutStreambuf::read_global_config: can't understand "
	    << "conf->log_to_stderr = " << conf->log_to_stderr << "\n";
	dout_emergency(oss.str());
	break;
    }
  }

  if ((!conf->log_file.empty()) || (!conf->log_dir.empty())) {
    if (_read_ofile_config(conf) == 0) {
      flags |= DOUTSB_FLAG_OFILE;
    }
  }
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

template <typename charT, typename traits>
int DoutStreambuf<charT, traits>::
handle_pid_change(const md_config_t *conf)
{
  // should hold the dout_lock here
  if (!(flags & DOUTSB_FLAG_OFILE))
    return 0;

  string new_opath(_calculate_opath(conf));
  if (opath == new_opath)
    return 0;

  if (!isym_path.empty()) {
    // Re-create the instance symlink
    int ret = create_symlink(new_opath, isym_path);
    if (ret) {
      ostringstream oss;
      oss << __func__ << ": failed to (re)create instance symlink\n";
      dout_emergency(oss.str());
      return ret;
    }
  }

  int ret = ::rename(opath.c_str(), new_opath.c_str());
  if (ret) {
    int err = errno;
    ostringstream oss;
    oss << __func__ << ": failed to rename '" << opath << "' to "
        << "'" << new_opath << "': " << cpp_strerror(err) << "\n";
    dout_emergency(oss.str());
    return err;
  }

  opath = new_opath;

  return 0;
}

template <typename charT, typename traits>
std::string DoutStreambuf<charT, traits>::config_to_str() const
{
  // should hold the dout_lock here
  ostringstream oss;
  oss << "flags = 0x" << std::hex << flags << std::dec << "\n";
  oss << "ofd = " << ofd << "\n";
  oss << "opath = '" << opath << "'\n";
  oss << "isym_path = '" << isym_path << "'\n";
  return oss.str();
}

template <typename charT, typename traits>
void DoutStreambuf<charT, traits>::
dout_emergency_to_file_and_syslog(const char * const str) const
{
  int len = strlen(str);
  if (ofd >= 0) {
    if (safe_write(ofd, str, len)) {
      ; // ignore error code
    }
  }
  if (flags & DOUTSB_FLAG_SYSLOG) {
    syslog(LOG_USER | LOG_CRIT, "%s", str);
  }
}

// This is called to flush the buffer.
// This is called when we're done with the file stream (or when .flush() is called).
template <typename charT, typename traits>
typename DoutStreambuf<charT, traits>::int_type
DoutStreambuf<charT, traits>::sync()
{
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
  this->setp(obuf + TIME_FMT_SZ, obuf + OBUF_SZ - TIME_FMT_SZ - 5);
}

template <typename charT, typename traits>
std::string DoutStreambuf<charT, traits>::
_calculate_opath(const md_config_t *conf) const
{
  // should hold the dout_lock here

  // If conf->log_file was specified, that takes the highest priority
  if (!conf->log_file.empty()) {
    string log_file(normalize_relative(conf->log_file.c_str()));
    if (conf->log_per_instance) {
      ostringstream oss;
      oss << log_file << "." << getpid();
      return oss.str();
    }
    else
      return log_file;
  }

  string log_dir;
  if (conf->log_dir.empty())
    log_dir = normalize_relative(".");
  else
    log_dir = normalize_relative(conf->log_dir.c_str());

  if (conf->log_per_instance) {
    char hostname[255];
    memset(hostname, 0, sizeof(hostname));
    int ret = gethostname(hostname, sizeof(hostname));
    if (ret) {
      int err = errno;
      ostringstream oss;
      oss << __func__ << ": error calling gethostname: " << cpp_strerror(err) << "\n";
      dout_emergency(oss.str());
      return "";
    }
    ostringstream oss;
    oss << log_dir << "/" << hostname << "." << getpid();
    return oss.str();
  }
  else {
    ostringstream oss;
    oss << log_dir << "/" << conf->name.to_str() << ".log";
    return oss.str();
  }
}

template <typename charT, typename traits>
std::string DoutStreambuf<charT, traits>::
_get_symlink_dir(const md_config_t *conf) const
{
  if (!conf->log_sym_dir.empty())
    return normalize_relative(conf->log_sym_dir.c_str());
  else
    return get_dirname(opath);
}

template <typename charT, typename traits>
int DoutStreambuf<charT, traits>::
_read_ofile_config(const md_config_t *conf)
{
  int ret;

  opath.clear();
  symlink_dir.clear();
  isym_path.clear();

  opath = _calculate_opath(conf);
  if (opath.empty()) {
    ostringstream oss;
    oss << __func__ << ": _calculate_opath failed.\n";
    dout_emergency(oss.str());
    return 1;
  }

  symlink_dir = _get_symlink_dir(conf);

  if (conf->log_per_instance) {
    // Calculate instance symlink path (isym_path)
    ostringstream iss;
    iss << symlink_dir << "/" << conf->name.to_str();
    isym_path = iss.str();

    // Rotate isym_path
    ret = _rotate_files(conf, isym_path);
    if (ret) {
      ostringstream oss;
      oss << __func__ << ": failed to rotate instance symlinks\n";
      dout_emergency(oss.str());
      return ret;
    }

    // Create isym_path
    ret = create_symlink(opath, isym_path);
    if (ret) {
      ostringstream oss;
      oss << __func__ << ": failed to create instance symlink\n";
      dout_emergency(oss.str());
      return ret;
    }
  }

  assert(ofd == -1);
  ofd = open(opath.c_str(),
	    O_CREAT | O_WRONLY | O_APPEND, S_IWUSR | S_IRUSR);
  if (ofd == -1) {
    int err = errno;
    ostringstream oss;
    oss << "failed to open log file '" << opath << "': "
	<< cpp_strerror(err) << "\n";
    dout_emergency(oss.str());
    return err;
  }

  return 0;
}

template <typename charT, typename traits>
int DoutStreambuf<charT, traits>::
_rotate_files(const md_config_t *conf, const std::string &base)
{
  // Given a file name base, and a directory like this:
  // base
  // base.1
  // base.2
  // base.3
  // base.4
  // unrelated_blah
  // unrelated_blah.1
  //
  // We'll take the following actions:
  // base		  rename to base.1
  // base.1		  rename to base.2
  // base.2		  rename to base.3
  // base.3		  (unlink)
  // base.4		  (unlink)
  // unrelated_blah	  (do nothing)
  // unrelated_blah.1	  (do nothing)

  signed int i;
  for (i = -1; i < INT_MAX; ++i) {
    ostringstream oss;
    oss << base;
    if (i != -1)
      oss << "." << i;
    string path(oss.str());

    if (::access(path.c_str(), R_OK | W_OK))
      break;
  }
  signed int max_symlink = i - 1;

  for (signed int j = max_symlink; j >= -1; --j) {
    ostringstream oss;
    oss << base;
    if (j != -1)
      oss << "." << j;
    string path(oss.str());

    signed int k = j + 1;
    if (k >= conf->log_sym_history) {
      if (::unlink(path.c_str())) {
	int err = errno;
	ostringstream ess;
	ess << __func__ << ": failed to unlink '" << path << "': "
	    << cpp_strerror(err) << "\n";
	dout_emergency(ess.str());
	return err;
      }
      //*_dout << "---- " << getpid() << " removed " << path << " ----"
      //       << std::endl;
    }
    else {
      ostringstream pss;
      pss << base << "." << k;
      string new_path(pss.str());
      if (::rename(path.c_str(), new_path.c_str())) {
	int err = errno;
	ostringstream ess;
	ess << __func__ << ": failed to rename '" << path << "' to "
	    << "'" << new_path << "': " << cpp_strerror(err) << "\n";
	dout_emergency(ess.str());
	return err;
      }
//      *_dout << "---- " << getpid() << " renamed " << path << " -> "
//      << newpath << " ----" << std::endl;
    }
  }
  return 0;
}

/* This function may be called from a signal handler.
 * This function may be called before dout has been initialized.
 */
void dout_emergency(const char * const str)
{
  int len = strlen(str);
  if (safe_write(STDERR_FILENO, str, len)) {
    // ignore errors
    ;
  }
  if (safe_write(STDOUT_FILENO, str, len)) {
    ; // ignore error code
  }
  /* Normally we would take the lock before even checking _doss, but since
   * this is an emergency, we can't do that. */
  if (_doss) {
    _doss->dout_emergency_to_file_and_syslog(str);
  }
}

void dout_emergency(const std::string &str)
{
  dout_emergency(str.c_str());
}

// Explicit template instantiation
template class DoutStreambuf <char>;
