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

#define __STDC_FORMAT_MACROS // for PRId64, etc.

#include "common/ProfLogger.h"
#include "common/Thread.h"
#include "common/config.h"
#include "common/config_obs.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <map>
#include <poll.h>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#define PFL_SUCCESS ((void*)(intptr_t)0)
#define PFL_FAIL ((void*)(intptr_t)1)
#define COUNT_DISABLED ((uint64_t)(int64_t)-1)

using std::ostringstream;

enum prof_log_data_any_t {
  PROF_LOG_DATA_ANY_NONE,
  PROF_LOG_DATA_ANY_U64,
  PROF_LOG_DATA_ANY_DOUBLE
};

/*
 * UNIX domain sockets created by an application persist even after that
 * application closes, unless they're explicitly unlinked. This is because the
 * directory containing the socket keeps a reference to the socket.
 *
 * This code makes things a little nicer by unlinking those dead sockets when
 * the application exits normally.
 */
static pthread_mutex_t cleanup_lock = PTHREAD_MUTEX_INITIALIZER;
static std::vector <const char*> cleanup_files;
static bool cleanup_atexit = false;

static void remove_cleanup_file(const char *file)
{
  pthread_mutex_lock(&cleanup_lock);
  TEMP_FAILURE_RETRY(unlink(file));
  for (std::vector <const char*>::iterator i = cleanup_files.begin();
       i != cleanup_files.end(); ++i) {
    if (strcmp(file, *i) == 0) {
      free((void*)*i);
      cleanup_files.erase(i);
      break;
    }
  }
  pthread_mutex_unlock(&cleanup_lock);
}

static void remove_all_cleanup_files()
{
  pthread_mutex_lock(&cleanup_lock);
  for (std::vector <const char*>::iterator i = cleanup_files.begin();
       i != cleanup_files.end(); ++i) {
    TEMP_FAILURE_RETRY(unlink(*i));
    free((void*)*i);
  }
  cleanup_files.clear();
  pthread_mutex_unlock(&cleanup_lock);
}

static void add_cleanup_file(const char *file)
{
  char *fname = strdup(file);
  if (!fname)
    return;
  pthread_mutex_lock(&cleanup_lock);
  cleanup_files.push_back(fname);
  if (!cleanup_atexit) {
    atexit(remove_all_cleanup_files);
    cleanup_atexit = true;
  }
  pthread_mutex_unlock(&cleanup_lock);
}

/*
 * This thread listens on the UNIX domain socket for incoming connections. When
 * it gets one, it writes out the ProfLogger data using blocking I/O.
 *
 * It also listens to m_shutdown_fd. If there is any data sent to this pipe,
 * the thread terminates itself gracefully, allowing the parent class to join()
 * it.
 */
class ProfLogThread : public Thread
{
public:
  static std::string create_shutdown_pipe(int *pipe_rd, int *pipe_wr)
  {
    int pipefd[2];
    int ret = pipe2(pipefd, O_CLOEXEC);
    if (ret < 0) {
      int err = errno;
      ostringstream oss;
      oss << "ProfLogThread::create_shutdown_pipe error: "
	  << cpp_strerror(err);
      return oss.str();
    }

    *pipe_rd = pipefd[0];
    *pipe_wr = pipefd[1];
    return "";
  }

  static std::string bind_and_listen(const std::string &sock_path, int *fd)
  {
    if (sock_path.size() > sizeof(sockaddr_un::sun_path) - 1) {
      ostringstream oss;
      oss << "ProfLogThread::bind_and_listen: "
	  << "The UNIX domain socket path " << sock_path << " is too long! The "
	  << "maximum length on this system is "
	  << (sizeof(sockaddr_un::sun_path) - 1);
      return oss.str();
    }
    int sock_fd = socket(PF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
      int err = errno;
      ostringstream oss;
      oss << "ProfLogThread::bind_and_listen: "
	  << "failed to create socket: " << cpp_strerror(err);
      return oss.str();
    }
    struct sockaddr_un address;
    memset(&address, 0, sizeof(struct sockaddr_un));
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(sockaddr_un::sun_path), sock_path.c_str());
    if (bind(sock_fd, (struct sockaddr*)&address,
	       sizeof(struct sockaddr_un)) != 0) {
      int err = errno;
      if (err == EADDRINUSE) {
	// The old UNIX domain socket must still be there.
	// Let's unlink it and try again.
	TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
	if (bind(sock_fd, (struct sockaddr*)&address,
		   sizeof(struct sockaddr_un)) == 0) {
	  err = 0;
	}
	else {
	  err = errno;
	}
      }
      if (err != 0) {
	ostringstream oss;
	oss << "ProfLogThread::bind_and_listen: "
	    << "failed to bind the UNIX domain socket to '" << sock_path
	    << "': " << cpp_strerror(err);
	close(sock_fd);
	return oss.str();
      }
    }
    if (listen(sock_fd, 5) != 0) {
      int err = errno;
      ostringstream oss;
      oss << "ProfLogThread::bind_and_listen: "
	  << "failed to listen to socket: " << cpp_strerror(err);
      close(sock_fd);
      TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
      return oss.str();
    }
    *fd = sock_fd;
    return "";
  }

  ProfLogThread(int sock_fd, int shutdown_fd, ProfLoggerCollection *parent)
    : m_sock_fd(sock_fd),
      m_shutdown_fd(shutdown_fd),
      m_parent(parent)
  {
  }

  virtual ~ProfLogThread()
  {
    if (m_sock_fd != -1)
      close(m_sock_fd);
    if (m_shutdown_fd != -1)
      close(m_shutdown_fd);
  }

  virtual void* entry()
  {
    while (true) {
      struct pollfd fds[2];
      memset(fds, 0, sizeof(fds));
      fds[0].fd = m_sock_fd;
      fds[0].events = POLLIN | POLLRDBAND;
      fds[1].fd = m_shutdown_fd;
      fds[1].events = POLLIN | POLLRDBAND;

      int ret = poll(fds, 2, NULL);
      if (ret < 0) {
	if (ret == -EINTR) {
	  continue;
	}
	int err = errno;
	lderr(m_parent->m_cct) << "ProfLogThread: poll(2) error: '"
	    << cpp_strerror(err) << dendl;
	return PFL_FAIL;
      }

      if (fds[0].revents & POLLIN) {
	// Send out some data
	do_accept();
      }
      if (fds[1].revents & POLLIN) {
	// Parent wants us to shut down
	return PFL_SUCCESS;
      }
    }
  }

private:
  const static int MAX_PFL_RETRIES = 10;

  bool do_accept()
  {
    int ret;
    struct sockaddr_un address;
    socklen_t address_length;
    ldout(m_parent->m_cct, 30) << "ProfLogThread: calling accept" << dendl;
    int connection_fd = accept(m_sock_fd, (struct sockaddr*) &address,
				   &address_length);
    ldout(m_parent->m_cct, 30) << "ProfLogThread: finished accept" << dendl;
    if (connection_fd < 0) {
      int err = errno;
      lderr(m_parent->m_cct) << "ProfLogThread: do_accept error: '"
	  << cpp_strerror(err) << dendl;
      return false;
    }

    std::vector<char> buffer;
    buffer.reserve(512);
    {
      Mutex::Locker lck(m_parent->m_lock); // Take lock to access m_loggers
      buffer.push_back('{');
      for (prof_logger_set_t::iterator l = m_parent->m_loggers.begin();
	   l != m_parent->m_loggers.end(); ++l)
      {
	(*l)->write_json_to_buf(buffer);
      }
      buffer.push_back('}');
      buffer.push_back('\0');
    }

    uint32_t len = htonl(buffer.size());
    ret = safe_write(connection_fd, &len, sizeof(len));
    if (ret < 0) {
      lderr(m_parent->m_cct) << "ProfLogThread: error writing message size: "
	  << cpp_strerror(ret) << dendl;
      close(connection_fd);
      return false;
    }
    ret = safe_write(connection_fd, &buffer[0], buffer.size());
    if (ret < 0) {
      lderr(m_parent->m_cct) << "ProfLogThread: error writing message: "
	  << cpp_strerror(ret) << dendl;
      close(connection_fd);
      return false;
    }

    close(connection_fd);
    ldout(m_parent->m_cct, 30) << "ProfLogThread: do_accept succeeded." << dendl;
    return true;
  }

  ProfLogThread(ProfLogThread &rhs);
  const ProfLogThread &operator=(const ProfLogThread &rhs);

  int m_sock_fd;
  int m_shutdown_fd;
  ProfLoggerCollection *m_parent;
};

ProfLoggerCollection::
ProfLoggerCollection(CephContext *cct)
  : m_cct(cct),
    m_thread(NULL),
    m_lock("ProfLoggerCollection"),
    m_shutdown_fd(-1)
{
}

ProfLoggerCollection::
~ProfLoggerCollection()
{
  Mutex::Locker lck(m_lock);
  shutdown();
  for (prof_logger_set_t::iterator l = m_loggers.begin();
	l != m_loggers.end(); ++l) {
    delete *l;
  }
  m_loggers.clear();
}

const char** ProfLoggerCollection::
get_tracked_conf_keys() const
{
  static const char *KEYS[] =
	{ "profiling_logger_uri", NULL };
  return KEYS;
}

void ProfLoggerCollection::
handle_conf_change(const md_config_t *conf,
		   const std::set <std::string> &changed)
{
  Mutex::Locker lck(m_lock);
  if (conf->profiling_logger_uri.empty()) {
    shutdown();
  }
  else {
    if (!init(conf->profiling_logger_uri)) {
      lderr(m_cct) << "Initializing profiling logger failed!" << dendl;
    }
  }
}

void ProfLoggerCollection::
logger_add(class ProfLogger *l)
{
  Mutex::Locker lck(m_lock);
  prof_logger_set_t::iterator i = m_loggers.find(l);
  assert(i == m_loggers.end());
  m_loggers.insert(l);
}

void ProfLoggerCollection::
logger_remove(class ProfLogger *l)
{
  Mutex::Locker lck(m_lock);
  prof_logger_set_t::iterator i = m_loggers.find(l);
  assert(i != m_loggers.end());
  delete *i;
  m_loggers.erase(i);
}

void ProfLoggerCollection::
logger_clear()
{
  Mutex::Locker lck(m_lock);
  prof_logger_set_t::iterator i = m_loggers.begin();
  prof_logger_set_t::iterator i_end = m_loggers.end();
  for (; i != i_end; ) {
    delete *i;
    m_loggers.erase(i++);
  }
}

bool ProfLoggerCollection::
init(const std::string &uri)
{
  /* Shut down old thread, if it exists.  */
  shutdown();

  /* Set up things for the new thread */
  std::string err;
  int pipe_rd, pipe_wr;
  err = ProfLogThread::create_shutdown_pipe(&pipe_rd, &pipe_wr);
  if (!err.empty()) {
    lderr(m_cct) << "ProfLoggerCollection::init: error: " << err << dendl;
    return false;
  }
  int sock_fd;
  err = ProfLogThread::bind_and_listen(uri, &sock_fd);
  if (!err.empty()) {
    lderr(m_cct) << "ProfLoggerCollection::init: failed: " << err << dendl;
    close(pipe_rd);
    close(pipe_wr);
    return false;
  }

  /* Create new thread */
  m_thread = new (std::nothrow) ProfLogThread(sock_fd, pipe_rd, this);
  if (!m_thread) {
    TEMP_FAILURE_RETRY(unlink(uri.c_str()));
    close(sock_fd);
    close(pipe_rd);
    close(pipe_wr);
    return false;
  }
  m_uri = uri;
  m_thread->create();
  m_shutdown_fd = pipe_wr;
  add_cleanup_file(m_uri.c_str());
  return true;
}

void ProfLoggerCollection::
shutdown()
{
  if (m_thread) {
    // Send a byte to the shutdown pipe that the thread is listening to
    char buf[1] = { 0x0 };
    int ret = safe_write(m_shutdown_fd, buf, sizeof(buf));
    m_shutdown_fd = -1;

    if (ret == 0) {
      // Join and delete the thread
      m_thread->join();
      delete m_thread;
    }
    else {
      lderr(m_cct) << "ProfLoggerCollection::shutdown: failed to write "
	      "to thread shutdown pipe: error " << ret << dendl;
    }
    m_thread = NULL;
    remove_cleanup_file(m_uri.c_str());
    m_uri.clear();
  }
}

ProfLogger::
~ProfLogger()
{
}

void ProfLogger::
inc(int idx, uint64_t amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  prof_log_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PROF_LOG_DATA_ANY_U64)
    return;
  data.u.u64 += amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

void ProfLogger::
set(int idx, uint64_t amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  prof_log_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PROF_LOG_DATA_ANY_U64)
    return;
  data.u.u64 = amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

uint64_t ProfLogger::
get(int idx) const
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const prof_log_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PROF_LOG_DATA_ANY_DOUBLE)
    return 0;
  return data.u.u64;
}

void ProfLogger::
finc(int idx, double amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  prof_log_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PROF_LOG_DATA_ANY_DOUBLE)
    return;
  data.u.dbl += amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

void ProfLogger::
fset(int idx, double amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  prof_log_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PROF_LOG_DATA_ANY_DOUBLE)
    return;
  data.u.dbl = amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

double ProfLogger::
fget(int idx) const
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const prof_log_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PROF_LOG_DATA_ANY_DOUBLE)
    return 0.0;
  return data.u.dbl;
}

void ProfLogger::
write_json_to_buf(std::vector <char> &buffer)
{
  char buf[512];
  Mutex::Locker lck(m_lock);

  prof_log_data_vec_t::const_iterator d = m_data.begin();
  prof_log_data_vec_t::const_iterator d_end = m_data.end();
  for (; d != d_end; ++d) {
    const prof_log_data_any_d &data(*d);
    buf[0] = '\0';
    if (d->count != COUNT_DISABLED) {
      switch (d->type) {
	case PROF_LOG_DATA_ANY_U64:
	  snprintf(buf, sizeof(buf), "'%s':{'count':%" PRId64 ","
		  "'sum':%" PRId64 "},", 
		  data.name, data.count, data.u.u64);
	  break;
	case PROF_LOG_DATA_ANY_DOUBLE:
	  snprintf(buf, sizeof(buf), "'%s':{'count':%" PRId64 ","
		  "'sum':%g},",
		  data.name, data.count, data.u.dbl);
	  break;
	default:
	  assert(0);
	  break;
      }
    }
    else {
      switch (d->type) {
	case PROF_LOG_DATA_ANY_U64:
	  snprintf(buf, sizeof(buf), "'%s':%" PRId64 ",",
		   data.name, data.u.u64);
	  break;
	case PROF_LOG_DATA_ANY_DOUBLE:
	  snprintf(buf, sizeof(buf), "'%s':%g,", data.name, data.u.dbl);
	  break;
	default:
	  assert(0);
	  break;
      }
    }
    size_t strlen_buf = strlen(buf);
    std::vector<char>::size_type sz = buffer.size();
    buffer.resize(sz + strlen_buf);
    memcpy(&buffer[sz], buf, strlen_buf);
  }
}

const std::string &ProfLogger::
get_name() const
{
  return m_name;
}

ProfLogger::
ProfLogger(CephContext *cct, const std::string &name,
	   int lower_bound, int upper_bound)
  : m_cct(cct),
    m_lower_bound(lower_bound),
    m_upper_bound(upper_bound),
    m_name(std::string("ProfLogger::") + name.c_str()),
    m_lock(m_name.c_str())
{
  m_data.resize(upper_bound - lower_bound - 1);
}

ProfLogger::prof_log_data_any_d::
prof_log_data_any_d()
  : name(NULL),
    type(PROF_LOG_DATA_ANY_NONE),
    count(COUNT_DISABLED)
{
  memset(&u, 0, sizeof(u));
}

ProfLoggerBuilder::
ProfLoggerBuilder(CephContext *cct, const std::string &name,
                  int first, int last)
  : m_prof_logger(new ProfLogger(cct, name, first, last))
{
}

ProfLoggerBuilder::
~ProfLoggerBuilder()
{
  if (m_prof_logger)
    delete m_prof_logger;
  m_prof_logger = NULL;
}

void ProfLoggerBuilder::
add_u64(int idx, const char *name)
{
  add_impl(idx, name, PROF_LOG_DATA_ANY_U64, COUNT_DISABLED);
}

void ProfLoggerBuilder::
add_fl(int idx, const char *name)
{
  add_impl(idx, name, PROF_LOG_DATA_ANY_DOUBLE, COUNT_DISABLED);
}

void ProfLoggerBuilder::
add_fl_avg(int idx, const char *name)
{
  add_impl(idx, name, PROF_LOG_DATA_ANY_DOUBLE, 0);
}

void ProfLoggerBuilder::
add_impl(int idx, const char *name, int ty, uint64_t count)
{
  assert(idx > m_prof_logger->m_lower_bound);
  assert(idx < m_prof_logger->m_upper_bound);
  ProfLogger::prof_log_data_vec_t &vec(m_prof_logger->m_data);
  ProfLogger::prof_log_data_any_d
    &data(vec[idx - m_prof_logger->m_lower_bound - 1]);
  data.name = name;
  data.type = ty;
  data.count = count;
}

ProfLogger *ProfLoggerBuilder::
create_proflogger()
{
  ProfLogger::prof_log_data_vec_t::const_iterator d = m_prof_logger->m_data.begin();
  ProfLogger::prof_log_data_vec_t::const_iterator d_end = m_prof_logger->m_data.end();
  for (; d != d_end; ++d) {
    assert(d->type != PROF_LOG_DATA_ANY_NONE);
  }
  ProfLogger *ret = m_prof_logger;
  m_prof_logger = NULL;
  return ret;
}
