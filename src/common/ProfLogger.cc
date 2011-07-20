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
#include "common/dout.h"
#include "common/errno.h"

#include <errno.h>
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

#define COUNT_DISABLED ((uint64_t)(int64_t)-1)

using std::ostringstream;

enum prof_log_data_any_t {
  PROF_LOG_DATA_ANY_NONE,
  PROF_LOG_DATA_ANY_U64,
  PROF_LOG_DATA_ANY_DOUBLE
};

ProfLoggerCollection::
ProfLoggerCollection(CephContext *cct)
  : m_cct(cct),
    m_lock("ProfLoggerCollection")
{
}

ProfLoggerCollection::
~ProfLoggerCollection()
{
  Mutex::Locker lck(m_lock);
  for (prof_logger_set_t::iterator l = m_loggers.begin();
	l != m_loggers.end(); ++l) {
    delete *l;
  }
  m_loggers.clear();
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

void ProfLoggerCollection::
write_json_to_buf(std::vector <char> &buffer)
{
  Mutex::Locker lck(m_lock);
  buffer.push_back('{');
  for (prof_logger_set_t::iterator l = m_loggers.begin();
       l != m_loggers.end(); ++l)
  {
    (*l)->write_json_to_buf(buffer);
  }
  buffer.push_back('}');
  buffer.push_back('\0');
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

static inline void append_to_vector(std::vector <char> &buffer, char *buf)
{
  size_t strlen_buf = strlen(buf);
  std::vector<char>::size_type sz = buffer.size();
  buffer.resize(sz + strlen_buf);
  memcpy(&buffer[sz], buf, strlen_buf);
}

void ProfLogger::
write_json_to_buf(std::vector <char> &buffer)
{
  char buf[512];
  Mutex::Locker lck(m_lock);

  snprintf(buf, sizeof(buf), "\"%s\":{", m_name.c_str());
  append_to_vector(buffer, buf);

  prof_log_data_vec_t::const_iterator d = m_data.begin();
  prof_log_data_vec_t::const_iterator d_end = m_data.end();
  for (; d != d_end; ++d) {
    const prof_log_data_any_d &data(*d);
    buf[0] = '\0';
    if (d->count != COUNT_DISABLED) {
      switch (d->type) {
	case PROF_LOG_DATA_ANY_U64:
	  snprintf(buf, sizeof(buf), "\"%s\":{\"count\":%" PRId64 ","
		  "\"sum\":%" PRId64 "},", 
		  data.name, data.count, data.u.u64);
	  break;
	case PROF_LOG_DATA_ANY_DOUBLE:
	  snprintf(buf, sizeof(buf), "\"%s\":{\"count\":%" PRId64 ","
		  "\"sum\":%g},",
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
	  snprintf(buf, sizeof(buf), "\"%s\":%" PRId64 ",",
		   data.name, data.u.u64);
	  break;
	case PROF_LOG_DATA_ANY_DOUBLE:
	  snprintf(buf, sizeof(buf), "\"%s\":%g,", data.name, data.u.dbl);
	  break;
	default:
	  assert(0);
	  break;
      }
    }
    append_to_vector(buffer, buf);
  }

  buffer.push_back('}');
  buffer.push_back(',');
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
    m_name(name.c_str()),
    m_lock_name(std::string("ProfLogger::") + name.c_str()),
    m_lock(m_lock_name.c_str())
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
