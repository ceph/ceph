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

#include "common/perf_counters.h"
#include "common/dout.h"
#include "common/errno.h"

#include <errno.h>
#include <inttypes.h>
#include <map>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <string>

#define COUNT_DISABLED ((uint64_t)(int64_t)-1)

using std::ostringstream;

enum perf_counters_data_any_t {
  PERF_COUNTERS_DATA_ANY_NONE,
  PERF_COUNTERS_DATA_ANY_U64,
  PERF_COUNTERS_DATA_ANY_DOUBLE
};

PerfCountersCollection::
PerfCountersCollection(CephContext *cct)
  : m_cct(cct),
    m_lock("PerfCountersCollection")
{
}

PerfCountersCollection::
~PerfCountersCollection()
{
  Mutex::Locker lck(m_lock);
  for (perf_counters_set_t::iterator l = m_loggers.begin();
	l != m_loggers.end(); ++l) {
    delete *l;
  }
  m_loggers.clear();
}

void PerfCountersCollection::
logger_add(class PerfCounters *l)
{
  Mutex::Locker lck(m_lock);
  perf_counters_set_t::iterator i = m_loggers.find(l);
  assert(i == m_loggers.end());
  m_loggers.insert(l);
}

void PerfCountersCollection::
logger_remove(class PerfCounters *l)
{
  Mutex::Locker lck(m_lock);
  perf_counters_set_t::iterator i = m_loggers.find(l);
  assert(i != m_loggers.end());
  delete *i;
  m_loggers.erase(i);
}

void PerfCountersCollection::
logger_clear()
{
  Mutex::Locker lck(m_lock);
  perf_counters_set_t::iterator i = m_loggers.begin();
  perf_counters_set_t::iterator i_end = m_loggers.end();
  for (; i != i_end; ) {
    delete *i;
    m_loggers.erase(i++);
  }
}

void PerfCountersCollection::
write_json_to_buf(std::vector <char> &buffer)
{
  Mutex::Locker lck(m_lock);
  buffer.push_back('{');
  perf_counters_set_t::iterator l = m_loggers.begin();
  perf_counters_set_t::iterator l_end = m_loggers.end();
  if (l != l_end) {
    while (true) {
      (*l)->write_json_to_buf(buffer);
      if (++l == l_end)
	break;
      buffer.push_back(',');
    }
  }
  buffer.push_back('}');
  buffer.push_back('\0');
}

PerfCounters::
~PerfCounters()
{
}

void PerfCounters::
inc(int idx, uint64_t amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PERF_COUNTERS_DATA_ANY_U64)
    return;
  data.u.u64 += amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

void PerfCounters::
set(int idx, uint64_t amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PERF_COUNTERS_DATA_ANY_U64)
    return;
  data.u.u64 = amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

uint64_t PerfCounters::
get(int idx) const
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PERF_COUNTERS_DATA_ANY_DOUBLE)
    return 0;
  return data.u.u64;
}

void PerfCounters::
finc(int idx, double amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PERF_COUNTERS_DATA_ANY_DOUBLE)
    return;
  data.u.dbl += amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

void PerfCounters::
fset(int idx, double amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PERF_COUNTERS_DATA_ANY_DOUBLE)
    return;
  data.u.dbl = amt;
  if (data.count != COUNT_DISABLED)
    data.count++;
}

double PerfCounters::
fget(int idx) const
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (data.type != PERF_COUNTERS_DATA_ANY_DOUBLE)
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

void PerfCounters::
write_json_to_buf(std::vector <char> &buffer)
{
  char buf[512];
  Mutex::Locker lck(m_lock);

  snprintf(buf, sizeof(buf), "\"%s\":{", m_name.c_str());
  append_to_vector(buffer, buf);

  perf_counter_data_vec_t::const_iterator d = m_data.begin();
  perf_counter_data_vec_t::const_iterator d_end = m_data.end();
  if (d == d_end) {
    buffer.push_back('}');
    return;
  }
  while (true) {
    const perf_counter_data_any_d &data(*d);
    buf[0] = '\0';
    if (d->count != COUNT_DISABLED) {
      switch (d->type) {
	case PERF_COUNTERS_DATA_ANY_U64:
	  snprintf(buf, sizeof(buf), "\"%s\":{\"count\":%" PRId64 ","
		  "\"sum\":%" PRId64 "}", 
		  data.name, data.count, data.u.u64);
	  break;
	case PERF_COUNTERS_DATA_ANY_DOUBLE:
	  snprintf(buf, sizeof(buf), "\"%s\":{\"count\":%" PRId64 ","
		  "\"sum\":%g}",
		  data.name, data.count, data.u.dbl);
	  break;
	default:
	  assert(0);
	  break;
      }
    }
    else {
      switch (d->type) {
	case PERF_COUNTERS_DATA_ANY_U64:
	  snprintf(buf, sizeof(buf), "\"%s\":%" PRId64,
		   data.name, data.u.u64);
	  break;
	case PERF_COUNTERS_DATA_ANY_DOUBLE:
	  snprintf(buf, sizeof(buf), "\"%s\":%g", data.name, data.u.dbl);
	  break;
	default:
	  assert(0);
	  break;
      }
    }
    append_to_vector(buffer, buf);
    if (++d == d_end)
      break;
    buffer.push_back(',');
  }
  buffer.push_back('}');
}

const std::string &PerfCounters::
get_name() const
{
  return m_name;
}

PerfCounters::
PerfCounters(CephContext *cct, const std::string &name,
	   int lower_bound, int upper_bound)
  : m_cct(cct),
    m_lower_bound(lower_bound),
    m_upper_bound(upper_bound),
    m_name(name.c_str()),
    m_lock_name(std::string("PerfCounters::") + name.c_str()),
    m_lock(m_lock_name.c_str())
{
  m_data.resize(upper_bound - lower_bound - 1);
}

PerfCounters::perf_counter_data_any_d::
perf_counter_data_any_d()
  : name(NULL),
    type(PERF_COUNTERS_DATA_ANY_NONE),
    count(COUNT_DISABLED)
{
  memset(&u, 0, sizeof(u));
}

PerfCountersBuilder::
PerfCountersBuilder(CephContext *cct, const std::string &name,
                  int first, int last)
  : m_perf_counters(new PerfCounters(cct, name, first, last))
{
}

PerfCountersBuilder::
~PerfCountersBuilder()
{
  if (m_perf_counters)
    delete m_perf_counters;
  m_perf_counters = NULL;
}

void PerfCountersBuilder::
add_u64(int idx, const char *name)
{
  add_impl(idx, name, PERF_COUNTERS_DATA_ANY_U64, COUNT_DISABLED);
}

void PerfCountersBuilder::
add_fl(int idx, const char *name)
{
  add_impl(idx, name, PERF_COUNTERS_DATA_ANY_DOUBLE, COUNT_DISABLED);
}

void PerfCountersBuilder::
add_fl_avg(int idx, const char *name)
{
  add_impl(idx, name, PERF_COUNTERS_DATA_ANY_DOUBLE, 0);
}

void PerfCountersBuilder::
add_impl(int idx, const char *name, int ty, uint64_t count)
{
  assert(idx > m_perf_counters->m_lower_bound);
  assert(idx < m_perf_counters->m_upper_bound);
  PerfCounters::perf_counter_data_vec_t &vec(m_perf_counters->m_data);
  PerfCounters::perf_counter_data_any_d
    &data(vec[idx - m_perf_counters->m_lower_bound - 1]);
  data.name = name;
  data.type = ty;
  data.count = count;
}

PerfCounters *PerfCountersBuilder::
create_perf_counters()
{
  PerfCounters::perf_counter_data_vec_t::const_iterator d = m_perf_counters->m_data.begin();
  PerfCounters::perf_counter_data_vec_t::const_iterator d_end = m_perf_counters->m_data.end();
  for (; d != d_end; ++d) {
    assert(d->type != PERF_COUNTERS_DATA_ANY_NONE);
  }
  PerfCounters *ret = m_perf_counters;
  m_perf_counters = NULL;
  return ret;
}
