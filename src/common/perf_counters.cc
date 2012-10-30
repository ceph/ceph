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

using std::ostringstream;

PerfCountersCollection::PerfCountersCollection(CephContext *cct)
  : m_cct(cct),
    m_lock("PerfCountersCollection")
{
}

PerfCountersCollection::~PerfCountersCollection()
{
  clear();
}

void PerfCountersCollection::add(class PerfCounters *l)
{
  Mutex::Locker lck(m_lock);

  // make sure the name is unique
  perf_counters_set_t::iterator i;
  i = m_loggers.find(l);
  while (i != m_loggers.end()) {
    ostringstream ss;
    ss << l->get_name() << "-" << (void*)l;
    l->set_name(ss.str());
    i = m_loggers.find(l);
  }

  m_loggers.insert(l);
}

void PerfCountersCollection::remove(class PerfCounters *l)
{
  Mutex::Locker lck(m_lock);
  perf_counters_set_t::iterator i = m_loggers.find(l);
  assert(i != m_loggers.end());
  m_loggers.erase(i);
}

void PerfCountersCollection::clear()
{
  Mutex::Locker lck(m_lock);
  perf_counters_set_t::iterator i = m_loggers.begin();
  perf_counters_set_t::iterator i_end = m_loggers.end();
  for (; i != i_end; ) {
    m_loggers.erase(i++);
  }
}

void PerfCountersCollection::write_json_to_buf(bufferlist& bl, bool schema)
{
  Mutex::Locker lck(m_lock);
  bl.append('{');
  perf_counters_set_t::iterator l = m_loggers.begin();
  perf_counters_set_t::iterator l_end = m_loggers.end();
  if (l != l_end) {
    while (true) {
      (*l)->write_json_to_buf(bl, schema);
      if (++l == l_end)
	break;
      bl.append(',');
    }
  }
  bl.append('}');
}

// ---------------------------

PerfCounters::~PerfCounters()
{
}

void PerfCounters::inc(int idx, uint64_t amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return;
  data.u.u64 += amt;
  if (data.type & PERFCOUNTER_LONGRUNAVG)
    data.avgcount++;
}

void PerfCounters::dec(int idx, uint64_t amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  assert(!(data.type & PERFCOUNTER_LONGRUNAVG));
  if (!(data.type & PERFCOUNTER_U64))
    return;
  assert(data.u.u64 >= amt);
  data.u.u64 -= amt;
}

void PerfCounters::set(int idx, uint64_t amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return;
  data.u.u64 = amt;
  if (data.type & PERFCOUNTER_LONGRUNAVG)
    data.avgcount++;
}

uint64_t PerfCounters::get(int idx) const
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return 0;
  return data.u.u64;
}

void PerfCounters::finc(int idx, double amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_FLOAT))
    return;
  data.u.dbl += amt;
  if (data.type & PERFCOUNTER_LONGRUNAVG)
    data.avgcount++;
}

void PerfCounters::fset(int idx, double amt)
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_FLOAT))
    return;
  data.u.dbl = amt;
  if (data.type & PERFCOUNTER_LONGRUNAVG)
    assert(0);
}

double PerfCounters::fget(int idx) const
{
  Mutex::Locker lck(m_lock);
  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_FLOAT))
    return 0.0;
  return data.u.dbl;
}

void PerfCounters::write_json_to_buf(bufferlist& bl, bool schema)
{
  char buf[512];
  Mutex::Locker lck(m_lock);

  snprintf(buf, sizeof(buf), "\"%s\":{", m_name.c_str());
  bl.append(buf);

  perf_counter_data_vec_t::const_iterator d = m_data.begin();
  perf_counter_data_vec_t::const_iterator d_end = m_data.end();
  if (d == d_end) {
    bl.append('}');
    return;
  }
  while (true) {
    const perf_counter_data_any_d &data(*d);
    buf[0] = '\0';
    if (schema)
      data.write_schema_json(buf, sizeof(buf));
    else
      data.write_json(buf, sizeof(buf));

    bl.append(buf);
    if (++d == d_end)
      break;
    bl.append(',');
  }
  bl.append('}');
}

const std::string &PerfCounters::get_name() const
{
  return m_name;
}

PerfCounters::PerfCounters(CephContext *cct, const std::string &name,
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

PerfCounters::perf_counter_data_any_d::perf_counter_data_any_d()
  : name(NULL),
    type(PERFCOUNTER_NONE),
    avgcount(0)
{
  memset(&u, 0, sizeof(u));
}

void  PerfCounters::perf_counter_data_any_d::write_schema_json(char *buf, size_t buf_sz) const
{
  snprintf(buf, buf_sz, "\"%s\":{\"type\":%d}", name, type);
}

void  PerfCounters::perf_counter_data_any_d::write_json(char *buf, size_t buf_sz) const
{
  if (type & PERFCOUNTER_LONGRUNAVG) {
    if (type & PERFCOUNTER_U64) {
      snprintf(buf, buf_sz, "\"%s\":{\"avgcount\":%" PRId64 ","
	      "\"sum\":%" PRId64 "}", 
	      name, avgcount, u.u64);
    }
    else if (type & PERFCOUNTER_FLOAT) {
      snprintf(buf, buf_sz, "\"%s\":{\"avgcount\":%" PRId64 ","
	      "\"sum\":%g}",
	      name, avgcount, u.dbl);
    }
    else {
      assert(0);
    }
  }
  else {
    if (type & PERFCOUNTER_U64) {
      snprintf(buf, buf_sz, "\"%s\":%" PRId64,
	       name, u.u64);
    }
    else if (type & PERFCOUNTER_FLOAT) {
      snprintf(buf, buf_sz, "\"%s\":%g", name, u.dbl);
    }
    else {
      assert(0);
    }
  }
}

PerfCountersBuilder::PerfCountersBuilder(CephContext *cct, const std::string &name,
                  int first, int last)
  : m_perf_counters(new PerfCounters(cct, name, first, last))
{
}

PerfCountersBuilder::~PerfCountersBuilder()
{
  if (m_perf_counters)
    delete m_perf_counters;
  m_perf_counters = NULL;
}

void PerfCountersBuilder::add_u64_counter(int idx, const char *name)
{
  add_impl(idx, name, PERFCOUNTER_U64 | PERFCOUNTER_COUNTER);
}

void PerfCountersBuilder::add_u64(int idx, const char *name)
{
  add_impl(idx, name, PERFCOUNTER_U64);
}

void PerfCountersBuilder::add_u64_avg(int idx, const char *name)
{
  add_impl(idx, name, PERFCOUNTER_U64 | PERFCOUNTER_LONGRUNAVG);
}

void PerfCountersBuilder::add_fl(int idx, const char *name)
{
  add_impl(idx, name, PERFCOUNTER_FLOAT);
}

void PerfCountersBuilder::add_fl_avg(int idx, const char *name)
{
  add_impl(idx, name, PERFCOUNTER_FLOAT | PERFCOUNTER_LONGRUNAVG);
}

void PerfCountersBuilder::add_impl(int idx, const char *name, int ty)
{
  assert(idx > m_perf_counters->m_lower_bound);
  assert(idx < m_perf_counters->m_upper_bound);
  PerfCounters::perf_counter_data_vec_t &vec(m_perf_counters->m_data);
  PerfCounters::perf_counter_data_any_d
    &data(vec[idx - m_perf_counters->m_lower_bound - 1]);
  assert(data.type == PERFCOUNTER_NONE);
  data.name = name;
  data.type = (enum perfcounter_type_d)ty;
}

PerfCounters *PerfCountersBuilder::create_perf_counters()
{
  PerfCounters::perf_counter_data_vec_t::const_iterator d = m_perf_counters->m_data.begin();
  PerfCounters::perf_counter_data_vec_t::const_iterator d_end = m_perf_counters->m_data.end();
  for (; d != d_end; ++d) {
    if (d->type == PERFCOUNTER_NONE) {
      assert(d->type != PERFCOUNTER_NONE);
    }
  }
  PerfCounters *ret = m_perf_counters;
  m_perf_counters = NULL;
  return ret;
}
