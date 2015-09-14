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

#include "include/int_types.h"

#include "common/perf_counters.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Formatter.h"

#include <errno.h>
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

bool PerfCountersCollection::reset(const std::string &name)
{
  bool result = false;
  Mutex::Locker lck(m_lock);
  perf_counters_set_t::iterator i = m_loggers.begin();
  perf_counters_set_t::iterator i_end = m_loggers.end();

  if (!strcmp(name.c_str(), "all"))  {
    while (i != i_end) {
      (*i)->reset();
      ++i;
    }
    result = true;
  } else {
    while (i != i_end) {
      if (!name.compare((*i)->get_name())) {
	(*i)->reset();
	result = true;
	break;
      }
      ++i;
    }
  }

  return result;
}


/**
 * Serialize current values of performance counters.  Optionally
 * output the schema instead, or filter output to a particular
 * PerfCounters or particular named counter.
 *
 * @param logger name of subsystem logger, e.g. "mds_cache", may be empty
 * @param counter name of counter within subsystem, e.g. "num_strays",
 *                may be empty.
 * @param schema if true, output schema instead of current data.
 */
void PerfCountersCollection::dump_formatted(
    Formatter *f,
    bool schema,
    const std::string &logger,
    const std::string &counter)
{
  Mutex::Locker lck(m_lock);
  f->open_object_section("perfcounter_collection");
  
  for (perf_counters_set_t::iterator l = m_loggers.begin();
       l != m_loggers.end(); ++l) {
    // Optionally filter on logger name, pass through counter filter
    if (logger.empty() || (*l)->get_name() == logger) {
      (*l)->dump_formatted(f, schema, counter);
    }
  }
  f->close_section();
}

// ---------------------------

PerfCounters::~PerfCounters()
{
}

void PerfCounters::inc(int idx, uint64_t amt)
{
  if (!m_cct->_conf->perf)
    return;

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return;
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount.inc();
    data.u64.add(amt);
    data.avgcount2.inc();
  } else {
    data.u64.add(amt);
  }
}

void PerfCounters::dec(int idx, uint64_t amt)
{
  if (!m_cct->_conf->perf)
    return;

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  assert(!(data.type & PERFCOUNTER_LONGRUNAVG));
  if (!(data.type & PERFCOUNTER_U64))
    return;
  data.u64.sub(amt);
}

void PerfCounters::set(int idx, uint64_t amt)
{
  if (!m_cct->_conf->perf)
    return;

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return;
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount.inc();
    data.u64.set(amt);
    data.avgcount2.inc();
  } else {
    data.u64.set(amt);
  }
}

uint64_t PerfCounters::get(int idx) const
{
  if (!m_cct->_conf->perf)
    return 0;

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return 0;
  return data.u64.read();
}

void PerfCounters::tinc(int idx, utime_t amt)
{
  if (!m_cct->_conf->perf)
    return;

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return;
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount.inc();
    data.u64.add(amt.to_nsec());
    data.avgcount2.inc();
  } else {
    data.u64.add(amt.to_nsec());
  }
}

void PerfCounters::tinc(int idx, ceph::timespan amt)
{
  if (!m_cct->_conf->perf)
    return;

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return;
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount.inc();
    data.u64.add(amt.count());
    data.avgcount2.inc();
  } else {
    data.u64.add(amt.count());
  }
}

void PerfCounters::tset(int idx, utime_t amt)
{
  if (!m_cct->_conf->perf)
    return;

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return;
  data.u64.set(amt.to_nsec());
  if (data.type & PERFCOUNTER_LONGRUNAVG)
    assert(0);
}

utime_t PerfCounters::tget(int idx) const
{
  if (!m_cct->_conf->perf)
    return utime_t();

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return utime_t();
  uint64_t v = data.u64.read();
  return utime_t(v / 1000000000ull, v % 1000000000ull);
}

pair<uint64_t, uint64_t> PerfCounters::get_tavg_ms(int idx) const
{
  if (!m_cct->_conf->perf)
    return make_pair(0, 0);

  assert(idx > m_lower_bound);
  assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return make_pair(0, 0);
  if (!(data.type & PERFCOUNTER_LONGRUNAVG))
    return make_pair(0, 0);
  pair<uint64_t,uint64_t> a = data.read_avg();
  return make_pair(a.second, a.first / 1000000ull);
}

void PerfCounters::reset()
{
  perf_counter_data_vec_t::iterator d = m_data.begin();
  perf_counter_data_vec_t::iterator d_end = m_data.end();

  while (d != d_end) {
    d->reset();
    ++d;
  }
}

void PerfCounters::dump_formatted(Formatter *f, bool schema,
    const std::string &counter)
{
  f->open_object_section(m_name.c_str());
  
  for (perf_counter_data_vec_t::const_iterator d = m_data.begin();
       d != m_data.end(); ++d) {
    if (!counter.empty() && counter != d->name) {
      // Optionally filter on counter name
      continue;
    }

    if (schema) {
      f->open_object_section(d->name);
      f->dump_int("type", d->type);

      if (d->description) {
        f->dump_string("description", d->description);
      } else {
        f->dump_string("description", "");
      }

      if (d->nick != NULL) {
        f->dump_string("nick", d->nick);
      } else {
        f->dump_string("nick", "");
      }
      f->close_section();
    } else {
      if (d->type & PERFCOUNTER_LONGRUNAVG) {
	f->open_object_section(d->name);
	pair<uint64_t,uint64_t> a = d->read_avg();
	if (d->type & PERFCOUNTER_U64) {
	  f->dump_unsigned("avgcount", a.second);
	  f->dump_unsigned("sum", a.first);
	} else if (d->type & PERFCOUNTER_TIME) {
	  f->dump_unsigned("avgcount", a.second);
	  f->dump_format_unquoted("sum", "%" PRId64 ".%09" PRId64,
				  a.first / 1000000000ull,
				  a.first % 1000000000ull);
	} else {
	  assert(0);
	}
	f->close_section();
      } else {
	uint64_t v = d->u64.read();
	if (d->type & PERFCOUNTER_U64) {
	  f->dump_unsigned(d->name, v);
	} else if (d->type & PERFCOUNTER_TIME) {
	  f->dump_format_unquoted(d->name, "%" PRId64 ".%09" PRId64,
				  v / 1000000000ull,
				  v % 1000000000ull);
	} else {
	  assert(0);
	}
      }
    }
  }
  f->close_section();
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

void PerfCountersBuilder::add_u64_counter(int idx, const char *name,
    const char *description, const char *nick)
{
  add_impl(idx, name, description, nick, PERFCOUNTER_U64 | PERFCOUNTER_COUNTER);
}

void PerfCountersBuilder::add_u64(int idx, const char *name,
    const char *description, const char *nick)
{
  add_impl(idx, name, description, nick, PERFCOUNTER_U64);
}

void PerfCountersBuilder::add_u64_avg(int idx, const char *name,
    const char *description, const char *nick)
{
  add_impl(idx, name, description, nick, PERFCOUNTER_U64 | PERFCOUNTER_LONGRUNAVG);
}

void PerfCountersBuilder::add_time(int idx, const char *name,
    const char *description, const char *nick)
{
  add_impl(idx, name, description, nick, PERFCOUNTER_TIME);
}

void PerfCountersBuilder::add_time_avg(int idx, const char *name,
    const char *description, const char *nick)
{
  add_impl(idx, name, description, nick, PERFCOUNTER_TIME | PERFCOUNTER_LONGRUNAVG);
}

void PerfCountersBuilder::add_impl(int idx, const char *name,
    const char *description, const char *nick, int ty)
{
  assert(idx > m_perf_counters->m_lower_bound);
  assert(idx < m_perf_counters->m_upper_bound);
  PerfCounters::perf_counter_data_vec_t &vec(m_perf_counters->m_data);
  PerfCounters::perf_counter_data_any_d
    &data(vec[idx - m_perf_counters->m_lower_bound - 1]);
  assert(data.type == PERFCOUNTER_NONE);
  data.name = name;
  data.description = description;
  data.nick = nick;
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
