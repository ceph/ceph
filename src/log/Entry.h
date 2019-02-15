// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_ENTRY_H
#define __CEPH_LOG_ENTRY_H

#include "log/LogClock.h"

#include "common/StackStringStream.h"

#include "boost/container/small_vector.hpp"

#include <pthread.h>

#include <string_view>

namespace ceph {
namespace logging {

class Entry {
public:
  using time = log_time;

  Entry() = delete;
  Entry(short pr, short sub) :
    m_stamp(clock().now()),
    m_thread(pthread_self()),
    m_prio(pr),
    m_subsys(sub)
  {}
  Entry(const Entry &) = default;
  Entry& operator=(const Entry &) = default;
  Entry(Entry &&e) = default;
  Entry& operator=(Entry &&e) = default;
  virtual ~Entry() = default;

  virtual std::string_view strv() const = 0;
  virtual std::size_t size() const = 0;

  time m_stamp;
  pthread_t m_thread;
  short m_prio, m_subsys;

private:
  static log_clock& clock() {
    static log_clock clock;
    return clock;
  }
};

/* This should never be moved to the heap! Only allocate this on the stack. See
 * CachedStackStringStream for rationale.
 */
class MutableEntry : public Entry {
public:
  MutableEntry() = delete;
  MutableEntry(short pr, short sub) : Entry(pr, sub) {}
  MutableEntry(const MutableEntry&) = delete;
  MutableEntry& operator=(const MutableEntry&) = delete;
  MutableEntry(MutableEntry&&) = delete;
  MutableEntry& operator=(MutableEntry&&) = delete;
  ~MutableEntry() override = default;

  std::ostream& get_ostream() {
    return *cos;
  }

  std::string_view strv() const override {
    return cos->strv();
  }
  std::size_t size() const override {
    return cos->strv().size();
  }

private:
  CachedStackStringStream cos;
};

class ConcreteEntry : public Entry {
public:
  ConcreteEntry() = delete;
  ConcreteEntry(const Entry& e) : Entry(e) {
    auto strv = e.strv();
    str.reserve(strv.size());
    str.insert(str.end(), strv.begin(), strv.end());
  }
  ConcreteEntry& operator=(const Entry& e) {
    Entry::operator=(e);
    auto strv = e.strv();
    str.reserve(strv.size());
    str.assign(strv.begin(), strv.end());
    return *this;
  }
  ConcreteEntry(ConcreteEntry&& e) : Entry(e), str(std::move(e.str)) {}
  ConcreteEntry& operator=(ConcreteEntry&& e) {
    Entry::operator=(e);
    str = std::move(e.str);
    return *this;
  }
  ~ConcreteEntry() override = default;

  std::string_view strv() const override {
    return std::string_view(str.data(), str.size());
  }
  std::size_t size() const override {
    return str.size();
  }

private:
  boost::container::small_vector<char, 1024> str;
};

}
}

#endif
