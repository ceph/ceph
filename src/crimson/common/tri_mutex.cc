// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "tri_mutex.h"

#include <seastar/util/later.hh>

SET_SUBSYS(osd);
//TODO: SET_SUBSYS(crimson_tri_mutex);

seastar::future<> read_lock::lock()
{
  return static_cast<tri_mutex*>(this)->lock_for_read();
}

void read_lock::unlock()
{
  static_cast<tri_mutex*>(this)->unlock_for_read();
}

seastar::future<> write_lock::lock()
{
  return static_cast<tri_mutex*>(this)->lock_for_write();
}

void write_lock::unlock()
{
  static_cast<tri_mutex*>(this)->unlock_for_write();
}

seastar::future<> excl_lock::lock()
{
  return static_cast<tri_mutex*>(this)->lock_for_excl();
}

void excl_lock::unlock()
{
  static_cast<tri_mutex*>(this)->unlock_for_excl();
}

tri_mutex::~tri_mutex()
{
  LOG_PREFIX(tri_mutex::~tri_mutex());
  DEBUGDPP("", *this);
  assert(!is_acquired());
}

seastar::future<> tri_mutex::lock_for_read()
{
  LOG_PREFIX(tri_mutex::lock_for_read());
  DEBUGDPP("", *this);
  if (try_lock_for_read()) {
    DEBUGDPP("lock_for_read successfully", *this);
    return seastar::now();
  }
  DEBUGDPP("can't lock_for_read, adding to waiters", *this);
  waiters.emplace_back(seastar::promise<>(), type_t::read);
  return waiters.back().pr.get_future();
}

bool tri_mutex::try_lock_for_read() noexcept
{
  LOG_PREFIX(tri_mutex::try_lock_for_read());
  DEBUGDPP("", *this);
  if (!writers && !exclusively_used && waiters.empty()) {
    ++readers;
    return true;
  }
  return false;
}

void tri_mutex::unlock_for_read()
{
  LOG_PREFIX(tri_mutex::unlock_for_read());
  DEBUGDPP("", *this);
  assert(readers > 0);
  if (--readers == 0) {
    wake();
  }
}

void tri_mutex::demote_to_read()
{
  LOG_PREFIX(tri_mutex::demote_to_read());
  DEBUGDPP("", *this);
  assert(exclusively_used);
  exclusively_used = false;
  ++readers;
}

seastar::future<> tri_mutex::lock_for_write()
{
  LOG_PREFIX(tri_mutex::lock_for_write());
  DEBUGDPP("", *this);
  if (try_lock_for_write()) {
    DEBUGDPP("lock_for_write successfully", *this);
    return seastar::now();
  }
  DEBUGDPP("can't lock_for_write, adding to waiters", *this);
  waiters.emplace_back(seastar::promise<>(), type_t::write);
  return waiters.back().pr.get_future();
}

bool tri_mutex::try_lock_for_write() noexcept
{
  LOG_PREFIX(tri_mutex::try_lock_for_write());
  DEBUGDPP("", *this);
  if (!readers && !exclusively_used && waiters.empty()) {
    ++writers;
    return true;
  }
  return false;
}

void tri_mutex::unlock_for_write()
{
  LOG_PREFIX(tri_mutex::unlock_for_write());
  DEBUGDPP("", *this);
  assert(writers > 0);
  if (--writers == 0) {
    wake();
  }
}

void tri_mutex::demote_to_write()
{
  LOG_PREFIX(tri_mutex::demote_to_write());
  DEBUGDPP("", *this);
  assert(exclusively_used);
  exclusively_used = false;
  ++writers;
}

// for exclusive users
seastar::future<> tri_mutex::lock_for_excl()
{
  LOG_PREFIX(tri_mutex::lock_for_excl());
  DEBUGDPP("", *this);
  if (try_lock_for_excl()) {
    DEBUGDPP("lock_for_excl, successfully", *this);
    return seastar::now();
  }
  DEBUGDPP("can't lock_for_excl, adding to waiters", *this);
  waiters.emplace_back(seastar::promise<>(), type_t::exclusive);
  return waiters.back().pr.get_future();
}

bool tri_mutex::try_lock_for_excl() noexcept
{
  LOG_PREFIX(tri_mutex::try_lock_for_excl());
  DEBUGDPP("", *this);
  if (readers == 0u && writers == 0u && !exclusively_used) {
    exclusively_used = true;
    return true;
  } else {
    return false;
  }
}

void tri_mutex::unlock_for_excl()
{
  LOG_PREFIX(tri_mutex::unlock_for_excl());
  DEBUGDPP("", *this);
  assert(exclusively_used);
  exclusively_used = false;
  wake();
}

bool tri_mutex::is_acquired() const
{
  LOG_PREFIX(tri_mutex::is_acquired());
  DEBUGDPP("", *this);
  if (readers != 0u) {
    return true;
  } else if (writers != 0u) {
    return true;
  } else if (exclusively_used) {
    return true;
  } else {
    return false;
  }
}

void tri_mutex::wake()
{
  LOG_PREFIX(tri_mutex::wake());
  DEBUGDPP("", *this);
  assert(!readers && !writers && !exclusively_used);
  type_t type = type_t::none;
  while (!waiters.empty()) {
    auto& waiter = waiters.front();
    if (type == type_t::exclusive) {
      break;
    } if (type == type_t::none) {
      type = waiter.type;
    } else if (type != waiter.type) {
      // to be woken in the next batch
      break;
    }
    switch (type) {
    case type_t::read:
      ++readers;
      break;
    case type_t::write:
      ++writers;
      break;
    case type_t::exclusive:
      exclusively_used = true;
      break;
    default:
      assert(0);
    }
    DEBUGDPP("waking up", *this);
    waiter.pr.set_value();
    waiters.pop_front();
  }
  DEBUGDPP("no waiters", *this);
}
