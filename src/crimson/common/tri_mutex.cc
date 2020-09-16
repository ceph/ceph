// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "tri_mutex.h"

seastar::future<> tri_mutex::lock_for_read()
{
  if (try_lock_for_read()) {
    return seastar::make_ready_future<>();
  }
  waiters.emplace_back(seastar::promise<>(), type_t::read);
  return waiters.back().pr.get_future();
}

bool tri_mutex::try_lock_for_read() noexcept
{
  if (!writers && !exclusively_used && waiters.empty()) {
    ++readers;
    return true;
  } else {
    return false;
  }
}

void tri_mutex::unlock_for_read()
{
  assert(readers > 0);
  if (--readers == 0) {
    wake();
  }
}

seastar::future<> tri_mutex::lock_for_write(bool greedy)
{
  if (try_lock_for_write(greedy)) {
    return seastar::make_ready_future<>();
  }
  waiters.emplace_back(seastar::promise<>(), type_t::write);
  return waiters.back().pr.get_future();
}

bool tri_mutex::try_lock_for_write(bool greedy) noexcept
{
  if (!readers && !exclusively_used) {
    if (greedy || waiters.empty()) {
      ++writers;
      return true;
    }
  }
  return false;
}

void tri_mutex::unlock_for_write()
{
  assert(writers > 0);
  if (--writers == 0) {
    wake();
  }
}

// for exclusive users
seastar::future<> tri_mutex::lock_for_excl()
{
  if (try_lock_for_excl()) {
    return seastar::make_ready_future<>();
  }
  waiters.emplace_back(seastar::promise<>(), type_t::exclusive);
  return waiters.back().pr.get_future();
}

bool tri_mutex::try_lock_for_excl() noexcept
{
  if (!readers && !writers && !exclusively_used) {
    exclusively_used = true;
    return true;
  } else {
    return false;
  }
}

void tri_mutex::unlock_for_excl()
{
  assert(exclusively_used);
  exclusively_used = false;
  wake();
}

bool tri_mutex::is_acquired() const
{
  if (readers) {
    return true;
  } else if (writers) {
    return true;
  } else if (exclusively_used) {
    return true;
  } else {
    return false;
  }
}

void tri_mutex::wake()
{
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
    waiter.pr.set_value();
    waiters.pop_front();
  }
}
