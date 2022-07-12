#include "condition_variable_debug.h"
#include "common/mutex_debug.h"

namespace ceph {

condition_variable_debug::condition_variable_debug()
  : waiter_mutex{nullptr}
{
  int r = pthread_cond_init(&cond, nullptr);
  if (r) {
    throw std::system_error(r, std::generic_category());
  }
}

condition_variable_debug::~condition_variable_debug()
{
  pthread_cond_destroy(&cond);
}

void condition_variable_debug::wait(std::unique_lock<mutex_debug>& lock)
{
  // make sure this cond is used with one mutex only
  ceph_assert(waiter_mutex == nullptr ||
         waiter_mutex == lock.mutex());
  waiter_mutex = lock.mutex();
  ceph_assert(waiter_mutex->is_locked());
  waiter_mutex->_pre_unlock();
  if (int r = pthread_cond_wait(&cond, waiter_mutex->native_handle());
      r != 0) {
    throw std::system_error(r, std::generic_category());
  }
  waiter_mutex->_post_lock();
}

void condition_variable_debug::notify_one()
{
  // make sure signaler is holding the waiter's lock.
  ceph_assert(waiter_mutex == nullptr ||
         waiter_mutex->is_locked());
  if (int r = pthread_cond_signal(&cond); r != 0) {
    throw std::system_error(r, std::generic_category());
  }
}

void condition_variable_debug::notify_all(bool sloppy)
{
  if (!sloppy) {
    // make sure signaler is holding the waiter's lock.
    ceph_assert(waiter_mutex == NULL ||
                waiter_mutex->is_locked());
  }
  if (int r = pthread_cond_broadcast(&cond); r != 0 && !sloppy) {
    throw std::system_error(r, std::generic_category());
  }
}

std::cv_status condition_variable_debug::_wait_until(mutex_debug* mutex,
                                                     timespec* ts)
{
  // make sure this cond is used with one mutex only
  ceph_assert(waiter_mutex == nullptr ||
         waiter_mutex == mutex);
  waiter_mutex = mutex;
  ceph_assert(waiter_mutex->is_locked());

  waiter_mutex->_pre_unlock();
  int r = pthread_cond_timedwait(&cond, waiter_mutex->native_handle(), ts);
  waiter_mutex->_post_lock();
  switch (r) {
  case 0:
    return std::cv_status::no_timeout;
  case ETIMEDOUT:
    return std::cv_status::timeout;
  default:
    throw std::system_error(r, std::generic_category());
  }
}

} // namespace ceph
