#include <algorithm>
#include <chrono>
#include <thread>
#include <errno.h>
#include <sys/time.h>
#include "gtest/gtest.h"

#include "include/rados/librados.hpp"
#include "cls/lock/cls_lock_client.h"

#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"

using namespace std::chrono_literals;
using namespace librados;

typedef RadosTestPP LibRadosLockPP;
typedef RadosTestECPP LibRadosLockECPP;

TEST_F(LibRadosLockPP, LockExclusivePP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockPP1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockPP, LockSharedPP) {
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockPP2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_shared("foo", "TestLockPP2", "Cookie", "Tag", "", NULL, 0));
}

TEST_F(LibRadosLockPP, LockExclusiveDurPP) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_exclusive = [this](timeval* tv) {
    return ioctx.lock_exclusive("foo", "TestLockPP3", "Cookie", "", tv, 0);
  };
  constexpr int expected = 0;
  ASSERT_EQ(expected, lock_exclusive(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_exclusive, nullptr));
}

TEST_F(LibRadosLockPP, LockSharedDurPP) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_shared = [this](timeval* tv) {
    return ioctx.lock_shared("foo", "TestLockPP4", "Cookie", "Tag", "", tv, 0);
  };
  constexpr int expected = 0;
  ASSERT_EQ(expected, lock_shared(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_shared, nullptr));
}

TEST_F(LibRadosLockPP, LockMayRenewPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP5", "Cookie", "", NULL, LOCK_FLAG_MAY_RENEW));
}

TEST_F(LibRadosLockPP, UnlockPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.unlock("foo", "TestLockPP6", "Cookie"));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP6", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockPP, ListLockersPP) {
  std::stringstream sstm;
  sstm << "client." << cluster.get_instance_id();
  std::string me = sstm.str();
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockPP7", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(0, ioctx.unlock("foo", "TestLockPP7", "Cookie"));
  {
    int exclusive;
    std::string tag;
    std::list<librados::locker_t> lockers;
    ASSERT_EQ(0, ioctx.list_lockers("foo", "TestLockPP7", &exclusive, &tag, &lockers));
  }
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockPP7", "Cookie", "Tag", "", NULL, 0));
  {
    int exclusive;
    std::string tag;
    std::list<librados::locker_t> lockers;
    ASSERT_EQ(1, ioctx.list_lockers("foo", "TestLockPP7", &exclusive, &tag, &lockers));
    std::list<librados::locker_t>::iterator it = lockers.begin();
    ASSERT_FALSE(lockers.end() == it);
    ASSERT_EQ(me, it->client);
    ASSERT_EQ("Cookie", it->cookie);
  }
}

TEST_F(LibRadosLockPP, BreakLockPP) {
  int exclusive;
  std::string tag;
  std::list<librados::locker_t> lockers;
  std::stringstream sstm;
  sstm << "client." << cluster.get_instance_id();
  std::string me = sstm.str();
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP8", "Cookie",  "", NULL, 0));
  ASSERT_EQ(1, ioctx.list_lockers("foo", "TestLockPP8", &exclusive, &tag, &lockers));
  std::list<librados::locker_t>::iterator it = lockers.begin();
  ASSERT_FALSE(lockers.end() == it);
  ASSERT_EQ(me, it->client);
  ASSERT_EQ("Cookie", it->cookie);
  ASSERT_EQ(0, ioctx.break_lock("foo", "TestLockPP8", it->client, "Cookie"));
}

// EC testing
TEST_F(LibRadosLockECPP, LockExclusivePP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockECPP1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockECPP, LockSharedPP) {
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockECPP2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_shared("foo", "TestLockECPP2", "Cookie", "Tag", "", NULL, 0));
}

TEST_F(LibRadosLockECPP, LockExclusiveDurPP) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_exclusive = [this](timeval* tv) {
    return ioctx.lock_exclusive("foo", "TestLockECPP3", "Cookie", "", tv, 0);
  };
  constexpr int expected = 0;
  ASSERT_EQ(expected, lock_exclusive(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_exclusive, nullptr));
}

TEST_F(LibRadosLockECPP, LockSharedDurPP) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_shared = [this](timeval* tv) {
    return ioctx.lock_shared("foo", "TestLockECPP4", "Cookie", "Tag", "", tv, 0);
  };
  const int expected = 0;
  ASSERT_EQ(expected, lock_shared(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_shared, nullptr));
}

TEST_F(LibRadosLockECPP, LockMayRenewPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockECPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP5", "Cookie", "", NULL, LOCK_FLAG_MAY_RENEW));
}

TEST_F(LibRadosLockECPP, UnlockPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.unlock("foo", "TestLockECPP6", "Cookie"));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP6", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockECPP, ListLockersPP) {
  std::stringstream sstm;
  sstm << "client." << cluster.get_instance_id();
  std::string me = sstm.str();
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockECPP7", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(0, ioctx.unlock("foo", "TestLockECPP7", "Cookie"));
  {
    int exclusive;
    std::string tag;
    std::list<librados::locker_t> lockers;
    ASSERT_EQ(0, ioctx.list_lockers("foo", "TestLockECPP7", &exclusive, &tag, &lockers));
  }
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockECPP7", "Cookie", "Tag", "", NULL, 0));
  {
    int exclusive;
    std::string tag;
    std::list<librados::locker_t> lockers;
    ASSERT_EQ(1, ioctx.list_lockers("foo", "TestLockECPP7", &exclusive, &tag, &lockers));
    std::list<librados::locker_t>::iterator it = lockers.begin();
    ASSERT_FALSE(lockers.end() == it);
    ASSERT_EQ(me, it->client);
    ASSERT_EQ("Cookie", it->cookie);
  }
}

TEST_F(LibRadosLockECPP, BreakLockPP) {
  int exclusive;
  std::string tag;
  std::list<librados::locker_t> lockers;
  std::stringstream sstm;
  sstm << "client." << cluster.get_instance_id();
  std::string me = sstm.str();
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP8", "Cookie",  "", NULL, 0));
  ASSERT_EQ(1, ioctx.list_lockers("foo", "TestLockECPP8", &exclusive, &tag, &lockers));
  std::list<librados::locker_t>::iterator it = lockers.begin();
  ASSERT_FALSE(lockers.end() == it);
  ASSERT_EQ(me, it->client);
  ASSERT_EQ("Cookie", it->cookie);
  ASSERT_EQ(0, ioctx.break_lock("foo", "TestLockECPP8", it->client, "Cookie"));
}
