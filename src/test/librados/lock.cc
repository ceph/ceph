#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "cls/lock/cls_lock_client.h"

#include <algorithm>
#include <chrono>
#include <thread>
#include <errno.h>
#include "gtest/gtest.h"
#include <sys/time.h>

using namespace std::chrono_literals;
using namespace librados;

typedef RadosTest LibRadosLock;
typedef RadosTestPP LibRadosLockPP;
typedef RadosTestEC LibRadosLockEC;
typedef RadosTestECPP LibRadosLockECPP;


template<class Rep, class Period, typename Func, typename... Args,
         typename Return = std::result_of_t<Func&&(Args&&...)>>
Return wait_until(const std::chrono::duration<Rep, Period>& rel_time,
                const std::chrono::duration<Rep, Period>& step,
                const Return& expected,
                Func&& func, Args&&... args)
{
  std::this_thread::sleep_for(rel_time - step);
  for (auto& s : {step, step}) {
    if (!s.count()) {
      break;
    }
    auto ret = func(std::forward<Args>(args)...);
    if (ret == expected) {
      return ret;
    }
    std::this_thread::sleep_for(s);
  }
  return func(std::forward<Args>(args)...);
}

TEST_F(LibRadosLock, LockExclusive) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLock1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockPP, LockExclusivePP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockPP1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLock, LockShared) {
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLock2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_shared(ioctx, "foo", "TestLock2", "Cookie", "Tag", "", NULL, 0));
}

TEST_F(LibRadosLockPP, LockSharedPP) {
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockPP2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_shared("foo", "TestLockPP2", "Cookie", "Tag", "", NULL, 0));
}

TEST_F(LibRadosLock, LockExclusiveDur) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_exclusive = [this](timeval* tv) {
    return rados_lock_exclusive(ioctx, "foo", "TestLock3", "Cookie", "", tv, 0);
  };
  constexpr int expected = 0;
  ASSERT_EQ(expected, lock_exclusive(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_exclusive, nullptr));
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

TEST_F(LibRadosLock, LockSharedDur) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_shared = [this](timeval* tv) {
    return rados_lock_shared(ioctx, "foo", "TestLock4", "Cookie", "Tag", "", tv, 0);
  };
  constexpr int expected = 0;
  ASSERT_EQ(expected, lock_shared(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_shared, nullptr));
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

TEST_F(LibRadosLock, LockRenew) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLock5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock5", "Cookie", "", NULL, LOCK_FLAG_RENEW));
}

TEST_F(LibRadosLockPP, LockRenewPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP5", "Cookie", "", NULL, LOCK_FLAG_RENEW));
}

TEST_F(LibRadosLock, Unlock) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_unlock(ioctx, "foo", "TestLock6", "Cookie"));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock6", "Cookie", "", NULL,  0));
}

TEST_F(LibRadosLockPP, UnlockPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.unlock("foo", "TestLockPP6", "Cookie"));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockPP6", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLock, ListLockers) {
  int exclusive;
  char tag[1024];
  char clients[1024];
  char cookies[1024];
  char addresses[1024];
  size_t tag_len = 1024;
  size_t clients_len = 1024;
  size_t cookies_len = 1024;
  size_t addresses_len = 1024;
  std::stringstream sstm;
  sstm << "client." << rados_get_instance_id(cluster);
  std::string me = sstm.str();
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLock7", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(0, rados_unlock(ioctx, "foo", "TestLock7", "Cookie"));
  ASSERT_EQ(0, rados_list_lockers(ioctx, "foo", "TestLock7", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLock7", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-34, rados_list_lockers(ioctx, "foo", "TestLock7", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  tag_len = 1024;
  clients_len = 1024;
  cookies_len = 1024;
  addresses_len = 1024;
  ASSERT_EQ(1, rados_list_lockers(ioctx, "foo", "TestLock7", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  ASSERT_EQ(0, exclusive);
  ASSERT_EQ(0, strcmp(tag, "Tag"));
  ASSERT_EQ(strlen("Tag") + 1, tag_len);
  ASSERT_EQ(0, strcmp(me.c_str(), clients));
  ASSERT_EQ(me.size() + 1, clients_len);
  ASSERT_EQ(0, strcmp(cookies, "Cookie"));
  ASSERT_EQ(strlen("Cookie") + 1, cookies_len);
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

TEST_F(LibRadosLock, BreakLock) {
  int exclusive;
  char tag[1024];
  char clients[1024];
  char cookies[1024];
  char addresses[1024];
  size_t tag_len = 1024;
  size_t clients_len = 1024;
  size_t cookies_len = 1024;
  size_t addresses_len = 1024;
  std::stringstream sstm;
  sstm << "client." << rados_get_instance_id(cluster);
  std::string me = sstm.str();
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock8", "Cookie", "", NULL, 0));
  ASSERT_EQ(1, rados_list_lockers(ioctx, "foo", "TestLock8", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  ASSERT_EQ(1, exclusive);
  ASSERT_EQ(0, strcmp(tag, ""));
  ASSERT_EQ(1U, tag_len);
  ASSERT_EQ(0, strcmp(me.c_str(), clients));
  ASSERT_EQ(me.size() + 1, clients_len);
  ASSERT_EQ(0, strcmp(cookies, "Cookie"));
  ASSERT_EQ(strlen("Cookie") + 1, cookies_len);
  ASSERT_EQ(0, rados_break_lock(ioctx, "foo", "TestLock8", clients, "Cookie"));
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
TEST_F(LibRadosLockEC, LockExclusive) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLockEC1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockECPP, LockExclusivePP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockECPP1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockEC, LockShared) {
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLockEC2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_shared(ioctx, "foo", "TestLockEC2", "Cookie", "Tag", "", NULL, 0));
}

TEST_F(LibRadosLockECPP, LockSharedPP) {
  ASSERT_EQ(0, ioctx.lock_shared("foo", "TestLockECPP2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_shared("foo", "TestLockECPP2", "Cookie", "Tag", "", NULL, 0));
}

TEST_F(LibRadosLockEC, LockExclusiveDur) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_exclusive = [this](timeval* tv) {
    return rados_lock_exclusive(ioctx, "foo", "TestLockEC3", "Cookie", "", tv, 0);
  };
  constexpr int expected = 0;
  ASSERT_EQ(expected, lock_exclusive(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_exclusive, nullptr));
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

TEST_F(LibRadosLockEC, LockSharedDur) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  auto lock_shared = [this](timeval* tv) {
    return rados_lock_shared(ioctx, "foo", "TestLockEC4", "Cookie", "Tag", "", tv, 0);
  };
  constexpr int expected = 0;
  ASSERT_EQ(expected, lock_shared(&tv));
  ASSERT_EQ(expected, wait_until(1.0s, 0.1s, expected, lock_shared, nullptr));
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

TEST_F(LibRadosLockEC, LockRenew) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLockEC5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC5", "Cookie", "", NULL, LOCK_FLAG_RENEW));
}

TEST_F(LibRadosLockECPP, LockRenewPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, ioctx.lock_exclusive("foo", "TestLockECPP5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP5", "Cookie", "", NULL, LOCK_FLAG_RENEW));
}

TEST_F(LibRadosLockEC, Unlock) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_unlock(ioctx, "foo", "TestLockEC6", "Cookie"));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC6", "Cookie", "", NULL,  0));
}

TEST_F(LibRadosLockECPP, UnlockPP) {
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, ioctx.unlock("foo", "TestLockECPP6", "Cookie"));
  ASSERT_EQ(0, ioctx.lock_exclusive("foo", "TestLockECPP6", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockEC, ListLockers) {
  int exclusive;
  char tag[1024];
  char clients[1024];
  char cookies[1024];
  char addresses[1024];
  size_t tag_len = 1024;
  size_t clients_len = 1024;
  size_t cookies_len = 1024;
  size_t addresses_len = 1024;
  std::stringstream sstm;
  sstm << "client." << rados_get_instance_id(cluster);
  std::string me = sstm.str();
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLockEC7", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(0, rados_unlock(ioctx, "foo", "TestLockEC7", "Cookie"));
  ASSERT_EQ(0, rados_list_lockers(ioctx, "foo", "TestLockEC7", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLockEC7", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-34, rados_list_lockers(ioctx, "foo", "TestLockEC7", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  tag_len = 1024;
  clients_len = 1024;
  cookies_len = 1024;
  addresses_len = 1024;
  ASSERT_EQ(1, rados_list_lockers(ioctx, "foo", "TestLockEC7", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  ASSERT_EQ(0, exclusive);
  ASSERT_EQ(0, strcmp(tag, "Tag"));
  ASSERT_EQ(strlen("Tag") + 1, tag_len);
  ASSERT_EQ(0, strcmp(me.c_str(), clients));
  ASSERT_EQ(me.size() + 1, clients_len);
  ASSERT_EQ(0, strcmp(cookies, "Cookie"));
  ASSERT_EQ(strlen("Cookie") + 1, cookies_len);
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

TEST_F(LibRadosLockEC, BreakLock) {
  int exclusive;
  char tag[1024];
  char clients[1024];
  char cookies[1024];
  char addresses[1024];
  size_t tag_len = 1024;
  size_t clients_len = 1024;
  size_t cookies_len = 1024;
  size_t addresses_len = 1024;
  std::stringstream sstm;
  sstm << "client." << rados_get_instance_id(cluster);
  std::string me = sstm.str();
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC8", "Cookie", "", NULL, 0));
  ASSERT_EQ(1, rados_list_lockers(ioctx, "foo", "TestLockEC8", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len ));
  ASSERT_EQ(1, exclusive);
  ASSERT_EQ(0, strcmp(tag, ""));
  ASSERT_EQ(1U, tag_len);
  ASSERT_EQ(0, strcmp(me.c_str(), clients));
  ASSERT_EQ(me.size() + 1, clients_len);
  ASSERT_EQ(0, strcmp(cookies, "Cookie"));
  ASSERT_EQ(strlen("Cookie") + 1, cookies_len);
  ASSERT_EQ(0, rados_break_lock(ioctx, "foo", "TestLockEC8", clients, "Cookie"));
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
