#include "include/rados/librados.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "cls/lock/cls_lock_client.h"

#include <algorithm>
#include <chrono>
#include <thread>
#include <errno.h>
#include "gtest/gtest.h"
#include <sys/time.h>

#include "crimson_utils.h"

using namespace std::chrono_literals;

typedef RadosTest LibRadosLock;
typedef RadosTestEC LibRadosLockEC;


TEST_F(LibRadosLock, LockExclusive) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLock1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLock, LockShared) {
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLock2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_shared(ioctx, "foo", "TestLock2", "Cookie", "Tag", "", NULL, 0));
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


TEST_F(LibRadosLock, LockMayRenew) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLock5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock5", "Cookie", "", NULL, LOCK_FLAG_MAY_RENEW));
}

TEST_F(LibRadosLock, Unlock) {
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_unlock(ioctx, "foo", "TestLock6", "Cookie"));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLock6", "Cookie", "", NULL,  0));
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

// EC testing
TEST_F(LibRadosLockEC, LockExclusive) {
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC1", "Cookie", "", NULL,  0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLockEC1", "Cookie", "", NULL, 0));
}

TEST_F(LibRadosLockEC, LockShared) {
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, rados_lock_shared(ioctx, "foo", "TestLockEC2", "Cookie", "Tag", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_shared(ioctx, "foo", "TestLockEC2", "Cookie", "Tag", "", NULL, 0));
}

TEST_F(LibRadosLockEC, LockExclusiveDur) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosLockEC, LockSharedDur) {
  SKIP_IF_CRIMSON();
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


TEST_F(LibRadosLockEC, LockMayRenew) {
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC5", "Cookie", "", NULL, 0));
  ASSERT_EQ(-EEXIST, rados_lock_exclusive(ioctx, "foo", "TestLockEC5", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC5", "Cookie", "", NULL, LOCK_FLAG_MAY_RENEW));
}

TEST_F(LibRadosLockEC, Unlock) {
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC6", "Cookie", "", NULL, 0));
  ASSERT_EQ(0, rados_unlock(ioctx, "foo", "TestLockEC6", "Cookie"));
  ASSERT_EQ(0, rados_lock_exclusive(ioctx, "foo", "TestLockEC6", "Cookie", "", NULL,  0));
}

TEST_F(LibRadosLockEC, ListLockers) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosLockEC, BreakLock) {
  SKIP_IF_CRIMSON();
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

