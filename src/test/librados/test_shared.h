// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*
// vim: ts=8 sw=2 smarttab

#pragma once

#include <unistd.h>
#include <chrono>
#include <map>
#include <string>
#include <thread>

#include "include/buffer_fwd.h"

// helpers shared by librados tests
std::string get_temp_pool_name(const std::string &prefix = "test-rados-api-");
void assert_eq_sparse(ceph::bufferlist& expected,
                      const std::map<uint64_t, uint64_t>& extents,
                      ceph::bufferlist& actual);
class TestAlarm
{
public:
  TestAlarm() {
    alarm(1200);
  }
  ~TestAlarm() {
    alarm(0);
  }
};

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
