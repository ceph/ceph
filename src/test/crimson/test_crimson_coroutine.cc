// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>
#include <numeric>

#include "seastar/core/sleep.hh"

#include "crimson/common/coroutine.h"
#include "crimson/common/errorator.h"
#include "crimson/common/interruptible_future.h"
#include "crimson/common/log.h"

#include "test/crimson/gtest_seastar.h"

struct coroutine_test_t : public seastar_test_suite_t {
  struct interruption_state_t {
    bool interrupted = false;
  } interruption_state;

  class test_interruption : public std::exception
  {};

  class test_interrupt_cond {
    interruption_state_t *int_state = nullptr;
  public:
    test_interrupt_cond() = delete;
    test_interrupt_cond(interruption_state_t *int_state)
      : int_state(int_state) {}

    template <typename T>
    std::optional<T> may_interrupt() {
      ceph_assert(int_state);
      if (int_state->interrupted) {
	return seastar::futurize<T>::make_exception_future(
	  test_interruption()
	);
      } else {
	return std::nullopt;
      }
    }

    template <typename T>
    static constexpr bool is_interruption_v = std::is_same_v<
      T, test_interruption>;

    static bool is_interruption(std::exception_ptr& eptr) {
      if (*eptr.__cxa_exception_type() == typeid(test_interruption))
	return true;
      return false;
    }
  };
  using interruptor = crimson::interruptible::interruptor<test_interrupt_cond>;

  using ertr = crimson::errorator<crimson::ct_error::invarg>;
  using iertr = crimson::interruptible::interruptible_errorator<
    test_interrupt_cond,
    ertr>;

  using ertr2 = ertr::extend<
    crimson::ct_error::eagain>;
  using iertr2 = crimson::interruptible::interruptible_errorator<
    test_interrupt_cond,
    ertr2>;

  using ertr3 = ertr::extend<
    crimson::ct_error::enoent>;
  using iertr3 = crimson::interruptible::interruptible_errorator<
    test_interrupt_cond,
    ertr3>;

  void interrupt() {
    interruption_state.interrupted = true;
  }

  seastar::future<> set_up_fut() final {
    interruption_state.interrupted = false;
    return seastar::now();
  }


  template <typename E, typename F>
  auto cwi(E &&errf, F &&f) {
    return interruptor::with_interruption(
      scl(std::forward<F>(f)),
      std::forward<E>(errf),
      &interruption_state);
  }
};

namespace crimson::interruptible {
template
thread_local interrupt_cond_t<coroutine_test_t::test_interrupt_cond>
interrupt_cond<coroutine_test_t::test_interrupt_cond>;
}

TEST_F(coroutine_test_t, test_coroutine)
{
  run_scl([]() -> seastar::future<> {
    constexpr int CHECK = 20;
    auto unwrapped = co_await seastar::make_ready_future<int>(CHECK);
    EXPECT_EQ(unwrapped, CHECK);
  });
}

TEST_F(coroutine_test_t, test_ertr_coroutine_basic)
{
  run_ertr_scl([]() -> ertr::future<> {
    constexpr int CHECK = 20;
    auto unwrapped = co_await ertr::make_ready_future<int>(CHECK);
    EXPECT_EQ(unwrapped, CHECK);
  });
}

TEST_F(coroutine_test_t, test_ertr_coroutine_vanilla_future)
{
  run_ertr_scl([]() -> ertr::future<> {
    constexpr int CHECK = 20;
    auto unwrapped = co_await seastar::make_ready_future<int>(CHECK);
    EXPECT_EQ(unwrapped, CHECK);
  });
}

TEST_F(coroutine_test_t, test_ertr_coroutine_error)
{
  run_scl([this]() -> seastar::future<> {
    auto fut = scl([]() -> ertr::future<int> {
      std::ignore = co_await ertr::future<int>(
	crimson::ct_error::invarg::make()
      );
      EXPECT_EQ("above co_await should throw", nullptr);
      co_return 10;
    })();
    auto ret = co_await std::move(fut).handle_error(
      [](const crimson::ct_error::invarg &e) {
	return 20;
      }
    );
    EXPECT_EQ(ret, 20);
  });
}

#if 0
// This one is left in, but commented out, as a test which *should fail to
// build* due to trying to co_await a more errorated future.
TEST_F(coroutine_test_t, test_basic_ertr_coroutine_error_should_not_build)
{
  run_ertr_scl([]() -> ertr::future<int> {
    constexpr int CHECK = 20;
    auto unwrapped = co_await ertr2::make_ready_future<int>(CHECK);
    EXPECT_EQ(unwrapped, CHECK);
    co_return 10;
  });
}
#endif

TEST_F(coroutine_test_t, interruptible_coroutine_basic)
{
  run_scl([this]() -> seastar::future<> {
    seastar::promise<int> p;
    auto ret = cwi(
      [](auto) { return 2; },
      [f=p.get_future()]() mutable -> interruptor::future<int> {
	auto x = co_await interruptor::make_interruptible(std::move(f));
	co_return x;
      });
    p.set_value(0);
    auto awaited = co_await std::move(ret);
    EXPECT_EQ(awaited, 0);
  });
}

TEST_F(coroutine_test_t, interruptible_coroutine_interrupted)
{
  run_scl([this]() -> seastar::future<> {
    seastar::promise<int> p;
    auto ret = cwi(
      [](auto) { return 2; },
      [f=p.get_future()]() mutable -> interruptor::future<int> {
	auto x = co_await interruptor::make_interruptible(std::move(f));
	co_return x;
      });
    interrupt();
    p.set_value(0);
    auto awaited = co_await std::move(ret);
    EXPECT_EQ(awaited, 2);
  });
}

TEST_F(coroutine_test_t, dual_interruptible_coroutine)
{
  run_scl([this]() -> seastar::future<> {
    seastar::promise<int> p, p2;
    auto fut1 = cwi(
      [](auto) { return 2; },
      [&p, f=p2.get_future()]() mutable -> interruptor::future<int> {
	auto x = co_await interruptor::make_interruptible(std::move(f));
	p.set_value(1);
	co_return x;
      });
    auto fut2 = cwi(
      [](auto) { return 2; },
      [&p2, f=p.get_future()]() mutable -> interruptor::future<int> {
	p2.set_value(0);
	auto x = co_await interruptor::make_interruptible(std::move(f));
	co_return x;
      });

    auto ret1 = co_await std::move(fut1);
    auto ret2 = co_await std::move(fut2);
    EXPECT_EQ(ret1, 0);
    EXPECT_EQ(ret2, 1);
  });
}

TEST_F(coroutine_test_t, dual_interruptible_coroutine_interrupted)
{
  run_scl([this]() -> seastar::future<> {
    seastar::promise<int> p, p2;
    auto fut1 = cwi(
      [](auto) { return 2; },
      [this, &p, f=p2.get_future()]() mutable -> interruptor::future<int> {
	auto x = co_await interruptor::make_interruptible(std::move(f));
	interrupt();
	p.set_value(1);
	co_return x;
      });
    auto fut2 = cwi(
      [](auto) { return 2; },
      [&p2, f=p.get_future()]() mutable -> interruptor::future<int> {
	p2.set_value(0);
	auto x = co_await interruptor::make_interruptible(std::move(f));
	co_return x;
      });

    auto ret1 = co_await std::move(fut1);
    auto ret2 = co_await std::move(fut2);
    EXPECT_EQ(ret1, 0);
    EXPECT_EQ(ret2, 2);
  });
}

TEST_F(coroutine_test_t, test_iertr_coroutine_basic)
{
  run_ertr_scl([this]() -> ertr2::future<> {
    auto ret = co_await cwi(
      [](auto) { return 10; },
      []() -> iertr::future<int> {
	co_return 20;
      });
    EXPECT_EQ(ret, 20);
  });
}

TEST_F(coroutine_test_t, test_iertr_coroutine_interruption_as_error)
{
  run_ertr_scl([this]() -> ertr2::future<> {
    auto ret = co_await cwi(
      [](auto) {
	return ertr2::future<int>(crimson::ct_error::eagain::make());
      },
      []() -> iertr::future<int> {
	co_return 20;
      });
    EXPECT_EQ(ret, 20);
  });
}

TEST_F(coroutine_test_t, test_iertr_coroutine_interruption_as_error_interrupted)
{
  run_ertr_scl([this]() -> ertr::future<> {
    seastar::promise<> p;
    auto f = cwi(
      [](auto) {
	return ertr2::future<int>(crimson::ct_error::eagain::make());
      },
      [&p]() -> iertr::future<int> {
        co_await iertr::make_interruptible(p.get_future());
	co_return 20;
      });
    interrupt();
    p.set_value();
    auto ret = co_await f.handle_error(
      crimson::ct_error::eagain::handle([](const auto &) {
	return 30;
      }),
      crimson::ct_error::pass_further_all{}
    );
    EXPECT_EQ(ret, 30);
  });
}

#if 0
// the cwi invocation below would yield an ertr2 due to the interruption handler
TEST_F(coroutine_test_t, test_iertr_coroutine_interruption_should_not_compile)
{
  run_ertr_scl([this]() -> ertr::future<> {
    auto ret = co_await cwi(
      [](auto) {
	ertr2::future<int>(crimson::ct_error::eagain::make());
      },
      []() -> iertr::future<int> {
	co_return 20;
      });
    EXPECT_EQ(ret, 20);
  });
}
#endif

#if 0
// can't co_await a vanilla future from an interruptible coroutine
TEST_F(coroutine_test_t, test_iertr_coroutine_interruption_should_not_compile2)
{
  run_ertr_scl([this]() -> ertr2::future<> {
    auto ret = co_await cwi(
      [](auto) {
	return ertr2::future<int>(crimson::ct_error::eagain::make());
      },
      []() -> iertr::future<int> {
	co_await seastar::now();
	co_return 20;
      });
    EXPECT_EQ(ret, 20);
  });
}
#endif

