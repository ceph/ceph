// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/sleep.hh>

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/interruptible_future.h"
#include "crimson/common/log.h"

using namespace crimson;

class test_interruption : public std::exception
{};

class TestInterruptCondition {
public:
  TestInterruptCondition(bool interrupt)
    : interrupt(interrupt) {}

  template <typename T>
  std::pair<bool, std::optional<T>> may_interrupt() {
    if (interrupt)
      return std::pair<bool, std::optional<T>>(
	  true, seastar::futurize<T>::make_exception_future(test_interruption()));
    else
      return std::pair<bool, std::optional<T>>(false, std::optional<T>());
  }

  template <typename T>
  static constexpr bool is_interruption_v = std::is_same_v<T, test_interruption>;

  static bool is_interruption(std::exception_ptr& eptr) {
    if (*eptr.__cxa_exception_type() == typeid(test_interruption))
      return true;
    return false;
  }
private:
  bool interrupt = false;
};

TEST_F(seastar_test_suite_t, basic)
{
  using interruptor =
    interruptible::interruptor<TestInterruptCondition>;
  run_async([] {
    interruptor::with_interruption(
      [] {
	ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	return interruptor::make_interruptible(seastar::now())
	.then_interruptible([] {
	  ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	}).then_interruptible([] {
	  ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	  return errorator<ct_error::enoent>::make_ready_future<>();
	}).safe_then_interruptible([] {
	  ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	  return seastar::now();
	}, errorator<ct_error::enoent>::all_same_way([] {
	  ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	  })
	);
      }, [](std::exception_ptr) {}, false).get0();

    interruptor::with_interruption(
      [] {
	ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	return interruptor::make_interruptible(seastar::now())
	.then_interruptible([] {
	  ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	});
      }, [](std::exception_ptr) {
	ceph_assert(!interruptible::interrupt_cond<TestInterruptCondition>);
	return seastar::now();
      }, true).get0();


  });
}

TEST_F(seastar_test_suite_t, loops)
{
  using interruptor =
    interruptible::interruptor<TestInterruptCondition>;
  std::cout << "testing interruptible loops" << std::endl;
  run_async([] {
    std::cout << "beginning" << std::endl;
    interruptor::with_interruption(
      [] {
	std::cout << "interruptiion enabled" << std::endl;
	ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	return interruptor::make_interruptible(seastar::now())
	.then_interruptible([] {
	  std::cout << "test seastar future do_for_each" << std::endl;
	  std::vector<int> vec = {1, 2};
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
	      ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	      return seastar::now();
	    });
	  });
	}).then_interruptible([] {
	  std::cout << "test interruptible seastar future do_for_each" << std::endl;
	  std::vector<int> vec = {1, 2};
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
	      ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	      return interruptor::make_interruptible(seastar::now());
	    });
	  });
	}).then_interruptible([] {
	  std::cout << "test seastar future repeat" << std::endl;
	  return interruptor::repeat([] {
	    ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	    return interruptor::make_interruptible(
		seastar::make_ready_future<
		  seastar::stop_iteration>(
		    seastar::stop_iteration::yes));
	  });
	}).then_interruptible([] {
	  std::cout << "test interruptible seastar future repeat" << std::endl;
	  return interruptor::repeat([] {
	    ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	    return seastar::make_ready_future<
		    seastar::stop_iteration>(
		      seastar::stop_iteration::yes);
	  });
	}).then_interruptible([] {
	  std::cout << "test interruptible errorated future do_for_each" << std::endl;
	  std::vector<int> vec = {1, 2};
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    using namespace std::chrono_literals;
	    return interruptor::make_interruptible(seastar::now()).then_interruptible([&vec] {
	      return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
		return interruptor::make_interruptible(
		  errorator<ct_error::enoent>::make_ready_future<>());
	      }).safe_then_interruptible([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
		return seastar::now();
	      }, errorator<ct_error::enoent>::all_same_way([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	      }));
	    });
	  });
	}).then_interruptible([] {
	  std::cout << "test errorated future do_for_each" << std::endl;
	  std::vector<int> vec = {1, 2};
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    using namespace std::chrono_literals;
	    return interruptor::make_interruptible(seastar::now()).then_interruptible([&vec] {
	      return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
		return errorator<ct_error::enoent>::make_ready_future<>();
	      }).safe_then_interruptible([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
		return seastar::now();
	      }, errorator<ct_error::enoent>::all_same_way([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	      }));
	    });
	  });
	}).then_interruptible([] {
	  ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>);
	  return seastar::now();
	});
      }, [](std::exception_ptr) {}, false).get0();
  });
}

using base_intr = interruptible::interruptor<TestInterruptCondition>;

using base_ertr = errorator<ct_error::enoent, ct_error::eagain>;
using base_iertr = interruptible::interruptible_errorator<
  TestInterruptCondition,
  base_ertr>;

using base2_ertr = base_ertr::extend<ct_error::input_output_error>;
using base2_iertr = interruptible::interruptible_errorator<
  TestInterruptCondition,
  base2_ertr>;

template <typename F>
auto with_intr(F &&f) {
  return base_intr::with_interruption_to_error<ct_error::eagain>(
    std::forward<F>(f),
    TestInterruptCondition(false));
}

TEST_F(seastar_test_suite_t, errorated)
{
  run_async([] {
    base_ertr::future<> ret = with_intr(
      []() {
	return base_iertr::now();
      }
    );
    ret.unsafe_get0();
  });
}

TEST_F(seastar_test_suite_t, errorated_value)
{
  run_async([] {
    base_ertr::future<int> ret = with_intr(
      []() {
	return base_iertr::make_ready_future<int>(
	  1
	);
      });
    EXPECT_EQ(ret.unsafe_get0(), 1);
  });
}

TEST_F(seastar_test_suite_t, expand_errorated_value)
{
  run_async([] {
    base2_ertr::future<> ret = with_intr(
      []() {
	return base_iertr::make_ready_future<int>(
	  1
	).si_then([](auto) {
	  return base2_iertr::make_ready_future<>();
	});
      });
    ret.unsafe_get0();
  });
}

#if 0
// This seems to cause a hang in the gcc-9 linker on bionic
TEST_F(seastar_test_suite_t, handle_error)
{
  run_async([] {
    base_ertr::future<> ret = with_intr(
      []() {
	return base2_iertr::make_ready_future<int>(
	  1
	).handle_error_interruptible(
	  base_iertr::pass_further{},
	  ct_error::assert_all{"crash on eio"}
	).si_then([](auto) {
	  return base_iertr::now();
	});
      });
    ret.unsafe_get0();
  });
}
#endif
