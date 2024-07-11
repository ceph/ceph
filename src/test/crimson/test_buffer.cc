#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include "include/buffer.h"
#include "test/crimson/ctest_utils.h"

// allocate a foreign buffer on each cpu, collect them all into a bufferlist,
// and destruct it on this cpu
seastar::future<> test_foreign_bufferlist()
{
  auto make_foreign_buffer = [] (unsigned cpu) {
    return seastar::smp::submit_to(cpu, [=] {
      bufferlist bl;
      seastar::temporary_buffer<char> buf("abcd", 4);
      bl.append(buffer::create(std::move(buf)));
      return bl;
    });
  };
  auto reduce = [] (bufferlist&& lhs, bufferlist&& rhs) {
    bufferlist bl;
    bl.claim_append(lhs);
    bl.claim_append(rhs);
    return bl;
  };
  return seastar::map_reduce(seastar::smp::all_cpus(), make_foreign_buffer,
                             bufferlist(), reduce).then(
    [] (bufferlist&& bl) {
      if (bl.length() != 4 * seastar::smp::count) {
        auto e = std::make_exception_ptr(std::runtime_error("wrong buffer size"));
        return seastar::make_exception_future<>(e);
      }
      bl.clear();
      return seastar::make_ready_future<>();
    });
}

int main(int argc, char** argv)
{
  seastar::app_template app{get_smp_opts_from_ctest()};
  return app.run(argc, argv, [] {
    return seastar::now().then(
      &test_foreign_bufferlist
    ).then([] {
      std::cout << "All tests succeeded" << std::endl;
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
