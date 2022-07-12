#include <cassert>
#include <boost/asio.hpp>
#include <boost/system/system_error.hpp>

constexpr int max_completions = 10'000'000;
int completed = 0;

boost::asio::io_context c;

void nested_cb() {
  if (++completed < max_completions)
    c.post(&nested_cb);
}

int main(void) {
  c.post(&nested_cb);
  c.run();
  assert(completed == max_completions);
  return 0;
}
