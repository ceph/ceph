#include "config.h"
#include "include/encoding.h"
#include "common/common_init.h"

#include <iostream>
#include <stdlib.h>
#include <vector>

using std::cout;
using std::cerr;
using std::string;
using std::vector;

// Test string serialization
static int test_string_sz(const char *str)
{
  bufferlist bl(100000);

  try {
    // source
    string src(str);
    encode(src, bl);

    // dest
    string dst;
    bufferlist::iterator i(bl.begin());
    decode(dst, i);
    if (src != dst) {
      cout << "src = " << src << ", but dst = " << dst << std::endl;
      return 1;
    }
  }
  catch (const ceph::buffer::malformed_input &i) {
    cout << "got exception " << i.what() << std::endl;
    return 1;
  }
  catch (const std::exception &e) {
    cout << "got exception " << e.what() << std::endl;
    return 1;
  }
  catch (...) {
    cout << "unknown exception!" << std::endl;
    return 1;
  }
  return 0;
}

int main(int argc, const char **argv)
{
  int ret = 0;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  ceph_set_default_id("admin");
  common_set_defaults(false);
  common_init(args, "ceph", true);

  ret = test_string_sz("I am the very model of a modern major general");
  if (ret)
    goto done;

  ret = test_string_sz("");
  if (ret)
    goto done;

  ret = test_string_sz("foo bar baz\n");
  if (ret)
    goto done;

done:
  if (ret) {
    cout << "FAILURE" << std::endl;
    return(EXIT_FAILURE);
  }
  cout << "SUCCESS" << std::endl;
  return(EXIT_SUCCESS);
}
