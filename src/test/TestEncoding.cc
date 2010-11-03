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

typedef std::multimap < int, std::string > multimap_t;
typedef multimap_t::value_type val_ty;

static std::ostream& operator<<(std::ostream& oss, const multimap_t &multimap)
{
  for (multimap_t::const_iterator m = multimap.begin();
       m != multimap.end();
       ++m)
  {
    oss << m->first << "->" << m->second << " ";
  }
  return oss;
}

template < typename T >
static int test_encode_and_decode(const T& src)
{
  bufferlist bl(1000000);

  try {
    // source
    encode(src, bl);

    // dest
    T dst;
    bufferlist::iterator i(bl.begin());
    decode(dst, i);
    if (src != dst) {
      cout << "src = " << src << ", but dst = " << dst << std::endl;
      return 1;
    }
//    else {
//      cout << "src = " << src << ", and dst = " << dst << std::endl;
//    }
  }
  catch (const ceph::buffer::malformed_input &e) {
    cout << "got exception " << e.what() << std::endl;
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

static int test_string_sz(const char *str)
{
  string my_str(str);
  return test_encode_and_decode < std::string >(my_str);
}

static int multimap_tests(void)
{
  multimap_t multimap;
  multimap.insert( val_ty(1, "foo") );
  multimap.insert( val_ty(2, "bar") );
  multimap.insert( val_ty(2, "baz") );
  multimap.insert( val_ty(3, "lucky number 3") );
  multimap.insert( val_ty(10000, "large number") );

  return test_encode_and_decode < multimap_t >(multimap);
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

  ret = multimap_tests();
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
