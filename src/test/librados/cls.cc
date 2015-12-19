#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using std::map;
using std::ostringstream;
using std::string;

TEST(LibRadosCls, DNE) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // create an object
  string oid = "foo";
  bufferlist bl;
  ASSERT_EQ(0, ioctx.write(oid, bl, bl.length(), 0));

  // call a bogus class
  ASSERT_EQ(-EOPNOTSUPP, ioctx.exec(oid, "doesnotexistasdfasdf", "method", bl, bl));

  // call a bogus method on existent class
  ASSERT_EQ(-EOPNOTSUPP, ioctx.exec(oid, "lock", "doesnotexistasdfasdfasdf", bl, bl));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
