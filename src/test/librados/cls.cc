#include <errno.h>
#include <map>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"

using namespace librados;
using std::map;
using std::ostringstream;
using std::string;

namespace cls::bogus_classes {
struct ClassId {
  static constexpr auto name = "doesnotexistasdfasdf";
};
struct ClassIdLock { // This is a real class. We are adding a bogus method.
  static constexpr auto name = "doesnotexistasdfasdf";
};
namespace method {
constexpr auto bogus_class = ClsMethod<RdTag, ClassId>("method");
constexpr auto bogus_method = ClsMethod<RdTag, ClassIdLock>("doesnotexistasdfasdfasdf");
}
}

using namespace cls::bogus_classes;

// Ignore use of legacy interface, which allows specifying cls methods which do
// not exist.
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
  ASSERT_EQ(-EOPNOTSUPP, ioctx.exec(oid, method::bogus_class, bl, bl));

  // call a bogus method on existent class
  ASSERT_EQ(-EOPNOTSUPP, ioctx.exec(oid, method::bogus_method, bl, bl));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}