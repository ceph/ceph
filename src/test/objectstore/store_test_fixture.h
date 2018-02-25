#include <string>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

class ObjectStore;

class StoreTestFixture : virtual public ::testing::Test {
  const std::string type;
  const std::string data_dir;

public:
  boost::scoped_ptr<ObjectStore> store;
  ObjectStore::CollectionHandle ch;

  StoreTestFixture(const std::string& type)
    : type(type), data_dir(type + ".test_temp_dir")
  {}
  void SetUp() override;
  void TearDown() override;
};
