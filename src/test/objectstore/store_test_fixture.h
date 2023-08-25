#include <string>
#include <stack>
#include <memory>
#include <gtest/gtest.h>
#include "common/config_fwd.h"

class ObjectStore;

class StoreTestFixture : virtual public ::testing::Test {
  const std::string type;
  const std::string data_dir;

  std::stack<std::pair<std::string, std::string>> saved_settings;
  ConfigProxy* conf = nullptr;

public:
  std::unique_ptr<ObjectStore> store;
  ObjectStore::CollectionHandle ch;

  explicit StoreTestFixture(const std::string& type)
    : type(type), data_dir(type + ".test_temp_dir")
  {}

  void SetUp() override;
  void TearDown() override;

  void SetVal(ConfigProxy& conf, const char* key, const char* val);
  struct SettingsBookmark {
    StoreTestFixture& s;
    size_t pos;

    SettingsBookmark(StoreTestFixture& _s, size_t p) : s(_s), pos(p)
    {}

    ~SettingsBookmark() {
      s.PopSettings(pos);
    }
  };
  SettingsBookmark BookmarkSettings() {
    return SettingsBookmark(*this, saved_settings.size());
  }
  void PopSettings(size_t);
  void CloseAndReopen();
};
