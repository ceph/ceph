#ifndef TEST_LIBRBD_MANAGED_LOCK_MOCK_LOCK_WATCHER
#define TEST_LIBRBD_MANAGED_LOCK_MOCK_LOCK_WATCHER

#include "librbd/Lock.h"
#include "common/WorkQueue.h"
#include "gmock/gmock.h"

namespace librbd {
namespace managed_lock {

struct MockLockWatcher {
  static const std::string WATCHER_LOCK_TAG;
  static MockLockWatcher *s_instance;

  MockLockWatcher() {
  }

  MOCK_METHOD1(flush, void(Context *));
  MOCK_METHOD0(work_queue, ContextWQ*());

  MOCK_METHOD0(is_registered, bool());
  MOCK_METHOD0(get_watch_handle, uint64_t());

  MOCK_METHOD1(register_watch, void(Context *));
  MOCK_METHOD1(unregister_watch, void(Context *));

  MOCK_METHOD0(notify_acquired_lock, void());
  MOCK_METHOD0(notify_released_lock, void());
  MOCK_METHOD0(notify_request_lock, void());

  MOCK_METHOD0(encode_lock_cookie, std::string());
  static bool decode_lock_cookie(const std::string& tag, uint64_t *handle);
};

}
}


#endif // TEST_LIBRBD_MANAGED_LOCK_MOCK_LOCK_WATCHER
