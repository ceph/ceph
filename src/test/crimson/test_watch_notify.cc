// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/crimson/gtest_seastar.h"

// watch.h transitively provides watch_info_t and entity_name_t.
#include "crimson/osd/watch.h"

using crimson::osd::Watch;
using crimson::osd::WatchRef;

namespace {

WatchRef make_test_watch(uint64_t cookie = 1)
{
  watch_info_t winfo{cookie, /*timeout_seconds=*/30, entity_addr_t{}};
  return Watch::create_for_test(winfo, entity_name_t::CLIENT(cookie));
}

} // anonymous namespace

// Regression test for the crimson notify-timeout race.
//
// Notify::create_n_propagate() adds every watcher to Notify::watchers
// synchronously and arms the notify timeout up front, but each watcher only
// records the notify in Watch::in_progress_notifies once its asynchronous
// Watch::start_notify() runs. Because propagation is sequential and awaits a
// network send per watcher, the timeout can fire before start_notify() has
// completed for every watcher (more likely with many watchers on one object
// under load). Notify::do_notify_timeout() then iterates the full watcher set
// and calls Watch::cancel_notify() for a watcher that has not recorded the
// notify yet.
//
// Before the fix cancel_notify() assumed the entry existed: it asserted the
// iterator was valid and then erased it. On a missing entry that lookup returns
// end(), so the assert fires. The fix makes a missing entry a no-op, mirroring
// notify_ack(). This exercises that path directly -- constructing a full
// Watch/Notify/PG stack is impractical in a unit test, but cancel_notify()
// touches none of it, so the connection- and PG-agnostic test constructor is
// sufficient.
struct watch_notify_test_t : public seastar_test_suite_t {};

TEST_F(watch_notify_test_t, cancel_notify_missing_is_noop)
{
  run_async([] {
    auto watch = make_test_watch();
    // No notify was ever started on this watch, so in_progress_notifies is
    // empty. Pre-fix this aborted; post-fix it must be a clean no-op.
    watch->cancel_notify(0xdeadbeef);
    // A second call must be equally harmless.
    watch->cancel_notify(0);
    SUCCEED();
  });
}

// notify_ack() already guards the same lookup; assert the sibling invariant so
// the two stay consistent.
TEST_F(watch_notify_test_t, notify_ack_missing_is_noop)
{
  run_async([] {
    auto watch = make_test_watch();
    ceph::bufferlist reply;
    watch->notify_ack(0xdeadbeef, reply).get();
    SUCCEED();
  });
}
