// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"
#include "journal/Journaler.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/api/Io.h"
#include "librbd/api/Snapshot.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ReadResult.h"
#include "librbd/journal/Types.h"

void register_test_journal_stress() {
}

namespace librbd {
namespace journal {

class TestJournalStress : public TestFixture {
};

TEST_F(TestJournalStress, DiscardWithPruneWriteOverlap) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  // Overlap discards and writes while discard pruning is occurring. This tests
  // the conditions under which https://tracker.ceph.com/issues/63422 occurred.

  // Create an image that is multiple objects so that we can force multiple
  // image extents on the discard path.
  CephContext* cct = reinterpret_cast<CephContext*>(_rados.cct());
  auto object_size = 1ull << cct->_conf.get_val<uint64_t>("rbd_default_order");
  auto image_size = 4 * object_size;

  // Write-around cache required for overlapping I/O delays.
  cct->_conf.set_val_or_die("rbd_cache_writethrough_until_flush", "false");
  cct->_conf.set_val_or_die("rbd_cache_policy", "writearound");
  // XXX: Work around https://tracker.ceph.com/issues/63681, which this test
  // exposes when run under Valgrind.
  cct->_conf.set_val_or_die("librados_thread_count", "15");
  cct->_conf.apply_changes(nullptr);

  auto image_name = get_temp_image_name();
  ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, image_name, image_size));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(image_name, &ictx));

  std::thread write_thread(
    [ictx, object_size]() {
      std::string payload(object_size, '1');

      for (auto i = 0; i < 200; i++) {
        // Alternate overlaps with the two objects that the discard below
        // touches.
        for (auto offset = object_size;
             offset < object_size * 3;
             offset += object_size) {
          bufferlist payload_bl;
          payload_bl.append(payload);
          auto aio_comp = new librbd::io::AioCompletion();
          api::Io<>::aio_write(*ictx, aio_comp, 0, payload.size(),
                               std::move(payload_bl), 0, true);
          ASSERT_EQ(0, aio_comp->wait_for_complete());
          aio_comp->release();
        }
      }
    }
  );

  auto discard_exit = false;
  std::thread discard_thread(
    [ictx, object_size, &discard_exit]() {
      while (!discard_exit) {
        // We offset the discard by -4096 bytes and set discard granularity to
        // 8192; this should cause two image extents to be formed in
        // AbstractImageWriteRequest<I>::send_request() on objects 1 and 2,
        // overlapping with the writes above.
        auto aio_comp = new librbd::io::AioCompletion();
        api::Io<>::aio_discard(*ictx, aio_comp, object_size - 4096,
                               2 * object_size, 8192, true);
        ASSERT_EQ(0, aio_comp->wait_for_complete());
        aio_comp->release();
      }
    }
  );

  write_thread.join();
  discard_exit = true;
  discard_thread.join();
}

} // namespace journal
} // namespace librbd
