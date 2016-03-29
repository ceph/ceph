#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/radosstriper/libradosstriper.h"
#include "include/radosstriper/libradosstriper.hpp"
#include "test/librados/test.h"
#include "test/libradosstriper/TestCase.h"

#include <fcntl.h>
#include <semaphore.h>
#include <errno.h>

using namespace librados;
using namespace libradosstriper;
using std::pair;

class AioTestData
{
public:
  AioTestData() : m_complete(false), m_safe(false) {
    m_sem = sem_open("test_libradosstriper_aio_sem", O_CREAT, 0644, 0);
  }

  ~AioTestData() {
    sem_close(m_sem);
  }

  sem_t *m_sem;
  bool m_complete;
  bool m_safe;
};

void set_completion_complete(rados_completion_t cb, void *arg)
{
  AioTestData *test = static_cast<AioTestData*>(arg);
  test->m_complete = true;
  sem_post(test->m_sem);
}

void set_completion_safe(rados_completion_t cb, void *arg)
{
  AioTestData *test = static_cast<AioTestData*>(arg);
  test->m_safe = true;
  sem_post(test->m_sem);
}

TEST_F(StriperTest, SimpleWrite) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "StriperTest", my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
}

TEST_F(StriperTestPP, SimpleWritePP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("SimpleWritePP", my_completion, bl1, sizeof(buf), 0));
  TestAlarm alarm;
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
}

TEST_F(StriperTest, WaitForSafe) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "WaitForSafe", my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  rados_aio_wait_for_safe(my_completion);
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
}

TEST_F(StriperTestPP, WaitForSafePP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("WaitForSafePP", my_completion, bl1, sizeof(buf), 0));
  TestAlarm alarm;
  my_completion->wait_for_safe();
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
}

TEST_F(StriperTest, RoundTrip) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "RoundTrip", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTrip", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTest, RoundTrip2) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "RoundTrip2", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTrip2", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_safe(my_completion2);
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTestPP, RoundTripPP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("RoundTripPP", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  bufferlist bl2;
  AioCompletion *my_completion2 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_read("RoundTripPP", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTestPP, RoundTripPP2) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("RoundTripPP2", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  bufferlist bl2;
  AioCompletion *my_completion2 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_read("RoundTripPP2", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_safe();
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTest, IsComplete) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "IsComplete", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "IsComplete", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_complete.
    while (true) {
      int is_complete = rados_aio_is_complete(my_completion2);
      if (is_complete)
	break;
    }
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTestPP, IsCompletePP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("IsCompletePP", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  bufferlist bl2;
  AioCompletion *my_completion2 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_read("IsCompletePP", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_complete.
    while (true) {
      int is_complete = my_completion2->is_complete();
      if (is_complete)
	break;
    }
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTest, IsSafe) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "IsSafe", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_safe.
    while (true) {
      int is_safe = rados_aio_is_safe(my_completion);
      if (is_safe)
	break;
    }
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "IsSafe", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTestPP, IsSafePP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("IsSafePP", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_safe.
    while (true) {
      int is_safe = my_completion->is_safe();
      if (is_safe)
	break;
    }
  }
  bufferlist bl2;
  AioCompletion *my_completion2 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_read("IsSafePP", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTest, RoundTripAppend) {
  AioTestData test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_append(striper, "RoundTripAppend", my_completion, buf, sizeof(buf)));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion);
  }
  char buf2[128];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_append(striper, "RoundTripAppend", my_completion2, buf2, sizeof(buf)));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion3));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTripAppend", my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion3);
  }
  ASSERT_EQ((int)(sizeof(buf) + sizeof(buf2)), rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf), buf2, sizeof(buf2)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST_F(StriperTestPP, RoundTripAppendPP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_append("RoundTripAppendPP", my_completion, bl1, sizeof(buf)));
  {
    TestAlarm alarm;
    my_completion->wait_for_complete();
  }
  char buf2[128];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_append("RoundTripAppendPP", my_completion2, bl2, sizeof(buf2)));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  bufferlist bl3;
  AioCompletion *my_completion3 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_read("RoundTripAppendPP", my_completion3, &bl3, 2 * sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion3->wait_for_complete();
  }
  ASSERT_EQ(sizeof(buf) + sizeof(buf2), (unsigned)my_completion3->get_return_value());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(bl3.c_str() + sizeof(buf), buf2, sizeof(buf2)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
  my_completion2->release();
  my_completion3->release();
}

TEST_F(StriperTest, Flush) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "Flush", my_completion, buf, sizeof(buf), 0));
  rados_striper_aio_flush(striper);
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "Flush", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTestPP, FlushPP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("FlushPP", my_completion, bl1, sizeof(buf), 0));
  striper.aio_flush();
  bufferlist bl2;
  AioCompletion *my_completion2 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_read("FlushPP", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTest, RoundTripWriteFull) {
  AioTestData test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "RoundTripWriteFull", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion);
  }
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_write_full(striper, "RoundTripWriteFull", my_completion2, buf2, sizeof(buf2)));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion
            ((void*)&test_data, set_completion_complete, set_completion_safe, &my_completion3));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTripWriteFull", my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion3);
  }
  ASSERT_EQ(sizeof(buf2), (unsigned)rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST_F(StriperTestPP, RoundTripWriteFullPP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("RoundTripWriteFullPP", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion->wait_for_complete();
  }
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_write_full("RoundTripWriteFullPP", my_completion2, bl2));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  bufferlist bl3;
  AioCompletion *my_completion3 = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, striper.aio_read("RoundTripWriteFullPP", my_completion3, &bl3, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion3->wait_for_complete();
  }
  ASSERT_EQ(sizeof(buf2), (unsigned)my_completion3->get_return_value());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  sem_wait(test_data.m_sem);
  sem_wait(test_data.m_sem);
  my_completion->release();
  my_completion2->release();
  my_completion3->release();
}
