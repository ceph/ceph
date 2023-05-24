#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/radosstriper/libradosstriper.h"
#include "include/radosstriper/libradosstriper.hpp"
#include "test/librados/test.h"
#include "test/libradosstriper/TestCase.h"

#include <boost/scoped_ptr.hpp>
#include <fcntl.h>
#include <semaphore.h>
#include <errno.h>

using namespace librados;
using namespace libradosstriper;
using std::pair;

class AioTestData
{
public:
  AioTestData() : m_complete(false) {
    sem_init(&m_sem, 0, 0);
  }

  ~AioTestData() {
    sem_destroy(&m_sem);
  }

  void notify() {
    sem_post(&m_sem);
  }

  void wait() {
    sem_wait(&m_sem);
  }

  bool m_complete;

private:
  sem_t m_sem;
};

void set_completion_complete(rados_completion_t cb, void *arg)
{
  AioTestData *test = static_cast<AioTestData*>(arg);
  test->m_complete = true;
  test->notify();
}

TEST_F(StriperTest, SimpleWrite) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "StriperTest", my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  test_data.wait();
  rados_aio_release(my_completion);
}

TEST_F(StriperTestPP, SimpleWritePP) {
  AioTestData test_data;
  AioCompletion *my_completion = librados::Rados::aio_create_completion
    ((void*)&test_data, set_completion_complete);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("SimpleWritePP", my_completion, bl1, sizeof(buf), 0));
  TestAlarm alarm;
  test_data.wait();
  my_completion->release();
}

TEST_F(StriperTest, WaitForSafe) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "WaitForSafe", my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  rados_aio_wait_for_complete(my_completion);
  test_data.wait();
  rados_aio_release(my_completion);
}

TEST_F(StriperTestPP, WaitForSafePP) {
  AioTestData test_data;
  AioCompletion *my_completion =
    librados::Rados::aio_create_completion(&test_data,
                                           set_completion_complete);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("WaitForSafePP", my_completion, bl1, sizeof(buf), 0));
  TestAlarm alarm;
  my_completion->wait_for_complete();
  test_data.wait();
  my_completion->release();
}

TEST_F(StriperTest, RoundTrip) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "RoundTrip", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    test_data.wait();
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTrip", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  test_data.wait();
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTest, RoundTrip2) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "RoundTrip2", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    test_data.wait();
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTrip2", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  test_data.wait();
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTestPP, RoundTripPP) {
  AioTestData test_data;
  AioCompletion *my_completion =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("RoundTripPP", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    test_data.wait();
  }
  bufferlist bl2;
  AioCompletion *my_completion2 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  ASSERT_EQ(0, striper.aio_read("RoundTripPP", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  test_data.wait();
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTestPP, RoundTripPP2) {
  AioTestData test_data;
  AioCompletion *my_completion =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("RoundTripPP2", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    test_data.wait();
  }
  bufferlist bl2;
  AioCompletion *my_completion2 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  ASSERT_EQ(0, striper.aio_read("RoundTripPP2", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  test_data.wait();
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTest, IsComplete) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "IsComplete", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    test_data.wait();
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion2));
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
  test_data.wait();
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTestPP, IsCompletePP) {
  AioTestData test_data;
  AioCompletion *my_completion =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("IsCompletePP", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    test_data.wait();
  }
  bufferlist bl2;
  AioCompletion *my_completion2 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
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
  test_data.wait();
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTest, IsSafe) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
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
  test_data.wait();
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "IsSafe", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  test_data.wait();
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTest, RoundTripAppend) {
  AioTestData test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                           set_completion_complete,
                                           &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_append(striper, "RoundTripAppend", my_completion, buf, sizeof(buf)));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion);
  }
  test_data.wait();
  char buf2[128];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_append(striper, "RoundTripAppend", my_completion2, buf2, sizeof(buf)));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  test_data.wait();
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion3));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTripAppend", my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion3);
  }
  ASSERT_EQ((int)(sizeof(buf) + sizeof(buf2)), rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf), buf2, sizeof(buf2)));
  test_data.wait();
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST_F(StriperTestPP, RoundTripAppendPP) {
  AioTestData test_data;
  AioCompletion *my_completion =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_append("RoundTripAppendPP", my_completion, bl1, sizeof(buf)));
  {
    TestAlarm alarm;
    my_completion->wait_for_complete();
  }
  test_data.wait();
  char buf2[128];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  ASSERT_EQ(0, striper.aio_append("RoundTripAppendPP", my_completion2, bl2, sizeof(buf2)));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  test_data.wait();
  bufferlist bl3;
  AioCompletion *my_completion3 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  ASSERT_EQ(0, striper.aio_read("RoundTripAppendPP", my_completion3, &bl3, 2 * sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion3->wait_for_complete();
  }
  test_data.wait();
  ASSERT_EQ(sizeof(buf) + sizeof(buf2), (unsigned)my_completion3->get_return_value());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(bl3.c_str() + sizeof(buf), buf2, sizeof(buf2)));

  my_completion->release();
  my_completion2->release();
  my_completion3->release();
}

TEST_F(StriperTest, Flush) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "Flush", my_completion, buf, sizeof(buf), 0));
  rados_striper_aio_flush(striper);
  test_data.wait();
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "Flush", my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  test_data.wait();
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST_F(StriperTestPP, FlushPP) {
  AioTestData test_data;
  AioCompletion *my_completion =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("FlushPP", my_completion, bl1, sizeof(buf), 0));
  striper.aio_flush();
  test_data.wait();
  bufferlist bl2;
  AioCompletion *my_completion2 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  ASSERT_EQ(0, striper.aio_read("FlushPP", my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  test_data.wait();
  my_completion->release();
  my_completion2->release();
}

TEST_F(StriperTest, RoundTripWriteFull) {
  AioTestData test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_aio_write(striper, "RoundTripWriteFull", my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion);
  }
  test_data.wait();
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion2));
  ASSERT_EQ(0, rados_striper_aio_write_full(striper, "RoundTripWriteFull", my_completion2, buf2, sizeof(buf2)));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion2);
  }
  test_data.wait();
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion3));
  ASSERT_EQ(0, rados_striper_aio_read(striper, "RoundTripWriteFull", my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    rados_aio_wait_for_complete(my_completion3);
  }
  ASSERT_EQ(sizeof(buf2), (unsigned)rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
  test_data.wait();
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST_F(StriperTestPP, RoundTripWriteFullPP) {
  AioTestData test_data;
  AioCompletion *my_completion =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.aio_write("RoundTripWriteFullPP", my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion->wait_for_complete();
  }
  test_data.wait();
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  ASSERT_EQ(0, striper.aio_write_full("RoundTripWriteFullPP", my_completion2, bl2));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
  }
  test_data.wait();
  bufferlist bl3;
  AioCompletion *my_completion3 =
    librados::Rados::aio_create_completion(&test_data, set_completion_complete);
  ASSERT_EQ(0, striper.aio_read("RoundTripWriteFullPP", my_completion3, &bl3, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion3->wait_for_complete();
  }
  ASSERT_EQ(sizeof(buf2), (unsigned)my_completion3->get_return_value());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  test_data.wait();
  my_completion->release();
  my_completion2->release();
  my_completion3->release();
}

TEST_F(StriperTest, RemoveTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  // create oabject
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "RemoveTest", buf, sizeof(buf), 0));
  // async remove it
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, rados_aio_create_completion2(&test_data,
                                            set_completion_complete,
                                            &my_completion));
  ASSERT_EQ(0, rados_striper_aio_remove(striper, "RemoveTest", my_completion));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  test_data.wait();
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  rados_aio_release(my_completion);
  // check we get ENOENT on reading
  ASSERT_EQ(-ENOENT, rados_striper_read(striper, "RemoveTest", buf2, sizeof(buf2), 0));
}

TEST_F(StriperTestPP, RemoveTestPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("RemoveTestPP", bl, sizeof(buf), 0));
  AioCompletion *my_completion = cluster.aio_create_completion(nullptr, nullptr);
  ASSERT_EQ(0, striper.aio_remove("RemoveTestPP", my_completion));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  ASSERT_EQ(-ENOENT, striper.read("RemoveTestPP", &bl2, sizeof(buf), 0));
  my_completion->release();
}
