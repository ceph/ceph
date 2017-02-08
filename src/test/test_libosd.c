// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <signal.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>

#include "libcephd/ceph_osd.h"


static int test_sync_write(struct libosd *osd,
			   const uint8_t volume[16])
{
  char buf[64] = {};
  int r;

  // flags=NONE -> EINVAL
  r = libosd_write(osd, "sync-inval0", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_FLAGS_NONE, NULL, NULL);
  if (r != -EINVAL) {
    fprintf(stderr, "libosd_write(flags=NONE) returned %d, "
	"expected -EINVAL\n", r);
    return -1;
  }

  // flags=UNSTABLE|STABLE -> EINVAL
  r = libosd_write(osd, "sync-inval2", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE | LIBOSD_WRITE_CB_STABLE,
		   NULL, NULL);
  if (r != -EINVAL) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE|STABLE) returned %d, "
	"expected -EINVAL\n", r);
    return -1;
  }

  // flags=UNSTABLE
  r = libosd_write(osd, "sync-unstable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE, NULL, NULL);
  if (r < 0) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE) failed with %d\n", r);
    return r;
  }
  if (r != sizeof(buf)) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE) wrote %d bytes, "
       " expected %ld\n", r, sizeof(buf));
    return -1;
  }

  // flags=STABLE
  r = libosd_write(osd, "sync-stable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_STABLE, NULL, NULL);
  if (r < 0) {
    fprintf(stderr, "libosd_write(flags=STABLE) failed with %d\n", r);
    return r;
  }
  if (r != sizeof(buf)) {
    fprintf(stderr, "libosd_write(flags=STABLE) wrote %d bytes, "
       " expected %ld\n", r, sizeof(buf));
    return -1;
  }
  return 0;
}

static int test_sync_read(struct libosd *osd, const uint8_t volume[16])
{
  char buf[64] = {};
  int r;

  // read object created by test_sync_write()
  r = libosd_read(osd, "sync-stable", volume, 0, sizeof(buf), buf,
		  LIBOSD_READ_FLAGS_NONE, NULL, NULL);
  if (r < 0) {
    fprintf(stderr, "libosd_read() failed with %d\n", r);
    return r;
  }
  if (r != sizeof(buf)) {
    fprintf(stderr, "libosd_read() read %d bytes, expected %ld\n",
	r, sizeof(buf));
    return -1;
  }
  return 0;
}

struct io_completion {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int result;
  uint64_t length;
  int done;
};

static void completion_cb(int result, uint64_t length, int flags, void *user)
{
  struct io_completion *io = (struct io_completion*)user;
  printf("completion cb result %d length %ld\n", result, length);

  pthread_mutex_lock(&io->mutex);
  io->result = result;
  io->length = length;
  io->done++;
  pthread_cond_signal(&io->cond);
  pthread_mutex_unlock(&io->mutex);
}

static int wait_for_completion(struct io_completion *io, int count)
{
  pthread_mutex_lock(&io->mutex);
  while (io->done < count && io->result == 0)
    pthread_cond_wait(&io->cond, &io->mutex);
  pthread_mutex_unlock(&io->mutex);
  return io->result;
}

static int test_async_write(struct libosd *osd, const uint8_t volume[16])
{
  char buf[64] = {};
  struct io_completion io1 = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .done = 0
  };
  struct io_completion io2 = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .done = 0
  };
  struct io_completion io3 = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .done = 0
  };
  int r;

  // flags=NONE -> EINVAL
  r = libosd_write(osd, "async-inval0", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_FLAGS_NONE, completion_cb, NULL);
  if (r != -EINVAL) {
    fprintf(stderr, "libosd_write(flags=NONE) returned %d, "
	"expected -EINVAL\n", r);
    return -1;
  }

  // flags=UNSTABLE
  r = libosd_write(osd, "async-unstable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE, completion_cb, &io1);
  if (r != 0) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE) failed with %d\n", r);
    return r;
  }
  r = wait_for_completion(&io1, 1);
  if (r != 0) {
    fprintf(stderr, "completion_cb(flags=UNSTABLE) got result %d\n", r);
    return r;
  }
  if (io1.length != sizeof(buf)) {
    fprintf(stderr, "completion_cb(flags=UNSTABLE) got length %ld, "
	"expected %ld\n", io1.length, sizeof(buf));
    return -1;
  }

  // flags=STABLE
  r = libosd_write(osd, "async-stable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_STABLE, completion_cb, &io2);
  if (r != 0) {
    fprintf(stderr, "libosd_write(flags=STABLE) failed with %d\n", r);
    return r;
  }
  r = wait_for_completion(&io2, 1);
  if (r != 0) {
    fprintf(stderr, "completion_cb(flags=STABLE) got result %d\n", r);
    return r;
  }
  if (io2.length != sizeof(buf)) {
    fprintf(stderr, "completion_cb(flags=STABLE) got length %ld, "
	"expected %ld\n", io2.length, sizeof(buf));
    return -1;
  }

  // flags=UNSTABLE|STABLE
  r = libosd_write(osd, "async-unstable-stable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE | LIBOSD_WRITE_CB_STABLE,
		   completion_cb, &io3);
  if (r != 0) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE|STABLE) failed with %d\n", r);
    return r;
  }
  r = wait_for_completion(&io3, 1); // wait for both callbacks
  if (r != 0) {
    fprintf(stderr, "completion_cb(flags=UNSTABLE|STABLE) got result %d\n", r);
    return r;
  }
  if (io3.length != sizeof(buf)) {
    fprintf(stderr, "completion_cb(flags=UNSTABLE) got length %ld, "
	"expected %ld\n", io3.length, sizeof(buf));
    return -1;
  }
  return 0;
}

static int test_async_read(struct libosd *osd, const uint8_t volume[16])
{
  char buf[64] = {};
  struct io_completion io = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .done = 0
  };
  int r;

  r = libosd_read(osd, "async-stable", volume, 0, sizeof(buf), buf,
		  LIBOSD_READ_FLAGS_NONE, completion_cb, &io);
  if (r != 0) {
    fprintf(stderr, "libosd_read() failed with %d\n", r);
    return r;
  }
  r = wait_for_completion(&io, 1);
  if (r != 0) {
    fprintf(stderr, "completion_cb() got result %d\n", r);
    return r;
  }
  if (io.length != sizeof(buf)) {
    fprintf(stderr, "completion_cb() got length %ld, "
	"expected %ld\n", io.length, sizeof(buf));
    return -1;
  }
  return 0;
}


static int run_tests(struct libosd *osd, const uint8_t volume[16])
{
  int r = test_sync_write(osd, volume);
  if (r != 0) {
    fprintf(stderr, "test_sync_write() failed with %d\n", r);
    return r;
  }

  r = test_sync_read(osd, volume);
  if (r != 0) {
    fprintf(stderr, "test_sync_read() failed with %d\n", r);
    return r;
  }

  r = test_async_write(osd, volume);
  if (r != 0) {
    fprintf(stderr, "test_async_write() failed with %d\n", r);
    return r;
  }

  r = test_async_read(osd, volume);
  if (r != 0) {
    fprintf(stderr, "test_async_read() failed with %d\n", r);
    return r;
  }
  puts("libosd tests passed");
  return 0;
}

int main(int argc, const char *argv[])
{
  uint8_t volume[16];
  int r = 0;
  struct libosd_init_args args = {
    .id = 0,
    .config = NULL,
  };
  struct libosd *osd = libosd_init(&args);
  if (osd == NULL) {
    fputs("osd init failed\n", stderr);
    return -1;
  }
  signal(SIGINT, libosd_signal);

  r = libosd_get_volume(osd, "rbd", volume);
  if (r != 0) {
    fprintf(stderr, "libosd_get_volume() failed with %d\n", r);
  } else {
    r = run_tests(osd, volume);
  }

  libosd_shutdown(osd);
  libosd_join(osd);
  libosd_cleanup(osd);
  return r;
}
